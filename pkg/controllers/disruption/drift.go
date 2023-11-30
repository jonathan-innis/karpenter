/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/apis/v1beta1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient  client.Client
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	recorder    events.Recorder
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return options.FromContext(ctx).FeatureGates.Drift &&
		c.NodeClaim.StatusConditions().GetCondition(v1beta1.Drifted).IsTrue()
}

// SortCandidates orders drifted candidates by when they've drifted
func (d *Drift) filterAndSortCandidates(ctx context.Context, candidates []*Candidate) ([]*Candidate, error) {
	candidates, err := filterCandidates(ctx, d.kubeClient, d.recorder, candidates)
	if err != nil {
		return nil, fmt.Errorf("filtering candidates, %w", err)
	}
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().GetCondition(v1beta1.Drifted).LastTransitionTime.Inner.Time.Before(
			candidates[j].NodeClaim.StatusConditions().GetCondition(v1beta1.Drifted).LastTransitionTime.Inner.Time)
	})
	return candidates, nil
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommand(ctx context.Context, candidates ...*Candidate) (Command, error) {
	candidates, err := d.filterAndSortCandidates(ctx, candidates)
	if err != nil {
		return Command{}, err
	}
	disruptionEligibleNodesGauge.With(map[string]string{
		methodLabel:            d.Type(),
		consolidationTypeLabel: d.ConsolidationType(),
	}).Set(float64(len(candidates)))

	// Disrupt all empty drifted candidates, as they require no scheduling simulations.
	if empty := lo.Filter(candidates, func(c *Candidate, _ int) bool {
		return len(c.pods) == 0
	}); len(empty) > 0 {
		return Command{
			candidates: empty,
		}, nil
	}

	for _, candidate := range candidates {
		// Check if we need to create any NodeClaims.
		results, err := simulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return Command{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, "Scheduling simulation failed to schedule all pods")...)
			continue
		}
		if len(results.NewNodeClaims) == 0 {
			return Command{
				candidates: []*Candidate{candidate},
			}, nil
		}
		return Command{
			candidates:   []*Candidate{candidate},
			replacements: results.NewNodeClaims,
		}, nil
	}
	return Command{}, nil
}

func (d *Drift) Type() string {
	return metrics.DriftReason
}

func (d *Drift) ConsolidationType() string {
	return ""
}