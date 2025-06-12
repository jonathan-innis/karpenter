/*
Copyright The Kubernetes Authors.

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
	"sort"
	"time"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"

	provisioningdynamic "sigs.k8s.io/karpenter/pkg/controllers/provisioning/dynamic"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Drift is a subreconciler that deletes drifted candidates.
type Drift struct {
	kubeClient client.Client
	cluster    *state.Cluster
	controller *provisioningdynamic.Controller
	recorder   events.Recorder
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, controller *provisioningdynamic.Controller, recorder events.Recorder) *Drift {
	return &Drift{
		kubeClient: kubeClient,
		cluster:    cluster,
		controller: controller,
		recorder:   recorder,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	return c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommands generates a disruption command given candidates
//
//nolint:gocyclo
func (d *Drift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var cmds []Command
	for _, candidate := range candidates {
		log.FromContext(ctx).WithValues("candidate", candidate.NodeClaim.Name).Info("considering candidate for drift")
		if timeoutCtx.Err() != nil {
			break
		}
		// Filter out empty candidates. If there was an empty node that wasn't consolidated before this, we should
		// assume that it was due to budgets. If we don't filter out budgets, users who set a budget for `empty`
		// can find their nodes disrupted here, which while that in itself isn't an issue for empty nodes, it could
		// constrain the `drift` budget.
		if len(candidate.reschedulablePods) == 0 && candidate.NodePool.Spec.Replicas == nil {
			continue
		}
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}

		var results scheduling.Results
		var err error
		if candidate.NodePool.Spec.Replicas != nil && d.cluster.NodePoolNodesFor(candidate.NodePool.Name) <= int(lo.FromPtr(candidate.NodePool.Spec.Replicas)) {
			results = scheduling.Results{
				NewNodeClaims: []*scheduling.NodeClaim{{NodeClaimTemplate: *scheduling.NewNodeClaimTemplate(candidate.NodePool), Pods: candidate.reschedulablePods}},
			}
		} else {
			// Check if we need to create any NodeClaims.
			results, err = SimulateScheduling(timeoutCtx, d.kubeClient, d.cluster, d.controller, candidate)
			if err != nil {
				// if a candidate is now deleting, just retry
				if errors.Is(err, errCandidateDeleting) {
					continue
				}
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				return nil, err
			}
		}

		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		cmds = append(cmds, Command{
			Candidates:   []*Candidate{candidate},
			Replacements: replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:      results,
		})
	}
	return cmds, nil
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
