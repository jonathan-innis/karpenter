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
	"math"
	"sort"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"

	provisioningdynamic "sigs.k8s.io/karpenter/pkg/controllers/provisioning/dynamic"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Overprovisioned is a subreconciler that deletes candidates that extend beyond a static NodePool's desired replicas.
type Overprovisioned struct {
	kubeClient client.Client
	cluster    *state.Cluster
	controller *provisioningdynamic.Controller
	recorder   events.Recorder
}

func NewOverprovisioned(kubeClient client.Client, cluster *state.Cluster, controller *provisioningdynamic.Controller, recorder events.Recorder) *Overprovisioned {
	return &Overprovisioned{
		kubeClient: kubeClient,
		cluster:    cluster,
		controller: controller,
		recorder:   recorder,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (o *Overprovisioned) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	if c.NodePool.Spec.Replicas == nil {
		return false
	}
	return o.cluster.NodePoolNodesFor(c.NodePool.Name) > int(lo.FromPtr(c.NodePool.Spec.Replicas))
}

// ComputeCommand generates a disruption command given candidates
func (o *Overprovisioned) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.CreationTimestamp.Before(&candidates[j].NodeClaim.CreationTimestamp)
	})

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var cmds []Command
	nodePoolCandidateMapping := map[string]int{}
	for _, candidate := range candidates {
		if timeoutCtx.Err() != nil {
			break
		}
		// Filter out empty candidates. If there was an empty node that wasn't consolidated before this, we should
		// assume that it was due to budgets. If we don't filter out budgets, users who set a budget for `empty`
		// can find their nodes disrupted here, which while that in itself isn't an issue for empty nodes, it could
		// constrain the `overpvoisioned` budget.
		if len(candidate.reschedulablePods) == 0 {
			continue
		}
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(timeoutCtx, o.kubeClient, o.cluster, o.controller, candidate)
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

		validCommand := true
		for nodePoolName, nodeClaims := range lo.GroupBy(results.NewNodeClaims, func(n *pscheduling.NodeClaim) string { return n.NodePoolName }) {
			nodePool := &v1.NodePool{}
			if err = o.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
				validCommand = false
				break
			}
			nodeLimit := int64(math.MaxInt64)
			if v, ok := nodePool.Spec.Limits[corev1.ResourceName("nodes")]; ok {
				nodeLimit = v.Value()
			}
			if o.cluster.TotalNodePoolNodesFor(nodePoolName)+nodePoolCandidateMapping[nodePoolName]+len(nodeClaims) > int(nodeLimit) {
				validCommand = false
				break
			}
		}
		if !validCommand {
			continue
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			o.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}
		disruptionBudgetMapping[candidate.NodePool.Name]--
		nodePoolCandidateMapping[candidate.NodePool.Name]++
		cmds = append(cmds, Command{
			Candidates:   []*Candidate{candidate},
			Replacements: replacementsFromNodeClaims(results.NewNodeClaims...),
			Results:      results,
		})
	}
	return cmds, nil
}

func (o *Overprovisioned) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonOverprovisioned
}

func (o *Overprovisioned) Class() string {
	return EventualDisruptionClass
}

func (o *Overprovisioned) ConsolidationType() string {
	return ""
}
