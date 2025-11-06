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
	"fmt"
	"math"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/tracing"

	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// StaticDrift is a subreconciler that deletes drifted static candidates.
type StaticDrift struct {
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
	cloudprovider cloudprovider.CloudProvider
}

func NewStaticDrift(cluster *state.Cluster, provisioner *provisioning.Provisioner, cloudprovider cloudprovider.CloudProvider) *StaticDrift {
	return &StaticDrift{
		cluster:       cluster,
		provisioner:   provisioner,
		cloudprovider: cloudprovider,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *StaticDrift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	ctx = tracing.StartSpan(ctx, "ShouldDisrupt", trace.WithAttributes(
		attribute.String("candidate.node", c.Node.Name),
		attribute.String("candidate.nodeclaim", c.NodeClaim.Name),
		attribute.String("candidate.nodepool", c.NodePool.Name)),
	)

	if !c.OwnedByStaticNodePool() {
		tracing.EndSpan(ctx, fmt.Errorf("candidate is not owned by a static nodepool"))
		return false
	}
	if !c.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue() {
		tracing.EndSpan(ctx, fmt.Errorf("candidate does not have the drifted condition"))
		return false
	}
	tracing.EndSpan(ctx, nil)
	return true
}

func (d *StaticDrift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (cmds []Command, err error) {
	ctx = tracing.StartSpan(ctx, "ComputeCommands", trace.WithAttributes(
		attribute.Int("candidates.count", len(candidates))),
	)
	defer tracing.EndSpan(ctx, err)

	// Group candidates by nodepool name
	candidatesByNodePool := lo.GroupBy(candidates, func(candidate *Candidate) string {
		return candidate.NodePool.Name
	})

	for npName, npCandidates := range candidatesByNodePool {
		nodePoolCtx := tracing.StartSpan(ctx, "ComputeCommands.filterNodePoolCandidates", trace.WithAttributes(attribute.String("nodepool.name", npName), attribute.Int("candidate.count", len(npCandidates))))

		np := npCandidates[0].NodePool

		if disruptionBudgetMapping[npName] == 0 {
			tracing.SpanFromContext(nodePoolCtx).AddEvent("candidates are constrained by budgets")
			tracing.EndSpan(nodePoolCtx, nil)
			continue
		}

		limit, ok := np.Spec.Limits[resources.Node]
		nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
		// Current nodes (includes in‑flight per your cluster state)
		runningNodes, _, nodesPendingDisruptionCount := d.cluster.NodePoolState.GetNodeCount(npName)

		// We dont want to disrupt nodes until scale down is complete
		if int64(runningNodes+nodesPendingDisruptionCount) > lo.FromPtr(np.Spec.Replicas) {
			tracing.SpanFromContext(nodePoolCtx).AddEvent("candidates are not allowed to be disrupted because the node pool is currently above its desired size")
			tracing.EndSpan(nodePoolCtx, nil)
			continue
		}

		maxDrifts := lo.Min([]int64{
			int64(disruptionBudgetMapping[np.Name]),
			int64(len(npCandidates)),
		})

		// Acquire limits from cluster state without bursting over
		maxAllowedDrifts := d.cluster.NodePoolState.ReserveNodeCount(npName, nodeLimit, maxDrifts)
		tracing.SpanFromContext(nodePoolCtx).SetAttributes(attribute.Int("maxAllowedDrifts", int(maxAllowedDrifts)))

		// We will not get a negative value here
		if maxAllowedDrifts == 0 {
			tracing.SpanFromContext(nodePoolCtx).AddEvent("no candidates are allowed to be disrupted because maxDrifts for nodepool has been reached")
			tracing.EndSpan(nodePoolCtx, nil)
			continue
		}

		// Select candidates up to maxAllowedDrifts
		for _, c := range npCandidates[:maxAllowedDrifts] {
			nct := scheduling.NewNodeClaimTemplate(np)
			result := scheduling.Results{
				NewNodeClaims: []*scheduling.NodeClaim{{NodeClaimTemplate: *nct}},
			}
			cmds = append(cmds, Command{
				Candidates:   []*Candidate{c},
				Replacements: replacementsFromNodeClaims(result.NewNodeClaims...),
				Results:      result,
			})
		}
		tracing.SpanFromContext(nodePoolCtx).AddEvent("created replacements for candidates up to maxAllowedDrifts", trace.WithAttributes(
			attribute.Int("candidates.count", int(maxAllowedDrifts))),
		)
		tracing.EndSpan(nodePoolCtx, nil)
	}
	tracing.SpanFromContext(ctx).AddEvent("completed search of all static drift commands, creating commands for all nodepools", trace.WithAttributes(attribute.Int("commands.count", len(cmds))))
	return cmds, nil
}

func (d *StaticDrift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *StaticDrift) Class() string {
	return EventualDisruptionClass
}

func (d *StaticDrift) ConsolidationType() string {
	return ""
}
