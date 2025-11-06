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

	"github.com/awslabs/operatorpkg/option"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/operator/tracing"
)

// Emptiness is a subreconciler that deletes empty candidates.
type Emptiness struct {
	consolidation
	validator Validator
}

func NewEmptiness(c consolidation, opts ...option.Function[MethodOptions]) *Emptiness {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewEmptinessValidator(c))}, opts...)...)
	return &Emptiness{consolidation: c, validator: o.validator}
}

// ShouldDisrupt is a predicate used to filter candidates
func (e *Emptiness) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	ctx = tracing.StartSpan(ctx, "ShouldDisrupt", trace.WithAttributes(
		attribute.String("candidate.node", c.Node.Name),
		attribute.String("candidate.nodeclaim", c.NodeClaim.Name),
		attribute.String("candidate.nodepool", c.NodePool.Name)),
	)

	if c.OwnedByStaticNodePool() {
		tracing.EndSpan(ctx, fmt.Errorf("candidate is owned by a static nodepool"))
		return false
	}
	// If consolidation is disabled, don't do anything. This emptiness should run for both WhenEmpty and WhenEmptyOrUnderutilized
	if c.NodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		tracing.EndSpan(ctx, fmt.Errorf("candidate is not consolidatable because nodepool %q has consolidation disabled", c.NodePool.Name))
		e.recorder.Publish(disruptionevents.Unconsolidatable(c.Node, c.NodeClaim, fmt.Sprintf("NodePool %q has consolidation disabled", c.NodePool.Name))...)
		return false
	}
	// A node hosting virtual buffer pods is not empty — the provisioner placed
	// buffer capacity here intentionally. Deleting it would trigger immediate
	// re-provisioning (pointless churn). This is the only disruption path that
	// checks HasBufferPods; single/multi-node consolidation naturally accounts
	// for buffer pods via SimulateScheduling (which calls GetPendingPods, injecting
	// virtual pods into the pending set — any replacement must fit them too).
	if e.cluster.HasBufferPods(c.ProviderID()) {
		e.recorder.Publish(disruptionevents.Unconsolidatable(c.Node, c.NodeClaim, fmt.Sprintf("Node %q has buffer pods", c.Node.Name))...)
		return false
	}
	// return true if there are no pods and the nodeclaim is consolidatable
	if len(c.reschedulablePods) != 0 {
		tracing.EndSpan(ctx, fmt.Errorf("candidate has reschedulable pods"))
		return false
	}
	if !c.NodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue() {
		tracing.EndSpan(ctx, fmt.Errorf("candidate does not have the consolidatable condition"))
		return false
	}
	tracing.EndSpan(ctx, nil)
	return true
}

// ComputeCommand generates a disruption command given candidates
//
//nolint:gocyclo
func (e *Emptiness) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (cmds []Command, err error) {
	ctx = tracing.StartSpan(ctx, "ComputeCommands", trace.WithAttributes(attribute.Int("candidates.count", len(candidates))))
	defer tracing.EndSpan(ctx, err)

	if e.IsConsolidated() {
		tracing.SpanFromContext(ctx).AddEvent("cluster has been recently consolidated so exiting early")
		return []Command{}, nil
	}
	candidates = e.sortCandidates(candidates)

	empty := make([]*Candidate, 0, len(candidates))
	constrainedByBudgets := false
	for _, candidate := range candidates {
		candidateCtx := tracing.StartSpan(ctx, "ComputeCommands.filterCandidates", trace.WithAttributes(
			attribute.String("candidate.node", candidate.Node.Name),
			attribute.String("candidate.nodeclaim", candidate.NodeClaim.Name),
			attribute.String("candidate.nodepool", candidate.NodePool.Name),
			attribute.Int("candidate.reschedulablePods", len(candidate.reschedulablePods)),
			attribute.Int("candidate.remainingBudget", disruptionBudgetMapping[candidate.NodePool.Name])),
		)
		if len(candidate.reschedulablePods) > 0 {
			tracing.SpanFromContext(candidateCtx).AddEvent("candidate has reschedulable pods")
			tracing.EndSpan(candidateCtx, nil)
			continue
		}
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			// set constrainedByBudgets to true if any node was a candidate but was constrained by a budget
			constrainedByBudgets = true
			tracing.SpanFromContext(candidateCtx).AddEvent("candidate is constrained by budgets")
			tracing.EndSpan(candidateCtx, nil)
			continue
		}
		// If there's disruptions allowed for the candidate's nodepool,
		// add it to the list of candidates, and decrement the budget.
		empty = append(empty, candidate)
		disruptionBudgetMapping[candidate.NodePool.Name]--
		tracing.EndSpan(candidateCtx, nil)
	}
	// none empty, so do nothing
	tracing.SpanFromContext(ctx).SetAttributes(attribute.Bool("constrainedByBudgets", constrainedByBudgets))
	if len(empty) == 0 {
		tracing.SpanFromContext(ctx).SetAttributes(attribute.Int("candidates.empty.count", len(empty)))
		tracing.SpanFromContext(ctx).AddEvent("no empty candidates found")

		// if there are no candidates, but a nodepool had a fully blocking budget,
		// don't mark the cluster as consolidated, as it's possible this nodepool
		// should be consolidated the next time we try to disrupt.
		if !constrainedByBudgets {
			e.markConsolidated()
		}
		return []Command{}, nil
	}

	cmd := Command{
		Candidates: empty,
	}
	validCmd, err := e.validator.Validate(ctx, cmd, commandValidationDelay)
	if err != nil {
		if IsValidationError(err) {
			tracing.SpanFromContext(ctx).AddEvent(fmt.Sprintf("abandoning empty node consolidation attempt due to pod churn, command is no longer valid, %s", err))
			log.FromContext(ctx).V(1).WithValues(cmd.LogValues()...).Info("abandoning empty node consolidation attempt due to pod churn, command is no longer valid")
			return []Command{}, nil
		}
		return []Command{}, err
	}
	tracing.SpanFromContext(ctx).AddEvent("found valid empty node consolidation command", trace.WithAttributes(
		attribute.Int("candidates.count", len(validCmd.Candidates)),
		attribute.Int("replacements.count", len(validCmd.Replacements))),
	)
	return []Command{validCmd}, nil
}

func (e *Emptiness) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (e *Emptiness) Class() string {
	return GracefulDisruptionClass
}

func (e *Emptiness) ConsolidationType() string {
	return "empty"
}
