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

package static

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	cluster       *state.Cluster
	provisioner   *provisioning.Provisioner
}

func NewController(kubeClient client.Client, cluster *state.Cluster, recorder events.Recorder, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		cluster:       cluster,
		provisioner:   provisioning.NewProvisioner(kubeClient, recorder, cluster),
	}
}

// Reconcile the resource
func (c *Controller) Reconcile(ctx context.Context, np *v1.NodePool) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "provisioning.static")

	if !nodepoolutils.IsManaged(np, c.cloudProvider) {
		return reconcile.Result{}, nil
	}
	if !np.StatusConditions().Root().IsTrue() {
		return reconcile.Result{}, nil
	}
	if np.Spec.Replicas == nil {
		return reconcile.Result{}, nil
	}
	if !c.cluster.HasSynced() {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	runningNodes := c.cluster.NodePoolNodesFor(np.Name)
	totalNodes := c.cluster.TotalNodePoolNodesFor(np.Name)
	nodeLimit := int64(math.MaxInt64)
	if v, ok := np.Spec.Limits[corev1.ResourceName("nodes")]; ok {
		nodeLimit = v.Value()
	}
	if runningNodes >= int(lo.FromPtr(np.Spec.Replicas)) || totalNodes >= int(nodeLimit) {
		return reconcile.Result{RequeueAfter: time.Minute}, nil
	}

	// Create the number of NodeClaims equal to the desired replica count
	var nodeClaims []*scheduling.NodeClaim
	nct := scheduling.NewNodeClaimTemplate(np)
	for range lo.Min([]int{int(lo.FromPtr(np.Spec.Replicas)) - runningNodes, int(nodeLimit) - totalNodes}) {
		nodeClaims = append(nodeClaims, &scheduling.NodeClaim{NodeClaimTemplate: *nct})
	}
	if _, err := c.provisioner.CreateNodeClaims(ctx, nodeClaims, provisioning.WithReason(metrics.ProvisionedReason)); err != nil {
		return reconcile.Result{}, fmt.Errorf("creating nodeclaims, %w", err)
	}
	return reconcile.Result{RequeueAfter: time.Minute}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("provisioning.static").
		For(&v1.NodePool{}, builder.WithPredicates(nodepoolutils.IsManagedPredicateFuncs(c.cloudProvider), predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				return (e.ObjectOld.(*v1.NodePool).Spec.Replicas != e.ObjectNew.(*v1.NodePool).Spec.Replicas) ||
					(!e.ObjectOld.(*v1.NodePool).StatusConditions().Root().IsTrue() != e.ObjectNew.(*v1.NodePool).StatusConditions().Root().IsTrue())
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		})).
		Watches(&v1.NodeClaim{}, nodeutils.NodeClaimEventHandler(c.kubeClient), builder.WithPredicates(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return true
			},
			GenericFunc: func(e event.GenericEvent) bool {
				return false
			},
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
