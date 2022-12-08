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

package state

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter-core/pkg/apis/config/settings"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha1"
	"github.com/aws/karpenter-core/pkg/apis/v1alpha5"
	"github.com/aws/karpenter-core/pkg/cloudprovider"
	"github.com/aws/karpenter-core/pkg/scheduling"
	podutils "github.com/aws/karpenter-core/pkg/utils/pod"
	"github.com/aws/karpenter-core/pkg/utils/resources"
	"github.com/aws/karpenter-core/pkg/utils/sets"
)

// Cluster maintains cluster state that is often needed but expensive to compute.
type Cluster struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	clock         clock.Clock

	// State: Node Status & Pod -> Node Binding
	mu sync.RWMutex

	// Nodes contains
	inflightNodes map[string]*Node                // inflight node name -> inflight node
	nodes         map[string]*Node                // node name -> node
	bindings      map[types.NamespacedName]string // pod namespaced named -> node name

	inflightNodeNameToProviderID map[string]string // inflight node name -> provider id
	nodeNameToProviderID         map[string]string // node name -> provider id

	inflightProviderIDs          sets.Set[string] // determines whether there is an inflight representation of this node
	initializedProviderIDs       sets.Set[string] // initialized nodes based on providerID
	markedForDeletionProviderIDs sets.Set[string] // marked for deletion nodes based on providerID
	nominatedProviderIDs         *cache.Cache     // nominated nodes based on providerID

	antiAffinityPods sync.Map // mapping of pod namespaced name to *v1.Pod of pods that have required anti affinities

	// consolidationState is a number indicating the state of the cluster with respect to consolidation.  If this number
	// hasn't changed, it indicates that the cluster hasn't changed in a state which would enable consolidation if
	// it previously couldn't occur.
	consolidationState int64
}

func NewCluster(ctx context.Context, clk clock.Clock, client client.Client, cp cloudprovider.CloudProvider) *Cluster {
	// The nominationPeriod is how long we consider a node as 'likely to be used' after a pending pod was
	// nominated for it. This time can very depending on the batching window size + time spent scheduling
	// so we try to adjust based off the window size.
	nominationPeriod := 2 * settings.FromContext(ctx).BatchMaxDuration.Duration
	if nominationPeriod < 10*time.Second {
		nominationPeriod = 10 * time.Second
	}

	c := &Cluster{
		clock:         clk,
		kubeClient:    client,
		cloudProvider: cp,
		inflightNodes: map[string]*Node{},
		nodes:         map[string]*Node{},
		bindings:      map[types.NamespacedName]string{},

		inflightNodeNameToProviderID: map[string]string{},
		nodeNameToProviderID:         map[string]string{},

		inflightProviderIDs:          sets.New[string](),
		initializedProviderIDs:       sets.New[string](),
		markedForDeletionProviderIDs: sets.New[string](),
		nominatedProviderIDs:         cache.New(nominationPeriod, 10*time.Second),
	}
	c.nominatedProviderIDs.OnEvicted(c.onNominatedNodeEviction)
	return c
}

// Node is a cached version of a node in the cluster that maintains state which is expensive to compute every time it's
// needed.  This currently contains node utilization across all the allocatable resources, but will soon be used to
// compute topology information.
// +k8s:deepcopy-gen=true
type Node struct {
	Node *v1.Node
	// Capacity is the total resources on the node.
	Capacity v1.ResourceList
	// Allocatable is the total amount of resources on the node after os overhead.
	Allocatable v1.ResourceList
	// Available is allocatable minus anything allocated to pods.
	Available v1.ResourceList
	// Available is the total amount of resources that are available on the node.  This is the Allocatable minus the
	// resources requested by all pods bound to the node.
	// DaemonSetRequested is the total amount of resources that have been requested by daemon sets.  This allows users
	// of the Node to identify the remaining resources that we expect future daemonsets to consume.  This is already
	// included in the calculation for Available.
	DaemonSetRequested v1.ResourceList
	DaemonSetLimits    v1.ResourceList
	// HostPort usage of all pods that are bound to the node
	HostPortUsage *scheduling.HostPortUsage
	VolumeUsage   *scheduling.VolumeLimits
	VolumeLimits  scheduling.VolumeCount

	podRequests map[types.NamespacedName]v1.ResourceList
	podLimits   map[types.NamespacedName]v1.ResourceList

	// PodTotalRequests is the total resources on pods scheduled to this node
	PodTotalRequests v1.ResourceList
	// PodTotalLimits is the total resource limits scheduled to this node
	PodTotalLimits v1.ResourceList
	// MarkedForDeletion marks this node to say that there is some controller that is
	// planning to delete this node so consider pods that are present on it available for scheduling
	MarkedForDeletion bool
}

// ForPodsWithAntiAffinity calls the supplied function once for each pod with required anti affinity terms that is
// currently bound to a node. The pod returned may not be up-to-date with respect to status, however since the
// anti-affinity terms can't be modified, they will be correct.
func (c *Cluster) ForPodsWithAntiAffinity(fn func(p *v1.Pod, n *v1.Node) bool) {
	c.antiAffinityPods.Range(func(key, value interface{}) bool {
		pod := value.(*v1.Pod)
		c.mu.RLock()
		defer c.mu.RUnlock()
		nodeName, ok := c.bindings[client.ObjectKeyFromObject(pod)]
		if !ok {
			return true
		}
		node, ok := c.nodes[nodeName]
		if !ok {
			// if we receive the node deletion event before the pod deletion event, this can happen
			return true
		}
		return fn(pod, node.Node)
	})
}

// ForEachNode calls the supplied function once per node object that is being tracked. It is not safe to store the
// state.Node object, it should be only accessed from within the function provided to this method.
func (c *Cluster) ForEachNode(f func(n *Node) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeMap := map[string]*Node{}
	// We take all the inflight nodes to be valid nodes. Then, if there are real nodes that have the same providerID
	// and are initialized, we take those to be the source-of-truth over the inflight nodes.
	for _, node := range c.inflightNodes {
		if c.markedForDeletionProviderIDs.Has(node.Node.Spec.ProviderID) {
			node.MarkedForDeletion = true
		}
		nodeMap[node.Node.Spec.ProviderID] = node
	}
	for _, node := range c.nodes {
		if c.markedForDeletionProviderIDs.Has(node.Node.Spec.ProviderID) {
			node.MarkedForDeletion = true
		}
		if _, ok := nodeMap[node.Node.Spec.ProviderID]; ok && !c.initializedProviderIDs.Has(node.Node.Spec.ProviderID) {
			continue
		}
		nodeMap[node.Node.Spec.ProviderID] = node
	}
	nodes := lo.Values(nodeMap)

	// sort nodes by creation time, so we provide a consistent ordering
	sort.Slice(nodes, func(a, b int) bool {
		if nodes[a].Node.CreationTimestamp != nodes[b].Node.CreationTimestamp {
			return nodes[a].Node.CreationTimestamp.Time.Before(nodes[b].Node.CreationTimestamp.Time)
		}
		// sometimes we get nodes created in the same second, so sort again by node UID to provide a consistent ordering
		return nodes[a].Node.UID < nodes[b].Node.UID
	})
	for _, node := range nodes {
		if !f(node) {
			return
		}
	}
}

// IsNodeNominated returns true if the given node was expected to have a pod bound to it during a recent scheduling
// batch
func (c *Cluster) IsNodeNominated(nodeName string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if id, ok := c.nodeNameToProviderID[nodeName]; ok {
		_, found := c.nominatedProviderIDs.Get(id)
		return found
	}
	if id, ok := c.inflightNodeNameToProviderID[nodeName]; ok {
		_, found := c.nominatedProviderIDs.Get(id)
		return found
	}
	return false
}

// NominateNodeForPod records that a node was the target of a pending pod during a scheduling batch
func (c *Cluster) NominateNodeForPod(nodeName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if id, ok := c.nodeNameToProviderID[nodeName]; ok {
		c.nominatedProviderIDs.SetDefault(id, nil)
	}
	if id, ok := c.inflightNodeNameToProviderID[nodeName]; ok {
		c.nominatedProviderIDs.SetDefault(id, nil)
	}
}

// UnmarkForDeletion removes the marking on the node as a node the controller intends to delete
func (c *Cluster) UnmarkForDeletion(nodeNames ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, name := range nodeNames {
		if id, ok := c.nodeNameToProviderID[name]; ok {
			c.markedForDeletionProviderIDs.Delete(id)
		}
		if id, ok := c.inflightNodeNameToProviderID[name]; ok {
			c.markedForDeletionProviderIDs.Delete(id)
		}
	}
}

// MarkForDeletion marks the node as pending deletion in the internal cluster state
func (c *Cluster) MarkForDeletion(nodeNames ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, name := range nodeNames {
		if id, ok := c.nodeNameToProviderID[name]; ok {
			c.markedForDeletionProviderIDs.Insert(id)
		}
		if id, ok := c.inflightNodeNameToProviderID[name]; ok {
			c.markedForDeletionProviderIDs.Insert(id)
		}
	}
}

// newNode always returns a node, even if some portion of the update has failed
func (c *Cluster) newNode(ctx context.Context, node *v1.Node) (*Node, error) {
	n := &Node{
		Node:          node,
		Capacity:      v1.ResourceList{},
		Allocatable:   v1.ResourceList{},
		Available:     v1.ResourceList{},
		HostPortUsage: scheduling.NewHostPortUsage(),
		VolumeUsage:   scheduling.NewVolumeLimits(c.kubeClient),
		VolumeLimits:  scheduling.VolumeCount{},
		podRequests:   map[types.NamespacedName]v1.ResourceList{},
		podLimits:     map[types.NamespacedName]v1.ResourceList{},
	}
	if err := multierr.Combine(
		c.populateCapacity(ctx, node, n),
		c.populateVolumeLimits(ctx, node, n),
		c.populateResourceRequests(ctx, node, n),
	); err != nil {
		return nil, err
	}
	return n, nil
}

func (c *Cluster) newInflightNode(machine *v1alpha1.Machine) *Node {
	return &Node{
		Node: &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:              machine.Name,
				CreationTimestamp: machine.CreationTimestamp,
				DeletionTimestamp: machine.DeletionTimestamp,
				Labels:            machine.Labels,
				Annotations:       machine.Annotations,
			},
			Spec: v1.NodeSpec{
				Taints:     append(machine.Spec.Taints, machine.Spec.StartupTaints...),
				ProviderID: machine.Status.ProviderID,
			},
			Status: v1.NodeStatus{
				Capacity:    machine.Status.Capacity,
				Allocatable: machine.Status.Allocatable,
			},
		},
		Capacity:    machine.Status.Capacity,
		Allocatable: machine.Status.Allocatable,
	}
}

// nolint:gocyclo
// TODO joinnis: Note that this entire function can be removed and reduced down to checking the
// Node status for the capacity and allocatable when we have migrated away from using node labels and we have fully
// migrated everyone over to using the Machine CR
func (c *Cluster) populateCapacity(ctx context.Context, node *v1.Node, n *Node) error {
	// Use node's values if initialized
	if node.Labels[v1alpha5.LabelNodeInitialized] == "true" {
		n.Allocatable = node.Status.Allocatable
		n.Capacity = node.Status.Capacity
		return nil
	}
	// Fallback to instance type capacity otherwise
	provisioner := &v1alpha5.Provisioner{}
	// In flight nodes not owned by karpenter are not included in calculations
	if _, ok := node.Labels[v1alpha5.ProvisionerNameLabelKey]; !ok {
		return nil
	}
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Labels[v1alpha5.ProvisionerNameLabelKey]}, provisioner); err != nil {
		// Nodes that are not owned by an existing provisioner are not included in calculations
		return client.IgnoreNotFound(fmt.Errorf("getting provisioner, %w", err))
	}
	instanceTypes, err := c.cloudProvider.GetInstanceTypes(ctx, provisioner)
	if err != nil {
		return err
	}
	instanceType, ok := lo.Find(instanceTypes, func(it *cloudprovider.InstanceType) bool { return it.Name == node.Labels[v1.LabelInstanceTypeStable] })
	if !ok {
		return fmt.Errorf("instance type '%s' not found", node.Labels[v1.LabelInstanceTypeStable])
	}

	n.Capacity = lo.Assign(node.Status.Capacity) // ensure map not nil
	// Use instance type resource value if resource isn't currently registered in .Status.Capacity
	for resourceName, quantity := range instanceType.Capacity {
		if resources.IsZero(node.Status.Capacity[resourceName]) {
			n.Capacity[resourceName] = quantity
		}
	}
	n.Allocatable = lo.Assign(node.Status.Allocatable) // ensure map not nil
	// Use instance type resource value if resource isn't currently registered in .Status.Allocatable
	for resourceName, quantity := range instanceType.Capacity {
		if resources.IsZero(node.Status.Allocatable[resourceName]) {
			n.Allocatable[resourceName] = quantity
		}
	}
	return nil
}

func (c *Cluster) populateResourceRequests(ctx context.Context, node *v1.Node, n *Node) error {
	var pods v1.PodList
	if err := c.kubeClient.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return fmt.Errorf("listing pods, %w", err)
	}
	var requested []v1.ResourceList
	var limits []v1.ResourceList
	var daemonsetRequested []v1.ResourceList
	var daemonsetLimits []v1.ResourceList
	for i := range pods.Items {
		pod := &pods.Items[i]
		if podutils.IsTerminal(pod) {
			continue
		}
		requests := resources.RequestsForPods(pod)
		podLimits := resources.LimitsForPods(pod)
		podKey := client.ObjectKeyFromObject(pod)
		n.podRequests[podKey] = requests
		n.podLimits[podKey] = podLimits
		c.bindings[podKey] = n.Node.Name
		if podutils.IsOwnedByDaemonSet(pod) {
			daemonsetRequested = append(daemonsetRequested, requests)
			daemonsetLimits = append(daemonsetLimits, podLimits)
		}
		requested = append(requested, requests)
		limits = append(limits, podLimits)
		n.HostPortUsage.Add(ctx, pod)
		n.VolumeUsage.Add(ctx, pod)
	}

	n.DaemonSetRequested = resources.Merge(daemonsetRequested...)
	n.DaemonSetLimits = resources.Merge(daemonsetLimits...)
	n.PodTotalRequests = resources.Merge(requested...)
	n.PodTotalLimits = resources.Merge(limits...)
	n.Available = resources.Subtract(n.Allocatable, resources.Merge(requested...))
	return nil
}

func (c *Cluster) populateVolumeLimits(ctx context.Context, node *v1.Node, n *Node) error {
	var csiNode storagev1.CSINode
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, &csiNode); err != nil {
		return client.IgnoreNotFound(fmt.Errorf("getting CSINode to determine volume limit for %s, %w", node.Name, err))
	}

	for _, driver := range csiNode.Spec.Drivers {
		if driver.Allocatable == nil {
			continue
		}
		n.VolumeLimits[driver.Name] = int(ptr.Int32Value(driver.Allocatable.Count))
	}
	return nil
}

func (c *Cluster) DeleteMachine(machineName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if id, ok := c.inflightNodeNameToProviderID[machineName]; ok {
		c.nominatedProviderIDs.Delete(id)
		c.inflightProviderIDs.Delete(id)
		c.initializedProviderIDs.Delete(id)
		c.markedForDeletionProviderIDs.Delete(id)
	}
	delete(c.inflightNodes, machineName)
	delete(c.inflightNodeNameToProviderID, machineName)
	c.recordConsolidationChange()
}

func (c *Cluster) DeleteNode(nodeName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if id, ok := c.nodeNameToProviderID[nodeName]; ok {
		if !c.inflightProviderIDs.Has(id) {
			c.markedForDeletionProviderIDs.Delete(id)
			c.nominatedProviderIDs.Delete(id)
		}
	}
	delete(c.nodes, nodeName)
	delete(c.nodeNameToProviderID, nodeName)
	c.recordConsolidationChange()
}

func (c *Cluster) UpdateMachine(machine *v1alpha1.Machine) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update the initialized providerIDs and alert consolidation of the change
	if machine.StatusConditions().GetCondition(v1alpha1.MachineInitialized) != nil &&
		machine.StatusConditions().GetCondition(v1alpha1.MachineInitialized).Status == v1.ConditionTrue {
		if !c.initializedProviderIDs.Has(machine.Status.ProviderID) {
			c.recordConsolidationChange()
		}
		c.initializedProviderIDs.Insert(machine.Status.ProviderID)
	} else {
		if c.initializedProviderIDs.Has(machine.Status.ProviderID) {
			c.recordConsolidationChange()
		}
		c.initializedProviderIDs.Delete(machine.Status.ProviderID)
	}

	// If the machine is in a deleting state, we should take note of this and mark for deletion
	if !machine.DeletionTimestamp.IsZero() {
		c.markedForDeletionProviderIDs.Insert(machine.Status.ProviderID)
		c.recordConsolidationChange()
	}
	n := c.newInflightNode(machine)
	c.inflightNodes[machine.Name] = n
	c.inflightNodeNameToProviderID[machine.Name] = machine.Status.ProviderID
	c.inflightProviderIDs.Insert(machine.Status.ProviderID)
}

// UpdateNode is called for every node reconciliation
func (c *Cluster) UpdateNode(ctx context.Context, node *v1.Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// We need to mock the provider ID for a node if it isn't set
	if node.Spec.ProviderID == "" {
		node.Spec.ProviderID = node.Name
	}
	n, err := c.newNode(ctx, node)
	if err != nil {
		return err
	}

	if oldNode, ok := c.nodes[node.Name]; ok {
		if oldNode.Node.Labels[v1alpha5.LabelNodeInitialized] != n.Node.Labels[v1alpha5.LabelNodeInitialized] {
			c.recordConsolidationChange()
		}
	}
	if !c.inflightProviderIDs.Has(n.Node.Spec.ProviderID) {
		// If this isn't a node with an inflight node, marked for deletion is handled by the node
		if !node.DeletionTimestamp.IsZero() {
			c.markedForDeletionProviderIDs.Insert(n.Node.Spec.ProviderID)
			c.recordConsolidationChange()
		}
		// If the providerID changed, we should cleanup the old provider info
		if c.nodeNameToProviderID[node.Name] != n.Node.Spec.ProviderID {
			id := c.nodeNameToProviderID[node.Name]
			c.markedForDeletionProviderIDs.Delete(id)
			c.nominatedProviderIDs.Delete(id)
		}
	}
	c.nodes[node.Name] = n
	c.nodeNameToProviderID[node.Name] = n.Node.Spec.ProviderID
	return nil
}

// ClusterConsolidationState returns a number representing the state of the cluster with respect to consolidation.  If
// consolidation can't occur and this number hasn't changed, there is no point in re-attempting consolidation. This
// allows reducing overall CPU utilization by pausing consolidation when the cluster is in a static state.
func (c *Cluster) ClusterConsolidationState() int64 {
	cs := atomic.LoadInt64(&c.consolidationState)
	// If 5 minutes elapsed since the last time the consolidation state was changed, we change the state anyway. This
	// ensures that at least once every 5 minutes we consider consolidating our cluster in case something else has
	// changed (e.g. instance type availability) that we can't detect which would allow consolidation to occur.
	if c.clock.Now().After(time.UnixMilli(cs).Add(5 * time.Minute)) {
		c.recordConsolidationChange()
		return atomic.LoadInt64(&c.consolidationState)
	}
	return cs
}

// DeletePod is called when the pod has been deleted
func (c *Cluster) DeletePod(podKey types.NamespacedName) {
	c.antiAffinityPods.Delete(podKey)
	c.updateNodeUsageFromPodCompletion(podKey)
	c.recordConsolidationChange()
}

func (c *Cluster) updateNodeUsageFromPodCompletion(podKey types.NamespacedName) {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeName, bindingKnown := c.bindings[podKey]
	if !bindingKnown {
		// we didn't think the pod was bound, so we weren't tracking it and don't need to do anything
		return
	}

	delete(c.bindings, podKey)
	n, ok := c.nodes[nodeName]
	if !ok {
		// we weren't tracking the node yet, so nothing to do
		return
	}
	// pod has been deleted so our available capacity increases by the resources that had been
	// requested by the pod
	n.Available = resources.Merge(n.Available, n.podRequests[podKey])
	n.PodTotalRequests = resources.Subtract(n.PodTotalRequests, n.podRequests[podKey])
	n.PodTotalLimits = resources.Subtract(n.PodTotalLimits, n.podLimits[podKey])
	delete(n.podRequests, podKey)
	delete(n.podLimits, podKey)
	n.HostPortUsage.DeletePod(podKey)
	n.VolumeUsage.DeletePod(podKey)

	// We can't easily track the changes to the DaemonsetRequested here as we no longer have the pod.  We could keep up
	// with this separately, but if a daemonset pod is being deleted, it usually means the node is going down.  In the
	// worst case we will resync to correct this.
}

// UpdatePod is called every time the pod is reconciled
func (c *Cluster) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	var err error
	if podutils.IsTerminal(pod) {
		c.updateNodeUsageFromPodCompletion(client.ObjectKeyFromObject(pod))
	} else {
		err = c.updateNodeUsageFromPod(ctx, pod)
	}
	c.updatePodAntiAffinities(pod)
	return err
}

func (c *Cluster) updatePodAntiAffinities(pod *v1.Pod) {
	// We intentionally don't track inverse anti-affinity preferences. We're not
	// required to enforce them so it just adds complexity for very little
	// value. The problem with them comes from the relaxation process, the pod
	// we are relaxing is not the pod with the anti-affinity term.
	if podKey := client.ObjectKeyFromObject(pod); podutils.HasRequiredPodAntiAffinity(pod) {
		c.antiAffinityPods.Store(podKey, pod)
	} else {
		c.antiAffinityPods.Delete(podKey)
	}
}

// updateNodeUsageFromPod is called every time a reconcile event occurs for the pod. If the pods binding has changed
// (unbound to bound), we need to update the resource requests on the node.
func (c *Cluster) updateNodeUsageFromPod(ctx context.Context, pod *v1.Pod) error {
	// nothing to do if the pod isn't bound, checking early allows avoiding unnecessary locking
	if pod.Spec.NodeName == "" {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	podKey := client.ObjectKeyFromObject(pod)
	oldNodeName, bindingKnown := c.bindings[podKey]
	if bindingKnown {
		if oldNodeName == pod.Spec.NodeName {
			// we are already tracking the pod binding, so nothing to update
			return nil
		}
		// the pod has switched nodes, this can occur if a pod name was re-used and it was deleted/re-created rapidly,
		// binding to a different node the second time
		n, ok := c.nodes[oldNodeName]
		if ok {
			// we were tracking the old node, so we need to reduce its capacity by the amount of the pod that has
			// left it
			delete(c.bindings, podKey)
			n.Available = resources.Merge(n.Available, n.podRequests[podKey])
			n.PodTotalRequests = resources.Subtract(n.PodTotalRequests, n.podRequests[podKey])
			n.PodTotalLimits = resources.Subtract(n.PodTotalLimits, n.podLimits[podKey])
			n.HostPortUsage.DeletePod(podKey)
			delete(n.podRequests, podKey)
			delete(n.podLimits, podKey)
		}
	} else {
		// new pod binding has occurred
		c.recordConsolidationChange()
	}

	// did we notice that the pod is bound to a node and didn't know about the node before?
	n, ok := c.nodes[pod.Spec.NodeName]
	if !ok {
		var node v1.Node
		if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, &node); err != nil {
			return client.IgnoreNotFound(fmt.Errorf("getting node, %w", err))
		}

		var err error
		// node didn't exist, but creating it will pick up this newly bound pod as well
		n, err = c.newNode(ctx, &node)
		if err != nil {
			// no need to delete c.nodes[node.Name] as it wasn't stored previously
			return err
		}
		c.nodes[node.Name] = n
		return nil
	}

	// sum the newly bound pod's requests and limits into the existing node and record the binding
	podRequests := resources.RequestsForPods(pod)
	podLimits := resources.LimitsForPods(pod)
	// our available capacity goes down by the amount that the pod had requested
	n.Available = resources.Subtract(n.Available, podRequests)
	n.PodTotalRequests = resources.Merge(n.PodTotalRequests, podRequests)
	n.PodTotalLimits = resources.Merge(n.PodTotalLimits, podLimits)
	// if it's a daemonset, we track what it has requested separately
	if podutils.IsOwnedByDaemonSet(pod) {
		n.DaemonSetRequested = resources.Merge(n.DaemonSetRequested, podRequests)
		n.DaemonSetLimits = resources.Merge(n.DaemonSetRequested, podLimits)
	}
	n.HostPortUsage.Add(ctx, pod)
	n.VolumeUsage.Add(ctx, pod)
	n.podRequests[podKey] = podRequests
	n.podLimits[podKey] = podLimits
	c.bindings[podKey] = n.Node.Name
	return nil
}

func (c *Cluster) recordConsolidationChange() {
	atomic.StoreInt64(&c.consolidationState, c.clock.Now().UnixMilli())
}

// Reset the cluster state for unit testing
func (c *Cluster) Reset(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes = map[string]*Node{}
	c.bindings = map[types.NamespacedName]string{}
	c.antiAffinityPods = sync.Map{}
}