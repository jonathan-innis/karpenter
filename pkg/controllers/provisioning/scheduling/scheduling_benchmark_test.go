//go:build test_performance

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

package scheduling_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime/pprof"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

const MinPodsPerSec = 100.0
const PrintStats = false

//nolint:gosec
var r = rand.New(rand.NewSource(42))

// To run the benchmarks use:
// `go test -tags=test_performance -run=XXX -bench=.`
//
// to get something statistically significant for comparison we need to run them several times and then
// compare the results between the old performance and the new performance.
// ```sh
//
//	go test -tags=test_performance -run=XXX -bench=. -count=10 | tee /tmp/old
//	# make your changes to the code
//	go test -tags=test_performance -run=XXX -bench=. -count=10 | tee /tmp/new
//	benchstat /tmp/old /tmp/new
//
// ```
func BenchmarkScheduling1(b *testing.B) {
	benchmarkScheduler(b, 400, 1)
}
func BenchmarkScheduling50(b *testing.B) {
	benchmarkScheduler(b, 400, 50)
}
func BenchmarkScheduling100(b *testing.B) {
	benchmarkScheduler(b, 400, 100)
}
func BenchmarkScheduling500(b *testing.B) {
	benchmarkScheduler(b, 400, 500)
}
func BenchmarkScheduling1000(b *testing.B) {
	benchmarkScheduler(b, 400, 1000)
}
func BenchmarkScheduling2000(b *testing.B) {
	benchmarkScheduler(b, 400, 2000)
}
func BenchmarkScheduling5000(b *testing.B) {
	benchmarkScheduler(b, 400, 5000)
}
func BenchmarkScheduling10000(b *testing.B) {
	benchmarkScheduler(b, 400, 10000)
}
func BenchmarkScheduling20000(b *testing.B) {
	benchmarkScheduler(b, 400, 20000)
}

// TestSchedulingProfile is used to gather profiling metrics, benchmarking is primarily done with standard
// Go benchmark functions
// go test -tags=test_performance -run=SchedulingProfile
func TestSchedulingProfile(t *testing.T) {
	tw := tabwriter.NewWriter(os.Stdout, 8, 8, 2, ' ', 0)

	cpuf, err := os.Create("schedule.cpuprofile")
	if err != nil {
		t.Fatalf("error creating CPU profile: %s", err)
	}
	lo.Must0(pprof.StartCPUProfile(cpuf))
	defer pprof.StopCPUProfile()

	heapf, err := os.Create("schedule.heapprofile")
	if err != nil {
		t.Fatalf("error creating heap profile: %s", err)
	}
	defer func() { lo.Must0(pprof.WriteHeapProfile(heapf)) }()

	totalPods := 0
	totalNodes := 0
	var totalTime time.Duration
	for _, instanceCount := range []int{400} {
		for _, podCount := range []int{1, 50, 100, 500, 1000, 1500, 2000, 5000, 10000, 20000} {
			start := time.Now()
			res := testing.Benchmark(func(b *testing.B) { benchmarkScheduler(b, instanceCount, podCount) })
			totalTime += time.Since(start) / time.Duration(res.N)
			nodeCount := res.Extra["nodes"]
			fmt.Fprintf(tw, "%d instances %d pods\t%d nodes\t%s per scheduling\t%s per pod\n", instanceCount, podCount, int(nodeCount), time.Duration(res.NsPerOp()), time.Duration(res.NsPerOp()/int64(podCount)))
			totalPods += podCount
			totalNodes += int(nodeCount)
		}
	}
	fmt.Println("scheduled", totalPods, "against", totalNodes, "nodes in total in", totalTime, float64(totalPods)/totalTime.Seconds(), "pods/sec")
	tw.Flush()
}

// nolint:gocyclo
func benchmarkScheduler(b *testing.B, instanceCount, podCount int) {
	// create context from background with test options
	// test options disable logging by default
	ctx = options.ToContext(context.Background(), test.Options())
	nodePool := test.NodePool(v1.NodePool{
		Spec: v1.NodePoolSpec{
			Limits: v1.Limits{
				corev1.ResourceCPU:    resource.MustParse("10000000"),
				corev1.ResourceMemory: resource.MustParse("10000000Gi"),
			},
		},
	})

	// Apply limits to both of the NodePools
	instanceTypes := fake.InstanceTypes(instanceCount)
	cloudProvider = fake.NewCloudProvider()
	cloudProvider.InstanceTypes = instanceTypes

	client := fakecr.NewFakeClient()
	pods := makeDiversePods(podCount)
	clock := &clock.RealClock{}
	cluster = state.NewCluster(clock, client, cloudProvider)
	topology, err := scheduling.NewTopology(ctx, client, cluster, nil, []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{
		nodePool.Name: instanceTypes,
	}, pods)
	if err != nil {
		b.Fatalf("creating topology, %s", err)
	}

	scheduler := scheduling.NewScheduler(
		ctx,
		client,
		[]*v1.NodePool{nodePool},
		cluster,
		nil,
		topology,
		map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes},
		nil,
		events.NewRecorder(&record.FakeRecorder{}),
		clock,
		scheduling.DisableReservedCapacityFallback,
	)

	b.ResetTimer()
	// Pack benchmark
	start := time.Now()
	podsScheduledInRound1 := 0
	nodesInRound1 := 0
	for i := 0; i < b.N; i++ {
		results, err := scheduler.Solve(ctx, pods)
		if err != nil {
			b.Fatalf("expected scheduler to schedule all pods without error, got %s", err)
		}
		if len(results.PodErrors) > 0 {
			b.Fatalf("expected all pods to schedule, got %d pods that didn't", len(results.PodErrors))
		}
		if i == 0 {
			minPods := math.MaxInt64
			maxPods := 0
			var podCounts []int
			for _, n := range results.NewNodeClaims {
				podCounts = append(podCounts, len(n.Pods))
				podsScheduledInRound1 += len(n.Pods)
				nodesInRound1 = len(results.NewNodeClaims)
				if len(n.Pods) > maxPods {
					maxPods = len(n.Pods)
				}
				if len(n.Pods) < minPods {
					minPods = len(n.Pods)
				}
			}
			if PrintStats {
				meanPodsPerNode := float64(podsScheduledInRound1) / float64(nodesInRound1)
				variance := 0.0
				for _, pc := range podCounts {
					variance += math.Pow(float64(pc)-meanPodsPerNode, 2.0)
				}
				variance /= float64(nodesInRound1)
				stddev := math.Sqrt(variance)
				fmt.Printf("%d instance types %d pods resulted in %d nodes with pods per node min=%d max=%d mean=%f stddev=%f\n",
					instanceCount, podCount, nodesInRound1, minPods, maxPods, meanPodsPerNode, stddev)
			}
		}
	}
	duration := time.Since(start)
	podsPerSec := float64(len(pods)) / (duration.Seconds() / float64(b.N))
	b.ReportMetric(podsPerSec, "pods/sec")
	b.ReportMetric(float64(podsScheduledInRound1), "pods")
	b.ReportMetric(float64(nodesInRound1), "nodes")

	// we don't care if it takes a bit of time to schedule a few pods as there is some setup time required for sorting
	// instance types, computing topologies, etc.  We want to ensure that the larger batches of pods don't become too
	// slow.
	if len(pods) > 100 {
		if podsPerSec < MinPodsPerSec {
			b.Fatalf("scheduled %f pods/sec, expected at least %f", podsPerSec, MinPodsPerSec)
		}
	}
}

func makeDiversePods(count int) []*corev1.Pod {
	var pods []*corev1.Pod
	numTypes := 5
	pods = append(pods, makeGenericPods(count/numTypes)...)
	pods = append(pods, makeTopologySpreadPods(count/numTypes, corev1.LabelTopologyZone)...)
	pods = append(pods, makeTopologySpreadPods(count/numTypes, corev1.LabelHostname)...)
	pods = append(pods, makePodAffinityPods(count/numTypes, corev1.LabelTopologyZone)...)
	pods = append(pods, makePodAntiAffinityPods(count/numTypes, corev1.LabelHostname)...)

	// fill out due to count being not evenly divisible with generic pods
	nRemaining := count - len(pods)
	pods = append(pods, makeGenericPods(nRemaining)...)
	return pods
}

func makePodAntiAffinityPods(count int, key string) []*corev1.Pod {
	var pods []*corev1.Pod
	// all of these pods have anti-affinity to each other
	labels := map[string]string{
		"app": "nginx",
	}
	for i := 0; i < count; i++ {
		pods = append(pods, test.Pod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					UID:    uuid.NewUUID(),
				},
				PodAntiRequirements: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
						TopologyKey:   key,
					},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    randomCPU(),
						corev1.ResourceMemory: randomMemory(),
					},
				}}))
	}
	return pods
}
func makePodAffinityPods(count int, key string) []*corev1.Pod {
	var pods []*corev1.Pod
	for i := 0; i < count; i++ {
		// We use self-affinity here because using affinity that relies on other pod
		// domains doens't guarantee that all pods can schedule. In the case where you are not
		// using self-affinity and the domain doesn't exist, scheduling will fail for all pods with
		// affinities against this domain
		labels := randomAffinityLabels()
		pods = append(pods, test.Pod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					UID:    uuid.NewUUID(),
				},
				PodRequirements: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{MatchLabels: labels},
						TopologyKey:   key,
					},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    randomCPU(),
						corev1.ResourceMemory: randomMemory(),
					},
				}}))
	}
	return pods
}

func makeTopologySpreadPods(count int, key string) []*corev1.Pod {
	var pods []*corev1.Pod
	for i := 0; i < count; i++ {
		pods = append(pods, test.Pod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: randomLabels(),
					UID:    uuid.NewUUID(),
				},
				TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
					{
						MaxSkew:           1,
						TopologyKey:       key,
						WhenUnsatisfiable: corev1.DoNotSchedule,
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: randomLabels(),
						},
					},
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    randomCPU(),
						corev1.ResourceMemory: randomMemory(),
					},
				}}))
	}
	return pods
}

func makeGenericPods(count int) []*corev1.Pod {
	var pods []*corev1.Pod
	for i := 0; i < count; i++ {
		pods = append(pods, test.Pod(
			test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{
					Labels: randomLabels(),
					UID:    uuid.NewUUID(),
				},
				ResourceRequirements: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    randomCPU(),
						corev1.ResourceMemory: randomMemory(),
					},
				}}))
	}
	return pods
}

func randomAffinityLabels() map[string]string {
	return map[string]string{
		"my-affininity": randomLabelValue(),
	}
}

func randomLabels() map[string]string {
	return map[string]string{
		"my-label": randomLabelValue(),
	}
}

func randomLabelValue() string {
	labelValues := []string{"a", "b", "c", "d", "e", "f", "g"}
	return labelValues[r.Intn(len(labelValues))]
}

func randomMemory() resource.Quantity {
	mem := []int{100, 256, 512, 1024, 2048, 4096}
	return resource.MustParse(fmt.Sprintf("%dMi", mem[r.Intn(len(mem))]))
}

func randomCPU() resource.Quantity {
	cpu := []int{100, 250, 500, 1000, 1500}
	return resource.MustParse(fmt.Sprintf("%dm", cpu[r.Intn(len(cpu))]))
}
