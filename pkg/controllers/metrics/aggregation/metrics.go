package aggregation

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	karpentermetrics "sigs.k8s.io/karpenter/pkg/metrics"
)

var (
	nodeInitializationThresholdSeconds      = 300
	podUnboundThresholdSeconds              = 300
	podReadyThresholdSeconds                = 600
	podSchedulingDurationThresholdSeconds   = 300
	clusterNodeInitializedExceededThreshold = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: karpentermetrics.Namespace,
			Subsystem: "cluster",
			Name:      "nodes_initialized_exceeded_threshold",
			Help:      "health status of nodes in target cluster",
		},
		[]string{},
	)
	clusterPodUnboundExceededThreshold = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: karpentermetrics.Namespace,
			Subsystem: "cluster",
			Name:      "pods_unbound_exceeded_threshold",
			Help:      "unbound pod threshold is exceeded on a single pod in target cluster",
		},
		[]string{},
	)
	clusterPodReadyExceededThreshold = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: karpentermetrics.Namespace,
			Subsystem: "cluster",
			Name:      "pods_ready_exceeded_threshold",
			Help:      "ready pod threshold is exceeded on a single pod in target cluster",
		},
		[]string{},
	)
	clusterPodSchedulingDurationExceededThreshold = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: karpentermetrics.Namespace,
			Subsystem: "cluster",
			Name:      "pods_scheduling_duration_exceeded_threshold",
			Help:      "scheduling duration pod threshold is exceeded on a single pod in target cluster",
		},
		[]string{},
	)
	clusterNodeCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: karpentermetrics.Namespace,
			Subsystem: "cluster",
			Name:      "node_count",
			Help:      "number of nodes in the cluster",
		},
		[]string{"capacity_type"},
	)
)
