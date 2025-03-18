package aggregation

import (
	"context"
	"fmt"
	"runtime"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/singleton"
	prometheusmodel "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
}

func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

func (c *Controller) Reconcile(ctx context.Context) (reconcile.Result, error) {
	AggregateGauge(ctx, clusterNodeInitializedExceededThreshold, "operator_nodeclaim_status_condition_current_status_seconds", map[string]string{
		"type":   "Initialized",
		"status": "Unknown",
	}, float64(nodeInitializationThresholdSeconds))
	AggregateGauge(ctx, clusterPodUnboundExceededThreshold, "karpenter_pods_provisioning_unbound_time_seconds", map[string]string{}, float64(podUnboundThresholdSeconds))
	AggregateGauge(ctx, clusterPodReadyExceededThreshold, "karpenter_pods_provisioning_unstarted_time_seconds", map[string]string{}, float64(podReadyThresholdSeconds))
	AggregateGauge(ctx, clusterPodSchedulingDurationExceededThreshold, "karpenter_pods_provisioning_scheduling_undecided_time_seconds", map[string]string{}, float64(podSchedulingDurationThresholdSeconds))

	var rtm runtime.MemStats
	runtime.ReadMemStats(&rtm)
	log.FromContext(ctx).WithValues("heap_alloc", fmt.Sprintf("%dMB", rtm.HeapAlloc/1024/1024), "heap_inuse", fmt.Sprintf("%dMB", rtm.HeapInuse/1024/1024)).Info("retrieved memstats info")
	return reconcile.Result{RequeueAfter: time.Second}, nil
}

func (c *Controller) Name() string {
	return "metrics.aggregation"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func AggregateGauge(ctx context.Context, metricsExceededThreshold opmetrics.GaugeMetric, metricName string, labelValues map[string]string, duration float64) {
	metric, found := findMetricWithLabelValues(ctx, metricName, labelValues)
	if !found {
		metricsExceededThreshold.Set(0, map[string]string{})
		return
	}
	if metric.GetGauge().GetValue() > duration {
		metricsExceededThreshold.Set(1, map[string]string{})
		return
	}
	metricsExceededThreshold.Set(0, map[string]string{})
	return
}

func findMetricWithLabelValues(ctx context.Context, name string, labelValues map[string]string) (*prometheusmodel.Metric, bool) {
	metrics, err := crmetrics.Registry.Gather()
	if err != nil {
		return nil, false
	}
	totalCount := 0
	for _, m := range metrics {
		totalCount += len(m.Metric)
	}
	log.FromContext(ctx).WithValues("metric_count", totalCount).Info("searching for metrics")

	mf, found := lo.Find(metrics, func(mf *prometheusmodel.MetricFamily) bool {
		return mf.GetName() == name
	})
	if !found {
		return nil, false
	}
	for _, m := range mf.Metric {
		temp := lo.Assign(labelValues)
		for _, labelPair := range m.Label {
			if v, ok := temp[labelPair.GetName()]; ok && v == labelPair.GetValue() {
				delete(temp, labelPair.GetName())
			}
		}
		if len(temp) == 0 {
			return m, true
		}
	}
	return nil, false
}
