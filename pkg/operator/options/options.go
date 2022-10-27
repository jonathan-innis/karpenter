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

package options

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"runtime/debug"

	"go.uber.org/multierr"
	"knative.dev/pkg/logging"

	"github.com/aws/karpenter-core/pkg/utils/env"
)

type AWSNodeNameConvention string

const (
	IPName       AWSNodeNameConvention = "ip-name"
	ResourceName AWSNodeNameConvention = "resource-name"
)

// Options for running this binary
type Options struct {
	*flag.FlagSet
	// Vendor Neutral
	ServiceName          string
	WebhookPort          int
	MetricsPort          int
	HealthProbePort      int
	KubeClientQPS        int
	KubeClientBurst      int
	EnableProfiling      bool
	EnableLeaderElection bool
	MemoryLimit          int64
	// AWS Specific
	ClusterName               string  `deprecated:"true"`
	ClusterEndpoint           string  `deprecated:"true"`
	VMMemoryOverhead          float64 `deprecated:"true"`
	AWSNodeNameConvention     string  `deprecated:"true"`
	AWSENILimitedPodDensity   bool    `deprecated:"true"`
	AWSDefaultInstanceProfile string  `deprecated:"true"`
	AWSEnablePodENI           bool    `deprecated:"true"`
	AWSIsolatedVPC            bool    `deprecated:"true"`
}

// New creates an Options struct and registers CLI flags and environment variables to fill-in the Options struct fields
func New() *Options {
	opts := &Options{}
	f := flag.NewFlagSet("karpenter", flag.ContinueOnError)
	opts.FlagSet = f

	// Vendor Neutral
	f.StringVar(&opts.ServiceName, "karpenter-service", env.WithDefaultString("KARPENTER_SERVICE", ""), "The Karpenter Service name for the dynamic webhook certificate")
	f.IntVar(&opts.WebhookPort, "webhook-port", env.WithDefaultInt("PORT", 8443), "The port the webhook endpoint binds to for validation and mutation of resources")
	f.IntVar(&opts.MetricsPort, "metrics-port", env.WithDefaultInt("METRICS_PORT", 8080), "The port the metric endpoint binds to for operating metrics about the controller itself")
	f.IntVar(&opts.HealthProbePort, "health-probe-port", env.WithDefaultInt("HEALTH_PROBE_PORT", 8081), "The port the health probe endpoint binds to for reporting controller health")
	f.IntVar(&opts.KubeClientQPS, "kube-client-qps", env.WithDefaultInt("KUBE_CLIENT_QPS", 200), "The smoothed rate of qps to kube-apiserver")
	f.IntVar(&opts.KubeClientBurst, "kube-client-burst", env.WithDefaultInt("KUBE_CLIENT_BURST", 300), "The maximum allowed burst of queries to the kube-apiserver")
	f.BoolVar(&opts.EnableProfiling, "enable-profiling", env.WithDefaultBool("ENABLE_PROFILING", false), "Enable the profiling on the metric endpoint")
	f.BoolVar(&opts.EnableLeaderElection, "leader-elect", env.WithDefaultBool("LEADER_ELECT", true), "Start leader election client and gain leadership before executing the main loop. Enable this when running replicated components for high availability.")
	f.Int64Var(&opts.MemoryLimit, "memory-limit", env.WithDefaultInt64("MEMORY_LIMIT", -1), "Memory limit on the container running the controller. The GC soft memory limit is set to 90% of this value.")
	// AWS Specific
	f.StringVar(&opts.ClusterName, "cluster-name", env.WithDefaultString("CLUSTER_NAME", ""), "The kubernetes cluster name for resource discovery.  DEPRECATED: Use 'clusterName' in 'karpenter-global-settings'")
	f.StringVar(&opts.ClusterEndpoint, "cluster-endpoint", env.WithDefaultString("CLUSTER_ENDPOINT", ""), "The external kubernetes cluster endpoint for new nodes to connect with.  DEPRECATED: Use 'clusterEndpoint' in 'karpenter-global-settings'")
	f.Float64Var(&opts.VMMemoryOverhead, "vm-memory-overhead", env.WithDefaultFloat64("VM_MEMORY_OVERHEAD", 0.075), "The VM memory overhead as a percent that will be subtracted from the total memory for all instance types.  DEPRECATED: Use 'aws.vmMemoryOverheadPercent' in 'karpenter-global-settings'")
	f.StringVar(&opts.AWSNodeNameConvention, "aws-node-name-convention", env.WithDefaultString("AWS_NODE_NAME_CONVENTION", string(IPName)), "The node naming convention used by the AWS cloud provider. DEPRECATED: Use 'aws.nodeNameConvention' in 'karpenter-global-settings'")
	f.BoolVar(&opts.AWSENILimitedPodDensity, "aws-eni-limited-pod-density", env.WithDefaultBool("AWS_ENI_LIMITED_POD_DENSITY", true), "Indicates whether new nodes should use ENI-based pod density. DEPRECATED: Use `.spec.kubeletConfiguration.maxPods` to set pod density on a per-provisioner basis")
	f.StringVar(&opts.AWSDefaultInstanceProfile, "aws-default-instance-profile", env.WithDefaultString("AWS_DEFAULT_INSTANCE_PROFILE", ""), "The default instance profile to use when provisioning nodes in AWS. DEPRECATED: Use 'aws.defaultInstanceProfile' in 'karpenter-global-settings'")
	f.BoolVar(&opts.AWSEnablePodENI, "aws-enable-pod-eni", env.WithDefaultBool("AWS_ENABLE_POD_ENI", false), "If true then instances that support pod ENI will report a vpc.amazonaws.com/pod-eni resource.  DEPRECATED: Use 'aws.enablePodENI' in 'karpenter-global-settings'")
	f.BoolVar(&opts.AWSIsolatedVPC, "aws-isolated-vpc", env.WithDefaultBool("AWS_ISOLATED_VPC", false), "If true then assume we can't reach AWS services which don't have a VPC endpoint. This also has the effect of disabling look-ups to the AWS pricing endpoint.  DEPRECATED: Use 'aws.isolatedVPC' in 'karpenter-global-settings'")

	if opts.MemoryLimit > 0 {
		newLimit := int64(float64(opts.MemoryLimit) * 0.9)
		debug.SetMemoryLimit(newLimit)
	}
	return opts
}

// MustParse reads the user passed flags, environment variables, and default values.
// Options are valided and panics if an error is returned
func (o *Options) MustParse(ctx context.Context) *Options {
	err := o.Parse(os.Args[1:])

	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		panic(err)
	}
	if err := o.Validate(); err != nil {
		panic(err)
	}
	o.logDeprecations(ctx)
	return o
}

func (o *Options) logDeprecations(ctx context.Context) {
	t := reflect.TypeOf(o).Elem()
	v := reflect.ValueOf(o).Elem()
	for i := 0; i < t.NumField(); i++ {
		if t.Field(i).Tag.Get("deprecated") != "" {
			if !v.Field(i).IsZero() {
				logging.FromContext(ctx).Warnf("%s is deprecated. See https://karpenter.sh/ for details on upgrading from deprecated parameters", t.Field(i).Name)
			}
		}
	}
}

func (o Options) Validate() (err error) {
	err = multierr.Append(err, o.validateEndpoint())
	if o.ClusterName == "" {
		err = multierr.Append(err, fmt.Errorf("CLUSTER_NAME is required"))
	}
	awsNodeNameConvention := AWSNodeNameConvention(o.AWSNodeNameConvention)
	if awsNodeNameConvention != IPName && awsNodeNameConvention != ResourceName {
		err = multierr.Append(err, fmt.Errorf("aws-node-name-convention may only be either ip-name or resource-name"))
	}
	return err
}

func (o Options) validateEndpoint() error {
	endpoint, err := url.Parse(o.ClusterEndpoint)
	// url.Parse() will accept a lot of input without error; make
	// sure it's a real URL
	if err != nil || !endpoint.IsAbs() || endpoint.Hostname() == "" {
		return fmt.Errorf("\"%s\" not a valid CLUSTER_ENDPOINT URL", o.ClusterEndpoint)
	}
	return nil
}

func (o Options) GetAWSNodeNameConvention() AWSNodeNameConvention {
	return AWSNodeNameConvention(o.AWSNodeNameConvention)
}
