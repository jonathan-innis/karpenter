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

package tracing

import (
	"context"
	"fmt"
	"strings"

	"github.com/awslabs/operatorpkg/object"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Controller struct {
	name       string
	gvk        schema.GroupVersionKind
	reconciler reconcile.Reconciler
}

func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	ctx = baggage.ContextWithBaggage(ctx, lo.Must(baggage.New(lo.Must(baggage.NewMember("reconcileID", string(controller.ReconcileIDFromContext(ctx)))))))
	if c.gvk.Kind != "" {
		lo.Must(baggage.FromContext(ctx).SetMember(lo.Must(baggage.NewMember(fmt.Sprintf("%s.name", strings.ToLower(c.gvk.Kind)), req.Name))))
		if req.Namespace != "" {
			lo.Must(baggage.FromContext(ctx).SetMember(lo.Must(baggage.NewMember(fmt.Sprintf("%s.namespace", strings.ToLower(c.gvk.Kind)), req.Namespace))))
		}
	}
	ctx = StartSpan(ctx, fmt.Sprintf("%s.Reconcile", c.name))
	res, err := c.reconciler.Reconcile(ctx, req)
	if res.RequeueAfter > 0 {
		SpanFromContext(ctx).SetAttributes(attribute.String("result.RequeueAfter", res.RequeueAfter.String()))
	}
	if err != nil {
		SpanFromContext(ctx).RecordError(err)
		SpanFromContext(ctx).SetStatus(codes.Error, err.Error())
	} else {
		SpanFromContext(ctx).SetStatus(codes.Ok, "completed successfully")
	}
	SpanFromContext(ctx).End()
	return res, err
}

func WithObjectTracing[T client.Object](reconciler reconcile.Reconciler, name string) reconcile.Reconciler {
	gvk := object.GVK(object.New[T]())
	return &Controller{
		name:       name,
		gvk:        gvk,
		reconciler: reconciler,
	}
}

func WithTracing(reconciler reconcile.Reconciler, name string) reconcile.Reconciler {
	return &Controller{
		name:       name,
		reconciler: reconciler,
	}
}
