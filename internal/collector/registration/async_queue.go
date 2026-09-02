// Package registration provides query registration for metrics sources.
// This file registers the query used by the async_queue analyzer (Milestone 1).
package registration

import (
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/collector/source"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/logging"
)

const (
	// QueryAsyncBacklog is the query name for the model-level async broker
	// backlog (count of pending async/batch requests). Registered by
	// RegisterAsyncQueueAnalyzerQueries and read by
	// ReplicaMetricsCollector.CollectAsyncBacklog into
	// domain.AnalyzerInput.AsyncBacklog.
	QueryAsyncBacklog = "async_backlog"

	// DefaultAsyncBacklogMetric is the backlog metric name emitted by llm-d-async.
	// The doubled "async_async_" segment is llm-d-async's existing naming
	// convention, not a typo. Referenced as a shared constant per open question §10.1
	// of docs/design/async-queue-autoscaling-roadmap.md so the name is not
	// duplicated across the codebase.
	DefaultAsyncBacklogMetric = "llm_d_async_async_broker_backlog"

	// DefaultAsyncBacklogQuery is the default PromQL used when an operator does not
	// supply their own via the async_queue analyzer's `backlogQuery` parameter. It
	// assumes the backlog metric is scoped by namespace and model_name like WVA's
	// other queries. Deployments whose async metric is instead keyed by
	// queue_name/pool_name (llm-d-async's native label set) should override
	// `backlogQuery` with a PromQL scoped to their queue/pool — the config-driven
	// path exists precisely for that case.
	DefaultAsyncBacklogQuery = `sum(` + DefaultAsyncBacklogMetric + `{namespace="{{.namespace}}",model_name="{{.modelID}}"})`
)

// RegisterAsyncQueueAnalyzerQueries registers the single async_queue backlog
// query. It must be called once at engine startup, alongside the other analyzer
// registrations, only when the async_queue analyzer is enabled.
//
// backlogQueryTemplate is the operator-supplied PromQL (the async_queue
// analyzer's `backlogQuery` parameter); when empty, DefaultAsyncBacklogQuery is
// used. The template may reference {{.namespace}} and {{.modelID}}, both of which
// the collector always supplies.
func RegisterAsyncQueueAnalyzerQueries(sourceRegistry *source.SourceRegistry, backlogQueryTemplate string) {
	metricsSource := sourceRegistry.Get("prometheus")
	if metricsSource == nil {
		ctrl.Log.V(logging.DEBUG).Info("Prometheus source not registered, skipping async_queue analyzer query registration")
		return
	}
	registry := metricsSource.QueryList()

	template := backlogQueryTemplate
	if template == "" {
		template = DefaultAsyncBacklogQuery
	}

	registry.MustRegister(source.QueryTemplate{
		Name:        QueryAsyncBacklog,
		Type:        source.QueryTypePromQL,
		Template:    template,
		Params:      []string{source.ParamNamespace, source.ParamModelID},
		Description: "Model-level async broker backlog (count of pending async/batch requests) for the async_queue analyzer",
	})
}
