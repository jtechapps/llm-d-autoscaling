package registration

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/collector/source"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/collector/source/prometheus"
)

var _ = Describe("RegisterAsyncQueueAnalyzerQueries", func() {
	var (
		ctx      context.Context
		registry *source.SourceRegistry
		mockAPI  *mockPrometheusAPI
	)

	BeforeEach(func() {
		ctx = context.Background()
		registry = source.NewSourceRegistry()
		mockAPI = &mockPrometheusAPI{}
	})

	Context("when prometheus source is registered", func() {
		var queryList *source.QueryList

		BeforeEach(func() {
			metricsSource := prometheus.NewPrometheusSource(ctx, mockAPI, prometheus.DefaultPrometheusSourceConfig())
			Expect(registry.Register("prometheus", metricsSource)).To(Succeed())
		})

		It("registers the default backlog query when no template is supplied", func() {
			RegisterAsyncQueueAnalyzerQueries(registry, "")
			queryList = registry.Get("prometheus").QueryList()

			q := queryList.Get(QueryAsyncBacklog)
			Expect(q).NotTo(BeNil())
			Expect(q.Type).To(Equal(source.QueryTypePromQL))

			rendered, err := queryList.Build(QueryAsyncBacklog, map[string]string{
				source.ParamNamespace: "test-ns",
				source.ParamModelID:   "test-model",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rendered).To(ContainSubstring(DefaultAsyncBacklogMetric))
			Expect(rendered).To(ContainSubstring(`namespace="test-ns"`))
			Expect(rendered).To(ContainSubstring(`model_name="test-model"`))
		})

		It("registers an operator-supplied template verbatim", func() {
			custom := `sum(my_async_backlog{queue_name="q",namespace="{{.namespace}}"})`
			RegisterAsyncQueueAnalyzerQueries(registry, custom)
			queryList = registry.Get("prometheus").QueryList()

			rendered, err := queryList.Build(QueryAsyncBacklog, map[string]string{
				source.ParamNamespace: "test-ns",
				source.ParamModelID:   "test-model",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(rendered).To(ContainSubstring(`my_async_backlog`))
			Expect(rendered).To(ContainSubstring(`namespace="test-ns"`))
			Expect(rendered).NotTo(ContainSubstring(DefaultAsyncBacklogMetric))
		})

		It("panics if called twice on the same registry", func() {
			RegisterAsyncQueueAnalyzerQueries(registry, "")
			Expect(func() {
				RegisterAsyncQueueAnalyzerQueries(registry, "")
			}).To(Panic())
		})
	})

	Context("when prometheus source is not registered", func() {
		It("does not panic", func() {
			Expect(func() {
				RegisterAsyncQueueAnalyzerQueries(registry, "")
			}).NotTo(Panic())
		})
	})
})
