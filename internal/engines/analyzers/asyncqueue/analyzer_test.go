package asyncqueue

import (
	"context"
	"math"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/config"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/domain"
)

const (
	tauUp   = 0.85
	tauDown = 0.70
)

// makeConfig builds a SaturationScalingConfig carrying a single async_queue
// analyzer entry with the given parameters and the default V2 thresholds.
func makeConfig(params map[string]any) *config.SaturationScalingConfig {
	return &config.SaturationScalingConfig{
		ScaleUpThreshold:  tauUp,
		ScaleDownBoundary: tauDown,
		Analyzers: []config.AnalyzerScoreConfig{
			{Type: AnalyzerName, Score: 1.0, Parameters: params},
		},
	}
}

// makeInput builds an AnalyzerInput for a single non-disaggregated variant.
func makeInput(cfg *config.SaturationScalingConfig, backlog float64, current, pending int) domain.AnalyzerInput {
	return domain.AnalyzerInput{
		ModelID:      "m",
		Namespace:    "ns",
		Config:       cfg,
		AsyncBacklog: backlog,
		VariantStates: []domain.VariantReplicaState{
			{VariantName: "v1", CurrentReplicas: current, PendingReplicas: pending, Role: domain.RoleBoth},
		},
	}
}

// engineRC mirrors the engine's universal-threshold post-step for RequiredCapacity.
func engineRC(r *domain.AnalyzerResult) float64 {
	return math.Max(0, r.TotalDemand/tauUp-r.TotalAnticipatedSupply)
}

// engineSC mirrors the engine's universal-threshold post-step for SpareCapacity.
func engineSC(r *domain.AnalyzerResult) float64 {
	return math.Max(0, r.TotalSupply-r.TotalDemand/tauDown)
}

var _ = Describe("AsyncQueueAnalyzer.Analyze", func() {
	var (
		analyzer *AsyncQueueAnalyzer
		ctx      context.Context
	)

	BeforeEach(func() {
		analyzer = NewAsyncQueueAnalyzer()
		ctx = context.Background()
	})

	It("reports its name", func() {
		Expect(analyzer.Name()).To(Equal(AnalyzerName))
	})

	It("scales 0→1 when cold (backlog > 0, no replicas)", func() {
		cfg := makeConfig(nil)
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 0, 0))
		Expect(err).NotTo(HaveOccurred())

		Expect(res.TotalDemand).To(BeNumerically("~", defaultPerReplicaCapacity*tauUp, 1e-9))
		Expect(res.TotalSupply).To(Equal(0.0))
		Expect(res.TotalAnticipatedSupply).To(Equal(0.0))
		// Engine post-step yields exactly one pod of Required Capacity.
		Expect(engineRC(res)).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		Expect(engineSC(res)).To(Equal(0.0))
	})

	It("holds at one replica when warm (backlog > 0, one ready replica)", func() {
		cfg := makeConfig(nil)
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 1, 0))
		Expect(err).NotTo(HaveOccurred())

		Expect(res.TotalSupply).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		Expect(res.TotalAnticipatedSupply).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		// Anticipated supply cancels demand → no scale-up; deadband → no scale-down.
		Expect(engineRC(res)).To(BeNumerically("~", 0.0, 1e-9))
		Expect(engineSC(res)).To(BeNumerically("~", 0.0, 1e-9))
	})

	It("does not scale up again while a replica is pending", func() {
		cfg := makeConfig(nil)
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 1, 1))
		Expect(err).NotTo(HaveOccurred())

		// One pending, zero ready: anticipated supply still counts the pending pod.
		Expect(res.TotalSupply).To(Equal(0.0))
		Expect(res.TotalAnticipatedSupply).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		Expect(engineRC(res)).To(BeNumerically("~", 0.0, 1e-9))
	})

	It("produces spare capacity when drained past the cooldown", func() {
		cfg := makeConfig(map[string]any{"minScaleDownAge": "0s"})
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 0, 1, 0))
		Expect(err).NotTo(HaveOccurred())

		Expect(res.TotalDemand).To(Equal(0.0))
		Expect(res.TotalSupply).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		Expect(engineSC(res)).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
		Expect(engineRC(res)).To(Equal(0.0))
	})

	It("holds warm during the cooldown window after the backlog empties", func() {
		base := time.Unix(1_700_000_000, 0)
		analyzer.now = func() time.Time { return base }
		cfg := makeConfig(map[string]any{"minScaleDownAge": "1h"})

		// Backlog present: records the cooldown start.
		_, err := analyzer.Analyze(ctx, makeInput(cfg, 50, 1, 0))
		Expect(err).NotTo(HaveOccurred())

		// 30 minutes later, backlog empty but within the 1h cooldown → still warm.
		analyzer.now = func() time.Time { return base.Add(30 * time.Minute) }
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 0, 1, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.TotalDemand).To(BeNumerically("~", defaultPerReplicaCapacity*tauUp, 1e-9))
		Expect(engineSC(res)).To(Equal(0.0))

		// 90 minutes later, cooldown lapsed → drain allowed.
		analyzer.now = func() time.Time { return base.Add(90 * time.Minute) }
		res, err = analyzer.Analyze(ctx, makeInput(cfg, 0, 1, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.TotalDemand).To(Equal(0.0))
		Expect(engineSC(res)).To(BeNumerically("~", defaultPerReplicaCapacity, 1e-9))
	})

	It("treats a backlog below minQueueThreshold as empty", func() {
		cfg := makeConfig(map[string]any{"minQueueThreshold": 10, "minScaleDownAge": "0s"})
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 3, 0, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.TotalDemand).To(Equal(0.0))

		// At the threshold it counts as pending.
		res, err = analyzer.Analyze(ctx, makeInput(cfg, 10, 0, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.TotalDemand).To(BeNumerically("~", defaultPerReplicaCapacity*tauUp, 1e-9))
	})

	It("advertises per-replica capacity on every variant for cost-based selection", func() {
		cfg := makeConfig(nil)
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 0, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.VariantCapacities).To(HaveLen(1))
		Expect(res.VariantCapacities[0].PerReplicaCapacity).To(BeNumerically(">", 0))
		Expect(res.VariantCapacities[0].VariantName).To(Equal("v1"))
	})

	It("leaves RequiredCapacity and SpareCapacity zero for the engine post-step", func() {
		cfg := makeConfig(nil)
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 0, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.RequiredCapacity).To(Equal(0.0))
		Expect(res.SpareCapacity).To(Equal(0.0))
	})

	It("returns an error when the config is not a SaturationScalingConfig", func() {
		input := domain.AnalyzerInput{ModelID: "m", Namespace: "ns", Config: nil}
		_, err := analyzer.Analyze(ctx, input)
		Expect(err).To(HaveOccurred())
	})

	It("returns an error when a parameter is malformed", func() {
		cfg := makeConfig(map[string]any{"defaultPerReplicaRPS": "nope"})
		_, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 0, 0))
		Expect(err).To(HaveOccurred())
	})

	It("respects a custom per-replica capacity in the emitted demand", func() {
		cfg := makeConfig(map[string]any{"defaultPerReplicaRPS": 40.0})
		res, err := analyzer.Analyze(ctx, makeInput(cfg, 100, 0, 0))
		Expect(err).NotTo(HaveOccurred())
		Expect(res.TotalDemand).To(BeNumerically("~", 40.0*tauUp, 1e-9))
		Expect(engineRC(res)).To(BeNumerically("~", 40.0, 1e-9))
	})
})
