// Package asyncqueue implements the Milestone 1 async/batch autoscaling
// analyzer. It turns the async broker backlog (emitted by llm-d-async and
// scraped into AnalyzerInput.AsyncBacklog) into a binary 0↔1 demand signal that
// the shared V2 optimization engine actuates via HPA/KEDA. It is a
// demand-producing analyzer only; it does not actuate.
//
// See docs/design/async-queue-autoscaling-roadmap.md for the full design.
package asyncqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/config"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/domain"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/engines/aggregation"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/logging"
)

// AsyncQueueAnalyzer is the Milestone 1 async/batch analyzer. It implements
// domain.Analyzer.
//
// Milestone 1 behavior — binary (0↔1), backlog-driven:
//   - Cold  (backlog > 0, no active/pending replicas): emit demand for one pod,
//     so the engine's RC post-step yields exactly one replica of Required
//     Capacity (0→1).
//   - Warm  (backlog > 0, a replica is active or pending): hold at one replica —
//     anticipated supply cancels the demand so RC = 0, and the τ_up/τ_down
//     deadband keeps SC = 0. No multi-pod scale-out in M1.
//   - Drained (backlog == 0): emit zero demand once the scale-down cooldown
//     (minScaleDownAge) has elapsed since the backlog was last non-empty, so the
//     engine reports SpareCapacity and the variant drains to zero. Within the
//     cooldown the variant is held warm to absorb new arrivals.
//
// The analyzer is stateful across reconcile cycles: it tracks, per model, the
// last time the backlog was non-empty to implement the cooldown.
type AsyncQueueAnalyzer struct {
	mu sync.Mutex
	// lastNonEmpty maps a model key ("namespace\x00modelID") to the last time the
	// backlog for that model was observed non-empty.
	lastNonEmpty map[string]time.Time
	// now is the clock, injectable for deterministic tests.
	now func() time.Time
}

// NewAsyncQueueAnalyzer constructs an AsyncQueueAnalyzer with an empty cooldown
// cache and the real wall clock.
func NewAsyncQueueAnalyzer() *AsyncQueueAnalyzer {
	return &AsyncQueueAnalyzer{
		lastNonEmpty: make(map[string]time.Time),
		now:          time.Now,
	}
}

// Name returns the analyzer's identifier.
func (a *AsyncQueueAnalyzer) Name() string { return AnalyzerName }

// Analyze computes the Milestone 1 binary demand signal for a model across all
// its variants. It leaves RequiredCapacity/SpareCapacity zero; the engine's
// universal threshold post-step writes them from the model-level totals returned
// here.
func (a *AsyncQueueAnalyzer) Analyze(ctx context.Context, input domain.AnalyzerInput) (*domain.AnalyzerResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	logger := ctrl.LoggerFrom(ctx).WithName(AnalyzerName)

	satConfig, ok := input.Config.(*config.SaturationScalingConfig)
	if !ok {
		return nil, ErrConfigType{got: input.Config}
	}

	p, err := a.resolveParams(satConfig)
	if err != nil {
		return nil, err
	}

	// τ_up used by the engine's post-step for this analyzer. Emitting demand as
	// PerReplicaCapacity × τ_up makes the τ_up division in the post-step resolve
	// to exactly one pod of demand, keeping the M1 decision binary regardless of
	// the configured threshold.
	scaleUp := resolveScaleUp(satConfig)

	// Build per-variant capacities. Every variant advertises the configured
	// per-replica capacity — including zero-replica variants — so the optimizer
	// can select one to wake from zero (0→1). Accelerator/cost identity is
	// supplied by the saturation carrier during the optimizer's merge.
	vcs := make([]domain.VariantCapacity, 0, len(input.VariantStates))
	for _, vs := range input.VariantStates {
		vcs = append(vcs, domain.VariantCapacity{
			VariantName:        vs.VariantName,
			Role:               vs.Role,
			ReplicaCount:       vs.CurrentReplicas - vs.PendingReplicas,
			PendingReplicas:    vs.PendingReplicas,
			PerReplicaCapacity: p.perReplicaCapacity,
			TotalCapacity:      float64(vs.CurrentReplicas-vs.PendingReplicas) * p.perReplicaCapacity,
			Reason:             "M1-async",
		})
	}

	modelKey := input.Namespace + "\x00" + input.ModelID
	demand := a.demand(modelKey, input.AsyncBacklog, p, scaleUp)

	totalSupply := aggregation.SumTotalSupply(vcs)
	result := &domain.AnalyzerResult{
		AnalyzerName:           AnalyzerName,
		ModelID:                input.ModelID,
		Namespace:              input.Namespace,
		AnalyzedAt:             a.now(),
		VariantCapacities:      vcs,
		TotalSupply:            totalSupply,
		TotalAnticipatedSupply: aggregation.SumTotalAnticipatedSupply(vcs),
		TotalDemand:            demand,
		Utilization:            safeDivide(demand, totalSupply),
		// RequiredCapacity / SpareCapacity intentionally left zero — the engine's
		// universal threshold post-step overwrites them.
	}

	logger.V(logging.DEBUG).Info("async_queue analysis",
		"modelID", input.ModelID,
		"namespace", input.Namespace,
		"backlog", input.AsyncBacklog,
		"demand", demand,
		"totalSupply", totalSupply,
		"perReplicaCapacity", p.perReplicaCapacity,
	)
	return result, nil
}

// demand returns the Milestone 1 demand value for a model. Demand is one pod's
// worth (perReplicaCapacity × scaleUp) whenever work is pending — either the
// backlog is at or above minQueueThreshold, or it drained within the last
// minScaleDownAge (cooldown, keeping the variant warm) — and zero once the
// cooldown lapses.
//
// It updates the per-model cooldown timestamp as a side effect and is safe for
// concurrent use.
func (a *AsyncQueueAnalyzer) demand(modelKey string, backlog float64, p params, scaleUp float64) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	pending := backlog >= p.minQueueThreshold && backlog > 0
	if pending {
		a.lastNonEmpty[modelKey] = now
	} else if last, ok := a.lastNonEmpty[modelKey]; ok && now.Sub(last) < p.minScaleDownAge {
		// Backlog empty but still within the cooldown window: hold warm.
		pending = true
	} else {
		// Cooldown lapsed (or never seen): allow drain and forget the model so the
		// cache does not grow unbounded across models that come and go.
		delete(a.lastNonEmpty, modelKey)
	}

	if !pending {
		return 0
	}
	return p.perReplicaCapacity * scaleUp
}

// resolveParams finds this analyzer's entry in the saturation config and parses
// its parameters. An absent entry (analyzer registered but not configured for
// this model) yields defaults.
func (a *AsyncQueueAnalyzer) resolveParams(cfg *config.SaturationScalingConfig) (params, error) {
	for _, aw := range cfg.Analyzers {
		if aw.EffectiveType() == AnalyzerName {
			return parseParams(aw.Parameters)
		}
	}
	return parseParams(nil)
}

// resolveScaleUp returns the τ_up the engine will apply to this analyzer's
// result, mirroring the engine's resolveThresholds: the per-analyzer override if
// set, else the global scaleUpThreshold, falling back to the package default when
// non-positive.
func resolveScaleUp(cfg *config.SaturationScalingConfig) float64 {
	up := cfg.ScaleUpThreshold
	for _, aw := range cfg.Analyzers {
		if aw.EffectiveType() == AnalyzerName {
			up = aw.EffectiveScaleUpThreshold(cfg.ScaleUpThreshold)
			break
		}
	}
	if up <= 0 {
		return defaultScaleUpThreshold
	}
	return up
}

// safeDivide returns a/b, or 0 when b is zero.
func safeDivide(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// ErrConfigType is returned when AnalyzerInput.Config is not the expected
// *config.SaturationScalingConfig.
type ErrConfigType struct{ got any }

func (e ErrConfigType) Error() string {
	return fmt.Sprintf("async_queue: expected *config.SaturationScalingConfig, got %T", e.got)
}
