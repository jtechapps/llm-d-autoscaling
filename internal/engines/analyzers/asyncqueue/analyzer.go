package asyncqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/config"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/domain"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/engines/aggregation"
)

// MetricSample represents a single metric observation with labels.
type MetricSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// AsyncQueueAnalyzer computes required capacity signals based on async request backlog.
// It implements domain.Analyzer.
type AsyncQueueAnalyzer struct {
	mu                 sync.Mutex
	lastScaleDownTimes map[string]time.Time // key: namespace/modelID -> time

	// mockMetricSamples allows unit tests and metrics providers to inject raw metric samples
	mockMetricSamples []MetricSample
}

// NewAsyncQueueAnalyzer creates a new AsyncQueueAnalyzer instance.
func NewAsyncQueueAnalyzer() *AsyncQueueAnalyzer {
	return &AsyncQueueAnalyzer{
		lastScaleDownTimes: make(map[string]time.Time),
	}
}

// Name implements domain.Analyzer.
func (a *AsyncQueueAnalyzer) Name() string {
	return AnalyzerName
}

// SetMockMetricSamples sets mock samples for unit testing.
func (a *AsyncQueueAnalyzer) SetMockMetricSamples(samples []MetricSample) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mockMetricSamples = samples
}

// SetLastScaleDownTime manually overrides the last scale-down time (useful for unit tests).
func (a *AsyncQueueAnalyzer) SetLastScaleDownTime(namespace, modelID string, t time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	modelKey := namespace + "/" + modelID
	a.lastScaleDownTimes[modelKey] = t
}

// Analyze implements domain.Analyzer.
func (a *AsyncQueueAnalyzer) Analyze(
	ctx context.Context,
	input domain.AnalyzerInput,
) (*domain.AnalyzerResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cfg := a.extractConfig(input)
	now := time.Now()
	modelKey := input.Namespace + "/" + input.ModelID

	// 1. Extract Queue Depth / Backlog
	queueDepth := a.ExtractQueueDepth(input, cfg)

	// 2. Determine current active and pending replicas
	totalActiveReplicas := 0
	totalPendingReplicas := 0
	for _, vs := range input.VariantStates {
		totalActiveReplicas += vs.CurrentReplicas
		totalPendingReplicas += vs.PendingReplicas
	}

	// 3. Track scale-down timestamp for min_scale_down_age cooldown
	a.mu.Lock()
	if totalActiveReplicas == 0 && queueDepth == 0 {
		if _, exists := a.lastScaleDownTimes[modelKey]; !exists {
			a.lastScaleDownTimes[modelKey] = now
		}
	} else if totalActiveReplicas > 0 {
		delete(a.lastScaleDownTimes, modelKey)
	}
	lastScaleDown := a.lastScaleDownTimes[modelKey]
	a.mu.Unlock()

	// 4. Calculate effective demand
	effectiveDemand := float64(0)

	// Determine scaleUpThreshold from SaturationScalingConfig
	scaleUpThreshold := 0.85
	if sc, ok := input.Config.(*config.SaturationScalingConfig); ok && sc.ScaleUpThreshold > 0 {
		scaleUpThreshold = sc.ScaleUpThreshold
	}

	// Calculate per-replica supply (used across variants)
	perReplicaSupply := cfg.DefaultPerReplicaRPS
	if perReplicaSupply <= 0 {
		perReplicaSupply = DefaultPerReplicaRPS
	}

	if cfg.Mode == ModeDrainTime {
		// "drain_time" mode: calculate demand based on queue drain time SLA
		drainSeconds := cfg.TargetDrainTime.Seconds()
		if drainSeconds <= 0 {
			drainSeconds = DefaultTargetDrainTime.Seconds()
		}

		if totalActiveReplicas == 0 && totalPendingReplicas == 0 {
			timeSinceScaleDown := now.Sub(lastScaleDown)
			if queueDepth >= cfg.HighQueueThreshold {
				// Immediate scale-up: high backlog bypasses cooldown
				effectiveDemand = float64(queueDepth) / drainSeconds
			} else if queueDepth >= cfg.MinQueueThreshold && (lastScaleDown.IsZero() || timeSinceScaleDown >= cfg.MinScaleDownAge) {
				// Cooldown expired: small backlog triggers scale-up
				effectiveDemand = float64(queueDepth) / drainSeconds
			} else {
				// In cooldown window and below high threshold: suppress demand to prevent thrashing
				effectiveDemand = 0
			}
		} else {
			// Active serving or already provisioning: regular drain rate
			if queueDepth > 0 {
				effectiveDemand = float64(queueDepth) / drainSeconds
			}
		}

		if cfg.MaxReplicas > 0 {
			maxDemand := float64(cfg.MaxReplicas) * perReplicaSupply * scaleUpThreshold
			if effectiveDemand > maxDemand {
				effectiveDemand = maxDemand
			}
		}
	} else {
		// "binary" mode (default): scale up to 1 pod when backlog exists, scale down to 0 when backlog is 0.
		// Ignore drain time predictions.
		if totalActiveReplicas == 0 && totalPendingReplicas == 0 {
			timeSinceScaleDown := now.Sub(lastScaleDown)
			if queueDepth >= cfg.HighQueueThreshold {
				// Immediate scale-up to 1 pod: high backlog bypasses cooldown
				effectiveDemand = perReplicaSupply * scaleUpThreshold
			} else if queueDepth >= cfg.MinQueueThreshold && (lastScaleDown.IsZero() || timeSinceScaleDown >= cfg.MinScaleDownAge) {
				// Cooldown expired: backlog triggers scale-up to 1 pod
				effectiveDemand = perReplicaSupply * scaleUpThreshold
			} else {
				// In cooldown window and below high threshold: suppress demand to prevent thrashing
				effectiveDemand = 0
			}
		} else {
			// Active serving or provisioning:
			// If backlog > 0, demand is sustained for 1 pod (holds 1 replica, prevents scale-down and prevents scale-up > 1).
			// If backlog == 0, demand is 0 (allows scale-down to 0).
			if queueDepth > 0 {
				effectiveDemand = perReplicaSupply * scaleUpThreshold
			} else {
				effectiveDemand = 0
			}
		}
	}

	// 5. Populate per-variant capacity
	var variantCapacities []domain.VariantCapacity
	for _, vs := range input.VariantStates {
		totalCapacity := float64(vs.CurrentReplicas) * perReplicaSupply
		utilization := float64(0)
		if totalCapacity > 0 {
			utilization = effectiveDemand / totalCapacity
		}

		role := vs.Role
		if role == "" {
			role = domain.RoleBoth
		}

		variantCapacities = append(variantCapacities, domain.VariantCapacity{
			VariantName:        vs.VariantName,
			Role:               role,
			ReplicaCount:       vs.CurrentReplicas,
			PendingReplicas:    vs.PendingReplicas,
			PerReplicaCapacity: perReplicaSupply,
			TotalCapacity:      totalCapacity,
			TotalDemand:        effectiveDemand,
			Utilization:        utilization,
			Reason:             "async_queue_backlog",
		})
	}

	result := &domain.AnalyzerResult{
		AnalyzerName:           AnalyzerName,
		ModelID:                input.ModelID,
		Namespace:              input.Namespace,
		AnalyzedAt:             now,
		VariantCapacities:      variantCapacities,
		TotalSupply:            aggregation.SumTotalSupply(variantCapacities),
		TotalAnticipatedSupply:  aggregation.SumTotalAnticipatedSupply(variantCapacities),
		TotalDemand:            effectiveDemand,
	}

	return result, nil
}

// ExtractQueueDepth extracts the queue depth / backlog matching the config.
func (a *AsyncQueueAnalyzer) ExtractQueueDepth(input domain.AnalyzerInput, cfg *Config) int64 {
	a.mu.Lock()
	mockSamples := a.mockMetricSamples
	a.mu.Unlock()

	// 1. If mock samples are provided, use them
	if len(mockSamples) > 0 {
		return a.aggregateSamples(mockSamples, cfg)
	}

	// 2. If SchedulerQueue is populated on AnalyzerInput (e.g. from EPP / flowcontrol), use it
	if input.SchedulerQueue != nil && input.SchedulerQueue.QueueSize > 0 {
		return input.SchedulerQueue.QueueSize
	}

	return 0
}

func (a *AsyncQueueAnalyzer) aggregateSamples(samples []MetricSample, cfg *Config) int64 {
	var totalBacklog int64 = 0
	seenSeries := make(map[string]struct{})

	for _, sample := range samples {
		// 1. Match metric name
		if sample.Name != cfg.MetricName && sample.Name != DefaultMetricName && sample.Name != DefaultFallbackMetricName {
			continue
		}

		// 2. Match QueueName (exact if set, wildcard if empty)
		queueName := sample.Labels["queue_name"]
		if cfg.QueueName != "" && queueName != cfg.QueueName {
			continue
		}

		// 3. Match PoolName (exact if set, wildcard if empty)
		poolName := sample.Labels["pool_name"]
		if cfg.PoolName != "" && poolName != cfg.PoolName {
			continue
		}

		// 4. Match QueueID (exact if set, wildcard if empty)
		queueID := sample.Labels["queue_id"]
		if cfg.QueueID != "" && queueID != cfg.QueueID {
			continue
		}

		// 5. Deduplicate series
		seriesKey := fmt.Sprintf("%s|%s|%s", queueID, queueName, poolName)
		if _, seen := seenSeries[seriesKey]; seen {
			continue
		}
		seenSeries[seriesKey] = struct{}{}

		// 6. Aggregate
		totalBacklog += int64(sample.Value)
	}

	return totalBacklog
}

func (a *AsyncQueueAnalyzer) extractConfig(input domain.AnalyzerInput) *Config {
	if sc, ok := input.Config.(*config.SaturationScalingConfig); ok {
		for _, ac := range sc.Analyzers {
			if ac.EffectiveType() == AnalyzerName || ac.Name == AnalyzerName {
				if cfg, err := ParseConfig(ac.Parameters); err == nil {
					return cfg
				}
			}
		}
	}
	return DefaultConfig()
}
