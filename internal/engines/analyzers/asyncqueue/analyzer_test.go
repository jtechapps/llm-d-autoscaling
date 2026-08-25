package asyncqueue

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/config"
	"github.com/llm-d/llm-d-workload-variant-autoscaler/internal/domain"
)

func TestParseConfig_Defaults(t *testing.T) {
	cfg, err := ParseConfig(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.QueueName != "" {
		t.Errorf("expected empty QueueName (wildcard), got %q", cfg.QueueName)
	}
	if cfg.PoolName != "" {
		t.Errorf("expected empty PoolName (wildcard), got %q", cfg.PoolName)
	}
	if cfg.HighQueueThreshold != DefaultHighQueueThreshold {
		t.Errorf("expected highQueueThreshold %d, got %d", DefaultHighQueueThreshold, cfg.HighQueueThreshold)
	}
	if cfg.MinQueueThreshold != DefaultMinQueueThreshold {
		t.Errorf("expected minQueueThreshold %d, got %d", DefaultMinQueueThreshold, cfg.MinQueueThreshold)
	}
	if cfg.MinScaleDownAge != DefaultMinScaleDownAge {
		t.Errorf("expected minScaleDownAge %v, got %v", DefaultMinScaleDownAge, cfg.MinScaleDownAge)
	}
	if cfg.TargetDrainTime != DefaultTargetDrainTime {
		t.Errorf("expected targetDrainTime %v, got %v", DefaultTargetDrainTime, cfg.TargetDrainTime)
	}
}

func TestParseConfig_CustomParameters(t *testing.T) {
	raw := map[string]any{
		"queueName":            "llm-d-async:requests:vllm-pool",
		"poolName":             "vllm-pool",
		"highQueueThreshold":   100,
		"minQueueThreshold":    5,
		"minScaleDownAge":      "30m",
		"targetDrainTime":      "10m",
		"defaultPerReplicaRPS": 25.0,
	}

	cfg, err := ParseConfig(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.QueueName != "llm-d-async:requests:vllm-pool" {
		t.Errorf("expected QueueName %q, got %q", "llm-d-async:requests:vllm-pool", cfg.QueueName)
	}
	if cfg.PoolName != "vllm-pool" {
		t.Errorf("expected PoolName %q, got %q", "vllm-pool", cfg.PoolName)
	}
	if cfg.HighQueueThreshold != 100 {
		t.Errorf("expected HighQueueThreshold 100, got %d", cfg.HighQueueThreshold)
	}
	if cfg.MinQueueThreshold != 5 {
		t.Errorf("expected MinQueueThreshold 5, got %d", cfg.MinQueueThreshold)
	}
	if cfg.MinScaleDownAge != 30*time.Minute {
		t.Errorf("expected MinScaleDownAge 30m, got %v", cfg.MinScaleDownAge)
	}
	if cfg.TargetDrainTime != 10*time.Minute {
		t.Errorf("expected TargetDrainTime 10m, got %v", cfg.TargetDrainTime)
	}
	if cfg.DefaultPerReplicaRPS != 25.0 {
		t.Errorf("expected DefaultPerReplicaRPS 25.0, got %v", cfg.DefaultPerReplicaRPS)
	}
}

func TestAsyncQueueAnalyzer_MetricFiltering(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()

	samples := []MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool-1",
				"pool_name":  "vllm-pool-1",
				"queue_id":   "q1",
			},
			Value: 40,
		},
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool-2",
				"pool_name":  "vllm-pool-2",
				"queue_id":   "q2",
			},
			Value: 60,
		},
		{
			// Duplicate scrape of q1
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool-1",
				"pool_name":  "vllm-pool-1",
				"queue_id":   "q1",
			},
			Value: 40,
		},
	}
	analyzer.SetMockMetricSamples(samples)

	// Scenario 1: Specific queue matching
	cfg1 := &Config{
		QueueName:  "llm-d-async:requests:vllm-pool-1",
		PoolName:   "vllm-pool-1",
		MetricName: DefaultMetricName,
	}
	depth1 := analyzer.aggregateSamples(samples, cfg1)
	if depth1 != 40 {
		t.Errorf("expected depth 40 for specific queue, got %d", depth1)
	}

	// Scenario 2: Wildcard queue aggregation across all pools
	cfg2 := &Config{
		QueueName:  "",
		PoolName:   "",
		MetricName: DefaultMetricName,
	}
	depth2 := analyzer.aggregateSamples(samples, cfg2)
	if depth2 != 100 { // 40 + 60 (deduplicated)
		t.Errorf("expected aggregated depth 100, got %d", depth2)
	}

	// Scenario 3: Wildcard queue within a specific pool
	cfg3 := &Config{
		QueueName:  "",
		PoolName:   "vllm-pool-2",
		MetricName: DefaultMetricName,
	}
	depth3 := analyzer.aggregateSamples(samples, cfg3)
	if depth3 != 60 {
		t.Errorf("expected pool-2 depth 60, got %d", depth3)
	}
}

func TestAsyncQueueAnalyzer_ScaleFromZero_BinaryMode(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()
	analyzer.SetLastScaleDownTime("llm-d-demo", "Qwen/Qwen3-8B", time.Now().Add(-10*time.Minute)) // within 1h cooldown

	// 500 requests queued (> highQueueThreshold 50)
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 500,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 0, // 0 replicas (scaled to zero)
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			ScaleUpThreshold: 0.85,
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"queueName":            "llm-d-async:requests:vllm-pool",
						"highQueueThreshold":   50,
						"minQueueThreshold":    1,
						"minScaleDownAge":      "1h",
						"defaultPerReplicaRPS": 12.0,
						// Binary mode (default): ignores drain time predictions and targets exactly 1 pod
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// In binary mode: demand = 12.0 * 0.85 = 10.2 (sufficient to request exactly 1 pod)
	expectedDemand := 12.0 * 0.85
	if result.TotalDemand != expectedDemand {
		t.Errorf("expected TotalDemand %f, got %f", expectedDemand, result.TotalDemand)
	}

	if result.TotalSupply != 0 {
		t.Errorf("expected TotalSupply 0 with 0 replicas, got %f", result.TotalSupply)
	}
}

func TestAsyncQueueAnalyzer_ScaleFromZero_DrainTimeMode(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()
	analyzer.SetLastScaleDownTime("llm-d-demo", "Qwen/Qwen3-8B", time.Now().Add(-10*time.Minute))

	// 500 requests queued (> highQueueThreshold 50)
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 500,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 0,
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"mode":               ModeDrainTime,
						"queueName":          "llm-d-async:requests:vllm-pool",
						"highQueueThreshold": 50,
						"minQueueThreshold":  1,
						"minScaleDownAge":    "1h",
						"targetDrainTime":    "5m", // 300s
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// In drain_time mode: 500 / 300s = 1.6667 req/s demand
	expectedDemand := 500.0 / 300.0
	if result.TotalDemand != expectedDemand {
		t.Errorf("expected TotalDemand %f, got %f", expectedDemand, result.TotalDemand)
	}

	if result.TotalSupply != 0 {
		t.Errorf("expected TotalSupply 0 with 0 replicas, got %f", result.TotalSupply)
	}
}

func TestAsyncQueueAnalyzer_ScaleFromZero_CooldownSuppression(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()
	// Recently scaled down (10 mins ago), within 1 hour cooldown window
	analyzer.SetLastScaleDownTime("llm-d-demo", "Qwen/Qwen3-8B", time.Now().Add(-10*time.Minute))

	// Small backlog of 5 requests (< highQueueThreshold 50)
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 5,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 0,
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"queueName":          "llm-d-async:requests:vllm-pool",
						"highQueueThreshold": 50,
						"minQueueThreshold":  1,
						"minScaleDownAge":    "1h",
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// In cooldown window and < 50 requests -> demand must be suppressed to 0 to prevent GPU thrashing
	if result.TotalDemand != 0 {
		t.Errorf("expected TotalDemand 0 during cooldown, got %f", result.TotalDemand)
	}
}

func TestAsyncQueueAnalyzer_ScaleFromZero_CooldownExpired(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()
	// Scaled down 2 hours ago (> 1 hour cooldown window)
	analyzer.SetLastScaleDownTime("llm-d-demo", "Qwen/Qwen3-8B", time.Now().Add(-2*time.Hour))

	// Small backlog of 5 requests (>= minQueueThreshold 1)
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 5,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 0,
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			ScaleUpThreshold: 0.85,
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"queueName":            "llm-d-async:requests:vllm-pool",
						"highQueueThreshold":   50,
						"minQueueThreshold":    1,
						"minScaleDownAge":      "1h",
						"defaultPerReplicaRPS": 12.0,
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// Cooldown expired: 5 requests triggers scale-up to 1 pod in binary mode
	expectedDemand := 12.0 * 0.85
	if result.TotalDemand != expectedDemand {
		t.Errorf("expected TotalDemand %f, got %f", expectedDemand, result.TotalDemand)
	}
}

func TestAsyncQueueAnalyzer_PendingReplicasAnticipatedSupply(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()

	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 100,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 0, // 0 ready
				PendingReplicas: 1, // 1 pending pod (currently booting/warming)
			},
		},
		Config: &config.SaturationScalingConfig{
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"queueName":            "llm-d-async:requests:vllm-pool",
						"defaultPerReplicaRPS": 12.0,
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	if result.TotalSupply != 0 {
		t.Errorf("expected TotalSupply 0 (0 ready), got %f", result.TotalSupply)
	}

	// TotalAnticipatedSupply must include the 1 pending replica (1 * 12.0 = 12.0)
	if result.TotalAnticipatedSupply != 12.0 {
		t.Errorf("expected TotalAnticipatedSupply 12.0 (from 1 pending pod), got %f", result.TotalAnticipatedSupply)
	}
}

func TestAsyncQueueAnalyzer_HoldAtOneReplica(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()

	// 5000 requests in backlog
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:vllm-pool",
				"pool_name":  "vllm-pool",
			},
			Value: 5000,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4",
				CurrentReplicas: 1, // Already has 1 ready replica
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			ScaleUpThreshold: 0.85,
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"queueName":            "llm-d-async:requests:vllm-pool",
						"defaultPerReplicaRPS": 12.0,
						// Binary mode holds at 1 replica even with 5000 queued requests
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	// Demand is held at 1 pod's capacity (10.2 req/s)
	expectedDemand := 12.0 * 0.85
	if result.TotalDemand != expectedDemand {
		t.Errorf("expected TotalDemand %f, got %f", expectedDemand, result.TotalDemand)
	}

	// TotalSupply is 12.0 req/s
	if result.TotalSupply != 12.0 {
		t.Errorf("expected TotalSupply 12.0, got %f", result.TotalSupply)
	}
}

func TestAsyncQueueAnalyzer_MultiVariant_DrainTimeMode(t *testing.T) {
	analyzer := NewAsyncQueueAnalyzer()

	// 3600 requests queued across all pools
	analyzer.SetMockMetricSamples([]MetricSample{
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:pool-1",
				"pool_name":  "pool-1",
				"queue_id":   "q1",
			},
			Value: 2000,
		},
		{
			Name: "llm_d_async_async_broker_backlog",
			Labels: map[string]string{
				"queue_name": "llm-d-async:requests:pool-2",
				"pool_name":  "pool-2",
				"queue_id":   "q2",
			},
			Value: 1600,
		},
	})

	input := domain.AnalyzerInput{
		ModelID:   "Qwen/Qwen3-8B",
		Namespace: "llm-d-demo",
		VariantStates: []domain.VariantReplicaState{
			{
				VariantName:     "qwen3-8b-l4-spot",
				CurrentReplicas: 2,
				PendingReplicas: 0,
			},
			{
				VariantName:     "qwen3-8b-l40s",
				CurrentReplicas: 1,
				PendingReplicas: 0,
			},
		},
		Config: &config.SaturationScalingConfig{
			Analyzers: []config.AnalyzerScoreConfig{
				{
					Name: AnalyzerName,
					Parameters: map[string]any{
						"mode":                 ModeDrainTime,
						"targetDrainTime":      "6m", // 360s -> Demand = 3600 / 360 = 10 req/s
						"defaultPerReplicaRPS": 10.0,
					},
				},
			},
		},
	}

	result, err := analyzer.Analyze(context.Background(), input)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	if len(result.VariantCapacities) != 2 {
		t.Fatalf("expected 2 variant capacities, got %d", len(result.VariantCapacities))
	}

	// 3600 / 360s = 10 req/s demand
	if result.TotalDemand != 10.0 {
		t.Errorf("expected TotalDemand 10.0, got %f", result.TotalDemand)
	}

	// Total supply: 3 replicas * 10 req/s = 30 req/s
	if result.TotalSupply != 30.0 {
		t.Errorf("expected TotalSupply 30.0, got %f", result.TotalSupply)
	}
}
