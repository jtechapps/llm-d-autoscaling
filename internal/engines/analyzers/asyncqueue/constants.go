package asyncqueue

import "time"

const (
	// AnalyzerName is the unique identifier for the AsyncQueueAnalyzer.
	AnalyzerName = "async_queue"

	// ModeBinary is the initial/default autoscaling mode: scale up to 1 pod when backlog > 0,
	// scale down to 0 when backlog == 0, ignoring drain time predictions.
	ModeBinary = "binary"

	// ModeDrainTime calculates demand dynamically based on target drain time SLA.
	ModeDrainTime = "drain_time"

	// DefaultMode is the default autoscaling mode.
	DefaultMode = ModeBinary

	// DefaultMaxReplicas is the default replica cap in drain_time mode (0 = uncapped).
	DefaultMaxReplicas = 0

	// DefaultMetricName is the Prometheus metric name emitted by llm-d-async.
	DefaultMetricName = "llm_d_async_async_broker_backlog"

	// DefaultFallbackMetricName is the alternative queue depth metric name.
	DefaultFallbackMetricName = "llm_d_async_async_queue_depth"

	// DefaultHighQueueThreshold triggers immediate scale-up from zero, bypassing cooldown.
	DefaultHighQueueThreshold int64 = 50

	// DefaultMinQueueThreshold is the minimum queue depth required to scale up after cooldown.
	DefaultMinQueueThreshold int64 = 1

	// DefaultMinScaleDownAge is the cooldown window following scale-down to 0.
	DefaultMinScaleDownAge = 1 * time.Hour

	// DefaultTargetDrainTime is the target duration to drain the backlog (e.g. 5m).
	DefaultTargetDrainTime = 5 * time.Minute

	// DefaultPerReplicaRPS is the baseline throughput per replica in requests/sec.
	DefaultPerReplicaRPS float64 = 12.0
)
