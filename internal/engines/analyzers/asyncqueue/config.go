package asyncqueue

import (
	"strconv"
	"time"
)

// Config holds the configuration for the AsyncQueueAnalyzer.
type Config struct {
	// Mode specifies the autoscaling mode: "binary" (default) or "drain_time".
	// In "binary" mode, the analyzer scales up to 1 pod when there is backlog,
	// and scales down to 0 when the backlog is empty, ignoring drain time predictions.
	Mode string `yaml:"mode"`

	// MaxReplicas is the maximum number of replicas the async queue analyzer can request.
	// Defaults to 1 for binary scale-up-to-1 / scale-down-to-0 mode.
	MaxReplicas int `yaml:"maxReplicas"`

	// QueueName is the exact queue name to match (e.g. "llm-d-async:requests:vllm-pool").
	// When empty (""), the analyzer aggregates across ALL queues.
	QueueName string `yaml:"queueName"`

	// PoolName is the worker pool name to match (e.g. "vllm-pool").
	// When empty (""), the analyzer aggregates across ALL pools.
	PoolName string `yaml:"poolName"`

	// QueueID is the optional queue ID to match.
	// When empty (""), matches any queue ID.
	QueueID string `yaml:"queueID"`

	// MetricName is the Prometheus metric name to query for backlog.
	// Defaults to "llm_d_async_async_broker_backlog".
	MetricName string `yaml:"metricName"`

	// HighQueueThreshold is the backlog size that triggers immediate scale-up from 0.
	HighQueueThreshold int64 `yaml:"highQueueThreshold"`

	// MinQueueThreshold is the minimum backlog to scale up after minScaleDownAge has elapsed.
	MinQueueThreshold int64 `yaml:"minQueueThreshold"`

	// MinScaleDownAge is the cooldown window after scaling down to 0 before minQueueThreshold applies.
	MinScaleDownAge time.Duration `yaml:"minScaleDownAge"`

	// TargetDrainTime is the target duration to drain the entire backlog in drain_time mode (Demand = Backlog / TargetDrainTime).
	TargetDrainTime time.Duration `yaml:"targetDrainTime"`

	// DefaultPerReplicaRPS is the default throughput capacity per replica in requests per second.
	DefaultPerReplicaRPS float64 `yaml:"defaultPerReplicaRPS"`
}

// DefaultConfig returns the default configuration.
func DefaultConfig() *Config {
	return &Config{
		Mode:                 DefaultMode,
		MaxReplicas:          DefaultMaxReplicas,
		QueueName:            "",
		PoolName:             "",
		QueueID:              "",
		MetricName:           DefaultMetricName,
		HighQueueThreshold:   DefaultHighQueueThreshold,
		MinQueueThreshold:    DefaultMinQueueThreshold,
		MinScaleDownAge:      DefaultMinScaleDownAge,
		TargetDrainTime:      DefaultTargetDrainTime,
		DefaultPerReplicaRPS: DefaultPerReplicaRPS,
	}
}

// ParseConfig parses raw parameters from AnalyzerScoreConfig.Parameters.
func ParseConfig(raw map[string]any) (*Config, error) {
	cfg := DefaultConfig()
	if raw == nil {
		return cfg, nil
	}

	if mode, ok := raw["mode"].(string); ok && mode != "" {
		cfg.Mode = mode
	}
	if maxR, ok := parseInt(raw["maxReplicas"]); ok {
		cfg.MaxReplicas = maxR
	}
	if qn, ok := raw["queueName"].(string); ok {
		cfg.QueueName = qn
	}
	if pn, ok := raw["poolName"].(string); ok {
		cfg.PoolName = pn
	}
	if qid, ok := raw["queueID"].(string); ok {
		cfg.QueueID = qid
	}
	if mn, ok := raw["metricName"].(string); ok && mn != "" {
		cfg.MetricName = mn
	}

	if hq, ok := parseInt64(raw["highQueueThreshold"]); ok {
		cfg.HighQueueThreshold = hq
	}
	if mq, ok := parseInt64(raw["minQueueThreshold"]); ok {
		cfg.MinQueueThreshold = mq
	}
	if msa, ok := parseDuration(raw["minScaleDownAge"]); ok {
		cfg.MinScaleDownAge = msa
	}
	if tdt, ok := parseDuration(raw["targetDrainTime"]); ok {
		cfg.TargetDrainTime = tdt
	}
	if rps, ok := parseFloat64(raw["defaultPerReplicaRPS"]); ok {
		cfg.DefaultPerReplicaRPS = rps
	}

	return cfg, nil
}

func parseInt(v any) (int, bool) {
	switch val := v.(type) {
	case int:
		return val, true
	case int64:
		return int(val), true
	case float64:
		return int(val), true
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i, true
		}
	}
	return 0, false
}

func parseInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int64:
		return val, true
	case float64:
		return int64(val), true
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

func parseFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func parseDuration(v any) (time.Duration, bool) {
	switch val := v.(type) {
	case time.Duration:
		return val, true
	case string:
		if d, err := time.ParseDuration(val); err == nil {
			return d, true
		}
	case int64:
		return time.Duration(val), true
	case float64:
		return time.Duration(val), true
	}
	return 0, false
}
