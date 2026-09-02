package asyncqueue

import (
	"fmt"
	"time"
)

// params holds the async_queue analyzer's typed, validated parameters, parsed
// from the free-form `parameters` map on its saturation-config entry. Field
// names mirror the Milestone 1 keys in the roadmap's §8.4 configuration schema.
type params struct {
	// mode selects the scaling behavior. Milestone 1 implements only "binary".
	mode string
	// perReplicaCapacity is the assumed per-replica drain rate (parsed from the
	// `defaultPerReplicaRPS` key). In M1 its magnitude is inert (see constants.go);
	// it exists as the engine's capacity→replica denominator.
	perReplicaCapacity float64
	// minQueueThreshold is the minimum backlog depth that counts as pending work.
	minQueueThreshold float64
	// minScaleDownAge is the scale-down cooldown: how long a warmed variant is
	// held at one replica after the backlog empties before draining to zero.
	minScaleDownAge time.Duration
}

// parseParams extracts and validates the async_queue analyzer's parameters from
// the free-form map on its AnalyzerScoreConfig entry. Recognized keys with the
// wrong type or an out-of-range value are rejected so misconfiguration surfaces
// at load time rather than as a silent no-op (see roadmap §8.3). Keys belonging
// to later milestones (deadline/quota settings) are tolerated. A nil map yields
// all defaults.
func parseParams(raw map[string]any) (params, error) {
	p := params{
		mode:               modeBinary,
		perReplicaCapacity: defaultPerReplicaCapacity,
		minQueueThreshold:  defaultMinQueueThreshold,
		minScaleDownAge:    defaultMinScaleDownAge,
	}

	if v, ok := raw["mode"]; ok {
		s, ok := v.(string)
		if !ok {
			return params{}, fmt.Errorf("async_queue: mode must be a string, got %T", v)
		}
		if s != modeBinary {
			return params{}, fmt.Errorf("async_queue: mode %q is not supported in Milestone 1 (only %q)", s, modeBinary)
		}
		p.mode = s
	}

	if v, ok := raw["defaultPerReplicaRPS"]; ok {
		f, err := paramFloat("defaultPerReplicaRPS", v)
		if err != nil {
			return params{}, err
		}
		if f <= 0 {
			return params{}, fmt.Errorf("async_queue: defaultPerReplicaRPS must be > 0, got %v", f)
		}
		p.perReplicaCapacity = f
	}

	if v, ok := raw["minQueueThreshold"]; ok {
		f, err := paramFloat("minQueueThreshold", v)
		if err != nil {
			return params{}, err
		}
		if f < 0 {
			return params{}, fmt.Errorf("async_queue: minQueueThreshold must be >= 0, got %v", f)
		}
		p.minQueueThreshold = f
	}

	if v, ok := raw["minScaleDownAge"]; ok {
		d, err := paramDuration("minScaleDownAge", v)
		if err != nil {
			return params{}, err
		}
		if d < 0 {
			return params{}, fmt.Errorf("async_queue: minScaleDownAge must be >= 0, got %s", d)
		}
		p.minScaleDownAge = d
	}

	return p, nil
}

// paramFloat coerces a YAML-decoded parameter to float64. yaml.v3 decodes
// integers as int and decimals as float64 when the target is interface{}, so
// both are accepted (mirrors config.paramFloat).
func paramFloat(key string, v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("async_queue: parameter %q must be a number, got %T", key, v)
	}
}

// paramDuration coerces a YAML-decoded parameter to a time.Duration. A string is
// parsed with time.ParseDuration (e.g. "1h", "90s"); a bare number is
// interpreted as seconds for convenience.
func paramDuration(key string, v any) (time.Duration, error) {
	switch n := v.(type) {
	case string:
		d, err := time.ParseDuration(n)
		if err != nil {
			return 0, fmt.Errorf("async_queue: parameter %q must be a duration (e.g. \"1h\"): %w", key, err)
		}
		return d, nil
	case int:
		return time.Duration(n) * time.Second, nil
	case int64:
		return time.Duration(n) * time.Second, nil
	case float64:
		return time.Duration(n * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("async_queue: parameter %q must be a duration string or number of seconds, got %T", key, v)
	}
}
