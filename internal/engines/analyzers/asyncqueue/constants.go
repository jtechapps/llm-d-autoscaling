package asyncqueue

import "time"

// AnalyzerName is the analyzer plugin type string used to select this analyzer
// via the saturation-scaling ConfigMap (`type: async_queue`).
const AnalyzerName = "async_queue"

// defaultPerReplicaCapacity is the assumed sustained request throughput a single
// replica can drain, in the analyzer's own demand units (req/s). In Milestone 1
// the binary demand function makes the magnitude inert — any positive value
// yields the same 0↔1 result — because demand is emitted as PerReplicaCapacity ×
// τ_up so the engine's post-step resolves it to exactly one pod of Required
// Capacity. It exists so the engine's capacity→replica conversion has a
// well-defined, nonzero denominator, and becomes load-bearing only in
// Milestone 2 (T_est_drain = Backlog / PerReplicaCapacity).
const defaultPerReplicaCapacity = 12.0

// defaultMinQueueThreshold is the minimum backlog depth that counts as pending
// work. Below it the backlog is treated as empty (subject to the cooldown). The
// default of 1 means any single queued request wakes a replica.
const defaultMinQueueThreshold = 1.0

// modeBinary is the only mode implemented in Milestone 1: a binary 0↔1 decision
// driven purely by backlog presence. The deadline_aware (M2) and dynamic_drain
// (M3) modes are declared in the roadmap's config schema but not yet implemented;
// parseParams rejects them so misconfiguration surfaces at load time.
const modeBinary = "binary"

// defaultMinScaleDownAge is how long a warmed variant is held at one replica
// after the backlog empties before it is allowed to drain to zero. Operators
// tune this to the window over which they are willing to let requests accumulate
// between wake-ups: a longer cooldown accumulates more work per wake-up and
// scales up less often; a shorter one reacts sooner at the cost of more cold
// starts.
const defaultMinScaleDownAge = time.Hour

// defaultScaleUpThreshold mirrors config.SaturationScalingConfig's default
// scaleUpThreshold. Used only as a fail-safe when the resolved τ_up is
// non-positive (config not yet defaulted); the engine applies the authoritative
// value in its universal threshold post-step.
const defaultScaleUpThreshold = 0.85
