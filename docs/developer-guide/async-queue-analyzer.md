# Async Queue Analyzer

## Overview

The Async Queue Analyzer is a **backlog-driven, wake-on-demand scaling analyzer** for
async/batch workloads. It turns the depth of the llm-d-async broker backlog into a scaling
signal so a model that has drained to zero replicas is woken as soon as work arrives, and
allowed to drain back to zero once the backlog clears.

Where the Saturation and Throughput analyzers reason about *how much* decode capacity a running
fleet has, the Async Queue Analyzer answers a coarser question: **is there any pending async
work, and if so, is at least one replica up to serve it?**

> **Status — Milestone 1 (M1): binary wake-on-backlog (0↔1).** This analyzer currently
> implements only the binary decision described below. Deadline-aware fractional demand (M2) and
> dynamic multi-replica drain (M3) are specified in
> [docs/design/async-queue-autoscaling-roadmap.md](../design/async-queue-autoscaling-roadmap.md)
> but not yet implemented. The `mode` parameter accepts only `binary`; any other value is
> rejected at config-load time.
>
> **Demand-only.** The analyzer never actuates. It publishes raw `Total*` demand/supply fields;
> the engine's universal-threshold post-step converts them to RequiredCapacity/SpareCapacity, and
> the existing optimizer + scale-from-zero path performs the actual 0→1 wake and drain-to-zero.

**Key concepts:**
- **Backlog** — the model-level count of async/batch requests waiting in the llm-d-async broker,
  read into `AnalyzerInput.AsyncBacklog`.
- **Binary demand** — whenever work is pending, the analyzer emits exactly one pod's worth of
  demand, so the engine's post-step resolves to one replica of RequiredCapacity (0→1). It never
  requests a second replica in M1.
- **Cooldown (`minScaleDownAge`)** — after the backlog empties, the variant is held warm at one
  replica for this window before it is allowed to drain, so short gaps between batches do not
  cause repeated cold starts.

## Table of Contents

- [Overview](#overview)
- [Enablement](#enablement)
- [Configuration](#configuration)
  - [Parameters](#parameters)
  - [Metric source](#metric-source)
- [Behavior](#behavior)
  - [State machine](#state-machine)
  - [The binary demand math](#the-binary-demand-math)
- [Architecture](#architecture)
  - [Package structure](#package-structure)
  - [Data flow](#data-flow)
  - [State and high availability](#state-and-high-availability)
- [Constants and tuning](#constants-and-tuning)
- [References](#references)

## Enablement

The analyzer is **opt-in**, following the same registration model as the Throughput Analyzer.
At startup the controller checks whether any saturation-config entry lists `async_queue` with
`enabled != false` (`config.AsyncQueueAnalyzerEnabled()`). If none does, the analyzer is not
registered and never participates.

**Runtime toggling requires a restart.** Registration is frozen after `StartOptimizeLoop`;
adding `async_queue` to the ConfigMap takes effect only after a controller restart. The
per-cycle `effectiveEnabled` gate then governs whether the already-registered analyzer votes in
any given cycle (see the [multi-analyzer pipeline guide](multi-analyzer-pipeline.md)).

## Configuration

Enable the analyzer by adding it to the `analyzers:` list in the `wva-saturation-scaling-config`
ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: wva-saturation-scaling-config
  namespace: <workload-variant-autoscaler-namespace>
data:
  default: |
    analyzerName: saturation
    scaleUpThreshold: 0.85
    scaleDownBoundary: 0.70
    analyzers:
      - name: saturation
        score: 1.0
      - name: async_queue
        score: 1.0
        parameters:
          mode: binary
          minQueueThreshold: 1
          minScaleDownAge: 1h
```

**Keep saturation in the list.** The Async Queue Analyzer's per-variant capacities carry no
accelerator/cost identity — it relies on the saturation result as the identity carrier so the
optimizer can translate its one-pod demand onto a concrete variant. Omitting saturation is a
supported config but removes that identity (and saturation's scale-down veto); see
[saturation-scaling-config.md — Multi-Analyzer Pipeline](saturation-scaling-config.md#multi-analyzer-pipeline).

### Parameters

All parameters are optional; omitting a key uses its default. Unknown keys are tolerated (they
belong to later milestones), but a **known** key with a bad value fails config load so
misconfiguration surfaces immediately.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `mode` | string | `binary` | Decision mode. Only `binary` is implemented in M1; any other value (`deadline_aware`, `dynamic_drain`) is rejected. |
| `backlogQuery` | string | *(built-in)* | Operator-supplied PromQL for the backlog metric. See [Metric source](#metric-source). |
| `defaultPerReplicaRPS` | number (> 0) | `12.0` | Assumed per-replica drain rate (req/s). **Inert in M1** — any positive value yields the same 0↔1 result. Becomes load-bearing in M2. |
| `minQueueThreshold` | number (≥ 0) | `1` | Minimum backlog depth that counts as pending work. Below it the backlog is treated as empty. `1` means any single queued request wakes a replica. |
| `minScaleDownAge` | duration (≥ 0) | `1h` | Cooldown: how long the variant is held warm after the backlog empties before it may drain to zero. Accepts a Go duration string (`30m`, `1h`) or a bare number of seconds. `0s` drains as soon as the backlog clears. |

### Metric source

By default the analyzer reads the llm-d-async broker backlog scoped by namespace and model, via:

```promql
sum(llm_d_async_async_broker_backlog{namespace="{{.namespace}}",model_name="{{.modelID}}"})
```

> The doubled `async_async_` segment is llm-d-async's own metric name, not a typo.

llm-d-async natively labels its metrics by `queue_name`/`pool_name` rather than
`namespace`/`model_name`. Deployments using that native label set should override the query via
the `backlogQuery` parameter with PromQL scoped to their queue/pool — the collector always
supplies `{{.namespace}}` and `{{.modelID}}`, and any template that renders to a single scalar
per model is valid:

```yaml
parameters:
  backlogQuery: |
    sum(llm_d_async_async_broker_backlog{pool_name="batch-pool",namespace="{{.namespace}}"})
```

The query is registered once at startup by `RegisterAsyncQueueAnalyzerQueries` and read each
cycle by `ReplicaMetricsCollector.CollectAsyncBacklog` into `AnalyzerInput.AsyncBacklog`. When
the metric is unavailable (broker absent, query returns no series), the backlog reads `0` and the
model is treated as drained.

## Behavior

### State machine

Per model, the analyzer maps `(backlog, replica state, cooldown)` to one of three outcomes:

| State | Condition | Emitted signal | Engine outcome |
|---|---|---|---|
| **Cold** | `backlog ≥ minQueueThreshold`, no active/pending replica | one pod of demand | RC = one replica → **scale 0→1** |
| **Warm** | `backlog ≥ minQueueThreshold`, a replica is active or pending | demand canceled by anticipated supply | RC = 0, SC = 0 → **hold at one** |
| **Drained** | `backlog < minQueueThreshold` **and** cooldown lapsed | zero demand | SC = TotalSupply → **drain to zero** |

Within the cooldown window (backlog empty but `now − lastNonEmpty < minScaleDownAge`), the model
is treated as still pending — held warm — so a lull between batches does not trigger a drain and
subsequent cold start.

### The binary demand math

The analyzer emits demand as `PerReplicaCapacity × τ_up` whenever work is pending, and `0`
otherwise. The engine's universal-threshold post-step then computes:

```
RC = max(0, TotalDemand / τ_up − TotalAnticipatedSupply)
SC = max(0, TotalSupply − TotalDemand / τ_down)
```

Because demand is exactly `PerReplicaCapacity × τ_up`, the `τ_up` division cancels and the
decision is binary regardless of the configured threshold:

- **Cold:** `TotalAnticipatedSupply = 0` → `RC = PerReplicaCapacity` = one replica.
- **Warm:** the active/pending replica contributes `PerReplicaCapacity` of anticipated supply,
  which cancels the demand → `RC = 0`; the τ_up/τ_down deadband keeps `SC = 0`.
- **Drained:** `TotalDemand = 0` → `SC = TotalSupply` = the running replicas, freeing them to
  drain.

This is why `defaultPerReplicaRPS` is inert in M1: it is both the demand numerator and the
capacity→replica denominator, so its magnitude divides out. It exists so the conversion has a
well-defined nonzero denominator and becomes meaningful in M2 (`T_drain = Backlog / PerReplicaCapacity`).

## Architecture

### Package structure

```
internal/engines/analyzers/asyncqueue/
├── constants.go   AnalyzerName, defaults (perReplicaCapacity, minQueueThreshold,
│                  minScaleDownAge, scaleUpThreshold fail-safe)
├── config.go      params struct + parseParams (typed parsing, validation)
└── analyzer.go    AsyncQueueAnalyzer: Analyze() + demand() cooldown logic
```

Supporting wiring lives outside the package:

- **`internal/collector/registration/async_queue.go`** — registers the `async_backlog` PromQL
  query (default or operator override).
- **`internal/collector/replica_metrics.go`** — `CollectAsyncBacklog` reads the query into the
  input.
- **`internal/config/config.go`** — `AsyncQueueAnalyzerEnabled()` and `AsyncBacklogQuery()`
  accessors.
- **`cmd/main.go`** — startup-gated registration.

### Data flow

```
┌────────────┐
│ Prometheus │
└──────┬─────┘
       │ llm_d_async_async_broker_backlog   (QueryAsyncBacklog → AsyncBacklog)
       ↓
┌─────────────────────────┐
│ ReplicaMetricsCollector │  CollectAsyncBacklog(ctx, modelID, namespace)
└──────┬──────────────────┘
       │ AnalyzerInput{AsyncBacklog, VariantStates, Config, ...}
       ↓
┌──────────────────────────────────────────────┐
│ AsyncQueueAnalyzer.Analyze()                 │
│   pending = backlog ≥ minQueueThreshold      │
│            (or within minScaleDownAge)       │
│   TotalDemand   = pending ? PRC × τ_up : 0   │
│   TotalSupply / TotalAnticipatedSupply       │
│     = Σ over variants                        │
│   RC = 0, SC = 0  (engine fills)             │
└──────┬───────────────────────────────────────┘
       │ AnalyzerResult
       ↓
┌──────────────────────────────────────────────┐
│ Engine universal threshold post-step         │
│   RC = max(0, TotalDemand/τ_up − Anticipated) │
│   SC = max(0, TotalSupply − TotalDemand/τ_dn) │
└──────┬───────────────────────────────────────┘
       │ NamedAnalyzerResult (ballot entry, Enabled/Live)
       ↓
┌──────────────────────────────────────┐
│ Multi-analyzer optimizer             │  selects variant via saturation identity;
│ + scale-from-zero engine             │  actuates 0→1 wake / drain-to-zero
└──────────────────────────────────────┘
```

### State and high availability

`AsyncQueueAnalyzer` is stateful across cycles: it keeps a per-model `lastNonEmpty` timestamp
(`map[namespace\x00modelID]time.Time`) to implement the cooldown. The state is **in-memory only**
— no persistence to etcd or Kubernetes — and the map self-evicts models once their cooldown
lapses, so it stays bounded to active models.

In HA mode the reconcile loop runs only on the elected leader, so this state is local to the
leader process. On failover the incoming leader starts with an empty map: a model that was being
held warm purely by the cooldown may drain slightly early, and a model with a live backlog is
simply re-woken on the next cycle. The gap is bounded and self-correcting, so no external state
store is warranted.

## Constants and tuning

| Constant | Default | Description |
|---|---|---|
| `defaultPerReplicaCapacity` | `12.0` | Assumed per-replica drain rate (req/s). Inert in M1; overridable via `defaultPerReplicaRPS`. |
| `defaultMinQueueThreshold` | `1.0` | Minimum backlog counted as pending. Overridable via `minQueueThreshold`. |
| `defaultMinScaleDownAge` | `1h` | Warm-hold cooldown after the backlog empties. Overridable via `minScaleDownAge`. |
| `modeBinary` | `binary` | The only implemented mode in M1. |
| `defaultScaleUpThreshold` | `0.85` | Fail-safe τ_up used only when the resolved threshold is non-positive; the engine applies the authoritative value. |

## References

- Design & roadmap (M1–M3): [docs/design/async-queue-autoscaling-roadmap.md](../design/async-queue-autoscaling-roadmap.md)
- Sibling analyzer: [Throughput Analyzer](throughput-analyzer.md)
- Config & combine algorithm: [saturation-scaling-config.md](saturation-scaling-config.md)
- Pipeline & enablement gating: [multi-analyzer pipeline](multi-analyzer-pipeline.md)
