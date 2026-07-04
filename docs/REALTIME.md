# Realtime latency: event-driven engine and the native GIL-free hot path

rcsp runs the same graph in simulation (as fast as possible) or in real time
(paced to the wall clock). This document covers reaction latency in realtime
mode — how fast the engine reacts to a pushed input or fires a timer — and how to
push it from milliseconds toward sub-microsecond.

## 1. The event-driven realtime loop (shipped)

The realtime driver (`run_realtime`, `src/lib.rs`) originally slept a fixed 1 ms
each iteration, so both input draining and timer firing were quantized to ~1 ms.

It is now **event-driven**:

- It blocks only until the **next scheduled event's deadline** (or the run end),
  so timers fire on their exact time — no 1 ms quantization.
- A pushed input **wakes the loop immediately**: `GenericPushAdapter.push_tick`
  puts the value on its data queue *and* a token on a shared wakeup
  `queue.Queue`; the engine blocks on that queue's `get(timeout=deadline)`, which
  releases the GIL while waiting and returns the instant a token arrives.
- Idle CPU stays near zero (no busy polling), and the GIL is released while
  blocked, so background threads (Kafka consumers, the persistence writer) keep
  running.

`realtime_max_idle` (default 1 ms) caps how long the loop blocks when nothing is
scheduled, so it always notices the run's end promptly.

**Measured:** push→node reaction drops from the ~1 ms floor to **~tens–hundreds
of µs** (median ~150 µs on a shared box). The residual is not the engine — it is
Python: waking a Python thread and acquiring the GIL to run the node body.

## 2. The ceiling: Python and the GIL

Node bodies are Python callables that run under the GIL. Every reaction therefore
costs at least one GIL hand-off between the producer thread and the engine, plus
Python thread scheduling — tens of µs, and non-deterministic under load. No amount
of loop tuning removes this: **as long as Python is on the hot path, ~10–100 µs
with jitter is the floor.** Busy-spinning in Rust does *not* help here — it would
hold the GIL and starve the very producer/writer threads it's waiting on.

## 3. The native GIL-free hot path (design)

For sub-µs, deterministic latency, remove Python from the hot path entirely.

**Constraint.** The latency-critical subgraph must be **native-kernel only** —
`const`/`timer`/`delay`/`binop`/`filter`/`sample`/`merge`/`count`/`firstN` — over
typed values (`Int`/`Float`/`Bool`), with **no** Python nodes and no
`Value::Py`. A build-time guard rejects a graph with any Python node on the path:
*"native realtime requires a native-only graph."*

**Lock-free I/O.** Replace the Python `queue.Queue` with a bounded **lock-free
ring buffer** (`crossbeam_queue::ArrayQueue`) for input, and a ring (or a
preallocated typed columnar buffer) for output. A **native producer** — a Rust
thread reading a socket/feed/hardware — writes to the ring without ever taking the
GIL.

**Executor.** `run_realtime_native` wraps the whole loop in
`py.allow_threads(|| … )` — so it holds no GIL — and:
1. pops inputs from the ring,
2. runs `process_cycle_native`, a `py`-free variant of `process_cycle` operating
   only on typed `Value`s and native kernels,
3. writes typed outputs to the output ring.

With the GIL gone, the loop can **busy-spin** (`std::hint::spin_loop`) for the
lowest latency, or `park_timeout` to the next deadline for low idle CPU. Typed
outputs are converted to Python once, after the run.

**Latency envelope.** A native cycle is a few hundred ns; a lock-free ring
hand-off is tens of ns → **sub-µs and deterministic**. End-to-end sub-µs requires
the **producer to be native** too: a Python producer is bounded by GIL-acquire on
each `push_tick`.

### Measured proof-of-concept

`rcsp.native_latency_benchmark(iters)` runs exactly this pipeline — a Rust
producer thread → `ArrayQueue` → a GIL-released consumer running a native binop —
and reports latency percentiles. Representative result:

```
min ≈ 250 ns   median ≈ 470 ns   p90 ≈ 570 ns
```

i.e. **~0.5 µs median**, ~300× below the Python event-driven path, confirming the
design's envelope. The lock-free ring primitive (`crossbeam-queue`) and this
benchmark ship today.

### The native executor (shipped)

`rcsp.run(graph, ..., realtime="native")` runs the whole engine loop GIL-free:

- The graph is **compiled** to a native form — a Python-free value type (`NV`)
  and native nodes — after a **native-only guard** rejects any `@node`/`print`
  (raising a clear error pointing you back to `realtime=True`).
- The entire discrete-event loop then runs inside `py.allow_threads` (no GIL, no
  lock), spinning over the wall clock and draining a lock-free input ring
  (`rcsp.NativePushAdapter` → `crossbeam-queue`). Typed outputs are converted to
  Python once, after the run.
- It is verified to produce **identical results** to the normal engine on the
  same native graph (`tests/test_native_executor.py`).

Supported native kernels: `const`, `timer`, `delay`, `count`, `firstN`, binops
(`+ - * /`, comparisons), `filter`, `sample`, `merge`, and graph outputs — i.e.
the whole baselib except Python `@node`s and `print`. A Python producer can feed
`NativePushAdapter.push_tick` (bounded by GIL-acquire per push); a native
producer writes the ring directly for end-to-end sub-µs.

## 4. Is the producer fast enough?

Reaction latency (push→node) does **not** answer this: it is stamped at push
time, so a slow producer's lateness happens *before* the measurement and is
invisible. You have to measure the **producer** directly, which
`rcsp.producer_benchmark(duration, target_rate=…)` does:

- **throughput** — push in a tight loop and count ticks/s (and confirm the engine
  drained them all). On a shared box this is ~10^5 ticks/s for one Python thread,
  because each `push_tick` runs Python bytecode + `queue.put`s under the GIL.
- **pacing jitter** — at a target rate, the actual inter-push interval vs the
  intended one (e.g. ~200µs median but a fatter p99 under GIL/scheduling/GC).

Compare those to your feed's required rate and jitter budget. A Python producer is
fine below ~10^5 ticks/s with ~100µs jitter; above that, or for deterministic
pacing, the producer must be **native** — the sub-µs native benchmark hits its
numbers precisely because *both* producer and consumer are native (Rust thread →
lock-free ring → Rust consumer). End-to-end sub-µs needs a native producer, not
just a native engine.

## Choosing a mode

| Need | Use |
|---|---|
| Backtest / batch | simulation (`realtime=False`) — as fast as possible |
| Live, ~tens of µs, arbitrary Python nodes | `realtime=True` (event-driven; default) |
| Sub-µs, deterministic, native compute only | `realtime="native"` (native-only graph, GIL-free) |
