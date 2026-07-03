# rcsp design

`rcsp` is a clone of [Point72's CSP](https://github.com/Point72/csp) with the
engine rewritten in Rust. This document describes the engine model, the
Rust ↔ Python boundary, and how the design relates to CSP and to dataflow
frameworks like timely/differential dataflow.

## 1. The programming model (same as CSP)

You describe a computation as a **directed acyclic graph**:

* **Edges** are typed **time series** — streams of `(timestamp, value)` events.
* **Nodes** are computations (`@rcsp.node`) that consume input edges and emit on
  output edges. A node runs only in the engine cycles where one of its inputs
  *ticks*.
* A **graph** (`@rcsp.graph`) is just Python code that wires nodes together.
* `rcsp.run(graph, starttime, endtime, realtime=...)` executes the graph, either
  as a discrete-event **simulation** (as fast as possible over historical time)
  or in **real time** (paced to the wall clock). The same graph runs in both.

Every input exposes two flags inside a node body:

* `valid` — the input has ever produced a value.
* `ticked` — the input produced a value *in the current cycle*.

## 2. The engine (Rust)

The engine is a **discrete-event simulator** with **rank-ordered intra-cycle
propagation** — the two properties that make CSP correct and fast. It lives in
[`src/lib.rs`](../src/lib.rs) and is exposed to Python as `rcsp._rcsp.Engine`.

### Time and cycles

Time is an `i64` count of nanoseconds since the Unix epoch. A global priority
queue orders **future injections** by `(time, seq)`. All work sharing a single
timestamp forms one **engine cycle**.

### Topological ranks — glitch-free propagation

Before running, the engine computes a **topological rank** for every node
(`rank = 1 + max(rank of input producers)`; sources are rank 0). Within a cycle,
nodes fire in ascending rank order. Because a node's rank is strictly greater
than every upstream node's, it always observes *all* upstream ticks from the
current timestamp before it runs. A diamond

```
        ┌──► a = t + 1 ──┐
   t ───┤                ├──► c = a + b
        └──► b = t * 2 ──┘
```

therefore makes `c` tick **exactly once** per `t`, with consistent values — no
transient "glitch" where `c` sees the new `a` but the stale `b`. Cycles in the
graph (feedback) are rejected at rank-computation time.

### The run loop

```
seed source kernels (const/timer/curve, python start-blocks) into the queue
while the queue's next time ≤ endtime:
    now = next time
    (optionally sleep to wall clock if realtime)
    drain every injection at `now`, marking consumer nodes dirty
    pop dirty nodes in rank order:
        run the node
        emit → set edge value+tick=now, mark higher-rank consumers dirty (same cycle)
        alarms → push future injections onto the queue
```

Emitting propagates **within the current cycle**; alarms (timers, delays, user
`schedule_alarm`) are how new timestamps — and thus real time — enter the graph.

### Native kernels vs. Python nodes

Like CSP (whose baselib is C++), `rcsp` splits work:

* **Native Rust kernels** implement the baselib: `const`, `timer`, `delay`,
  `count`, `firstN`, `filter`, `sample`, `merge`, arithmetic/comparison binops,
  `print`, and graph outputs. Edge values use a typed `Value` enum
  (`Int/Float/Bool/Str/Py`) so numeric work stays in Rust.
* **Python nodes** (`@rcsp.node`) are driven by the same engine. Each cycle the
  engine hands the node its inputs' `(values, ticked, valid)` and receives back
  a list of emissions and alarm-schedule requests.

## 3. The Rust ↔ Python boundary

The Python layer (`python/rcsp/`) is thin:

* `_graph.py` — `Builder` (owns the `Engine`) and `Edge` (operator overloading
  wires native binop kernels).
* `_node.py` — the `@node` decorator plus the per-tick runtime: input proxies,
  `ticked`/`valid`/`output`/`state`/`starting` and the alarm helpers, all driven
  through a `contextvar` so node bodies read like CSP's.
* `_baselib.py`, `_run.py`, `_types.py` — baselib wiring, the `run` driver, and
  the `ts` / `Outputs` markers.

A node body observes inputs through `_Input` proxies that delegate arithmetic to
their current value, so `return x + y` works while `ticked(x)` still resolves
the edge — no AST rewriting required.

## 4. Relationship to CSP

`rcsp` reproduces CSP's **semantics** — the graph/node/edge model, tick/valid,
rank-ordered cycles, simulation vs. realtime, baselib, alarms, multi-output
nodes — on a Rust engine instead of C++. It is a faithful *subset*, not a
drop-in replacement. Notable differences:

* Node bodies use a `contextvar`-driven runtime instead of CSP's AST transform,
  so state/alarms are declared via `rcsp.state(...)` and `@node(alarms=[...])`
  rather than `with csp.state()` / `with csp.alarms()`.
* Baskets, `csp.struct`, adapter managers, dynamic graphs, and the full
  `csp.stats`/`csp.math` libraries are out of scope for this clone.

## 5. Why not timely / differential dataflow?

Timely and differential dataflow were the suggested starting points, and they
share DNA with this engine (logical timestamps, progress tracking, dataflow
operators). We implemented a **native discrete-event scheduler** instead because
it maps 1:1 onto CSP's semantics:

* CSP's core guarantee is **rank-ordered, glitch-free propagation within a single
  wall-clock/simulated timestamp**. Timely's frontier/progress model tracks when
  a timestamp is *complete* across workers — powerful for distributed,
  incremental computation, but a different contract than "run these nodes in
  topological order at this instant." Bridging the two adds complexity without
  buying CSP's semantics.
* CSP nodes are arbitrary, stateful **Python callbacks**. Timely operators are
  Rust closures over batched, partitioned streams; hosting per-tick Python
  callbacks inside timely operators would fight its batching model.
* A DES scheduler is exactly what CSP's own C++ engine is, so this is the most
  faithful backend — and it keeps the door open to a timely-backed distributed
  executor as a future alternative engine behind the same Python API.

Differential dataflow would be the natural foundation for a future
`rcsp.stats`-style incremental-aggregation layer.
