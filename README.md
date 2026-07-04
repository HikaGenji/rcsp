# rcsp

A clone of [Point72's CSP](https://github.com/Point72/csp) (Composable Stream
Processing) with a **Rust** event-graph engine.

`rcsp` keeps CSP's programming model — you build a directed graph of `@node`
computations connected by time-series edges, then run it as a discrete-event
simulation or in real time — but replaces CSP's C++ engine with a compiled Rust
backend (via [PyO3](https://pyo3.rs)). This mirrors CSP's own architecture:
**Rust drives scheduling; Python expresses the computation.**

See [`docs/DESIGN.md`](docs/DESIGN.md) for the full engine model and a
comparison with CSP, and [`docs/EXAMPLES.md`](docs/EXAMPLES.md) for the status
of every CSP example ported into rcsp.

## Features

- `@node` / `@graph`, `ts[T]`, single and named (`Outputs`) outputs
- `ticked` / `valid`, per-node `state`, `starting`, and user alarms
- baselib: `const`, `timer`, `sample`, `filter`, `merge`, `count`, `firstN`,
  `delay`, `split`, `curve`, `print`, `apply`, plus native arithmetic on edges
- pass **arbitrary Python objects** (NumPy arrays, Polars/pandas DataFrames)
  through edges and as scalar node params; `rcsp.apply(fn, *edges)` runs any
  function over edge values
- **`feedback`** edges for graph cycles (e.g. algo ↔ exchange loops)
- **`GenericPushAdapter`** for pushing realtime data in from other threads
- **`show_graph`** / `graph_to_dot` / `graph_to_mermaid` graph visualization
- **`rcsp.stats`** rolling-window statistics (mean, sum, var, stddev, min, max,
  median, …) over tick-count or time windows
- **`rcsp.profiler`** per-node execution counts and timing
- **NumPy/Polars interop** — arrays flow through edges; `to_polars` collects results
- **file I/O adapters** — `read_parquet`/`read_csv` (pull), `write_parquet`/`write_csv` (output)
- **adapter managers** — fan one source out to per-key streams
- **dynamic graphs** — `rcsp.dynamic` spawns per-key sub-graphs at runtime
- **realtime audit** — `run(persist="audit.jsonl", realtime=True)` streams every
  node's output to disk live (opt-in; `tail -f` to watch computations happen)
- **Kafka adapters** — `rcsp.KafkaAdapterManager` subscribe/publish (with an
  in-memory broker double for tests)
- discrete-event **simulation** and wall-clock **realtime** modes, one code path
- **event-driven realtime** — reacts to pushed inputs in ~tens of µs (no fixed
  poll); **`realtime="native"`** runs a native-only graph with the whole engine
  loop GIL-free for sub-µs, deterministic latency (see [`docs/REALTIME.md`](docs/REALTIME.md))

Ported CSP examples live under [`examples/csp_ports/`](examples/csp_ports/),
mirroring CSP's category layout (basics, feedback, adapters, the NAND-gate
computer).

## Quick start

```python
from datetime import datetime, timedelta
import rcsp
from rcsp import ts


@rcsp.node
def add(x: ts[int], y: ts[int]) -> ts[int]:
    if rcsp.ticked(x, y) and rcsp.valid(x, y):
        return x + y


@rcsp.graph
def g():
    s = add(rcsp.const(1), rcsp.const(2))
    rcsp.print("sum", s)
    rcsp.add_graph_output("sum", s)


out = rcsp.run(g, starttime=datetime(2020, 1, 1))
print(out["sum"])   # [(datetime(2020, 1, 1, ...), 3)]
```

## Building from source

Requires a Rust toolchain and Python 3.8+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin
maturin develop --release
```

Then run the examples and tests:

```bash
python examples/e1_basic.py
pip install pytest && pytest tests/
```
