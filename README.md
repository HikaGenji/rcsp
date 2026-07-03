# rcsp

A clone of [Point72's CSP](https://github.com/Point72/csp) (Composable Stream
Processing) with a **Rust** event-graph engine.

`rcsp` keeps CSP's programming model — you build a directed graph of `@node`
computations connected by time-series edges, then run it as a discrete-event
simulation or in real time — but replaces CSP's C++ engine with a compiled Rust
backend (via [PyO3](https://pyo3.rs)). This mirrors CSP's own architecture:
**Rust drives scheduling; Python expresses the computation.**

See [`docs/DESIGN.md`](docs/DESIGN.md) for the full engine model and a
comparison with CSP.

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
