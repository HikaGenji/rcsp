"""Dynamic graphs — sub-graphs spawned at runtime per key.

Port of CSP's ``examples/06_advanced/e1_dynamic.py``. A single control stream
carries ``(symbol, price)`` ticks for symbols not known up front. The first time
a symbol appears, ``rcsp.dynamic`` instantiates a per-symbol sub-graph (here a
running VWAP-ish average) and splices it into the running engine.

The engine runs in stepped mode: between cycles the driver builds new sub-graphs,
and the next cycle integrates them (re-ranks, re-wires, seeds).
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def average(px: ts[object]) -> ts[float]:
    s = rcsp.state(total=0.0, n=0)
    if rcsp.ticked(px):
        s.total += float(px.value)
        s.n += 1
        return s.total / s.n


def per_symbol(symbol, prices):
    """Sub-graph instantiated once per symbol."""
    avg = average(prices)
    rcsp.print(f"{symbol}.avg", avg)
    rcsp.add_graph_output(f"{symbol}_avg", avg)


@rcsp.graph
def my_graph():
    ticks = [
        (1, "AAPL", 150.0),
        (2, "MSFT", 300.0),
        (3, "AAPL", 152.0),
        (4, "GOOG", 100.0),
        (5, "MSFT", 305.0),
        (6, "AAPL", 154.0),
        (7, "GOOG", 102.0),
    ]
    control = rcsp.curve(object, [(timedelta(seconds=t), (sym, px)) for t, sym, px in ticks])
    rcsp.dynamic(control, per_symbol)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=8))
    print("\nsub-graphs instantiated at runtime:", sorted(out.keys()))
    for name in sorted(out):
        print(f"  {name}: {[round(v, 2) for _, v in out[name]]}")


if __name__ == "__main__":
    main()
