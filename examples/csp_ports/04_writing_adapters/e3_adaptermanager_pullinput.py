"""An adapter manager fanning one source out to many symbol streams.

Port of CSP's ``examples/04_writing_adapters/e3_adaptermanager_pullinput.py``.
A single :class:`rcsp.ReplayAdapterManager` holds all ``(time, symbol, price)``
rows; ``subscribe(symbol)`` returns an independent time series per symbol —
including symbols that tick at the same timestamp.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def pct_change(px: ts[float]) -> ts[float]:
    s = rcsp.state(prev=None)
    if rcsp.ticked(px):
        prev, s.prev = s.prev, px.value
        if prev:
            return (px.value - prev) / prev * 100.0


@rcsp.graph
def my_graph():
    st = datetime(2020, 1, 1)
    rows = [
        (st + timedelta(seconds=1), "AAPL", 150.0),
        (st + timedelta(seconds=1), "MSFT", 300.0),
        (st + timedelta(seconds=2), "AAPL", 151.5),
        (st + timedelta(seconds=2), "MSFT", 298.0),
        (st + timedelta(seconds=3), "AAPL", 149.0),
        (st + timedelta(seconds=3), "MSFT", 305.0),
    ]

    mgr = rcsp.ReplayAdapterManager(rows)
    for symbol in ("AAPL", "MSFT"):
        px = mgr.subscribe(symbol)
        rcsp.print(symbol, px)
        rcsp.add_graph_output(symbol, px)
        rcsp.add_graph_output(f"{symbol}_ret", pct_change(px))


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=4))
    for sym in ("AAPL", "MSFT"):
        print(f"{sym}: {[v for _, v in out[sym]]}  "
              f"returns={[round(v, 2) for _, v in out[f'{sym}_ret']]}")


if __name__ == "__main__":
    main()
