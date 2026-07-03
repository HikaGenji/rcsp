"""Replaying historical data with a pull-style adapter.

Port of CSP's ``examples/04_writing_adapters/e2_pullinput.py``. CSP's PullInput
adapter replays a pre-generated, time-stamped series into the graph in
simulation. rcsp's ``curve`` is exactly that pull adapter, so here we generate a
synthetic price series and replay it — the engine pulls each point at its
timestamp, in order.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


def make_series(start, n):
    """A deterministic synthetic price walk: (timestamp, value) pairs."""
    price = 100.0
    data = []
    for i in range(1, n + 1):
        price += (1 if i % 2 else -1) * 0.25 * i
        data.append((start + timedelta(seconds=i), round(price, 2)))
    return data


@rcsp.node
def returns(px: ts[float]) -> ts[float]:
    s = rcsp.state(prev=None)
    if rcsp.ticked(px):
        prev = s.prev
        s.prev = px.value
        if prev is not None:
            return px.value - prev


@rcsp.graph
def my_graph():
    st = datetime(2020, 1, 1)
    prices = rcsp.curve(float, make_series(st, 8))  # the "pull" replay

    rcsp.print("price", prices)
    rcsp.print("return", returns(prices))
    rcsp.add_graph_output("price", prices)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=10))
    print("\nreplayed", len(out["price"]), "prices:", [v for _, v in out["price"]])


if __name__ == "__main__":
    main()
