"""Ticking time series and stateful accumulation.

Port of CSP's ``examples/01_basics/e2_ticking.py``. Two ``curve`` adapters
replay integer series at different times; ``add`` sums them (ticking whenever
either input ticks) and ``accum`` keeps a running total in node state.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def add(x: ts[int], y: ts[int]) -> ts[int]:
    if rcsp.valid(x, y):
        return x + y


@rcsp.node
def accum(val: ts[int]) -> ts[int]:
    s = rcsp.state(total=0)
    if rcsp.ticked(val):
        s.total += val
        return s.total


@rcsp.graph
def my_graph():
    st = datetime(2020, 1, 1)

    x = rcsp.curve(int, [(st + timedelta(1), 1), (st + timedelta(2), 2), (st + timedelta(3), 3)])
    y = rcsp.curve(int, [(st + timedelta(1), -1), (st + timedelta(3), -1), (st + timedelta(4), -1)])

    total = add(x, y)
    acc = accum(total)

    rcsp.print("x", x)
    rcsp.print("y", y)
    rcsp.print("sum", total)
    rcsp.print("accum", acc)

    rcsp.add_graph_output("accum", acc)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(days=5))
    print("\naccum series:", [v for _, v in out["accum"]])


if __name__ == "__main__":
    main()
