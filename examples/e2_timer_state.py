"""Timers, stateful nodes and native baselib ops.

A timer drives a stateful running-sum node and a native ``count`` kernel, then
we print both streams as they tick through simulated time.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def running_sum(x: ts[float]) -> ts[float]:
    # State persists across ticks; initialised once via keyword defaults.
    s = rcsp.state(total=0.0)
    if rcsp.ticked(x):
        s.total += x
        return s.total


@rcsp.graph
def my_graph():
    beat = rcsp.timer(timedelta(seconds=1), 2.0)   # ticks 2.0 every second

    total = running_sum(beat)
    n = rcsp.count(beat)                             # native kernel

    rcsp.print("tick", beat)
    rcsp.print("running_sum", total)
    rcsp.print("count", n)

    rcsp.add_graph_output("running_sum", total)
    rcsp.add_graph_output("count", n)


def main():
    out = rcsp.run(
        my_graph,
        starttime=datetime(2020, 1, 1),
        endtime=timedelta(seconds=5),
    )
    print("\nfinal running_sum series:", [v for _, v in out["running_sum"]])


if __name__ == "__main__":
    main()
