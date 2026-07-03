"""Profiling a graph.

Port of CSP's ``examples/99_developer_tools/e1_profiling.py``. Wrap a run in
``rcsp.profiler.Profiler()`` to collect per-node execution counts and cumulative
time, then print a sorted report.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def heavy(x: ts[int]) -> ts[float]:
    if rcsp.ticked(x):
        acc = 0.0
        for i in range(2000):
            acc += (x.value * i) ** 0.5
        return acc


@rcsp.node
def light(x: ts[int]) -> ts[int]:
    if rcsp.ticked(x):
        return x + 1


@rcsp.graph
def my_graph():
    t = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
    rcsp.add_graph_output("heavy", heavy(t))
    rcsp.add_graph_output("light", light(t))


def main():
    with rcsp.profiler.Profiler() as p:
        rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=200))

    info = p.results()
    print(f"total node executions: {info.total_executions}")
    print(f"total engine time:     {info.total_ns / 1e6:.2f} ms\n")
    info.print_stats()


if __name__ == "__main__":
    main()
