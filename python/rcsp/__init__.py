"""rcsp — a clone of Point72's CSP (Composable Stream Processing) with a Rust
event-graph engine.

Public API mirrors the subset of ``csp`` most graphs use::

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
"""

from ._types import Outputs, TsType, ts
from ._graph import Edge, graph
from ._node import (
    node,
    ticked,
    valid,
    output,
    state,
    starting,
    now,
    schedule_alarm,
    alarmed,
    alarm_value,
)
from ._baselib import (
    const,
    timer,
    sample,
    filter,
    merge,
    count,
    firstN,
    delay,
    print_node,
    curve,
)
from ._run import run, add_graph_output

# csp spells the print adapter ``csp.print``; expose the same name.
print = print_node

__version__ = "0.1.0"

__all__ = [
    "ts",
    "TsType",
    "Outputs",
    "Edge",
    "node",
    "graph",
    "ticked",
    "valid",
    "output",
    "state",
    "starting",
    "now",
    "schedule_alarm",
    "alarmed",
    "alarm_value",
    "const",
    "timer",
    "sample",
    "filter",
    "merge",
    "count",
    "firstN",
    "delay",
    "print",
    "print_node",
    "curve",
    "run",
    "add_graph_output",
]
