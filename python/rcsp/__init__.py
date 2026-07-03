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
from ._graph import Edge, graph, feedback, Feedback
from ._adapters import GenericPushAdapter, schedule_on_engine_stop
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
    split,
    apply,
)
from ._run import run, add_graph_output
from ._dynamic import dynamic
from ._viz import show_graph, graph_to_dot, graph_to_mermaid
from ._interop import to_polars, to_polars_wide, curve_from_frame
from ._io import (
    read_parquet,
    read_csv,
    write_parquet,
    write_csv,
    read_frame,
    AdapterManager,
    ReplayAdapterManager,
    CsvAdapterManager,
)
from ._kafka import KafkaAdapterManager, InMemoryKafka
from . import stats
from . import _profiler as profiler

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
    "feedback",
    "Feedback",
    "GenericPushAdapter",
    "schedule_on_engine_stop",
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
    "split",
    "apply",
    "run",
    "add_graph_output",
    "dynamic",
    "show_graph",
    "graph_to_dot",
    "graph_to_mermaid",
    "to_polars",
    "to_polars_wide",
    "curve_from_frame",
    "read_parquet",
    "read_csv",
    "write_parquet",
    "write_csv",
    "read_frame",
    "AdapterManager",
    "ReplayAdapterManager",
    "CsvAdapterManager",
    "KafkaAdapterManager",
    "InMemoryKafka",
    "stats",
    "profiler",
]
