"""Passing DataFrames to node functions.

rcsp edges carry arbitrary Python objects, so Polars (or pandas) DataFrames flow
through the graph with no special handling. Two patterns:

1. A *static* reference DataFrame passed as a scalar node parameter — captured
   once at graph-build time and reused every tick (great for lookup/enrichment).
2. A DataFrame flowing *on an edge* as a time-series value — sourced with
   ``rcsp.const`` / ``rcsp.curve(object, ...)``, consumed with ``d.value`` or via
   ``rcsp.apply`` to run a DataFrame operation without writing a full node.

Note: native Edge arithmetic (``edge_a + edge_b``) is numeric-only. Do DataFrame
math inside a ``@node`` body or with ``rcsp.apply`` — Edge ``+``/``-`` on a
DataFrame raises a clear error.
"""

from datetime import datetime, timedelta

import polars as pl

import rcsp
from rcsp import ts

# A static reference table, passed to the node as a scalar parameter.
REFERENCE = pl.DataFrame({"symbol": ["AAPL", "MSFT", "GOOG"], "multiplier": [1.5, 2.0, 0.5]})


@rcsp.node
def enrich(trade: ts[object], table=REFERENCE) -> ts[float]:
    """Look the symbol up in the reference DataFrame and scale the quantity."""
    if rcsp.ticked(trade):
        symbol, qty = trade.value
        multiplier = table.filter(pl.col("symbol") == symbol)["multiplier"][0]
        return qty * multiplier


@rcsp.node
def describe(frame: ts[object]) -> ts[str]:
    """A whole DataFrame arrives on an edge; read it with ``.value``."""
    if rcsp.ticked(frame):
        df = frame.value
        return f"{df.height} rows, columns={df.columns}"


@rcsp.graph
def my_graph():
    trades = rcsp.curve(
        object,
        [
            (timedelta(seconds=1), ("AAPL", 10)),
            (timedelta(seconds=2), ("MSFT", 5)),
            (timedelta(seconds=3), ("GOOG", 20)),
        ],
    )
    rcsp.print("notional", enrich(trades))
    rcsp.add_graph_output("notional", enrich(trades))

    # A DataFrame batch as a single edge value.
    batch = rcsp.const(pl.DataFrame({"px": [100.0, 101.0, 102.0, 99.0]}))
    rcsp.print("batch", describe(batch))
    rcsp.add_graph_output("mean_px", rcsp.apply(lambda df: df["px"].mean(), batch))


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=4))
    print("\nnotional series:", [v for _, v in out["notional"]])
    print("mean px (via apply):", [v for _, v in out["mean_px"]])


if __name__ == "__main__":
    main()
