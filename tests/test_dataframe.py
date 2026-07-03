"""Passing DataFrames (and arbitrary objects) to node functions."""

from datetime import datetime, timedelta

import pytest

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _vals(rows):
    return [v for _, v in rows]


def test_scalar_dataframe_param_for_lookup():
    pl = pytest.importorskip("polars")
    ref = pl.DataFrame({"symbol": ["AAPL", "MSFT"], "mult": [1.5, 2.0]})

    @rcsp.node
    def enrich(trade: ts[object], table=ref) -> ts[float]:
        if rcsp.ticked(trade):
            sym, qty = trade.value
            return qty * table.filter(pl.col("symbol") == sym)["mult"][0]

    @rcsp.graph
    def g():
        trades = rcsp.curve(
            object,
            [(timedelta(seconds=1), ("AAPL", 10)), (timedelta(seconds=2), ("MSFT", 5))],
        )
        rcsp.add_graph_output("n", enrich(trades))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _vals(out["n"]) == [15.0, 10.0]


def test_dataframe_flows_on_edge():
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"px": [100.0, 101.0, 102.0]})

    @rcsp.node
    def shape(d: ts[object]) -> ts[object]:
        if rcsp.ticked(d):
            # the exact same frame object arrives, by reference
            return (d.value.height, tuple(d.value.columns))

    @rcsp.graph
    def g():
        rcsp.add_graph_output("shape", shape(rcsp.const(df)))

    out = rcsp.run(g, starttime=ST)
    assert _vals(out["shape"]) == [(3, ("px",))]


def test_apply_runs_dataframe_op():
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"px": [10.0, 20.0, 30.0]})

    @rcsp.graph
    def g():
        e = rcsp.const(df)
        rcsp.add_graph_output("mean", rcsp.apply(lambda f: f["px"].mean(), e))
        rcsp.add_graph_output("sum", rcsp.apply(lambda f: f["px"].sum(), e))

    out = rcsp.run(g, starttime=ST)
    assert _vals(out["mean"]) == [20.0]
    assert _vals(out["sum"]) == [60.0]


def test_apply_multiple_edges():
    @rcsp.graph
    def g():
        a = rcsp.const(3)
        b = rcsp.const(4)
        rcsp.add_graph_output("hyp", rcsp.apply(lambda x, y: (x * x + y * y) ** 0.5, a, b))

    out = rcsp.run(g, starttime=ST)
    assert _vals(out["hyp"]) == [5.0]


def test_nonnumeric_edge_arithmetic_raises():
    pl = pytest.importorskip("polars")
    df = pl.DataFrame({"px": [1.0]})

    @rcsp.graph
    def g():
        rcsp.add_graph_output("bad", rcsp.const(df) + rcsp.const(df))

    with pytest.raises(ValueError, match="numeric time-series values"):
        rcsp.run(g, starttime=ST)
