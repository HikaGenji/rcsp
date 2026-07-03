"""Tests for profiling, NumPy/Polars interop, file I/O adapters, and managers."""

from datetime import datetime, timedelta

import pytest

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _vals(rows):
    return [v for _, v in rows]


@rcsp.node
def _busy(x: ts[int]) -> ts[int]:
    if rcsp.ticked(x):
        acc = 0
        for i in range(200):
            acc += i
        return x + acc


def test_profiler_collects_per_node_stats():
    @rcsp.graph
    def g():
        t = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        rcsp.add_graph_output("b", _busy(t))

    with rcsp.profiler.Profiler() as p:
        rcsp.run(g, starttime=ST, endtime=timedelta(seconds=10))

    info = p.results()
    assert info is not None
    names = {n.name for n in info.nodes}
    assert "_busy" in names and "timer" in names
    busy = next(n for n in info.nodes if n.name == "_busy")
    assert busy.count == 10          # ran once per timer tick
    assert busy.total_ns > 0
    assert info.total_executions >= 30


def test_profiler_absent_without_context():
    # A normal run must not require the profiler and returns outputs as usual.
    @rcsp.graph
    def g():
        rcsp.add_graph_output("c", rcsp.count(rcsp.timer(timedelta(seconds=1), 1)))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _vals(out["c"]) == [1, 2, 3]


def test_numpy_values_flow_through_edges():
    np = pytest.importorskip("numpy")

    @rcsp.node
    def vecsum(x: ts[object]) -> ts[float]:
        if rcsp.ticked(x):
            return float(np.sum(x.value))

    @rcsp.graph
    def g():
        arr = rcsp.curve(object, [(timedelta(seconds=1), np.array([1.0, 2.0, 3.0]))])
        rcsp.add_graph_output("s", vecsum(arr))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=2))
    assert _vals(out["s"]) == [6.0]


def test_to_polars():
    pytest.importorskip("polars")

    @rcsp.graph
    def g():
        x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1)) * 1.0
        rcsp.add_graph_output("x", x)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    df = rcsp.to_polars(out)
    assert df.columns == ["time", "name", "value"]
    assert df.height == 3
    wide = rcsp.to_polars_wide(out)
    assert "x" in wide.columns


def test_parquet_write_then_read(tmp_path):
    pytest.importorskip("polars")
    path = str(tmp_path / "out.parquet")

    @rcsp.graph
    def gw():
        x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1)) * 1.0
        rcsp.write_parquet(path, price=x)

    rcsp.run(gw, starttime=ST, endtime=timedelta(seconds=4))

    import polars as pl
    df = pl.read_parquet(path)
    assert df.height == 4
    assert set(df.columns) == {"time", "price"}   # wide: one column per series

    @rcsp.graph
    def gr():
        cols = rcsp.read_parquet(path, value_cols=["price"])
        rcsp.add_graph_output("replayed", cols["price"])

    out = rcsp.run(gr, starttime=ST, endtime=timedelta(seconds=6))
    assert _vals(out["replayed"]) == [1.0, 2.0, 3.0, 4.0]


def test_adapter_manager_splits_by_key():
    rows = [
        (ST + timedelta(seconds=1), "AAPL", 150.0),
        (ST + timedelta(seconds=1), "MSFT", 300.0),   # same timestamp, different key
        (ST + timedelta(seconds=2), "AAPL", 151.0),
        (ST + timedelta(seconds=3), "MSFT", 301.0),
    ]

    @rcsp.graph
    def g():
        mgr = rcsp.ReplayAdapterManager(rows)
        rcsp.add_graph_output("AAPL", mgr.subscribe("AAPL"))
        rcsp.add_graph_output("MSFT", mgr.subscribe("MSFT"))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=5))
    assert _vals(out["AAPL"]) == [150.0, 151.0]
    assert _vals(out["MSFT"]) == [300.0, 301.0]
