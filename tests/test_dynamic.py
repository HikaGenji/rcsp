"""Tests for dynamic graphs (runtime sub-graph instantiation)."""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _vals(rows):
    return [v for _, v in rows]


@rcsp.node
def _running_sum(px: ts[object]) -> ts[float]:
    s = rcsp.state(total=0.0)
    if rcsp.ticked(px):
        s.total += float(px.value)
        return s.total


def test_dynamic_spawns_per_key_subgraphs():
    def factory(key, value_stream):
        rcsp.add_graph_output(f"{key}_sum", _running_sum(value_stream))
        rcsp.add_graph_output(f"{key}_raw", value_stream)

    @rcsp.graph
    def g():
        control = rcsp.curve(
            object,
            [
                (timedelta(seconds=1), ("AAPL", 10.0)),
                (timedelta(seconds=2), ("MSFT", 100.0)),
                (timedelta(seconds=3), ("AAPL", 20.0)),
                (timedelta(seconds=4), ("MSFT", 200.0)),
                (timedelta(seconds=5), ("AAPL", 30.0)),
            ],
        )
        rcsp.dynamic(control, factory)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))

    assert set(out) == {"AAPL_sum", "AAPL_raw", "MSFT_sum", "MSFT_raw"}
    # Each sub-graph starts the tick AFTER the one that spawned it, so the
    # first appearance (AAPL@1, MSFT@2) is not processed by its sub-graph.
    assert _vals(out["AAPL_raw"]) == [20.0, 30.0]
    assert _vals(out["AAPL_sum"]) == [20.0, 50.0]
    assert _vals(out["MSFT_raw"]) == [200.0]
    assert _vals(out["MSFT_sum"]) == [200.0]


def test_dynamic_bare_keys():
    # control ticks bare keys; sub-graph counts appearances of its key.
    def factory(key, stream):
        rcsp.add_graph_output(f"{key}_n", rcsp.count(stream))

    @rcsp.graph
    def g():
        control = rcsp.curve(
            object,
            [
                (timedelta(seconds=1), "A"),
                (timedelta(seconds=2), "A"),
                (timedelta(seconds=3), "B"),
                (timedelta(seconds=4), "A"),
                (timedelta(seconds=5), "B"),
            ],
        )
        rcsp.dynamic(control, factory)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))
    # A spawned @1 → counts appearances at 2,4 ; B spawned @3 → counts at 5
    assert _vals(out["A_n"]) == [1, 2]
    assert _vals(out["B_n"]) == [1]


def test_static_graph_unaffected():
    # A graph with no dynamics still runs via the normal path.
    @rcsp.graph
    def g():
        rcsp.add_graph_output("c", rcsp.count(rcsp.timer(timedelta(seconds=1), 1)))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _vals(out["c"]) == [1, 2, 3]
