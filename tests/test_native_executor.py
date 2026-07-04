"""The native GIL-free executor: correctness, push flow, and the native guard."""

from datetime import datetime, timedelta, timezone

import pytest

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _vals(rows):
    return [v for _, v in rows]


def _native_graph():
    @rcsp.graph
    def g():
        t = rcsp.count(rcsp.timer(timedelta(seconds=0.1), 1))
        a = t + 1
        b = t * 2
        c = a + b
        rcsp.add_graph_output("c", c)
        rcsp.add_graph_output("big", rcsp.filter(t > 2, c))

    return g


def test_native_matches_normal_engine():
    g = _native_graph()
    normal = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=0.35))
    native = rcsp.run(g, starttime=datetime.now(timezone.utc),
                      endtime=timedelta(seconds=0.35), realtime="native")
    # same values, computed by two entirely different execution paths
    assert _vals(native["c"]) == _vals(normal["c"])
    assert _vals(native["big"]) == _vals(normal["big"])
    assert _vals(normal["c"])  # non-empty


def test_native_push_producer():
    import threading
    import time

    class Prod(threading.Thread):
        def __init__(self, a):
            super().__init__(daemon=True)
            self.a = a

        def run(self):
            self.a.wait_for_start(timeout=2)
            for i in range(1, 11):
                time.sleep(0.01)
                self.a.push_tick(i)

    @rcsp.graph
    def g():
        a = rcsp.NativePushAdapter(int)
        x = a.out()
        rcsp.add_graph_output("doubled", x * 2)
        rcsp.add_graph_output("big", rcsp.filter(x > 5, x))
        Prod(a).start()

    out = rcsp.run(g, starttime=datetime.now(timezone.utc),
                   endtime=timedelta(seconds=0.5), realtime="native")
    assert _vals(out["doubled"]) == [2 * i for i in range(1, 11)]
    assert _vals(out["big"]) == [6, 7, 8, 9, 10]


def test_native_rejects_python_node():
    @rcsp.node
    def inc(x: ts[int]) -> ts[int]:
        if rcsp.ticked(x):
            return x.value + 1

    @rcsp.graph
    def g():
        rcsp.add_graph_output("y", inc(rcsp.const(1)))

    with pytest.raises(ValueError, match="native-only"):
        rcsp.run(g, starttime=ST, endtime=timedelta(seconds=0.1), realtime="native")


def test_native_rejects_persist(tmp_path):
    @rcsp.graph
    def g():
        rcsp.add_graph_output("c", rcsp.count(rcsp.timer(timedelta(seconds=0.1), 1)))

    with pytest.raises(NotImplementedError, match="native"):
        rcsp.run(g, starttime=ST, endtime=timedelta(seconds=0.2),
                 realtime="native", persist=str(tmp_path / "a.jsonl"))
