"""Tests for realtime auto-persistence (rcsp.run(persist=...))."""

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import rcsp
from rcsp import ts


def _now():
    return datetime.now(timezone.utc)


@rcsp.node
def _scaled(c: ts[int]) -> ts[float]:
    if rcsp.ticked(c):
        return c.value * 1.5


def _audit_graph():
    @rcsp.graph
    def g():
        n = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        _ = n * 2          # binop node
        rcsp.add_graph_output("scaled", _scaled(n))

    return g


def test_persist_jsonl_captures_every_node(tmp_path):
    path = str(tmp_path / "audit.jsonl")
    rcsp.run(_audit_graph(), starttime=_now(), endtime=timedelta(seconds=2),
             realtime=True, persist=path)

    rows = [json.loads(line) for line in open(path)]
    nodes = {r["node"] for r in rows}
    # producer nodes: timer, count, the * binop, the user _scaled node, const(2)
    kinds = {n.split("#")[0] for n in nodes}
    assert {"timer", "count", "*", "_scaled", "const"} <= kinds
    # print/graph_output sinks have no output edge → never persisted
    assert not any(n.startswith("print") or n.startswith("graph_output") for n in nodes)
    # values are present and stringified
    assert all(set(r) == {"time", "node", "value"} for r in rows)


def test_persist_csv_has_header_and_rows(tmp_path):
    path = str(tmp_path / "audit.csv")
    rcsp.run(_audit_graph(), starttime=_now(), endtime=timedelta(seconds=2),
             realtime=True, persist=path)

    lines = open(path).read().splitlines()
    assert lines[0] == "time,node,value"
    assert len(lines) > 1
    assert all(len(line.split(",")) >= 3 for line in lines[1:])


def test_persist_streams_live_during_run(tmp_path):
    path = str(tmp_path / "live.jsonl")

    @rcsp.graph
    def g():
        rcsp.add_graph_output("c", rcsp.count(rcsp.timer(timedelta(seconds=1), 1)))

    sizes = []

    def poll():
        for _ in range(5):
            time.sleep(0.5)
            sizes.append(os.path.getsize(path) if os.path.exists(path) else 0)

    t = threading.Thread(target=poll, daemon=True)
    t.start()
    rcsp.run(g, starttime=_now(), endtime=timedelta(seconds=3), realtime=True, persist=path)
    t.join()

    # file exists and grew before the run ended (rows written live, not at stop)
    assert sizes[-1] > 0
    assert any(0 < s < sizes[-1] for s in sizes)


def test_persist_rejects_parquet(tmp_path):
    @rcsp.graph
    def g():
        rcsp.add_graph_output("c", rcsp.count(rcsp.timer(timedelta(seconds=1), 1)))

    with pytest.raises(ValueError, match="csv or .jsonl"):
        rcsp.run(g, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=1),
                 persist=str(tmp_path / "x.parquet"))


def test_persist_rejects_dynamic(tmp_path):
    @rcsp.graph
    def g():
        ctrl = rcsp.curve(object, [(timedelta(seconds=1), ("A", 1))])
        rcsp.dynamic(ctrl, lambda key, stream: rcsp.add_graph_output(key, stream))

    with pytest.raises(NotImplementedError, match="dynamic graphs"):
        rcsp.run(g, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=2),
                 persist=str(tmp_path / "y.jsonl"))
