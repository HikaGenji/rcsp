"""Tests for feedback, split, and the realtime push adapter."""

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _values(rows):
    return [v for _, v in rows]


def test_split_routes_by_flag():
    @rcsp.graph
    def g():
        cv = rcsp.curve(
            float,
            [
                (timedelta(seconds=1), 5.0),
                (timedelta(seconds=2), -3.0),
                (timedelta(seconds=3), 7.0),
            ],
        )
        sides = rcsp.split(cv > 0, cv)
        rcsp.add_graph_output("pos", sides.true)
        rcsp.add_graph_output("neg", sides.false)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _values(out["pos"]) == [5.0, 7.0]
    assert _values(out["neg"]) == [-3.0]


def test_feedback_loop_terminates_and_advances_time():
    @dataclass
    class Order:
        id: int

    @dataclass
    class Report:
        id: int

    @rcsp.node(alarms=["ack"])
    def exchange(order: ts[object]) -> ts[object]:
        if rcsp.ticked(order):
            rcsp.schedule_alarm("ack", timedelta(seconds=0.7), order.value)
        if rcsp.alarmed("ack"):
            return Report(id=rcsp.alarm_value("ack").id)

    @rcsp.node(alarms=["send"])
    def algo(report: ts[object]) -> ts[object]:
        s = rcsp.state(n=0)
        if rcsp.starting():
            rcsp.schedule_alarm("send", timedelta(seconds=1), True)
        if rcsp.ticked(report):
            rcsp.schedule_alarm("send", timedelta(seconds=1), True)
        if rcsp.alarmed("send"):
            s.n += 1
            return Order(id=s.n)

    @rcsp.graph
    def g():
        fb = rcsp.feedback(object)
        orders = algo(fb.out())
        reports = exchange(orders)
        fb.bind(reports)
        rcsp.add_graph_output("orders", orders)
        rcsp.add_graph_output("reports", reports)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))
    # order1@1.0 -> ack@1.7 -> order2@2.7 -> ack@3.4 -> order3@4.4 -> ack@5.1
    assert [o.id for o in _values(out["orders"])] == [1, 2, 3]
    assert [r.id for r in _values(out["reports"])] == [1, 2, 3]


def test_feedback_without_advancing_time_does_not_hang():
    # A feedback that ticks in the same timestamp must resolve in a follow-up
    # cycle (per-cycle `ticked`), not loop forever.
    @rcsp.node
    def gate(fb: ts[int]) -> ts[int]:
        s = rcsp.state(done=False)
        if rcsp.starting():
            return 1
        # do NOT re-emit on feedback → loop must settle
        return None

    @rcsp.graph
    def g():
        fb = rcsp.feedback(int)
        out = gate(fb.out())
        fb.bind(out)
        rcsp.add_graph_output("out", out)

    # gate has no alarms so it won't run_at_start; emit via a const trigger.
    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=1))
    # Just needs to terminate; no assertion on values beyond not hanging.
    assert "out" in out


def test_push_adapter_realtime():
    received = []

    class Driver(threading.Thread):
        def __init__(self, adapter):
            super().__init__(daemon=True)
            self._adapter = adapter
            self._running = True

        def run(self):
            self._adapter.wait_for_start(timeout=2.0)
            i = 0
            while self._running:
                i += 1
                self._adapter.push_tick(i)
                time.sleep(0.1)

        def stop(self):
            self._running = False

    @rcsp.graph
    def g():
        adapter = rcsp.GenericPushAdapter(int)
        driver = Driver(adapter)
        driver.start()
        rcsp.add_graph_output("pushed", adapter.out())
        rcsp.schedule_on_engine_stop(driver.stop)

    out = rcsp.run(
        g,
        starttime=datetime.now(timezone.utc),
        endtime=timedelta(seconds=0.6),
        realtime=True,
    )
    vals = _values(out["pushed"])
    # ~6 ticks in 0.6s at 0.1s cadence; allow slack for scheduling jitter.
    assert len(vals) >= 3
    assert vals == list(range(1, len(vals) + 1))
