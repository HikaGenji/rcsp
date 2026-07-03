"""End-to-end tests for the rcsp engine and Python API."""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts

ST = datetime(2020, 1, 1)


def _values(rows):
    return [v for _, v in rows]


def _seconds(rows):
    return [(t.second, v) for t, v in rows]


def test_const_add():
    @rcsp.node
    def add(x: ts[int], y: ts[int]) -> ts[int]:
        if rcsp.ticked(x, y) and rcsp.valid(x, y):
            return x + y

    @rcsp.graph
    def g():
        rcsp.add_graph_output("s", add(rcsp.const(1), rcsp.const(2)))

    out = rcsp.run(g, starttime=ST)
    assert _values(out["s"]) == [3]


def test_timer_and_count():
    @rcsp.graph
    def g():
        beat = rcsp.timer(timedelta(seconds=1), 1)
        rcsp.add_graph_output("c", rcsp.count(beat))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=5))
    assert _values(out["c"]) == [1, 2, 3, 4, 5]


def test_stateful_running_sum():
    @rcsp.node
    def running_sum(x: ts[float]) -> ts[float]:
        s = rcsp.state(total=0.0)
        if rcsp.ticked(x):
            s.total += x
            return s.total

    @rcsp.graph
    def g():
        rcsp.add_graph_output("sum", running_sum(rcsp.timer(timedelta(seconds=1), 2.0)))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=4))
    assert _values(out["sum"]) == [2.0, 4.0, 6.0, 8.0]


def test_native_arithmetic_and_filter():
    @rcsp.graph
    def g():
        c = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))  # 1..5
        doubled = c * 2
        big = rcsp.filter(c > 2, doubled)
        rcsp.add_graph_output("doubled", doubled)
        rcsp.add_graph_output("big", big)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=5))
    assert _values(out["doubled"]) == [2, 4, 6, 8, 10]
    assert _values(out["big"]) == [6, 8, 10]


def test_delay():
    @rcsp.graph
    def g():
        c = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        rcsp.add_graph_output("d", rcsp.delay(c, timedelta(seconds=2)))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=5))
    # c ticks 1..5 at seconds 1..5; delayed by 2 → seconds 3,4,5 with values 1,2,3
    assert _seconds(out["d"]) == [(3, 1), (4, 2), (5, 3)]


def test_curve_adapter():
    @rcsp.graph
    def g():
        cv = rcsp.curve(
            float,
            [
                (timedelta(seconds=1), 10.0),
                (timedelta(seconds=2), 20.0),
                (timedelta(seconds=3), 30.0),
            ],
        )
        rcsp.add_graph_output("cv", cv)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=5))
    assert _seconds(out["cv"]) == [(1, 10.0), (2, 20.0), (3, 30.0)]


def test_diamond_is_glitch_free():
    """A diamond must produce exactly one, consistent tick per source cycle."""
    seen = []

    @rcsp.node
    def record(x: ts[float]) -> ts[float]:
        if rcsp.ticked(x):
            seen.append(x.value)
            return x.value

    @rcsp.graph
    def g():
        t = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))  # 1,2,3
        a = t + 1
        b = t * 2
        c = a + b  # 4,7,10
        rcsp.add_graph_output("c", record(c))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _values(out["c"]) == [4, 7, 10]
    assert seen == [4, 7, 10]  # one tick each, no intermediate glitches


def test_multi_output_node():
    @rcsp.node
    def split_sign(x: ts[float]) -> rcsp.Outputs(pos=ts[float], neg=ts[float]):
        if rcsp.ticked(x):
            if x.value >= 0:
                rcsp.output(pos=x.value)
            else:
                rcsp.output(neg=x.value)

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
        r = split_sign(cv)
        rcsp.add_graph_output("pos", r.pos)
        rcsp.add_graph_output("neg", r.neg)

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=3))
    assert _values(out["pos"]) == [5.0, 7.0]
    assert _values(out["neg"]) == [-3.0]


def test_custom_alarm_node():
    @rcsp.node(alarms=["tick"])
    def heartbeat(period_s: float) -> ts[int]:
        s = rcsp.state(n=0)
        if rcsp.starting():
            rcsp.schedule_alarm("tick", timedelta(seconds=period_s), True)
        if rcsp.alarmed("tick"):
            s.n += 1
            rcsp.schedule_alarm("tick", timedelta(seconds=period_s), True)
            return s.n

    @rcsp.graph
    def g():
        rcsp.add_graph_output("hb", heartbeat(1.5))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))
    assert _seconds(out["hb"]) == [(1, 1), (3, 2), (4, 3), (6, 4)]


def test_sample_and_merge():
    @rcsp.graph
    def g():
        fast = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        trig = rcsp.timer(timedelta(seconds=2), True)
        rcsp.add_graph_output("sampled", rcsp.sample(trig, fast))
        a = rcsp.timer(timedelta(seconds=2), 100)
        b = rcsp.timer(timedelta(seconds=3), 200)
        rcsp.add_graph_output("merged", rcsp.merge(a, b))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))
    # trigger at 2,4,6 samples the counter's current value
    assert _seconds(out["sampled"]) == [(2, 2), (4, 4), (6, 6)]
    # a at 2,4,6 ; b at 3,6 ; at t=6 both tick → a wins
    assert _seconds(out["merged"]) == [(2, 100), (3, 200), (4, 100), (6, 100)]


def test_firstN():
    @rcsp.graph
    def g():
        c = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        rcsp.add_graph_output("first3", rcsp.firstN(c, 3))

    out = rcsp.run(g, starttime=ST, endtime=timedelta(seconds=6))
    assert _values(out["first3"]) == [1, 2, 3]
