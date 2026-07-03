"""Tests for the rolling-window stats library."""

import statistics
from datetime import datetime, timedelta

import rcsp
from rcsp import stats, ts

ST = datetime(2020, 1, 1)


def _vals(rows):
    return [v for _, v in rows]


def _run(build_outputs, seconds=5):
    @rcsp.graph
    def g():
        x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1)) * 1.0  # 1.0 .. seconds
        for name, edge in build_outputs(x).items():
            rcsp.add_graph_output(name, edge)

    return rcsp.run(g, starttime=ST, endtime=timedelta(seconds=seconds))


def test_rolling_sum_and_mean_tick_window():
    out = _run(lambda x: {"sum": stats.sum(x, 3), "mean": stats.mean(x, 3)})
    assert _vals(out["sum"]) == [1.0, 3.0, 6.0, 9.0, 12.0]
    assert _vals(out["mean"]) == [1.0, 1.5, 2.0, 3.0, 4.0]


def test_rolling_min_max_count():
    out = _run(lambda x: {
        "min": stats.min(x, 3),
        "max": stats.max(x, 3),
        "count": stats.count(x, 3),
    })
    assert _vals(out["min"]) == [1.0, 1.0, 1.0, 2.0, 3.0]
    assert _vals(out["max"]) == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _vals(out["count"]) == [1.0, 2.0, 3.0, 3.0, 3.0]


def test_rolling_stddev_matches_stdlib():
    out = _run(lambda x: {"sd": stats.stddev(x, 3)})
    sd = _vals(out["sd"])
    # trailing 3-tick sample stddev; last window is [3,4,5]
    assert round(sd[-1], 6) == round(statistics.stdev([3.0, 4.0, 5.0]), 6)
    assert sd[0] == 0.0  # single point


def test_time_window():
    out = _run(lambda x: {"m": stats.mean(x, timedelta(seconds=2.5))})
    # 2.5s window at 1s cadence keeps the last 3 points
    assert _vals(out["m"]) == [1.0, 1.5, 2.0, 3.0, 4.0]


def test_min_data_points_suppresses_until_ready():
    out = _run(lambda x: {"m": stats.mean(x, 3, min_data_points=3)})
    # only emits once 3 points are in the window → from the 3rd tick on
    assert _vals(out["m"]) == [2.0, 3.0, 4.0]


def test_median():
    out = _run(lambda x: {"med": stats.median(x, 3)})
    # windows: [1],[1,2],[1,2,3],[2,3,4],[3,4,5]
    assert _vals(out["med"]) == [1.0, 1.5, 2.0, 3.0, 4.0]
