"""SPY-vs-basket arbitrage example: correctness + profiling/latency sanity.

Covers the importable module (the notebook is a thin plotting shell over it and
is validated locally, not in CI).
"""

import os
import sys
from datetime import timedelta

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "examples", "spy_basket")))
import basket_arb as mod  # noqa: E402


def test_weights_sum_to_one():
    w = mod.load_weights()
    assert 400 <= len(w) < 600
    assert abs(sum(w.values()) - 1.0) < 1e-6      # bundled CSV is 8-dp rounded
    assert all(v > 0 for v in w.values())


def test_feed_timestamps_strictly_increasing():
    feed = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10, duration=0.2)
    times = [t for t, _ in feed.rows]
    assert times == sorted(times)
    assert len(set(times)) == len(times)          # all distinct → no single-edge collision
    assert feed.n_quotes == feed.n_constituent_quotes + feed.n_spy_quotes


def test_rate_guard_raises():
    # 1000 stocks * 2000 q/s over 0.1s ≈ 2e5 quotes in 0.1s = 2e6/s > 1e6 ceiling
    with pytest.raises(ValueError):
        mod.synth_feed(mod.load_weights(), n_stocks=400, quotes_per_sec_per_stock=3000, duration=0.1)


def test_fair_matches_hand_computation():
    # A tiny hand-built feed: two constituents tick, then one SPY quote.
    # idx = 0.6*(110/100) + 0.4*(55/50) = 0.66 + 0.44 = 1.10 ; fair = 450*1.10 = 495.
    opens = {"A": 100.0, "B": 50.0}
    weights = {"A": 0.6, "B": 0.4}
    rows = [
        (timedelta(microseconds=1), mod.Quote("A", 110.0)),
        (timedelta(microseconds=2), mod.Quote("B", 55.0)),
        (timedelta(microseconds=3), mod.Quote("SPY", 400.0)),
    ]
    feed = mod.Feed(rows=rows, opens=opens, weights=weights, spy_open=450.0,
                    margin=0.1, n_constituent_quotes=2, n_spy_quotes=1, step_us=1)
    res = mod.simulate(feed)
    assert res["fair"][-1][1] == pytest.approx(495.0, abs=1e-9)
    assert res["edge"][-1][1] == pytest.approx(95.0, abs=1e-9)   # 495 - 400


def test_injected_mispricing_triggers_buy():
    feed = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10, duration=0.3)
    res = mod.simulate(feed)
    signals = [v for _, v in res["signal"]]
    assert 1 in signals               # a BUY signal fired (SPY cheap vs basket)
    assert res["n_trades"] > 0
    assert max(v for _, v in res["position"]) >= 1


def test_flat_market_no_trades():
    # No dislocations and no SPY noise → edge ~ 0 → never breaches the margin.
    feed = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10,
                          duration=0.2, dislocations=[], spy_noise=0.0)
    res = mod.simulate(feed)
    assert res["n_trades"] == 0
    assert all(v == 0 for _, v in res["signal"])


def test_profile_run_sane():
    feed = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10, duration=0.2)
    prof = mod.profile_run(feed)
    assert prof["per_node"]["basket_engine"]["count"] == feed.n_quotes
    assert prof["throughput_qps"] > 0


def test_realtime_latency_sane():
    lat = mod.realtime_latency(mod.load_weights(), n_stocks=5, bursts=50, period=0.001)
    assert lat["n_samples"] > 0
    assert lat["median_us"] > 0
    assert lat["p90_us"] >= lat["median_us"]
    assert lat["median_us"] < 50_000        # loose upper bound for loaded CI


def test_producer_ceiling_sane():
    prod = mod.producer_ceiling(target_rate=20000, duration=0.2)
    assert prod["max_rate_per_s"] > 1000
    assert "kept_up" in prod


def test_feed_is_deterministic():
    a = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10, duration=0.2, seed=7)
    b = mod.synth_feed(mod.load_weights(), n_stocks=5, quotes_per_sec_per_stock=10, duration=0.2, seed=7)
    assert [(t, q.symbol, q.mid) for t, q in a.rows] == [(t, q.symbol, q.mid) for t, q in b.rows]
