"""The producer-capability probe reports throughput and pacing jitter."""

import rcsp


def test_producer_benchmark_throughput():
    r = rcsp.producer_benchmark(duration=0.2)
    assert {"max_rate_per_s", "pushed", "delivered", "kept_up"} <= set(r)
    assert r["pushed"] > 0
    assert r["max_rate_per_s"] > 1000        # any real machine clears this easily
    # the engine drained everything the producer pushed
    assert r["delivered"] == r["pushed"]
    assert r["kept_up"] is True


def test_producer_benchmark_pacing():
    r = rcsp.producer_benchmark(duration=0.2, target_rate=2000)
    assert "pacing_median_us" in r and "pacing_p99_us" in r
    # aiming at 2000/s → ~500µs spacing; median should be in that ballpark
    assert 200 < r["pacing_median_us"] < 5000
    assert r["pacing_p99_us"] >= r["pacing_median_us"]
