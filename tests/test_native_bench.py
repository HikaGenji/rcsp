"""The native GIL-free hot-path benchmark returns sane, sub-millisecond stats."""

import rcsp


def test_native_latency_benchmark_shape():
    stats = rcsp.native_latency_benchmark(1000)
    assert set(stats) == {"iters", "min_ns", "median_ns", "p90_ns", "max_ns", "mean_ns"}
    assert stats["iters"] == 1000
    assert 0 < stats["min_ns"] <= stats["median_ns"] <= stats["p90_ns"] <= stats["max_ns"]


def test_native_path_is_low_latency():
    stats = rcsp.native_latency_benchmark(2000)
    # No Python/GIL on the hot path → median far under a microsecond in practice;
    # assert a generous ceiling (50µs) so it's robust on loaded CI while still
    # proving it's orders of magnitude below the ~1ms Python floor.
    assert stats["median_ns"] < 50_000
