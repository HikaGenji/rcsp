"""Latency benchmarks for the native GIL-free hot path (see docs/REALTIME.md)."""

from ._rcsp import Engine


def native_latency_benchmark(iters=2000):
    """Measure the native hot-path latency envelope: a Rust producer feeds a
    lock-free ring buffer consumed by a GIL-released loop running a native
    compute per item. Returns a dict of latency percentiles in nanoseconds
    (``min_ns``/``median_ns``/``p90_ns``/``max_ns``/``mean_ns``).

    This is the floor achievable with **no Python on the hot path** — typically
    sub-microsecond — versus the ~tens-of-µs of the Python event-driven loop
    (bounded by GIL hand-off).
    """
    return Engine().bench_native(iters)
