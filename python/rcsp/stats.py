"""Rolling-window statistics (a slice of ``csp.stats``).

Each function returns a time series that ticks whenever its input ticks,
carrying the statistic over a trailing window. The window is either:

* an ``int`` — the last N ticks, or
* a :class:`~datetime.timedelta` — all ticks within the trailing time span.

Example::

    import rcsp
    from rcsp import stats, ts

    prices = rcsp.curve(float, [...])
    avg = stats.mean(prices, timedelta(seconds=10))     # 10-second rolling mean
    sd = stats.stddev(prices, 20)                        # 20-tick rolling stddev
"""

import builtins
from collections import deque
from datetime import timedelta

from ._node import node, now, state, ticked
from ._types import ts

# The public names `sum`/`min`/`max` below shadow the builtins, so bind the
# builtins we need for the reductions up front.
_sum = builtins.sum
_min = builtins.min
_max = builtins.max


def _reduce(kind, vals, ddof):
    n = len(vals)
    if kind == "count":
        return float(n)
    if kind == "sum":
        return float(_sum(vals))
    if kind == "mean":
        return _sum(vals) / n
    if kind == "min":
        return float(_min(vals))
    if kind == "max":
        return float(_max(vals))
    if kind == "first":
        return float(vals[0])
    if kind == "last":
        return float(vals[-1])
    if kind == "prod":
        p = 1.0
        for v in vals:
            p *= v
        return p
    if kind in ("var", "stddev"):
        denom = n - ddof
        if denom <= 0:
            return 0.0
        m = _sum(vals) / n
        var = _sum((v - m) ** 2 for v in vals) / denom
        return var if kind == "var" else var ** 0.5
    if kind == "median":
        sv = sorted(vals)
        mid = n // 2
        return float(sv[mid]) if n % 2 else (sv[mid - 1] + sv[mid]) / 2.0
    raise ValueError(f"unknown stat {kind}")


@node
def _rolling(x: ts[object], window, count_mode, kind, ddof, min_points) -> ts[float]:
    s = state()
    if not hasattr(s, "buf"):
        s.buf = deque()
    if ticked(x):
        buf = s.buf
        t = now()
        buf.append((t, float(x.value)))
        if count_mode:
            while len(buf) > window:
                buf.popleft()
        else:
            while buf and (t - buf[0][0]) > window:
                buf.popleft()
        vals = [v for _, v in buf]
        if len(vals) < min_points:
            return None
        return _reduce(kind, vals, ddof)


def _win(interval):
    """(window, count_mode) from an int (ticks) or timedelta (time)."""
    if isinstance(interval, timedelta):
        return interval, False
    return int(interval), True


def _stat(kind):
    def fn(x, interval, min_data_points=1):
        w, count_mode = _win(interval)
        return _rolling(x, w, count_mode, kind, 0, min_data_points)

    fn.__name__ = kind
    fn.__doc__ = f"Rolling {kind} of ``x`` over ``interval`` (ticks or time)."
    return fn


count = _stat("count")
sum = _stat("sum")
mean = _stat("mean")
min = _stat("min")
max = _stat("max")
first = _stat("first")
last = _stat("last")
prod = _stat("prod")
median = _stat("median")


def var(x, interval, ddof=1, min_data_points=1):
    """Rolling variance of ``x`` (``ddof=1`` → sample variance)."""
    w, count_mode = _win(interval)
    return _rolling(x, w, count_mode, "var", ddof, min_data_points)


def stddev(x, interval, ddof=1, min_data_points=1):
    """Rolling standard deviation of ``x`` (``ddof=1`` → sample stddev)."""
    w, count_mode = _win(interval)
    return _rolling(x, w, count_mode, "stddev", ddof, min_data_points)


__all__ = [
    "count", "sum", "mean", "min", "max", "first", "last",
    "prod", "median", "var", "stddev",
]
