"""NumPy / Polars interop.

rcsp edges carry arbitrary Python objects, so NumPy arrays flow through nodes
with no special handling — a node can ``return np.mean(window)`` or emit an
array. These helpers bridge to Polars for offline analysis of run results and
build inputs from frames.
"""

from ._baselib import curve
from ._node import _dt_to_ns


def to_polars(result):
    """Convert a :func:`rcsp.run` result to a long-format Polars DataFrame with
    columns ``time``, ``name``, ``value`` (best for scalar series)."""
    import polars as pl

    rows = []
    for name, series in result.items():
        for t, v in series:
            rows.append({"time": t, "name": name, "value": v})
    return pl.DataFrame(rows)


def to_polars_wide(result):
    """Convert a :func:`rcsp.run` result to a wide Polars DataFrame: one column
    per output name, outer-joined on ``time`` (missing ticks are null)."""
    import polars as pl

    frame = None
    for name, series in result.items():
        df = pl.DataFrame(
            {"time": [t for t, _ in series], name: [v for _, v in series]}
        )
        frame = df if frame is None else frame.join(df, on="time", how="full", coalesce=True)
    if frame is None:
        return pl.DataFrame({"time": []})
    return frame.sort("time")


def curve_from_frame(frame, value_col, time_col="time"):
    """Build a ``curve`` edge from a Polars DataFrame's ``time``/value columns."""
    data = list(zip(frame[time_col].to_list(), frame[value_col].to_list()))
    return curve(object, data)
