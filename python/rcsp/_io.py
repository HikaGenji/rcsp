"""File I/O adapters and a small adapter-manager framework.

* :func:`read_parquet` / :func:`read_csv` — pull adapters that replay a file's
  rows as time series in simulation.
* :func:`write_parquet` / :func:`write_csv` — output adapters that record ticks
  and write them out when the engine stops.
* :class:`AdapterManager` / :class:`ReplayAdapterManager` — coordinate many keyed
  streams from a single source (one file → per-symbol time series).
"""

from ._adapters import schedule_on_engine_stop
from ._baselib import curve
from ._graph import current_builder
from ._node import node, now, state, ticked
from ._types import ts


# --- readers (pull adapters) ---------------------------------------------------

def _read_frame(path):
    import polars as pl

    if str(path).endswith(".csv"):
        return pl.read_csv(path)
    return pl.read_parquet(path)


def read_frame(frame, time_col="time", value_cols=None):
    """Replay an in-memory Polars frame: returns ``{column: edge}``."""
    times = frame[time_col].to_list()
    cols = value_cols or [c for c in frame.columns if c != time_col]
    out = {}
    for c in cols:
        out[c] = curve(object, list(zip(times, frame[c].to_list())))
    return out


def read_parquet(path, time_col="time", value_cols=None):
    """Replay a Parquet file's rows; returns ``{column: edge}``."""
    return read_frame(_read_frame(path), time_col, value_cols)


def read_csv(path, time_col="time", value_cols=None):
    """Replay a CSV file's rows; returns ``{column: edge}``."""
    return read_frame(_read_frame(path), time_col, value_cols)


# --- writers (output adapters) -------------------------------------------------

@node
def _record(x: ts[object], sink) -> None:
    if ticked(x):
        sink.append((now(), x.value))


def _write(path, named_edges, writer):
    # Collect each edge's ticks, then write a wide frame: `time` plus one column
    # per named edge, outer-joined on time (missing ticks are null).
    accs = {name: [] for name in named_edges}
    for name, edge in named_edges.items():
        _record(edge, accs[name])

    def flush():
        import polars as pl

        frame = None
        for name, rows in accs.items():
            df = pl.DataFrame(
                {"time": [t for t, _ in rows], name: [v for _, v in rows]}
            )
            frame = df if frame is None else frame.join(
                df, on="time", how="full", coalesce=True
            )
        if frame is not None:
            writer(frame.sort("time"), path)

    schedule_on_engine_stop(flush)


def write_parquet(path, **named_edges):
    """Write each named edge's ticks to a Parquet file (long format) at stop."""
    _write(path, named_edges, lambda df, p: df.write_parquet(p))


def write_csv(path, **named_edges):
    """Write each named edge's ticks to a CSV file (long format) at stop."""
    _write(path, named_edges, lambda df, p: df.write_csv(p))


# --- adapter managers ----------------------------------------------------------

class AdapterManager:
    """Base for managers that fan a single source out to per-key streams.

    Subclasses populate ``self._groups`` — ``{key: [(time, value), ...]}``.
    :meth:`subscribe` returns the (independently ticking) stream for one key.
    Each key gets its own edge, so keys that tick at the same timestamp don't
    collide (a single edge holds only one value per engine cycle).
    """

    def __init__(self):
        self._groups = {}
        self._edges = {}

    def keys(self):
        return list(self._groups.keys())

    def subscribe(self, key):
        if key not in self._edges:
            self._edges[key] = curve(object, self._groups.get(key, []))
        return self._edges[key]


class ReplayAdapterManager(AdapterManager):
    """Replay ``(time, key, value)`` rows from one source, split per key."""

    def __init__(self, rows):
        super().__init__()
        groups = {}
        for t, k, v in sorted(rows, key=lambda r: r[0]):
            groups.setdefault(k, []).append((t, v))
        self._groups = groups


class CsvAdapterManager(ReplayAdapterManager):
    """A :class:`ReplayAdapterManager` sourced from a CSV/Parquet file."""

    def __init__(self, path, time_col="time", key_col="symbol", value_col="value"):
        frame = _read_frame(path)
        rows = list(
            zip(
                frame[time_col].to_list(),
                frame[key_col].to_list(),
                frame[value_col].to_list(),
            )
        )
        super().__init__(rows)
