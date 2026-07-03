"""Engine entry point: :func:`run` builds the graph and drives the Rust engine."""

from datetime import datetime, timedelta

from ._graph import Builder, Edge, _builder
from ._node import _dt_to_ns, _ns_to_dt, _to_ns


def add_graph_output(name, x):
    """Record every tick of ``x`` under ``name``; returned by :func:`run`."""
    from ._graph import current_builder

    current_builder().engine.add_graph_output(name, x.id)


def run(graph, *args, starttime, endtime=None, realtime=False, **kwargs):
    """Build ``graph`` and run the engine over ``[starttime, endtime]``.

    ``endtime`` may be an absolute :class:`~datetime.datetime` or a
    :class:`~datetime.timedelta` relative to ``starttime``. Returns a dict
    mapping each :func:`add_graph_output` name to a list of ``(datetime, value)``.
    """
    builder = Builder()
    token = _builder.set(builder)
    try:
        graph(*args, **kwargs)
    finally:
        _builder.reset(token)

    start_ns = _dt_to_ns(starttime)
    if endtime is None:
        end_ns = start_ns
    elif isinstance(endtime, timedelta):
        end_ns = start_ns + _to_ns(endtime)
    else:
        end_ns = _dt_to_ns(endtime)

    raw = builder.engine.run(start_ns, end_ns, realtime)
    return {
        name: [(_ns_to_dt(t), v) for t, v in rows]
        for name, rows in raw.items()
    }
