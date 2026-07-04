"""Engine entry point: :func:`run` builds the graph and drives the Rust engine."""

from datetime import datetime, timedelta

from ._graph import Builder, Edge, _builder
from ._node import _dt_to_ns, _ns_to_dt, _to_ns


def add_graph_output(name, x):
    """Record every tick of ``x`` under ``name``; returned by :func:`run`."""
    from ._graph import current_builder

    current_builder().engine.add_graph_output(name, x.id)


def run(graph, *args, starttime, endtime=None, realtime=False, persist=None, **kwargs):
    """Build ``graph`` and run the engine over ``[starttime, endtime]``.

    ``endtime`` may be an absolute :class:`~datetime.datetime` or a
    :class:`~datetime.timedelta` relative to ``starttime``. Returns a dict
    mapping each :func:`add_graph_output` name to a list of ``(datetime, value)``.

    ``persist`` (opt-in, off by default): a ``.csv``/``.jsonl`` path to stream
    every node's output to, live, as an audit log. See :mod:`rcsp._persist`.
    """
    from ._profiler import _current as _current_profiler

    builder = Builder()
    token = _builder.set(builder)
    try:
        graph(*args, **kwargs)

        if persist:
            if builder.dynamics:
                raise NotImplementedError("persist=... is not supported with dynamic graphs")
            from ._persist import attach_persist

            attach_persist(builder, persist)

        start_ns = _dt_to_ns(starttime)
        if endtime is None:
            end_ns = start_ns
        elif isinstance(endtime, timedelta):
            end_ns = start_ns + _to_ns(endtime)
        else:
            end_ns = _dt_to_ns(endtime)

        profiler = _current_profiler()
        # Let realtime push adapters begin producing, then run; always fire
        # engine-stop callbacks afterwards (e.g. to join driver threads).
        for adapter in builder.push_adapters:
            adapter._signal_start()
        try:
            if builder.dynamics:
                if realtime:
                    raise NotImplementedError("dynamic graphs support simulation only")
                raw = _run_dynamic(builder, start_ns, end_ns, profiler)
            else:
                raw = builder.engine.run(start_ns, end_ns, realtime, profiler is not None)
                if profiler is not None:
                    profiler._ingest(builder.engine.profiling_report())
        finally:
            for callback in builder.stop_callbacks:
                try:
                    callback()
                except Exception:
                    pass
    finally:
        _builder.reset(token)

    return {
        name: [(_ns_to_dt(t), v) for t, v in rows]
        for name, rows in raw.items()
    }


def _run_dynamic(builder, start_ns, end_ns, profiler):
    """Stepped driver: advance the engine one timestamp at a time, instantiating
    dynamic sub-graphs between steps (the builder context is active here)."""
    from ._dynamic import instantiate_pending

    engine = builder.engine
    engine.begin(start_ns, end_ns, profiler is not None)
    instantiate_pending(builder)  # any keys already known (rare)
    while True:
        t = engine.step()
        instantiate_pending(builder)
        if t is None:
            break
    if profiler is not None:
        profiler._ingest(engine.profiling_report())
    return engine.outputs()
