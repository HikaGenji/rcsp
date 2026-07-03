"""Built-in nodes (``rcsp.baselib`` equivalent).

Most of these map straight onto native Rust kernels for speed; ``curve`` is a
small Python-node adapter that replays a fixed series."""

import collections
from datetime import datetime, timedelta

from ._graph import Edge, current_builder
from ._node import _dt_to_ns, _to_ns, node, ticked, valid, output
from ._types import Outputs, ts


def const(value):
    """A constant that ticks once at start time."""
    return current_builder().const(value)


def timer(interval, value=True):
    """Tick ``value`` every ``interval`` (a :class:`~datetime.timedelta`)."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_timer(out, _to_ns(interval), value)
    return Edge(b, out)


def sample(trigger, x):
    """Sample ``x``'s current value whenever ``trigger`` ticks."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_sample(trigger.id, x.id, out)
    return Edge(b, out)


def filter(flag, x):
    """Pass ``x`` through only while ``flag`` is true."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_filter(flag.id, x.id, out)
    return Edge(b, out)


def merge(x, y):
    """Merge two series; ``x`` wins if both tick in the same cycle."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_merge(x.id, y.id, out)
    return Edge(b, out)


def count(x):
    """Count the number of ticks seen on ``x``."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_count(x.id, out)
    return Edge(b, out)


def firstN(x, n):
    """Pass through only the first ``n`` ticks of ``x``."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_firstn(x.id, out, int(n))
    return Edge(b, out)


def delay(x, delta):
    """Re-emit each tick of ``x`` delayed by ``delta`` (a timedelta)."""
    b = current_builder()
    out = b.engine.new_edge()
    b.engine.add_delay(x.id, out, _to_ns(delta))
    return Edge(b, out)


def print_node(name, x):
    """Print each tick of ``x`` with a label to stdout."""
    current_builder().engine.add_print(name, x.id)


@node
def _split_node(flag: ts[bool], x: ts[object]) -> Outputs(true=ts[object], false=ts[object]):
    if ticked(x) and valid(flag):
        if flag.value:
            output(true=x.value)
        else:
            output(false=x.value)


def split(flag, x):
    """Route ``x`` to ``.true`` or ``.false`` depending on ``flag`` (mirrors
    ``csp.split``)."""
    return _split_node(flag, x)


def apply(fn, *edges):
    """Apply ``fn`` to the current values of ``edges`` each cycle, emitting the
    result. Ticks when any input ticks and all are valid; a ``None`` result is
    skipped. This is the ergonomic way to run an arbitrary operation — including
    DataFrame/NumPy ops — on edge values without writing a full ``@node``::

        mid = rcsp.apply(lambda b, a: (b + a) / 2, bid, ask)
        mean = rcsp.apply(lambda df: df["px"].mean(), frame_edge)
    """
    b = current_builder()
    out = b.engine.new_edge()
    ids = [e.id for e in edges]

    def cb(now_ns, values, ticked_flags, valid_flags):
        if any(ticked_flags) and all(valid_flags):
            result = fn(*values)
            if result is not None:
                return ([(0, result)], [])
        return ([], [])

    b.engine.add_python_node(cb, ids, [], [out], getattr(fn, "__name__", "apply"), False)
    return Edge(b, out)


def curve(typ, data):
    """Replay a fixed series ``data`` of ``(time, value)`` pairs.

    ``time`` may be a :class:`~datetime.timedelta` offset from start time or an
    absolute :class:`~datetime.datetime`.
    """
    b = current_builder()
    out = b.engine.new_edge()
    alarm = b.engine.new_edge()

    points = []
    for t, v in data:
        points.append((t, v))
    seeded = {"done": False}

    def cb(now_ns, values, ticked_flags, valid_flags):
        emissions = []
        alarms = []
        if not seeded["done"]:
            seeded["done"] = True
            for t, v in points:
                if isinstance(t, timedelta):
                    delay_ns = _to_ns(t)
                else:  # absolute datetime
                    delay_ns = _dt_to_ns(t) - now_ns
                if delay_ns < 0:
                    delay_ns = 0
                alarms.append((0, delay_ns, v))
        # input layout: [alarm]; when the alarm fires, emit its value
        if ticked_flags[0]:
            emissions.append((0, values[0]))
        return (emissions, alarms)

    b.engine.add_python_node(cb, [], [alarm], [out], "curve", True)
    return Edge(b, out)
