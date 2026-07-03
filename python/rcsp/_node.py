"""The ``@node`` decorator and the per-tick runtime it drives.

A ``@node`` function is *not* executed when it appears in a graph. Instead it is
wired into the Rust engine as a Python node whose body is invoked once per
engine cycle in which one of its inputs ticks. During that invocation:

* time-series inputs are presented as :class:`_Input` proxies that behave like
  their current value in arithmetic but also carry ``ticked``/``valid`` flags,
* :func:`ticked`, :func:`valid`, :func:`output`, :func:`state`, :func:`starting`
  and the alarm helpers read/write the active tick context,
* a bare ``return expr`` emits ``expr`` on the single output.
"""

import collections
import contextvars
import inspect
from datetime import datetime, timedelta, timezone

from ._types import Outputs, TsType

_ctx = contextvars.ContextVar("rcsp_tick_ctx")


def _v(o):
    """Unwrap an input proxy to its underlying value."""
    return o.value if isinstance(o, _Input) else o


class _Input:
    """A view of a time-series input during a single tick.

    Delegates arithmetic/comparison to the underlying value so node bodies can
    write ``x + y`` while ``ticked(x)`` / ``valid(x)`` still work.
    """

    __slots__ = ("value", "_ticked", "_valid")

    def __init__(self, value, ticked, valid):
        self.value = value
        self._ticked = bool(ticked)
        self._valid = bool(valid)

    # numeric / string protocol -------------------------------------------------
    def __add__(self, o):
        return self.value + _v(o)

    def __radd__(self, o):
        return _v(o) + self.value

    def __sub__(self, o):
        return self.value - _v(o)

    def __rsub__(self, o):
        return _v(o) - self.value

    def __mul__(self, o):
        return self.value * _v(o)

    def __rmul__(self, o):
        return _v(o) * self.value

    def __truediv__(self, o):
        return self.value / _v(o)

    def __rtruediv__(self, o):
        return _v(o) / self.value

    def __floordiv__(self, o):
        return self.value // _v(o)

    def __mod__(self, o):
        return self.value % _v(o)

    def __neg__(self):
        return -self.value

    def __abs__(self):
        return abs(self.value)

    def __lt__(self, o):
        return self.value < _v(o)

    def __le__(self, o):
        return self.value <= _v(o)

    def __gt__(self, o):
        return self.value > _v(o)

    def __ge__(self, o):
        return self.value >= _v(o)

    def __eq__(self, o):
        return self.value == _v(o)

    def __ne__(self, o):
        return self.value != _v(o)

    def __float__(self):
        return float(self.value)

    def __int__(self):
        return int(self.value)

    def __bool__(self):
        return bool(self.value)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return f"<ts value={self.value!r} ticked={self._ticked} valid={self._valid}>"


class _State:
    """A mutable per-node-instance namespace, persistent across ticks."""

    def get(self, key, default=None):
        return getattr(self, key, default)


class _TickCtx:
    __slots__ = ("now_ns", "state", "alarm_state", "alarm_names",
                 "emissions", "alarms", "out_names", "starting")

    def __init__(self, now_ns, state, alarm_state, alarm_names, out_names, starting):
        self.now_ns = now_ns
        self.state = state
        self.alarm_state = alarm_state
        self.alarm_names = alarm_names
        self.out_names = out_names
        self.emissions = []
        self.alarms = []
        self.starting = starting


# --- helpers used inside node bodies -------------------------------------------

def ticked(*inputs):
    """True if *any* of the given inputs ticked in the current cycle."""
    return any(i._ticked for i in inputs)


def valid(*inputs):
    """True if *all* of the given inputs have a value."""
    return all(i._valid for i in inputs)


def output(*args, **kwargs):
    """Emit node outputs. Use ``output(value)`` for a single output or
    ``output(name=value, ...)`` for named outputs."""
    ctx = _ctx.get()
    if args:
        if len(args) != 1:
            raise TypeError("output() takes a single positional value")
        ctx.emissions.append((0, _v(args[0])))
    for name, value in kwargs.items():
        ctx.emissions.append((ctx.out_names.index(name), _v(value)))


def state(**defaults):
    """Return this node instance's persistent state namespace, initialising
    any keyword defaults on first use."""
    ctx = _ctx.get()
    s = ctx.state
    for k, val in defaults.items():
        if not hasattr(s, k):
            setattr(s, k, val)
    return s


def starting():
    """True only during the node's first invocation (its start cycle)."""
    return _ctx.get().starting


def now():
    """The current engine time as a timezone-aware UTC :class:`datetime`."""
    return _ns_to_dt(_ctx.get().now_ns)


def schedule_alarm(name, delay, value):
    """Schedule alarm ``name`` to fire ``delay`` from now carrying ``value``."""
    ctx = _ctx.get()
    ctx.alarms.append((ctx.alarm_names.index(name), _to_ns(delay), _v(value)))


def alarmed(name):
    """True if alarm ``name`` fired in the current cycle."""
    return _ctx.get().alarm_state[name][0]


def alarm_value(name):
    """The value delivered by alarm ``name`` this cycle."""
    return _ctx.get().alarm_state[name][1]


# --- time conversions ----------------------------------------------------------

def _to_ns(delta):
    if isinstance(delta, timedelta):
        return int(round(delta.total_seconds() * 1e9))
    if isinstance(delta, (int, float)):
        return int(delta)
    raise TypeError(f"expected timedelta, got {type(delta)}")


def _dt_to_ns(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(round(dt.timestamp() * 1e9))


def _ns_to_dt(ns):
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)


# --- the decorator -------------------------------------------------------------

def node(fn=None, *, alarms=None):
    """Decorator turning a Python function into an rcsp node.

    Usage::

        @node
        def add(x: ts[int], y: ts[int]) -> ts[int]:
            if ticked(x, y) and valid(x, y):
                return x + y

        @node(alarms=["tick"])
        def heartbeat() -> ts[bool]:
            if starting():
                schedule_alarm("tick", timedelta(seconds=1), True)
            if alarmed("tick"):
                schedule_alarm("tick", timedelta(seconds=1), True)
                return True
    """
    alarm_names = list(alarms or [])

    def wrap(func):
        return _make_node(func, alarm_names)

    if fn is None:
        return wrap
    return wrap(fn)


def _make_node(func, alarm_names):
    sig = inspect.signature(func)
    ts_params = [
        name for name, p in sig.parameters.items()
        if isinstance(p.annotation, TsType)
    ]
    ann = sig.return_annotation
    if isinstance(ann, Outputs):
        out_names = list(ann.fields.keys())
        single = False
    elif isinstance(ann, TsType):
        out_names = [None]
        single = True
    else:
        out_names = []
        single = False

    def wired(*args, **kwargs):
        from ._graph import Edge, current_builder

        builder = current_builder()
        bound = sig.bind(*args, **kwargs)
        bound.apply_defaults()

        ts_names, ts_ids, scalar_binding = [], [], {}
        for pname, pval in bound.arguments.items():
            if pname in ts_params:
                if not isinstance(pval, Edge):
                    raise TypeError(
                        f"node '{func.__name__}' argument '{pname}' expects a ts edge"
                    )
                ts_names.append(pname)
                ts_ids.append(pval.id)
            else:
                scalar_binding[pname] = pval

        out_ids = [builder.engine.new_edge() for _ in out_names]
        alarm_ids = [builder.engine.new_edge() for _ in alarm_names]
        node_state = _State()

        cb = _make_callback(func, ts_names, scalar_binding, out_names, alarm_names, node_state)
        run_at_start = bool(alarm_names)
        builder.engine.add_python_node(
            cb, ts_ids, alarm_ids, out_ids, func.__name__, run_at_start
        )

        if not out_names:
            return None
        if single:
            return Edge(builder, out_ids[0])
        Result = collections.namedtuple("Outputs", out_names)
        return Result(*(Edge(builder, oid) for oid in out_ids))

    wired.__name__ = func.__name__
    wired.__doc__ = func.__doc__
    wired.__wrapped__ = func
    return wired


def _make_callback(func, ts_names, scalar_binding, out_names, alarm_names, node_state):
    n_ts = len(ts_names)

    def cb(now_ns, values, ticked_flags, valid_flags):
        proxies = {
            name: _Input(values[i], ticked_flags[i], valid_flags[i])
            for i, name in enumerate(ts_names)
        }
        alarm_state = {
            aname: (bool(ticked_flags[n_ts + j]), values[n_ts + j])
            for j, aname in enumerate(alarm_names)
        }
        is_start = not getattr(node_state, "__started__", False)
        node_state.__started__ = True

        ctx = _TickCtx(now_ns, node_state, alarm_state, alarm_names, out_names, is_start)
        token = _ctx.set(ctx)
        try:
            ret = func(**proxies, **scalar_binding)
        finally:
            _ctx.reset(token)

        emissions = ctx.emissions
        if out_names == [None] and ret is not None:
            emissions.append((0, _v(ret)))
        return (emissions, ctx.alarms)

    return cb
