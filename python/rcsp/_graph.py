"""Graph-building context: the :class:`Builder` owning a Rust ``Engine``, the
:class:`Edge` handle for time-series edges, and the ``@graph`` decorator."""

import contextvars

from ._rcsp import Engine

_builder = contextvars.ContextVar("rcsp_builder")


def current_builder():
    try:
        return _builder.get()
    except LookupError:
        raise RuntimeError(
            "no active rcsp graph; build edges inside a @graph run via rcsp.run(...)"
        )


class Builder:
    """Owns the engine under construction while a graph body executes."""

    def __init__(self):
        self.engine = Engine()
        # Realtime push adapters to signal at start, and callbacks to run at stop.
        self.push_adapters = []
        self.stop_callbacks = []

    def new_edge(self):
        return Edge(self, self.engine.new_edge())

    def const(self, value):
        eid = self.engine.new_edge()
        self.engine.add_const(eid, value)
        return Edge(self, eid)

    def binop(self, op, a, b):
        a = self._as_edge(a)
        b = self._as_edge(b)
        out = self.engine.new_edge()
        self.engine.add_binop(op, a.id, b.id, out)
        return Edge(self, out)

    def _as_edge(self, x):
        if isinstance(x, Edge):
            return x
        return self.const(x)


class Edge:
    """A handle to a time-series edge in the graph being built.

    Supports arithmetic and comparison, each of which wires a native binop node
    and returns a new edge — mirroring csp's ``ts`` arithmetic.
    """

    __slots__ = ("builder", "id")

    def __init__(self, builder, edge_id):
        self.builder = builder
        self.id = edge_id

    def _op(self, op, other, reflected=False):
        if reflected:
            return self.builder.binop(op, other, self)
        return self.builder.binop(op, self, other)

    def __add__(self, o):
        return self._op("add", o)

    def __radd__(self, o):
        return self._op("add", o, True)

    def __sub__(self, o):
        return self._op("sub", o)

    def __rsub__(self, o):
        return self._op("sub", o, True)

    def __mul__(self, o):
        return self._op("mul", o)

    def __rmul__(self, o):
        return self._op("mul", o, True)

    def __truediv__(self, o):
        return self._op("div", o)

    def __rtruediv__(self, o):
        return self._op("div", o, True)

    def __gt__(self, o):
        return self._op("gt", o)

    def __lt__(self, o):
        return self._op("lt", o)

    def __ge__(self, o):
        return self._op("ge", o)

    def __le__(self, o):
        return self._op("le", o)

    def eq(self, o):
        return self._op("eq", o)

    def ne(self, o):
        return self._op("ne", o)

    def __repr__(self):
        return f"<Edge {self.id}>"


def graph(fn):
    """Marks a function as a graph. Currently a light wrapper — graphs are
    ordinary callables invoked while a :class:`Builder` is active."""
    fn.__rcsp_graph__ = True
    return fn


class Feedback:
    """A feedback edge, letting a graph form a cycle (mirrors ``csp.feedback``).

    ``out()`` gives an edge you can consume *before* its producer exists;
    ``bind(x)`` later connects ``x`` to it. The fed-back value is delivered in
    the next engine cycle at the same timestamp — the one-cycle delay that keeps
    the graph a DAG for ranking while still expressing the loop.
    """

    def __init__(self, typ=None):
        self._builder = current_builder()
        self._edge = self._builder.engine.new_edge()
        self._bound = False

    def out(self):
        return Edge(self._builder, self._edge)

    def bind(self, x):
        if self._bound:
            raise RuntimeError("feedback already bound")
        self._bound = True
        fb_edge = self._edge

        def cb(now_ns, values, ticked_flags, valid_flags):
            # inputs = [x, fb_edge(as alarm)]; re-inject x onto the feedback
            # edge with a zero delay → fires next cycle, same timestamp.
            alarms = []
            if ticked_flags[0]:
                alarms.append((0, 0, values[0]))
            return ([], alarms)

        self._builder.engine.add_python_node(
            cb, [x.id], [fb_edge], [], "feedback", False
        )


def feedback(typ=None):
    """Create a :class:`Feedback` edge (see :class:`Feedback`)."""
    return Feedback(typ)
