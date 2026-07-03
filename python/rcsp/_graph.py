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
