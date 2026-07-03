"""Dynamic graphs (a slice of ``csp.dynamic``).

``rcsp.dynamic(control, factory)`` grows the graph at runtime: whenever the
``control`` stream produces a key it hasn't seen, a sub-graph is instantiated for
that key and spliced into the running engine. This is driven by the engine's
stepped API — between engine cycles (when the engine isn't borrowed) the driver
builds new sub-graphs, which the next cycle integrates (re-ranks, re-wires,
seeds).

``control`` ticks either a bare ``key`` or a ``(key, value)`` tuple. ``factory``
is ``factory(key, value_stream)`` where ``value_stream`` ticks that key's value
on each subsequent appearance. Sub-graphs typically call
``rcsp.add_graph_output`` to expose their results.

Limitations: simulation only (no realtime), and a key's sub-graph starts from
the tick *after* the one that spawned it.
"""

from ._graph import current_builder
from ._node import node, ticked
from ._types import ts


@node
def _dyn_controller(ctrl: ts[object], seen, pending) -> None:
    if ticked(ctrl):
        v = ctrl.value
        key = v[0] if isinstance(v, tuple) else v
        if key not in seen:
            seen.add(key)
            pending.append(key)


@node
def _dyn_demux(ctrl: ts[object], key) -> ts[object]:
    if ticked(ctrl):
        v = ctrl.value
        if isinstance(v, tuple):
            if v[0] == key:
                return v[1]
        elif v == key:
            return v


def dynamic(control, factory):
    """Instantiate ``factory(key, value_stream)`` for each new key on ``control``."""
    builder = current_builder()
    state = {"pending": [], "factory": factory, "control": control, "seen": set()}
    _dyn_controller(control, state["seen"], state["pending"])
    builder.dynamics.append(state)
    return state


def instantiate_pending(builder):
    """Build sub-graphs for any keys discovered since the last call."""
    for d in builder.dynamics:
        pending = d["pending"]
        while pending:
            key = pending.pop(0)
            value_stream = _dyn_demux(d["control"], key)
            d["factory"](key, value_stream)
