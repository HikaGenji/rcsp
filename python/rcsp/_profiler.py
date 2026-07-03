"""Graph profiling (mirrors ``csp.profiler``).

Measure per-node execution counts and cumulative time for a run::

    with rcsp.profiler.Profiler() as p:
        rcsp.run(my_graph, starttime=..., endtime=...)
    info = p.results()
    info.print_stats()
"""

import contextvars

_active = contextvars.ContextVar("rcsp_profiler", default=None)


class NodeStat:
    __slots__ = ("id", "name", "count", "total_ns")

    def __init__(self, nid, name, count, total_ns):
        self.id = nid
        self.name = name
        self.count = count
        self.total_ns = total_ns

    @property
    def avg_ns(self):
        return self.total_ns / self.count if self.count else 0.0

    def __repr__(self):
        return (f"NodeStat(name={self.name!r}, count={self.count}, "
                f"total_ns={self.total_ns}, avg_ns={self.avg_ns:.1f})")


class ProfilerInfo:
    """Results of a profiled run."""

    def __init__(self, rows):
        self.nodes = [NodeStat(*r) for r in rows]

    @property
    def total_ns(self):
        return sum(n.total_ns for n in self.nodes)

    @property
    def total_executions(self):
        return sum(n.count for n in self.nodes)

    def by_total_time(self):
        return sorted(self.nodes, key=lambda n: n.total_ns, reverse=True)

    def print_stats(self, top=None):
        rows = self.by_total_time()
        if top is not None:
            rows = rows[:top]
        print(f"{'node':<22}{'count':>10}{'total(µs)':>14}{'avg(µs)':>12}")
        print("-" * 58)
        for n in rows:
            print(f"{n.name:<22}{n.count:>10}{n.total_ns / 1e3:>14.2f}"
                  f"{n.avg_ns / 1e3:>12.3f}")
        print("-" * 58)
        print(f"{'TOTAL':<22}{self.total_executions:>10}{self.total_ns / 1e3:>14.2f}")


class Profiler:
    """Context manager that profiles graphs run within its ``with`` block."""

    def __init__(self):
        self._info = None
        self._token = None

    def __enter__(self):
        self._token = _active.set(self)
        return self

    def __exit__(self, *exc):
        _active.reset(self._token)
        return False

    def _ingest(self, rows):
        self._info = ProfilerInfo(rows)

    def results(self):
        """Return the :class:`ProfilerInfo` collected by the last run."""
        return self._info


def _current():
    return _active.get()
