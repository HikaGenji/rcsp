"""Realtime auto-persistence of every node's output.

Opt-in via ``rcsp.run(..., persist="audit.jsonl")``. After the graph is built,
a recording sink is attached to every node's output edge; each tick it hands
``(time, node, value)`` to a background writer thread through a thread-safe
queue, and the writer streams rows to a line-oriented file (JSONL or CSV),
flushing each one so you can ``tail -f`` it and audit the system live.

Why it's collision-free: the engine is single-threaded, so the sinks run in-line
as ordinary graph nodes and only ever ``queue.put`` (never touch the file). The
writer thread only ``queue.get``s and writes (never touches engine state). The
queue is the single synchronization boundary. Values are stringified on the main
thread at put time, so no mutable object is shared across threads.

Live streaming works in ``realtime=True`` runs (the engine yields the GIL every
~1ms). In fast simulation the engine holds the GIL in a tight loop, so rows drain
opportunistically and are fully flushed when the run ends.
"""

import json
import queue
import threading

from ._adapters import schedule_on_engine_stop
from ._graph import Edge
from ._node import node, now, ticked
from ._types import ts


@node
def _persist_record(x: ts[object], label, q) -> None:
    # Runs in-line in the single-threaded engine; O(1), thread-safe hand-off.
    if ticked(x):
        q.put((now(), label, str(x.value)))


def _csv_field(value):
    if any(c in value for c in ',"\n\r'):
        return '"' + value.replace('"', '""') + '"'
    return value


class _Writer(threading.Thread):
    """Drains the queue and streams rows to disk, flushing each line."""

    def __init__(self, path, q):
        super().__init__(daemon=True)
        self._path = path
        self._q = q
        self._stop = threading.Event()
        self._jsonl = path.endswith(".jsonl")

    def run(self):
        with open(self._path, "w") as f:
            if not self._jsonl:
                f.write("time,node,value\n")
                f.flush()
            while not (self._stop.is_set() and self._q.empty()):
                try:
                    t, label, val = self._q.get(timeout=0.1)
                except queue.Empty:
                    continue
                iso = t.isoformat()
                if self._jsonl:
                    f.write(json.dumps({"time": iso, "node": label, "value": val}) + "\n")
                else:
                    f.write(f"{_csv_field(iso)},{_csv_field(label)},{_csv_field(val)}\n")
                f.flush()

    def stop(self):
        self._stop.set()


def attach_persist(builder, path):
    """Attach a streaming audit sink to every node's output edge."""
    if not (path.endswith(".csv") or path.endswith(".jsonl")):
        raise ValueError(
            "realtime persist needs a .csv or .jsonl path "
            "(Parquet is columnar and not row-streamable)"
        )

    q = queue.Queue()
    nodes, _producers = builder.engine.topology()
    for nid, name, _rank, _inputs, outputs in nodes:
        # Sinks (print/graph_output/feedback) have empty outputs → skipped.
        for k, edge_id in enumerate(outputs):
            label = f"{name}#{nid}" if len(outputs) == 1 else f"{name}#{nid}[{k}]"
            _persist_record(Edge(builder, edge_id), label, q)

    writer = _Writer(path, q)
    writer.start()

    def _finish():
        writer.stop()
        writer.join(timeout=5.0)  # ensure all rows are flushed and file closed

    schedule_on_engine_stop(_finish)
    return writer
