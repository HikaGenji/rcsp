"""Auto-persist every computation to a live audit log.

Passing ``persist="<path>.jsonl"`` (or ``.csv``) to ``rcsp.run`` attaches a
recording sink to *every* node's output edge and streams ``(time, node, value)``
rows to disk as each tick happens — so you can ``tail -f`` the file and audit the
running system in realtime. It's opt-in: without ``persist`` nothing is attached.

Collision-free by design: the sinks are ordinary graph nodes (single-threaded,
in-line), handing values to a background writer thread through a thread-safe
queue; the writer never touches engine state and the engine never touches the file.
"""

import json
import tempfile
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import ts


@rcsp.node
def scaled(count: ts[int]) -> ts[float]:
    if rcsp.ticked(count):
        return count.value * 1.5


@rcsp.graph
def my_graph():
    beat = rcsp.timer(timedelta(seconds=1), 1)
    n = rcsp.count(beat)
    doubled = n * 2          # native binop node — also persisted
    rcsp.print("scaled", scaled(n))


def main():
    path = tempfile.mktemp(suffix=".jsonl")
    print(f"streaming every node's output to {path}")
    print("(tail -f it during a longer run to audit live)\n")

    rcsp.run(
        my_graph,
        starttime=datetime.now(timezone.utc),
        endtime=timedelta(seconds=3),
        realtime=True,
        persist=path,          # <-- opt-in realtime persistence
    )

    rows = [json.loads(line) for line in open(path)]
    print(f"persisted {len(rows)} ticks across nodes: {sorted({r['node'] for r in rows})}")
    for r in rows[:8]:
        print(f"  {r['time'][11:23]}  {r['node']:<10} = {r['value']}")


if __name__ == "__main__":
    main()
