"""Realtime reaction latency: event-driven Python path vs native hot path.

Two numbers:

1. The **event-driven** realtime loop reacts to a pushed input in ~tens of µs
   (down from the old ~1 ms polling floor) — good for arbitrary Python nodes, but
   bounded by the GIL hand-off between the producer thread and the engine.
2. The **native GIL-free hot path** (Rust producer → lock-free ring → GIL-released
   native compute) reacts in ~sub-µs — the envelope when no Python is on the hot
   path. See docs/REALTIME.md.
"""

import threading
import time
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import ts


@rcsp.node
def latency_us(x: ts[object]) -> ts[float]:
    if rcsp.ticked(x):
        return (time.perf_counter() - x.value) * 1e6


class Producer(threading.Thread):
    def __init__(self, adapter):
        super().__init__(daemon=True)
        self._adapter = adapter

    def run(self):
        self._adapter.wait_for_start(timeout=2)
        for _ in range(20):
            time.sleep(0.03)
            self._adapter.push_tick(time.perf_counter())


@rcsp.graph
def my_graph():
    adapter = rcsp.GenericPushAdapter(object)
    Producer(adapter).start()
    rcsp.add_graph_output("lat", latency_us(adapter.out()))


def main():
    # 1) Python event-driven path
    out = rcsp.run(
        my_graph,
        starttime=datetime.now(timezone.utc),
        endtime=timedelta(seconds=1.0),
        realtime=True,
    )
    lat = sorted(v for _, v in out["lat"])
    print("event-driven realtime (Python node on the hot path):")
    print(f"  push→node  median={lat[len(lat)//2]:.1f}µs  p90={lat[int(len(lat)*0.9)]:.1f}µs")

    # 2) Native GIL-free hot path
    ns = rcsp.native_latency_benchmark(5000)
    print("\nnative GIL-free hot path (no Python on the hot path):")
    print(f"  ring→compute  median={ns['median_ns']/1e3:.3f}µs  "
          f"p90={ns['p90_ns']/1e3:.3f}µs  min={ns['min_ns']/1e3:.3f}µs")


if __name__ == "__main__":
    main()
