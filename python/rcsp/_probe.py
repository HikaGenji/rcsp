"""Measure whether a Python producer can generate ticks fast enough.

Realtime reaction latency (push→node) does *not* tell you whether the producer
keeps up — it's stamped at push time, so producer lateness is invisible. This
probe measures the producer side directly:

* **throughput** — how many ticks/s a Python thread can push through a real
  ``GenericPushAdapter`` into a running engine on this machine, and whether the
  engine drained all of them;
* **pacing jitter** (optional) — at a target rate, the actual inter-push interval
  vs the intended one.

Compare the results against your feed's required rate and jitter budget. A Python
producer is GIL-bound (~10^5 ticks/s here, non-deterministic); for higher rates or
deterministic pacing you need a native producer (see docs/REALTIME.md).
"""

import threading
import time
from datetime import datetime, timedelta, timezone

from ._adapters import GenericPushAdapter
from ._graph import graph as _graph_decorator
from ._run import add_graph_output, run


def producer_benchmark(duration=0.3, target_rate=None):
    """Measure this machine's Python-producer capability.

    Returns a dict with ``max_rate_per_s``, ``pushed``, ``delivered``,
    ``kept_up`` and, when ``target_rate`` is given, ``pacing_median_us`` and
    ``pacing_p99_us`` for a producer aiming at that rate.
    """
    result = {}

    # --- 1) max throughput: push as fast as possible ---
    cnt = {"pushed": 0, "dur": 0.0}

    class _Flood(threading.Thread):
        def __init__(self, adapter):
            super().__init__(daemon=True)
            self.a = adapter

        def run(self):
            self.a.wait_for_start(timeout=2)
            t0 = time.perf_counter()
            end = t0 + duration
            n = 0
            while time.perf_counter() < end:
                self.a.push_tick(n)
                n += 1
            cnt["pushed"] = n
            cnt["dur"] = time.perf_counter() - t0

    @_graph_decorator
    def _g_flood():
        a = GenericPushAdapter(int)
        _Flood(a).start()
        add_graph_output("x", a.out())

    out = run(_g_flood, starttime=datetime.now(timezone.utc),
              endtime=timedelta(seconds=duration + 0.3), realtime=True)
    result["max_rate_per_s"] = cnt["pushed"] / cnt["dur"] if cnt["dur"] else 0.0
    result["pushed"] = cnt["pushed"]
    result["delivered"] = len(out["x"])
    result["kept_up"] = result["delivered"] == result["pushed"]

    # --- 2) pacing jitter at a target rate ---
    if target_rate:
        interval = 1.0 / target_rate
        ticks = min(int(target_rate * duration), 5000) or 1
        ivs = []

        class _Paced(threading.Thread):
            def __init__(self, adapter):
                super().__init__(daemon=True)
                self.a = adapter

            def run(self):
                self.a.wait_for_start(timeout=2)
                nxt = time.perf_counter()
                last = None
                for _ in range(ticks):
                    now = time.perf_counter()
                    while now < nxt:      # busy-wait to the target instant (µs precision)
                        now = time.perf_counter()
                    if last is not None:
                        ivs.append((now - last) * 1e6)
                    last = now
                    self.a.push_tick(now)
                    nxt += interval

        @_graph_decorator
        def _g_paced():
            a = GenericPushAdapter(object)
            _Paced(a).start()
            add_graph_output("x", a.out())

        run(_g_paced, starttime=datetime.now(timezone.utc),
            endtime=timedelta(seconds=ticks * interval + 0.3), realtime=True)
        ivs.sort()
        if ivs:
            result["target_rate_per_s"] = target_rate
            result["pacing_median_us"] = ivs[len(ivs) // 2]
            result["pacing_p99_us"] = ivs[min(int(len(ivs) * 0.99), len(ivs) - 1)]

    return result
