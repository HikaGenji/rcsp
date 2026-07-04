"""NYC subway arrivals as a stream.

Port of CSP's ``examples/07_end_to_end/mta.ipynb`` (real-time NYC transit). This
runnable script streams train-arrival updates and tracks delays per route.

Source: each update is ``{route, stop, delay_s}``.
  * default (offline): a background thread pushes deterministic synthetic arrivals;
  * ``--live``: the real MTA feed is GTFS-realtime (protobuf) and needs
    ``gtfs-realtime-bindings`` + an API key — see ``_decode_gtfs`` below. The
    decoded updates feed the *same* push adapter, so the graph is unchanged.

Graph: system-wide rolling mean delay, on-time ratio (delay < 60s), and per-route
mean delay.
"""

import argparse
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import stats, ts

ROUTES = ["1", "2", "3", "A", "C"]
STOPS = ["Times Sq", "14 St", "Chambers St", "96 St", "Fulton St"]


class _SyntheticArrivals(threading.Thread):
    def __init__(self, adapter, rate_hz=25.0):
        super().__init__(daemon=True)
        self._adapter = adapter
        self._period = 1.0 / rate_hz
        self._rng = random.Random(7)
        self._running = True

    def run(self):
        self._adapter.wait_for_start(timeout=2)
        while self._running:
            route = self._rng.choice(ROUTES)
            # express routes run a bit later on average
            base = 90 if route in ("2", "3") else 40
            self._adapter.push_tick({
                "route": route,
                "stop": self._rng.choice(STOPS),
                "delay_s": max(0, int(self._rng.gauss(base, 45))),
            })
            time.sleep(self._period)

    def stop(self):
        self._running = False


def _decode_gtfs(_raw_bytes):
    """Decode a real MTA GTFS-realtime protobuf message into arrival updates.

    Left as a documented stub: install ``gtfs-realtime-bindings``, fetch
    ``https://api-endpoint.mta.info/...`` with your API key, parse the
    ``FeedMessage``, and yield ``{route, stop, delay_s}`` dicts to ``push_tick``.
    """
    raise NotImplementedError(
        "real MTA feed needs gtfs-realtime-bindings + an MTA API key; "
        "decode the protobuf FeedMessage and push_tick {route, stop, delay_s}"
    )


@rcsp.node
def field(ev: ts[object], name: str) -> ts[object]:
    if rcsp.ticked(ev):
        return ev.value[name]


@rcsp.node
def per_route_mean(ev: ts[object]) -> ts[object]:
    s = rcsp.state(agg=None)
    if s.agg is None:
        s.agg = {}
    if rcsp.ticked(ev):
        r = ev.value["route"]
        tot, cnt = s.agg.get(r, (0, 0))
        s.agg[r] = (tot + ev.value["delay_s"], cnt + 1)
        return {r: round(t / c, 1) for r, (t, c) in sorted(s.agg.items())}


@rcsp.node
def on_time_ratio(delay: ts[float]) -> ts[float]:
    s = rcsp.state(ontime=0, total=0)
    if rcsp.ticked(delay):
        s.total += 1
        if delay.value < 60:
            s.ontime += 1
        return s.ontime / s.total


def main(live=False, duration=3.0):
    window = timedelta(seconds=2)

    @rcsp.graph
    def g():
        adapter = rcsp.GenericPushAdapter(object)
        if live:
            raise SystemExit(_decode_gtfs.__doc__)
        src = _SyntheticArrivals(adapter)
        src.start()
        rcsp.schedule_on_engine_stop(src.stop)

        ev = adapter.out()
        delay = field(ev, "delay_s")
        tick = rcsp.timer(timedelta(seconds=1), True)

        rcsp.print("mean_delay_s", rcsp.sample(tick, stats.mean(delay, window)))
        rcsp.print("on_time", rcsp.sample(tick, on_time_ratio(delay)))
        rcsp.print("by_route", rcsp.sample(tick, per_route_mean(ev)))
        rcsp.add_graph_output("trains", rcsp.count(ev))
        rcsp.add_graph_output("by_route", per_route_mean(ev))

    out = rcsp.run(g, starttime=datetime.now(timezone.utc),
                   endtime=timedelta(seconds=duration), realtime=True)
    trains = out["trains"][-1][1] if out["trains"] else 0
    by_route = out["by_route"][-1][1] if out["by_route"] else {}
    print(f"\n{trains} arrivals; final mean delay by route (s): {by_route}")
    return {"trains": trains, "by_route": by_route}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="use the real MTA GTFS feed (needs deps + key)")
    ap.add_argument("--duration", type=float, default=3.0)
    a = ap.parse_args()
    main(live=a.live, duration=a.duration)
