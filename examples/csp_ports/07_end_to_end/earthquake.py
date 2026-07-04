"""Recent earthquakes as a stream.

Port of CSP's ``examples/07_end_to_end/earthquake.ipynb`` (which plots recent
quakes on a world map). This runnable script replays quakes in time order and
computes running statistics.

Source: each quake is ``{mag, place, depth}`` arriving at its origin time.
  * default (offline): a bundled deterministic sample, replayed with ``curve``
    (the last hour compressed into a few seconds);
  * ``--live``: fetch USGS ``all_hour.geojson`` (needs open network).

Graph: running count, rolling max magnitude, a "significant" (M≥4.5) filter, and
the strongest quake so far.
"""

import argparse
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import stats, ts

# A deterministic sample (mag, place, depth_km), oldest → newest.
SAMPLE = [
    (1.2, "10km NE of Ridgecrest, CA", 3.1),
    (2.4, "22km S of Volcano, HI", 5.0),
    (4.6, "South of the Fiji Islands", 540.0),
    (1.8, "8km W of Cobb, CA", 1.5),
    (3.1, "54km SW of Sand Point, AK", 30.2),
    (5.2, "Off the coast of Central Chile", 45.0),
    (0.9, "3km NW of The Geysers, CA", 2.0),
    (2.7, "Nevada", 8.4),
    (4.9, "Kuril Islands", 60.0),
    (1.5, "12km E of Mammoth Lakes, CA", 4.0),
    (6.1, "near the coast of Southern Peru", 25.0),
    (2.2, "18km SE of Anza, CA", 12.5),
    (3.8, "Aleutian Islands, AK", 33.0),
]


def _load_live():
    import json
    import urllib.request

    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    d = json.load(urllib.request.urlopen(url, timeout=10))
    feats = sorted(d["features"], key=lambda f: f["properties"]["time"])
    out = []
    for f in feats:
        p = f["properties"]
        depth = f["geometry"]["coordinates"][2]
        out.append((p.get("mag") or 0.0, p.get("place") or "?", depth))
    return out


@rcsp.node
def strongest(mag: ts[float], place: ts[object]) -> ts[object]:
    s = rcsp.state(best=-1.0, where="")
    if rcsp.ticked(mag) and mag.value > s.best:
        s.best = mag.value
        s.where = place.value
        return (round(s.best, 1), s.where)


def main(live=False, duration=3.0):
    quakes = _load_live() if live else SAMPLE
    n = len(quakes)

    @rcsp.graph
    def g():
        # Replay in time order, the feed window compressed onto `duration`.
        step = duration / max(n, 1)
        mag = rcsp.curve(float, [(timedelta(seconds=(i + 1) * step), q[0]) for i, q in enumerate(quakes)])
        place = rcsp.curve(object, [(timedelta(seconds=(i + 1) * step), q[1]) for i, q in enumerate(quakes)])

        rcsp.print("quake", place)
        rcsp.print("mag", mag)
        rcsp.add_graph_output("count", rcsp.count(mag))
        rcsp.add_graph_output("rolling_max", stats.max(mag, timedelta(seconds=duration)))
        rcsp.add_graph_output("significant", rcsp.filter(mag >= 4.5, mag))
        rcsp.add_graph_output("strongest", strongest(mag, place))

    out = rcsp.run(g, starttime=datetime.now(timezone.utc),
                   endtime=timedelta(seconds=duration + 0.5), realtime=True)
    total = out["count"][-1][1] if out["count"] else 0
    sig = len(out["significant"])
    strong = out["strongest"][-1][1] if out["strongest"] else None
    print(f"\n{total} quakes; {sig} significant (M≥4.5); strongest {strong}")
    return {"total": total, "significant": sig, "strongest": strong}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="fetch the real USGS feed")
    ap.add_argument("--duration", type=float, default=3.0)
    a = ap.parse_args()
    main(live=a.live, duration=a.duration)
