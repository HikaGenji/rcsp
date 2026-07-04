"""Monitor live Wikimedia edits as a stream.

Port of CSP's ``examples/07_end_to_end/wikimedia.ipynb``. CSP's notebook consumes
Wikimedia's live recent-change SSE stream and dashboards it; this runnable script
does the same streaming computation and prints a rolling summary.

Source: each edit is ``{wiki, user, bot, title, bytes}``.
  * default (offline): a background thread pushes deterministic synthetic edits;
  * ``--live``: read the real SSE feed (needs open network — the sandbox blocks it).

Graph: total edits, a trailing-window edit rate, bot-vs-human ratio, rolling bytes
changed, and the current busiest wiki — all with rcsp's realtime push adapter and
rolling stats.
"""

import argparse
import json
import random
import threading
import time
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import stats, ts

WIKIS = ["enwiki", "dewiki", "frwiki", "commonswiki", "wikidatawiki", "eswiki"]


# ---- sources -----------------------------------------------------------------

class _SyntheticEdits(threading.Thread):
    """Deterministic offline stand-in for the live SSE feed."""

    def __init__(self, adapter, rate_hz=40.0):
        super().__init__(daemon=True)
        self._adapter = adapter
        self._period = 1.0 / rate_hz
        self._rng = random.Random(42)
        self._running = True

    def run(self):
        self._adapter.wait_for_start(timeout=2)
        while self._running:
            e = {
                "wiki": self._rng.choices(WIKIS, weights=[8, 3, 3, 5, 4, 2])[0],
                "user": f"user{self._rng.randint(1, 500)}",
                "bot": self._rng.random() < 0.35,
                "bytes": self._rng.randint(-800, 1500),
            }
            self._adapter.push_tick(e)
            time.sleep(self._period)

    def stop(self):
        self._running = False


class _LiveEdits(threading.Thread):
    """Read Wikimedia's recent-change SSE stream (requires open network)."""

    URL = "https://stream.wikimedia.org/v2/stream/recentchange"

    def __init__(self, adapter):
        super().__init__(daemon=True)
        self._adapter = adapter
        self._running = True

    def run(self):
        import urllib.request

        self._adapter.wait_for_start(timeout=2)
        resp = urllib.request.urlopen(self.URL, timeout=10)
        for raw in resp:
            if not self._running:
                break
            if raw.startswith(b"data:"):
                try:
                    d = json.loads(raw[5:].strip())
                except Exception:
                    continue
                length = d.get("length") or {}
                self._adapter.push_tick({
                    "wiki": d.get("wiki", "?"),
                    "user": d.get("user", "?"),
                    "bot": bool(d.get("bot")),
                    "bytes": (length.get("new") or 0) - (length.get("old") or 0),
                })

    def stop(self):
        self._running = False


# ---- graph -------------------------------------------------------------------

@rcsp.node
def bot_ratio(edit: ts[object]) -> ts[float]:
    s = rcsp.state(bots=0, total=0)
    if rcsp.ticked(edit):
        s.total += 1
        if edit.value["bot"]:
            s.bots += 1
        return s.bots / s.total


@rcsp.node
def top_wiki(edit: ts[object]) -> ts[object]:
    s = rcsp.state(counts=None)
    if s.counts is None:
        s.counts = {}
    if rcsp.ticked(edit):
        w = edit.value["wiki"]
        s.counts[w] = s.counts.get(w, 0) + 1
        lead = max(s.counts, key=s.counts.get)
        return (lead, s.counts[lead])


@rcsp.node
def field(edit: ts[object], name: str) -> ts[object]:
    if rcsp.ticked(edit):
        return edit.value[name]


def build(adapter, window):
    edits = adapter.out()
    total = rcsp.count(edits)
    ones = rcsp.apply(lambda e: 1, edits)                   # numeric edge for stats
    rate = stats.count(ones, window)                        # edits in the window
    ratio = bot_ratio(edits)
    lead = top_wiki(edits)
    bytes_win = stats.sum(field(edits, "bytes"), window)

    # sample the running aggregates once a second for a tidy printout
    tick = rcsp.timer(timedelta(seconds=1), True)
    rcsp.print("edits", rcsp.sample(tick, total))
    rcsp.print("edits/win", rcsp.sample(tick, rate))
    rcsp.print("bot_ratio", rcsp.sample(tick, ratio))
    rcsp.print("top_wiki", rcsp.sample(tick, lead))
    rcsp.print("bytes/win", rcsp.sample(tick, bytes_win))

    rcsp.add_graph_output("total", total)
    rcsp.add_graph_output("bot_ratio", ratio)
    rcsp.add_graph_output("top_wiki", lead)


def main(live=False, duration=3.0):
    window = timedelta(seconds=2)

    @rcsp.graph
    def g():
        adapter = rcsp.GenericPushAdapter(object)
        src = _LiveEdits(adapter) if live else _SyntheticEdits(adapter)
        src.start()
        rcsp.schedule_on_engine_stop(src.stop)
        build(adapter, window)

    out = rcsp.run(g, starttime=datetime.now(timezone.utc),
                   endtime=timedelta(seconds=duration), realtime=True)
    if out["total"]:
        print(f"\nprocessed {out['total'][-1][1]} edits; "
              f"final bot ratio {out['bot_ratio'][-1][1]:.0%}; "
              f"busiest wiki {out['top_wiki'][-1][1]}")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="use the real Wikimedia SSE feed")
    ap.add_argument("--duration", type=float, default=3.0)
    a = ap.parse_args()
    main(live=a.live, duration=a.duration)
