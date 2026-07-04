"""ETF-vs-basket arbitrage: SPY vs its ~500 constituents, in rcsp.

The strategy
------------
Every constituent *i* has an open price ``O_i`` and a current mid ``P_i``. The
cap-weighted basket index level is::

    idx = Σ_i  w_i · (P_i / O_i)         (weights sum to 1; idx == 1 at the open)

so ``idx`` is just ``1 + Σ w_i·r_i`` where ``r_i`` is the return since open. SPY's
**fair value** is ``spy_open · idx``. We compare it to SPY's traded mid::

    edge = fair − spy_mid
      edge >  margin  →  SPY is cheap vs the basket  →  BUY  SPY
      edge < −margin  →  SPY is rich  vs the basket  →  SELL SPY

and track a 1-lot position with mark-to-market PnL.

Why this is a good rcsp example
-------------------------------
There is no N-ary combine in rcsp (and a ``@node`` has fixed input arity), so a
500-name basket is folded exactly the way a real feed handler does it: **one
multiplexed quote stream ``{symbol, mid}`` into a single stateful node** that
maintains ``idx`` incrementally in **O(1) per quote** (subtract the symbol's old
contribution, add the new one). SPY rides the same stream as symbol ``"SPY"``, so
the simulation graph (``curve``) and the realtime graph (``GenericPushAdapter``)
share the identical node.

Offline by design: real S&P 500 *weights* aren't reachable from this sandbox
(SSGA/slickcharts are blocked), so ``sp500_weights.csv`` is a modeled snapshot
(real mega-cap top weights + a cap-weight power-law tail) built from the live
ticker list. See ``load_weights(live=True)`` and the README.

Run:  python examples/spy_basket/basket_arb.py --n-stocks 500 --quotes-per-sec 100
"""

import argparse
import csv
import math
import os
import threading
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import ts

_HERE = os.path.dirname(os.path.abspath(__file__))
_WEIGHTS_CSV = os.path.join(_HERE, "sp500_weights.csv")
SPY_OPEN = 450.0


# --- the multiplexed quote message -------------------------------------------

class Quote:
    """One market-data update. Module-level with ``__slots__`` so it pickles
    (future sharding) and is cheap. ``push_ts`` is only used on the realtime path."""

    __slots__ = ("symbol", "mid", "push_ts")

    def __init__(self, symbol, mid, push_ts=None):
        self.symbol = symbol
        self.mid = mid
        self.push_ts = push_ts


# --- weights ------------------------------------------------------------------

def _model_weights(tickers):
    """A modeled cap-weight vector over ``tickers`` (real mega-cap top block +
    power-law tail), normalized to 1.0. Used to (re)build the snapshot."""
    top = {
        "NVDA": 7.3, "MSFT": 6.6, "AAPL": 5.6, "AMZN": 3.9, "META": 2.8,
        "GOOGL": 2.3, "GOOG": 1.9, "AVGO": 2.4, "TSLA": 1.9, "BRK-B": 1.7,
        "JPM": 1.5, "LLY": 1.3, "V": 1.0, "XOM": 1.0, "UNH": 0.9,
        "MA": 0.8, "COST": 0.8, "HD": 0.8, "PG": 0.7, "WMT": 0.9,
        "JNJ": 0.7, "NFLX": 0.7, "ABBV": 0.7, "BAC": 0.6, "CRM": 0.6,
    }
    tset = set(tickers)
    rest = sorted((t for t in tickers if t not in top), key=lambda t: zlib.crc32(t.encode()))
    tail_budget = 100.0 - sum(top.values())
    tail = {t: 1.0 / (i + 8.0) for i, t in enumerate(rest)}
    tail_total = sum(tail.values()) or 1.0
    w = {t: v for t, v in top.items() if t in tset}
    for t, r in tail.items():
        w[t] = tail_budget * r / tail_total
    s = sum(w.values()) or 1.0
    return {t: v / s for t, v in w.items()}


def _fetch_sp500_tickers():
    """Live S&P 500 ticker list from GitHub raw (reachable where the network is
    open). Weights themselves aren't reachable here — see module docstring."""
    import io
    import urllib.request

    url = ("https://raw.githubusercontent.com/datasets/"
           "s-and-p-500-companies-financials/master/data/constituents.csv")
    raw = urllib.request.urlopen(url, timeout=20).read().decode()
    rows = csv.DictReader(io.StringIO(raw))
    return sorted({r["Symbol"].strip().replace(".", "-") for r in rows if r.get("Symbol")})


def load_weights(path=None, live=False):
    """Return ``{ticker: weight}`` (weights sum to 1.0). Reads the bundled
    snapshot by default; ``live=True`` re-fetches the ticker list and re-derives
    the modeled weights."""
    if live:
        return _model_weights(_fetch_sp500_tickers())
    with open(path or _WEIGHTS_CSV) as f:
        rows = list(csv.DictReader(f))
    return {r["symbol"]: float(r["weight"]) for r in rows}


# --- deterministic per-stock parameters (stable across processes via crc32) ---

def _crc(sym, seed):
    return zlib.crc32(sym.encode()) ^ (seed & 0xFFFFFFFF)


def _open_price(sym):
    return 50.0 + (zlib.crc32(sym.encode()) % 400)


def _beta(sym):
    return 0.7 + ((zlib.crc32(sym.encode()) >> 8) % 601) / 1000.0   # [0.7, 1.3]


def _top_subset(weights, n):
    """The ``n`` heaviest names, renormalized to sum to 1.0."""
    top = sorted(weights.items(), key=lambda kv: -kv[1])[:n]
    s = sum(w for _, w in top) or 1.0
    return {t: w / s for t, w in top}


# --- the synthetic market-data feed ------------------------------------------

@dataclass
class Feed:
    rows: list                 # [(timedelta, Quote)], sorted, strictly-increasing µs
    opens: dict                # {symbol: O_i}
    weights: dict              # effective (subset, renormalized) weights
    spy_open: float
    margin: float              # decision threshold in price units
    n_constituent_quotes: int
    n_spy_quotes: int
    step_us: int
    dislocations: list = field(default_factory=list)

    @property
    def n_quotes(self):
        return self.n_constituent_quotes + self.n_spy_quotes


def _default_dislocations(duration, margin):
    """A few windows (start_frac, end_frac, amount): amount>0 pushes SPY *below*
    fair (→ cheap → BUY); amount<0 pushes it above (→ rich → SELL)."""
    return [
        (0.20, 0.32, 3.0 * margin),
        (0.45, 0.55, -3.0 * margin),
        (0.70, 0.85, 4.0 * margin),
    ]


def synth_feed(weights, quotes_per_sec_per_stock=100, n_stocks=500, duration=2.0,
               spy_quotes_per_sec=50, margin_bps=5.0, spy_open=SPY_OPEN,
               dislocations=None, spy_noise=0.3, seed=7):
    """Build a deterministic, internally-consistent multiplexed quote feed.

    Constituent prices follow a shared market-factor random walk (× per-stock
    beta) plus idiosyncratic noise. SPY's mid is set to the *engine's own* fair
    value (running last-price index) minus a dislocation plus noise, so injected
    mispricings reliably trigger signals and the engine reproduces the index
    exactly. All quotes get strictly-increasing microsecond timestamps (a single
    edge holds one value per cycle, so ticks must not share a timestamp)."""
    import random

    w = _top_subset(weights, n_stocks)
    syms = list(w.keys())
    opens = {s: _open_price(s) for s in syms}
    margin = margin_bps / 1e4 * spy_open
    if dislocations is None:
        dislocations = _default_dislocations(duration, margin)

    # Shared market factor on a coarse grid, interpolated by time.
    n_grid = 200
    rng_m = random.Random(seed)
    mkt = [0.0]
    for _ in range(n_grid):
        mkt.append(mkt[-1] + rng_m.gauss(0.0, 0.0008))

    def market_at(t):
        k = min(n_grid, int(t / duration * n_grid))
        return mkt[k]

    # Per-stock constituent quotes (phys_time, symbol, mid) with an idio walk.
    events = []
    for s in syms:
        rng = random.Random(_crc(s, seed))
        beta, o = _beta(s), opens[s]
        n_per = max(1, int(quotes_per_sec_per_stock * duration))
        idio = 0.0
        for j in range(1, n_per + 1):
            t = j / quotes_per_sec_per_stock
            if t > duration:
                break
            idio += rng.gauss(0.0, 0.0006)
            mid = o * math.exp(beta * market_at(t) + idio)
            events.append((t, s, round(mid, 4)))
    n_constituent = len(events)

    # SPY time markers (mids filled during the ordered pass below).
    n_spy = max(1, int(spy_quotes_per_sec * duration))
    spy_times = [(j / spy_quotes_per_sec, "SPY", None) for j in range(1, n_spy + 1)
                 if j / spy_quotes_per_sec <= duration]

    # Single time-ordered pass: maintain the last-price index; set SPY mids from
    # the engine's own fair value so the sim is self-consistent.
    rng_spy = random.Random(seed ^ 12345)
    merged = sorted(events + spy_times, key=lambda e: e[0])
    px = dict(opens)
    idx = 1.0
    spy_count = 0
    out_rows = []
    for t, s, mid in merged:
        if s == "SPY":
            fair = spy_open * idx
            disloc = 0.0
            for (a, b, amt) in dislocations:
                if a <= (t / duration) < b:
                    disloc += amt
            spy_mid = fair - disloc + rng_spy.gauss(0.0, spy_noise * margin)
            out_rows.append((t, Quote("SPY", round(spy_mid, 4))))
            spy_count += 1
        else:
            idx += w[s] * (mid - px[s]) / opens[s]
            px[s] = mid
            out_rows.append((t, Quote(s, mid)))

    # Assign a strictly-increasing microsecond grid preserving time order.
    n_total = len(out_rows)
    rate = n_total / duration
    if rate > 1e6:
        raise ValueError(
            f"combined quote rate {rate:.0f}/s exceeds ~1e6/s: microsecond "
            f"timestamps would collide on the single edge. Lower n_stocks, "
            f"quotes_per_sec_per_stock, or raise duration.")
    step_us = max(1, int(duration * 1e6 / n_total))
    rows = [(timedelta(microseconds=(k + 1) * step_us), q) for k, (_, q) in enumerate(out_rows)]

    return Feed(rows=rows, opens=opens, weights=w, spy_open=spy_open, margin=margin,
                n_constituent_quotes=n_constituent, n_spy_quotes=spy_count,
                step_us=step_us, dislocations=list(dislocations))


# --- the strategy node --------------------------------------------------------

@rcsp.node
def basket_engine(quote: ts[object], opens: dict, weights: dict, spy_open: float,
                  margin: float) -> rcsp.Outputs(
                      fair=ts[float], edge=ts[float], signal=ts[int],
                      position=ts[int], pnl=ts[float], trades=ts[int]):
    """Fold the whole basket in O(1)/quote; decide + account on SPY ticks."""
    s = rcsp.state(px=None, idx=1.0, last_spy=None, pos=0, cash=0.0, n_trades=0)
    if s.px is None:
        s.px = dict(opens)
    if not rcsp.ticked(quote):
        return
    q = quote.value
    if q.symbol != "SPY":
        # constituent quote: incremental index update, no emit
        o = opens[q.symbol]
        s.idx += weights[q.symbol] * (q.mid - s.px[q.symbol]) / o
        s.px[q.symbol] = q.mid
        return
    # SPY quote: fair value, edge, signal, position, PnL
    s.last_spy = q.mid
    fair = spy_open * s.idx
    edge = fair - s.last_spy
    signal = 1 if edge > margin else (-1 if edge < -margin else 0)
    if edge > margin and s.pos <= 0:
        s.pos += 1
        s.cash -= s.last_spy
        s.n_trades += 1
    elif edge < -margin and s.pos >= 0:
        s.pos -= 1
        s.cash += s.last_spy
        s.n_trades += 1
    pnl = s.cash + s.pos * s.last_spy
    rcsp.output(fair=fair, edge=edge, signal=signal,
                position=s.pos, pnl=pnl, trades=s.n_trades)


# --- graph + simulation -------------------------------------------------------

def build_graph(feed, margin=None):
    m = feed.margin if margin is None else margin

    @rcsp.graph
    def g():
        quotes = rcsp.curve(object, feed.rows)
        eng = basket_engine(quotes, feed.opens, feed.weights, feed.spy_open, m)
        rcsp.add_graph_output("fair", eng.fair)
        rcsp.add_graph_output("edge", eng.edge)
        rcsp.add_graph_output("signal", eng.signal)
        rcsp.add_graph_output("position", eng.position)
        rcsp.add_graph_output("pnl", eng.pnl)
        rcsp.add_graph_output("trades", eng.trades)
    return g


def simulate(feed, margin=None):
    """Run the whole feed through the graph (fast simulation) and report results
    plus engine throughput (quotes / wall-clock second)."""
    g = build_graph(feed, margin)
    t0 = time.perf_counter()
    out = rcsp.run(g, starttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                   endtime=timedelta(seconds=feed.n_quotes * feed.step_us / 1e6 + 1.0))
    wall_s = time.perf_counter() - t0
    n_trades = out["trades"][-1][1] if out["trades"] else 0
    return {
        "fair": out["fair"], "edge": out["edge"], "signal": out["signal"],
        "position": out["position"], "pnl": out["pnl"], "trades": out["trades"],
        "n_trades": n_trades,
        "final_pnl": out["pnl"][-1][1] if out["pnl"] else 0.0,
        "n_quotes": feed.n_quotes,
        "wall_s": wall_s,
        "throughput_qps": feed.n_quotes / wall_s if wall_s else 0.0,
    }


def profile_run(feed, margin=None):
    """Run under the profiler; return per-node count/total/avg plus throughput."""
    g = build_graph(feed, margin)
    t0 = time.perf_counter()
    with rcsp.profiler.Profiler() as p:
        rcsp.run(g, starttime=datetime(2020, 1, 1, tzinfo=timezone.utc),
                 endtime=timedelta(seconds=feed.n_quotes * feed.step_us / 1e6 + 1.0))
    wall_s = time.perf_counter() - t0
    info = p.results()
    per_node = {n.name: {"count": n.count, "total_ns": n.total_ns, "avg_ns": n.avg_ns}
                for n in info.by_total_time()}
    return {"per_node": per_node, "total_ns": info.total_ns,
            "n_quotes": feed.n_quotes, "wall_s": wall_s,
            "throughput_qps": feed.n_quotes / wall_s if wall_s else 0.0}


# --- realtime reaction latency ------------------------------------------------

@rcsp.node
def _latency_us(quote: ts[object]) -> ts[float]:
    if rcsp.ticked(quote) and quote.value.push_ts is not None:
        return (time.perf_counter() - quote.value.push_ts) * 1e6


class _FeedThread(threading.Thread):
    """Push constituent + SPY quotes (stamped with perf_counter) at a paced rate."""

    def __init__(self, adapter, syms, opens, bursts, period, seed):
        super().__init__(daemon=True)
        self._a, self._syms, self._opens = adapter, syms, opens
        self._bursts, self._period, self._seed = bursts, period, seed

    def run(self):
        import random
        rng = random.Random(self._seed)
        self._a.wait_for_start(timeout=2)
        for i in range(self._bursts):
            s = rng.choice(self._syms)
            self._a.push_tick(Quote(s, self._opens[s] * (1 + rng.uniform(-0.01, 0.01)),
                                    push_ts=time.perf_counter()))
            if i % 5 == 0:
                self._a.push_tick(Quote("SPY", SPY_OPEN, push_ts=time.perf_counter()))
            time.sleep(self._period)


def realtime_latency(weights, n_stocks=20, bursts=200, period=0.001, seed=7):
    """Measure quote→node reaction latency on the realtime event-driven loop
    (Python aggregator on the hot path → forces realtime=True, not 'native')."""
    w = _top_subset(weights, n_stocks)
    opens = {s: _open_price(s) for s in w}
    syms = list(w.keys())

    @rcsp.graph
    def g():
        adapter = rcsp.GenericPushAdapter(object)
        q = adapter.out()
        basket_engine(q, opens, w, SPY_OPEN, 0.02)          # representative load
        rcsp.add_graph_output("lat", _latency_us(q))
        _FeedThread(adapter, syms, opens, bursts, period, seed).start()

    out = rcsp.run(g, starttime=datetime.now(timezone.utc),
                   endtime=timedelta(seconds=bursts * period + 0.5), realtime=True)
    lat = sorted(v for _, v in out["lat"])
    if not lat:
        return {"median_us": 0.0, "p90_us": 0.0, "p99_us": 0.0, "n_samples": 0}
    return {
        "median_us": lat[len(lat) // 2],
        "p90_us": lat[min(int(len(lat) * 0.9), len(lat) - 1)],
        "p99_us": lat[min(int(len(lat) * 0.99), len(lat) - 1)],
        "n_samples": len(lat),
    }


def producer_ceiling(target_rate=50000, duration=0.3):
    """Can a single Python producer sustain the feed's rate? (500·100 = 50k/s.)"""
    return rcsp.producer_benchmark(duration=duration, target_rate=target_rate)


# --- CLI ----------------------------------------------------------------------

def main(n_stocks=500, quotes_per_sec=100, duration=2.0, margin_bps=5.0, live=False):
    weights = load_weights(live=live)
    print(f"loaded {len(weights)} S&P 500 weights "
          f"(sum={sum(weights.values()):.4f}){' [live]' if live else ' [bundled snapshot]'}")

    feed = synth_feed(weights, quotes_per_sec_per_stock=quotes_per_sec,
                      n_stocks=n_stocks, duration=duration, margin_bps=margin_bps)
    print(f"feed: {feed.n_constituent_quotes} constituent + {feed.n_spy_quotes} SPY "
          f"quotes over {duration}s  (step={feed.step_us}µs, margin={feed.margin:.4f})")

    res = simulate(feed)
    print(f"\nsimulation: {res['n_trades']} trades, final PnL {res['final_pnl']:.2f}")
    print(f"throughput: {res['n_quotes']} quotes in {res['wall_s']:.3f}s "
          f"= {res['throughput_qps']/1e3:.1f}k quotes/s (single core)")

    prof = profile_run(feed)
    be = prof["per_node"].get("basket_engine", {})
    print(f"\nprofiler: basket_engine count={be.get('count')} "
          f"avg={be.get('avg_ns', 0)/1e3:.2f}µs/quote  (O(1) fold of {n_stocks} names)")

    lat = realtime_latency(weights)
    print(f"realtime reaction: median={lat['median_us']:.1f}µs "
          f"p90={lat['p90_us']:.1f}µs p99={lat['p99_us']:.1f}µs "
          f"(n={lat['n_samples']})")

    need = n_stocks * quotes_per_sec
    prod = producer_ceiling()
    print(f"producer ceiling: ~{prod['max_rate_per_s']/1e3:.0f}k ticks/s "
          f"(kept_up={prod['kept_up']}); feed needs {need/1e3:.0f}k/s")
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n-stocks", type=int, default=500)
    ap.add_argument("--quotes-per-sec", type=int, default=100)
    ap.add_argument("--duration", type=float, default=2.0)
    ap.add_argument("--margin-bps", type=float, default=5.0)
    ap.add_argument("--live", action="store_true", help="re-derive weights from the live ticker list")
    a = ap.parse_args()
    main(n_stocks=a.n_stocks, quotes_per_sec=a.quotes_per_sec,
         duration=a.duration, margin_bps=a.margin_bps, live=a.live)
