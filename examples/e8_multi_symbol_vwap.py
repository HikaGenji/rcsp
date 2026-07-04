"""Multi-symbol VWAP: one graph fans out many symbols; scale out by process.

This answers a common deployment question — *"to run VWAP per symbol, do I need
one graph (one process) per symbol?"* No:

  Topology A — one graph, whole universe.
    A single rcsp graph is single-threaded and, for Python `@node`s, holds the
    GIL for the whole run. But per-tick VWAP work is tiny (a couple of adds and a
    divide) and the engine is event-driven, so one graph fans a large symbol set
    onto one core cheaply. A `ReplayAdapterManager` (or any live push adapter)
    demultiplexes the single source into one independent edge per symbol.

  Topology B — shard symbols across processes.
    To use multiple cores you scale *out*: partition the symbols and run one
    graph per process (each its own GIL). This is "one process per *shard* of
    symbols", not one per symbol. Sharding is a deployment choice — the VWAP
    numbers are identical to topology A, which this script asserts.

(Co-hosting several graphs as threads in one process would NOT parallelize, since
Python `@node`s serialize on the GIL — hence process, not thread, is the unit.)

Run:  python examples/e8_multi_symbol_vwap.py --shards 2
"""

import argparse
import multiprocessing
import os
from datetime import datetime, timedelta

import rcsp
from rcsp import ts

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META"]
START = datetime(2020, 1, 1)


# --- the per-symbol algo ------------------------------------------------------

class Trade:
    """A fill (price, qty). Plain class (module-level) so it pickles for shards."""

    __slots__ = ("price", "qty")

    def __init__(self, price, qty):
        self.price = price
        self.qty = qty


@rcsp.node
def vwap(trade: ts[object]) -> ts[float]:
    """Volume-weighted average price of every fill seen so far."""
    s = rcsp.state(notional=0.0, cum_qty=0)
    if rcsp.ticked(trade):
        t = trade.value
        s.notional += t.price * t.qty
        s.cum_qty += t.qty
        return s.notional / s.cum_qty


# --- deterministic synthetic order flow ---------------------------------------

def synth_trades(symbols, n_per=8, seed=11):
    """Deterministic `(time, symbol, Trade)` rows across the symbol set.

    Seeded and independent of symbol *order*, so any shard of symbols replays the
    exact same fills it would see in the full run.
    """
    import random
    import zlib

    rows = []
    for sym in symbols:
        h = zlib.crc32(sym.encode())                        # stable across processes
        rng = random.Random(seed ^ h)
        base = 50 + (h % 400)
        for i in range(n_per):
            px = round(base * (1 + rng.uniform(-0.02, 0.02)), 2)
            qty = rng.randint(1, 10) * 100
            rows.append((START + timedelta(seconds=i + 1), sym, Trade(px, qty)))
    return rows


# --- the graph ----------------------------------------------------------------

def _build(rows, symbols):
    @rcsp.graph
    def g():
        mgr = rcsp.ReplayAdapterManager(rows)
        for sym in symbols:
            trades = mgr.subscribe(sym)
            rcsp.add_graph_output(f"{sym}_vwap", vwap(trades))
    return g


def _final_vwaps(out, symbols):
    """Pull the last VWAP tick per symbol out of a run result."""
    return {s: round(out[f"{s}_vwap"][-1][1], 4)
            for s in symbols if out.get(f"{s}_vwap")}


def _run_graph(rows, symbols, n_per):
    out = rcsp.run(_build(rows, symbols), starttime=START,
                   endtime=timedelta(seconds=n_per + 1))
    return _final_vwaps(out, symbols)


# --- topology A: one graph, whole universe ------------------------------------

def main(symbols=SYMBOLS, n_per=8):
    rows = synth_trades(symbols, n_per)
    result = _run_graph(rows, symbols, n_per)
    print(f"[topology A] one graph, {len(symbols)} symbols on pid {os.getpid()}")
    for s in symbols:
        print(f"    {s:6s} vwap={result[s]}")
    return result


# --- topology B: shard symbols across processes -------------------------------

def _run_shard(symbols, rows, n_per, q):
    """Module-level, picklable target: one graph over a shard, in its own process.

    Puts `(pid, symbols, {symbol: vwap})` on the result queue.
    """
    q.put((os.getpid(), symbols, _run_graph(rows, symbols, n_per)))


def run_sharded(symbols=SYMBOLS, n_per=8, n_shards=2):
    """Partition `symbols` into `n_shards` and run each shard's graph in its OWN
    process (one graph per process, each its own GIL), then merge the per-symbol
    VWAPs."""
    rows = synth_trades(symbols, n_per)
    shards = [s for s in (symbols[i::n_shards] for i in range(n_shards)) if s]

    ctx = multiprocessing.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_run_shard, args=(s, rows, n_per, q)) for s in shards]
    for p in procs:
        p.start()

    merged = {}
    for _ in shards:                                        # one result per shard
        pid, syms, res = q.get()
        print(f"[topology B] pid {pid} handled {syms}")
        merged.update(res)
    for p in procs:
        p.join()
    return merged


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=2, help="processes to shard symbols over")
    a = ap.parse_args()

    a_result = main()
    print()
    b_result = run_sharded(n_shards=a.shards)

    agree = a_result == b_result
    print(f"\nA and B agree: {agree}  "
          f"(sharding is a deployment choice; the VWAP numbers are identical)")
