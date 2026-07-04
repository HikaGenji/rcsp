"""Multi-symbol VWAP: correctness + the deployment invariant (A == B)."""

import os
import sys

# Import the example by its real module name (not importlib), and put examples/
# on sys.path — the spawned shard processes propagate sys.path and re-import it.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "examples")))
import e8_multi_symbol_vwap as mod  # noqa: E402


def _expected_vwaps(symbols, n_per):
    """Hand-computed sum(price*qty)/sum(qty) per symbol from the same rows."""
    rows = mod.synth_trades(symbols, n_per)
    acc = {}
    for _t, sym, trade in rows:
        notional, qty = acc.get(sym, (0.0, 0))
        acc[sym] = (notional + trade.price * trade.qty, qty + trade.qty)
    return {s: round(notional / qty, 4) for s, (notional, qty) in acc.items()}


def test_single_graph_vwap_correct():
    syms = ["AAPL", "MSFT", "GOOG"]
    got = mod.main(symbols=syms, n_per=6)
    assert got == _expected_vwaps(syms, 6)


def test_sharded_equals_single_graph():
    # Deployment invariant: sharding symbols across processes yields the same
    # VWAPs as running the whole universe in one graph.
    syms = ["AAPL", "MSFT", "GOOG", "AMZN"]
    single = mod.main(symbols=syms, n_per=6)
    sharded = mod.run_sharded(symbols=syms, n_per=6, n_shards=2)
    assert sharded == single
