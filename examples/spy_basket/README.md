# SPY vs. constituents — ETF/basket arbitrage

A complete streaming simulation: trade **SPY** against its ~500 constituents. SPY's
**fair value** is derived from the constituent **weights × return since open**
(`idx = Σ wᵢ·Pᵢ/Oᵢ`, `fair = spy_open·idx`); we **buy SPY when it's cheaper than
fair by a margin** and sell when it's richer, tracking a 1-lot position and PnL.

## Why it's a good rcsp example

There is no N-ary combine in rcsp and a `@node` has fixed input arity, so the
500-name basket is folded the way a real feed handler does it: **one multiplexed
`{symbol, mid}` quote stream into a single stateful node** that maintains the index
incrementally in **O(1) per quote**. SPY rides the same stream as symbol `"SPY"`,
so the simulation graph (`curve`) and the realtime graph (`GenericPushAdapter`)
share the identical node.

## Files

| File | What |
|---|---|
| `basket_arb.py` | all logic: `load_weights`, `synth_feed`, `build_graph`, the `basket_engine` node, `simulate`, `profile_run`, `realtime_latency`, `producer_ceiling`, `main` |
| `spy_basket.ipynb` | the notebook — loads weights, builds the feed, runs the sim, plots fair-vs-mid / edge / PnL, and reports profiling + latency/throughput |
| `sp500_weights.csv` | bundled weight snapshot (see data note below) |

## Run

```bash
# the module (prints trades, throughput, per-node profile, latency, producer ceiling)
python examples/spy_basket/basket_arb.py --n-stocks 500 --quotes-per-sec 100 --duration 2.0

# the notebook (needs jupyter + matplotlib)
pip install nbconvert matplotlib ipykernel
jupyter nbconvert --to notebook --execute examples/spy_basket/spy_basket.ipynb
```

Representative single-core output (500 stocks × 100 quotes/s over 2s ≈ 100k quotes):

```
throughput: 100100 quotes in 0.78s = 128k quotes/s (single core)
profiler:   basket_engine count=100100 avg=3.9µs/quote  (O(1) fold of 500 names)
realtime reaction: median≈105µs p90≈145µs p99≈180µs
producer ceiling: ~130k ticks/s (kept_up=True); feed needs 50k/s
```

## Configurable feed

`synth_feed(weights, quotes_per_sec_per_stock=100, n_stocks=500, duration=2.0,
spy_quotes_per_sec=50, margin_bps=5.0, ...)` builds a deterministic, correlated
market-data feed (shared market factor × per-stock beta + idiosyncratic noise) with
injected dislocations so the strategy trades. Every quote gets a **distinct
microsecond timestamp** — a single edge holds one value per engine cycle, so ticks
must not share a timestamp; the combined rate is asserted ≤ 1e6 quotes/s.

## Latency / throughput

- **Engine throughput** (simulation) = quotes ÷ wall-clock — one core folds the
  whole 500-name basket at ~10⁵ quotes/s.
- **Per-node µs** from `rcsp.profiler.Profiler` — `basket_engine` runs once per
  quote at a few µs, proving the O(1) fold.
- **Realtime reaction** (`realtime=True`, event-driven) — quote→node latency in the
  tens–hundreds of µs (Python `@node` on the hot path, so not the native GIL-free
  mode).
- **Producer ceiling** (`rcsp.producer_benchmark`) — a single Python producer vs.
  the 500×100 = 50k quotes/s the feed needs. Above that, shard producers across
  processes (see [`../e8_multi_symbol_vwap.py`](../e8_multi_symbol_vwap.py)) or use
  a native producer.

## Data note

Real S&P 500 **weights** aren't reachable from this sandbox (SSGA / slickcharts are
blocked by the proxy). GitHub raw *is* reachable, so `sp500_weights.csv` is a
**modeled snapshot** — real mega-cap top weights plus a cap-weight power-law tail —
built from the live ticker list. `load_weights(live=True)` re-fetches the tickers
and re-derives the weights where the network is open. The strategy math only needs a
realistic normalized weight vector; swap in true SSGA weights for production.
