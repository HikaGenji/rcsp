"""A small market-data graph: mid-price and a weighted spread signal.

Demonstrates ``curve`` adapters replaying historical bids/asks, edge arithmetic
compiled to native binop kernels, and a multi-output node — the kind of graph
CSP is built for. The diamond (bid/ask both feed mid and spread) resolves
glitch-free: downstream nodes tick once per input cycle with consistent values.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def quote_signal(mid: ts[float], spread: ts[float]) -> rcsp.Outputs(
    tradable=ts[bool], mid_out=ts[float]
):
    """Emit the mid, and a boolean 'tradable' when the spread is tight."""
    if rcsp.valid(mid, spread):
        rcsp.output(mid_out=mid.value)
        rcsp.output(tradable=spread.value < 0.05)


@rcsp.graph
def market_graph():
    bid = rcsp.curve(
        float,
        [
            (timedelta(seconds=1), 99.90),
            (timedelta(seconds=2), 99.95),
            (timedelta(seconds=3), 100.00),
        ],
    )
    ask = rcsp.curve(
        float,
        [
            (timedelta(seconds=1), 100.10),
            (timedelta(seconds=2), 99.98),
            (timedelta(seconds=3), 100.02),
        ],
    )

    mid = (bid + ask) / 2.0     # native binop kernels
    spread = ask - bid

    sig = quote_signal(mid, spread)

    rcsp.print("mid", mid)
    rcsp.print("spread", spread)
    rcsp.print("tradable", sig.tradable)

    rcsp.add_graph_output("mid", mid)
    rcsp.add_graph_output("spread", spread)
    rcsp.add_graph_output("tradable", sig.tradable)


def main():
    out = rcsp.run(
        market_graph,
        starttime=datetime(2020, 1, 1),
        endtime=timedelta(seconds=5),
    )
    print("\nmid series:", [round(v, 4) for _, v in out["mid"]])
    print("tradable:  ", [v for _, v in out["tradable"]])


if __name__ == "__main__":
    main()
