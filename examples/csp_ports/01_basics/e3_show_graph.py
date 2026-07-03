"""Visualizing a graph's structure.

Port of CSP's ``examples/01_basics/e3_show_graph.py``. Builds a bid/ask spread
graph and renders it with ``rcsp.show_graph`` — an image if the Graphviz ``dot``
binary is installed, otherwise the DOT source. Also prints the DOT and Mermaid
text, which need no external tools.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def spread(bid: ts[float], ask: ts[float]) -> ts[float]:
    if rcsp.valid(bid, ask):
        return ask - bid


@rcsp.graph
def bid_ask_graph():
    bid = rcsp.timer(timedelta(seconds=1), 99.9)
    ask = rcsp.timer(timedelta(seconds=1), 100.1)

    mid = (bid + ask) / 2.0
    sprd = spread(bid, ask)

    rcsp.print("mid", mid)
    rcsp.print("spread", sprd)


def main():
    print("=== DOT ===")
    print(rcsp.graph_to_dot(bid_ask_graph))
    print("\n=== Mermaid ===")
    print(rcsp.graph_to_mermaid(bid_ask_graph))

    out = rcsp.show_graph(bid_ask_graph, filename="bid_ask_graph.png")
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
