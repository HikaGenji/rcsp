"""VWAP and PnL over a stream of trades.

Port of CSP's ``examples/01_basics/e4_trade_pnl.py``. Trades (as dataclasses,
rcsp's stand-in for ``csp.Struct``) are split into buys and sells; each side's
VWAP is computed with a stateful node, and PnL is marked against the bid/ask
mid. Demonstrates ``rcsp.split``, stateful multi-output nodes, and structs.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@dataclass
class Trade:
    price: float
    qty: int
    buy: bool


@rcsp.node
def field(obj: ts[object], name: str) -> ts[object]:
    if rcsp.ticked(obj):
        return getattr(obj.value, name)


@rcsp.node
def vwap(trade: ts[object]) -> rcsp.Outputs(vwap=ts[float], qty=ts[int]):
    s = rcsp.state(notional=0.0, cum_qty=0)
    if rcsp.ticked(trade):
        t = trade.value
        s.notional += t.price * t.qty
        s.cum_qty += t.qty
        rcsp.output(vwap=s.notional / s.cum_qty, qty=s.cum_qty)


@rcsp.node
def pnl(avg_px: ts[float], qty: ts[int], mark: ts[float], is_buy: bool) -> ts[float]:
    if rcsp.valid(avg_px, qty, mark):
        direction = 1.0 if is_buy else -1.0
        return direction * (mark - avg_px) * qty


@rcsp.graph
def my_graph():
    st = datetime(2020, 1, 1)

    bid = rcsp.curve(float, [(st + timedelta(seconds=i), 99.0 + i * 0.1) for i in range(1, 7)])
    ask = rcsp.curve(float, [(st + timedelta(seconds=i), 101.0 + i * 0.1) for i in range(1, 7)])
    mark = (bid + ask) / 2.0

    trades = rcsp.curve(
        Trade,
        [
            (st + timedelta(seconds=1), Trade(100.0, 100, True)),
            (st + timedelta(seconds=2), Trade(101.0, 200, False)),
            (st + timedelta(seconds=3), Trade(99.5, 150, True)),
            (st + timedelta(seconds=4), Trade(102.0, 100, False)),
            (st + timedelta(seconds=5), Trade(100.5, 300, True)),
            (st + timedelta(seconds=6), Trade(101.5, 250, False)),
        ],
    )

    sides = rcsp.split(field(trades, "buy"), trades)
    buys, sells = sides.true, sides.false

    buy_vwap = vwap(buys)
    sell_vwap = vwap(sells)

    buy_pnl = pnl(buy_vwap.vwap, buy_vwap.qty, mark, True)
    sell_pnl = pnl(sell_vwap.vwap, sell_vwap.qty, mark, False)
    total_pnl = buy_pnl + sell_pnl

    rcsp.print("buy_vwap", buy_vwap.vwap)
    rcsp.print("sell_vwap", sell_vwap.vwap)
    rcsp.print("total_pnl", total_pnl)
    rcsp.add_graph_output("total_pnl", total_pnl)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=6))
    print("\nfinal total PnL:", round(out["total_pnl"][-1][1], 2))


if __name__ == "__main__":
    main()
