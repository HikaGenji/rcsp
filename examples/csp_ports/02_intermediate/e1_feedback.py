"""Feedback edges — a trading loop between an algo and an exchange.

Port of CSP's ``examples/02_intermediate/e1_feedback.py``. The algo sends orders
and the exchange acknowledges them after a delay; the exec reports are fed
*back* into the algo with ``rcsp.feedback`` so it only sends the next order once
the previous one is acknowledged. Without feedback this cycle couldn't be
expressed in a DAG.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@dataclass
class Order:
    id: int
    price: float
    qty: int


@dataclass
class ExecReport:
    id: int
    price: float
    status: str


@rcsp.node(alarms=["ack"])
def my_exchange(order: ts[object]) -> ts[object]:
    """Acknowledge each order 0.7s after receiving it."""
    if rcsp.ticked(order):
        rcsp.schedule_alarm("ack", timedelta(seconds=0.7), order.value)
    if rcsp.alarmed("ack"):
        o = rcsp.alarm_value("ack")
        return ExecReport(id=o.id, price=o.price, status="FILLED")


@rcsp.node(alarms=["send"])
def my_algo(exec_report: ts[object]) -> ts[object]:
    """Send an order at start, then one more each time an ack arrives."""
    s = rcsp.state(next_id=0, price=100.0)
    if rcsp.starting():
        rcsp.schedule_alarm("send", timedelta(seconds=1), True)
    if rcsp.ticked(exec_report):
        rcsp.schedule_alarm("send", timedelta(seconds=1), True)
    if rcsp.alarmed("send"):
        s.next_id += 1
        s.price += 0.01
        return Order(id=s.next_id, price=round(s.price, 2), qty=100)


@rcsp.graph
def my_graph():
    exec_report_fb = rcsp.feedback(ExecReport)

    orders = my_algo(exec_report_fb.out())
    exec_report = my_exchange(orders)

    exec_report_fb.bind(exec_report)

    rcsp.print("order", orders)
    rcsp.print("exec_report", exec_report)
    rcsp.add_graph_output("orders", orders)
    rcsp.add_graph_output("reports", exec_report)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=6))
    print("\norders sent:", len(out["orders"]), "acks received:", len(out["reports"]))


if __name__ == "__main__":
    main()
