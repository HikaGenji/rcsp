"""Streaming from/to Kafka.

Port of CSP's ``examples/03_using_adapters/kafka/e1_kafka.py``. A
``KafkaAdapterManager`` consumes quotes from one topic, computes a rolling mean,
and publishes the signal back to another topic.

This example runs against an in-process :class:`rcsp.InMemoryKafka` broker so it
works with no external services. To point at a real cluster, replace the two
manager constructions with ``rcsp.KafkaAdapterManager(broker="host:9092")`` — the
graph itself doesn't change. (Real brokers need ``pip install rcsp[kafka]``.)
"""

from datetime import datetime, timedelta, timezone

import rcsp
from rcsp import stats, ts


@rcsp.node
def to_price(msg: ts[object]) -> ts[float]:
    if rcsp.ticked(msg):
        return float(msg.value["px"])


@rcsp.graph
def trading_graph(in_mgr, out_mgr):
    quotes = in_mgr.subscribe("quotes")
    price = to_price(quotes)
    signal = stats.mean(price, 3)          # rolling mean over last 3 quotes

    rcsp.print("price", price)
    rcsp.print("signal", signal)
    rcsp.add_graph_output("signal", signal)

    out_mgr.publish("signals", signal)     # publish the signal downstream


def main():
    broker = rcsp.InMemoryKafka()
    broker.preload("quotes", [{"px": 100.0}, {"px": 101.0}, {"px": 102.0}, {"px": 99.0}])

    in_mgr = rcsp.KafkaAdapterManager(consumer=broker.consumer("quotes"), poll_timeout_ms=20)
    out_mgr = rcsp.KafkaAdapterManager(producer=broker.producer())

    out = rcsp.run(
        trading_graph, in_mgr, out_mgr,
        starttime=datetime.now(timezone.utc),
        endtime=timedelta(seconds=0.5),
        realtime=True,
    )
    print("\nsignals produced:", [round(v, 3) for _, v in out["signal"]])
    print("published to 'signals' topic:", len(broker.messages("signals")), "messages")


if __name__ == "__main__":
    main()
