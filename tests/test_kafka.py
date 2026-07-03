"""Tests for the Kafka adapters, using the in-memory broker double."""

import json
from datetime import datetime, timedelta, timezone

import rcsp


def _now():
    return datetime.now(timezone.utc)


def test_kafka_input_consumes_all_messages():
    broker = rcsp.InMemoryKafka()
    broker.preload("quotes", [{"px": 1}, {"px": 2}, {"px": 3}])

    @rcsp.graph
    def g():
        q = rcsp.KafkaAdapterManager(consumer=broker.consumer("quotes"),
                                     poll_timeout_ms=20).subscribe("quotes")
        rcsp.add_graph_output("q", q)

    out = rcsp.run(g, starttime=_now(), endtime=timedelta(seconds=0.4), realtime=True)
    assert [v["px"] for _, v in out["q"]] == [1, 2, 3]


def test_kafka_output_publishes_each_tick():
    broker = rcsp.InMemoryKafka()

    @rcsp.graph
    def g():
        x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))   # 1,2,3
        rcsp.KafkaAdapterManager(producer=broker.producer()).publish("signals", x)

    rcsp.run(g, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=3))
    assert [json.loads(m) for m in broker.messages("signals")] == [1, 2, 3]


def test_kafka_round_trip():
    broker = rcsp.InMemoryKafka()

    @rcsp.graph
    def produce():
        x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1))
        rcsp.KafkaAdapterManager(producer=broker.producer()).publish("t", x)

    rcsp.run(produce, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=3))

    @rcsp.graph
    def consume():
        q = rcsp.KafkaAdapterManager(consumer=broker.consumer("t"),
                                     poll_timeout_ms=20).subscribe("t")
        rcsp.add_graph_output("back", q)

    out = rcsp.run(consume, starttime=_now(), endtime=timedelta(seconds=0.4), realtime=True)
    assert [v for _, v in out["back"]] == [1, 2, 3]
