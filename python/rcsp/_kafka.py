"""Kafka input/output adapters (a slice of ``csp.adapters.kafka``).

:class:`KafkaAdapterManager` coordinates streaming from/to Kafka:

* ``subscribe(topic)`` runs a background consumer that pushes messages into the
  graph (built on :class:`rcsp.GenericPushAdapter`, so it needs ``realtime=True``).
* ``publish(topic, edge)`` sends each tick of ``edge`` to a topic.

The Kafka client is created lazily via ``kafka-python`` (``pip install
rcsp[kafka]``). For testing without a broker, inject a ``consumer`` / ``producer``
that mimics the ``kafka-python`` ``poll()`` / ``send()`` interface.
"""

import json
import queue
import threading
import time
import types

from ._adapters import GenericPushAdapter, schedule_on_engine_stop
from ._node import node, ticked
from ._types import ts


def _default_deserializer(raw):
    if isinstance(raw, (bytes, bytearray)):
        try:
            return json.loads(raw)
        except Exception:
            return raw.decode(errors="replace")
    return raw


def _default_serializer(value):
    if isinstance(value, (bytes, bytearray)):
        return value
    if isinstance(value, str):
        return value.encode()
    return json.dumps(value).encode()


def _make_consumer(topic, broker, group_id):
    from kafka import KafkaConsumer

    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="latest",
    )


def _make_producer(broker):
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=broker)


class _ConsumerDriver(threading.Thread):
    """Polls a Kafka consumer and pushes deserialized values into the graph."""

    def __init__(self, consumer, adapter, deserializer, poll_timeout_ms):
        super().__init__(daemon=True)
        self._consumer = consumer
        self._adapter = adapter
        self._deser = deserializer
        self._timeout = poll_timeout_ms
        self._running = True

    def run(self):
        self._adapter.wait_for_start()
        while self._running:
            batch = self._consumer.poll(timeout_ms=self._timeout)
            if not batch:
                continue
            for records in batch.values():
                for record in records:
                    self._adapter.push_tick(self._deser(record.value))

    def stop(self):
        self._running = False
        try:
            self._consumer.close()
        except Exception:
            pass


@node
def _kafka_publish(x: ts[object], topic, producer, serializer) -> None:
    if ticked(x):
        producer.send(topic, serializer(x.value))


class KafkaAdapterManager:
    """Coordinate Kafka input/output for a graph.

    Example::

        mgr = rcsp.KafkaAdapterManager(broker="localhost:9092")
        quotes = mgr.subscribe("quotes")
        rcsp.add_graph_output("q", quotes)
        mgr.publish("signals", my_signal)
        rcsp.run(g, ..., realtime=True)
    """

    def __init__(self, broker="localhost:9092", *, group_id=None,
                 consumer=None, producer=None, poll_timeout_ms=200):
        self._broker = broker
        self._group_id = group_id
        self._consumer = consumer   # injected shared consumer (e.g. for tests)
        self._producer = producer
        self._poll_timeout_ms = poll_timeout_ms

    def subscribe(self, topic, value_deserializer=None):
        """Return a time series of messages consumed from ``topic``."""
        adapter = GenericPushAdapter(object)
        consumer = self._consumer or _make_consumer(topic, self._broker, self._group_id)
        driver = _ConsumerDriver(
            consumer, adapter, value_deserializer or _default_deserializer,
            self._poll_timeout_ms,
        )
        driver.start()
        schedule_on_engine_stop(driver.stop)
        return adapter.out()

    def publish(self, topic, edge, value_serializer=None):
        """Send each tick of ``edge`` to ``topic``."""
        producer = self._producer or _make_producer(self._broker)
        _kafka_publish(edge, topic, producer, value_serializer or _default_serializer)

        def _flush():
            try:
                producer.flush()
            except Exception:
                pass

        schedule_on_engine_stop(_flush)


class InMemoryKafka:
    """A tiny in-process stand-in for a Kafka broker, for tests and demos.

    Provides ``producer()`` / ``consumer(topic)`` objects with the same
    ``send`` / ``poll`` surface the adapters use, so a publish→subscribe round
    trip (including serialization) works with no external broker::

        broker = rcsp.InMemoryKafka()
        broker.preload("quotes", [{"px": 1}, {"px": 2}])
        mgr = rcsp.KafkaAdapterManager(consumer=broker.consumer("quotes"))
    """

    def __init__(self):
        self._queues = {}
        self._lock = threading.Lock()

    def _q(self, topic):
        with self._lock:
            return self._queues.setdefault(topic, queue.Queue())

    def preload(self, topic, values):
        for v in values:
            self._q(topic).put(v if isinstance(v, (bytes, bytearray)) else json.dumps(v).encode())

    def messages(self, topic):
        """Drain and return everything currently queued on ``topic`` (bytes)."""
        out = []
        try:
            while True:
                out.append(self._q(topic).get_nowait())
        except queue.Empty:
            pass
        return out

    def producer(self):
        broker = self

        class _Producer:
            def send(self, topic, value):
                broker._q(topic).put(value)

            def flush(self):
                pass

            def close(self):
                pass

        return _Producer()

    def consumer(self, topic):
        broker = self

        class _Consumer:
            def poll(self, timeout_ms=0):
                q = broker._q(topic)
                out = []
                try:
                    while True:
                        out.append(q.get_nowait())
                except queue.Empty:
                    pass
                if not out:
                    time.sleep(timeout_ms / 1000.0)
                    return {}
                return {topic: [types.SimpleNamespace(value=v) for v in out]}

            def close(self):
                pass

        return _Consumer()
