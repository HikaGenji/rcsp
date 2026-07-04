"""Realtime input adapters (a small slice of ``csp.adapters``).

:class:`GenericPushAdapter` lets an external thread push values into a running
realtime graph, exactly like ``csp.GenericPushAdapter``. Values are placed on a
thread-safe queue that the Rust engine drains from its realtime loop.
"""

import queue
import threading

from ._graph import Edge, current_builder


class GenericPushAdapter:
    """Push external, thread-produced data into a realtime graph.

    Example::

        adapter = rcsp.GenericPushAdapter(int)
        driver = MyThread(adapter)   # calls adapter.push_tick(...) in a loop
        driver.start()
        rcsp.print("data", adapter.out())
    """

    def __init__(self, typ=None):
        self._builder = current_builder()
        self._queue = queue.Queue()
        self._wakeup = self._builder.wakeup   # wakes the engine loop on push
        self._started = threading.Event()
        self._edge = self._builder.engine.new_edge()
        self._builder.engine.register_push_adapter(self._edge, self._queue)
        self._builder.push_adapters.append(self)

    def out(self):
        """The time-series edge carrying pushed values."""
        return Edge(self._builder, self._edge)

    def push_tick(self, value):
        """Push a value into the graph (thread-safe)."""
        self._queue.put(value)
        self._wakeup.put(None)   # wake the realtime loop immediately

    def wait_for_start(self, timeout=None):
        """Block until the engine has started (or ``timeout`` seconds pass)."""
        return self._started.wait(timeout)

    def _signal_start(self):
        self._started.set()


def schedule_on_engine_stop(callback):
    """Register ``callback`` to run once, after the engine finishes."""
    current_builder().stop_callbacks.append(callback)
