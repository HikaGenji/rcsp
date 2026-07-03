"""Pushing realtime data into a graph from another thread.

Port of CSP's ``examples/04_writing_adapters/e1_generic_push_adapter.py``. A
background :class:`Driver` thread pushes an incrementing counter into the graph
once per second via ``rcsp.GenericPushAdapter``; the engine, running in
``realtime=True`` mode, drains the pushes and ticks them through the graph.
"""

import threading
import time
from datetime import datetime, timedelta, timezone

import rcsp


class Driver(threading.Thread):
    def __init__(self, adapter):
        super().__init__(daemon=True)
        self._adapter = adapter
        self._running = True

    def run(self):
        self._adapter.wait_for_start()
        counter = 0
        while self._running:
            counter += 1
            self._adapter.push_tick(counter)
            time.sleep(1.0)

    def stop(self):
        self._running = False


@rcsp.graph
def my_graph():
    adapter = rcsp.GenericPushAdapter(int)
    driver = Driver(adapter)
    driver.start()

    rcsp.print("pushed", adapter.out())
    rcsp.add_graph_output("pushed", adapter.out())

    # Ensure the driver thread is stopped when the engine finishes.
    rcsp.schedule_on_engine_stop(driver.stop)


def main():
    out = rcsp.run(
        my_graph,
        starttime=datetime.now(timezone.utc),
        endtime=timedelta(seconds=3),
        realtime=True,
    )
    print("\nreceived", len(out["pushed"]), "pushed ticks:", [v for _, v in out["pushed"]])


if __name__ == "__main__":
    main()
