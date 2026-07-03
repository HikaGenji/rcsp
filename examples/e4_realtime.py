"""The same graph, in real time.

With ``realtime=True`` the engine paces itself against the wall clock, so a
1-second timer actually fires once per real second. The identical graph runs in
simulation (as fast as possible) when ``realtime=False`` — CSP's core promise of
one code path for backtest and production.
"""

from datetime import datetime, timedelta

import rcsp


@rcsp.graph
def clock_graph():
    beat = rcsp.timer(timedelta(seconds=1), 1)
    n = rcsp.count(beat)
    rcsp.print("beat", n)
    rcsp.add_graph_output("beat", n)


def main(realtime=True):
    start = datetime.utcnow()
    out = rcsp.run(
        clock_graph,
        starttime=start,
        endtime=timedelta(seconds=3),
        realtime=realtime,
    )
    print("received", len(out["beat"]), "beats")


if __name__ == "__main__":
    main(realtime=True)
