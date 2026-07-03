"""NumPy rolling stats + Polars output.

Port of CSP's ``examples/02_intermediate/e3_numpy_stats.py``. NumPy arrays flow
through rcsp edges unchanged, so a node can keep a window and compute NumPy
statistics on it. The run result is then collected into a Polars DataFrame.
"""

from datetime import datetime, timedelta

import numpy as np

import rcsp
from rcsp import ts


@rcsp.node
def np_stats(x: ts[float], window: int) -> rcsp.Outputs(mean=ts[float], std=ts[float]):
    s = rcsp.state(buf=None)
    if s.buf is None:
        s.buf = []
    if rcsp.ticked(x):
        s.buf.append(x.value)
        arr = np.array(s.buf[-window:])
        rcsp.output(mean=float(np.mean(arr)))
        rcsp.output(std=float(np.std(arr)))


@rcsp.graph
def my_graph():
    prices = rcsp.curve(
        float,
        [(timedelta(seconds=i), 100.0 + np.sin(i / 2.0) * 5.0) for i in range(1, 21)],
    )
    r = np_stats(prices, 5)
    rcsp.add_graph_output("price", prices)
    rcsp.add_graph_output("mean", r.mean)
    rcsp.add_graph_output("std", r.std)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=20))
    df = rcsp.to_polars_wide(out)
    print(df.tail(5))


if __name__ == "__main__":
    main()
