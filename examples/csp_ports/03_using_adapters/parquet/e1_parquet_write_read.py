"""Writing and reading Parquet.

Port of CSP's ``examples/03_using_adapters/parquet/e1_parquet_write_read.py``.
One graph computes a series and writes it to Parquet via an output adapter; a
second graph replays that Parquet file back into the engine with a pull adapter.
"""

import tempfile
from datetime import datetime, timedelta

import rcsp
from rcsp import ts


@rcsp.node
def cumulative(x: ts[float]) -> ts[float]:
    s = rcsp.state(total=0.0)
    if rcsp.ticked(x):
        s.total += x
        return s.total


@rcsp.graph
def write_graph(path):
    x = rcsp.count(rcsp.timer(timedelta(seconds=1), 1)) * 1.0
    cum = cumulative(x)
    rcsp.write_parquet(path, value=x, cumulative=cum)


@rcsp.graph
def read_graph(path):
    cols = rcsp.read_parquet(path, value_cols=["value"])
    rcsp.print("replayed", cols["value"])
    rcsp.add_graph_output("replayed", cols["value"])


def main():
    path = tempfile.mktemp(suffix=".parquet")

    rcsp.run(write_graph, path, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=5))

    import polars as pl
    print("wrote Parquet:")
    print(pl.read_parquet(path))

    print("\nreplaying it back through the engine:")
    out = rcsp.run(read_graph, path, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=6))
    print("replayed values:", [v for _, v in out["replayed"]])


if __name__ == "__main__":
    main()
