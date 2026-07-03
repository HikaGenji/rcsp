"""Rolling-window statistics on a price series.

Port of CSP's ``examples/02_intermediate/e2_stats.py``. Replays a synthetic
price curve and computes a rolling mean and standard deviation over a trailing
time window, then derives Bollinger-style bands (mean ± 2·stddev) — a classic
use of ``csp.stats``.
"""

from datetime import datetime, timedelta

import rcsp
from rcsp import stats, ts


@rcsp.node
def bands(mean: ts[float], sd: ts[float], k: float) -> rcsp.Outputs(upper=ts[float], lower=ts[float]):
    if rcsp.valid(mean, sd):
        rcsp.output(upper=mean.value + k * sd.value)
        rcsp.output(lower=mean.value - k * sd.value)


def make_prices(start, n):
    price = 100.0
    data = []
    for i in range(1, n + 1):
        price += (1 if i % 3 else -2) * 0.5
        data.append((start + timedelta(seconds=i), round(price, 2)))
    return data


@rcsp.graph
def my_graph():
    st = datetime(2020, 1, 1)
    prices = rcsp.curve(float, make_prices(st, 20))

    window = timedelta(seconds=5)
    avg = stats.mean(prices, window, min_data_points=2)
    sd = stats.stddev(prices, window, min_data_points=2)

    b = bands(avg, sd, 2.0)

    rcsp.print("price", prices)
    rcsp.print("mean", avg)
    rcsp.print("stddev", sd)
    rcsp.add_graph_output("mean", avg)
    rcsp.add_graph_output("upper", b.upper)
    rcsp.add_graph_output("lower", b.lower)


def main():
    out = rcsp.run(my_graph, starttime=datetime(2020, 1, 1), endtime=timedelta(seconds=20))
    print("\nrolling mean points:", len(out["mean"]))
    if out["mean"]:
        _, m = out["mean"][-1]
        _, u = out["upper"][-1]
        _, low = out["lower"][-1]
        print(f"final band: lower={low:.2f}  mean={m:.2f}  upper={u:.2f}")


if __name__ == "__main__":
    main()
