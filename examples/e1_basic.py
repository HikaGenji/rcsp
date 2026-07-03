"""The canonical CSP first-graph example, in rcsp.

Mirrors https://github.com/Point72/csp — a node that adds two constants.
"""

from datetime import datetime

import rcsp
from rcsp import ts


@rcsp.node
def add(x: ts[int], y: ts[int]) -> ts[int]:
    if rcsp.ticked(x, y) and rcsp.valid(x, y):
        return x + y


@rcsp.graph
def my_graph():
    x = rcsp.const(1)
    y = rcsp.const(2)

    total = add(x, y)

    rcsp.print("x", x)
    rcsp.print("y", y)
    rcsp.print("sum", total)


def main():
    rcsp.run(my_graph, starttime=datetime(2020, 1, 1))


if __name__ == "__main__":
    main()
