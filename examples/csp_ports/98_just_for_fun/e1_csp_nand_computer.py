"""A computer built from a single primitive — the NAND gate.

Port of CSP's ``examples/98_just_for_fun/e1_csp_nand_computer.py``. There is
exactly ONE node in this program: :func:`nand`. Every other "gate" is a
``@graph`` that only wires NAND nodes together — at runtime the sole thing that
executes is the NAND node. We build up to a 16-bit ripple-carry adder and add
42001 + 136 entirely out of NAND gates.
"""

from datetime import datetime

import rcsp
from rcsp import ts

WIDTH = 16


# ---- the one and only node in this application ----
@rcsp.node
def nand(a: ts[bool], b: ts[bool]) -> ts[bool]:
    if rcsp.ticked(a, b) and rcsp.valid(a, b):
        return not (bool(a) and bool(b))


# ---- gates, built purely by wiring NAND nodes ----
@rcsp.graph
def not_(a):
    return nand(a, a)


@rcsp.graph
def and_(a, b):
    return not_(nand(a, b))


@rcsp.graph
def or_(a, b):
    return nand(not_(a), not_(b))


@rcsp.graph
def xor_(a, b):
    return and_(or_(a, b), nand(a, b))


# ---- adders ----
@rcsp.graph
def half_adder(a, b):
    return xor_(a, b), and_(a, b)  # (sum, carry)


@rcsp.graph
def full_adder(a, b, cin):
    s1, c1 = half_adder(a, b)
    s2, c2 = half_adder(s1, cin)
    return s2, or_(c1, c2)  # (sum, carry_out)


@rcsp.graph
def ripple_adder(a_bits, b_bits):
    carry = rcsp.const(False)
    sum_bits = []
    for a, b in zip(a_bits, b_bits):
        s, carry = full_adder(a, b, carry)
        sum_bits.append(s)
    return sum_bits, carry  # LSB-first bits, final carry


# ---- integer <-> bit-basket helpers ----
def number_to_bits(n, width=WIDTH):
    """LSB-first list of const bit edges."""
    return [rcsp.const(bool((n >> i) & 1)) for i in range(width)]


@rcsp.graph
def build(x, y):
    a_bits = number_to_bits(x)
    b_bits = number_to_bits(y)
    sum_bits, carry = ripple_adder(a_bits, b_bits)
    for i, bit in enumerate(sum_bits):
        rcsp.add_graph_output(f"bit{i}", bit)
    rcsp.add_graph_output("carry", carry)


def main():
    x, y = 42001, 136
    out = rcsp.run(build, x, y, starttime=datetime(2020, 1, 1))

    # Reconstruct the integer from the output bits.
    result = 0
    for i in range(WIDTH):
        if out[f"bit{i}"][0][1]:
            result |= 1 << i
    if out["carry"][0][1]:
        result |= 1 << WIDTH

    print(f"{x} + {y} = {result}   (via {WIDTH}-bit NAND adder)")
    print(f"binary: {x:016b} + {y:016b} = {result:016b}")
    assert result == x + y, "NAND adder produced the wrong sum!"
    print("OK — arithmetic emerged entirely from NAND gates.")


if __name__ == "__main__":
    main()
