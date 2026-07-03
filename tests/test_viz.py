"""Tests for graph visualization (topology, DOT, Mermaid)."""

from datetime import timedelta

import rcsp
from rcsp import ts


@rcsp.node
def spread(bid: ts[float], ask: ts[float]) -> ts[float]:
    if rcsp.valid(bid, ask):
        return ask - bid


@rcsp.graph
def _g():
    bid = rcsp.timer(timedelta(seconds=1), 99.9)
    ask = rcsp.timer(timedelta(seconds=1), 100.1)
    mid = (bid + ask) / 2.0
    rcsp.print("mid", mid)
    rcsp.print("spread", spread(bid, ask))


def test_dot_has_nodes_and_edges():
    dot = rcsp.graph_to_dot(_g)
    assert dot.startswith("digraph rcsp")
    assert 'label="spread"' in dot          # the user node appears
    assert 'label="+"' in dot               # binop labelled by operator
    assert "->" in dot                      # at least one edge


def test_mermaid_flowchart():
    m = rcsp.graph_to_mermaid(_g)
    assert m.startswith("flowchart LR")
    assert "-->" in m
    assert '["spread"]' in m


def test_show_graph_returns_dot_without_filename():
    src = rcsp.show_graph(_g)
    assert "digraph rcsp" in src


def test_show_graph_writes_dot_file(tmp_path):
    out = rcsp.show_graph(_g, filename=str(tmp_path / "g.dot"))
    assert out.endswith(".dot")
    assert "digraph rcsp" in open(out).read()


def test_topology_edges_are_consistent():
    # Every producer id referenced must be a real node index.
    from rcsp._viz import _build

    nodes, producers = _build(_g, (), {}).engine.topology()
    node_ids = {n[0] for n in nodes}
    for edge_id, producer in producers.items():
        assert producer in node_ids
