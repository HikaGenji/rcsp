"""Graph visualization (mirrors ``csp.show_graph``).

Builds the graph without running it, then renders its structure. Output formats:

* :func:`graph_to_dot` — Graphviz DOT (always available)
* :func:`graph_to_mermaid` — Mermaid flowchart (always available)
* :func:`show_graph` — render to an image via Graphviz if the ``dot`` binary is
  installed, otherwise fall back to writing the DOT source.
"""

from ._graph import Builder, _builder


def _build(graph, args, kwargs):
    builder = Builder()
    token = _builder.set(builder)
    try:
        graph(*args, **kwargs)
    finally:
        _builder.reset(token)
    return builder


def _edges(nodes, producers):
    """Yield (producer_node, consumer_node) pairs, de-duplicated."""
    seen = set()
    for nid, _name, _rank, inputs, _outputs in nodes:
        for e in inputs:
            p = producers.get(e)
            if p is not None and (p, nid) not in seen:
                seen.add((p, nid))
                yield p, nid


def graph_to_dot(graph, *args, **kwargs):
    """Return the graph as a Graphviz DOT string."""
    nodes, producers = _build(graph, args, kwargs).engine.topology()
    lines = [
        "digraph rcsp {",
        "  rankdir=LR;",
        '  node [shape=box, style="rounded,filled", fillcolor="#eef2ff", '
        'color="#5b6ee1", fontname="Helvetica"];',
        '  edge [color="#888", arrowsize=0.7];',
    ]
    for nid, name, _rank, _inputs, _outputs in nodes:
        lines.append(f'  n{nid} [label="{name}"];')
    for p, c in _edges(nodes, producers):
        lines.append(f"  n{p} -> n{c};")
    lines.append("}")
    return "\n".join(lines)


def graph_to_mermaid(graph, *args, **kwargs):
    """Return the graph as a Mermaid ``flowchart`` string."""
    nodes, producers = _build(graph, args, kwargs).engine.topology()
    lines = ["flowchart LR"]
    for nid, name, _rank, _inputs, _outputs in nodes:
        lines.append(f'  n{nid}["{name}"]')
    for p, c in _edges(nodes, producers):
        lines.append(f"  n{p} --> n{c}")
    return "\n".join(lines)


def _render(dot_src, out_path, fmt):
    """Try to render DOT to an image; return True on success."""
    try:
        import graphviz  # noqa: F401

        graphviz.Source(dot_src).pipe(format=fmt, outfile=out_path)
        return True
    except Exception:
        pass
    try:
        import subprocess

        subprocess.run(
            ["dot", f"-T{fmt}", "-o", out_path],
            input=dot_src.encode(),
            check=True,
            capture_output=True,
        )
        return True
    except Exception:
        return False


def show_graph(graph, *args, filename=None, **kwargs):
    """Render the graph. With no ``filename``, returns the DOT source string.

    With a ``filename`` (``.png``/``.svg``/``.pdf``/``.dot``), writes it and
    returns the written path. Image formats need the Graphviz ``dot`` binary;
    if it's missing, the DOT source is written to a sibling ``.dot`` file.
    """
    dot_src = graph_to_dot(graph, *args, **kwargs)
    if filename is None:
        return dot_src

    fmt = filename.rsplit(".", 1)[-1].lower()
    if fmt == "dot":
        with open(filename, "w") as f:
            f.write(dot_src)
        return filename

    if _render(dot_src, filename, fmt):
        return filename

    fallback = filename.rsplit(".", 1)[0] + ".dot"
    with open(fallback, "w") as f:
        f.write(dot_src)
    return fallback
