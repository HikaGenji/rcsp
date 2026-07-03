# CSP example ports

These examples mirror examples from [Point72/csp](https://github.com/Point72/csp/tree/main/examples),
organized under the same category folders, rewritten against rcsp's API.

| Path | Mirrors | Demonstrates |
|---|---|---|
| `01_basics/e2_ticking.py` | `01_basics/e2_ticking.py` | ticking series, `curve`, stateful `accum` |
| `01_basics/e3_show_graph.py` | `01_basics/e3_show_graph.py` | `show_graph` / DOT / Mermaid visualization |
| `01_basics/e4_trade_pnl.py` | `01_basics/e4_trade_pnl.py` | `split`, structs (dataclasses), VWAP, PnL |
| `02_intermediate/e1_feedback.py` | `02_intermediate/e1_feedback.py` | `feedback` edges, alarms |
| `02_intermediate/e2_stats.py` | `02_intermediate/e2_stats.py` | `rcsp.stats` rolling mean/stddev, Bollinger bands |
| `04_writing_adapters/e1_generic_push_adapter.py` | `04_writing_adapters/e1_generic_push_adapter.py` | realtime `GenericPushAdapter`, threads |
| `04_writing_adapters/e2_pullinput.py` | `04_writing_adapters/e2_pullinput.py` | pull/replay via `curve` |
| `98_just_for_fun/e1_csp_nand_computer.py` | `98_just_for_fun/e1_csp_nand_computer.py` | one NAND node → a 16-bit adder |

For the full mapping of *every* CSP example (including the ones that need
subsystems rcsp doesn't implement), see [`../../docs/EXAMPLES.md`](../../docs/EXAMPLES.md).

Run them from the repo root, e.g.:

```bash
python examples/csp_ports/98_just_for_fun/e1_csp_nand_computer.py
```
