# CSP examples → rcsp status

This maps every example in [Point72/csp `examples/`](https://github.com/Point72/csp/tree/main/examples)
to its status in rcsp. rcsp is a faithful **subset** of CSP, so examples that
depend on subsystems rcsp doesn't implement are marked accordingly.

Legend: ✅ ported · 🟡 adapted (same concept, rcsp idiom) · ⛔ not applicable /
needs an unimplemented subsystem.

## 01_basics

| CSP example | rcsp | Where |
|---|---|---|
| `e1_basic.py` — sum two constants | ✅ | [`examples/e1_basic.py`](../examples/e1_basic.py) |
| `e2_ticking.py` — accumulate a ticking series | ✅ | [`examples/csp_ports/01_basics/e2_ticking.py`](../examples/csp_ports/01_basics/e2_ticking.py) |
| `e3_show_graph.py` — visualize the graph | ⛔ | needs graphviz graph rendering (not implemented) |
| `e4_trade_pnl.py` — VWAP + PnL | ✅ | [`examples/csp_ports/01_basics/e4_trade_pnl.py`](../examples/csp_ports/01_basics/e4_trade_pnl.py) |
| `e5_retail_cart.py` — cart with time-based discounts | 🟡 | covered by trade_pnl's struct + stateful pattern; not separately ported |

## 02_intermediate

| CSP example | rcsp | Where |
|---|---|---|
| `e1_feedback.py` — feedback edges | ✅ | [`examples/csp_ports/02_intermediate/e1_feedback.py`](../examples/csp_ports/02_intermediate/e1_feedback.py) |
| `e2_stats.py` — `csp.stats` library | ⛔ | needs the rolling-window stats library (not implemented) |
| `e3_numpy_stats.py` — NumPy rolling stats | ⛔ | needs NumPy-array time series + stats (not implemented) |
| `e4_exprtk.py` — ExprTk expression eval | ⛔ | needs the ExprTk adapter (not implemented) |

## 03_using_adapters

| CSP example | rcsp | Where |
|---|---|---|
| `kafka/e1_kafka.py` | ⛔ | needs the Kafka adapter |
| `parquet/e1_parquet_write_read.py` | ⛔ | needs the Parquet adapter |
| `websocket/e1_websocket_client.py` | ⛔ | needs the websocket adapter |
| `websocket/e2_websocket_output.py` | ⛔ | needs the websocket adapter |

## 04_writing_adapters

| CSP example | rcsp | Where |
|---|---|---|
| `e1_generic_push_adapter.py` — realtime push | ✅ | [`examples/csp_ports/04_writing_adapters/e1_generic_push_adapter.py`](../examples/csp_ports/04_writing_adapters/e1_generic_push_adapter.py) |
| `e2_pullinput.py` — replay historical data | ✅ | [`examples/csp_ports/04_writing_adapters/e2_pullinput.py`](../examples/csp_ports/04_writing_adapters/e2_pullinput.py) (via `curve`) |
| `e3_adaptermanager_pullinput.py` | 🟡 | single-source pull covered; adapter-manager fan-out not modelled |
| `e4_pushinput.py` — custom push adapter | 🟡 | covered by `GenericPushAdapter` |
| `e5_adaptermanager_pushinput.py` | ⛔ | needs an adapter-manager framework |
| `e6_outputadapter.py` | 🟡 | output covered by `add_graph_output` / `print` |
| `e7_adaptermanager_inputoutput.py` | ⛔ | needs an adapter-manager framework |

## 05_cpp

| CSP example | rcsp | Notes |
|---|---|---|
| `1_cpp_node/` | ⛔ | N/A — rcsp's engine is Rust, not C++. The native-kernel equivalent lives in `src/lib.rs`. |
| `2_cpp_node_with_struct/` | ⛔ | same as above |

## 06_advanced

| CSP example | rcsp | Notes |
|---|---|---|
| `e1_dynamic.py` — dynamic graphs | ⛔ | needs runtime graph mutation (not implemented) |
| `e2_pandas_extension.py` | ⛔ | needs the pandas extension |

## 07_end_to_end

| CSP example | rcsp | Notes |
|---|---|---|
| `mta.ipynb`, `seismic_waveform.ipynb`, `wikimedia.ipynb`, `earthquake.ipynb` | ⛔ | live-data notebooks needing network adapters + plotting |

## 98_just_for_fun

| CSP example | rcsp | Where |
|---|---|---|
| `e1_csp_nand_computer.py` — 16-bit adder from NAND gates | ✅ | [`examples/csp_ports/98_just_for_fun/e1_csp_nand_computer.py`](../examples/csp_ports/98_just_for_fun/e1_csp_nand_computer.py) |

## 99_developer_tools

| CSP example | rcsp | Notes |
|---|---|---|
| `e1_profiling.py` | ⛔ | needs a graph profiler (not implemented) |

## Summary

Everything expressible with rcsp's current feature set is ported. The ⛔
entries are honest gaps: they require subsystems (stats, NumPy/pandas interop,
Kafka/Parquet/websocket adapters, adapter managers, dynamic graphs, graph
visualization, profiling) or a C++ toolchain that rcsp does not provide. See
[`DESIGN.md`](DESIGN.md) for the scope rationale.
