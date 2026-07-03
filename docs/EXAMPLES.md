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
| `e3_show_graph.py` — visualize the graph | ✅ | [`examples/csp_ports/01_basics/e3_show_graph.py`](../examples/csp_ports/01_basics/e3_show_graph.py) |
| `e4_trade_pnl.py` — VWAP + PnL | ✅ | [`examples/csp_ports/01_basics/e4_trade_pnl.py`](../examples/csp_ports/01_basics/e4_trade_pnl.py) |
| `e5_retail_cart.py` — cart with time-based discounts | 🟡 | covered by trade_pnl's struct + stateful pattern; not separately ported |

## 02_intermediate

| CSP example | rcsp | Where |
|---|---|---|
| `e1_feedback.py` — feedback edges | ✅ | [`examples/csp_ports/02_intermediate/e1_feedback.py`](../examples/csp_ports/02_intermediate/e1_feedback.py) |
| `e2_stats.py` — `csp.stats` library | ✅ | [`examples/csp_ports/02_intermediate/e2_stats.py`](../examples/csp_ports/02_intermediate/e2_stats.py) |
| `e3_numpy_stats.py` — NumPy rolling stats | ✅ | [`examples/csp_ports/02_intermediate/e3_numpy_stats.py`](../examples/csp_ports/02_intermediate/e3_numpy_stats.py) |
| `e4_exprtk.py` — ExprTk expression eval | ⛔ | needs the ExprTk adapter (not implemented) |

## 03_using_adapters

| CSP example | rcsp | Where |
|---|---|---|
| `kafka/e1_kafka.py` | ✅ | [`examples/csp_ports/03_using_adapters/kafka/e1_kafka.py`](../examples/csp_ports/03_using_adapters/kafka/e1_kafka.py) |
| `parquet/e1_parquet_write_read.py` | ✅ | [`examples/csp_ports/03_using_adapters/parquet/e1_parquet_write_read.py`](../examples/csp_ports/03_using_adapters/parquet/e1_parquet_write_read.py) |
| `websocket/e1_websocket_client.py` | ⛔ | needs the websocket adapter |
| `websocket/e2_websocket_output.py` | ⛔ | needs the websocket adapter |

## 04_writing_adapters

| CSP example | rcsp | Where |
|---|---|---|
| `e1_generic_push_adapter.py` — realtime push | ✅ | [`examples/csp_ports/04_writing_adapters/e1_generic_push_adapter.py`](../examples/csp_ports/04_writing_adapters/e1_generic_push_adapter.py) |
| `e2_pullinput.py` — replay historical data | ✅ | [`examples/csp_ports/04_writing_adapters/e2_pullinput.py`](../examples/csp_ports/04_writing_adapters/e2_pullinput.py) (via `curve`) |
| `e3_adaptermanager_pullinput.py` | ✅ | [`examples/csp_ports/04_writing_adapters/e3_adaptermanager_pullinput.py`](../examples/csp_ports/04_writing_adapters/e3_adaptermanager_pullinput.py) |
| `e4_pushinput.py` — custom push adapter | 🟡 | covered by `GenericPushAdapter` |
| `e5_adaptermanager_pushinput.py` | 🟡 | `AdapterManager` covers pull fan-out; realtime push-manager not modelled |
| `e6_outputadapter.py` | ✅ | `write_parquet` / `write_csv` output adapters |
| `e7_adaptermanager_inputoutput.py` | 🟡 | input manager + output adapters exist; combined manager not modelled |

## 05_cpp

| CSP example | rcsp | Notes |
|---|---|---|
| `1_cpp_node/` | ⛔ | N/A — rcsp's engine is Rust, not C++. The native-kernel equivalent lives in `src/lib.rs`. |
| `2_cpp_node_with_struct/` | ⛔ | same as above |

## 06_advanced

| CSP example | rcsp | Notes |
|---|---|---|
| `e1_dynamic.py` — dynamic graphs | ✅ | [`examples/csp_ports/06_advanced/e1_dynamic.py`](../examples/csp_ports/06_advanced/e1_dynamic.py) |
| `e2_pandas_extension.py` | 🟡 | the pandas *ts-type extension* isn't ported, but DataFrames pass as scalar params and `ts[object]` edge values — see [`examples/e5_dataframes.py`](../examples/e5_dataframes.py) |

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
| `e1_profiling.py` | ✅ | [`examples/csp_ports/99_developer_tools/e1_profiling.py`](../examples/csp_ports/99_developer_tools/e1_profiling.py) |

## Summary

The remaining ⛔ entries are honest gaps: they require a websocket server,
pandas interop, or a C++ toolchain. See [`DESIGN.md`](DESIGN.md) for the scope
rationale.

Now available in rcsp:

- Graph visualization: `rcsp.show_graph` / `rcsp.graph_to_dot` /
  `rcsp.graph_to_mermaid` (image rendering needs the Graphviz `dot` binary).
- Rolling-window statistics: `rcsp.stats.{mean,sum,count,min,max,var,stddev,
  median,first,last,prod}` over tick-count or time windows.
- Profiling: `rcsp.profiler.Profiler()` for per-node counts and cumulative time.
- NumPy/Polars interop: arrays flow through edges natively; `rcsp.to_polars` /
  `to_polars_wide` collect run results.
- File I/O adapters: `rcsp.read_parquet` / `read_csv` (pull) and
  `write_parquet` / `write_csv` (output).
- Adapter managers: `rcsp.ReplayAdapterManager` / `CsvAdapterManager` fan one
  source out to per-key streams.
- Dynamic graphs: `rcsp.dynamic(control, factory)` instantiates a sub-graph per
  new key at runtime (simulation), via the engine's stepped-execution API.
- Kafka: `rcsp.KafkaAdapterManager` `subscribe`/`publish` (via `kafka-python`),
  with an in-process `rcsp.InMemoryKafka` broker double for tests/demos.
- DataFrames / arbitrary objects: pass Polars/pandas frames as scalar node params
  or as `ts[object]` edge values; `rcsp.apply(fn, *edges)` runs a function over
  edge values. See [`examples/e5_dataframes.py`](../examples/e5_dataframes.py).
