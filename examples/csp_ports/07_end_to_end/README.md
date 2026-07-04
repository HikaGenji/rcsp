# End-to-end streaming examples

Ports of CSP's [`examples/07_end_to_end`](https://github.com/Point72/csp/tree/main/examples/07_end_to_end).
CSP's originals are Jupyter notebooks with live feeds and maps/plots; these are
**runnable scripts** that perform the same streaming computation and print a
rolling summary. Each runs **offline** on synthetic/replayed data by default and
takes a `--live` flag to use the real feed where the network is open (this sandbox
blocks the feeds, so live isn't exercised here). Outputs are returned as graph
outputs — collect them with `rcsp.to_polars` if you want to plot.

| Script | Mirrors | Demonstrates |
|---|---|---|
| `wikimedia.py` | `wikimedia.ipynb` | live edit stream → edit rate, bot ratio, top wiki (`GenericPushAdapter` + rolling `stats`) |
| `earthquake.py` | `earthquake.ipynb` | quake replay → running count, rolling max magnitude, significance filter (`curve` + `stats`) |
| `mta.py` | `mta.ipynb` | subway arrivals → per-route mean delay, on-time ratio (stateful nodes + `stats.mean`) |
| `seismic.py` | `seismic_waveform.ipynb` | STA/LTA event trigger, **streaming result == NumPy batch result** |

Live wiring: `wikimedia --live` reads Wikimedia's recent-change SSE stream and
`earthquake --live` fetches the USGS GeoJSON feed. `mta --live` needs
`gtfs-realtime-bindings` + an MTA API key (protobuf decode stub included), and
`seismic --live` needs `obspy` (documented stub) — both feed the *same* graph.

```bash
python examples/csp_ports/07_end_to_end/seismic.py       # streaming == batch
python examples/csp_ports/07_end_to_end/wikimedia.py     # offline synthetic stream
python examples/csp_ports/07_end_to_end/wikimedia.py --live   # real feed (open network)
```
