"""Seismic waveform: streaming detection == batch detection.

Port of CSP's ``examples/07_end_to_end/seismic_waveform.ipynb``, whose point is
that the *same* analysis runs on a live stream or on a recorded array. Here we run
a classic STA/LTA event trigger (short-term vs long-term average of |amplitude|)
both ways and show they agree exactly.

  * default (offline): a synthetic waveform (background + noise + an injected
    event burst) streamed sample-by-sample through rcsp;
  * ``--live``: real waveforms come from ObsPy — see ``_load_obspy``.

The streaming ratio series (rcsp rolling ``stats.mean``) is compared element-wise
to the batch ratio series (NumPy), and triggers are derived identically.
"""

import argparse
from datetime import datetime, timedelta, timezone

import numpy as np

import rcsp
from rcsp import stats, ts

SHORT, LONG, THRESHOLD, REFRACTORY = 10, 50, 3.0, 40


def _make_waveform(n=400, seed=0):
    rng = np.random.default_rng(seed)
    t = np.arange(n)
    wave = 0.3 * np.sin(2 * np.pi * t / 25.0) + rng.normal(0, 0.15, n)
    wave[200:260] += rng.normal(0, 1.2, 60)   # the "event"
    return wave


def _load_obspy():
    """Real waveforms via ObsPy (documented stub).

    ``pip install obspy``; fetch a trace, e.g.::

        from obspy.clients.fdsn import Client
        st = Client("IRIS").get_waveforms("IU", "ANMO", "00", "BHZ", t0, t1)
        return st[0].data
    """
    raise NotImplementedError("real seismic data needs obspy; see _load_obspy docstring")


def _triggers(ratio, threshold=THRESHOLD, refractory=REFRACTORY):
    """Rising-edge crossings of `ratio` above `threshold`, with a refractory gap.
    `ratio[k]` corresponds to sample index LONG-1+k."""
    out, last = [], -refractory
    prev = 0.0
    for k, r in enumerate(ratio):
        idx = LONG - 1 + k
        if prev <= threshold < r and idx - last >= refractory:
            out.append(idx)
            last = idx
        prev = r
    return out


def _batch(wave):
    a = np.abs(wave)
    csum = np.cumsum(np.insert(a, 0, 0))
    roll = lambda w: (csum[w:] - csum[:-w]) / w   # mean of window ending at each index
    sta = roll(SHORT)
    lta = roll(LONG)
    # align both to sample index j >= LONG-1
    ratio = [sta[j - SHORT + 1] / lta[j - LONG + 1] for j in range(LONG - 1, len(wave))]
    return ratio


def _streaming(wave):
    @rcsp.graph
    def g():
        sig = rcsp.curve(float, [(timedelta(milliseconds=i + 1), float(v)) for i, v in enumerate(wave)])
        amp = rcsp.apply(abs, sig)
        sta = stats.mean(amp, SHORT, min_data_points=SHORT)
        lta = stats.mean(amp, LONG, min_data_points=LONG)
        rcsp.add_graph_output("ratio", sta / lta)

    st = datetime(2020, 1, 1, tzinfo=timezone.utc)
    out = rcsp.run(g, starttime=st, endtime=timedelta(seconds=len(wave) + 1))  # simulation
    return [v for _, v in out["ratio"]]


def main(live=False):
    wave = _load_obspy() if live else _make_waveform()

    stream_ratio = _streaming(wave)
    batch_ratio = _batch(wave)

    match = len(stream_ratio) == len(batch_ratio) and np.allclose(stream_ratio, batch_ratio, atol=1e-9)
    stream_trig = _triggers(stream_ratio)
    batch_trig = _triggers(batch_ratio)

    print(f"samples: {len(wave)}   ratio points: streaming={len(stream_ratio)} batch={len(batch_ratio)}")
    print(f"streaming ratio == batch ratio: {match}")
    print(f"streaming triggers (sample idx): {stream_trig}")
    print(f"batch     triggers (sample idx): {batch_trig}")
    print(f"triggers agree: {stream_trig == batch_trig}  "
          f"(event injected at samples 200-260)")
    return {"match": match, "stream_trig": stream_trig, "batch_trig": batch_trig}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="use ObsPy waveforms (needs obspy)")
    a = ap.parse_args()
    main(live=a.live)
