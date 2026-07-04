"""Smoke + correctness tests for the ported 07_end_to_end examples (offline)."""

import importlib.util
import os

import pytest

_DIR = os.path.join(os.path.dirname(__file__), "..", "examples", "csp_ports", "07_end_to_end")


def _load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_DIR, f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_wikimedia_offline_streams():
    out = _load("wikimedia").main(duration=0.6)
    assert out["total"] and out["total"][-1][1] > 0
    assert 0.0 <= out["bot_ratio"][-1][1] <= 1.0


def test_earthquake_offline():
    r = _load("earthquake").main(duration=1.0)
    assert r["total"] == 13
    assert r["significant"] == 4          # M 4.6, 5.2, 4.9, 6.1
    assert r["strongest"][0] == 6.1


def test_mta_offline_per_route():
    r = _load("mta").main(duration=0.6)
    assert r["trains"] > 0
    assert set(r["by_route"]).issubset({"1", "2", "3", "A", "C"})


def test_seismic_streaming_equals_batch():
    pytest.importorskip("numpy")
    r = _load("seismic").main()
    assert r["match"] is True                      # streaming ratio == batch ratio
    assert r["stream_trig"] == r["batch_trig"]     # identical triggers
    # the injected event (samples 200-260) is detected
    assert any(190 <= t <= 260 for t in r["stream_trig"])
