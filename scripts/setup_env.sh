#!/usr/bin/env bash
# Prepare a dev environment for rcsp: a venv with the Rust extension built and
# test tooling installed. Safe to re-run — it is idempotent and fast once built.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v cargo >/dev/null 2>&1; then
  echo "[rcsp setup] cargo not found; install a Rust toolchain to build the engine." >&2
  exit 0
fi

if [ ! -d .venv ]; then
  echo "[rcsp setup] creating virtualenv"
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

python -m pip install --quiet --upgrade pip >/dev/null 2>&1 || true
python -m pip install --quiet maturin pytest >/dev/null 2>&1

echo "[rcsp setup] building the Rust extension (maturin develop)"
maturin develop --release >/dev/null 2>&1 && echo "[rcsp setup] ready: source .venv/bin/activate && pytest" \
  || echo "[rcsp setup] build failed; run 'maturin develop' manually to see errors" >&2
