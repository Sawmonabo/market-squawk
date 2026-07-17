#!/usr/bin/env bash
set -euo pipefail

base_rustflags="${RUSTFLAGS:-}"
if [[ -n "$base_rustflags" ]]; then
  export RUSTFLAGS="$base_rustflags --cfg loom"
else
  export RUSTFLAGS="--cfg loom"
fi

cargo clippy \
  -p market-squawk-sources \
  --all-targets \
  --all-features \
  --release \
  --locked \
  -- \
  -D warnings

cargo test \
  -p market-squawk-sources \
  loom_models_admission_terminalization_and_clean_close_races \
  --lib \
  --all-features \
  --release \
  --locked \
  -- \
  --test-threads=1
