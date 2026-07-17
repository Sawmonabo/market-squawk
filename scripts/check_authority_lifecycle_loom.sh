#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
"$script_directory/run_exact_loom_gate.sh" \
  --package market-squawk-sources \
  --model \
  policy::persistence::lifecycle::tests::loom_model::admission_terminalization_and_clean_close_races
