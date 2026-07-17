#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
"$script_directory/run_exact_loom_gate.sh" \
  --package market-squawk-platform \
  --model capture::queue::loom_model::clone_drop_overflow_poison_and_last_close \
  --model capture::accounting::loom_model::live_transition_abandonment_and_checked_drop_fallback \
  --model capture::accounting::loom_model::coherent_snapshot_rejects_aba \
  --model capture::queue::loom_model::shutdown_before_wait \
  --model capture::queue::loom_model::send_close_and_drain_races \
  --model capture::writer::runtime::loom_model::fixed_storage_transfer_and_final_drop
