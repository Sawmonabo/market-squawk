#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
"$script_directory/run_exact_loom_gate.sh" \
  --package market-squawk-platform \
  --model capture::queue::loom_model::concurrent_last_sender_drop_closes_receiver \
  --model capture::accounting::loom_model::live_transition_abandonment_and_checked_drop_fallback \
  --model capture::accounting::loom_model::coherent_snapshot_rejects_aba \
  --model capture::queue::loom_model::shutdown_before_wait \
  --model capture::queue::loom_model::send_close_and_drain_races \
  --model capture::queue::loom_model::receiver_drop_linearizes_with_registered_send \
  --model capture::queue::loom_model::shared_operation_lifecycle_registration_and_close_are_one_modification_order \
  --model capture::writer::runtime::loom_model::fixed_storage_transfer_and_final_drop
