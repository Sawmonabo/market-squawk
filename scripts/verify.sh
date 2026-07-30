#!/usr/bin/env bash
set -euo pipefail

readonly TARGET_CEILING_KIB=$((20 * 1024 * 1024))
readonly DESKTOP_APP="apps/market-squawk-desktop"

lane=all
case "${1:-}" in
  "")
    ;;
  --policy-only)
    lane=policy
    ;;
  --lane)
    if [[ $# -ne 2 ]]; then
      printf 'usage: %s [--policy-only | --lane LANE]\n' "$0" >&2
      exit 2
    fi
    lane=$2
    ;;
  *)
    printf 'usage: %s [--policy-only | --lane LANE]\n' "$0" >&2
    exit 2
    ;;
esac

case "$lane" in
  all | policy | scripts | frontend | clippy | tests | ui | loom | release)
    ;;
  *)
    printf 'unknown verification lane: %s\n' "$lane" >&2
    exit 2
    ;;
esac

reject_cargo_directory_overrides() {
  local variable
  for variable in CARGO_TARGET_DIR CARGO_BUILD_BUILD_DIR; do
    if [[ -n "${!variable:-}" ]]; then
      printf 'verification forbids nonempty environment variable %s\n' "$variable" >&2
      return 2
    fi
  done
}

enforce_target_ceiling() {
  local phase="$1"
  local usage_kib=0

  if [[ -e target ]]; then
    read -r usage_kib _ < <(du -sk target)
  fi

  if ((usage_kib > TARGET_CEILING_KIB)); then
    printf 'verification target/ exceeds the 20 GiB %s ceiling: %s KiB > %s KiB\n' \
      "$phase" "$usage_kib" "$TARGET_CEILING_KIB" >&2
    return 2
  fi
}

finalize_verification() {
  local status=$?

  if [[ -n "${tmp_dir:-}" ]] && ! rm -rf "$tmp_dir"; then
    status=1
  fi
  if ! enforce_target_ceiling post-verification; then
    status=2
  fi

  exit "$status"
}

reject_ambient_capture_benchmark_variables() {
  local variable
  while IFS= read -r variable; do
    case "$variable" in
      CAPTURE_BENCH_*)
        printf 'verification forbids ambient environment variable %s\n' "$variable" >&2
        return 2
        ;;
    esac
  done < <(compgen -e)
}

run_policy() {
  python3 scripts/check_workspace_boundaries.py
  python3 scripts/check_generated_artifacts.py
  cargo deny check
  # The exact exceptions, upstream constraints, and refresh gates are documented in deny.toml.
  cargo audit --deny warnings \
    --ignore RUSTSEC-2024-0370 \
    --ignore RUSTSEC-2024-0411 \
    --ignore RUSTSEC-2024-0412 \
    --ignore RUSTSEC-2024-0413 \
    --ignore RUSTSEC-2024-0415 \
    --ignore RUSTSEC-2024-0416 \
    --ignore RUSTSEC-2024-0418 \
    --ignore RUSTSEC-2024-0419 \
    --ignore RUSTSEC-2024-0420 \
    --ignore RUSTSEC-2024-0436 \
    --ignore RUSTSEC-2025-0075 \
    --ignore RUSTSEC-2025-0080 \
    --ignore RUSTSEC-2025-0081 \
    --ignore RUSTSEC-2025-0098 \
    --ignore RUSTSEC-2025-0100
  gitleaks dir --redact --no-banner .
  gitleaks git --redact --no-banner
}

run_scripts() {
  python3 -m unittest discover -s scripts/tests -p 'test_*.py'
}

run_frontend() {
  if ! command -v pnpm >/dev/null 2>&1; then
    printf 'frontend verification requires the pnpm version pinned by %s/package.json\n' \
      "$DESKTOP_APP" >&2
    return 2
  fi

  pnpm --dir "$DESKTOP_APP" install --frozen-lockfile
  pnpm --dir "$DESKTOP_APP" typecheck
  pnpm --dir "$DESKTOP_APP" test --run
  pnpm --dir "$DESKTOP_APP" build
}

run_clippy() {
  cargo fmt --all -- --check
  RUSTFLAGS="" CARGO_ENCODED_RUSTFLAGS="-Dwarnings" \
    CAPTURE_BENCH_DEVELOPMENT_BACKEND=candidate \
    cargo clippy -p market-squawk-platform --all-targets --all-features --locked -- -D warnings
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
    cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
}

run_tests() {
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
    cargo test --workspace --all-features --locked --no-fail-fast
}

run_ui() {
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
    cargo test --locked \
      -p market-squawk-domain \
      -p market-squawk-live \
      -p market-squawk-execution \
      --test ui
}

run_loom() {
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard ./scripts/check_authority_lifecycle_loom.sh
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard ./scripts/check_capture_queue_loom.sh
}

run_release() {
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
    cargo build --workspace --all-features --release --locked --timings
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard python3 scripts/check_capture_frame_contracts.py

  tmp_dir="$(mktemp -d)"
  ./target/release/market-squawk --help >"$tmp_dir/help.txt"
  ./target/release/market-squawk \
    --data-dir "$tmp_dir" mock --events 100 >"$tmp_dir/snapshot.json"
  python3 - "$tmp_dir/snapshot.json" <<'PY'
import json
import pathlib
import sys

snapshot = json.loads(pathlib.Path(sys.argv[1]).read_text())
if snapshot.get("processed_events") != 101:
    raise SystemExit(f"unexpected processed event count: {snapshot!r}")
products = snapshot.get("products")
if not isinstance(products, dict) or "TEST-USD" not in products:
    raise SystemExit(f"offline mock product is missing: {snapshot!r}")
print("offline mock smoke test passed")
PY
  python3 scripts/smoke_mcp.py ./target/release/market-squawk
}

tmp_dir=""
trap finalize_verification EXIT
reject_cargo_directory_overrides
reject_ambient_capture_benchmark_variables
export CARGO_INCREMENTAL=0
enforce_target_ceiling pre-verification

if [[ "$lane" == all ]]; then
  run_policy
  run_scripts
  run_frontend
  run_clippy
  run_tests
  run_ui
  run_loom
  run_release
else
  "run_$lane"
fi
