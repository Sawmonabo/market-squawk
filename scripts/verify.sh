#!/usr/bin/env bash
set -euo pipefail

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

reject_ambient_capture_benchmark_variables

python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check
cargo audit --deny warnings
gitleaks dir --redact --no-banner .
gitleaks git --redact --no-banner
cargo fmt --all -- --check
RUSTFLAGS="" CARGO_ENCODED_RUSTFLAGS="-Dwarnings" \
  CAPTURE_BENCH_DEVELOPMENT_BACKEND=candidate \
  cargo clippy -p market-squawk-platform --all-targets --all-features --locked -- -D warnings
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
  cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
  cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
  cargo test --workspace --all-targets --all-features --locked
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
  cargo test --doc --workspace --all-features --locked
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard ./scripts/check_authority_lifecycle_loom.sh
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard ./scripts/check_capture_queue_loom.sh
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard \
  cargo build --workspace --all-features --release --locked
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard python3 scripts/check_capture_frame_contracts.py
CAPTURE_BENCH_DEVELOPMENT_BACKEND=standard cargo build -p market-squawk --all-features --locked

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
./target/debug/market-squawk --help >"$tmp_dir/help.txt"
./target/debug/market-squawk \
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
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
