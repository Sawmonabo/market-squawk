#!/usr/bin/env bash
set -euo pipefail

python3 scripts/check_brand.py
./scripts/tests/test_assert_expected_red.sh
python3 -m unittest discover -s scripts/tests -p 'test_*.py'
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_duplicate_dependencies.py
python3 scripts/check_generated_artifacts.py
python3 scripts/check_capture_frame_contracts.py
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --doc --workspace --all-features --locked
./scripts/check_authority_lifecycle_loom.sh
./scripts/check_capture_queue_loom.sh
cargo build --workspace --all-features --release --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
cargo build -p market-squawk --all-features --locked

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
./target/debug/market-squawk --help >"$tmp_dir/help.txt"
python3 - "$tmp_dir/help.txt" <<'PY'
import pathlib
import sys

help_text = pathlib.Path(sys.argv[1]).read_text()
expected_identity = "Local-first market tools that are diagnostic and authority-free. Any bot behavior is paper simulation only, with no production order authority."
first_line = help_text.splitlines()[0] if help_text else ""
if first_line != expected_identity:
    raise SystemExit(
        f"CLI help identity mismatch: expected {expected_identity!r}, found {first_line!r}"
    )
PY
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
