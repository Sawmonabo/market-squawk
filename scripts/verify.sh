#!/usr/bin/env bash
set -euo pipefail

python3 scripts/check_brand.py
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
cargo run --locked --quiet -- --data-dir "$tmp_dir" mock --events 100 >"$tmp_dir/snapshot.json"
python3 - "$tmp_dir/snapshot.json" <<'PY'
import json
import pathlib
import sys

snapshot = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert snapshot["processed_events"] == 101, snapshot
assert "TEST-USD" in snapshot["products"], snapshot
print("offline mock smoke test passed")
PY
