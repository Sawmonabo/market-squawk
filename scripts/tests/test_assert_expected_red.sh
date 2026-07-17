#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLASSIFIER="$ROOT/scripts/assert_expected_red.sh"
FIXTURES="$ROOT/scripts/tests/expected-red"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

assert_status() {
  local expected=$1
  local diagnostic=$2
  shift 2
  set +e
  "$@" >"$TMP_DIR/stdout" 2>"$TMP_DIR/stderr"
  local actual=$?
  set -e
  if test "$actual" -ne "$expected"; then
    printf 'expected status %s, got %s for: %s\n' "$expected" "$actual" "$*" >&2
    sed -n '1,120p' "$TMP_DIR/stdout" >&2
    sed -n '1,120p' "$TMP_DIR/stderr" >&2
    exit 1
  fi
  if test -n "$diagnostic" && ! grep -Fq -- "$diagnostic" "$TMP_DIR/stderr"; then
    printf 'missing diagnostic %s for: %s\n' "$diagnostic" "$*" >&2
    sed -n '1,120p' "$TMP_DIR/stderr" >&2
    exit 1
  fi
}

"$CLASSIFIER" "$FIXTURES/valid-runtime.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload' >/dev/null
"$CLASSIFIER" "$FIXTURES/valid-compiler.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload' >/dev/null

assert_status 67 'lacks an allowed Rust error code' \
  "$CLASSIFIER" "$FIXTURES/invalid-filter-only.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 68 'every expected-RED compiler diagnostic header' \
  "$CLASSIFIER" "$FIXTURES/invalid-unrelated-rust.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 66 'unrelated network failure' \
  "$CLASSIFIER" "$FIXTURES/invalid-mixed.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 67 'lacks an allowed Rust error code' \
  "$CLASSIFIER" "$FIXTURES/invalid-success.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 68 'mixed with an unrelated compiler/panic/assertion failure' \
  "$CLASSIFIER" "$FIXTURES/invalid-sentinel-assertion.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 68 'mixed with an unrelated compiler/panic/assertion failure' \
  "$CLASSIFIER" "$FIXTURES/invalid-sentinel-rust.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 68 'every expected-RED compiler diagnostic header' \
  "$CLASSIFIER" "$FIXTURES/invalid-uncorrelated-compiler.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 68 'every expected-RED compiler diagnostic header' \
  "$CLASSIFIER" "$FIXTURES/invalid-mixed-same-code.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'
assert_status 66 'unrelated compiler-termination failure' \
  "$CLASSIFIER" "$FIXTURES/invalid-compiler-termination.log" \
  MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
  'CapturePayload|capture_payload'

# The classifier intentionally cannot infer command status from a log. Prove the caller-side
# invariant: a runtime sentinel is classifiable only in the branch reached after nonzero exit.
if (exit 0); then
  :
else
  "$CLASSIFIER" "$FIXTURES/valid-runtime.log" \
    MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
    'CapturePayload|capture_payload' >/dev/null
  printf '%s\n' 'successful command incorrectly entered expected-RED classification' >&2
  exit 1
fi
if (exit 23); then
  printf '%s\n' 'failed command unexpectedly returned success' >&2
  exit 1
else
  "$CLASSIFIER" "$FIXTURES/valid-runtime.log" \
    MSQ_A4_RED_DOMAIN_PAYLOAD_CONTRACT 'error\[E(0046|0599)\]' \
    'CapturePayload|capture_payload' >/dev/null
fi

EMPTY="$TMP_DIR/empty.log"
: >"$EMPTY"
assert_status 65 'nonempty regular file' \
  "$CLASSIFIER" "$EMPTY" MSQ_A4_RED_X 'error\[E(0046)\]' CapturePayload
assert_status 65 'nonempty regular file' \
  "$CLASSIFIER" "$TMP_DIR/missing.log" MSQ_A4_RED_X 'error\[E(0046)\]' CapturePayload
assert_status 64 'arguments must be nonempty' \
  "$CLASSIFIER" "$FIXTURES/valid-compiler.log" '' 'error\[E(0046)\]' CapturePayload
assert_status 64 'sentinel must be' \
  "$CLASSIFIER" "$FIXTURES/valid-compiler.log" capture_authority_bridge \
  'error\[E(0046)\]' CapturePayload
assert_status 64 'too broad' \
  "$CLASSIFIER" "$FIXTURES/valid-compiler.log" MSQ_A4_RED_X 'error.*' CapturePayload
assert_status 64 'too broad' \
  "$CLASSIFIER" "$FIXTURES/valid-compiler.log" MSQ_A4_RED_X \
  'error\[E(0046)\]' capture

while IFS=$'\t' read -r class log_line; do
  test -n "$class"
  MATRIX_LOG="$TMP_DIR/matrix-$class.log"
  printf '%s\n%s\n' MSQ_A4_RED_X "$log_line" >"$MATRIX_LOG"
  assert_status 66 "unrelated $class failure" \
    "$CLASSIFIER" "$MATRIX_LOG" MSQ_A4_RED_X 'error\[E(0046)\]' CapturePayload
done <"$FIXTURES/invalid-environment-matrix.tsv"

printf '%s\n' 'expected-RED classifier tests passed'
