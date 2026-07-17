#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: assert_expected_red.sh LOG EXACT_SENTINEL ALLOWED_RUST_ERROR_REGEX REQUIRED_SYMBOL_REGEX' >&2
}

if test "$#" -ne 4; then
  usage
  exit 64
fi

LOG=$1
EXACT_SENTINEL=$2
ALLOWED_RUST_ERROR_REGEX=$3
REQUIRED_SYMBOL_REGEX=$4

if test ! -f "$LOG" || test ! -s "$LOG"; then
  printf 'expected-RED log must be a nonempty regular file: %s\n' "$LOG" >&2
  exit 65
fi

if test -z "$EXACT_SENTINEL" || test -z "$ALLOWED_RUST_ERROR_REGEX" || \
  test -z "$REQUIRED_SYMBOL_REGEX"; then
  printf '%s\n' 'expected-RED classifier arguments must be nonempty' >&2
  exit 64
fi

if ! printf '%s\n' "$EXACT_SENTINEL" | LC_ALL=C grep -Eq '^MSQ_A4_RED_[A-Z0-9_]+$'; then
  printf '%s\n' 'expected-RED sentinel must be one exact MSQ_A4_RED_* diagnostic token' >&2
  exit 64
fi

if printf '%s\n' "$ALLOWED_RUST_ERROR_REGEX" | LC_ALL=C grep -Eq \
  '(^|[^\\])\.\*|(^|[^[:alnum:]_])\.[+?]|\[[^]]*-[^]]*\]|\(\.\*\)' || \
  test "$ALLOWED_RUST_ERROR_REGEX" = error || \
  test "$ALLOWED_RUST_ERROR_REGEX" = '^error$'; then
  printf '%s\n' 'allowed Rust error expression is too broad' >&2
  exit 64
fi

case "$ALLOWED_RUST_ERROR_REGEX" in
  'error\[E('*')\]') ;;
  *)
    printf '%s\n' 'allowed Rust error expression must enumerate exact four-digit error codes' >&2
    exit 64
    ;;
esac
ERROR_CODES=${ALLOWED_RUST_ERROR_REGEX#'error\[E('}
ERROR_CODES=${ERROR_CODES%')\]'}
if ! printf '%s\n' "$ERROR_CODES" | LC_ALL=C grep -Eq \
  '^[0-9]{4}(\|[0-9]{4})*$'; then
  printf '%s\n' 'allowed Rust error expression must enumerate exact four-digit error codes' >&2
  exit 64
fi

if printf '%s\n' "$REQUIRED_SYMBOL_REGEX" | LC_ALL=C grep -Eq \
  '(^|[^\\])\.\*|(^|[^[:alnum:]_])\.[+?]|\[[^]]*-[^]]*|(^|\|)(test|tests|src|lib|main|capture|platform|domain|sources)(\||$)'; then
  printf '%s\n' 'required symbol expression is too broad or names only a target/module/file/domain' >&2
  exit 64
fi

if ! printf '%s\n' "$REQUIRED_SYMBOL_REGEX" | LC_ALL=C grep -Eq \
  '^[A-Za-z_][A-Za-z0-9_:]*(\|[A-Za-z_][A-Za-z0-9_:]*)*$'; then
  printf '%s\n' 'required symbol expression must enumerate exact Rust symbols' >&2
  exit 64
fi

reject_unrelated() {
  local class=$1
  local pattern=$2
  if LC_ALL=C grep -Eqi -- "$pattern" "$LOG"; then
    printf 'expected-RED log contains unrelated %s failure\n' "$class" >&2
    exit 66
  fi
}

# Reject environmental and malformed-test failures before considering intended evidence.
reject_unrelated dependency \
  'failed to (select a version|get|download)|download of .* failed|dependency resolution|package .* cannot be resolved'
reject_unrelated network \
  'could not resolve host|network is unreachable|connection (timed out|refused)|failed to lookup address|spurious network error'
reject_unrelated lockfile \
  'Cargo\.lock needs to be updated|lock file .* needs to be updated|lockfile .* (invalid|missing)|--locked was passed'
reject_unrelated manifest \
  'failed to parse manifest|manifest is missing|invalid type: .*Cargo\.toml|no targets specified in the manifest'
reject_unrelated toolchain \
  'toolchain .* is not installed|no release found for|rustup could not choose|rustc .* not found|cargo: command not found'
reject_unrelated target-or-component \
  'target .* (not found|may not be installed)|component .* is unavailable|can.t find crate for [`'"'"']std[`'"'"']|is not a recognized target'
reject_unrelated linker \
  'linking with .* failed|linker .* not found|undefined reference to|ld: .*error'
reject_unrelated permission \
  'permission denied|operation not permitted|access is denied'
reject_unrelated storage \
  'no space left on device|disk full|storage exhausted|quota exceeded'
reject_unrelated malformed-trybuild \
  'there are no trybuild tests enabled|successfully created new stderr files|wip/.*\.stderr|trybuild.*(malformed|panicked)'
reject_unrelated syntax \
  'mismatched closing delimiter|unclosed delimiter|unexpected closing delimiter|expected one of .* found|this file contains an unclosed delimiter'
reject_unrelated compiler-termination \
  'internal compiler error|LLVM ERROR|rustc.*(aborted|terminated|killed|timed out)|process didn.t exit successfully.*(signal|SIG[A-Z]+)|signal: [0-9]+|fatal runtime error'

# This script classifies a captured failure; it cannot prove the command's exit status. Every
# caller must first prove that the producing command returned nonzero. The deterministic caller
# fixtures exercise that invariant before runtime-sentinel classification.
if LC_ALL=C grep -Fxq -- "$EXACT_SENTINEL" "$LOG"; then
  if LC_ALL=C grep -Eq -- \
    '^error\[E[0-9]{4}\]:|(^|[[:space:]])panicked at|assertion .* failed|assertion failed|^thread .* panicked' \
    "$LOG"; then
    printf '%s\n' \
      'expected-RED runtime sentinel is mixed with an unrelated compiler/panic/assertion failure' >&2
    exit 68
  fi
  SENTINEL_COUNT=$(LC_ALL=C grep -Fxc -- "$EXACT_SENTINEL" "$LOG")
  if test "$SENTINEL_COUNT" -ne 1; then
    printf '%s\n' 'expected-RED runtime sentinel must occur exactly once' >&2
    exit 68
  fi
  printf '%s\n' 'expected-RED classified by exact runtime sentinel'
  exit 0
fi

if ! LC_ALL=C grep -Eq -- "$ALLOWED_RUST_ERROR_REGEX" "$LOG"; then
  printf '%s\n' 'expected-RED log lacks an allowed Rust error code' >&2
  exit 67
fi

RUST_ERROR_LINES=$(LC_ALL=C grep -E '^error\[E[0-9]{4}\]:' "$LOG" || true)
if test -z "$RUST_ERROR_LINES"; then
  printf '%s\n' 'expected-RED log lacks a Rust compiler diagnostic header' >&2
  exit 67
fi
if printf '%s\n' "$RUST_ERROR_LINES" | LC_ALL=C grep -Ev -- "$ALLOWED_RUST_ERROR_REGEX" \
  >/dev/null; then
  printf '%s\n' 'expected-RED log contains a Rust error code outside the allowed set' >&2
  exit 68
fi
while IFS= read -r rust_error_line; do
  if ! printf '%s\n' "$rust_error_line" | LC_ALL=C grep -Eq -- \
    "$ALLOWED_RUST_ERROR_REGEX.*(^|[^A-Za-z0-9_])(${REQUIRED_SYMBOL_REGEX})([^A-Za-z0-9_]|$)"; then
    printf '%s\n' \
      'every expected-RED compiler diagnostic header must name an allowed required symbol' >&2
    exit 68
  fi
done <<EOF
$RUST_ERROR_LINES
EOF

printf '%s\n' 'expected-RED classified by allowed Rust error and required symbol'
