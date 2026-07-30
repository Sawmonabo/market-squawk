#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: run_exact_loom_gate.sh --package PACKAGE --model FULL_PATH [--model FULL_PATH ...]' \
    >&2
}

reject_ambient_overrides() {
  local name
  while IFS= read -r name; do
    case "$name" in
      LOOM_* | RUSTFLAGS | RUSTDOCFLAGS | CARGO_ENCODED_RUSTFLAGS | CARGO_BUILD_* | \
        CARGO_TARGET_* | CARGO_PROFILE_* | RUSTC | RUSTC_WRAPPER | RUSTC_WORKSPACE_WRAPPER)
        printf 'exact Loom gate forbids ambient environment variable %s\n' "$name" >&2
        return 2
        ;;
    esac
  done < <(compgen -e)
}

package=''
models=()
while (($# > 0)); do
  case "$1" in
    --package)
      if (($# < 2)) || [[ -z "$2" ]]; then
        usage
        exit 2
      fi
      package=$2
      shift 2
      ;;
    --model)
      if (($# < 2)) || [[ -z "$2" ]]; then
        usage
        exit 2
      fi
      models+=("$2")
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$package" ]]; then
  printf '%s\n' 'exact Loom gate requires one package' >&2
  exit 2
fi
if ((${#models[@]} == 0)); then
  printf '%s\n' 'exact Loom gate requires at least one exact Loom model' >&2
  exit 2
fi

reject_ambient_overrides

for model in "${models[@]}"; do
  if [[ ! "$model" =~ ^[A-Za-z0-9_]+(::[A-Za-z0-9_]+)+$ ]] || \
    [[ "$model" != *'::loom_model::'* ]]; then
    printf 'invalid reserved Loom model path: %s\n' "$model" >&2
    exit 2
  fi
done

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repository_root="$(cd "$script_directory/.." && pwd -P)"
cd "$repository_root"

cargo_executable="$(command -v cargo)"
rustc_executable="$(command -v rustc)"
rustc_verbose="$($rustc_executable -vV)"
host_lines="$(printf '%s\n' "$rustc_verbose" | sed -n 's/^host: //p')"
host_count="$(printf '%s\n' "$host_lines" | sed '/^$/d' | wc -l | tr -d '[:space:]')"
if [[ "$host_count" != '1' ]] || [[ ! "$host_lines" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  printf '%s\n' 'exact Loom gate could not derive exactly one valid rustc host target' >&2
  exit 2
fi
host_target=$host_lines

temporary_directory="$(mktemp -d "${TMPDIR:-/tmp}/market-squawk-loom.XXXXXX")"
trap 'rm -rf "$temporary_directory"' EXIT
expected="$temporary_directory/expected"
expected_sorted="$temporary_directory/expected.sorted"
listing="$temporary_directory/listing"
listed="$temporary_directory/listed"
listed_sorted="$temporary_directory/listed.sorted"

printf '%s\n' "${models[@]}" >"$expected"
LC_ALL=C sort "$expected" >"$expected_sorted"
expected_count="$(wc -l <"$expected" | tr -d '[:space:]')"
expected_unique_count="$(LC_ALL=C uniq "$expected_sorted" | wc -l | tr -d '[:space:]')"
if [[ "$expected_count" != "$expected_unique_count" ]]; then
  printf '%s\n' 'exact Loom gate received a duplicate declared Loom model' >&2
  exit 2
fi

export RUSTFLAGS='--cfg market_squawk_loom'
export CARGO_INCREMENTAL=0

common_arguments=(
  -p "$package"
  --lib
  --all-features
  --release
  --locked
  --target "$host_target"
)

"$cargo_executable" test "${common_arguments[@]}" -- --list >"$listing"
sed -n 's/^\(.*::loom_model::[A-Za-z0-9_]*\): test$/\1/p' "$listing" >"$listed"
if [[ ! -s "$listed" ]]; then
  printf '%s\n' 'exact Loom gate listed zero reserved Loom models' >&2
  exit 2
fi
LC_ALL=C sort "$listed" >"$listed_sorted"
listed_count="$(wc -l <"$listed" | tr -d '[:space:]')"
listed_unique_count="$(LC_ALL=C uniq "$listed_sorted" | wc -l | tr -d '[:space:]')"
if [[ "$listed_count" != "$listed_unique_count" ]]; then
  printf '%s\n' 'exact Loom gate discovered a duplicate listed Loom model' >&2
  exit 2
fi
if ! cmp -s "$expected_sorted" "$listed_sorted"; then
  printf '%s\n' 'exact Loom model inventory mismatch' >&2
  diff -u "$expected_sorted" "$listed_sorted" >&2 || true
  exit 2
fi

"$cargo_executable" clippy "${common_arguments[@]}" -- -D warnings
"$cargo_executable" test "${common_arguments[@]}" loom_model -- --test-threads=1
