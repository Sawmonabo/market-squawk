#!/bin/sh

set -eu
umask 077

tag='__MARKET_SQUAWK_TAG__'
release_base="https://github.com/Sawmonabo/market-squawk/releases/download/$tag"
manifest_url="$release_base/market-squawk-release.json"

case "$(uname -s):$(uname -m)" in
  Darwin:arm64 | Darwin:aarch64)
    target='aarch64-apple-darwin'
    expected_sha256='__MARKET_SQUAWK_BOOTSTRAP_AARCH64_APPLE_DARWIN_SHA256__'
    ;;
  Darwin:x86_64)
    target='x86_64-apple-darwin'
    expected_sha256='__MARKET_SQUAWK_BOOTSTRAP_X86_64_APPLE_DARWIN_SHA256__'
    ;;
  Linux:x86_64 | Linux:amd64)
    target='x86_64-unknown-linux-gnu'
    expected_sha256='__MARKET_SQUAWK_BOOTSTRAP_X86_64_UNKNOWN_LINUX_GNU_SHA256__'
    ;;
  *)
    echo "Market Squawk does not publish a terminal installer for this platform." >&2
    echo "Use the native packages on the GitHub Releases page instead." >&2
    exit 2
    ;;
esac

case "$tag:$expected_sha256" in
  *'__MARKET_SQUAWK_'*)
    echo "This is the release-builder template, not a published installer." >&2
    exit 2
    ;;
esac

temporary="$(mktemp -d "${TMPDIR:-/tmp}/market-squawk-install.XXXXXX")"
bootstrap="$temporary/market-squawk-bootstrap-$target"
cleanup() {
  rm -f "$bootstrap"
  rmdir "$temporary" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM

curl \
  --fail \
  --location \
  --silent \
  --show-error \
  --proto '=https' \
  --tlsv1.2 \
  --connect-timeout 30 \
  --max-time 1800 \
  --max-redirs 5 \
  --retry 3 \
  --retry-delay 2 \
  --output "$bootstrap" \
  "$release_base/market-squawk-bootstrap-$target"

if command -v sha256sum >/dev/null 2>&1; then
  observed_sha256="$(sha256sum "$bootstrap" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  observed_sha256="$(shasum -a 256 "$bootstrap" | awk '{print $1}')"
else
  echo "A SHA-256 verifier (sha256sum or shasum) is required." >&2
  exit 2
fi

if [ "$observed_sha256" != "$expected_sha256" ]; then
  echo "The downloaded Market Squawk installer failed verification." >&2
  exit 2
fi

chmod 700 "$bootstrap"
"$bootstrap" install --manifest-url "$manifest_url"

echo "Market Squawk is installed. Open the native application or run its installed launcher."
