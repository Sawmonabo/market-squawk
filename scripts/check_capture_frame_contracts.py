#!/usr/bin/env python3
"""Reject compiler-visible RawCaptureFrameView implementations outside the reviewed set."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path


REPOSITORY = Path(__file__).resolve().parent.parent
IMPLEMENTOR_RELATIVE_PATH = Path(
    "doc/trait.impl/market_squawk_domain/capture/trait.RawCaptureFrameView.js"
)
TRAIT_RELATIVE_PATH = Path(
    "doc/market_squawk_domain/trait.RawCaptureFrameView.html"
)
EXPECTED_IMPLEMENTORS = {
    "market_squawk_platform::DiagnosticCaptureFrame",
    "market_squawk_sources::RawMarketFrame",
}
IMPLEMENTATION_START = re.compile(r'\[\["impl ')
TARGET_TITLE = re.compile(
    r' for <a class=\\"(?:struct|enum|type|union)\\" '
    r'href=\\"[^\"]+\\" title=\\"(?:struct|enum|type|union) '
    r'([^\"]+)\\">'
)
RUSTDOC_VERSION = re.compile(r'data-rustdoc-version="1\.97\.1 \([^\"]+\)"')


def semantic_implementors(implementor_javascript: str) -> set[str]:
    """Extract normalized target paths from pinned-rustdoc semantic implementor output."""

    starts = len(IMPLEMENTATION_START.findall(implementor_javascript))
    targets = set(TARGET_TITLE.findall(implementor_javascript))
    if len(targets) != starts:
        raise ValueError(
            "rustdoc implementor output was not one-to-one parseable: "
            f"implementation_count={starts}, unique_target_count={len(targets)}"
        )
    return targets


def generate_rustdoc(target_directory: Path) -> None:
    """Generate fresh all-workspace semantic documentation with the pinned stable toolchain."""

    environment = os.environ.copy()
    environment.update(
        {
            "CARGO_NET_OFFLINE": "true",
            "CARGO_TARGET_DIR": str(target_directory),
            "RUSTDOCFLAGS": "-D warnings",
        }
    )
    subprocess.run(
        [
            "cargo",
            "doc",
            "--workspace",
            "--all-features",
            "--no-deps",
            "--locked",
        ],
        cwd=REPOSITORY,
        env=environment,
        check=True,
    )


def validate_generated_rustdoc(target_directory: Path) -> None:
    """Validate the semantic trait inventory and pinned rustdoc provenance."""

    implementor_path = target_directory / IMPLEMENTOR_RELATIVE_PATH
    trait_path = target_directory / TRAIT_RELATIVE_PATH
    implementor_javascript = implementor_path.read_text(encoding="utf-8")
    trait_html = trait_path.read_text(encoding="utf-8")
    if not RUSTDOC_VERSION.search(trait_html):
        raise ValueError("capture frame inventory was not produced by pinned rustdoc 1.97.1")
    observed = semantic_implementors(implementor_javascript)
    if observed != EXPECTED_IMPLEMENTORS:
        missing = sorted(EXPECTED_IMPLEMENTORS - observed)
        unexpected = sorted(observed - EXPECTED_IMPLEMENTORS)
        raise ValueError(
            "compiler-visible RawCaptureFrameView production inventory changed without "
            f"contract review: missing={missing!r}, unexpected={unexpected!r}"
        )


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="market-squawk-frame-contracts-") as temporary:
        target_directory = Path(temporary) / "target"
        generate_rustdoc(target_directory)
        validate_generated_rustdoc(target_directory)
    print("compiler-derived capture frame production implementation inventory is exact")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
