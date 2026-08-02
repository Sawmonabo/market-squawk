#!/usr/bin/env python3
"""Closed command-line schema for capture-benchmark host evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

if __package__:
    from .capture_benchmark_host_schema import FIXTURE_MODE, PRODUCTION_MODE
else:
    from capture_benchmark_host_schema import FIXTURE_MODE, PRODUCTION_MODE


def parse_arguments() -> argparse.Namespace:
    """Parse and cross-validate the exact host-gate command contract."""

    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("phase", choices=("preflight", "postflight", "measure", "release"))
    parser.add_argument("--lock-dir", required=True, type=Path)
    parser.add_argument("--active-agent-attestation", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--evidence-mode",
        choices=(PRODUCTION_MODE, FIXTURE_MODE),
        default=PRODUCTION_MODE,
    )
    parser.add_argument("--observation-fixture", type=Path)
    parser.add_argument("--controlled-root", type=Path)
    parser.add_argument("--release-ticket", type=Path)
    parser.add_argument("--expected-lock-device", type=int)
    parser.add_argument("--expected-lock-inode", type=int)
    parser.add_argument("--expected-owner-device", type=int)
    parser.add_argument("--expected-owner-inode", type=int)
    parser.add_argument("--expected-nonce-sha256")
    parser.add_argument("--runner", type=Path)
    parser.add_argument("--build-evidence", type=Path)
    parser.add_argument(
        "--failure-injection",
        choices=(
            "file-fsync",
            "dir-fsync",
            "after-owner",
            "after-postflight",
            "missing-primitives",
            "partial-owner-write",
            "owner-write-failure",
            "monitor-competitor",
            "root-open-identity-mismatch",
            "partial-descriptor-read",
            "post-read-identity-mismatch",
        ),
    )
    parsed = parser.parse_args()
    if parsed.evidence_mode == PRODUCTION_MODE and (
        parsed.observation_fixture is not None
        or parsed.controlled_root is not None
        or parsed.failure_injection is not None
    ):
        parser.error("production mode forbids fixture-only path overrides")
    if parsed.evidence_mode == FIXTURE_MODE and (
        parsed.observation_fixture is None or parsed.controlled_root is None
    ):
        parser.error("fixture mode requires its closed observation seam and controlled root")
    if parsed.phase != "release" and (
        parsed.active_agent_attestation is None or parsed.output_dir is None
    ):
        parser.error("preflight and postflight require attestation and output paths")
    if parsed.phase == "release" and (
        parsed.release_ticket is None
        or parsed.expected_lock_device is None
        or parsed.expected_lock_inode is None
        or parsed.expected_owner_device is None
        or parsed.expected_owner_inode is None
        or parsed.expected_nonce_sha256 is None
    ):
        parser.error("release requires caller-bound preflight identity values")
    if parsed.phase == "measure" and (parsed.runner is None or parsed.build_evidence is None):
        parser.error("measure requires the exact runner and build-evidence paths")
    return parsed
