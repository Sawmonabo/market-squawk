from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[2]
RUNNER = ROOT / "scripts" / "run_exact_loom_gate.sh"
MODEL_A = "capture::queue::loom_model::send_close_and_drain_races"
MODEL_B = "capture::writer::runtime::loom_model::fixed_storage_transfer_and_final_drop"


class ExactLoomGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.bin_directory = self.directory / "bin"
        self.bin_directory.mkdir()
        self.log = self.directory / "cargo.log"
        self._write_executable(
            "rustc",
            """
            #!/bin/sh
            if [ "$1" = "-vV" ]; then
                printf '%s\n' 'rustc 1.97.1' 'host: aarch64-apple-darwin'
                exit 0
            fi
            exit 91
            """,
        )
        self._write_executable(
            "cargo",
            """
            #!/bin/sh
            printf '%s\n' "$*" >>"$FAKE_LOOM_CARGO_LOG"
            command_name=$1
            shift
            if [ "$command_name" = "clippy" ]; then
                [ "${FAKE_LOOM_FAIL_MODE:-}" != "clippy" ]
                exit
            fi
            if [ "$command_name" != "test" ]; then
                exit 92
            fi
            case " $* " in
                *" -- --list "*)
                    if [ "${FAKE_LOOM_FAIL_MODE:-}" = "list" ]; then
                        exit 17
                    fi
                    printf '%b' "$FAKE_LOOM_LISTING"
                    ;;
                *)
                    if [ "${FAKE_LOOM_FAIL_MODE:-}" = "execute" ]; then
                        exit 19
                    fi
                    ;;
            esac
            """,
        )

    def _write_executable(self, name: str, content: str) -> None:
        path = self.bin_directory / name
        path.write_text(textwrap.dedent(content).lstrip())
        path.chmod(0o755)

    def _environment(self, listing: str | None = None) -> dict[str, str]:
        allowed_names = {
            "HOME",
            "LANG",
            "LC_ALL",
            "LOGNAME",
            "PATH",
            "SHELL",
            "TMPDIR",
            "USER",
        }
        environment = {
            key: value for key, value in os.environ.items() if key in allowed_names
        }
        environment.update(
            {
                "PATH": f"{self.bin_directory}:/usr/bin:/bin",
                "FAKE_LOOM_CARGO_LOG": str(self.log),
                "FAKE_LOOM_LISTING": listing
                if listing is not None
                else f"{MODEL_B}: test\n{MODEL_A}: test\n",
            }
        )
        return environment

    def _run(
        self,
        *models: str,
        listing: str | None = None,
        extra_environment: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        environment = self._environment(listing)
        if extra_environment:
            environment.update(extra_environment)
        return subprocess.run(
            [
                str(RUNNER),
                "--package",
                "market-squawk-platform",
                *sum((["--model", model] for model in models), []),
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_exact_success_owns_environment_target_inventory_and_filters(self) -> None:
        result = self._run(MODEL_A, MODEL_B)
        self.assertEqual(result.returncode, 0, result.stderr)
        invocations = self.log.read_text().splitlines()
        common = (
            "-p market-squawk-platform --lib --all-features --release --locked "
            "--target aarch64-apple-darwin"
        )
        self.assertEqual(
            invocations,
            [
                f"test {common} -- --list",
                f"clippy {common} -- -D warnings",
                f"test {common} {MODEL_A} -- --exact --test-threads=1",
                f"test {common} {MODEL_B} -- --exact --test-threads=1",
            ],
        )

    def test_missing_model_fails_closed(self) -> None:
        result = self._run(MODEL_A, MODEL_B, listing=f"{MODEL_A}: test\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inventory mismatch", result.stderr)

    def test_renamed_model_fails_closed(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            listing=f"{MODEL_A}: test\n{MODEL_B}_renamed: test\n",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inventory mismatch", result.stderr)

    def test_extra_model_fails_closed(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            listing=(
                f"{MODEL_A}: test\n{MODEL_B}: test\n"
                "capture::accounting::loom_model::unexpected: test\n"
            ),
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("inventory mismatch", result.stderr)

    def test_duplicate_declared_model_fails_closed(self) -> None:
        result = self._run(MODEL_A, MODEL_A)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate declared Loom model", result.stderr)

    def test_duplicate_listed_model_fails_closed(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            listing=f"{MODEL_A}: test\n{MODEL_A}: test\n{MODEL_B}: test\n",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("duplicate listed Loom model", result.stderr)

    def test_zero_declared_models_fail_closed(self) -> None:
        result = self._run()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("at least one exact Loom model", result.stderr)

    def test_zero_listed_models_fail_closed(self) -> None:
        result = self._run(MODEL_A, listing="ordinary::test: test\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("listed zero reserved Loom models", result.stderr)

    def test_listing_failure_propagates(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            extra_environment={"FAKE_LOOM_FAIL_MODE": "list"},
        )
        self.assertEqual(result.returncode, 17)

    def test_clippy_failure_propagates(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            extra_environment={"FAKE_LOOM_FAIL_MODE": "clippy"},
        )
        self.assertNotEqual(result.returncode, 0)

    def test_execution_failure_propagates(self) -> None:
        result = self._run(
            MODEL_A,
            MODEL_B,
            extra_environment={"FAKE_LOOM_FAIL_MODE": "execute"},
        )
        self.assertEqual(result.returncode, 19)

    def test_forbidden_environment_names_fail_before_cargo(self) -> None:
        forbidden = {
            "LOOM_MAX_BRANCHES": "1",
            "RUSTFLAGS": "-C target-cpu=native",
            "RUSTDOCFLAGS": "--cfg hidden",
            "CARGO_ENCODED_RUSTFLAGS": "--cfg\x1fhidden",
            "CARGO_BUILD_TARGET": "x86_64-unknown-linux-gnu",
            "CARGO_TARGET_DIR": "/tmp/target",
            "CARGO_TARGET_AARCH64_APPLE_DARWIN_RUSTFLAGS": "-C opt-level=0",
            "CARGO_PROFILE_RELEASE_LTO": "false",
            "RUSTC": "/tmp/rustc",
            "RUSTC_WRAPPER": "/tmp/wrapper",
            "RUSTC_WORKSPACE_WRAPPER": "/tmp/wrapper",
        }
        for name, value in forbidden.items():
            with self.subTest(name=name):
                if self.log.exists():
                    self.log.unlink()
                result = self._run(
                    MODEL_A,
                    MODEL_B,
                    extra_environment={name: value},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(name, result.stderr)
                self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
