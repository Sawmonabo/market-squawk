from __future__ import annotations

from pathlib import Path
import re
import unittest


VERIFY_SCRIPT = Path(__file__).resolve().parents[1] / "verify.sh"


class VerifyScriptTests(unittest.TestCase):
    def test_generated_artifact_gate_runs_before_compilation(self) -> None:
        script = VERIFY_SCRIPT.read_text()
        artifact_check = "python3 scripts/check_generated_artifacts.py"
        self.assertIn(artifact_check, script)
        self.assertLess(script.index(artifact_check), script.index("cargo fmt"))

    def test_locked_all_feature_workspace_doctests_are_explicit(self) -> None:
        script = VERIFY_SCRIPT.read_text()
        self.assertRegex(
            script,
            re.compile(
                r"^cargo test --doc --workspace --all-features --locked$",
                flags=re.MULTILINE,
            ),
        )


if __name__ == "__main__":
    unittest.main()
