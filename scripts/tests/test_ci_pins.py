from __future__ import annotations

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"


class CiPinTests(unittest.TestCase):
    def test_every_external_action_is_pinned_to_an_immutable_commit(self) -> None:
        workflow = WORKFLOW.read_text()
        references = re.findall(r"^\s*- uses: ([^\s]+)$", workflow, flags=re.MULTILINE)
        self.assertTrue(references)
        for reference in references:
            with self.subTest(reference=reference):
                self.assertRegex(reference, r"^[^@]+@[0-9a-f]{40}$")

    def test_rust_setup_declares_the_exact_toolchain_and_components(self) -> None:
        workflow = WORKFLOW.read_text()
        self.assertIn("toolchain: 1.97.0", workflow)
        self.assertIn("components: rustfmt, clippy", workflow)


if __name__ == "__main__":
    unittest.main()
