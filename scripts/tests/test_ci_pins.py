from __future__ import annotations

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
REPOSITORY = WORKFLOW.parents[2]


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
        self.assertNotIn("toolchain: 1.97.0", workflow)
        self.assertEqual(workflow.count("toolchain: 1.97.1"), 3)
        self.assertIn("components: rustfmt, clippy", workflow)

        toolchain = (REPOSITORY / "rust-toolchain.toml").read_text()
        workspace = (REPOSITORY / "Cargo.toml").read_text()
        boundary_checker = (REPOSITORY / "scripts" / "check_workspace_boundaries.py").read_text()
        active_plan = (
            REPOSITORY
            / "docs"
            / "superpowers"
            / "plans"
            / "2026-07-16-market-squawk-complete-remaining-work.md"
        ).read_text()
        self.assertIn('channel = "1.97.1"', toolchain)
        self.assertIn('rust-version = "1.97.1"', workspace)
        self.assertIn('"rust_version": "1.97.1"', boundary_checker)
        self.assertNotIn('"rust_version": "1.97"', boundary_checker)
        self.assertIn("**Tech Stack:** Rust 1.97.1 stable", active_plan)
        self.assertNotIn("**Tech Stack:** Rust 1.97.0 stable", active_plan)


if __name__ == "__main__":
    unittest.main()
