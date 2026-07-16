from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]
STAGE_ONE_PLAN = (
    ROOT
    / "docs"
    / "superpowers"
    / "plans"
    / "2026-07-16-market-squawk-stage-1-foundation.md"
)
AUTHORITY_DOCS = (
    ROOT / "docs" / "architecture" / "target-state.md",
    ROOT / "docs" / "plans" / "implementation-plan.md",
    ROOT / "docs" / "research" / "2026-07-16-q1-contract-decisions.md",
)


class DocumentationContractTests(unittest.TestCase):
    def test_task_four_uses_the_binding_based_live_provenance_shape(self) -> None:
        plan = STAGE_ONE_PLAN.read_text()
        start = plan.index("- [ ] **Step 4: Implement canonical time and provenance**")
        end = plan.index("- [ ] **Step 5: Add canonical event families**")
        task_four = plan[start:end]

        self.assertIn("binding: LiveEvidenceBinding", task_four)
        self.assertIn("record_state: LiveRecordState", task_four)
        self.assertNotIn("source_id: SourceId", task_four)

    def test_authoritative_docs_do_not_overclaim_assessment_reference_validation(self) -> None:
        plan = STAGE_ONE_PLAN.read_text()
        start = plan.index("- [ ] **Step 4: Implement canonical time and provenance**")
        end = plan.index("- [ ] **Step 5: Add canonical event families**")
        task_four = plan[start:end]
        self.assertIn("does not dereference or prove", task_four)
        self.assertIn("caller-supplied archival", task_four)
        self.assertNotIn("carries the quality derived by", task_four)

        for path in AUTHORITY_DOCS:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIn("does not dereference or prove", path.read_text())


if __name__ == "__main__":
    unittest.main()
