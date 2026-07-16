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
PROVENANCE_SOURCE = ROOT / "crates" / "market-squawk-domain" / "src" / "provenance.rs"
LIVE_PROVENANCE_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "provenance" / "live.rs"
)
RESEARCH_PROVENANCE_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "provenance" / "research.rs"
)


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text().replace("///", "").split())


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

    def test_public_provenance_rustdoc_does_not_overclaim_archival_evidence(self) -> None:
        provenance = normalized_source(PROVENANCE_SOURCE)
        live = normalized_source(LIVE_PROVENANCE_SOURCE)

        self.assertNotIn("it requires a successful qualification", provenance)
        self.assertIn(
            "recorded label is only a caller-supplied archival assertion", provenance
        )
        self.assertIn("does not prove successful qualification", provenance)
        self.assertNotIn("linked to retained evidence", live)
        self.assertIn("caller-supplied archival assertion", live)
        self.assertIn("does not retain assessment evidence", live)
        self.assertIn("does not dereference the reference", live)
        self.assertIn("proves neither its existence nor its relationship", live)
        self.assertIn("never grants current execution authority", live)

    def test_payload_reference_rustdoc_preserves_opaque_locator_semantics(self) -> None:
        provenance = normalized_source(PROVENANCE_SOURCE)
        research = normalized_source(RESEARCH_PROVENANCE_SOURCE)

        self.assertIn("opaque source-side record locator", provenance)
        self.assertIn("no inherent existence, immutability, or retrievability guarantee", provenance)
        self.assertIn("opaque source record identity", research)
        self.assertIn("no inherent immutability guarantee", research)


if __name__ == "__main__":
    unittest.main()
