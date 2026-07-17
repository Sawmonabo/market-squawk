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
TARGET_ARCHITECTURE = ROOT / "docs" / "architecture" / "target-state.md"
Q1_CONTRACT_DECISIONS = (
    ROOT / "docs" / "research" / "2026-07-16-q1-contract-decisions.md"
)
METADATA_BINDING_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "classification" / "binding.rs"
)
PROVIDER_IDENTITIES_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "instrument" / "provider_identities.rs"
)
STALE_DOMAIN_REPORT = ROOT / ".superpowers" / "sdd" / "q1-final-domain-contracts-report.md"
STALE_PROVIDER_EVIDENCE_REPORT = (
    ROOT / ".superpowers" / "sdd" / "q1-final-provider-evidence-report.md"
)
EXACT_IDENTITY_REPORT = (
    ROOT / ".superpowers" / "sdd" / "q1-fix-exact-identity-evidence-report.md"
)
FINAL_DOCS_GATE_REPORT = ROOT / ".superpowers" / "sdd" / "q1-final-docs-gate-report.md"
CHANGELOG = ROOT / "CHANGELOG.md"
PROVENANCE_SOURCE = ROOT / "crates" / "market-squawk-domain" / "src" / "provenance.rs"
LIVE_PROVENANCE_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "provenance" / "live.rs"
)
RESEARCH_PROVENANCE_SOURCE = (
    ROOT / "crates" / "market-squawk-domain" / "src" / "provenance" / "research.rs"
)
Q2_CURRENT_STATE = ROOT / "docs" / "architecture" / "current-state.md"
Q2_GAP_ANALYSIS = ROOT / "docs" / "plans" / "gap-analysis.md"
Q2_IMPLEMENTATION_PLAN = ROOT / "docs" / "plans" / "implementation-plan.md"
Q2_CHECKPOINT_REVIEW = ROOT / "docs" / "reports" / "q2-checkpoint-review.md"
Q2_PROGRESS = ROOT / ".superpowers" / "sdd" / "progress.md"
Q2_STATE_DOCS = (
    Q2_CURRENT_STATE,
    Q2_GAP_ANALYSIS,
    Q2_IMPLEMENTATION_PLAN,
    Q2_CHECKPOINT_REVIEW,
    Q2_PROGRESS,
)
Q2_STATE_START = "<!-- q2-checkpoint-state"
Q2_STATE_END = "-->"
Q2_AUDIT_ANCHOR = "651a01e120dfe27a598b9475296733d238d870b7"
Q2_CANDIDATE_ID = "q2-integrated-remediation-2026-07-16"
Q2_ACTIVE_FINDINGS = ",".join(
    [*(f"Q2-I{number:02d}" for number in range(1, 12)), "Q2-M01", "Q2-M02"]
)


def normalized_source(path: Path) -> str:
    return " ".join(path.read_text().replace("///", "").split())


def q2_checkpoint_state(path: Path) -> dict[str, str]:
    document = path.read_text()
    start = document.index(Q2_STATE_START) + len(Q2_STATE_START)
    end = document.index(Q2_STATE_END, start)
    state: dict[str, str] = {}
    for line in document[start:end].splitlines():
        key, separator, value = line.strip().partition(":")
        if separator:
            state[key.strip()] = value.strip()
    return state


class DocumentationContractTests(unittest.TestCase):
    def test_q2_checkpoint_documents_share_one_machine_readable_state(self) -> None:
        states = []
        for path in Q2_STATE_DOCS:
            with self.subTest(path=path.relative_to(ROOT)):
                state = q2_checkpoint_state(path)
                self.assertEqual(state["candidate-id"], Q2_CANDIDATE_ID)
                self.assertEqual(state["audit-anchor"], Q2_AUDIT_ANCHOR)
                self.assertEqual(state["review-target"], "repository-head")
                self.assertEqual(state["prior-r01-r15"], "closed-as-framed")
                self.assertEqual(state["active-findings"], Q2_ACTIVE_FINDINGS)
                self.assertIn(
                    state["lifecycle"],
                    {
                        "remediation-in-progress",
                        "pending-exact-head-rereview",
                    },
                )
                states.append(state)
        self.assertTrue(states)
        self.assertTrue(all(state == states[0] for state in states[1:]))

    def test_q2_current_documents_do_not_reopen_superseded_r01_r15_findings(
        self,
    ) -> None:
        required_closure = "Q2-R01–R15 are closed as framed at `651a01e`"
        for path in Q2_STATE_DOCS:
            with self.subTest(path=path.relative_to(ROOT)):
                self.assertIn(required_closure, path.read_text())

        current_state = Q2_CURRENT_STATE.read_text()
        gap = Q2_GAP_ANALYSIS.read_text()
        implementation = Q2_IMPLEMENTATION_PLAN.read_text()

        self.assertNotIn(
            "Reviewed implementation baseline: integrated Q2 Tasks 1–8 commit `581d4fd`",
            current_state,
        )
        self.assertNotIn(
            "Q2-R01–Q2-R10 invalidate production-readiness claims until remediation",
            gap,
        )
        self.assertNotIn(
            "Q2 is **not** marked complete here: the root owner must integrate",
            implementation,
        )
        for identifier in (
            *(f"Q2-I{number:02d}" for number in range(1, 12)),
            "Q2-M01",
            "Q2-M02",
        ):
            self.assertIn(identifier, current_state)
            self.assertIn(identifier, gap)
            self.assertIn(identifier, implementation)

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

    def test_exact_evidence_docs_preserve_algorithm_identity_without_rejecting_changes(
        self,
    ) -> None:
        for path in (TARGET_ARCHITECTURE, Q1_CONTRACT_DECISIONS, STAGE_ONE_PLAN):
            with self.subTest(path=path.relative_to(ROOT)):
                source = normalized_source(path)
                self.assertIn(
                    "preserves the explicit digest algorithm as part of evidence identity",
                    source,
                )
                self.assertIn(
                    "changing the explicit algorithm while retaining the same bytes produces "
                    "distinct valid evidence",
                    source,
                )
                self.assertIn("rejects omitted content evidence and unknown fields", source)
                self.assertNotIn("algorithm transplants", source)
                self.assertNotIn("algorithm-erasing substitutions", source)

    def test_provider_identity_docs_separate_assertion_idempotence_from_metadata_mutation(
        self,
    ) -> None:
        for path in (TARGET_ARCHITECTURE, Q1_CONTRACT_DECISIONS, STAGE_ONE_PLAN):
            with self.subTest(path=path.relative_to(ROOT)):
                source = normalized_source(path)
                self.assertIn("creates no second logical assertion", source)
                self.assertIn(
                    "deterministically coalesces bounded locator and observation metadata",
                    source,
                )
                self.assertIn("returns `ObservationCoalesced`", source)
                self.assertIn(
                    "an exact repeat with no new metadata leaves canonical registry state unchanged",
                    source,
                )
                self.assertNotIn("idempotent no-op", source)

    def test_provider_identity_docs_bound_revision_authority_and_locator_cardinality(
        self,
    ) -> None:
        sections = (
            (
                TARGET_ARCHITECTURE,
                "Provider identity records bind",
                "`ChainId` validates",
            ),
            (
                Q1_CONTRACT_DECISIONS,
                "## Provider identity qualification",
                "## Chain namespace evidence",
            ),
            (
                STAGE_ONE_PLAN,
                "Provider-native identity assertions use",
                "- [ ] **Step 3: Implement exact scaled values**",
            ),
        )
        for path, start_marker, end_marker in sections:
            with self.subTest(path=path.relative_to(ROOT)):
                document = path.read_text()
                section = " ".join(
                    document[
                        document.index(start_marker) : document.index(end_marker)
                    ].split()
                )
                self.assertIn(
                    "caller/source-supplied revision and predecessor claims", section
                )
                self.assertIn(
                    "Authority must be established separately by the applicable registered "
                    "source and source-specific adapter verification",
                    section,
                )
                self.assertIn(
                    "these caller/source-supplied values do not establish it", section
                )
                self.assertIn(
                    "zero or more bounded canonical version-pinned locators", section
                )
                self.assertIn("`ProviderIdentityEvidence::MAX_LOCATORS = 64`", section)
                self.assertNotIn("authoritative metadata revision/evidence", section)
                self.assertNotIn("authoritative source-metadata revision", section)
                self.assertNotIn("valid newer authoritative revision", section)

    def test_exact_evidence_docs_do_not_treat_a_revision_binding_as_authority(self) -> None:
        for path in (TARGET_ARCHITECTURE, Q1_CONTRACT_DECISIONS, STAGE_ONE_PLAN):
            with self.subTest(path=path.relative_to(ROOT)):
                source = normalized_source(path)
                self.assertIn(
                    "the binding preserves association but does not by itself establish revision authority",
                    source,
                )
                self.assertIn(
                    "Authority must be established separately by the applicable registered "
                    "source and source-specific adapter verification",
                    source,
                )
                self.assertNotIn("exact payload evidence that established it", source)

    def test_metadata_revision_rustdoc_limits_the_identifier_claim(self) -> None:
        source = normalized_source(METADATA_BINDING_SOURCE)

        self.assertIn("Bounded caller/source-supplied revision identity", source)
        self.assertIn(
            "establishes neither authority nor immutability by itself",
            source,
        )
        self.assertIn("applicable registered source", source)
        self.assertIn("source-specific adapter verification", source)
        self.assertNotIn("surrounding evidence and registration", source)
        self.assertNotIn(
            "Immutable revision of the authoritative source metadata used by an assessment.",
            source,
        )

    def test_provider_identity_rustdoc_does_not_turn_evidence_into_authority(self) -> None:
        source = normalized_source(PROVIDER_IDENTITIES_SOURCE)

        self.assertIn("provider-supplied predecessor claim bound to exact evidence", source)
        self.assertIn("establishes neither authority nor immutability by itself", source)
        self.assertIn("bounded optional version-pinned locators", source)
        self.assertIn("non-substantive retrieval metadata", source)
        self.assertIn(
            "complete provider mapping assertion carrying exact content evidence", source
        )
        self.assertIn("source-specific adapter verification", source)
        self.assertNotIn("Complete immutable evidence", source)
        self.assertNotIn("evidence authorizing a provider metadata revision", source)
        self.assertNotIn("surrounding evidence establishes its authority", source)
        self.assertNotIn("optional version-pinned locator for the exact source assertion", source)

    def test_correction_reports_and_changelog_state_the_current_wire_contract(self) -> None:
        stale_report_prefix = STALE_DOMAIN_REPORT.read_text()[:500]
        exact_identity_report = normalized_source(EXACT_IDENTITY_REPORT)
        final_docs_gate_report = normalized_source(FINAL_DOCS_GATE_REPORT)
        changelog = normalized_source(CHANGELOG)

        self.assertIn("SUPERSEDED", stale_report_prefix)
        self.assertNotIn("generated-artifact checks", exact_identity_report)
        self.assertIn("creates no second logical assertion", final_docs_gate_report)
        self.assertIn("returns `ObservationCoalesced`", final_docs_gate_report)
        self.assertIn("`ExactPayloadEvidence`", changelog)
        self.assertIn("`RevisionBoundPayloadEvidence`", changelog)
        self.assertIn("legacy `source_reference`", changelog)
        self.assertIn("`provider_identity_registry`", changelog)

    def test_stale_provider_evidence_report_has_a_directional_supersession_banner(
        self,
    ) -> None:
        banner = STALE_PROVIDER_EVIDENCE_REPORT.read_text()[:1_000]

        self.assertIn("SUPERSEDED", banner)
        self.assertIn("q1-checkpoint-correction-report.md", banner)
        self.assertIn("../../docs/architecture/target-state.md", banner)
        self.assertIn(
            "../../docs/research/2026-07-16-q1-contract-decisions.md", banner
        )
        self.assertIn("locator metadata is non-substantive retrieval metadata", banner)


if __name__ == "__main__":
    unittest.main()
