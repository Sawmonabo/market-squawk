from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
GITHUB = ROOT / ".github"
AUDIT = ROOT / "docs" / "audits" / "2026-07-17-github-governance-audit.md"


def dependabot_ecosystem_blocks(document: str) -> dict[str, str]:
    matches = list(
        re.finditer(
            r"(?m)^  - package-ecosystem: (?P<name>[a-z0-9-]+)$",
            document,
        )
    )
    blocks: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(document)
        blocks[match.group("name")] = document[match.start() : end]
    return blocks


def issue_form_ids(document: str) -> list[str]:
    return re.findall(
        r"(?m)^  - type: (?:input|dropdown|textarea|checkboxes)\n"
        r"    id: ([a-zA-Z0-9_-]+)$",
        document,
    )


class RepositoryGovernancePolicyTests(unittest.TestCase):
    def test_dual_license_files_match_the_workspace_declaration(self) -> None:
        apache = (ROOT / "LICENSE-APACHE").read_text()
        mit = (ROOT / "LICENSE-MIT").read_text()
        readme = (ROOT / "README.md").read_text()

        header = [line.strip() for line in apache.splitlines() if line.strip()][:3]
        self.assertEqual(
            header,
            [
                "Apache License",
                "Version 2.0, January 2004",
                "http://www.apache.org/licenses/",
            ],
        )
        for heading in (
            "1. Definitions.",
            "2. Grant of Copyright License.",
            "3. Grant of Patent License.",
            "4. Redistribution.",
            "5. Submission of Contributions.",
            "6. Trademarks.",
            "7. Disclaimer of Warranty.",
            "8. Limitation of Liability.",
            "9. Accepting Warranty or Additional Liability.",
            "APPENDIX: How to apply the Apache License to your work.",
        ):
            self.assertIn(heading, apache)

        self.assertTrue(mit.startswith("MIT License\n\nCopyright (c) 2026 "))
        self.assertIn(
            "Permission is hereby granted, free of charge, to any person obtaining a copy",
            mit,
        )
        self.assertIn(
            'THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR',
            mit,
        )
        self.assertNotRegex(mit, r"(?i)<year>|<copyright|\bTBD\b")

        license_section = readme[readme.index("## License") :]
        self.assertIn("Apache-2.0 OR MIT", license_section)
        self.assertRegex(license_section, r"\[[^]]+\]\(LICENSE-APACHE\)")
        self.assertRegex(license_section, r"\[[^]]+\]\(LICENSE-MIT\)")

    def test_dependabot_updates_cargo_and_actions_weekly_under_human_ownership(self) -> None:
        document = (GITHUB / "dependabot.yml").read_text()
        blocks = dependabot_ecosystem_blocks(document)
        self.assertEqual(set(blocks), {"cargo", "github-actions"})

        for ecosystem, block in blocks.items():
            with self.subTest(ecosystem=ecosystem):
                self.assertRegex(block, r"(?m)^    directory: /$")
                self.assertRegex(block, r"(?m)^      interval: weekly$")
                self.assertRegex(block, r"(?m)^    open-pull-requests-limit: 5$")

        codeowners = (GITHUB / "CODEOWNERS").read_text()
        ownership = [
            line.split()
            for line in codeowners.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertIn(["*", "@Sawmonabo"], ownership)
        self.assertIn(["/.github/", "@Sawmonabo"], ownership)
        self.assertTrue(all(entry[-1] == "@Sawmonabo" for entry in ownership))

    def test_ci_cancels_only_superseded_pull_request_runs(self) -> None:
        workflow = (GITHUB / "workflows" / "ci.yml").read_text()
        concurrency = re.search(
            r"(?ms)^concurrency:\n(?P<body>.*?)(?=^permissions:)", workflow
        )
        self.assertIsNotNone(concurrency)
        body = concurrency.group("body") if concurrency else ""
        self.assertIn("github.workflow", body)
        self.assertIn("github.event_name", body)
        self.assertIn("github.event.pull_request.number || github.run_id", body)
        self.assertIn(
            "cancel-in-progress: ${{ github.event_name == 'pull_request' }}", body
        )
        self.assertNotRegex(body, r"(?m)^\s*cancel-in-progress: true$")

        self.assertIn("push:\n    branches: [main]", workflow)
        self.assertIn("permissions:\n  contents: read", workflow)

    def test_collaboration_templates_are_structured_and_route_sensitive_reports(self) -> None:
        pull_request = (GITHUB / "PULL_REQUEST_TEMPLATE.md").read_text()
        for heading in (
            "## Outcome and scope",
            "## Security and authority impact",
            "## Verification evidence",
            "## Blast radius and rollback",
            "## Reviewer checklist",
        ):
            self.assertIn(heading, pull_request)
        self.assertIn("Do not paste credentials", pull_request)
        self.assertIn(
            "https://github.com/Sawmonabo/market-squawk/blob/main/SECURITY.md",
            pull_request,
        )
        self.assertNotIn("](../SECURITY.md)", pull_request)
        self.assertNotIn("${{", pull_request)

        config = (GITHUB / "ISSUE_TEMPLATE" / "config.yml").read_text()
        self.assertRegex(config, r"(?m)^blank_issues_enabled: false$")
        self.assertIn(
            "https://github.com/Sawmonabo/market-squawk/blob/main/SECURITY.md",
            config,
        )
        self.assertIn("Do not disclose vulnerability details in a public issue", config)

        forms = {
            "bug": (GITHUB / "ISSUE_TEMPLATE" / "01-bug.yml").read_text(),
            "feature": (GITHUB / "ISSUE_TEMPLATE" / "02-feature.yml").read_text(),
        }
        self.assertRegex(forms["bug"], r'(?m)^labels: \["bug"\]$')
        self.assertRegex(forms["feature"], r'(?m)^labels: \["enhancement"\]$')
        for name, document in forms.items():
            with self.subTest(form=name):
                normalized = " ".join(document.split())
                ids = issue_form_ids(document)
                self.assertGreaterEqual(len(ids), 4)
                self.assertEqual(len(ids), len(set(ids)))
                self.assertGreaterEqual(document.count("required: true"), 4)
                self.assertIn("SECURITY.md", document)
                self.assertIn("Do not include credentials", normalized)
                self.assertNotRegex(document, r"\$\{\{|\{%|(?i:\bTBD\b|\bTODO\b)")

        self.assertIn("Do not report security vulnerabilities here", forms["bug"])

    def test_code_of_conduct_is_publishable_or_omission_is_explicit(self) -> None:
        audit = AUDIT.read_text()
        code_of_conduct = ROOT / "CODE_OF_CONDUCT.md"
        if code_of_conduct.exists():
            document = code_of_conduct.read_text()
            self.assertIn("Contributor Covenant 3.0 Code of Conduct", document)
            self.assertIn("Organization for Ethical Source", document)
            self.assertIn("CC BY-SA 4.0", document)
            self.assertNotIn("[NOTE:", document)
        else:
            self.assertIn("Code of Conduct: intentionally omitted", audit)
            self.assertIn("private conduct-reporting channel", audit)

    def test_audit_records_exact_observations_without_overclaiming_enforcement(self) -> None:
        document = AUDIT.read_text()
        for marker in (
            "Retrieved: 2026-07-17",
            "Audit base: `6d8801a1e9f5027bc3e1db3500a9a1a9f9fb85d6`",
            "Remote `main`: `c568ef07c676e0f0e440a5e218c653bcc8757e94`",
            "Branch protection and rulesets",
            "Secret scanning and push protection",
            "Auto-merge enforcement",
            "https://docs.github.com/",
            "https://www.contributor-covenant.org/version/3/0/",
        ):
            self.assertIn(marker, document)
        self.assertIn("does not enforce", document)
        self.assertNotIn("unavailable on this plan", document.lower())


if __name__ == "__main__":
    unittest.main()
