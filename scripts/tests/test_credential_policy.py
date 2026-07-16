from __future__ import annotations

from pathlib import Path
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / ".gitleaks.toml"


class CredentialPolicyTests(unittest.TestCase):
    def test_policy_extends_the_full_builtin_rule_set(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        self.assertIs(policy["extend"]["useDefault"], True)
        self.assertNotIn("disabledRules", policy["extend"])

    def test_only_generated_build_and_isolated_worktree_trees_are_excluded(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        self.assertEqual(
            policy["allowlists"],
            [
                {
                    "description": "Exclude generated Rust build outputs",
                    "paths": [r"(^|/)target(/|$)"],
                },
                {
                    "description": "Exclude generated Python bytecode caches",
                    "paths": [r"(^|/)__pycache__(/|$)"],
                },
                {
                    "description": "Exclude independently reviewed Git worktrees",
                    "paths": [r"(^|/)\.worktrees(/|$)"],
                },
                {
                    "description": "Exclude ignored transient agent research scratch",
                    "paths": [r"(^|/)\.agents/tmp(/|$)"],
                },
            ],
        )

    def test_historical_false_positive_allowance_is_rule_path_and_line_scoped(self) -> None:
        policy = tomllib.loads(POLICY.read_text())
        self.assertEqual(
            policy["rules"],
            [
                {
                    "id": "generic-api-key",
                    "allowlists": [
                        {
                            "description": "ASVS authorization-control prose is not a credential",
                            "condition": "AND",
                            "regexTarget": "line",
                            "regexes": [
                                "selected Level 3 controls for credential storage, "
                                + "execution/risk authorization,"
                            ],
                            "paths": [
                                "^docs/research/2026-07-15-market-squawk/reports/"
                                + r"reputable-sources/batch-001\.md$"
                            ],
                        }
                    ],
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
