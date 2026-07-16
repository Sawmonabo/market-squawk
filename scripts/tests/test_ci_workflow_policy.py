from __future__ import annotations

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci.yml"
ACTION_PIN = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
PORTABLE_COMMANDS = (
    "cargo build --workspace --all-features --locked",
    "cargo test --workspace --all-targets --all-features --locked",
)


def workflow_step_blocks(workflow: str) -> list[str]:
    lines = workflow.splitlines()
    blocks: list[str] = []
    current: list[str] = []
    step_indent: int | None = None
    for line in lines:
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if stripped.startswith("- ") and (step_indent is None or indent == step_indent):
            if current:
                blocks.append("\n".join(current))
            current = [line]
            step_indent = indent
        elif current:
            if stripped and indent < (step_indent or 0):
                blocks.append("\n".join(current))
                current = []
                step_indent = None
            else:
                current.append(line)
    if current:
        blocks.append("\n".join(current))
    return blocks


def job_block(workflow: str, job_name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(job_name)}:\n(?P<body>.*?)(?=^  [a-zA-Z0-9_-]+:\n|\Z)",
        workflow,
    )
    if match is None:
        raise AssertionError(f"workflow job is missing: {job_name}")
    return match.group("body")


class CiWorkflowPolicyTests(unittest.TestCase):
    def test_external_actions_are_immutable_and_checkout_never_persists_credentials(self) -> None:
        workflow = WORKFLOW.read_text()
        action_steps = [
            block for block in workflow_step_blocks(workflow) if "- uses:" in block
        ]
        self.assertTrue(action_steps)
        checkout_steps = 0
        for step in action_steps:
            reference_match = re.search(r"^- uses: (\S+)$", step.lstrip(), re.MULTILINE)
            self.assertIsNotNone(reference_match, step)
            reference = reference_match.group(1) if reference_match else ""
            with self.subTest(reference=reference):
                self.assertRegex(reference, ACTION_PIN)
                if reference.startswith("actions/checkout@"):
                    checkout_steps += 1
                    self.assertRegex(
                        step,
                        re.compile(r"^\s+persist-credentials: false$", re.MULTILINE),
                    )
        self.assertEqual(checkout_steps, 3)

    def test_only_explicit_supported_runner_labels_are_used(self) -> None:
        workflow = WORKFLOW.read_text()
        runner_labels = re.findall(r"^\s+runs-on: ([^\s]+)$", workflow, re.MULTILINE)
        self.assertCountEqual(
            runner_labels,
            ["ubuntu-24.04", "macos-15-intel", "windows-2025"],
        )
        self.assertNotRegex(workflow, r"\b[a-z0-9-]+-latest\b")
        self.assertNotIn("ubuntu-26.04", workflow)

    def test_linux_remains_the_full_local_verification_gate(self) -> None:
        linux = job_block(WORKFLOW.read_text(), "verify")
        self.assertIn("runs-on: ubuntu-24.04", linux)
        self.assertIn("- run: ./scripts/verify.sh", linux)

    def test_macos_and_windows_run_locked_all_target_workspace_coverage(self) -> None:
        workflow = WORKFLOW.read_text()
        for job_name, runner in (
            ("macos", "macos-15-intel"),
            ("windows", "windows-2025"),
        ):
            block = job_block(workflow, job_name)
            with self.subTest(job=job_name):
                self.assertIn(f"runs-on: {runner}", block)
                for command in PORTABLE_COMMANDS:
                    self.assertIn(f"- run: {command}", block)


if __name__ == "__main__":
    unittest.main()
