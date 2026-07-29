from __future__ import annotations

from pathlib import Path
import re
import unittest


WORKFLOWS = tuple(
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / name
    for name in ("ci.yml", "release.yml")
)
ACTION_PIN = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")


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


class CiWorkflowPolicyTests(unittest.TestCase):
    def test_external_actions_are_immutable_and_checkout_never_persists_credentials(self) -> None:
        for workflow_path in WORKFLOWS:
            action_steps = [
                block
                for block in workflow_step_blocks(workflow_path.read_text())
                if "- uses:" in block
            ]
            self.assertTrue(action_steps)
            checkout_steps = 0
            for step in action_steps:
                reference_match = re.search(
                    r"^- uses: (\S+)$",
                    step.lstrip(),
                    re.MULTILINE,
                )
                self.assertIsNotNone(reference_match, step)
                reference = reference_match.group(1) if reference_match else ""
                with self.subTest(workflow=workflow_path.name, reference=reference):
                    self.assertRegex(reference, ACTION_PIN)
                    if reference.startswith("actions/checkout@"):
                        checkout_steps += 1
                        self.assertRegex(
                            step,
                            re.compile(
                                r"^\s+persist-credentials: false$",
                                re.MULTILINE,
                            ),
                        )
            self.assertGreater(checkout_steps, 0)

    def test_stable_publication_waits_for_draft_package_installation(self) -> None:
        release = WORKFLOWS[1].read_text()
        self.assertRegex(
            release,
            re.compile(
                r"^  candidate-smoke:\n"
                r"(?:.*\n)*?"
                r"^  publish:\n"
                r"(?:.*\n)*?"
                r"^      - candidate-smoke$",
                re.MULTILINE,
            ),
        )


if __name__ == "__main__":
    unittest.main()
