"""Adversarial parser tests for compiler-derived capture frame inventory."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "check_capture_frame_contracts.py"
SPEC = importlib.util.spec_from_file_location("check_capture_frame_contracts", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load capture frame contract checker")
CHECKER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECKER)


def rustdoc_implementation(target: str, display: str | None = None) -> str:
    """Return one pinned-rustdoc-style implementor entry."""

    name = display or target.rsplit("::", maxsplit=1)[-1]
    return (
        '[["impl &lt;T&gt; RawCaptureFrameView for '
        f'<a class=\\"struct\\" href=\\"crate/struct.{name}.html\\" '
        f'title=\\"struct {target}\\">{name}</a>",0]]'
    )


class SemanticImplementorTests(unittest.TestCase):
    def test_normalizes_generic_and_qualified_compiler_targets(self) -> None:
        artifact = rustdoc_implementation("fixture::GenericFrame")
        self.assertEqual(
            CHECKER.semantic_implementors(artifact), {"fixture::GenericFrame"}
        )

    def test_macro_generated_implementor_is_visible_in_semantic_output(self) -> None:
        artifact = rustdoc_implementation("fixture::MacroGeneratedFrame")
        self.assertEqual(
            CHECKER.semantic_implementors(artifact),
            {"fixture::MacroGeneratedFrame"},
        )

    def test_source_comments_and_string_literals_are_not_inventory_inputs(self) -> None:
        artifact = rustdoc_implementation("fixture::RealFrame")
        artifact += "/* impl RawCaptureFrameView for CommentOnly */"
        artifact += '"impl RawCaptureFrameView for StringOnly"'
        self.assertEqual(CHECKER.semantic_implementors(artifact), {"fixture::RealFrame"})

    def test_rejects_unparseable_or_duplicate_target_output(self) -> None:
        artifact = rustdoc_implementation("fixture::Frame")
        artifact += rustdoc_implementation("fixture::Frame")
        with self.assertRaisesRegex(ValueError, "not one-to-one parseable"):
            CHECKER.semantic_implementors(artifact)


if __name__ == "__main__":
    unittest.main()
