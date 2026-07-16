from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "check_brand.py"
SPEC = importlib.util.spec_from_file_location("check_brand", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load check_brand.py")
check_brand = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_brand)


class BrandCheckTests(unittest.TestCase):
    def test_each_forbidden_content_token_is_reported(self) -> None:
        for index, token in enumerate(check_brand.TOKENS):
            with self.subTest(token=token):
                self.assertEqual(
                    check_brand.scan_text("example.txt", f"prefix {token} suffix\n"),
                    [f"example.txt:1:{token}"],
                )

    def test_forbidden_path_is_reported_even_for_binary_content(self) -> None:
        path = "fixtures/legacy" + "." + "mej"
        self.assertEqual(
            check_brand.scan_path(path),
            [f"{path}:0:{'.' + 'mej'}"],
        )

    def test_binary_file_is_not_treated_as_text(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.bin"
            path.write_bytes(("Market" + " Engine").encode() + b"\x00binary")
            text, error = check_brand.read_bounded_text(path)
        self.assertIsNone(text)
        self.assertIsNone(error)

    def test_oversized_text_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "large.txt"
            path.write_bytes(b"x" * (check_brand.MAX_TEXT_BYTES + 1))
            text, error = check_brand.read_bounded_text(path)
        self.assertIsNone(text)
        self.assertIn("exceeds", error or "")

    def test_allowed_compatibility_occurrence_is_narrow(self) -> None:
        path, line, token_index = next(iter(check_brand.ALLOWED_OCCURRENCES))
        token = check_brand.TOKENS[token_index]
        allowance = check_brand.ALLOWED_OCCURRENCES[(path, line, token_index)]
        self.assertTrue(
            check_brand.is_allowed(path, line, token_index, allowance.container)
        )
        self.assertFalse(
            check_brand.is_allowed(path, line + 1, token_index, allowance.container)
        )

    def test_changed_allowed_line_is_rejected(self) -> None:
        path, line, token_index = next(
            key for key in check_brand.ALLOWED_OCCURRENCES if key[1] > 0
        )
        allowance = check_brand.ALLOWED_OCCURRENCES[(path, line, token_index)]
        content = "\n" * (line - 1) + allowance.container + " changed\n"
        violations, used = check_brand.scan_text_with_usage(path, content)
        self.assertTrue(violations)
        self.assertNotIn((path, line, token_index), used)

    def test_duplicate_token_on_allowed_line_is_rejected(self) -> None:
        path, line, token_index = next(
            key for key in check_brand.ALLOWED_OCCURRENCES if key[1] > 0
        )
        allowance = check_brand.ALLOWED_OCCURRENCES[(path, line, token_index)]
        token = check_brand.TOKENS[token_index]
        content = "\n" * (line - 1) + allowance.container + token + "\n"
        violations, used = check_brand.scan_text_with_usage(path, content)
        self.assertTrue(violations)
        self.assertNotIn((path, line, token_index), used)

    def test_unused_allowance_fails_closed(self) -> None:
        violations = check_brand.unused_allowance_violations(set())
        self.assertEqual(len(violations), len(check_brand.ALLOWED_OCCURRENCES))


if __name__ == "__main__":
    unittest.main()
