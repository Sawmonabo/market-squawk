from __future__ import annotations

import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "check_brand.py"
SPEC = importlib.util.spec_from_file_location("check_brand", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load check_brand.py")
check_brand = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_brand)


class BrandCheckTests(unittest.TestCase):
    @staticmethod
    def first_text_allowance():
        return next(
            (key, allowance)
            for key, allowance in check_brand.ALLOWED_OCCURRENCES.items()
            if key[0] != key[2]
        )

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
        (path, token_index, container), allowance = self.first_text_allowance()
        self.assertTrue(
            check_brand.is_allowed(path, token_index, container)
        )
        self.assertFalse(
            check_brand.is_allowed(f"other/{path}", token_index, container)
        )

    def test_unrelated_preceding_lines_do_not_invalidate_exact_allowance(self) -> None:
        (path, token_index, container), _allowance = self.first_text_allowance()
        content = "unrelated heading\nunrelated paragraph\n" + container + "\n"
        violations, usage = check_brand.scan_text_with_usage(path, content)
        self.assertEqual(violations, [])
        self.assertEqual(usage[(path, token_index, container)], 1)

    def test_changed_allowed_line_is_rejected(self) -> None:
        (path, token_index, container), _allowance = self.first_text_allowance()
        content = container + " changed\n"
        violations, used = check_brand.scan_text_with_usage(path, content)
        self.assertTrue(violations)
        self.assertNotIn((path, token_index, container), used)

    def test_duplicate_token_on_allowed_line_is_rejected(self) -> None:
        (path, token_index, container), _allowance = self.first_text_allowance()
        token = check_brand.TOKENS[token_index]
        content = container + token + "\n"
        violations, used = check_brand.scan_text_with_usage(path, content)
        self.assertTrue(violations)
        self.assertNotIn((path, token_index, container), used)

    def test_duplicate_exact_allowed_lines_are_rejected(self) -> None:
        (path, token_index, container), allowance = self.first_text_allowance()
        content = f"{container}\n{container}\n"
        violations, usage = check_brand.scan_text_with_usage(path, content)
        self.assertTrue(violations)
        self.assertEqual(
            usage[(path, token_index, container)], allowance.expected_occurrences + 1
        )

    def test_declared_token_and_container_occurrence_counts_are_exact(self) -> None:
        path = "compatibility.txt"
        token_index = 3
        token = check_brand.TOKENS[token_index]
        container = f"legacy {token} and {token}"
        key = (path, token_index, container)
        allowance = check_brand.AllowedOccurrence(
            token_count=2, expected_occurrences=2
        )
        with mock.patch.object(check_brand, "ALLOWED_OCCURRENCES", {key: allowance}):
            violations, usage = check_brand.scan_text_with_usage(
                path, f"{container}\n{container}\n"
            )
            self.assertEqual(violations, [])
            self.assertEqual(check_brand.allowance_count_violations(usage), [])

            extra_violations, extra_usage = check_brand.scan_text_with_usage(
                path, f"{container}\n{container}\n{container}\n"
            )
            self.assertTrue(extra_violations)
            self.assertTrue(check_brand.allowance_count_violations(extra_usage))

            wrong_token_count = {
                key: check_brand.AllowedOccurrence(
                    token_count=1, expected_occurrences=2
                )
            }
            with mock.patch.object(
                check_brand, "ALLOWED_OCCURRENCES", wrong_token_count
            ):
                count_violations, count_usage = check_brand.scan_text_with_usage(
                    path, f"{container}\n{container}\n"
                )
                self.assertTrue(count_violations)
                self.assertEqual(count_usage, {})

    def test_unused_allowance_fails_closed(self) -> None:
        violations = check_brand.allowance_count_violations({})
        self.assertEqual(len(violations), len(check_brand.ALLOWED_OCCURRENCES))


if __name__ == "__main__":
    unittest.main()
