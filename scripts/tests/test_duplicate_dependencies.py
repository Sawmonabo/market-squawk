from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "check_duplicate_dependencies.py"
SPEC = importlib.util.spec_from_file_location("check_duplicate_dependencies", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load check_duplicate_dependencies.py")
check_duplicates = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_duplicates)


class DuplicateDependencyTests(unittest.TestCase):
    def test_duplicate_inventory_is_exact_and_order_independent(self) -> None:
        packages = [
            {"name": "alpha", "version": "2.0.0"},
            {"name": "single", "version": "1.0.0"},
            {"name": "alpha", "version": "1.0.0"},
        ]
        self.assertEqual(
            check_duplicates.duplicate_inventory(packages),
            {"alpha": ("1.0.0", "2.0.0")},
        )

    def test_new_duplicate_family_is_rejected(self) -> None:
        actual = {"new-family": ("1.0.0", "2.0.0")}
        violations = check_duplicates.inventory_violations(actual, {})
        self.assertEqual(
            violations,
            ["unexpected duplicate new-family: 1.0.0, 2.0.0"],
        )

    def test_version_drift_in_allowed_family_is_rejected(self) -> None:
        actual = {"alpha": ("1.0.0", "3.0.0")}
        allowed = {"alpha": ("1.0.0", "2.0.0")}
        violations = check_duplicates.inventory_violations(actual, allowed)
        self.assertEqual(
            violations,
            [
                "duplicate alpha changed: expected 1.0.0, 2.0.0; "
                "found 1.0.0, 3.0.0"
            ],
        )


if __name__ == "__main__":
    unittest.main()
