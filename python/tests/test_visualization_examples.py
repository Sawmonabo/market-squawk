from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

from market_squawk.visualization import VisualizationError, chart_spec, static_svg


ROOT = Path(__file__).resolve().parents[2]
_fixture = runpy.run_path(
    str(ROOT / "python" / "tests" / "test_data.py"), run_name="_dataset_fixture"
)["_fixture"]


class VisualizationAndExamplesContracts(unittest.TestCase):
    def test_chart_outputs_are_bounded_self_contained_and_deterministic(self) -> None:
        rows = [{"at": index, "value": index / 10} for index in range(4)]
        spec = chart_spec(rows, x="at", y="value", title="PIT fixture", max_points=4)
        encoded = json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()
        self.assertEqual(
            hashlib.sha256(encoded).hexdigest(),
            "8122ec9a3d7bbf045dd58b1b9b6420f08121d2dceacf52d834e2987d989889e2",
        )
        svg = static_svg(rows, x="at", y="value", title="PIT fixture", max_points=4)
        self.assertTrue(svg.startswith("<svg "))
        self.assertNotIn("<script", svg.lower())
        self.assertNotIn("href=", svg.lower())
        self.assertNotIn("url(", svg.lower())
        self.assertNotIn(str(ROOT), svg)
        with self.assertRaises(VisualizationError):
            chart_spec(rows + [{"at": 5, "value": 0.5}], x="at", y="value", max_points=4)

    def test_local_example_and_notebook_execute_without_downloads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            fixture = Path(temporary)
            export_sha256 = _fixture(fixture)
            environment = {
                "MARKET_SQUAWK_EXAMPLE_DATASET_ROOT": str(fixture),
                "MARKET_SQUAWK_EXAMPLE_EXPORT_SHA256": export_sha256,
            }
            with patch.dict(os.environ, environment):
                namespace = runpy.run_path(
                    str(ROOT / "python" / "examples" / "pit_research.py")
                )
                self.assertEqual(namespace["RESULT"]["rows"], 2)

                notebook = json.loads(
                    (ROOT / "python" / "examples" / "pit_research.ipynb").read_text()
                )
                scope = {"__name__": "__notebook_test__"}
                for cell in notebook["cells"]:
                    if cell["cell_type"] == "code":
                        exec("".join(cell["source"]), scope, scope)
                self.assertEqual(scope["RESULT"]["rows"], 2)


if __name__ == "__main__":
    unittest.main()
