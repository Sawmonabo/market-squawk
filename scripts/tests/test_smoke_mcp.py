from __future__ import annotations

import importlib.util
from pathlib import Path
import threading
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "smoke_mcp.py"
SPEC = importlib.util.spec_from_file_location("smoke_mcp", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load smoke_mcp.py")
smoke_mcp = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke_mcp)


class BlockingStream:
    def __init__(self) -> None:
        self.release = threading.Event()

    def readline(self) -> str:
        self.release.wait()
        return ""


class SmokeMcpTests(unittest.TestCase):
    def test_require_is_not_removed_by_python_optimization(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "missing tool"):
            smoke_mcp.require(False, "missing tool")

    def test_readline_has_a_deadline(self) -> None:
        stream = BlockingStream()
        try:
            with self.assertRaisesRegex(TimeoutError, "timed out"):
                smoke_mcp.readline_with_timeout(stream, 0.01)
        finally:
            stream.release.set()


if __name__ == "__main__":
    unittest.main()
