#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import tempfile


def request(process: subprocess.Popen[str], payload: dict) -> dict:
    assert process.stdin is not None
    assert process.stdout is not None
    process.stdin.write(json.dumps(payload) + "\n")
    process.stdin.flush()
    line = process.stdout.readline()
    if not line:
        raise RuntimeError("MCP server closed stdout")
    return json.loads(line)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: smoke_mcp.py /path/to/market-squawk", file=sys.stderr)
        return 2

    binary = pathlib.Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory() as data_dir:
        process = subprocess.Popen(
            [str(binary), "--data-dir", data_dir, "mcp", "--offline"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            initialized = request(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "smoke-test", "version": "1"},
                    },
                },
            )
            assert initialized["result"]["serverInfo"]["name"] == "market-squawk"

            tools = request(
                process,
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            )
            names = {tool["name"] for tool in tools["result"]["tools"]}
            assert "Market.GetSnapshot" in names
            assert "Risk.TriggerKillSwitch" in names
            print("MCP smoke test passed")
        finally:
            process.terminate()
            process.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
