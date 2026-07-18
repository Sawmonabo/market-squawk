#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import queue
import subprocess
import sys
import tempfile
import threading
from typing import TextIO


REQUEST_TIMEOUT_SECONDS = 5.0
SHUTDOWN_TIMEOUT_SECONDS = 5.0


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def readline_with_timeout(stream: TextIO, timeout_seconds: float) -> str:
    outcomes: queue.Queue[tuple[str | None, BaseException | None]] = queue.Queue(maxsize=1)

    def read_line() -> None:
        try:
            outcomes.put((stream.readline(), None))
        except BaseException as error:  # Propagate reader failures to the controlling thread.
            outcomes.put((None, error))

    threading.Thread(target=read_line, daemon=True).start()
    try:
        line, error = outcomes.get(timeout=timeout_seconds)
    except queue.Empty as error:
        raise TimeoutError(f"MCP response timed out after {timeout_seconds:.3f}s") from error
    if error is not None:
        raise RuntimeError("failed to read MCP response") from error
    return line or ""


def request(process: subprocess.Popen[str], payload: dict) -> dict:
    require(process.stdin is not None, "MCP process stdin is unavailable")
    require(process.stdout is not None, "MCP process stdout is unavailable")
    if process.poll() is not None:
        raise RuntimeError(f"MCP server exited before request with status {process.returncode}")
    process.stdin.write(json.dumps(payload) + "\n")
    process.stdin.flush()
    line = readline_with_timeout(process.stdout, REQUEST_TIMEOUT_SECONDS)
    if not line:
        raise RuntimeError("MCP server closed stdout")
    response = json.loads(line)
    require(isinstance(response, dict), "MCP response must be a JSON object")
    return response


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)


def main() -> int:
    if len(sys.argv) > 2:
        print("usage: smoke_mcp.py [/path/to/market-squawk]", file=sys.stderr)
        return 2

    binary = pathlib.Path(
        sys.argv[1] if len(sys.argv) == 2 else "target/debug/market-squawk"
    ).resolve()
    require(binary.is_file(), f"Market Squawk binary does not exist: {binary}")
    with (
        tempfile.TemporaryDirectory() as data_dir,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr_log,
    ):
        process = subprocess.Popen(
            [str(binary), "--data-dir", data_dir, "mcp", "--offline"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr_log,
            text=True,
            bufsize=1,
        )
        failure: BaseException | None = None
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
            require(
                initialized.get("result", {}).get("serverInfo", {}).get("name")
                == "market-squawk",
                "MCP initialize response has the wrong server identity",
            )

            tools = request(
                process,
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            )
            tool_entries = tools.get("result", {}).get("tools")
            require(isinstance(tool_entries, list), "MCP tools/list response has no tool list")
            names = {
                tool.get("name")
                for tool in tool_entries
                if isinstance(tool, dict) and isinstance(tool.get("name"), str)
            }
            require("Market.GetSnapshot" in names, "Market.GetSnapshot tool is missing")
            require("Risk.TriggerKillSwitch" in names, "Risk.TriggerKillSwitch tool is missing")
            tools_by_name = {
                tool["name"]: tool
                for tool in tool_entries
                if isinstance(tool, dict) and isinstance(tool.get("name"), str)
            }
            snapshot_contract = (
                tools_by_name["Market.GetSnapshot"]
                .get("_meta", {})
                .get("org.market-squawk/tool-contract", {})
            )
            require(
                snapshot_contract.get("maximumDataQuality") == "direct_unverified",
                "Market.GetSnapshot must expose its structured quality ceiling",
            )
            require(
                snapshot_contract.get("executionAuthority") == "none",
                "Market.GetSnapshot must expose its lack of execution authority",
            )
            kill_switch_contract = (
                tools_by_name["Risk.TriggerKillSwitch"]
                .get("_meta", {})
                .get("org.market-squawk/tool-contract", {})
            )
            require(
                kill_switch_contract.get("executionAuthority") == "none",
                "Risk.TriggerKillSwitch must expose its lack of execution authority",
            )
            require(
                kill_switch_contract.get("simulationAccess") == "none",
                "Risk.TriggerKillSwitch must not read paper-simulation state",
            )
            require(
                kill_switch_contract.get("controlAuthority")
                == "paper_simulation_stop_only",
                "Risk.TriggerKillSwitch must remain confined to paper-simulation control",
            )
            require(
                kill_switch_contract.get("resourceScope")
                == "current_paper_simulation_run",
                "Risk.TriggerKillSwitch must remain confined to the current local run",
            )
            print("MCP smoke test passed")
        except BaseException as error:
            failure = error
        finally:
            stop_process(process)
        if failure is not None:
            stderr_log.seek(0)
            diagnostics = stderr_log.read().strip()
            if diagnostics:
                print(f"MCP stderr:\n{diagnostics}", file=sys.stderr)
            raise failure
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
