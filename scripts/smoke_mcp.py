#!/usr/bin/env python3
from __future__ import annotations

import json
import pathlib
import queue
import subprocess
import sys
import tempfile
import threading
import time
from typing import TextIO


REQUEST_TIMEOUT_SECONDS = 15.0
SHUTDOWN_TIMEOUT_SECONDS = 25.0
SERVICE_START_TIMEOUT_SECONDS = 20.0


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


def finish_process(process: subprocess.Popen[str]) -> None:
    require(process.stdin is not None, "MCP process stdin is unavailable")
    process.stdin.close()
    try:
        return_code = process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        raise TimeoutError("MCP process did not complete bounded EOF shutdown") from error
    require(return_code == 0, f"MCP process exited with status {return_code}")


def wait_for_service(
    binary: pathlib.Path,
    data_dir: str,
    service: subprocess.Popen[str],
) -> None:
    deadline = time.monotonic() + SERVICE_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if service.poll() is not None:
            raise RuntimeError(
                f"installed service exited before readiness with status {service.returncode}"
            )
        probe = subprocess.run(
            [str(binary), "--data-dir", data_dir, "service", "status"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.1)
    raise TimeoutError(
        f"installed service did not reach readiness in {SERVICE_START_TIMEOUT_SECONDS:.3f}s"
    )


def main() -> int:
    arguments = sys.argv[1:]
    desktop_appimage = bool(arguments and arguments[0] == "--desktop-appimage")
    if desktop_appimage:
        arguments = arguments[1:]
    if len(arguments) > 1:
        print(
            "usage: smoke_mcp.py [--desktop-appimage] [/path/to/executable]",
            file=sys.stderr,
        )
        return 2

    binary = pathlib.Path(
        arguments[0] if arguments else "target/debug/market-squawk"
    ).resolve()
    require(binary.is_file(), f"Market Squawk binary does not exist: {binary}")
    with (
        tempfile.TemporaryDirectory() as data_dir,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr_log,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as service_stderr,
    ):
        service: subprocess.Popen[str] | None = None
        command = [
            str(binary),
            "--data-dir",
            data_dir,
            "mcp",
            "serve",
            "--client",
            "codex",
        ]
        if desktop_appimage:
            command = [str(binary), "--stdio-mcp", "--data-dir", data_dir]
        else:
            service_binary = binary.with_name(f"market-squawk-service{binary.suffix}")
            require(
                service_binary.is_file(),
                f"Market Squawk service binary does not exist: {service_binary}",
            )
            service = subprocess.Popen(
                [str(service_binary), "--data-dir", data_dir],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=service_stderr,
                text=True,
            )
            try:
                wait_for_service(binary, data_dir, service)
            except BaseException:
                stop_process(service)
                raise
        process = subprocess.Popen(
            command,
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
            require(process.stdin is not None, "MCP process stdin is unavailable")
            process.stdin.write(
                json.dumps(
                    {"jsonrpc": "2.0", "method": "notifications/initialized"}
                )
                + "\n"
            )
            process.stdin.flush()

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
            required_domains = {
                "Source",
                "Market",
                "Research",
                "Fundamental",
                "Macro",
                "Portfolio",
                "Analysis",
                "Model",
                "Decision",
                "FairValue",
                "Bot",
                "Execution",
                "Job",
            }
            observed_domains = {
                name.split(".", maxsplit=1)[0] for name in names if "." in name
            }
            missing_domains = sorted(required_domains - observed_domains)
            require(
                not missing_domains,
                f"MCP tool registry is missing required domains: {missing_domains}",
            )
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
                snapshot_contract.get("domain") == "market",
                "Market.GetSnapshot must expose its market-domain contract",
            )
            require(
                snapshot_contract.get("authorization") == "read_only",
                "Market.GetSnapshot must expose read-only authority",
            )
            require(
                snapshot_contract.get("result", {}).get("sourceEvidence")
                == "required",
                "Market.GetSnapshot must require source and quality evidence",
            )
            kill_switch_contract = (
                tools_by_name["Risk.TriggerKillSwitch"]
                .get("_meta", {})
                .get("org.market-squawk/tool-contract", {})
            )
            require(
                kill_switch_contract.get("domain") == "bot",
                "Risk.TriggerKillSwitch must expose its paper-bot domain",
            )
            require(
                kill_switch_contract.get("authorization") == "local_confirmation",
                "Risk.TriggerKillSwitch must require local confirmation",
            )
            status = request(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "Bot.GetStatus",
                        "arguments": {
                            "resultLimits": {
                                "maximumItems": 16,
                                "maximumBytes": 65536,
                            }
                        },
                    },
                },
            )
            require("error" not in status, f"Bot.GetStatus failed: {status}")
            require(
                status.get("result", {})
                .get("structuredContent", {})
                .get("data", {})
                .get("state")
                == "stopped",
                "Bot.GetStatus did not reach the production application",
            )
            mutation = request(
                process,
                {
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {
                        "name": "Risk.TriggerKillSwitch",
                        "arguments": {
                            "confirm": True,
                            "reason": "production MCP smoke",
                            "resultLimits": {
                                "maximumItems": 16,
                                "maximumBytes": 65536,
                            },
                        },
                    },
                },
            )
            require("error" not in mutation, f"Risk.TriggerKillSwitch failed: {mutation}")
            require(
                mutation.get("result", {})
                .get("structuredContent", {})
                .get("data", {})
                .get("shutdownComplete")
                is True,
                "Risk.TriggerKillSwitch did not complete through governed paper control",
            )
            print("MCP smoke test passed")
        except BaseException as error:
            failure = error
        finally:
            if failure is None:
                try:
                    finish_process(process)
                except BaseException as error:
                    failure = error
            else:
                stop_process(process)
            if service is not None:
                stop_process(service)
        if failure is not None:
            stderr_log.seek(0)
            diagnostics = stderr_log.read().strip()
            if diagnostics:
                print(f"MCP stderr:\n{diagnostics}", file=sys.stderr)
            service_stderr.seek(0)
            service_diagnostics = service_stderr.read().strip()
            if service_diagnostics:
                print(f"Service stderr:\n{service_diagnostics}", file=sys.stderr)
            raise failure
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
