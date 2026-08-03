#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import queue
import re
import secrets
import subprocess
import sys
import tempfile
import threading
import time
from typing import TextIO


REQUEST_TIMEOUT_SECONDS = 15.0
SHUTDOWN_TIMEOUT_SECONDS = 25.0
SERVICE_STATUS_TIMEOUT_SECONDS = 35.0
SERVICE_BOOTSTRAP_TIMEOUT_SECONDS = 45.0
SERVICE_START_TIMEOUT_SECONDS = 60.0
MCP_PROTOCOL_VERSION = "2026-07-28"
MAXIMUM_DIAGNOSTIC_BYTES = 8 * 1024


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise one named Market Squawk MCP relay over stdio."
    )
    parser.add_argument("binary", nargs="?", default="target/debug/market-squawk")
    parser.add_argument("--client", choices=("claude-code", "codex"), default="codex")
    parser.add_argument("--data-dir", type=pathlib.Path)
    parser.add_argument(
        "--running-service",
        action="store_true",
        help="Use the already-ready installed service instead of starting a sibling service.",
    )
    parser.add_argument(
        "--installed-relay",
        action="store_true",
        help="Treat binary as the installed market-squawk-mcp-relay executable.",
    )
    parser.add_argument("--desktop-appimage", action="store_true")
    arguments = parser.parse_args()
    if arguments.desktop_appimage and (
        arguments.running_service or arguments.installed_relay
    ):
        parser.error("--desktop-appimage cannot use --running-service or --installed-relay")
    if arguments.running_service and arguments.data_dir is None:
        parser.error("--running-service requires --data-dir")
    if arguments.installed_relay and not arguments.running_service:
        parser.error("--installed-relay requires --running-service")
    return arguments


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


def bounded_redacted_diagnostics(
    stream: TextIO, sensitive_values: tuple[str, ...] = ()
) -> str:
    stream.seek(0)
    encoded = stream.read(MAXIMUM_DIAGNOSTIC_BYTES + 1).encode(
        "utf-8", errors="replace"
    )
    truncated = len(encoded) > MAXIMUM_DIAGNOSTIC_BYTES
    text = encoded[:MAXIMUM_DIAGNOSTIC_BYTES].decode("utf-8", errors="replace")
    for sensitive in sensitive_values:
        if sensitive:
            text = text.replace(sensitive, "[REDACTED]")
    text = re.sub(
        r"(?i)(secret|credential|token|password|unlock)(\s*[:=]\s*)[^\s,;]+",
        r"\1\2[REDACTED]",
        text,
    )
    text = "".join(character for character in text if character in "\n\r\t" or character.isprintable())
    text = text.strip()
    if truncated:
        text = f"{text}\n[diagnostics truncated]" if text else "[diagnostics truncated]"
    return text


def bootstrap_service(
    binary: pathlib.Path,
    data_dir: str,
    requirement: str,
) -> None:
    unlock = secrets.token_urlsafe(32) if requirement == "encrypted_fallback_locked" else ""
    command = [
        str(binary),
        "--output",
        "json",
        "--data-dir",
        data_dir,
        "service",
        "bootstrap",
    ]
    if unlock:
        command.append("--stdin")
    elif requirement == "foreground_keyring_retry":
        command.append("--retry-after-foreground-keyring")
    else:
        raise RuntimeError("installed service returned an unsupported bootstrap requirement")
    with (
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stdout,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr,
    ):
        result = subprocess.run(
            command,
            input=f"{unlock}\n" if unlock else None,
            stdout=stdout,
            stderr=stderr,
            text=True,
            check=False,
            timeout=SERVICE_BOOTSTRAP_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            diagnostics = bounded_redacted_diagnostics(stderr, (unlock,))
            detail = f": {diagnostics}" if diagnostics else ""
            raise RuntimeError(
                f"installed CLI bootstrap exited with status {result.returncode}{detail}"
            )


def wait_for_service(
    binary: pathlib.Path,
    data_dir: str,
    service: subprocess.Popen[str],
) -> None:
    deadline = time.monotonic() + SERVICE_START_TIMEOUT_SECONDS
    bootstrap_attempted = False
    while time.monotonic() < deadline:
        if service.poll() is not None:
            raise RuntimeError(
                f"installed service exited before readiness with status {service.returncode}"
            )
        with (
            tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as probe_stdout,
            tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as probe_stderr,
        ):
            probe = subprocess.run(
                [
                    str(binary),
                    "--output",
                    "json",
                    "--data-dir",
                    data_dir,
                    "service",
                    "status",
                ],
                stdout=probe_stdout,
                stderr=probe_stderr,
                check=False,
                timeout=SERVICE_STATUS_TIMEOUT_SECONDS,
                text=True,
            )
            if probe.returncode == 0:
                probe_stdout.seek(0)
                try:
                    status = json.load(probe_stdout)
                except json.JSONDecodeError:
                    status = None
                if isinstance(status, dict) and status.get("status") == "ready":
                    return
                bootstrap = status.get("bootstrap") if isinstance(status, dict) else None
                if (
                    not bootstrap_attempted
                    and isinstance(bootstrap, dict)
                    and status.get("status") == "bootstrap_required"
                    and bootstrap.get("state") == "required"
                    and isinstance(bootstrap.get("requirement"), str)
                ):
                    bootstrap_attempted = True
                    bootstrap_service(binary, data_dir, bootstrap["requirement"])
        time.sleep(0.1)
    raise TimeoutError(
        f"installed service did not reach readiness in {SERVICE_START_TIMEOUT_SECONDS:.3f}s"
    )


def main() -> int:
    arguments = parse_arguments()
    binary = pathlib.Path(arguments.binary).resolve()
    require(binary.is_file(), f"Market Squawk binary does not exist: {binary}")
    with (
        tempfile.TemporaryDirectory() as temporary_data_dir,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as stderr_log,
        tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as service_stderr,
    ):
        data_dir = str(arguments.data_dir or temporary_data_dir)
        service: subprocess.Popen[str] | None = None
        command = [
            str(binary),
            "--data-dir",
            data_dir,
            "mcp",
            "serve",
            "--client",
            arguments.client,
        ]
        if arguments.desktop_appimage:
            command = [str(binary), "--stdio-mcp", "--data-dir", data_dir]
        elif arguments.installed_relay:
            relay_client = "claude" if arguments.client == "claude-code" else "codex"
            command = [
                str(binary),
                "--client",
                relay_client,
                "--data-dir",
                data_dir,
            ]
        if not arguments.desktop_appimage and not arguments.running_service:
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
                        "protocolVersion": MCP_PROTOCOL_VERSION,
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
            require(
                initialized.get("result", {}).get("protocolVersion")
                == MCP_PROTOCOL_VERSION,
                "MCP initialize response has the wrong protocol version",
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
            diagnostics = bounded_redacted_diagnostics(stderr_log)
            if diagnostics:
                print(f"MCP stderr:\n{diagnostics}", file=sys.stderr)
            service_diagnostics = bounded_redacted_diagnostics(service_stderr)
            if service_diagnostics:
                print(f"Service stderr:\n{service_diagnostics}", file=sys.stderr)
            raise failure
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
