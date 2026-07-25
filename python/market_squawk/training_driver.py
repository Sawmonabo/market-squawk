"""Sealed production driver for deterministic training, candidate export, and admission."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from typing import Any, Mapping, Sequence

from .bundle import BundleAuthorityRef, BundleExportError
from .data import DatasetIntegrityError, UtcNanoseconds, open_dataset
from .finance import OperationContext
from .training import (
    TrainingRun,
    TrainingValidationError,
    training_environment_receipt,
)


MAX_CONFIG_BYTES = 256 * 1024
MAX_OUTPUT_BYTES = 8 * 1024 * 1024
MAX_NATIVE_EXECUTABLE_BYTES = 256 * 1024 * 1024
NATIVE_READ_BYTES = 1024 * 1024
IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{0,126}[a-z0-9]$|^[a-z0-9]$")
PATH_COMPONENT = re.compile(r"^[a-z0-9][a-z0-9._-]{0,254}$")
HEX = re.compile(r"^[0-9a-f]{64}$")


class TrainingDriverError(ValueError):
    """The sealed driver rejected input, authority, publication, or release identity."""


def write_proposal(
    config_path: Path | str,
    output_path: Path | str,
) -> Mapping[str, Any]:
    """Train once and exclusively write exact independent-authority proposal bytes."""

    config = _load_config(config_path)
    proposal = _proposal(config)
    output = _write_exclusive(Path(output_path), proposal.authority_bytes)
    return {
        "authorityPath": str(output),
        "authoritySha256": proposal.authority_sha256,
        "artifactSha256": proposal.candidate.artifact_sha256,
        "metadataSha256": proposal.candidate.metadata_sha256,
        "trainingRunSha256": proposal.candidate.training_run_sha256,
    }


def finalize_candidate(
    config_path: Path | str,
    authority_path: Path | str,
    candidate_parent: str,
    request_path: Path | str,
) -> Mapping[str, Any]:
    """Retrain exactly, validate independent authority, and emit an admission request."""

    config = _load_config(config_path)
    proposal = _proposal(config)
    data_root = Path(config["dataset"]["root"])
    authority = _authority_ref(Path(authority_path), data_root, proposal.authority_sha256)
    output_root = _controlled_candidate_parent(data_root, candidate_parent)
    receipt = proposal.export(
        output_root,
        authority,
        context=_operation_context(config),
    )
    output_semantics = (
        "binary_probability"
        if config["training"]["modelKind"] == "logistic"
        else "regression"
    )
    onnx = config["onnx"]
    request = {
        "schemaVersion": 1,
        "candidateDirectory": f"{candidate_parent}/candidate",
        "metadata": {
            "relativePath": "bundle.json",
            "sha256": receipt.metadata_sha256,
        },
        "authority": {
            "path": str((authority.root / authority.relative_path).resolve()),
            "sha256": receipt.authority_sha256,
        },
        "dataset": {
            "exportSha256": receipt.dataset_export_sha256,
            "asOfUnixNanos": config["dataset"]["asOfUnixNanos"],
            "selectionSha256": receipt.dataset_selection_sha256,
            "catalogIdentitySha256": receipt.catalog_identity_sha256,
        },
        "backend": {
            "kind": "onnx",
            "modelSha256": receipt.artifact_sha256,
            "opset": onnx["opset"],
            "inputShape": [1, len(config["training"]["features"])],
            "outputShape": [1, 1],
            "outputSemantics": output_semantics,
            "inferenceDeadlineMillis": onnx["inferenceDeadlineMilliseconds"],
            "fallback": onnx["fallback"],
        },
    }
    request_output = _write_exclusive(Path(request_path), _canonical(request))
    return {
        "admissionRequest": str(request_output),
        "candidateDirectory": request["candidateDirectory"],
        "metadataSha256": receipt.metadata_sha256,
        "artifactSha256": receipt.artifact_sha256,
        "trainingRunSha256": receipt.training_run_sha256,
        "authoritySha256": receipt.authority_sha256,
    }


def admit_candidate(
    config_path: Path | str,
    request_path: Path | str,
) -> Mapping[str, Any]:
    """Invoke the exact signed sibling application for one confirmed durable admission."""

    config = _load_config(config_path)
    receipt = training_environment_receipt()
    release_root = Path(sys.prefix).resolve(strict=True)
    application = release_root / "bin" / "market-squawk"
    worker = release_root / "bin" / "market-squawk-onnx-worker"
    validator = release_root / "bin" / "market-squawk-model-validator"
    expected = (
        (application, receipt.application_sha256),
        (worker, receipt.onnx_worker_sha256),
        (validator, receipt.validator_sha256),
    )
    before = tuple(_executable_sha256(path) for path, _digest in expected)
    if any(observed != digest for observed, (_path, digest) in zip(before, expected, strict=True)):
        raise TrainingDriverError("signed native release identity mismatch")
    request = Path(request_path).absolute()
    command = [
        str(application),
        "--data-dir",
        config["dataset"]["root"],
        "--training-release-root",
        str(release_root),
        "--output",
        "json",
        "model",
        "admit",
        str(request),
        "--confirm",
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=70,
            env={
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/bin:/bin",
                "TZ": "UTC",
            },
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise TrainingDriverError("signed model admission did not complete") from error
    after = tuple(_executable_sha256(path) for path, _digest in expected)
    if (
        before != after
        or completed.returncode != 0
        or not completed.stdout
        or len(completed.stdout) > 64 * 1024
        or len(completed.stderr) > 64 * 1024
    ):
        raise TrainingDriverError("signed model admission was rejected")
    try:
        value = json.loads(completed.stdout.decode("ascii"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, TrainingDriverError) as error:
        raise TrainingDriverError("signed model admission evidence is invalid") from error
    if not isinstance(value, dict):
        raise TrainingDriverError("signed model admission evidence is invalid")
    return value


def _proposal(config: Mapping[str, Any]):
    dataset_config = config["dataset"]
    dataset = open_dataset(
        dataset_config["root"],
        dataset_config["exportSha256"],
        UtcNanoseconds(dataset_config["asOfUnixNanos"]),
        max_rows=dataset_config["maximumRows"],
        max_bytes=dataset_config["maximumBytes"],
        context=_operation_context(config),
    )
    training = config["training"]
    run = TrainingRun(
        dataset=dataset,
        features=training["features"],
        label=training["label"],
        seed=training["seed"],
        missing_policy=training["missingPolicy"],
        environment=training_environment_receipt(),
        model_id=training["modelId"],
        bundle_id=training["bundleId"],
        bundle_version=training["bundleVersion"],
    )
    return run.fit_evaluate(
        model_kind=training["modelKind"],
        artifact_format=training["artifactFormat"],
        context=_operation_context(config),
    )


def _operation_context(config: Mapping[str, Any]) -> OperationContext:
    operation = config["operation"]
    return OperationContext(
        operation["timeoutMilliseconds"],
        operation["maximumOperations"],
    )


def _load_config(path: Path | str) -> Mapping[str, Any]:
    value = _read_json(Path(path), MAX_CONFIG_BYTES)
    if not isinstance(value, dict) or set(value) != {
        "schemaVersion",
        "dataset",
        "training",
        "operation",
        "onnx",
    }:
        raise TrainingDriverError("training configuration shape is invalid")
    if value["schemaVersion"] != 1:
        raise TrainingDriverError("training configuration version is unsupported")
    dataset = _mapping(
        value["dataset"],
        {
            "root",
            "exportSha256",
            "asOfUnixNanos",
            "maximumRows",
            "maximumBytes",
        },
        "dataset",
    )
    root = _absolute_controlled_directory(dataset["root"], "dataset root")
    _hex(dataset["exportSha256"], "dataset export")
    _integer(dataset["asOfUnixNanos"], -(2**63), 2**63 - 1, "dataset cutoff")
    _integer(dataset["maximumRows"], 1, 100_000, "dataset row limit")
    _integer(
        dataset["maximumBytes"],
        1,
        256 * 1024 * 1024,
        "dataset byte limit",
    )
    training = _mapping(
        value["training"],
        {
            "features",
            "label",
            "seed",
            "missingPolicy",
            "modelId",
            "bundleId",
            "bundleVersion",
            "modelKind",
            "artifactFormat",
        },
        "training",
    )
    features = training["features"]
    if not isinstance(features, list) or not 1 <= len(features) <= 1_024:
        raise TrainingDriverError("training feature list is invalid")
    for feature in features:
        _feature(feature)
    _label(training["label"])
    _integer(training["seed"], 0, 2**64 - 1, "training seed")
    if training["missingPolicy"] not in {"reject", "drop_row"}:
        raise TrainingDriverError("missing-value policy is unsupported")
    _bounded_string(training["modelId"], 36, "model identity")
    _identifier(training["bundleId"], "bundle identity")
    _integer(training["bundleVersion"], 1, 2**64 - 1, "bundle version")
    if training["modelKind"] not in {"linear", "logistic"}:
        raise TrainingDriverError("model kind is unsupported")
    if training["artifactFormat"] != "onnx":
        raise TrainingDriverError("production driver requires the ONNX artifact format")
    operation = _mapping(
        value["operation"],
        {"timeoutMilliseconds", "maximumOperations"},
        "operation",
    )
    _integer(operation["timeoutMilliseconds"], 1, 60_000, "operation timeout")
    _integer(
        operation["maximumOperations"],
        1,
        50_000_000,
        "operation budget",
    )
    onnx = _mapping(
        value["onnx"],
        {"opset", "inferenceDeadlineMilliseconds", "fallback"},
        "ONNX",
    )
    if onnx["opset"] != 13:
        raise TrainingDriverError("ONNX producer supports only opset 13")
    _integer(
        onnx["inferenceDeadlineMilliseconds"],
        1,
        5_000,
        "ONNX inference deadline",
    )
    if onnx["fallback"] != "no_action":
        raise TrainingDriverError("ONNX fallback is unsupported")
    return {
        "schemaVersion": 1,
        "dataset": {**dataset, "root": str(root)},
        "training": training,
        "operation": operation,
        "onnx": onnx,
    }


def _read_json(path: Path, maximum: int) -> Any:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise TrainingDriverError("training input is unavailable") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
            raise TrainingDriverError("training input is not a bounded regular file")
        content = bytearray()
        while len(content) < before.st_size:
            chunk = os.read(descriptor, min(64 * 1024, before.st_size - len(content)))
            if not chunk:
                raise TrainingDriverError("training input changed during read")
            content.extend(chunk)
        if os.read(descriptor, 1):
            raise TrainingDriverError("training input exceeds its byte bound")
        after = os.fstat(descriptor)
        if _file_identity(before) != _file_identity(after):
            raise TrainingDriverError("training input changed during read")
    finally:
        os.close(descriptor)
    try:
        return json.loads(
            bytes(content).decode("ascii"),
            object_pairs_hook=_unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TrainingDriverError("training input JSON is invalid") from error


def _unique_object(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise TrainingDriverError("training JSON contains a duplicate field")
        value[key] = item
    return value


def _mapping(value: Any, fields: set[str], name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise TrainingDriverError(f"{name} configuration shape is invalid")
    return dict(value)


def _feature(value: Any) -> None:
    feature = _mapping(
        value,
        {"name", "version", "input_schema_sha256", "semantic_sha256"},
        "feature",
    )
    _identifier(feature["name"], "feature name")
    _integer(feature["version"], 1, 2**32 - 1, "feature version")
    _hex(feature["input_schema_sha256"], "feature input schema")
    _hex(feature["semantic_sha256"], "feature semantics")


def _label(value: Any) -> None:
    label = _mapping(
        value,
        {"kind", "scope", "corporate_action_sensitivity", "name", "version"},
        "label",
    )
    if label["kind"] != "label" or label["scope"] not in {
        "instrument",
        "account",
        "global",
    }:
        raise TrainingDriverError("label kind or scope is invalid")
    if label["corporate_action_sensitivity"] not in {
        "not_applicable",
        "requires_adjustment",
    }:
        raise TrainingDriverError("label action policy is invalid")
    _identifier(label["name"], "label name")
    _integer(label["version"], 1, 2**32 - 1, "label version")


def _integer(value: Any, minimum: int, maximum: int, name: str) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise TrainingDriverError(f"{name} is invalid")


def _hex(value: Any, name: str) -> None:
    if not isinstance(value, str) or HEX.fullmatch(value) is None or value == "0" * 64:
        raise TrainingDriverError(f"{name} digest is invalid")


def _identifier(value: Any, name: str) -> None:
    if not isinstance(value, str) or IDENTIFIER.fullmatch(value) is None:
        raise TrainingDriverError(f"{name} is invalid")


def _bounded_string(value: Any, maximum: int, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("ascii", errors="ignore")) != len(value)
        or len(value) > maximum
    ):
        raise TrainingDriverError(f"{name} is invalid")


def _absolute_controlled_directory(value: Any, name: str) -> Path:
    if not isinstance(value, str) or not value:
        raise TrainingDriverError(f"{name} is invalid")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.is_dir():
        raise TrainingDriverError(f"{name} is not an absolute controlled directory")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise TrainingDriverError(f"{name} contains an indirect coordinate")
    return resolved


def _authority_ref(
    path: Path,
    data_root: Path,
    expected_sha256: str,
) -> BundleAuthorityRef:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise TrainingDriverError("bundle authority must be one explicit absolute regular file")
    root = path.parent.resolve(strict=True)
    if root == data_root or root.is_relative_to(data_root) or data_root.is_relative_to(root):
        raise TrainingDriverError("bundle authority is not disjoint from the data root")
    try:
        return BundleAuthorityRef.exact(root, path.name, expected_sha256)
    except BundleExportError as error:
        raise TrainingDriverError("bundle authority does not match the proposal") from error


def _controlled_candidate_parent(data_root: Path, relative: str) -> Path:
    parts = _candidate_parts(relative)
    artifacts = data_root / "artifacts"
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0)
    no_follow_flags = flags | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(artifacts, no_follow_flags)
    except OSError as error:
        raise TrainingDriverError("data artifact root is unavailable") from error
    try:
        for part in parts:
            try:
                os.mkdir(part, 0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            next_descriptor = os.open(part, no_follow_flags, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        os.fsync(descriptor)
    except OSError as error:
        raise TrainingDriverError("candidate parent is not a controlled directory") from error
    finally:
        os.close(descriptor)
    return artifacts.joinpath(*parts)


def _candidate_parts(value: str) -> tuple[str, ...]:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("ascii", errors="ignore")) != len(value)
        or len(value) > 512
    ):
        raise TrainingDriverError("candidate parent coordinate is invalid")
    path = Path(value)
    parts = path.parts
    if (
        path.is_absolute()
        or not 2 <= len(parts) <= 32
        or parts[0] != "models"
        or any(PATH_COMPONENT.fullmatch(part) is None for part in parts)
    ):
        raise TrainingDriverError("candidate parent coordinate is invalid")
    return parts


def _write_exclusive(path: Path, content: bytes) -> Path:
    if not content or len(content) > MAX_OUTPUT_BYTES:
        raise TrainingDriverError("driver output exceeds its byte bound")
    absolute = path.absolute()
    parent = absolute.parent
    if parent.is_symlink() or not parent.is_dir():
        raise TrainingDriverError("driver output parent is not a controlled directory")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(absolute, flags, 0o600)
    except OSError as error:
        raise TrainingDriverError("driver output cannot be created exclusively") from error
    try:
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:
                raise TrainingDriverError("driver output write did not make progress")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return absolute


def _executable_sha256(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise TrainingDriverError("signed native executable is unavailable") from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAX_NATIVE_EXECUTABLE_BYTES
            or before.st_mode & 0o111 == 0
        ):
            raise TrainingDriverError("signed native executable is invalid")
        digest = hashlib.sha256()
        observed = 0
        while True:
            chunk = os.read(descriptor, NATIVE_READ_BYTES)
            if not chunk:
                break
            observed += len(chunk)
            if observed > before.st_size:
                raise TrainingDriverError("signed native executable changed")
            digest.update(chunk)
        after = os.fstat(descriptor)
        if observed != before.st_size or _file_identity(before) != _file_identity(after):
            raise TrainingDriverError("signed native executable changed")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
    )


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        content = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError) as error:
        raise TrainingDriverError("driver output is not canonical JSON") from error
    if not content or len(content) > MAX_OUTPUT_BYTES:
        raise TrainingDriverError("driver output exceeds its byte bound")
    return content


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="market-squawk-train")
    commands = parser.add_subparsers(dest="command", required=True)
    propose = commands.add_parser("propose")
    propose.add_argument("--config", required=True, type=Path)
    propose.add_argument("--output", required=True, type=Path)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--config", required=True, type=Path)
    finalize.add_argument("--authority", required=True, type=Path)
    finalize.add_argument("--candidate-parent", required=True)
    finalize.add_argument("--request", required=True, type=Path)
    admit = commands.add_parser("admit")
    admit.add_argument("--config", required=True, type=Path)
    admit.add_argument("--request", required=True, type=Path)
    options = parser.parse_args(argv)
    try:
        if options.command == "propose":
            result = write_proposal(options.config, options.output)
        elif options.command == "finalize":
            result = finalize_candidate(
                options.config,
                options.authority,
                options.candidate_parent,
                options.request,
            )
        else:
            result = admit_candidate(options.config, options.request)
    except (
        BundleExportError,
        DatasetIntegrityError,
        OSError,
        TrainingDriverError,
        TrainingValidationError,
        ValueError,
    ) as error:
        print(f"training driver rejected: {error}", file=sys.stderr)
        return 2
    print(_canonical(result).decode("ascii"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
