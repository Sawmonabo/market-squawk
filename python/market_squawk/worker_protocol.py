"""Closed, bounded NDJSON output for the sealed model-training worker."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, BinaryIO, Mapping
from uuid import UUID


SCHEMA_VERSION = 1
MAX_EVENT_BYTES = 16 * 1024
MAX_STREAM_BYTES = 256 * 1024
MAX_EVENTS = 64
MAX_MESSAGE_BYTES = 192
MAX_REVISION_BYTES = 128
MAX_OBJECTIVE_UNITS = 1_000_000_000
HEX = re.compile(r"^[0-9a-f]{64}$")
DIAGNOSTIC_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
PHASES = frozenset(
    {"validation", "training", "evaluation", "export", "complete", "cancelled", "failed"}
)


def _ascii(value: str, maximum: int, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or not value.isascii()
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"{name} is invalid")
    return value


def _digest(value: str, name: str) -> str:
    if not isinstance(value, str) or HEX.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"{name} is invalid")
    return value


@dataclass(frozen=True)
class CandidateEvidence:
    """Untrusted candidate coordinates and hashes awaiting Rust-owned revalidation."""

    admission_request_sha256: str
    candidate_directory: str
    metadata_sha256: str
    artifact_sha256: str
    training_run_sha256: str
    authority_sha256: str
    dataset_export_sha256: str
    dataset_selection_sha256: str
    catalog_identity_sha256: str
    training_environment_sha256: str
    training_code_revision: str

    def __post_init__(self) -> None:
        _ascii(self.candidate_directory, 1024, "candidate directory")
        for name in (
            "admission_request_sha256",
            "metadata_sha256",
            "artifact_sha256",
            "training_run_sha256",
            "authority_sha256",
            "dataset_export_sha256",
            "dataset_selection_sha256",
            "catalog_identity_sha256",
            "training_environment_sha256",
        ):
            _digest(getattr(self, name), name)
        _ascii(self.training_code_revision, MAX_REVISION_BYTES, "training code revision")

    def as_mapping(self) -> Mapping[str, str]:
        """Return the closed wire representation; this intentionally has no admission field."""

        return {
            "admissionRequestSha256": self.admission_request_sha256,
            "candidateDirectory": self.candidate_directory,
            "metadataSha256": self.metadata_sha256,
            "artifactSha256": self.artifact_sha256,
            "trainingRunSha256": self.training_run_sha256,
            "authoritySha256": self.authority_sha256,
            "datasetExportSha256": self.dataset_export_sha256,
            "datasetSelectionSha256": self.dataset_selection_sha256,
            "catalogIdentitySha256": self.catalog_identity_sha256,
            "trainingEnvironmentSha256": self.training_environment_sha256,
            "trainingCodeRevision": self.training_code_revision,
        }


class WorkerProtocolWriter:
    """Writes one ordered, bounded protocol generation and seals on one terminal frame."""

    def __init__(self, stream: BinaryIO, *, run_id: str, generation: int) -> None:
        canonical_run_id = str(UUID(run_id))
        if canonical_run_id != run_id:
            raise ValueError("run identity is invalid")
        if type(generation) is not int or not 1 <= generation <= 2**64 - 1:
            raise ValueError("worker generation is invalid")
        self._stream = stream
        self._run_id = canonical_run_id
        self._generation = generation
        self._sequence = 0
        self._event_count = 0
        self._stream_bytes = 0
        self._terminal = False
        self._completed_units = 0
        self._total_units: int | None = None

    def progress(
        self,
        phase: str,
        message: str,
        completed_units: int,
        total_units: int,
    ) -> None:
        """Write one nonterminal progress frame."""

        self._write(
            kind="progress",
            phase=phase,
            message=message,
            completed_units=completed_units,
            total_units=total_units,
            diagnostic_code=None,
            result=None,
        )

    def result(
        self,
        phase: str,
        message: str,
        evidence: CandidateEvidence,
        *,
        completed_units: int,
        total_units: int,
    ) -> None:
        """Write the sole successful terminal candidate-evidence frame."""

        self._write(
            kind="result",
            phase=phase,
            message=message,
            completed_units=completed_units,
            total_units=total_units,
            diagnostic_code=None,
            result=evidence.as_mapping(),
        )

    def error(
        self,
        phase: str,
        message: str,
        diagnostic_code: str,
        completed_units: int,
        total_units: int,
    ) -> None:
        """Write the sole failed terminal frame without exception or input disclosure."""

        if not isinstance(diagnostic_code, str) or DIAGNOSTIC_CODE.fullmatch(diagnostic_code) is None:
            raise ValueError("diagnostic code is invalid")
        self._write(
            kind="error",
            phase=phase,
            message=message,
            completed_units=completed_units,
            total_units=total_units,
            diagnostic_code=diagnostic_code,
            result=None,
        )

    def _write(
        self,
        *,
        kind: str,
        phase: str,
        message: str,
        completed_units: int,
        total_units: int,
        diagnostic_code: str | None,
        result: Mapping[str, Any] | None,
    ) -> None:
        if self._terminal:
            raise ValueError("worker protocol is already terminal")
        if phase not in PHASES:
            raise ValueError("worker phase is invalid")
        _ascii(message, MAX_MESSAGE_BYTES, "worker message")
        if (
            type(completed_units) is not int
            or type(total_units) is not int
            or not 0 <= completed_units <= total_units <= MAX_OBJECTIVE_UNITS
            or completed_units < self._completed_units
            or (self._total_units is not None and total_units != self._total_units)
        ):
            raise ValueError("worker objective is invalid")
        event = {
            "schemaVersion": SCHEMA_VERSION,
            "runId": self._run_id,
            "generation": self._generation,
            "sequence": self._sequence,
            "kind": kind,
            "phase": phase,
            "message": message,
            "completedUnits": completed_units,
            "totalUnits": total_units,
            "unit": "steps",
            "diagnosticCode": diagnostic_code,
            "result": result,
        }
        encoded = json.dumps(
            event,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        framed_bytes = len(encoded) + 1
        if (
            not encoded
            or len(encoded) > MAX_EVENT_BYTES
            or self._event_count >= MAX_EVENTS
            or self._stream_bytes + framed_bytes > MAX_STREAM_BYTES
        ):
            raise ValueError("worker protocol limit exceeded")
        self._stream.write(encoded + b"\n")
        self._stream.flush()
        self._sequence += 1
        self._event_count += 1
        self._stream_bytes += framed_bytes
        self._completed_units = completed_units
        self._total_units = total_units
        self._terminal = kind in {"result", "error"}
