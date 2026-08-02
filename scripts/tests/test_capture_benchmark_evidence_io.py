"""Focused tests for descriptor-capability benchmark evidence I/O."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "scripts"))
import capture_benchmark_evidence_io as evidence_io  # noqa: E402


class FixtureFailureInjectionScopeTest(unittest.TestCase):
    def test_scope_requires_the_exact_enum_and_explicit_fixture_mode(self) -> None:
        member = evidence_io.FailureInjection.FILE_FSYNC
        with self.assertRaises(evidence_io.GateError):
            with evidence_io._fixture_failure_injection(member, "production"):
                pass
        with self.assertRaises((evidence_io.GateError, TypeError)):
            with evidence_io._fixture_failure_injection(member.value, "fixture"):
                pass
        self.assertFalse(evidence_io._failure_injected(member))

class EvidenceIoBehaviorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root_path = Path(self.temporary.name).resolve()
        os.chmod(self.root_path, 0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_canonical_json_and_strict_object_decode_are_exact(self) -> None:
        encoded = evidence_io.canonical_json({"z": 1, "a": [True, None]})
        self.assertEqual(encoded, b'{"a":[true,null],"z":1}\n')
        self.assertEqual(evidence_io.read_json_bytes(encoded), {"a": [True, None], "z": 1})
        for invalid in (b'[]\n', b'{"a":1,"a":2}\n', b'{"a":'):
            with self.subTest(invalid=invalid):
                with self.assertRaises(evidence_io.GateError):
                    evidence_io.read_json_bytes(invalid)
        recursive: list[object] = []
        recursive.append(recursive)
        invalid_values = (
            {"value": float("nan")},
            {"value": float("inf")},
            {"value": float("-inf")},
            {"value": recursive},
            {"value": "\ud800"},
        )
        for invalid in invalid_values:
            with self.subTest(invalid=repr(invalid)):
                with self.assertRaises(evidence_io.GateError):
                    evidence_io.canonical_json(invalid)
        for encoded in (b'{"value":NaN}\n', b'{"value":Infinity}\n', b'{"value":-Infinity}\n'):
            with self.subTest(encoded=encoded):
                with self.assertRaises(evidence_io.GateError):
                    evidence_io.read_json_bytes(encoded)

    def test_capability_root_publishes_reads_and_refuses_clobber(self) -> None:
        root = evidence_io.CapabilityRoot.open(self.root_path)
        directory = root.open_directory(("evidence",), create_final=True)
        try:
            identity = evidence_io.publish_json(directory, "artifact.json", {"ok": True})
            artifact = self.root_path / "evidence" / "artifact.json"
            self.assertEqual(stat.S_IMODE(artifact.stat().st_mode), 0o600)
            self.assertEqual(root.read_file(artifact), b'{"ok":true}\n')
            self.assertEqual(identity, (artifact.stat().st_dev, artifact.stat().st_ino))
            with self.assertRaises(evidence_io.GateError):
                evidence_io.publish_json(directory, "artifact.json", {"ok": False})
            self.assertEqual(list((self.root_path / "evidence").glob(".tmp-*")), [])
        finally:
            os.close(directory)
            root.close()

    def test_capability_root_rejects_escape_symlink_and_unsafe_modes(self) -> None:
        root = evidence_io.CapabilityRoot.open(self.root_path)
        try:
            with self.assertRaises(evidence_io.GateError):
                root.relative(self.root_path.parent / "outside")
            private = self.root_path / "private"
            private.mkdir(mode=0o700)
            symlink = self.root_path / "linked"
            symlink.symlink_to(private, target_is_directory=True)
            with self.assertRaises((evidence_io.GateError, OSError)):
                root.open_directory(("linked",))
            unsafe = self.root_path / "unsafe"
            unsafe.mkdir(mode=0o755)
            with self.assertRaises(evidence_io.GateError):
                root.open_directory(("unsafe",))
        finally:
            root.close()

    def test_read_rejects_unsafe_or_changed_inputs(self) -> None:
        first = self.root_path / "first.json"
        second = self.root_path / "second.json"
        first.write_bytes(b'{"ok":true}\n')
        os.chmod(first, 0o600)
        os.link(first, second)
        root = evidence_io.CapabilityRoot.open(self.root_path)
        try:
            with self.assertRaises(evidence_io.GateError):
                root.read_file(first)
            second.unlink()
            os.chmod(first, 0o644)
            with self.assertRaises(evidence_io.GateError):
                root.read_file(first)
            os.chmod(first, 0o600)
            directory = self.root_path / "directory"
            directory.mkdir(mode=0o700)
            with self.assertRaises(evidence_io.GateError):
                root.read_file(directory)
            with evidence_io._fixture_failure_injection(
                evidence_io.FailureInjection.POST_READ_IDENTITY_MISMATCH, "fixture"
            ):
                with self.assertRaises(evidence_io.GateError):
                    root.read_file(first)
        finally:
            root.close()

    def test_exact_descriptor_read_honors_bounds_and_partial_read_fixture(self) -> None:
        value = b"0123456789" * 10
        path = self.root_path / "value"
        path.write_bytes(value)
        descriptor = os.open(path, os.O_RDONLY)
        try:
            with evidence_io._fixture_failure_injection(
                evidence_io.FailureInjection.PARTIAL_DESCRIPTOR_READ, "fixture"
            ):
                self.assertEqual(
                    evidence_io.read_exact_descriptor(descriptor, len(value), len(value)), value
                )
        finally:
            os.close(descriptor)
        descriptor = os.open(path, os.O_RDONLY)
        try:
            with self.assertRaises(evidence_io.GateError):
                evidence_io.read_exact_descriptor(descriptor, len(value), len(value) - 1)
        finally:
            os.close(descriptor)

    def test_publish_and_owner_write_faults_fail_closed_without_leakage(self) -> None:
        root = evidence_io.CapabilityRoot.open(self.root_path)
        directory = root.open_directory(("evidence",), create_final=True)
        try:
            with evidence_io._fixture_failure_injection(
                evidence_io.FailureInjection.FILE_FSYNC, "fixture"
            ):
                with self.assertRaises(evidence_io.GateError):
                    evidence_io.publish_json(directory, "artifact.json", {"ok": True})
            self.assertEqual(os.listdir(self.root_path / "evidence"), [])
            owner = os.open(
                "owner",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=directory,
            )
            try:
                with evidence_io._fixture_failure_injection(
                    evidence_io.FailureInjection.OWNER_WRITE_FAILURE, "fixture"
                ):
                    with self.assertRaises(evidence_io.GateError):
                        evidence_io.write_owner_all(owner, b"owner")
            finally:
                os.close(owner)
        finally:
            os.close(directory)
            root.close()

    def test_every_post_link_publication_failure_rolls_back_final_and_temporary_links(self) -> None:
        root = evidence_io.CapabilityRoot.open(self.root_path)
        directory = root.open_directory(("evidence",), create_final=True)
        try:
            for injection in (
                evidence_io.FailureInjection.DIRECTORY_FSYNC,
                evidence_io.FailureInjection.PUBLICATION_IDENTITY_MISMATCH,
            ):
                with self.subTest(injection=injection.value):
                    with evidence_io._fixture_failure_injection(injection, "fixture"):
                        with self.assertRaises(evidence_io.GateError):
                            evidence_io.publish_json(directory, "artifact.json", {"ok": True})
                    self.assertEqual(os.listdir(self.root_path / "evidence"), [])
        finally:
            os.close(directory)
            root.close()

    def test_production_root_is_capability_storage_below_the_git_common_parent(self) -> None:
        common = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=REPOSITORY,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        expected = Path(common).parent / "target" / "q2-a4-capture-benchmark"
        previous = Path.cwd()
        try:
            os.chdir(self.root_path)
            self.assertEqual(evidence_io.production_root(), expected)
        finally:
            os.chdir(previous)

    def test_production_root_does_not_execute_an_ambient_path_git(self) -> None:
        fake_bin = self.root_path / "fake-bin"
        fake_bin.mkdir(mode=0o700)
        marker = self.root_path / "ambient-git-ran"
        fake_git = fake_bin / "git"
        fake_git.write_text(
            f"#!/bin/sh\nprintf ran > '{marker}'\nexit 99\n",
            encoding="utf-8",
        )
        os.chmod(fake_git, 0o700)
        previous_path = os.environ.get("PATH")
        os.environ["PATH"] = str(fake_bin)
        try:
            root = evidence_io.production_root()
        finally:
            if previous_path is None:
                del os.environ["PATH"]
            else:
                os.environ["PATH"] = previous_path
        self.assertFalse(marker.exists())
        self.assertEqual(root.name, "q2-a4-capture-benchmark")

    def _linked_worktree_control(self) -> tuple[Path, Path, Path]:
        main = self.root_path / "main"
        common = main / ".git"
        git_directory = common / "worktrees" / "lane"
        worktree = self.root_path / "lane"
        git_directory.mkdir(parents=True)
        worktree.mkdir()
        control = worktree / ".git"
        control.write_text(f"gitdir: {git_directory}\n", encoding="utf-8")
        (git_directory / "commondir").write_text("../..\n", encoding="utf-8")
        return worktree, git_directory, control

    def test_production_root_parses_a_bounded_linked_worktree_control_graph(self) -> None:
        worktree, _git_directory, _control = self._linked_worktree_control()
        self.assertEqual(
            evidence_io._production_root_from_repository(worktree),
            self.root_path / "main" / "target" / "q2-a4-capture-benchmark",
        )

    def test_production_root_rejects_a_malformed_git_control_file(self) -> None:
        repository = self.root_path / "malformed"
        repository.mkdir()
        (repository / ".git").write_text("unknown: authority\n", encoding="utf-8")
        with self.assertRaises(evidence_io.GateError):
            evidence_io._production_root_from_repository(repository)

    def test_production_root_rejects_a_commondir_escape(self) -> None:
        worktree, git_directory, _control = self._linked_worktree_control()
        escaped = self.root_path / "escaped" / ".git"
        escaped.mkdir(parents=True)
        (git_directory / "commondir").write_text(str(escaped) + "\n", encoding="utf-8")
        with self.assertRaises(evidence_io.GateError):
            evidence_io._production_root_from_repository(worktree)

    def test_production_root_rejects_a_symlinked_commondir_control_file(self) -> None:
        worktree, git_directory, _control = self._linked_worktree_control()
        commondir = git_directory / "commondir"
        target = git_directory / "commondir-target"
        commondir.rename(target)
        commondir.symlink_to(target)
        with self.assertRaises(evidence_io.GateError):
            evidence_io._production_root_from_repository(worktree)

    def test_production_root_rejects_a_git_control_read_race(self) -> None:
        worktree, _git_directory, control = self._linked_worktree_control()
        real_stat = os.stat

        def raced_stat(path: object, *args: object, **kwargs: object) -> os.stat_result:
            metadata = real_stat(path, *args, **kwargs)
            if Path(os.fsdecode(path)) == control:
                changed = list(metadata)
                changed[1] += 1
                return os.stat_result(changed)
            return metadata

        with mock.patch.object(evidence_io.os, "stat", side_effect=raced_stat):
            with self.assertRaises(evidence_io.GateError):
                evidence_io._production_root_from_repository(worktree)


if __name__ == "__main__":
    unittest.main()
