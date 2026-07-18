from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "check_generated_artifacts.py"
SPEC = importlib.util.spec_from_file_location("check_generated_artifacts", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("unable to load check_generated_artifacts.py")
check_artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(check_artifacts)


class GeneratedArtifactCheckTests(unittest.TestCase):
    def test_generated_directories_and_suffixes_are_rejected(self) -> None:
        self.assertTrue(check_artifacts.path_violations("target/debug/market-squawk"))
        self.assertTrue(check_artifacts.path_violations("src/__pycache__/module.pyc"))
        self.assertTrue(check_artifacts.path_violations("coverage/report.profraw"))

    def test_credential_shaped_files_and_os_metadata_are_rejected(self) -> None:
        self.assertTrue(check_artifacts.path_violations(".env"))
        self.assertTrue(check_artifacts.path_violations("config/.env.production"))
        self.assertTrue(check_artifacts.path_violations("credentials/client.pem"))
        self.assertTrue(check_artifacts.path_violations("docs/.DS_Store"))

    def test_explicit_environment_templates_and_source_files_are_allowed(self) -> None:
        self.assertEqual(check_artifacts.path_violations(".env.example"), [])
        self.assertEqual(check_artifacts.path_violations("config/.env.sample"), [])
        self.assertEqual(check_artifacts.path_violations("crates/domain/src/lib.rs"), [])

    def test_noncanonical_and_cross_platform_ambiguous_paths_are_rejected(self) -> None:
        self.assertTrue(check_artifacts.path_violations("./src/lib.rs"))
        self.assertTrue(check_artifacts.path_violations("src\\generated.rs"))

    def test_binary_and_oversized_content_fail_closed(self) -> None:
        self.assertTrue(check_artifacts.content_violations("fixture.bin", b"a\0b"))
        self.assertTrue(
            check_artifacts.content_violations(
                "large.txt",
                b"x" * (check_artifacts.MAX_REPOSITORY_FILE_BYTES + 1),
            )
        )

    def test_invalid_utf8_requires_an_explicit_binary_allowance(self) -> None:
        self.assertTrue(check_artifacts.content_violations("fixture.bin", b"\xff\xfe"))
        self.assertEqual(
            check_artifacts.content_violations(
                "fixtures/approved.bin",
                b"\xff\xfe",
                allowed_binary_files=frozenset({"fixtures/approved.bin"}),
            ),
            [],
        )

    def test_inspection_rejects_symlinks_and_accepts_bounded_text(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.rs"
            source.write_text("pub fn bounded() {}\n")
            link = root / "link.rs"
            link.symlink_to(source)

            self.assertEqual(check_artifacts.inspect_file(root, "source.rs"), [])
            self.assertTrue(check_artifacts.inspect_file(root, "link.rs"))

    def test_repository_inputs_exclude_tracked_deletions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            retained = root / "retained.txt"
            deleted = root / "deleted.txt"
            retained.write_text("retained\n")
            deleted.write_text("deleted\n")
            subprocess.run(
                ["git", "add", "--", retained.name, deleted.name],
                cwd=root,
                check=True,
            )
            deleted.unlink()

            self.assertEqual(check_artifacts.repository_inputs(root), [retained.name])


if __name__ == "__main__":
    unittest.main()
