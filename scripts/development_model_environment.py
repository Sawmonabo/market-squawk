#!/usr/bin/env python3
"""Publish bounded editable-environment metadata after the old service stops."""

import argparse
import hashlib
import importlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import tomllib

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def descriptor(args):
    source_root = args.source_root.resolve(strict=True)
    environment_root = args.development_training_root.resolve(strict=True)
    interpreter_relative_path = "Scripts/python.exe" if os.name == "nt" else "bin/python"
    if Path(sys.prefix).resolve() != environment_root or Path(sys.executable).absolute() != environment_root / interpreter_relative_path:
        raise ValueError("Run this script with the managed venv interpreter, retaining its bin/python or Scripts/python.exe path.")
    if platform.python_implementation() != "CPython" or platform.python_version() != "3.14.6":
        raise ValueError("The editable training environment requires managed CPython 3.14.6.")

    project = tomllib.loads((source_root / "python" / "pyproject.toml").read_text("utf-8"))["project"]
    distribution = metadata.distribution("market-squawk")
    direct_url = json.loads(distribution.read_text("direct_url.json") or "{}")
    if direct_url.get("dir_info", {}).get("editable") is not True:
        raise ValueError("Market Squawk must be installed editable; run just refresh-model-runtime.")
    native = importlib.import_module("market_squawk._native")
    training = importlib.import_module("market_squawk.training")
    if Path(training.__file__).resolve() != source_root / "python" / "market_squawk" / "training.py":
        raise ValueError("The editable package does not resolve to the selected checkout; run just refresh-model-runtime.")
    if distribution.version != project["version"] or native.__version__ != project["version"]:
        raise ValueError("The native extension and editable project versions differ; run just refresh-model-runtime.")
    if native.__market_squawk_build_identity__ != "development-unsealed-v1":
        raise ValueError("The editable environment requires an unsealed development native extension.")
    native_revision = native.__market_squawk_native_build_revision__
    if not isinstance(native_revision, str) or not re.fullmatch(r"[a-z0-9._-]{1,128}", native_revision):
        raise ValueError("The native extension has no valid development build revision; run just refresh-model-runtime.")

    lock = (source_root / "python" / "requirements.lock").read_bytes()
    versions = {}
    for line in lock.decode("utf-8").splitlines():
        # uv's exported requirements list has one top-level pinned requirement
        # followed by indented hashes/comments. Resolve markers with packaging.
        if not line or line[0].isspace() or line.startswith("#"):
            continue
        requirement = Requirement(line.rstrip(" \\"))
        if requirement.marker is not None and not requirement.marker.evaluate():
            continue
        name = canonicalize_name(requirement.name)
        version = metadata.version(name)
        if version not in requirement.specifier:
            raise ValueError(f"Locked dependency {name} differs from the managed environment; run just refresh-model-runtime.")
        versions[name] = version
    if not 1 <= len(versions) <= 32:
        raise ValueError("The locked runtime dependency metadata must contain 1 to 32 distributions.")

    revision = args.source_revision
    if revision is None:
        revision = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip()
    source_revision = f"development-{revision}-editable"
    if not re.fullmatch(r"[a-z0-9._-]{1,128}", source_revision):
        raise ValueError("Source revision must be bounded lowercase development metadata.")

    helpers = {}
    for name in ("onnx_worker", "validator"):
        helper = getattr(args, name).absolute()
        if helper.is_symlink() or not helper.is_file() or helper.stat().st_size == 0 or not os.access(helper, os.X_OK):
            raise ValueError(f"The staged {name} must be an executable regular file.")
        helpers[name] = str(helper)
    return {
        "schema_version": 1,
        "kind": "source-development",
        "editable": True,
        "source_root": str(source_root),
        "source_revision": source_revision,
        "native_build_revision": native_revision,
        "project_version": project["version"],
        "python_version": platform.python_version(),
        "python_tag": "cp314",
        "requirements_lock_sha256": hashlib.sha256(lock).hexdigest(),
        "runtime_distributions": dict(sorted(versions.items())),
        "interpreter_relative_path": interpreter_relative_path,
        **helpers,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--development-training-root", required=True, type=Path)
    parser.add_argument("--onnx-worker", required=True, type=Path)
    parser.add_argument("--validator", required=True, type=Path)
    parser.add_argument("--source-revision", help="Checkout revision metadata; defaults to Git HEAD and always remains marked editable.")
    args = parser.parse_args()
    payload = (json.dumps(descriptor(args), indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(payload) > 16 * 1024:
        raise ValueError("Editable training environment metadata exceeds 16 KiB.")
    environment_root = args.development_training_root.resolve(strict=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=environment_root, prefix=".source-development-", suffix=".json", delete=False) as output:
            temporary = Path(output.name)
            output.write(payload)
        os.replace(temporary, environment_root / "source-development.json")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, ImportError, AttributeError, subprocess.SubprocessError) as error:
        raise SystemExit(f"Editable training environment: {error}") from error
