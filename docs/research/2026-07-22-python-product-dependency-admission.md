# Python product dependency admission — 2026-07-22

Audit base: `9aa06ac577a4dd1521378842c8d9f3d8f9cda329` (implementation anchor, not
release approval). This record admits only the local research/training product. Python and PyO3
remain absent from live and execution dependencies.

## Runtime and platform decision

- Supported interpreter contract: GIL-enabled CPython 3.10 through 3.14. Apache Arrow 25 lists
  Python 3.10–3.14 support, and every selected Python dependency has a compatible floor. The
  extension targets `cp310-abi3`; PyO3 documents that an `abi3-py310` extension is forward
  compatible with later GIL-enabled CPython versions. Free-threaded `abi3t` is not claimed.
- Reproducible offline binary coverage in version 0.1 is macOS 12 or newer on arm64, with exact
  PyArrow wheels locked separately for CPython 3.10, 3.11, 3.12, 3.13, and 3.14. This machine's
  executable gate uses CPython 3.13.7 on macOS 26.5.1 arm64. Other systems may build from source,
  but no unmeasured binary-platform claim is made.
- The build uses isolated `venv` environments and bundled `ensurepip`; the offline install uses
  pip `--no-index`, `--only-binary :all:`, and `--require-hashes`. The lock verifies filename,
  compatible wheel tags, byte size, SHA-256, and wheel core-metadata license before installation.

Primary sources: [Python release versions](https://www.python.org/doc/versions/),
[Python `venv`](https://docs.python.org/3/library/venv.html),
[offline `ensurepip`](https://docs.python.org/3.13/library/ensurepip.html),
[PyO3 ABI features](https://pyo3.rs/main/features),
[PyO3 build/distribution guidance](https://pyo3.rs/main/building-and-distribution),
[maturin mixed-project layout](https://www.maturin.rs/project_layout.html),
[Arrow 25 installation and supported Python versions](https://arrow.apache.org/docs/python/install.html),
[wheel compatibility tags](https://packaging.python.org/en/latest/specifications/platform-compatibility-tags/),
[pip secure installs](https://pip.pypa.io/en/stable/topics/secure-installs/),
[Cargo fetch](https://doc.rust-lang.org/cargo/commands/cargo-fetch.html), and
[Cargo environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html).

## Exact admissions

| Component | Version | License | Role |
| --- | --- | --- | --- |
| PyO3 crate | 0.29.0 | MIT OR Apache-2.0 | `abi3-py310` analytical extension |
| maturin | 1.14.1 | MIT OR Apache-2.0 | locked local wheel build backend |
| PyArrow | 25.0.0 | Apache-2.0 | Arrow/Parquet and exact Decimal128/timestamp interchange |
| pytest | 9.1.1 | MIT | four-file product gate |
| packaging | 26.2 | Apache-2.0 OR BSD-2-Clause | pytest dependency and tag reference |
| pluggy | 1.6.0 | MIT | pytest dependency |
| iniconfig | 2.3.0 | MIT | pytest dependency |
| Pygments | 2.20.0 | BSD-2-Clause | pytest dependency |

Exact artifact filenames, SHA-256 values, sizes, official `files.pythonhosted.org` URLs, and the
per-interpreter PyArrow selection are machine-readable in `python/wheelhouse-lock.json`; exact pip
requirement hashes are in `python/requirements.lock`. Package versions and license expressions were
checked against the official [PyPI JSON API](https://docs.pypi.org/api/json/) and the selected wheel
`METADATA`. PyO3 0.29.0 and its Rust 1.83 floor were checked through the official
[crates.io release record](https://crates.io/crates/pyo3/0.29.0), which is compatible with the
workspace's Rust 1.97.1 pin.

The builder permits network access only in explicitly authorized cache-preparation mode. That mode
runs `cargo fetch --locked` into an explicitly supplied ignored `CARGO_HOME`; Cargo documents that
an unfiltered fetch downloads all target dependencies so later Cargo commands can run offline. The
release gate reuses only that cache with `CARGO_NET_OFFLINE=true`, consumes the committed locks and
populated ignored wheelhouse, and records exact interpreter, pip, compiler, Cargo, lock,
dependency-wheel, project-wheel, and Rust-validator hashes.
