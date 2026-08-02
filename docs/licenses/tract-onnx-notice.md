# Required tract ONNX dependency notice

Market Squawk's required local ONNX backend uses `tract-onnx` 0.23.4. That dependency graph
includes the following file-level copyleft component:

| Inventory field | Value |
| --- | --- |
| Component | `dyn-eq` |
| Version | 0.1.3 |
| License | Mozilla Public License 2.0 (`MPL-2.0`) |
| Relationship | Unmodified transitive Rust dependency compiled into the required tract backend |
| Cargo package SHA-256 | `5c2d035d21af5cde1a6f5c7b444a5bf963520a9f142e5d06931178433d7d5388` |
| Upstream revision | [`674274e2aded185c0b4f54cc9745c844bce5fca9`](https://github.com/Rayzeq/dyn-eq/commit/674274e2aded185c0b4f54cc9745c844bce5fca9) |
| Source | [GitHub source at the upstream revision](https://github.com/Rayzeq/dyn-eq/tree/674274e2aded185c0b4f54cc9745c844bce5fca9) |
| Packaged source | [crates.io package source](https://crates.io/api/v1/crates/dyn-eq/0.1.3/download) |
| License text | [Upstream MPL-2.0 text](https://github.com/Rayzeq/dyn-eq/blob/674274e2aded185c0b4f54cc9745c844bce5fca9/LICENSE) |

The MPL 2.0 permits this component to be combined with differently licensed files in a larger work.
For executable distribution, recipients must be told how to obtain the MPL-covered source under
the MPL; Market Squawk's other files do not become MPL-covered merely by being compiled with it.
These boundaries are described in [MPL 2.0 sections 3.1–3.3](https://www.mozilla.org/en-US/MPL/2.0/)
and the [Mozilla MPL 2.0 FAQ](https://www.mozilla.org/en-US/MPL/2.0/FAQ/).

Market Squawk does not modify `dyn-eq`. A release containing the tract backend must include this
notice in its third-party notices/SBOM and retain the source and license links above. If the project
ever patches or vendors an MPL-covered file, the corresponding modified source must also be made
available under MPL-2.0. The `cargo-deny` exception is intentionally scoped to `dyn-eq@0.1.3`; a
different package or version requires a new review.
