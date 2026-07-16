# Stage 1 Security Tooling Refresh

**Checked:** 2026-07-16
**Purpose:** Pin concrete local/CI tool versions used by the Stage 1 foundation implementation plan.

## Results

| Tool | Pinned version | Official release source |
|---|---:|---|
| Cargo Deny | `0.20.2` | [Embark Studios cargo-deny release](https://github.com/EmbarkStudios/cargo-deny/releases/tag/0.20.2) |
| Cargo Audit | `0.22.2` | [RustSec cargo-audit release](https://github.com/rustsec/rustsec/releases/tag/cargo-audit/v0.22.2) |
| Cargo Machete | `0.9.2` | [cargo-machete release](https://github.com/bnjbvr/cargo-machete/releases/tag/v0.9.2) |
| Gitleaks | `8.30.1` | [Gitleaks release](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1) |

The version checks used the projects' public GitHub release APIs. Gitleaks `8.30.1` is also listed
as the latest tagged official container release with digest
`sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f`; Market Squawk does not
require the container and should use the native, checksum-verified release asset where it needs to
provision the tool.

## Implementation constraints

- Pin the exact versions in `scripts/tool-versions.env`.
- Use `cargo install --locked --version <version>` for the Cargo tools.
- Download Gitleaks only from its official release and verify the release checksum before execution.
- Local verification must not silently install or execute downloaded software; it should report the
  missing tool and the exact installation command.
- Re-check tool MSRV and the full locked workspace on Rust 1.97.0 during implementation. A release
  tag alone does not prove compatibility with the project's target toolchain.
- Keep advisory, license, dependency-use, credential, generated-artifact, and application tests as
  separate gates. No single tool proves all of them.

## Scope and limitations

This refresh records release selection, not a claim that these tools find every vulnerability,
license issue, unused dependency, secret, or generated artifact. Any policy exception requires a
bounded rationale, owner, and review/expiry date in the repository configuration.
