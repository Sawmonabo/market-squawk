# Q2 Build-Evidence Authority Boundary

Date: 2026-07-17
Status: threat-boundary record and limitation note; not benchmark or approval evidence
Rust baseline: 1.97.1

## Decision

Q2 does not implement hostile same-UID or byte-reproducible build-supply-chain attestation. That is a
separate, materially broader discipline than the product's requirement to measure performance
truthfully on a documented system. The final A4 contract instead binds a clean exact Git head,
direct Rust 1.97.1, locked dependencies, source/fixture/executable hashes, bounded host supervision,
an independent RSS observer, and paired standard-versus-ring runs under identical conditions. No
result produced under Rust 1.97.0 or the pre-change standard-channel run is eligible for approval.

Cargo uses `RUSTC` to select the compiler and passes the resolved value to build scripts. Authority
policy `sanitized-cargo-release-runner-v3` therefore rejects ambient `RUSTC`, resolves and
descriptor-hashes
the direct rustup-owned compiler, requires Rust 1.97.1 commit
`8bab26f4f68e0e26f0bb7960be334d5b520ea452`, and deliberately supplies that exact path to Cargo.
The benchmark package's build script requires the same path and digest, result schema 5 carries the
compiler digest through the runner and manifest, and preflight requires the measured host's direct
compiler digest to match. This binds the driver executable without expanding the non-hermetic threat
claim to its sysroot, linker, SDK, or hostile same-UID mutation. [Cargo environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html),
[rustup proxies](https://rust-lang.github.io/rustup/concepts/proxies.html),
[Rust 1.97.1](https://blog.rust-lang.org/2026/07/16/Rust-1.97.1/)

The authoritative fixed-quota runner is an explicit Cargo `[[bin]]` target named
`capture-admission-evidence`; the logical runner/evidence-copy name remains
`capture_admission_evidence`. It is built only with:

```bash
cargo build -p market-squawk-platform --bin capture-admission-evidence \
  --all-features --release --locked
```

This target change is an integrity correction, not a naming preference. Stable Cargo explicitly
ignores the profile `panic` setting for benchmark targets, even when a target disables the standard
benchmark harness with `harness = false`; the prior `[[bench]]` runner was therefore compiled with
`panic="unwind"` and could not truthfully claim the bound release profile's `panic="abort"`.
The release binary is bound to profile literal
`cargo-release-binary:opt-level=3:lto=thin:codegen-units=1:panic=abort:strip=symbols`, and the
target-side `cfg(panic = "abort")` assertion remains the final compile-time authority. Cargo also
forces the package build script itself to unwind, so `build.rs` validates its truthful
`CARGO_CFG_PANIC=unwind` host strategy and never treats that value as evidence of the binary target's
panic strategy. The separate
`capture_admission_criterion` benchmark remains adaptive engineering instrumentation with
`exploratory_zero_authority`; it cannot generate, schedule, or approve fixed-quota evidence.
[Cargo panic profile rules](https://doc.rust-lang.org/cargo/reference/profiles.html#panic),
[Rust `cfg(panic)`](https://doc.rust-lang.org/reference/conditional-compilation.html#panic),
[Rust panic strategy](https://doc.rust-lang.org/reference/panic.html#panic-strategy),
[Cargo artifact messages](https://doc.rust-lang.org/cargo/reference/external-tools.html#artifact-messages),
[`cargo build`](https://doc.rust-lang.org/cargo/commands/cargo-build.html)

The current preparer and host machinery is diagnostic. Its bounded I/O, process cleanup, schema,
and no-clobber checks are useful engineering controls, but they do not prove a hermetic compiler,
linker, SDK, dependency, dynamic-loader, or same-UID filesystem boundary. The clean unchanged
candidate's full verification and independent quarter review—not a self-signed baseline literal—are
the approval authority.

Within the diagnostic process topology, one containment owner is still required. A session-leader
wrapper creates the outer process group, records its numeric identity, and `execve`s Cargo without
changing PID/PGID. Cargo, build scripts, proc macros, compiler processes, linkers, and helper
commands must inherit that group. The Rust build helper rejects a missing, mismatched, or
group-creating policy before spawn. Local helper deadlines may cancel readers and kill/reap a
direct child, but only the outer supervisor owns whole-build extinction.

## Primary-source threat facts

- Cargo states that a package's `build.rs` is compiled and executed before the package, and that a
  build script “may perform any number of tasks.” Cargo recommends writing only under `OUT_DIR`, but
  this is a convention rather than an operating-system sandbox. Therefore arbitrary dependency
  build scripts are executable authority and must be contained or rejected, not treated as inert
  source. [Cargo build scripts](https://doc.rust-lang.org/cargo/reference/build-scripts.html)
- Cargo configuration can select a Rust compiler wrapper, define environment variables, choose a
  linker, add target-specific behavior, and otherwise alter a build. Authoritative operation must
  reject ambient configuration and wrappers and generate one exact private configuration.
  [Cargo configuration](https://doc.rust-lang.org/cargo/reference/config.html),
  [Cargo environment variables](https://doc.rust-lang.org/cargo/reference/environment-variables.html)
- `--frozen` combines locked resolution with offline behavior. It constrains dependency resolution
  and network access, but it does not sandbox build scripts, proc macros, compilers, linkers, or
  native helpers. The A4 contract therefore uses frozen/offline operation only as one layer of the
  input policy. [cargo build](https://doc.rust-lang.org/cargo/commands/cargo-build.html)
- Apple documents `killpg` as signaling the processes in one process group. An authoritative inner
  `process_group(0)` creates a nested group outside the outer Cargo PGID and defeats that boundary.
  The prior nested-group implementation and its inherited-only synthetic test were invalid as a
  proof of complete descendant extinction.
  [Apple `killpg(2)`](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/killpg.2.html)

## Broader reproducible-build boundary (not claimed by Q2)

A hostile same-UID/reproducible-build claim would additionally have to record and verify:

- exact commit and tree object IDs, repository object format, `.gitattributes`, every path, Git
  mode, blob object ID, byte length, and SHA-256 of the private source snapshot;
- explicit rejection of export substitutions, symlinks, gitlinks/submodules, unresolved LFS
  pointers, duplicate/archive-only entries, path escapes, noncanonical names, and untracked inputs;
- direct non-rustup-proxy Cargo and rustc bytes, Rust release/commit/host identity, the Rust 1.97.1
  distribution manifest/component inventory, sysroot and standard-library files, and provenance
  separately from the local copy identity;
- compiler driver, linker/`ld`, SDK identity and every native file admitted to the build closure,
  including dynamic-loader dependencies such as Mach-O dylibs where applicable;
- `Cargo.lock`, registry archive checksums, extracted `.cargo-checksum.json` file maps, all path and
  Git dependencies, proc macros, build scripts, and any native helper they can execute;
- a private generated Cargo home/config, an exact environment allowlist, an empty discovery path,
  no credentials, no ambient Cargo config, no wrappers, no injected rustflags, and no network;
- the session-leader wrapper and expected outer PGID policy, plus faithful extinction evidence for
  Cargo → real Rust build helper → inner child → descendant;
- a private target directory, a read-once built artifact, and one private executable created from
  those exact bytes for both binding inspection and final publication.

## Measurement practice

Criterion's maintainers document that host load and power state materially affect results and
recommend quiet comparable conditions, adequate warmup/measurement time, and cautious statistical
interpretation. They also warn that hosted virtual CI is noisy and its benchmark results should not
be relied upon. Q2 therefore measures on documented local hardware and never substitutes a blocked
or noisy hosted job for local evidence.
[Criterion output guidance](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_output.html),
[Criterion analysis](https://bheisler.github.io/criterion.rs/book/analysis.html),
[Criterion FAQ](https://bheisler.github.io/criterion.rs/book/faq.html)

Cargo documents `--frozen` as locked plus offline resolution. This helps hold dependency resolution
constant but is not an OS sandbox.
[Cargo lockfile command](https://doc.rust-lang.org/cargo/commands/cargo-generate-lockfile.html)

Reproducible Builds defines byte-for-byte reproducibility and its supply-chain plans as a separate
ecosystem-wide practice. Market Squawk's Q2 paired measurement must not imply that attestation.
[Reproducible Builds](https://reproducible-builds.org/),
[Reproducible Builds plans](https://reproducible-builds.org/docs/plans/)

## Approval boundary

An approval literal inside the artifact under review is not independent evidence. Final measurement
artifacts bind the exact candidate, inputs, paired repetitions, host fingerprint, executable, and
results. Approval additionally requires the unchanged clean-head full gate and grouped independent
quarter review with zero unresolved Critical, Important, or Minor findings. Q2 makes no detached
signature, pinned trust-root, or independent reproducible-build claim.
