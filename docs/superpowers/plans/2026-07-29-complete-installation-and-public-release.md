# Complete Installation and Public Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a complete Market Squawk installation through one stable `curl | sh` command and
clickable native GitHub Release packages, with the desktop, CLI, helpers, uv, managed CPython 3.14,
and the locked Python product installed and verified together.

**Architecture:** Keep release construction, installation, and product setup as separate
authorities. cargo-dist and native Tauri builders publish immutable platform artifacts; a small
Rust installer validates and atomically activates complete versioned bundles; the Obsidian Signal
desktop and existing CLI remain the only guided product-setup surfaces. The POSIX script is only a
platform bootstrap and never becomes the lifecycle or domain authority.

**Tech Stack:** Rust 1.97.1, cargo-dist 0.32.0, Tauri 2, GitHub Releases and artifact attestations,
uv 0.12.0, standard CPython 3.14.6, PyArrow 25.0.0, ZIP, SHA-256, Serde, Reqwest, and the existing
sealed Python release builder.

## Global Constraints

- Audit base: `e6f77d564b00a6e6911c30be60d441f0576e9e08`, tree
  `4c27aa5c06ab43e93b7a5cc270e8ccfaae26b9e4`.
- The audit base is a planning anchor, not Quarter 4 approval. Before the first implementation
  commit, refresh the diff and release interfaces against the accepted desktop head.
- Execute inline in the existing `feature/complete-installer` worktree. Do not create another
  worktree, task branch, or subagent review loop.
- Supported release targets are `x86_64-unknown-linux-gnu`, `x86_64-pc-windows-msvc`,
  `x86_64-apple-darwin`, and `aarch64-apple-darwin`.
- The V1 operating-system floors are Ubuntu 24.04-compatible x64 Linux, Windows 10 version 1809 or
  newer on x64, and macOS 12 or newer on Intel and Apple Silicon.
- The complete default always includes the Obsidian Signal desktop, `market-squawk`,
  `market-squawk-capture-helper`, `market-squawk-onnx-worker`,
  `market-squawk-model-validator`, `market-squawk-train`, `market-squawk-installer`, uv 0.12.0,
  standard CPython 3.14.6, and the exact locked Python environment.
- No installed path may require Rust, Node.js, pnpm, Python, uv, a container runtime, a database
  service, a cloud service, or a paid subscription to be preinstalled.
- The public stable release must be immutable, carry GitHub release attestations, and pass native
  signature verification where the platform supplies that trust surface. Unsigned pull-request
  packages are test artifacts and cannot be published as the stable release.
- Retain the active immutable version and one previous known-good version. Activation changes one
  atomic selector; it never modifies an active version in place.
- Reject bundles above 2 GiB compressed, 4 GiB expanded, 32,768 entries, 1 GiB per entry, or 1 MiB
  for the release manifest. Reject duplicate, absolute, parent-traversal, symlink, device, and
  unlisted paths before activation.
- Ordinary uninstall removes programs and launchers but preserves configuration, credentials,
  catalogs, portfolios, datasets, models, logs, and artifacts. Each mutable-data class requires a
  separate explicit deletion choice.
- Keep tests thin: add one installer unit-test module and extend the existing Python builder and
  application release test roots. Do not add a frontend test, snapshot, prose test, wrapper test,
  or standalone Rust integration-test executable.
- Keep every worktree-local Cargo target below 20 GiB, set `CARGO_INCREMENTAL=0` for agent and
  release gates, and do not share a target directory across worktrees.
- Run focused gates after each task. Run the broad unchanged-head verification and grouped Quarter
  4 review only after all installed-product lanes are assembled.

---

## File and Authority Map

| Path | Responsibility |
| --- | --- |
| `apps/market-squawk-installer/` | Manifest admission, download, staging, verification, activation, repair, rollback, update, uninstall, and stable launcher |
| `distribution/install.sh` | POSIX platform detection, exact bootstrap download, SHA-256 verification, and handoff to the Rust installer |
| `distribution/release-components.json` | Exact uv, CPython, package, platform, size, and source identities admitted into release construction |
| `dist-workspace.toml` | Pinned cargo-dist release targets and GitHub release orchestration |
| `scripts/build_python_release.py` | Existing sealed Python authority, generalized to the frozen CPython 3.14 platform matrix |
| `scripts/build_complete_release.py` | Deterministic assembly of one complete platform bundle and its closed inventory |
| `apps/market-squawk-desktop/scripts/stage-sidecars.mjs` | Staging of the already-built complete bundle into native Tauri packages |
| `.github/workflows/release.yml` | Draft-build-verify-attest-publish release transaction |
| `README.md` | User-first one-command and click-to-download installation |
| `docs/operations/installation-and-bootstrap.md` | Installation, update, repair, rollback, uninstall, and recovery procedures |

---

### Task 1: Freeze the accepted release contract and refresh the implementation base

**Files:**

- Modify: `docs/research/2026-07-28-cross-platform-installation-and-guided-setup.md`
- Modify: `docs/audits/2026-07-28-cross-platform-installation-evidence-audit.md`
- Create: `docs/architecture/decisions/0006-complete-versioned-release-bundles.md`
- Modify: `docs/architecture/decisions/README.md`
- Modify: `docs/project-memory.md`

**Interfaces:**

- Consumes: desktop candidate `e6f77d564b00a6e6911c30be60d441f0576e9e08` and the approved
  installer research.
- Produces: one code-owned support matrix and version policy used by every later task.

- [ ] **Step 1: Refresh the planning anchor**

  Compare the installer worktree to the accepted desktop branch and record the exact resulting
  base commit and tree. Re-run the dependency and platform checks for cargo-dist 0.32.0, uv 0.12.0,
  CPython 3.14.6, PyArrow 25.0.0, Tauri packaging, and GitHub immutable releases.

- [ ] **Step 2: Convert the research decisions from pending to accepted**

  Record the four target triples, operating-system floors, complete component inventory, first-class
  native and curl channels, one-version rollback retention, size limits, data-preserving uninstall,
  and stable-publication signing rule. Preserve direct official source links and the 2026-07-29
  review date.

- [ ] **Step 3: Record the architecture decision**

  The ADR must state:

  ```text
  release construction -> immutable complete bundle
  Rust installer        -> verified lifecycle and version selector
  Obsidian Signal/CLI    -> product setup and readiness
  ```

  It must reject a full shell installer, system-Python mutation, partial recommended installs,
  in-place upgrades, and README commands that point to nonexistent assets.

- [ ] **Step 4: Commit the independently useful decision artifacts**

  ```bash
  git add docs/research docs/audits docs/architecture/decisions docs/project-memory.md \
    docs/superpowers/plans/2026-07-29-complete-installation-and-public-release.md
  git commit -m "docs(release): freeze complete installation contract"
  ```

---

### Task 2: Implement the bounded Rust installation lifecycle

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `apps/market-squawk-installer/Cargo.toml`
- Create: `apps/market-squawk-installer/src/lib.rs`
- Create: `apps/market-squawk-installer/src/main.rs`
- Create: `apps/market-squawk-installer/src/command.rs`
- Create: `apps/market-squawk-installer/src/manifest.rs`
- Create: `apps/market-squawk-installer/src/platform.rs`
- Create: `apps/market-squawk-installer/src/archive.rs`
- Create: `apps/market-squawk-installer/src/store.rs`
- Create: `apps/market-squawk-installer/src/lifecycle.rs`

**Interfaces:**

- Consumes: HTTPS release URL or an operator-selected offline bundle, one controlled per-user
  installation root, and the exact platform identity.
- Produces:

  ```rust
  pub fn install(request: InstallRequest) -> Result<InstallReceipt, InstallError>;
  pub fn repair(request: RepairRequest) -> Result<InstallReceipt, InstallError>;
  pub fn update(request: UpdateRequest) -> Result<InstallReceipt, InstallError>;
  pub fn rollback(request: RollbackRequest) -> Result<InstallReceipt, InstallError>;
  pub fn uninstall(request: UninstallRequest) -> Result<UninstallReceipt, InstallError>;
  pub fn status(root: &Path) -> Result<InstallStatus, InstallError>;
  ```

- [ ] **Step 1: Write the minimal failing installer tests in the library**

  One `#[cfg(test)]` module must prove only these release-critical behaviors:

  1. an archive with a parent traversal, extra entry, or digest mismatch is rejected before any
     active selector exists;
  2. successful activation switches from the old complete version to the new complete version and
     rollback restores the old version;
  3. default uninstall removes program state while preserving a separately rooted data fixture.

  Run:

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-installer --lib --locked
  ```

  Expected: compile failure because the installer API does not yet exist.

- [ ] **Step 2: Define the closed release manifest**

  Use `serde(deny_unknown_fields)` on every wire type. The top-level wire shape is:

  ```rust
  pub struct ReleaseManifest {
      pub schema_version: u32,
      pub product: Box<str>,
      pub version: Box<str>,
      pub tag: Box<str>,
      pub repository: Box<str>,
      pub commit_sha: Box<str>,
      pub tree_sha: Box<str>,
      pub generated_at: Box<str>,
      pub targets: Vec<TargetRelease>,
  }

  pub struct TargetRelease {
      pub target: SupportedTarget,
      pub minimum_system: Box<str>,
      pub archive: ArtifactIdentity,
      pub components: Vec<ComponentIdentity>,
  }
  ```

  Require sorted unique targets and component paths, an exact `Sawmonabo/market-squawk`
  repository identity, the selected tag/version pair, lowercase SHA-256 values, exact byte sizes,
  the complete code-owned role set, and no unknown role.

- [ ] **Step 3: Add safe streaming download and archive admission**

  Download to a newly created `0700` staging directory, enforce the declared response and streamed
  byte ceilings, hash while writing, and reject redirects outside HTTPS. Read ZIP central-directory
  metadata before extraction; reject duplicate, encrypted, unsupported-compression, unsafe-type,
  unsafe-mode, unsafe-path, excessive-count, and excessive-expanded-size entries. Extract only
  listed regular files with no-follow creation.

- [ ] **Step 4: Add immutable store and activation state**

  Install into a platform-native per-user `versions/<version>-<manifest-sha256>/` directory.
  Write `installation.json` with schema version, active identity, previous known-good identity,
  manifest identity, install time, and component receipts. Commit it with write-rename-sync
  semantics under an exclusive installation lock. Never overwrite an existing immutable version.

- [ ] **Step 5: Add repair, update, rollback, uninstall, and status**

  Repair re-verifies every active component and reconstructs only from the same immutable release.
  Update stages and verifies a complete newer release before selector change. Rollback requires the
  retained previous version to re-pass manifest and component admission. Default uninstall removes
  versions, selector, launchers, and maintenance registration only. Mutable-data deletion flags are
  independent and require `--confirm-delete-<class>`.

- [ ] **Step 6: Add stable command and GUI entrypoints**

  `market-squawk-installer` exposes:

  ```text
  install --manifest-url RELEASE_MANIFEST_URL
  install --manifest RELEASE_MANIFEST_PATH --bundle COMPLETE_BUNDLE_PATH
  update
  repair
  rollback
  uninstall
  status
  launch --program CLOSED_PROGRAM_NAME
  ```

  The launcher accepts only code-owned program names and never a path or arbitrary argument vector.

- [ ] **Step 7: Make the installer tests pass and run focused lint**

  ```bash
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-installer --lib --locked
  CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-installer --all-targets --locked -- -D warnings
  cargo fmt --all --check
  ```

- [ ] **Step 8: Commit**

  ```bash
  git add Cargo.toml Cargo.lock apps/market-squawk-installer
  git commit -m "feat(installer): add verified versioned installation lifecycle"
  ```

---

### Task 3: Ship one sealed CPython 3.14 product on every supported target

**Files:**

- Modify: `python/pyproject.toml`
- Modify: `python/requirements.lock`
- Replace: `python/wheelhouse-lock.json`
- Create: `python/wheelhouse/aarch64-apple-darwin.json`
- Create: `python/wheelhouse/x86_64-apple-darwin.json`
- Create: `python/wheelhouse/x86_64-unknown-linux-gnu.json`
- Create: `python/wheelhouse/x86_64-pc-windows-msvc.json`
- Create: `distribution/release-components.json`
- Modify: `scripts/build_python_release.py`
- Modify: `scripts/tests/test_build_python_release.py`
- Modify: `crates/market-squawk-modeling/src/training_environment.rs`
- Modify: `apps/market-squawk/src/release/close.rs`
- Modify: `apps/market-squawk/src/release/demonstrate/local.rs`

**Interfaces:**

- Consumes: exact host target, locked uv 0.12.0 archive, locked uv-managed standard CPython
  3.14.6 archive, platform PyArrow wheel, universal Python dependencies, Rust binaries, and source
  closure.
- Produces: one `release-cp314` root whose manifest, environment receipt, interpreter, wheel
  RECORDs, native extension, training driver, application, validator, and ONNX worker all pass the
  existing independent Rust verifier.

- [ ] **Step 1: Update the existing builder tests to the new single-version matrix**

  Replace cp312/cp313 expectations with cp314 and add one table-driven platform assertion covering
  the four exact target descriptors. Keep all assertions in
  `scripts/tests/test_build_python_release.py`; do not add a test file.

  ```bash
  python3 -m unittest scripts.tests.test_build_python_release
  ```

  Expected: failure on the old two-version, macOS-arm64-only implementation.

- [ ] **Step 2: Freeze release component identities**

  `distribution/release-components.json` must bind:

  - uv 0.12.0 platform archive URL, size, and SHA-256 for all four targets;
  - the uv 0.12.0 managed CPython 3.14.6 standard-GIL platform archive URL, size, and SHA-256;
  - PyArrow 25.0.0 cp314 wheel URL, size, and SHA-256 for all four targets;
  - universal locked package artifacts;
  - minimum OS and platform tag;
  - source license and upstream release URL.

  All release downloads must come from this lock. The builder may not resolve “latest.”

- [ ] **Step 3: Generalize the sealed builder**

  Replace global macOS assumptions with one admitted `PlatformProfile` containing target triple,
  executable suffix, venv binary directory, interpreter relative path, wheel platform tag,
  deployment floor, and native toolchain evidence. Require exactly CPython 3.14.6 and build only
  `release-cp314`.

  Preserve bounded source admission, locked wheel download, exact hash checks, no-network venv
  sync, RECORD verification, native-extension hardening, release signing, and controlled-root
  rules. Platform-specific build commands must use the host target rather than cross-linking a
  native release on another OS.

- [ ] **Step 4: Generalize the Rust training verifier**

  Accept only Python `3.14.<patch>` with tag `cp314`. Admit the exact platform tag compiled into the
  release foundation, use `bin/python` on Unix and `Scripts/python.exe` on Windows, and replace the
  macOS-only project-wheel schema with a platform-neutral schema version. Preserve closed fields,
  two-pass file identity, permission checks, RECORD verification, and no-action behavior on any
  mismatch.

- [ ] **Step 5: Update terminal release evidence**

  The closer and demonstration must require exactly one nonempty `release-cp314` entry, bind it to
  the selected application and worker, and reject historical cp312/cp313 matrices for the new
  release schema.

- [ ] **Step 6: Run focused tests**

  ```bash
  python3 -m unittest scripts.tests.test_build_python_release
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-modeling --lib --locked
  CARGO_INCREMENTAL=0 cargo test -p market-squawk --test release_demonstration --locked
  CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-modeling -p market-squawk \
    --all-targets --all-features --locked -- -D warnings
  cargo fmt --all --check
  ```

- [ ] **Step 7: Commit**

  ```bash
  git add python distribution/release-components.json scripts/build_python_release.py \
    scripts/tests/test_build_python_release.py crates/market-squawk-modeling \
    apps/market-squawk/src/release
  git commit -m "feat(python): seal CPython 3.14 across release targets"
  ```

---

### Task 4: Assemble the complete offline-capable release bundle

**Files:**

- Create: `scripts/build_complete_release.py`
- Create: `distribution/install.sh`
- Create: `distribution/README.md`
- Modify: `.gitignore`
- Modify: `apps/market-squawk-installer/src/command.rs`
- Modify: `apps/market-squawk-installer/src/manifest.rs`
- Modify: `dist-workspace.toml`

**Interfaces:**

- Consumes: native Tauri bundle, Rust release binaries, `release-cp314`, uv, licenses, notices, and
  exact repository/release identities.
- Produces:

  ```text
  market-squawk-<version>-<target>.zip
  market-squawk-release.json
  market-squawk-bootstrap-<target>[.exe]
  install.sh
  SHA256SUMS
  ```

- [ ] **Step 1: Configure cargo-dist 0.32.0**

  Add `dist-workspace.toml` with exact cargo-dist version, the four target triples, GitHub CI,
  SHA-256 checksums, source tarball, and repository-owned extra artifacts. Validate with the pinned
  cargo-dist binary:

  ```bash
  cargo dist manifest --tag v0.2.0
  cargo dist plan --tag v0.2.0
  ```

- [ ] **Step 2: Build a deterministic closed bundle**

  `build_complete_release.py` accepts explicit `--target`, `--version`, `--commit`, `--tree`,
  `--python-release`, `--native-bundle`, and `--output`. It admits every input as a stable regular
  file, copies only the code-owned inventory, normalizes archive paths and timestamps, emits sorted
  entries with explicit modes, and asks `market-squawk-installer manifest build` to produce the
  authoritative component identities. It fails if an expected component is absent or an extra
  component enters the staging root.

- [ ] **Step 3: Implement the thin POSIX bootstrap**

  `distribution/install.sh` supports only Linux x64 and macOS Intel/Apple Silicon. It uses
  `uname`, `mktemp`, `umask 077`, HTTPS-only curl, fixed redirect and retry bounds, an exact
  release-relative bootstrap filename, the release-generated SHA-256 value, signal cleanup, and:

  ```sh
  "$bootstrap" install \
    --manifest-url \
    "https://github.com/Sawmonabo/market-squawk/releases/download/$tag/market-squawk-release.json"
  ```

  It contains no provider, Python-environment, PATH-editing, update, rollback, or uninstall logic.

- [ ] **Step 4: Exercise one local offline bundle**

  Build the host bundle, install it under an isolated temporary home using
  `--manifest ... --bundle ...`, run installer status, run the installed CLI `--version`, run
  `doctor`, verify the Python environment, launch and stop stdio MCP, repair, install a second
  fixture version, rollback, and uninstall while proving the data fixture remains.

- [ ] **Step 5: Commit**

  ```bash
  git add dist-workspace.toml distribution scripts/build_complete_release.py \
    apps/market-squawk-installer/src .gitignore
  git commit -m "feat(release): assemble complete verified platform bundles"
  ```

---

### Task 5: Make native desktop packages install the same complete product

**Files:**

- Modify: `apps/market-squawk-desktop/package.json`
- Modify: `apps/market-squawk-desktop/scripts/stage-sidecars.mjs`
- Modify: `apps/market-squawk-desktop/src-tauri/Cargo.toml`
- Modify: `apps/market-squawk-desktop/src-tauri/tauri.bundle.conf.json`
- Modify: `apps/market-squawk-desktop/src-tauri/src/lib.rs`
- Modify: `apps/market-squawk-desktop/src-tauri/src/bridge.rs`
- Modify: `apps/market-squawk-desktop/src/lib/schemas.ts`
- Modify: `apps/market-squawk-desktop/src/components/setup/setup-overview.tsx`

**Interfaces:**

- Consumes: the target's verified complete bundle and installer library.
- Produces: DMG/app, NSIS/MSI, AppImage/DEB packages that carry the exact complete product and
  activate it under the current user before presenting setup as ready.

- [ ] **Step 1: Stage the complete bundle as a native package resource**

  Extend the existing bounded staging program rather than creating a second Node script. It must
  require the complete bundle, public manifest, manifest digest, and installer binary from the
  current target release output, verify them, and stage them under one generated Tauri resource
  directory. Tauri still stages the CLI, capture helper, and ONNX worker as sidecars for startup
  compatibility.

- [ ] **Step 2: Reuse the Rust installer library during desktop startup**

  Before `LocalProduct` composition, detect the exact packaged release resource, call the installer
  library to install or admit the active identical release, and supply its `release-cp314` root to
  the normal configuration layer. If installation, repair, or rollback is required, show an honest
  recoverable setup state; never continue with a partial product or hard-coded readiness.

- [ ] **Step 3: Connect the existing Updates and recovery routes**

  Expose only typed `status`, `update`, `repair`, `rollback`, and default data-preserving uninstall
  operations through the existing bounded Tauri bridge. Keep arbitrary URL, shell, path, and
  deletion authority unavailable to the frontend.

- [ ] **Step 4: Verify without adding frontend tests**

  ```bash
  pnpm --dir apps/market-squawk-desktop typecheck
  pnpm --dir apps/market-squawk-desktop test --run
  pnpm --dir apps/market-squawk-desktop build
  CARGO_INCREMENTAL=0 cargo test -p market-squawk-installer --lib --locked
  CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-desktop -p market-squawk-installer \
    --all-targets --locked -- -D warnings
  ```

  The frontend must remain exactly one test file with three tests.

- [ ] **Step 5: Commit**

  ```bash
  git add apps/market-squawk-desktop
  git commit -m "feat(desktop): embed the complete verified release"
  ```

---

### Task 6: Publish immutable native installers and the stable curl endpoint

**Files:**

- Create: `.github/workflows/release.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/tests/test_ci_workflow_policy.py`
- Modify: `dist-workspace.toml`
- Modify: `docs/verification/usable-release-gate.md`

**Interfaces:**

- Consumes: one clean annotated `v0.2.0` tag at the approved exact commit plus protected platform
  signing credentials.
- Produces: one immutable GitHub Release whose complete asset set can be verified and downloaded
  without cloning the repository.

- [ ] **Step 1: Add release build jobs for all four targets**

  Each native runner checks out the exact tag without persisted credentials, installs pinned
  toolchains, builds the sealed CPython 3.14 product and complete bundle, builds native Tauri
  packages, performs installed-product smoke, and uploads only closed expected artifacts.

- [ ] **Step 2: Add native signing gates**

  macOS jobs import the protected Developer ID identity, sign the app and every executable,
  notarize, staple, and verify with `codesign`, `spctl`, and `stapler`. Windows jobs sign the
  bootstrap, executables, NSIS installer, and MSI, then verify publisher identity and timestamp.
  Missing credentials fail the stable workflow before publication.

- [ ] **Step 3: Assemble one draft release transaction**

  The release job creates a draft, downloads the four exact artifact sets, rejects duplicate or
  missing filenames, builds the closed cross-platform manifest and `install.sh`, verifies every
  digest and native signature again, generates GitHub artifact attestations, uploads all assets,
  and runs clean virtual-machine install-to-use jobs against the draft assets.

- [ ] **Step 4: Publish only after every predicate passes**

  Publish the draft as a non-prerelease only after Linux, Windows, both macOS architectures,
  headless curl installation, native package installation, Python 3.14 admission, desktop startup,
  CLI doctor, MCP, repair, update, rollback, default uninstall, release-attestation verification,
  and the unchanged-head release closer all succeed. Enable GitHub immutable releases before the
  publication run.

- [ ] **Step 5: Keep pull-request CI targeted but fail closed**

  Classify any installer, distribution, Python lock, package, workflow, manifest, signing, or
  release change as full code plus desktop work. Continue skipping compiler jobs only for proven
  documentation-only changes. Extend the existing workflow-policy test rather than adding a new
  checker or test file.

- [ ] **Step 6: Commit**

  ```bash
  git add .github dist-workspace.toml scripts/tests/test_ci_workflow_policy.py \
    docs/verification/usable-release-gate.md
  git commit -m "ci(release): publish verified native installation assets"
  ```

---

### Task 7: Make installation obvious and truthful in maintained documentation

**Files:**

- Modify: `README.md`
- Modify: `docs/operations/installation-and-bootstrap.md`
- Modify: `docs/operations/model-inference.md`
- Modify: `docs/reference/configuration.md`
- Modify: `docs/architecture/deployment.md`
- Modify: `docs/research/2026-07-28-cross-platform-installation-and-guided-setup.md`
- Modify: `docs/plans/delivery-ledger.md`
- Modify: `docs/project-memory.md`

**Interfaces:**

- Consumes: real published asset names, supported platforms, install roots, lifecycle commands, and
  exact successful evidence.
- Produces: a beginner-first README and operating documentation that never requires source-build
  knowledge for ordinary use.

- [ ] **Step 1: Replace the README quick start only after the endpoint works**

  Lead with:

  ```bash
  curl -fsSL \
    https://github.com/Sawmonabo/market-squawk/releases/latest/download/install.sh | sh
  ```

  Add direct GitHub Release links for macOS DMG, Windows NSIS/MSI, Linux AppImage/DEB, and headless
  bundles. Explain that the complete default includes desktop, CLI, helpers, Python 3.14, models,
  and setup. Move every Rust, Node, pnpm, Cargo, and Tauri command into Development.

- [ ] **Step 2: Document real lifecycle operations**

  Record install locations, first launch, update, repair, rollback, default uninstall,
  separately confirmed data deletion, offline installation, signature/attestation verification,
  logs, failure recovery, and success evidence for each supported platform.

- [ ] **Step 3: Update architecture and release truth**

  Add the versioned installation store and release trust boundary to deployment architecture.
  Record only exact implemented behavior and evidence in memory and the delivery ledger. Do not
  add prose tests, redirect pages, documentation scripts, or fictional commands.

- [ ] **Step 4: Commit**

  ```bash
  git add README.md docs
  git commit -m "docs: lead with complete one-command installation"
  ```

---

### Task 8: Freeze, verify, publish, and close the release

**Files:**

- Modify: `docs/plans/delivery-ledger.md`
- Modify: `docs/reports/usable-release-review.md`
- Modify: `docs/project-memory.md`

**Interfaces:**

- Consumes: all exact installed-product reports and one unchanged release candidate.
- Produces: Quarter 4 approval, an immutable public release, closed GitHub issues, and cleaned
  product worktrees and branches.

- [ ] **Step 1: Run focused local preflight**

  ```bash
  cargo fmt --all --check
  CARGO_INCREMENTAL=0 cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
  CARGO_INCREMENTAL=0 cargo test --workspace --all-features --locked
  pnpm --dir apps/market-squawk-desktop install --frozen-lockfile
  pnpm --dir apps/market-squawk-desktop typecheck
  pnpm --dir apps/market-squawk-desktop test --run
  pnpm --dir apps/market-squawk-desktop build
  ```

- [ ] **Step 2: Freeze one clean exact candidate and run the authoritative gate**

  Confirm clean status, exact commit/tree, origin equality, no active compiler, and disk headroom.
  Then run exactly once:

  ```bash
  CARGO_INCREMENTAL=0 ./scripts/verify.sh
  ```

  Do not amend or add documentation after this gate.

- [ ] **Step 3: Complete Quarter 4 review**

  Give the independent reviewer the same frozen commit, complete hosted build/install evidence,
  artifact names, sizes, SHA-256 values, native signature results, GitHub attestation results, and
  release-closer output. Any substantiated Critical, Important, or Minor finding rejects the
  candidate and must be remediated before exact-head rereview.

- [ ] **Step 4: Publish and verify the public surface**

  Publish the immutable release, then independently verify:

  ```bash
  gh release verify v0.2.0
  rm -rf /tmp/market-squawk-release-verify
  mkdir -m 700 /tmp/market-squawk-release-verify
  gh release download v0.2.0 --pattern install.sh --dir /tmp/market-squawk-release-verify
  gh release verify-asset v0.2.0 /tmp/market-squawk-release-verify/install.sh
  ```

  Run the public `releases/latest/download/install.sh` command from a fresh supported host and
  confirm it resolves to the exact approved release.

- [ ] **Step 5: Close GitHub state and clean execution infrastructure**

  Record exact evidence on PRs and issues `#36` and `#38`, close only delivered acceptance,
  set Project 5 items to Done, merge the accepted product commits, remove clean completed targets
  and worktrees, delete merged local/origin feature branches, prune metadata, and preserve any dirty
  or unique state.
