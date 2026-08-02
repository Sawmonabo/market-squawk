# V1 Installed-Product Dependency Admission

## Document control

| Field | Value |
| --- | --- |
| Document type | Implementation dependency and platform admission record |
| Audience | Runtime, MCP, desktop, modeling, installer, security, and release owners |
| Status | Task 0 decision baseline with Task 7 Python artifact admission recorded; release approval remains pending |
| Research date | 2026-08-01 |
| Last substantive review | 2026-08-02 |
| Audited commit | `f43da3aa5cbd887a35c9ef25c748b722c9d5c028` |
| Audited tree | `7387a1db499cf3fc6afb58792a28adfb7e5e4d84` |
| Governing design | [V1 installed-product design](../superpowers/specs/2026-08-01-market-squawk-v1-installed-product-experience-design.md) |
| Implementation plan | [V1 implementation plan](../superpowers/plans/2026-08-01-market-squawk-v1-installed-product-experience.md) |

This record persists the implementation-head refresh that supersedes stale dependency and platform
assumptions in the original plan. It is not a lockfile, release approval, or claim that the named
capability is already implemented. Refresh volatile versions, advisories, yanks, licenses, wheels,
and protocol facts immediately before each serialized lock mutation.

## Contents

- [Decision summary](#decision-summary)
- [Rust runtime and HTTP](#rust-runtime-and-http)
- [MCP protocol and RMCP](#mcp-protocol-and-rmcp)
- [Python, forecasting, and wheel admission](#python-forecasting-and-wheel-admission)
- [Frontend and native desktop](#frontend-and-native-desktop)
- [Per-user service lifecycle](#per-user-service-lifecycle)
- [Lock and acceptance gates](#lock-and-acceptance-gates)
- [Primary sources](#primary-sources)

## Decision summary

| Area | Decision | Status at the audited head |
| --- | --- | --- |
| Rust | Retain exact 1.97.1 | Already configured; 1.97.0 is rejected because 1.97.1 corrected its LLVM miscompilation |
| Async runtime | Pin Tokio 1.53.1 and Tokio-util 0.7.19 with explicit direct features | Lock/update required |
| HTTP | Admit Axum 0.8.9, Tower 0.5.3, and Tower HTTP 0.7.0 only for layers installed by production code; retain exact Hyper 1.11.0, Hyper-util 0.1.20, and http-body-util 0.1.4 | Manifest/lock required |
| MCP SDK | Upgrade RMCP 2.2.0 to 3.1.0 behind a Market Squawk protocol facade | Breaking source migration and lock required |
| MCP wire protocol | Use stable 2026-07-28 as the shared service's only primary protocol | Design/plan refreshed; implementation/conformance required |
| Forecast reduction | Use scikit-learn 1.9.0 primitives behind a bounded Market Squawk horizon adapter | Python lock and implementation required |
| Conformal evidence | Add MAPIE 1.4.1 for the explicitly approved time-series conformal path | Exact Python lock implemented; method-evidence implementation required |
| Rejected forecast package | Do not admit skforecast 0.23.0 | Rejected: no CPython 3.14 Intel-macOS Numba/llvmlite closure |
| Model export | Keep skl2onnx 1.20.0 and ONNX 1.22.0 only behind exact estimator/export/tract parity | Builder fix and exact matrix required |
| Wheel policy | Use packaging 26.2 public tag/parser/marker APIs | Builder and exact four-target inventories implemented |
| Python toolchain | Keep CPython 3.14.6 and PyArrow 25.0.0; refresh uv 0.12.0 to 0.12.1 | Exact four-target uv identities and wheel inventories implemented |
| Frontend | Admit the exact planned TanStack, chart, React peer, and dialog versions | Frontend/native locks and notices required |
| macOS service | Keep macOS 12+ and use a per-user LaunchAgent | Installer implementation and exact-floor evidence required |
| Windows service | Use Task Scheduler 2.0 at the exact current-user SID and least privilege | Installer implementation and exact-floor evidence required |
| Linux service | Use `systemd --user` without mandatory linger | Installer implementation and Ubuntu 24.04 evidence required |
| Process identity | Admit sysinfo 0.39.6 with only `system` for PID/start-time verification | Task 5 manifest/lock and supported-platform evidence required |

## Rust runtime and HTTP

| Package | Selected version | Direct feature boundary | License / support note |
| --- | ---: | --- | --- |
| Tokio | 1.53.1 | `io-std`, `io-util`, `macros`, `net`, `rt-multi-thread`, `signal`, `sync`, `time` where directly used; never `full` | MIT; MSRV 1.71 |
| Tokio-util | 0.7.19 | `rt`; add `codec` only where direct code uses it | MIT; MSRV 1.71 |
| Axum | 0.8.9 | Start with `http1`, `json`, `tokio`; retain `matched-path`, `query`, or `tracing` only when production code proves use | MIT; MSRV 1.80 |
| Tower | 0.5.3 | Only installed `limit`, `load-shed`, `timeout`, and `util` layers | MIT; MSRV 1.64 |
| Tower HTTP | 0.7.0 | Only installed body-limit, request-ID, sensitive-header, timeout, trace, and validation layers | MIT; direct 0.7 API review required |
| Hyper | 1.11.0 | Existing `http1` and `server` boundary | MIT; retain exact line |
| Hyper-util | 0.1.20 | Existing Tokio integration only unless direct code proves more | MIT; retain exact line |
| http-body-util | 0.1.4 | No package features required | MIT; retain exact line |
| Reqwest | 0.13.4 | Existing minimal JSON/Rustls client reused by the authenticated loopback `ApplicationClient`; no RMCP client feature | MIT/Apache-2.0; already admitted by provider and installer clients |
| Rusqlite | 0.40.1 | Existing bundled SQLite boundary reused by the durable job repository | MIT; no external database service |
| Process-wrap | 9.1.0 | Existing `std` wrapper with `process-group` on Unix and `job-object` on Windows | MIT/Apache-2.0; required for bounded worker-tree containment |
| Sysinfo | 0.39.6 | `default-features = false`, `system` only; current-process and exact-PID start time | MIT; MSRV 1.95; supports Linux, macOS, and Windows |
| Schemars | RMCP-owned 1.2.1 | No direct Market Squawk dependency; RMCP uses it for protocol schemas | MIT; do not force a duplicate 1.2.2 graph without direct production use |

Do not enable default/full feature sets merely to make a compile pass. Task 1 performs one
serialized resolution, inspects `cargo tree -e features`, duplicate versions, advisories, licenses,
and the lock diff, then uses `--locked` for every later command.

Task 5 found one missing cross-platform primitive: authenticated rendezvous records bind a PID and
process-start discriminator, but the application had no supported-platform implementation for
reading that discriminator. `sysinfo` 0.39.6 is admitted instead of repository-owned OS FFI. Its
documented `Process::start_time` returns epoch seconds, `ProcessesToUpdate::Some` refreshes only the
selected PID, and the upstream crate supports Linux, macOS, and Windows with Rust 1.95. Market
Squawk enables only `system`; component, disk, network, user, serialization, and multithreading
features remain disabled. PID/start time is one layer in stale-record rejection alongside the
signed runtime generation, owner-only rendezvous, single-instance lease, and authenticated health
check; it is not treated as a security credential.

## MCP protocol and RMCP

The stable MCP release at the research cutoff is `2026-07-28`. It is stateless and uses
per-request discovery/version/capability metadata. The shared service therefore:

- explicitly selects `ProtocolVersion::V_2026_07_28`;
- advertises a singleton admitted-version set unless named-client evidence authorizes a separate
  compatibility boundary;
- requires modern stateless metadata and exact HTTP header/body agreement;
- accepts POST only on the modern HTTP endpoint;
- does not mint or require `Mcp-Session-Id`;
- does not implement standalone GET/DELETE or `Last-Event-ID` replay; and
- keeps cross-call state only in explicit authenticated product handles and durable Job resources.

RMCP 3.1.0 is accepted conditionally behind a narrow facade. A bare version bump is rejected
because RMCP's tagged defaults can advertise every known version, retain legacy lifecycle behavior,
and use a `LATEST` constant that does not express Market Squawk's product decision. The facade owns
exact version selection, stateless configuration, closed Host/Origin/auth/limit policy, and
negative conformance tests. RMCP continues to own MCP framing and transport mechanics; Market
Squawk owns credentials, authorization, bounds, audit, jobs, artifacts, and product handles.

The candidate RMCP feature set is limited to the server, stdio, and Streamable HTTP server needed
by the shared endpoint and relay-facing protocol handler. The relay reuses the authenticated
`ApplicationClient` rather than adding RMCP's Reqwest HTTP-client edge. Do not admit RMCP client,
OAuth, native TLS, child-process, WebSocket, HTTP/2, macros, elicitation, or request-state features
without a separate implemented use and admission decision.

Claude Code and Codex use installer-owned stdio relay registrations by default so credentials do
not appear in their configuration or command arguments. Each relay is a small adapter to the one
shared service; it is not a second product daemon. A client-facing legacy lifecycle may be admitted
only when current evidence for that named client requires it. The relay cannot transfer implicit
legacy connection/session authority to the modern service.

## Python, forecasting, and wheel admission

### Forecast dependency decision

`skforecast==0.23.0` is inadmissible. Its top-level universal wheel requires Numba/llvmlite, and no
stable dependency line provides both CPython 3.14 and Intel-macOS wheels. Dropping Intel macOS,
building LLVM/Numba during installation, using a prerelease wheel, or patching upstream metadata
would violate the approved product contract.

The V1 closure instead uses:

- scikit-learn 1.9.0 for maintained estimators, direct/multi-output/chained reductions,
  `TimeSeriesSplit`, and quantile regressors;
- MAPIE 1.4.1 for the approved time-series conformal methods;
- skl2onnx 1.20.0 and ONNX 1.22.0 for the admitted central-model export path; and
- packaging 26.2 for wheel, requirement, specifier, marker, and compatibility-tag admission.

MLForecast 1.1.0 is the best ready-made reduction alternative, but its 26-28-distribution graph and
mandatory Optuna/Tqdm edge add runtime and license/notice surface that the focused adapter does not
need. Reconsider it only if measured implementation growth exceeds lag construction, ordered
horizon estimators, a bounded recursive loop, temporal fold orchestration, and calibration
coordination. MAPIE is admitted because conformal evidence is an explicit V1 requirement and adds
only one pure-Python package beyond the scikit-learn graph; its method assumptions, calibration
window, target coverage, and realized coverage must remain visible. Quantile bands and conformal
intervals are distinct typed results and neither is presented as certainty.

Python estimator objects remain contained worker state. They are never a trusted serialized model
format because pickle/joblib loading can execute code. Rust admits only the closed native/ONNX
bundle boundary.

### Wheel compatibility correction

The current builder's filename substring checks are not sufficient. They reject valid ABI3 wheels
such as ONNX 1.22.0's `cp312-abi3` artifacts and can admit free-threaded `cp314t`, too-new macOS,
wrong-architecture, generic Linux, or otherwise unsupported artifacts.

Use packaging 26.2 public APIs:

- `parse_wheel_filename` for normalized identity, version, build, and expanded tags;
- `Requirement`, `SpecifierSet`, and `Marker.evaluate` for dependency and target-marker replay;
- `cpython_tags` and `compatible_tags` for ordinary CPython 3.14 plus valid ABI3/pure tags; and
- `mac_platforms((12, 0), arch)` for the declared macOS floor and architecture ordering.

Generate foreign-target tags explicitly; never use a developer host's `sys_tags()` as proof for
another target. Derive the Linux ordered manylinux tags inside the exact admitted glibc-2.28
release-builder image and compare them in the native lane. Reject sdists, free-threaded ABI tags,
musllinux-only wheels, generic Linux, too-new floors, wrong architectures, yanked/prerelease
artifacts, mutable/VCS/local URLs, missing hashes/sizes, and ambiguous equal-rank candidates.
For the implemented selector, ambiguity means equal preferred-tag rank and equal declared-tag
breadth; a uniquely narrower candidate at the same rank is deterministic.

Every target lock binds the selected wheel's filename, normalized name/version, complete tags,
URL, SHA-256, size, yanked state, `Requires-Python`, metadata digest, and target selection. Inspect
the exact wheel archive for repeatable `License-File` entries, SPDX expressions, `RECORD` coverage,
and bundled native-library notices. Requirements, master wheel lock, target inventory, offline
installed distributions, and bundled native libraries must agree exactly.

### Task 7 exact artifact admission

The 2026-08-02 Task 7 refresh implemented the sole hashed requirements authority and one
generation-bound master/target manifest set. The activation generation is
`4dc8aee225412f0fee0d2bff3f77f56b4b65268d234ef8c25518da7891c01b8b`. The master contains 41
immutable wheel identities across 20 projects; each target inventory selects exactly one wheel per
applicable project:

| Target | Selected wheels | Platform condition |
| --- | ---: | --- |
| `aarch64-apple-darwin` | 19 | standard-GIL CPython 3.14, macOS 12+, arm64/universal2 |
| `x86_64-apple-darwin` | 19 | standard-GIL CPython 3.14, macOS 12+, x86-64/universal2 |
| `x86_64-unknown-linux-gnu` | 19 | standard-GIL CPython 3.14, admitted manylinux x86-64 tags through glibc 2.28 |
| `x86_64-pc-windows-msvc` | 20 | standard-GIL CPython 3.14, `win_amd64`; includes Windows-only Colorama |

The exact installed/test closure is Colorama 0.4.6 where its Windows marker applies, Iniconfig
2.3.0, Joblib 1.5.3, MAPIE 1.4.1, ml-dtypes 0.5.4, Narwhals 2.24.0, NumPy 2.5.1, ONNX 1.22.0,
packaging 26.2, Pluggy 1.6.0, protobuf 7.35.1, PyArrow 25.0.0, Pygments 2.20.0, pytest 9.1.1,
scikit-learn 1.9.0, SciPy 1.18.0, skl2onnx 1.20.0, threadpoolctl 3.6.0, and typing-extensions
4.16.0. Maturin 1.14.1 is the exact build-only root. MAPIE is an admitted mandatory V1 dependency,
not a deferred alternative.

The lock refresh downloads only the selected official PyPI wheels into temporary storage, checks
their URL, size, SHA-256, metadata digest, interpreter requirement, repeatable license files, and
complete wheel `RECORD`, then replays target markers and `Requires-Dist` against the exact selected
closure. Target inventories are written first and the master activation record last; all readers
require one matching generation and fail closed on partial replacement.

#### Compressed-tag ordering and deterministic selection

Packaging 26.2's public `parse_wheel_filename` API defaults `validate_order` to `False`. Market
Squawk retains that public default because Maturin 1.14.1's only arm-capable macOS wheel declares
the official compressed platform set as
`macosx_10_12_x86_64.macosx_11_0_arm64.macosx_10_12_universal2`, which is not lexical order.
Enabling the optional order validator would reject that official immutable artifact even though
its expanded tags contain the required arm64/universal2 compatibility. This exception changes only
compressed-component ordering: the builder still parses and binds every expanded tag and enforces
the standard-GIL ABI, supported architecture, operating-system floor, immutable identity, and
closed project set.

Wheel selection ranks candidates by the earliest matching tag in Market Squawk's ordered target
tag set. When candidates share that best rank, it selects the candidate declaring fewer tags so the
narrowest compatible artifact wins—for example, the exact x86-64 Maturin wheel rather than its
broader universal wheel on Intel macOS. Equal rank and equal tag breadth remain an ambiguity and are
rejected; filename order never grants authority.

#### Exact uv 0.12.1 component identities

The four uv archives were fetched from the official 0.12.1 GitHub release, checked against the
upstream checksum list, extracted, and recorded with both archive and executable identity:

| Target | Archive bytes / SHA-256 | Executable bytes / SHA-256 |
| --- | --- | --- |
| `aarch64-apple-darwin` | 17,679,560 / `77d2906988e8074fd43f2f329ec452ebbf9b0c257ba1c66451c71de70a6baf42` | 40,218,304 / `cf8774f78b8df0768991aeb5a1c78f9c61f3a0b4993c875b83d2b4a66b80bf9e` |
| `x86_64-apple-darwin` | 19,622,543 / `69d9f9a00337f25a50dcb13882052da08b8469bac11091c98c5694c3c6721467` | 48,074,964 / `88af0b228e9eaa017c670d73e8c74fbd220450cb74195025203dc0009335351e` |
| `x86_64-pc-windows-msvc` | 19,073,343 / `8fcb0cb46e1229065e344758980924e569bef5882ef45f46fada8fb24e06b74a` | 48,254,464 / `f537cc65c1791d9d1a022132302b21ecd48cdf0a605a7b345809fbe8af4e807d` |
| `x86_64-unknown-linux-gnu` | 21,760,555 / `90b2f223fb69d19db49e117da601f64978593417988530aa733d456141b4bcbb` | 56,107,008 / `92face6b1f0462ad911857957bd168cd4ae45515e2a2cb3fcc3ecbda3d4d82b1` |

### Model export parity

For every admitted estimator/preprocessor combination:

1. fit and predict in the exact Python environment;
2. convert with an explicit input schema and target opset;
3. run `onnx.checker.check_model(..., full_check=True)`;
4. record IR/opset/operator/type/shape/external-data/size/hash facts; and
5. compare output with the exact locked Rust tract runtime across representative and boundary data.

An unsupported conversion excludes that estimator from the live ONNX path. Research-only
interval computation may remain in the contained Python worker and must not be described as a live
ONNX output.

## Frontend and native desktop

| Package | Version | Purpose and admission condition |
| --- | ---: | --- |
| `@tanstack/react-query` | 5.101.4 | Bounded server-state request, cache, mutation, and reconnect lifecycle; no second product authority |
| `@tanstack/react-table` | 8.21.3 | Headless accessible table state over bounded server-side results |
| `lightweight-charts` | 5.2.0 | Dense financial and predictive time series; TradingView attribution and NOTICE are mandatory |
| `recharts` | 3.10.1 | Portfolio, risk, attribution, scenario, and analytical charts; measured route-level bundle gate |
| `react-is` | 19.2.8 | Exact React 19.2.8 peer for Recharts |
| Rust `tauri-plugin-dialog` | 2.7.2 | Rust-only controlled file selection with the plugin's supported GTK3 Linux backend; no JavaScript binding or WebView dialog/filesystem authority |

The frontend lock gate must inspect integrity, peer resolution, lifecycle scripts, exact licenses
and notices, duplicate modules, and the clean production Vite bundle delta. Lightweight Charts'
required TradingView attribution must appear in the product and packaged notices. Lazy-load chart
routes and measure actual raw/compressed chunks rather than using registry unpacked size as a
claim.

Task 1 owns the Rust dialog dependency manifest/lock once. Task 14 owns plugin registration,
generated permissions/capabilities, frontend dependencies, and the pnpm lock; it does not resolve
the Rust dependency a second time. The WebView receives only an opaque staged input ticket and
validated metadata, never an ambient file path or service credential.

## Per-user service lifecycle

The platform mechanisms preserve all supported floors without paid credentials:

- **macOS 12+:** one owner-scoped LaunchAgent in `~/Library/LaunchAgents`, launched in the
  foreground with exact immutable executable arguments and bounded restart behavior. Developer ID
  and notarization affect Gatekeeper trust, not LaunchAgent registration. `SMAppService` is macOS
  13+ and cannot be the only implementation.
- **Windows 10 1809+:** Task Scheduler 2.0 through its API, bound to the exact current-user SID,
  interactive token, and least-privilege/LUA run level. Store no password and use no SYSTEM,
  service account, or elevation. Prove current-user MSI behavior or do not claim the MSI conforms.
- **Ubuntu 24.04-compatible:** one `systemd --user` unit with no mandatory linger. Availability
  follows the signed-in user's manager/session and returns at login.

The service registration points to the exact immutable active version. Stable installer-owned
entrypoints serve the CLI, installer, and MCP relay. Install, update, repair, rollback, and uninstall
form one recoverable transaction over the release selector, stable entrypoints, native service
definition, authenticated health, and owned client-registration receipts.

## Lock and acceptance gates

Before a serialized dependency boundary is committed:

1. refresh releases, yanks, advisories, licenses, supported floors, and exact package artifacts at
   the current clean head;
2. update the governing design/plan and this record when the accepted decision changes;
3. perform one unlocked resolution, inspect the complete manifest/lock/feature/duplicate diff, and
   then use `--locked` only;
4. run the focused package/protocol/builder/frontend gates named in the implementation plan;
5. reconcile exact licenses/notices and complete-release inputs rather than project-page labels;
6. record clean head/tree and feature/origin equality before a Wave freeze; and
7. retain exact platform/package/conformance evidence for the grouped review rather than treating a
   focused compile as release approval.

MCP approval additionally requires stable-2026 discovery and metadata, negative version and
Host/Origin/auth tests, no leaked optional capabilities, two-client isolation, bounded request/SSE
resources, exact stdio behavior, and pinned official conformance evidence supplemented by
repository-owned security tests.

Python approval additionally requires four target-specific wheel-only closures, offline native
installation, exact imports, deterministic direct/recursive/exogenous/temporal/interval behavior,
model parity, license/notice reconciliation, and no runtime download or source build.

Frontend approval additionally requires frozen installation, peer and lifecycle-script proof,
license/NOTICE inventory, measured production chunks, exact capabilities, and packaged WebView
smokes on all four targets.

## Primary sources

Sources were checked on 2026-08-01; Python artifact and Packaging API evidence was refreshed on
2026-08-02.

- Rust and runtime: [Rust 1.97.1](https://blog.rust-lang.org/2026/07/16/Rust-1.97.1/),
  [Tokio releases](https://github.com/tokio-rs/tokio/releases), [Axum
  0.8.9](https://github.com/tokio-rs/axum/releases/tag/axum-v0.8.9), [Tower
  0.5.3](https://github.com/tower-rs/tower/releases/tag/tower-0.5.3), and [Tower HTTP
  0.7.0](https://github.com/tower-rs/tower-http/releases/tag/tower-http-v0.7.0);
  exact package documentation for [Reqwest 0.13.4](https://docs.rs/reqwest/0.13.4/reqwest/),
  [Rusqlite 0.40.1](https://docs.rs/rusqlite/0.40.1/rusqlite/), and
  [Process-wrap 9.1.0](https://docs.rs/process-wrap/9.1.0/process_wrap/); [sysinfo 0.39.6
  supported systems and MSRV](https://github.com/GuillaumeGomez/sysinfo), [process start-time
  contract](https://docs.rs/sysinfo/0.39.6/sysinfo/struct.Process.html#method.start_time), and
  [feature manifest](https://docs.rs/crate/sysinfo/0.39.6/source/Cargo.toml).
- MCP: [stable 2026-07-28 protocol](https://modelcontextprotocol.io/specification/2026-07-28/basic),
  [versioning](https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning),
  [stdio](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio),
  [Streamable HTTP](https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http),
  [changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog), and [RMCP
  3.1.0](https://github.com/modelcontextprotocol/rust-sdk/releases/tag/rmcp-v3.1.0).
- Python: [Python 3.14](https://www.python.org/downloads/), [uv
  0.12.1](https://github.com/astral-sh/uv/releases/tag/0.12.1), [PyArrow
  installation](https://arrow.apache.org/docs/python/install.html), exact release metadata for
  [scikit-learn 1.9.0](https://pypi.org/pypi/scikit-learn/1.9.0/json), [MAPIE
  1.4.1](https://pypi.org/pypi/mapie/1.4.1/json), [Maturin
  1.14.1](https://pypi.org/pypi/maturin/1.14.1/json), [skl2onnx
  1.20.0](https://pypi.org/pypi/skl2onnx/1.20.0/json), [ONNX
  1.22.0](https://pypi.org/pypi/onnx/1.22.0/json), and [packaging
  26.2](https://pypi.org/pypi/packaging/26.2/json).
- Forecast methods: [scikit-learn lagged forecasting and
  quantiles](https://scikit-learn.org/stable/auto_examples/applications/plot_time_series_lagged_features.html),
  [`TimeSeriesSplit`](https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html),
  [`MultiOutputRegressor`](https://scikit-learn.org/stable/modules/generated/sklearn.multioutput.MultiOutputRegressor.html),
  [MAPIE `TimeSeriesRegressor`](https://mapie.readthedocs.io/en/v1.4/generated/mapie.regression.TimeSeriesRegressor.html),
  and [EnbPI research](https://proceedings.mlr.press/v139/xu21h.html).
- Packaging: [wheel tags and ordered compatibility](https://packaging.pypa.io/en/stable/tags.html),
  [`parse_wheel_filename` and its public `validate_order=False`
  default](https://packaging.pypa.io/en/stable/utils.html), [platform tag
  specification](https://packaging.python.org/en/latest/specifications/platform-compatibility-tags/),
  [Core Metadata](https://packaging.python.org/en/latest/specifications/core-metadata/), [PEP
  639](https://peps.python.org/pep-0639/), and [SPDX expressions](https://spdx.github.io/spdx-spec/v3.0.1/annexes/spdx-license-expressions/).
- Frontend/native: [TanStack Query](https://tanstack.com/query/v5/docs/framework/react/guides/queries),
  [TanStack Table](https://tanstack.com/table/latest/docs/introduction), [Lightweight Charts
  attribution](https://tradingview.github.io/lightweight-charts/docs/5.0), [Recharts](https://recharts.github.io/),
  [Tauri dialog](https://v2.tauri.app/plugin/dialog/), and [Tauri
  capabilities](https://v2.tauri.app/security/capabilities/).
- Platform lifecycle: [Apple LaunchAgents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatinglaunchdJobs.html),
  [Microsoft logon task](https://learn.microsoft.com/en-us/windows/win32/taskschd/starting-an-executable-when-a-user-logs-on),
  [Microsoft run level](https://learn.microsoft.com/en-us/windows/win32/taskschd/principal-runlevel),
  [Ubuntu 24.04 release notes](https://documentation.ubuntu.com/release-notes/24.04/), and
  [`pam_systemd`](https://www.freedesktop.org/software/systemd/man/252/pam_systemd.html).
