# Docs Batch 001 Deep Dive

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews only the three assigned official-documentation families as of
**2026-07-15**:

1. `docs-036`: Rust 1.97.0 release notes plus the Rust 2024/Cargo resolver 3 family.
2. `docs-037`: Tokio runtime shutdown, cancellation, task tracking, and bounded `mpsc`.
3. `docs-038`: Serde, Reqwest, and Tokio-Tungstenite transport-boundary documentation.

The decision context is Market Squawk's pinned Rust 1.97.0 virtual workspace and its local,
bounded, integrity-gated source adapters. The review focuses on current versions, compatibility,
feature prerequisites, failure behavior, security-sensitive defaults, lifecycle semantics, and
the boundary between transport/parsing success and executable market-data qualification.

Facts supported directly by the assigned documentation are labeled **Confirmed**.
Recommendations and Market Squawk-specific interpretations are labeled **Inference**. All links
were accessed on **2026-07-15**.

## Sources Reviewed

| Assigned ID | Source family | Primary source | Companion pages reviewed | Freshness signal |
| --- | --- | --- | --- | --- |
| `docs-036` | Rust 1.97.0 and Cargo resolver 3 | [Rust release notes](https://doc.rust-lang.org/stable/releases.html) | [Rust 2024 resolver chapter](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html) | Release notes identify Rust 1.97.0 dated 2026-07-09. |
| `docs-037` | Tokio lifecycle and bounded queues | [Tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown) | [Tokio `mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Current Rustdoc identifies Tokio 1.52.3 dated 2026-07-12. |
| `docs-038` | Typed serialization, HTTP, and WebSockets | [Reqwest Rustdoc](https://docs.rs/reqwest/latest/reqwest/) | [Reqwest `ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html), [Reqwest `Error`](https://docs.rs/reqwest/latest/reqwest/struct.Error.html), [Serde overview](https://serde.rs/), [Serde container attributes](https://serde.rs/container-attrs.html), [Serde error handling](https://serde.rs/error-handling.html), [Tokio-Tungstenite Rustdoc](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/) | Current pages identify Reqwest 0.13.4 and Tokio-Tungstenite 0.30.0; Tokio-Tungstenite is dated 2026-07-11. |

## Findings

### 1. The specified Rust baseline is current, but compatibility still needs locked verification

**Confirmed.** The official release notes identify Rust **1.97.0**, released
**2026-07-09**, as the stable version at the head of the release history. Its Cargo changes
include stabilized `build.warnings` configuration and `resolver.lockfile-path`. The release also
changes the default symbol-mangling scheme to v0, which can affect older debuggers/profilers and
backtrace formatting, and changes a post-`shutdown` Windows socket write from `Other` to
`BrokenPipe` ([Rust 1.97.0 release notes](https://doc.rust-lang.org/stable/releases.html)).

**Confirmed.** Edition 2024 implies dependency resolver 3, which enables Rust-version-aware
fallback selection. Resolver selection is workspace-global and ignored in dependencies. A
virtual workspace does not inherit a package edition, so it must still set `resolver = "3"`
explicitly in `[workspace]`. The Edition Guide recommends verifying latest dependencies in CI
and says there is no automated resolver migration tool
([Rust 2024 resolver chapter](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)).

**Inference.** The requested root configuration—pinned `rust-toolchain.toml`, Edition 2024,
explicit resolver 3, inherited `rust-version`, and committed `Cargo.lock`—is consistent with the
official rules. Resolver 3 improves dependency selection; it does not prove that the resolved
all-feature graph compiles, tests, or behaves correctly on 1.97.0. The acceptance authority
should remain the locked `fmt`, `clippy`, `test`, and release-build commands on the pinned
toolchain.

**Inference.** Rust 1.97's new `build.warnings` capability may supplement workspace lint policy,
but it should not silently replace the specification's explicit Clippy invocation or alter
third-party dependency diagnostics. The symbol-mangling compatibility note should be included in
release-tooling validation if profiling, crash symbolization, or backtrace parsing is automated.

### 2. Tokio supplies lifecycle primitives, not Market Squawk's overflow policy

**Confirmed.** Tokio's shutdown guide decomposes graceful shutdown into three stages: determine
when to stop, notify participating tasks, and wait for them to finish. It uses cloned
`CancellationToken` values for cooperative notification and a `TaskTracker` that resolves its
`wait` future only after the tracker is closed and all tracked tasks complete
([Tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown)).

**Confirmed.** Tokio 1.52.3 `mpsc` provides bounded and unbounded variants behind the `sync`
feature. At capacity, a bounded channel makes sending wait for capacity and therefore provides
backpressure. An unbounded channel has infinite capacity and its `send` completes immediately
([Tokio `mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/)).

**Confirmed.** Channel failure and shutdown behavior is explicit: once all senders are dropped,
the receiver drains buffered messages and then returns `None`; dropping the receiver causes
future sends to error and unread buffered messages to be dropped. Clean shutdown instead calls
`Receiver::close()` and consumes the channel to completion. Tokio `mpsc` is runtime-agnostic,
although timeout-suffixed methods require a Tokio timer
([Tokio `mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/)).

**Inference.** Every live source-to-shard and shard-to-consumer queue should be bounded and have
a domain-level overflow outcome. Waiting indefinitely is not automatically safe for a live feed:
if backpressure causes sequence gaps or freshness expiry upstream, the stream should degrade or
quarantine and resynchronize. Tokio provides the bounded mechanism, but it does not define
sequence recovery, data-quality transitions, queue sizing, or whether a sender may wait, reject,
drop, or disconnect.

**Inference.** The shutdown tutorial's use of an unbounded channel to aggregate rare shutdown
conditions is not evidence that an unbounded channel is appropriate for market events. A cloned
`CancellationToken` plus tracked tasks is suitable for service lifecycle; source disconnect,
book invalidation, and stream resynchronization remain explicit adapter state transitions.

### 3. Serde is a DTO boundary; validation must be a separate fallible conversion

**Confirmed.** Serde separates Rust data structures implementing `Serialize`/`Deserialize` from
format implementations, with derive support based on Rust traits rather than runtime reflection
([Serde overview](https://serde.rs/)). Serde documents that syntax, wrong types, missing required
fields, and format/data-model mismatches may fail and are returned through format-specific
`Result` error types ([Serde error handling](https://serde.rs/error-handling.html)).

**Confirmed.** For self-describing formats such as JSON, unknown fields are ignored by default;
`#[serde(deny_unknown_fields)]` changes that to an error. Serde also supports fallible container
conversion through `#[serde(try_from = "FromType")]`. `deny_unknown_fields` cannot be combined
with `flatten`, and untagged enum matching may have uninformative errors and higher cost
([Serde container attributes](https://serde.rs/container-attrs.html)).

**Inference.** Provider wire DTOs should deserialize first and then cross a typed `TryFrom`
boundary that validates symbols, venue/instrument mapping, timestamps, sequence values, decimal
scale, tick/lot precision, enum exhaustiveness, and required provenance. Successful JSON parsing
does not establish those invariants or qualify a record as `DirectVerified`.

**Inference.** Strictness should be source- and message-specific. Execution-critical message
envelopes benefit from deliberate unknown-field handling and tests for schema drift, but globally
denying all unknown fields can turn harmless additive provider changes into outages. If unknown
fields are allowed, schema-version, fixture, health, and observability checks must still expose
material changes.

### 4. Reqwest defaults require explicit production hardening

**Confirmed.** Reqwest 0.13.4 provides an asynchronous Tokio client, JSON/form/body support,
custom redirects, proxies, TLS, and connection pooling. Its documentation recommends reusing a
`Client` for multiple requests to benefit from keep-alive pooling. Redirects are followed by
default up to ten hops, system proxies are enabled by default, and `ClientBuilder::no_proxy()`
disables automatic system-proxy use ([Reqwest Rustdoc](https://docs.rs/reqwest/latest/reqwest/)).

**Confirmed.** Total, read, and connect timeouts have no configured deadline by default.
`ClientBuilder::timeout` spans connection through completion of the response body;
`read_timeout` resets after each successful read; `connect_timeout` covers connection setup and
requires a Tokio timer. The documented default retry behavior is limited to protocol NACKs
([Reqwest `ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html)).

**Confirmed.** Reqwest exposes error classification for timeout, request, connect, body, decode,
and status errors. A status error is associated with use of `Response::error_for_status`, rather
than every non-success response becoming a transport error automatically
([Reqwest `Error`](https://docs.rs/reqwest/latest/reqwest/struct.Error.html)).

**Confirmed.** TLS is used by default for HTTPS. `ClientBuilder` can select native TLS or Rustls,
set certificates and client identity, and set protocol-version bounds. Certificate and hostname
validation are enabled by default; the documentation warns that disabling either introduces
significant man-in-the-middle exposure. Rustls key logging through `SSLKEYLOGFILE` is off by
default. Reqwest's HTTP/3 support is explicitly unstable and may change in patch releases
([Reqwest Rustdoc](https://docs.rs/reqwest/latest/reqwest/),
[Reqwest `ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html)).

**Inference.** Build one reusable client per intentional policy class with explicit total/read/
connect timeouts, redirect rules, endpoint allowlists, TLS backend/features, response-size limits,
user agent, and proxy decision. A privacy-sensitive local default should call `no_proxy()` unless
the user explicitly authorizes system proxies. Certificate/hostname validation must not be
disabled in production, and verbose connection/TLS-key logging must be treated as sensitive.

**Inference.** Retry classification belongs above raw Reqwest errors: distinguish connect and
timeout failures, HTTP `429`/`Retry-After`, retryable server errors, permanent client errors,
decode/schema errors, and cancellation. Retry only idempotent operations by default, cap attempts
and elapsed time, add jitter, and surface health degradation. Reqwest does not supply provider
quota policy, idempotency knowledge, or Market Squawk source-health transitions.

### 5. Tokio-Tungstenite supplies framed async transport, not venue integrity

**Confirmed.** Tokio-Tungstenite 0.30.0, dated 2026-07-11, integrates WebSocket handshakes and
streams with Tokio. `WebSocketStream` implements `Stream` and `Sink`. Connection APIs accept a
request for custom headers and offer configuration-aware variants. The connector supports
native TLS, Rustls, or a plain connection
([Tokio-Tungstenite Rustdoc](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)).

**Confirmed.** Configuration-aware connect functions expose an optional TLS connector and a
`disable_nagle` choice; the documentation recommends leaving Nagle enabled unless the caller
understands the tradeoff. The crate re-exports Tungstenite, which owns the underlying WebSocket
protocol logic and error types
([Tokio-Tungstenite Rustdoc](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)).

**Inference.** Adapters should explicitly select `wss` endpoints and an approved TLS feature,
bound WebSocket frame/message sizes through configuration, supervise read/write halves under
cancellation, and translate protocol/IO/TLS/close errors into typed source failures. Nagle and
buffer settings should remain measured tuning parameters, not presumed latency wins.

**Inference.** A successful WebSocket handshake and valid frame prove only transport/protocol
success. They do not establish venue authorization, sequence continuity, snapshot/delta
consistency, checksum validity, timestamp sanity, freshness, trading status, precision, or source
coverage. Those checks must precede `DirectVerified` and any immediate automated action.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** Rust 1.97.0 was released 2026-07-09. | [Rust releases](https://doc.rust-lang.org/stable/releases.html) | Version heading and date at the head of the stable release notes. | High | Direct primary release record. |
| **Confirmed:** Rust 1.97 defaults to v0 symbol mangling, with debugger/profiler/backtrace compatibility implications. | [Rust releases](https://doc.rust-lang.org/stable/releases.html) | Explicit 1.97 compatibility note. | High | Release tooling should be exercised; no application incompatibility is asserted. |
| **Confirmed:** Edition 2024 implies resolver 3 and Rust-version fallback. | [Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html) | Summary and details sections state both implications. | High | Applies at the top-level workspace. |
| **Confirmed:** A virtual workspace must explicitly declare resolver 3. | [Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html) | Virtual-workspace exception is explicit. | High | Directly applicable to the specified layout. |
| **Inference:** Resolver 3 does not replace a locked all-feature build on Rust 1.97. | [Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html) | Guide recommends latest-dependency CI but does not guarantee whole-graph compatibility. | High | Conservative build-governance conclusion. |
| **Confirmed:** Tokio bounded `mpsc` provides capacity backpressure; unbounded capacity is infinite. | [Tokio `mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Module description distinguishes the two variants. | High | `sync` feature required. |
| **Confirmed:** Dropping an `mpsc` receiver drops unread messages; clean shutdown closes then drains. | [Tokio `mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Disconnection and clean-shutdown sections. | High | Relevant to audit/persistence loss semantics. |
| **Confirmed:** Tokio shutdown uses cooperative cancellation plus task completion tracking. | [Tokio shutdown](https://tokio.rs/tokio/topics/shutdown) | CancellationToken clone/cancel flow and TaskTracker close/wait flow. | High | Cancellation does not itself define recovery policy. |
| **Inference:** Live queue overflow must degrade/quarantine rather than silently discard critical events. | [Tokio `mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Library offers backpressure and send errors, not market-data integrity policy. | High | Derived from Market Squawk's execution requirements. |
| **Confirmed:** Serde ignores unknown JSON fields by default unless `deny_unknown_fields` is used. | [Serde container attributes](https://serde.rs/container-attrs.html) | Attribute documentation states both behaviors. | High | Strictness requires source-specific judgment. |
| **Confirmed:** Serde serialization/deserialization failures are returned through typed `Result` errors. | [Serde error handling](https://serde.rs/error-handling.html) | Error-handling page lists syntax/type/data-model failure modes. | High | Domain validation remains separate. |
| **Confirmed:** Reqwest follows up to ten redirects and enables system proxies by default. | [Reqwest Rustdoc](https://docs.rs/reqwest/latest/reqwest/) | Redirect and proxy sections. | High | `no_proxy()` disables automatic proxy use. |
| **Confirmed:** Reqwest has no total/read/connect timeout by default. | [Reqwest `ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html) | Each timeout method documents its default. | High | Production clients need explicit deadlines. |
| **Confirmed:** Disabling Reqwest certificate or hostname verification creates significant MITM exposure. | [Reqwest `ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html) | Both dangerous methods carry explicit warnings. | High | Must remain enabled in production. |
| **Confirmed:** Tokio-Tungstenite 0.30.0 exposes WebSockets as `Stream`/`Sink` with native-TLS/Rustls/plain connector choices. | [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/) | Crate overview and connector enum. | High | Transport capability only. |
| **Inference:** Transport and decode success cannot qualify data as `DirectVerified`. | [Serde](https://serde.rs/), [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/) | These sources cover serialization and WebSocket protocol, not venue integrity. | High | Sequence/checksum/freshness/status validation is additional domain logic. |

## Source-Specific Notes

### `docs-036` — Rust 1.97 and resolver 3

- **Capability documented (Confirmed):** exact stable release/date, language/library/Cargo
  changes, and compatibility notes; Edition 2024's resolver behavior and virtual-workspace rule
  ([release notes](https://doc.rust-lang.org/stable/releases.html),
  [Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)).
- **Integration pattern (Inference):** pin 1.97.0, declare Edition 2024 and resolver 3 at the
  virtual-workspace root, inherit the declared Rust version, commit the lockfile, and use stable
  locked verification in CI and release scripts.
- **Limit/requirement (Confirmed):** resolver 3 is top-level/global, ignored in dependencies, and
  has no automated migration tool
  ([Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)).
- **Relevance:** directly validates the requested toolchain date and workspace-resolver choice.

### `docs-037` — Tokio lifecycle and queues

- **Capability documented (Confirmed):** cooperative cancellation, shutdown-condition
  aggregation, task joining, bounded backpressure, disconnection, and close/drain behavior
  ([shutdown guide](https://tokio.rs/tokio/topics/shutdown),
  [`mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/)).
- **Integration pattern (Inference):** source/service supervisor owns cancellation and tracked
  tasks; every event queue is sized and bounded; each send failure or capacity timeout maps to a
  named health and integrity transition.
- **Limit/requirement (Confirmed):** `mpsc` needs Tokio's `sync` feature, and timeout-suffixed
  methods require a Tokio timer
  ([`mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/)).
- **Relevance:** supplies bounded memory and deterministic lifecycle primitives without mixing
  persistence or analytics into the live path.

### `docs-038` — Serde, Reqwest, and Tokio-Tungstenite

- **Capability documented (Confirmed):** trait-driven DTO serialization; reusable pooled async
  HTTP; JSON/form/stream features; redirects, proxies, TLS, timeouts, and classified request
  errors; Tokio-integrated WebSocket `Stream`/`Sink`
  ([Serde](https://serde.rs/), [Reqwest](https://docs.rs/reqwest/latest/reqwest/),
  [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)).
- **Integration pattern (Inference):** explicit client security policy, typed source DTOs,
  fallible provider-to-domain conversions, bounded response/frame handling, cancellation, and
  typed source failure classification.
- **Limit/requirement (Confirmed):** JSON and streaming features are feature-gated; Reqwest's
  asynchronous client requires Tokio; TLS backend behavior is feature-specific; Tokio-
  Tungstenite's TLS dependencies are optional
  ([Reqwest](https://docs.rs/reqwest/latest/reqwest/),
  [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)).
- **Security relevance (Inference):** explicitly decide proxy behavior, forbid invalid-certificate
  and invalid-hostname modes in production, do not enable TLS key logging by default, and do not
  use proxies or TLS/browser fingerprint manipulation to conceal traffic or evade provider
  controls.

## Cross-Source Patterns

1. **Defaults are library defaults, not product policy.** **Confirmed:** resolver selection,
   queue capacity, timeouts, redirects, proxies, TLS backend/features, and Serde schema strictness
   all require explicit choices across the reviewed sources. **Inference:** consolidate those
   choices in workspace policy and typed configuration rather than relying on ambient state.

2. **Boundaries expose failure; applications attach meaning.** **Confirmed:** Tokio sends can
   wait or error, Serde can return decode/type failures, Reqwest classifies request failures, and
   Tokio-Tungstenite returns protocol/transport failures. **Inference:** Market Squawk must map
   each failure into retry, reconnect, resynchronize, stale, degraded, or quarantined states.

3. **Layered success is not data integrity.** **Confirmed:** the reviewed components cover build
   resolution, async coordination, serialization, HTTP, TLS, and WebSocket framing. None documents
   exchange sequence/checksum/freshness rules. **Inference:** only the full source-specific
   validation chain can produce executable-quality events.

4. **Version pinning and feature pinning are inseparable.** **Confirmed:** substantial behavior is
   feature-gated in Tokio, Reqwest, and Tokio-Tungstenite, while resolver 3 selects dependency
   versions using declared Rust compatibility. **Inference:** `Cargo.lock`, explicit Cargo
   features, Rust 1.97 locked builds, and dependency audits are a single release evidence set.

5. **Ambient networking settings can violate local-first expectations.** **Confirmed:** Reqwest
   enables system proxies by default and follows redirects by default. **Inference:** endpoint
   allowlists, redirect restrictions, and an explicit proxy decision are necessary to meet the
   no-hidden-outbound-request requirement.

## Limitations and Non-Findings

- **No all-feature Rust 1.97 compatibility proof.** Current Rustdoc versions and successful
  docs.rs publication do not establish the selected crates' full declared MSRV, the compatibility
  of every optional feature combination, or compatibility of Market Squawk's eventual locked
  dependency graph. That requires the specified local locked builds and tests.
- **No performance claim.** These sources document APIs and semantics, not Market Squawk's
  100,000-event/s or sub-millisecond p99 targets. Queue capacity, allocator behavior, TLS choice,
  decode cost, and Nagle settings require benchmarks on the documented target hardware.
- **No end-to-end cancellation guarantee.** Tokio cancellation is cooperative. The reviewed
  pages do not prove that every DNS lookup, TLS handshake, HTTP body stream, WebSocket operation,
  or provider-side request terminates immediately on cancellation.
- **No built-in bounded HTTP/WebSocket payload policy was established.** Reqwest and Tokio-
  Tungstenite expose streaming/configuration primitives, but this batch did not establish a safe
  universal response, header, frame, or message-size default for heterogeneous providers.
- **No venue integrity semantics.** Serde and Tokio-Tungstenite do not validate exchange
  sequences, snapshots, deltas, checksums, freshness, timestamps, status, or precision.
- **No retry safety guarantee.** Reqwest's documented default retry is narrow, and none of the
  assigned sources knows provider quotas, order/extraction idempotency, or `Retry-After` policy.
- **No TLS-backend recommendation from the sources.** Reqwest and Tokio-Tungstenite document
  native TLS and Rustls choices. Selecting one for portability, certificate-store behavior, FIPS
  constraints, or reproducibility needs separate platform requirements and locked tests.
- **No Serde schema-versioning system.** Attributes control serialization behavior but do not
  supply provenance schemas, version negotiation, compatibility policy, or provider drift
  monitoring.
- **No permission for concealment or quota evasion.** Proxy and TLS configurability are transport
  capabilities, not authorization for identity rotation, fingerprint spoofing, CAPTCHA bypass,
  concealment proxies, or distributed quota evasion. Those behaviors were not adopted.

## Source List

All sources were accessed on **2026-07-15**.

1. Rust Project, [Rust Release Notes](https://doc.rust-lang.org/stable/releases.html).
2. Rust Project, [Cargo: Rust-version aware resolver — Rust Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html).
3. Tokio Project, [Graceful Shutdown](https://tokio.rs/tokio/topics/shutdown).
4. Tokio Project, [`tokio::sync::mpsc` Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/).
5. Serde Project, [Serde Overview](https://serde.rs/).
6. Serde Project, [Container Attributes](https://serde.rs/container-attrs.html).
7. Serde Project, [Error Handling](https://serde.rs/error-handling.html).
8. Reqwest Project, [Reqwest 0.13.4 Rustdoc](https://docs.rs/reqwest/latest/reqwest/).
9. Reqwest Project, [`ClientBuilder` Rustdoc](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html).
10. Reqwest Project, [`Error` Rustdoc](https://docs.rs/reqwest/latest/reqwest/struct.Error.html).
11. Tokio-Tungstenite Project, [Tokio-Tungstenite 0.30.0 Rustdoc](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/).
