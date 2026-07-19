# Usable-release dependency and provider decisions

**Research date:** 2026-07-18

**Audit commit:** `a829278aca4d4fc27d5a0c0aaa8e5a49f2cb5659`

**Audit tree:** `6f5d9b7be896e9a5409f367c73aa4a5d95208a9c`

**Status:** decision input for the usable complete-release plan; not dependency admission or release
evidence

This report refreshes the dependency and provider decisions needed by Tasks 1-20 of the
[canonical usable-release plan](../superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md).
Facts are linked to primary project or provider material. Statements labeled **Decision** or
**Inference** are Market Squawk policy derived from those facts.

The audit commit contains five local packages. `cargo metadata --locked --all-features` reports the
application plus `domain`, `platform`, `sources`, and `live`; each declares Rust 1.97.1 and
`Apache-2.0 OR MIT`. None of the research, modeling, portfolio, valuation, complete MCP, or required
adapter packages exists yet. The versions below therefore remain proposed exact direct dependencies
until their first real producer and consumer are integrated through one reviewed lockfile change.

## Admission decision

1. Pin release-critical additions exactly. Disable default features unless the row explicitly says
   otherwise. `Cargo.lock`, `cargo tree -e features`, `cargo deny`, and `cargo audit` remain the
   authority for the resolved graph; a direct-version table is not a substitute for them.
2. Builds and runtime may not download native libraries, model files, schemas, taxonomies, or test
   data. Network access belongs only to an explicitly invoked, allowlisted provider adapter.
3. The default ONNX backend is self-contained `tract-onnx`. The release-candidate `ort` crate is
   opt-in, dynamically loads an operator-provided local ONNX Runtime, and is never required for a
   functional release.
4. Python is a mandatory research product, not part of the live path. Its floor is Python 3.10,
   driven by PyArrow 24.0.0. Rust/Python exchange occurs through versioned Parquet or Arrow IPC and a
   narrow PyO3 kernel boundary, never an assumed Arrow C++ ABI match.
5. Every provider uses one authorization identity and one process-authoritative budget. Caching,
   retries, bulk endpoints, and lawful failover are resilience mechanisms; identity, account,
   proxy, fingerprint, CAPTCHA, or distributed-request rotation is prohibited.

## Frozen Rust dependency set

### Analytical storage and SQL

| Direct dependency | Exact manifest request | License / floor | Admission rationale and transitive risk |
| --- | --- | --- | --- |
| [`datafusion`](https://docs.rs/crate/datafusion/54.0.0) | `=54.0.0`, `default-features = false`, `features = ["parquet", "recursive_protection", "sql"]` | Apache-2.0; Rust 1.88 | DataFusion 54.0.0 resolves Arrow and Parquet 58.3.0. Keep DataFusion's crypto, general compression, regex, Unicode, nested-expression, Avro, and Parquet-encryption features off. Its core graph still includes CSV/JSON datasource and `object_store` crates. Its `parquet` feature also enables Parquet's default codec set; Cargo feature unification means a direct `default-features = false` request cannot subtract those transitive defaults. This broader effective graph is admitted only with an audited exact lock and local manifest-pinned registration; no cloud object-store configuration is exposed. SQL recursion protection complements, but does not replace, owned AST, plan, row, byte, memory, deadline, and cancellation limits. See the [54.0.0 source manifest](https://docs.rs/crate/datafusion/54.0.0/source/Cargo.toml.orig). |
| [`arrow`](https://docs.rs/crate/arrow/58.3.0) | `=58.3.0`, `default-features = false`, `features = ["canonical_extension_types", "ipc"]` | Apache-2.0; Rust 1.85 | Pin the DataFusion-compatible line even though Arrow 59 exists. IPC is the controlled Rust/Python interchange. DataFusion also enables Arrow's defaults plus `prettyprint` and `chrono-tz`, so the effective graph contains Arrow CSV/JSON even though file adapters do not rely on them. Do not add `ffi`/`pyarrow`; the release uses serialized IPC/Parquet and a narrow PyO3 kernel API, not an in-process Arrow C ABI. |
| [`parquet`](https://docs.rs/crate/parquet/58.3.0) | `=58.3.0`, `default-features = false`, `features = ["arrow", "async", "crc", "zstd"]` | Apache-2.0; Rust 1.85 | Arrow conversion, asynchronous local publication, page CRC, and Zstandard writing are required. DataFusion feature unification additionally admits Parquet's default Snappy, Brotli, deflate, LZ4, Zstandard, base64, and SIMD UTF-8 graph plus `object_store`; Market Squawk writes one configured codec and never exposes direct object-store access. CLI, encryption, geospatial, experimental variants, and JSON remain off. Codec/build-script exposure must pass offline multi-platform builds; no runtime download is allowed. |
| [`rusqlite`](https://docs.rs/crate/rusqlite/0.40.1) | `=0.40.1`, `default-features = false`, `features = ["backup", "bundled"]` | MIT; crate MSRV undeclared | `bundled` gives the application one reviewed SQLite build instead of an ambient system ABI. rusqlite 0.40.1's `libsqlite3-sys` bundles SQLite 3.53.2, whose source is public domain. This compiles local C and therefore requires a documented C toolchain, but performs no download and needs no external database service. Disable loadable extensions, SQLCipher, virtual-table conveniences, serialization, and WASM. Because the crate does not declare an MSRV, exact Rust 1.97.1 compile/clippy/test evidence is an admission gate. See [rusqlite feature guidance](https://github.com/rusqlite/rusqlite#optional-features) and [SQLite copyright](https://www.sqlite.org/copyright.html). |

**Decision.** Arrow 59.1.0 is not mixed into this graph. All Decimal128, timestamp, provenance, and
schema round trips use the 58.3.0 family selected by DataFusion 54.0.0. Objects are written and
fsynced before a SQLite manifest transaction publishes them; neither SQLite nor DataFusion is ever
queried from the live event-to-action path.

### HTTP, files, archives, and secrets

| Direct dependency | Exact manifest request | License / floor | Admission rationale and transitive risk |
| --- | --- | --- | --- |
| [`reqwest`](https://docs.rs/crate/reqwest/0.13.4) | `=0.13.4`, `default-features = false`, `features = ["gzip", "json", "rustls-no-provider", "stream"]` | MIT OR Apache-2.0; Rust 1.85 | Disables native TLS, cookies, system proxy discovery, HTTP/2/3, SOCKS, multipart, and alternate compression. Install one application-selected rustls crypto provider explicitly and keep the existing endpoint allowlist, DNS/IP checks, redirect revalidation, body limits, deadlines, and cancellation. A compressed response is still bounded on decoded bytes. |
| [`csv`](https://docs.rs/crate/csv/1.4.0) | `=1.4.0` | Unlicense OR MIT; Rust 1.73 | Mature streaming parser, wrapped by owned row, field, decoded-byte, record-count, encoding, decimal, timestamp, and cancellation limits. Schema inference is opt-in and bounded. |
| [`quick-xml`](https://docs.rs/crate/quick-xml/0.41.0) | `=0.41.0`, `default-features = false`, `features = ["encoding"]` | MIT; Rust 1.79 | Use the pull reader, not unbounded Serde materialization. Reject DTDs, external entities, unexpected namespaces, excessive depth/text/attributes, and trailing content. This parser covers Treasury XML, XBRL, and OFX 2 XML; OFX 1 SGML receives an owned bounded tokenizer. |
| [`calamine`](https://docs.rs/crate/calamine/0.36.0) | `=0.36.0`, `default-features = false`, `features = ["dates"]` | MIT; Rust 1.88 | Required for Excel import. Its graph includes ZIP deflate, XML, code-page decoding, and fast numeric conversion. Admission requires archive entry/count/ratio/expanded-byte, worksheet/cell/string, date-system, formula-result, and type bounds before canonical conversion. |
| [`zip`](https://docs.rs/crate/zip/8.6.0) | `=8.6.0`, `default-features = false`, `features = ["deflate"]` | MIT; Rust 1.88 | Needed for SEC bulk archives and is already unified by calamine. Keep AES, bzip2, deflate64, LZMA, PPMd, time, XZ, and Zstandard off. Every member is path-normalized and streamed under aggregate expansion limits; encrypted and unsupported entries fail closed. |
| [`encoding_rs`](https://docs.rs/crate/encoding_rs/0.8.35) | `=0.8.35` | Apache-2.0 OR MIT plus BSD-3-Clause; Rust 1.36 | Explicitly decode supported legacy CSV/XML/portfolio encodings; reject undecodable or undeclared ambiguous input. The third license is already permissive but must appear in the release license inventory. |
| [`atomicwrites`](https://docs.rs/crate/atomicwrites/0.4.4) | `=0.4.4`, admitted only under `cfg(windows)` for `market-squawk-data` and `market-squawk-platform` | MIT; crate MSRV undeclared; last release/code activity in 2024 | Supplies one audited safe boundary for lossless-path, no-clobber, write-through publication while the workspace retains `unsafe_code = "forbid"`. Its Windows [`move_atomic`](https://docs.rs/crate/atomicwrites/0.4.4/source/src/lib.rs) accepts `Path` values and calls `MoveFileExW` with `MOVEFILE_WRITE_THROUGH` but not replacement or cross-volume-copy flags. Data owns close-before-publish, root/endpoint revalidation, writer exclusion, ambiguous-result reconciliation, and receipt-bound SQLite verification. Platform uses it only inside the retained two-slot authority-state protocol, which never treats an in-place replacement as its recovery proof. The crate lacks active maintenance and upstream Windows CI, and [does not establish atomic visibility](https://github.com/untitaker/rust-atomicwrites/issues/27); those accepted residual risks prohibit sole-authority in-place replacement. The target-only graph adds `windows-sys` 0.52; exact Rust 1.97.1 Windows build/test evidence and source re-audit on every version change are admission gates. |
| [`keyring`](https://docs.rs/crate/keyring/4.1.5) | `=4.1.5`, `default-features = false`, `features = ["v1"]` | MIT OR Apache-2.0; Rust 1.88 | OS store first: macOS Keychain, Windows Credential Manager, and Linux Secret Service. The Linux path brings D-Bus/Zbus runtime integration; unavailable or locked stores fall back explicitly rather than silently losing secrets. No CLI feature or database keystore is enabled. |
| [`argon2`](https://docs.rs/crate/argon2/0.5.3) | `=0.5.3`, `default-features = false`, `features = ["alloc", "zeroize"]` | MIT OR Apache-2.0; Rust 1.65 | Stable Argon2id KDF for the encrypted local fallback. Parameters, version, random salt, and memory/time/parallelism limits are persisted; the crate's intermediate hashes and the application-derived key are zeroized. Do not adopt the 0.6 release candidate. |
| [`chacha20poly1305`](https://docs.rs/crate/chacha20poly1305/0.11.0) | `=0.11.0`, `default-features = false`, `features = ["alloc", "getrandom", "zeroize"]` | Apache-2.0 OR MIT; Rust 1.85 | XChaCha20-Poly1305 authenticates version and key-name metadata. A unique OS-random nonce is mandatory; the unlock secret is never stored with ciphertext. Reduced-round modes are forbidden. |
| [`getrandom`](https://docs.rs/crate/getrandom/0.3.4) | `=0.3.4`, default features | MIT OR Apache-2.0; Rust 1.63 | Supplies OS entropy for fallback salts and key material without an application PRNG. The selected version is already present in the audited lock graph. WASM JavaScript support remains disabled; any entropy-source failure aborts secret creation or rotation before publication. |

**Decision.** These libraries supply mechanics, not trust. Provider JSON duplicate keys, XML
entities, ZIP bombs, Excel formulas, OFX ambiguity, and Parquet metadata are rejected or normalized
by Market Squawk-owned bounded adapters before domain construction. SQLite/database export support
reads only operator-selected local files through controlled paths; it never accepts an arbitrary
connection string or remote database.

### MCP, Python, and inference

| Dependency | Exact request | License / floor | Admission decision |
| --- | --- | --- | --- |
| [`rmcp`](https://docs.rs/crate/rmcp/2.2.0) | `=2.2.0`, `default-features = false`, `features = ["server", "transport-io"]` | Apache-2.0; crate MSRV undeclared | Minimal local stdio server only. `server` brings schema support; `transport-io` supplies stdio. Do not enable client, HTTP/SSE, OAuth/auth, reqwest, child-process, Unix-socket, command discovery, or remote transport features. Market Squawk still owns pre-dispatch structural bounds, lifecycle policy, deadlines, output backpressure, audit, artifacts, authorization, and risk. Exact Rust 1.97.1 gates are mandatory because the crate declares no MSRV. See the [official SDK features](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/crates/rmcp/Cargo.toml). |
| [`pyo3`](https://docs.rs/crate/pyo3/0.29.0) | `=0.29.0`, `default-features = false`, `features = ["abi3-py310", "extension-module", "macros"]` | MIT OR Apache-2.0; Rust 1.83; Python 3.8+ upstream | `abi3-py310` makes the project floor explicit and avoids building one extension ABI per interpreter. Bind only pure analytics/domain conversions; do not bind live ownership, source credentials, risk authority, approved orders, filesystem capabilities, or mutable adapter handles. |
| [`maturin`](https://pypi.org/project/maturin/1.14.1/) | `==1.14.1` build tool; sdist SHA-256 `9d6577a62cd08e0ceba7a0db06fb098e0c9b1b3429bad747a4f3a18215a1b3df` | MIT OR Apache-2.0; Python >=3.7 | Build-only, hash-locked, and provisioned in the offline release wheelhouse. It is not a product runtime dependency. |
| [`pyarrow`](https://pypi.org/project/pyarrow/24.0.0/) | `==24.0.0`; sdist SHA-256 `85fe721a14dd823aca09127acbb06c3ca723efbd436c004f16bca601b04dcc83` | Apache-2.0; Python >=3.10 | Mandatory manifest-bound Parquet/IPC consumer. Do not infer binary compatibility from the Rust Arrow version; validate the serialized schema, metadata, content hash, Decimal scale, timezone, and provenance at the file/IPC boundary. Python 3.10 is the project floor. |
| [`tract-onnx`](https://docs.rs/crate/tract-onnx/0.23.4) | `=0.23.4`, `default-features = false` | MIT OR Apache-2.0; Rust 1.91 | Required self-contained ONNX-compatible backend. It has no default feature or native runtime download. Bundle admission still allowlists supported operators, dimensions, tensor bytes, model bytes, threads, warm-up, and deadlines; any parse or inference error produces no automated action. |
| [`ort`](https://docs.rs/crate/ort/2.0.0-rc.12) | Optional `=2.0.0-rc.12`, `default-features = false`, `features = ["api-24", "load-dynamic", "std"]` | MIT OR Apache-2.0; Rust 1.88; no stable 2.x release | Optional isolated backend only. Explicitly exclude `download-binaries`, `copy-dylibs`, `fetch-models`, TLS, training, tracing, ndarray, and every hardware execution-provider feature. Load only an operator-provided local ONNX Runtime 1.24 library after exact version/platform/license/hash verification from a confined no-follow path. Run it behind a failure-isolated worker and keep `tract-onnx` as the stable fallback. ONNX Runtime is MIT licensed; its C/C++ shared library is a separately inventoried native artifact. See the [`ort` feature manifest](https://github.com/pykeio/ort/blob/9c840a386acc808aaaf5ac28ae0fc13ee164678c/Cargo.toml) and [ONNX Runtime compatibility table](https://onnxruntime.ai/docs/reference/compatibility.html). |

The Python lock contains only actual product consumers and their hash-locked transitives. It must
not add a broad notebook stack to satisfy the release. Statistical kernels may use floating point
behind explicit conversion boundaries; accounting, money, fees, cost basis, and portfolio ledgers
remain Decimal/currency typed. A clean Python 3.10 environment must install from the local wheelhouse
without reaching PyPI, read a committed manifest, run the finance/training workflow, and hand a
fully hashed model candidate to the Rust bundle validator.

## Provider access and qualification policy

Provider documentation changes independently of this repository. The following is the frozen
2026-07-18 access contract; Task 1 must recheck it at the approved-head refresh, and adapters record
the policy version and retrieval time used for every run.

| Provider | Official facts and coverage | Market Squawk policy |
| --- | --- | --- |
| Coinbase Exchange | The [WebSocket overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview) distinguishes public `ws-feed` from authenticated `ws-direct`, which has direct access to Coinbase Exchange servers, and defines exact per-product increasing sequences. The [`full` channel](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) requires subscribe, queue, REST level-3 snapshot, discard through the snapshot sequence, replay, then live apply. Published [WebSocket](https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits) and [REST](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits) limits are shared limits, not per-worker entitlements. Coverage is one Coinbase venue, not consolidated crypto or national-market coverage. | Credential-free `ws-feed` keeps the product functional for display, research, capture, and failover but cannot inherit direct-delivery evidence. Default immediate-action qualification is possible only on user-authorized `ws-direct` plus `full` and a matching REST snapshot after exact instrument/venue, generation, sequence, duplicate/out-of-order, timestamp, freshness, status, tick/lot, queue, and coverage gates pass. Any gap, overflow, disconnect, stale status, or snapshot mismatch quarantines and resynchronizes. `level2` remains non-authoritative because its documented payload lacks the same sequence evidence. One authorization identity and one subscription/REST budget are enforced; no rotation. |
| Kraken Spot | The [V2 book contract](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book) supplies snapshot/update timestamps and a CRC32 checksum but no numeric venue sequence field. The [checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) requires message-atomic level application, delete-on-zero, subscribed-depth truncation, exact decimal lexemes, and top-10 CRC construction. The [trade channel](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade) has per-book trade IDs, which do not sequence book updates. Coverage is one Kraken venue. | Never fabricate `SequenceNumber`. Implement a versioned composite profile: one connection generation, first provider snapshot, strict single-reader transport order, message-atomic apply, checksum after every snapshot/update, exact lexemes, no queue loss, monotonic receive evidence, timestamps/freshness/status/precision, and terminal quarantine on reconnect, overflow, parse/checksum, or crossed-book failure. Because a state checksum cannot prove receipt of every intermediate update, the default ceiling is `DirectUnverified` under the current valid-sequence-progression contract. Raising it requires an explicit reviewed domain-contract decision and unchanged-head external evidence; it is not an adapter-local choice. |
| SEC EDGAR | [Data APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) provide unauthenticated submissions and XBRL Company Facts plus nightly bulk archives. [Developer resources](https://www.sec.gov/about/developer-resources) cap aggregate access at 10 requests/second regardless of machine count; the [developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) requires a declared organization/contact user agent. Government-created and public filing content is reusable, but ticker mappings are not guaranteed complete or accurate. | Use one process-global limiter below the published ceiling, declared contact, conditional requests, nightly bulk files for bulk work, bounded exponential backoff with `Retry-After`, and content-addressed local persistence. Preserve submissions, accession, filing/acceptance/effective/publication evidence, amendments, XBRL context/unit/decimals, source bytes/hash, and unknown availability honestly. Do not poll faster by distributing requests. |
| FRED / ALFRED | The [API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html) covers both databases; [ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html) retains original releases and revisions through real-time periods and vintage dates. A registered [API key](https://fred.stlouisfed.org/docs/api/api_key.html) is required. Current V2 [error guidance](https://fred.stlouisfed.org/docs/api/fred/v2/errors.html) states two requests/second before 429. The [terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html) require attribution/notices, allow termination, and warn that third-party series retain their own copyrights and restrictions. | Use one user-supplied key and a conservative two-request/second provider-wide budget, redact the key, honor 429/`Retry-After`, paginate, and request explicit real-time bounds. Durable cache/persist/display/train admission is per series and operation: copyrighted or ambiguous series fail closed until rights evidence exists. Preserve missing markers, vintages, revisions, supersession, publication/availability uncertainty, series notes, and source hashes. User-owned lawful exports remain a fallback; no key creation automation or identity rotation. |
| BLS | [Getting started](https://www.bls.gov/developers/home.htm) documents public unregistered V1 and free registered V2. The [FAQ](https://www.bls.gov/developers/api_faqs.htm) lists V1 limits of 25 queries/day, 25 series/query, and 10 years/query; registered V2 allows 500, 50, and 20 respectively. The [terms](https://www.bls.gov/developers/termsOfService.htm) prohibit limit circumvention and require retrieval-date citation and the BLS derived-analysis disclaimer. | Credential-free V1 is the default. V2 is used only with an operator-supplied free registration key; Market Squawk never automates its CAPTCHA or registration. Deterministically chunk within the selected tier, persist the tier and retrieval date, treat response-level messages/partial results as typed outcomes, cache lawful results, preserve preliminary/footnote metadata, and stop on exhausted daily budget rather than changing identities. |
| U.S. Treasury | Treasury publishes an official [daily interest-rate XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) and [rate methodology/context](https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics). The yield curve is daily and derived from indicative quotations, not a live executable market feed. No numeric access allowance was found on the cited feed page. | Use allowlisted HTTPS GETs without credentials, a conservative configurable limiter, conditional cache, source hashes, schema/methodology version, and bounded XML parsing. Honor 429/5xx and `Retry-After`; do not infer an unpublished entitlement. Classify the daily series as official delayed research evidence with its indicative methodology, never `DirectVerified`, Level-1 evidence by default, or an executable quote. Local operator-supplied Treasury files are the availability fallback. |

## Mandatory approved-head refresh barrier

Nothing in this report authorizes a manifest or lockfile change against the audit commit. Before the
first consumer is merged, the integration owner must:

- refresh all exact versions, yanked/advisory status, declared floors, features, provider policies,
  and direct links against the independently approved implementation base;
- inspect the proposed full graph with `cargo tree -e features` and the minimal lock diff, including
  duplicate major versions, build scripts, C/C++/assembly, dynamic libraries, licenses, advisories,
  and packages capable of network access;
- prove the exact Rust 1.97.1 and Python 3.10 build/install paths offline from approved caches or a
  hash-locked wheelhouse; no dependency build may fetch a binary, model, schema, or taxonomy;
- run the real dependency, vulnerability, license, source, credential, and generated-artifact gates
  on the unchanged candidate; and
- amend this report or record a dated superseding decision if any fact changed. Silent substitution
  of a newer crate, provider endpoint, model runtime, or feature set is forbidden.

This refresh is a release barrier, not a reason to defer implementation. If a required provider's
rights or availability do not admit an operation, the adapter must expose the limitation and lawful
local-file fallback, and the release remains blocked where no mandatory usable path exists.
