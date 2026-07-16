# Docs Batch 003 Deep Dive

## Table of Contents

1. [Batch Scope](#batch-scope)
2. [Sources Reviewed](#sources-reviewed)
3. [Findings](#findings)
4. [Evidence Table](#evidence-table)
5. [Source-Specific Notes](#source-specific-notes)
6. [Cross-Source Patterns](#cross-source-patterns)
7. [Limitations and Non-Findings](#limitations-and-non-findings)
8. [Source List](#source-list)

## Batch Scope

This report reviews only `docs-042` (Model Context Protocol 2025-11-25),
`docs-043` (Coinbase Exchange WebSocket channels), and `docs-044` (Kraken
WebSocket v2 book checksum). It focuses on local stdio MCP bounds, Coinbase
execution-quality limitations, Kraken checksum/resynchronization rules, and concrete
tests. Sources were accessed on **2026-07-15**. **Confirmed** statements are directly
documented; **Inference** statements apply that evidence to Market Squawk.

## Sources Reviewed

| ID | Official family | Pages reviewed | Main use |
|---|---|---|---|
| `docs-042` | MCP 2025-11-25 specification | [Transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle), [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools), [progress](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress), [cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation), [schema](https://modelcontextprotocol.io/specification/2025-11-25/schema) | Framing, negotiation, schemas, timeouts, cancellation, audit/security |
| `docs-043` | Coinbase Exchange | [WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | Trades, books, sequences, status, and documented gaps |
| `docs-044` | Kraken Exchange | [WebSocket v2 book checksum](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | Atomic book maintenance, precision, CRC32 validation |

## Findings

### 1. MCP stdio framing and lifecycle are strict

**Confirmed.** MCP stdio uses UTF-8 JSON-RPC. The client launches the server as a
subprocess; messages are newline-delimited requests, responses, or notifications and
must not contain embedded newlines. Server `stdout` may contain only valid MCP
messages, while UTF-8 logs may go to `stderr` and do not necessarily indicate errors.
([MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports))

**Inference.** Market Squawk must reserve `stdout` exclusively for serialized MCP
frames; tracing, panic-hook text, banners, and diagnostics belong on redacted
`stderr`. The reader needs a maximum line size and must reject invalid UTF-8,
multi-line frames, malformed JSON-RPC, duplicate IDs, and trailing non-message text
without attempting recovery that could merge frames.

**Confirmed.** Initialization must be the first interaction. Client and server
negotiate protocol version and capabilities, then the client sends
`notifications/initialized`. During operation both sides may use only negotiated
capabilities. For stdio shutdown, the client should close server input, wait, then use
`SIGTERM` and finally `SIGKILL` if reasonable deadlines expire.
([MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle))

**Inference.** Implement an explicit state machine—`Starting`, `Initializing`,
`Operating`, `ShuttingDown`, `Closed`—and fail requests arriving in the wrong state.
Pin version `2025-11-25`; reject unsupported negotiated versions and unadvertised
capabilities. Shutdown must stop admission, cancel in-flight application work, flush
bounded audit records, and exit within configured deadlines.

### 2. MCP schemas, cancellation, and audit bounds require application enforcement

**Confirmed.** Every tool has a valid JSON Schema object for `inputSchema`, defaulting
to JSON Schema 2020-12 when `$schema` is absent. `outputSchema` is optional; when
present, servers must return conforming structured results and clients should validate
them. The specification distinguishes JSON-RPC protocol errors from tool-execution
errors returned with `isError: true`. ([MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools))

**Confirmed.** Servers must validate tool inputs, enforce access controls, rate-limit
invocations, and sanitize outputs. Clients should apply timeouts, validate results,
confirm sensitive actions, and log tool use for audit. Lifecycle guidance calls for
per-request timeouts and a maximum timeout even when progress resets an idle timer.
([MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools),
[MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle))

**Inference.** Every Market Squawk schema should set bounded arrays/strings and reject
unknown properties where practical. Tool services must additionally enforce maximum
time ranges, instruments, rows, bytes, concurrent calls, and artifact size. Large
results should be written only beneath the controlled artifact directory and returned
by opaque reference. Audit entries should record request ID, tool/schema version,
caller/session, normalized-argument hash, start/end, status, cancellation, result
summary, and artifact reference—never credentials or unrestricted payloads.

**Confirmed.** Progress tokens are unique across active requests; progress must
increase, refer only to active work, be rate-limited, and stop after completion.
Cancellation references a request issued in the same direction and believed active;
clients cannot cancel `initialize`. Receivers should stop work, free resources, and
not respond, but must handle completion/cancellation races. Task-augmented requests
use `tasks/cancel`, not ordinary cancellation notifications.
([MCP progress](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress),
[MCP cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation))

**Inference.** Cancellation must propagate into query, file, and service tokens, but
is cooperative rather than proof that work stopped. A hard deadline remains mandatory;
late responses are discarded and terminal audit status is idempotent. MCP is entirely
outside the live event-to-action path and cannot submit unchecked orders or bypass the
shared risk service.

### 3. Coinbase qualification is channel-specific, not provider-wide

**Confirmed.** Coinbase `level2` begins with a full snapshot and then sends absolute
price-level sizes; zero size deletes a level. Coinbase says this channel guarantees
all updates. `level2_batch` groups updates every 50 ms, and its timestamp corresponds
to the latest message in the batch. ([Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels))

**Confirmed.** The `full` channel documents a race-safe initialization procedure:
subscribe, queue WebSocket messages, fetch a REST snapshot, discard queued sequence
numbers at or below the snapshot sequence, replay the rest, then process live updates.
Full-channel messages carry sequences. Heartbeats arrive every second with sequence
and last-trade IDs to detect missed data. The separate matches channel can drop
messages and directs consumers to heartbeat plus REST recovery.
([Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels))

**Confirmed.** The status channel publishes product identity, base/quote increments,
trading status, and restrictions such as post-only, limit-only, and cancel-only.
Auction quotes are interval-based indicative values and explicitly not firm.
([Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels))

**Inference.** Coinbase Exchange is direct single-venue coverage, not consolidated
market coverage. `full` can become a `DirectVerified` candidate only after snapshot/
sequence continuity, generation, timestamps, status, precision, freshness, and local
book invariants all pass. A gap, duplicate conflict, stale timestamp, crossed book, or
reconnect immediately quarantines the instrument until a fresh snapshot/replay.

**Inference.** The assigned page documents no checksum for Coinbase level2, and its
level2 payload examples show no sequence field. “Guaranteed delivery” does not replace
observable adapter validation. Level2 alone therefore should not receive execution
qualification under Market Squawk's default policy unless continuity is independently
established by a documented supported mechanism and tested. Heartbeat freshness is
connection health, not proof that market prices are fresh. Batched level2, ticker
batch, matches-only, and auction-indicative data should remain research/display or
restricted fallback inputs by default.

### 4. Kraken checksum validation is atomic and precision-sensitive

**Confirmed.** Kraken's v2 `book` update includes a CRC32 checksum over the top ten
levels, regardless of subscribed depth. Consumers first apply every price-level change
in the message, delete levels whose quantity is zero, truncate to subscribed depth,
and only then calculate the checksum. Kraken does not send zero-quantity updates for
levels that merely fall outside subscribed depth.
([Kraken checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2))

**Confirmed.** Price and quantity must be parsed as decimal or string to preserve
full precision. The checksum text orders asks low-to-high and bids high-to-low, takes
the top ten, removes decimal points and leading zeros from each price and quantity,
concatenates asks then bids, computes CRC32, casts to unsigned 32-bit, and compares
with the message checksum. ([Kraken checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2))

**Inference.** A message is one atomic validation unit: do not publish intermediate
book states or checksum after each contained change. Although Kraken calls validation
optional, Market Squawk should validate every update used for execution because the
venue supports checksums. Periodic validation may serve research but not default
`DirectVerified` action.

**Inference.** On mismatch, quarantine the book and connection generation before any
strategy observes the state; discard the book, resubscribe or reconnect, accept a new
snapshot, and require a passing checksum before promotion. A CRC match proves only
top-ten synchronization under Kraken's algorithm—not timestamp freshness, instrument
mapping, status, precision policy, or deeper-book correctness.

### 5. Concrete verification tests

**Inference.** Required deterministic tests from this evidence are:

- **MCP:** newline framing and stdout contamination; malformed/oversized frames;
  request-before-initialize and unnegotiated capability rejection; input/output schema
  failures; row/time/instrument/result bounds; progress uniqueness/monotonicity/flood
  limits; cancellation before, during, and after completion; late-response discard;
  shutdown escalation; audit completeness, redaction, and artifact containment.
- **Coinbase:** queued full-channel updates around REST snapshot; gap, duplicate, and
  out-of-order sequences; absolute level2 replacement and zero deletion; reconnect
  generation quarantine; heartbeat without price updates remains market-stale;
  status/precision changes revoke eligibility; matches-drop recovery; batch and
  auction messages never auto-promote to execution quality.
- **Kraken:** official checksum fixture; multiple changes applied before one checksum;
  ask/bid ordering; preserved trailing/leading zeros; decimal-vs-float regression;
  zero deletion and depth truncation; top-ten-only behavior; mismatch quarantine;
  old-generation updates rejected during resync; fresh snapshot plus passing checksum
  required for requalification.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| stdio is UTF-8, newline-delimited JSON-RPC with protocol-only stdout. | [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) | stdio requirements | High | Logs go to stderr |
| Initialization/version/capability negotiation precedes operation. | [MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) | lifecycle phases | High | Enforce a state machine |
| Tool inputs use JSON Schema; declared structured outputs must conform. | [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) | schema rules | High | Validate both directions |
| MCP requires validation, access control, rate limits, and output sanitization. | [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) | security considerations | High | App must define bounds |
| Progress is active-token scoped and monotonic. | [MCP progress](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress) | progress behavior | High | Rate-limit notifications |
| Cancellation is race-prone and initialize is not cancellable. | [MCP cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation) | cancellation behavior | High | Maintain hard deadlines |
| Coinbase level2 is snapshot plus absolute updates; zero deletes. | [Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | level2 channel | High | No checksum documented |
| Coinbase full uses queued sequence replay around a REST snapshot. | [Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | full-channel procedure | High | Candidate integrity path |
| Coinbase matches messages may be dropped. | [Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | matches warning | High | Not executable alone |
| Kraken checksum follows all changes in a message. | [Kraken guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | maintenance order | High | Atomic validation unit |
| Kraken requires decimal/string precision and fixed top-ten ordering. | [Kraken guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | CRC32 algorithm | High | Never parse through float |
| **Inference:** checksum failure requires quarantine and fresh resync. | [Kraken guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | checksum identifies desynchronization | High | Requalify only after validation |

## Source-Specific Notes

- `docs-042`: **Inference.** Protocol compliance does not create business
  authorization; shared services must still enforce access, risk, and result bounds.
- `docs-043`: **Confirmed.** Coverage is Coinbase Exchange channel data.
  **Inference.** Record it as single-venue coverage and qualify each channel separately.
- `docs-044`: **Confirmed.** Kraken permits periodic checksum checks.
  **Inference.** Market Squawk's stricter execution policy should check every update.

## Cross-Source Patterns

1. Provider or protocol assurances are inputs to validation, not substitutes for
   observable state transitions and fail-closed policy.
2. Lifecycle generations matter for both MCP requests and market books; stale work or
   frames must not cross a reconnect boundary.
3. Bounded schemas, exact decimals, state ownership, and explicit cancellation are
   necessary at every untrusted boundary.
4. MCP remains control-plane only; market qualification and risk cannot be delegated
   to an LLM or bypassed through a tool.

## Limitations and Non-Findings

- The Coinbase page documents no level2 checksum and shows no sequence in level2
  update payloads; this report does not infer one.
- Heartbeats detect feed gaps but do not establish price freshness.
- The Kraken guide does not prescribe resynchronization steps, sequence validation,
  freshness, or trading-status checks; the quarantine/resnapshot procedure is an
  explicit Market Squawk inference.
- CRC32 covers Kraken's top ten levels, not all subscribed depth or every execution
  eligibility condition.
- MCP supplies protocol schemas but no Market Squawk-specific row, byte, time-range,
  instrument, artifact, or concurrency limits; the application must set them.
- No latency or throughput claim is made, and no source outside the assigned families
  was reviewed.

## Source List

Official sources, accessed **2026-07-15**:

- `docs-042`: [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
  [lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle),
  [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools),
  [progress](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress),
  [cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation),
  [schema](https://modelcontextprotocol.io/specification/2025-11-25/schema).
- `docs-043`: [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels).
- `docs-044`: [Kraken WebSocket v2 book checksum](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2).
