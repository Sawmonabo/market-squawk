# MCP framing and source shutdown: primary-source record

Status: implementation research record

As-of date: 2026-07-16

Scope: Q2 Lane C MCP stdio framing, Tokio task ownership, Coinbase WebSocket cancellation, and
bounded diagnostics

## Table of contents

- [Purpose and evidence labels](#purpose-and-evidence-labels)
- [Implementation and dependency anchor](#implementation-and-dependency-anchor)
- [Official MCP facts](#official-mcp-facts)
- [Official Tokio facts](#official-tokio-facts)
- [Official WebSocket facts](#official-websocket-facts)
- [Contract-to-evidence map](#contract-to-evidence-map)
- [Limits and non-claims](#limits-and-non-claims)
- [Primary sources](#primary-sources)

## Purpose and evidence labels

This record preserves the primary-source basis for Lane C and separates two kinds of statements:

- **Source fact** means a normative specification statement or an upstream API behavior documented
  by its owner.
- **Market Squawk policy/inference** means a local safety decision derived from those facts. It is
  not presented as an MCP, Tokio, or WebSocket requirement.

That separation is material. MCP specifies newline-delimited stdio but does not specify a request
byte ceiling. Tokio specifies cooperative cancellation mechanics but does not promise a hard
real-time deadline for code that never yields. RFC 6455 describes a clean WebSocket close handshake,
whereas Market Squawk deliberately chooses immediate transport drop when local cancellation must
bound shutdown.

Only official or primary sources were used: the versioned MCP specification and schema, Tokio and
tokio-util rustdoc, tokio-tungstenite and tungstenite rustdoc/source, and RFC 6455. Third-party
tutorials, issue commentary, and performance claims are excluded.

## Implementation and dependency anchor

The local Git object database confirms that the following commits are integrated on the root
`feat/stage-1-foundation` branch:

| Commit | Integrated change |
| --- | --- |
| `a36e624d9aa0b97fe3042c23bf1ace67232c3b99` | `fix(mcp): bound stdio request framing` |
| `9911d2e8fe1d3bfb1316957db8f771d51bccb3a0` | `fix(app): bound source shutdown ownership` |
| `89b220bcac0fb4a9e22d9512ee2a13fcad466ff2` | `docs(app): disambiguate diagnostic authority` |
| `a7be854aa035ba1d5ed60a948ef55a67a1bfed79` | `fix(app): preserve deferred cleanup failures` |

The validated nonzero source-shutdown configuration used by the app is the integrated prerequisite
`e76080b968782fb35ac0ed1f8d78ed60a7f269f7` (`feat(platform): bound source shutdown
configuration`).

At this research base, the integrated branch's `Cargo.lock` resolves Tokio `1.52.4`, tokio-util
`0.7.18`, tokio-tungstenite `0.26.2`, and tungstenite `0.26.2`. Current upstream documentation was
also checked as of 2026-07-16 where it makes a cancellation guarantee—or the absence of one—more
explicit. An upstream-version change requires revalidation rather than assuming identical behavior.

This is a research and traceability record. It is not a Q2 checkpoint approval or a performance
claim.

## Official MCP facts

### Source facts

The stable MCP 2025-11-25 stdio transport says that the client and server exchange individual
JSON-RPC requests, notifications, or responses over stdin/stdout. It defines newline delimiters,
forbids embedded newlines inside a message, and requires stdout to contain only valid MCP messages.
It likewise requires client stdin to contain valid MCP messages
([MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)).

The corresponding schema defines an on-wire `JSONRPCMessage` as a request, notification, or
response. A response is either a result response or an error response; the error response contains
JSON-RPC version `2.0`, an optional request ID, and an error object
([MCP schema](https://modelcontextprotocol.io/specification/2025-11-25/schema)).

The official MCP repository identifies itself as the source of the specification, protocol schema,
and documentation, and publishes the stable 2025-11-25 release
([repository](https://github.com/modelcontextprotocol/modelcontextprotocol),
[release](https://github.com/modelcontextprotocol/modelcontextprotocol/releases/tag/2025-11-25)).

### Market Squawk policy/inference

MCP does **not** define `MAX_MCP_LINE_BYTES`, a maximum-plus-one detection allocation, behavior for
an over-limit frame, or recovery after such a frame. Lane C therefore makes the following local
resource-safety policy explicit:

1. Allocate exactly the configured maximum plus one detection byte for the request and a separate
   fixed reader scratch buffer.
2. Accept an exact-limit frame, including the tested newline and end-of-input cases.
3. Detect maximum-plus-one without materializing or draining an unbounded line.
4. Emit one bounded JSON-RPC error response, which remains a valid MCP stdout message.
5. Terminate that stdio session instead of draining an attacker-controlled remainder or guessing
   where protocol synchronization resumes.

The single error followed by termination is a fail-closed engineering choice, not an MCP mandate.
Termination is what keeps post-violation work bounded: resuming would first require finding a later
newline, and an untrusted peer can withhold it indefinitely. The implementation's end-of-input
acceptance is also compatibility behavior; the normative stdio representation remains
newline-delimited.

Commit `a36e624` implements this contract. Its tests cover exact maximum, maximum plus one at newline
and EOF, fragmented input, multiple frames, CRLF, empty frames, cancellation, and an instrumented
upper bound on both frame storage and requested read size. No throughput or latency conclusion is
drawn from those functional tests.

## Official Tokio facts

### Cancellation signalling and branch selection

`CancellationToken` is a cooperative signal. `cancel()` cancels existing child tokens and wakes
tasks waiting on `cancelled()`. The cancellation future completes immediately when the token was
already cancelled and is documented as cancellation-safe. The upstream documentation also notes
that propagation across a token tree is not atomic while `cancel()` is in progress, although all
children are cancelled when it returns
([tokio-util `CancellationToken`](https://docs.rs/tokio-util/0.7.18/tokio_util/sync/struct.CancellationToken.html)).

Tokio's `select!` waits on concurrent branches, returns the winning branch, and cancels the
remaining branch futures by dropping them. In biased mode, the programmer owns polling-order
fairness; the documentation specifically advises putting a shutdown future before a continuously
ready stream when shutdown must not be starved. It also lists stream `next()` and channel `recv()`
among cancellation-safe operations, while warning that cancellation safety must be assessed for
other operations
([Tokio `select!`](https://docs.rs/tokio/latest/tokio/macro.select.html)).

### Deadlines and task ownership

Tokio `timeout` returns an elapsed error and cancels the wrapped future when the duration expires.
It checks the deadline before polling the wrapped future, so a future that does not yield can run
past the duration without producing a timeout
([Tokio `timeout`](https://docs.rs/tokio/latest/tokio/time/fn.timeout.html)).

A `JoinHandle` is the owned permission to observe task termination. Dropping it detaches the task;
the task can continue and its return value becomes unavailable. Awaiting `&mut JoinHandle` is
cancellation-safe in `select!`, so losing the selection does not lose the task output. Tokio also
guarantees that task-local destructors have completed before completion is observed through the
awaited handle
([Tokio `JoinHandle`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html)).

`JoinHandle::abort()` schedules cancellation; it does not synchronously prove that cancellation has
finished. Tokio's task documentation directs callers to await the `JoinHandle` to observe final task
termination. A task may still finish normally if it completes before cancellation takes effect, and
`spawn_blocking` work generally cannot be aborted once running
([Tokio task cancellation](https://docs.rs/tokio/latest/tokio/task/index.html#cancellation)).

### Market Squawk policy/inference

Those APIs support, but do not themselves create, a complete ownership protocol. Commit `9911d2e`
therefore gives the application one `SupervisedSourceTask` owner that retains both the cancellation
capability and `JoinHandle`:

1. Signal cancellation.
2. Await cooperative source completion through a validated, nonzero deadline.
3. If the deadline elapses, call `abort()`.
4. Await the same handle after abort so task destruction is observed and the handle is not detached.
5. Return a typed `Graceful`, `AbortedAtDeadline`, or `TaskFailed` outcome before proceeding to the
   event processor and capture writer.

The supervisor independently selects cancellation against the entire source-session future and the
reconnect backoff. Adapter-level blocking points are also cancellation-raced. These two layers are
deliberate: adapter checks provide prompt cooperative exit, while the owned task and abort-and-await
sequence are the final lifecycle backstop.

This is a bounded **cooperative async shutdown** contract, not a hard real-time preemption claim.
Code that blocks the executor thread or never reaches a yield point can defeat Tokio's timeout and
abort progress. Source implementations must remain asynchronous and nonblocking; blocking work
requires a separately owned boundary with its own termination semantics.

Commit `a7be854` closes an adjacent diagnostic path: even when a deferred capture reap invariant
fails, the application retains and composes the original pipeline failure instead of replacing it
with the cleanup error.

## Official WebSocket facts

### Protocol and library source facts

RFC 6455 requires an endpoint that receives Ping to answer with Pong unless it has already received
Close, and the Pong response carries the Ping application data. An endpoint receiving Close, when
it has not already sent Close, must send a Close response. The RFC distinguishes a completed closing
handshake from an underlying transport that closes without one
([RFC 6455, sections 5.5.1–5.5.3](https://www.rfc-editor.org/rfc/rfc6455#section-5.5.1)).

The same RFC says a Close reason is not necessarily human-readable and must not be shown directly to
end users. That statement is specific to the Close reason; it is not a general logging or redaction
standard for every adapter error
([RFC 6455, section 5.5.1](https://www.rfc-editor.org/rfc/rfc6455#section-5.5.1)).

tokio-tungstenite exposes a completed WebSocket as both a `Stream` and a `Sink`. Current official
rustdoc explicitly documents stream reads as cancellation-safe and states that the Sink side has no
documented cancellation-safety guarantee
([`WebSocketStream`](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/struct.WebSocketStream.html)).

Tungstenite documents that reading Ping or Close queues the corresponding protocol response. The
queued response is driven by later read, write, or flush activity. It documents `send` as write plus
flush, and a clean Close handshake as potentially requiring continued read or flush activity; for a
client, completion can wait for the server to close the underlying connection. The upstream
documentation also advises applications not to send a manual Pong for a received Ping because
tungstenite already queues it
([tungstenite `WebSocket`](https://docs.rs/tungstenite/latest/tungstenite/protocol/struct.WebSocket.html)).

### Market Squawk policy/inference

The provider-control contract has two distinct paths:

- For a provider Ping or provider-initiated Close, the current adapter enters an outbound
  control-write path and races that future against cancellation. Tests inject a Sink that never
  becomes ready and prove that cancellation wins without leaving the source owner blocked.
- For local client cancellation, the adapter does not initiate and wait for a clean Close handshake.
  It exits the connection future and drops the transport immediately, after which the source task is
  joined by its owner.

The immediate-drop policy prioritizes the local shutdown deadline over clean WebSocket closure. It
must be described as transport termination, not as a successful RFC 6455 closing handshake. The
integration test verifies that local cancellation does not wait for or emit a client Close before
the peer observes transport loss.

There is an important upstream interaction: tungstenite itself queues Ping and Close responses, and
its current documentation discourages manually answering a received Ping. Consequently, the Lane C
tests establish the **bounded cancellation of the explicit control-write path**; they do not prove
that this path is required by tungstenite, is the sole mechanism producing a wire response, or wins
a race with cancellation. A future refactor may drive tungstenite's queued response with a
cancellation-raced flush/read instead, but only with deterministic provider-Ping, provider-Close,
and stalled-write tests against the pinned library version. Until then, no documentation should
attribute automatic protocol behavior solely to Market Squawk's explicit send.

Commit `9911d2e` also races cancellation around connect, subscription, source-status and event
sends, socket reads, and reconnect sleep. This prevents a full channel or stalled transport from
defeating ordinary cooperative shutdown. The current sink contract does not support retry after a
cancelled send; cancellation exits the connection and drops its state.

## Contract-to-evidence map

| Implemented contract | Official fact | Market Squawk policy/inference | Integrated evidence |
| --- | --- | --- | --- |
| Maximum-plus-one bounded MCP framing | MCP stdio messages are newline-delimited valid JSON-RPC messages | Fixed request storage is maximum plus one detection byte, plus fixed reader scratch | `a36e624`; instrumented framing tests |
| One bounded error, then stdio termination | MCP permits JSON-RPC error responses and forbids non-MCP stdout | Do not unboundedly drain or guess resynchronization after oversize input | `a36e624`; oversize-session test |
| Cancellation-aware session operations | Tokens wake cancellation waiters; `select!` drops losing branches; stream reads are cancellation-safe | Put cancellation first and race the whole session plus each blocking adapter boundary | `9911d2e`; handshake, full-channel, reconnect, and control-write tests |
| Client cancellation immediately drops transport | A clean WebSocket client close can require continued I/O and server transport close | Local shutdown does not wait for a Close write; it drops transport and joins the owner | `9911d2e`; local WebSocket integration assertion |
| Provider Pong/Close control path is cancellation-raced | RFC 6455 requires responses; tungstenite queues automatic responses; Sink send lacks a documented cancellation guarantee | Bound the outbound control-write wait; cancellation may terminate before wire completion | `9911d2e`; stalled Pong and Close Sink tests |
| Deadline aborts and awaits | Timeout is cooperative; handle drop detaches; abort schedules; handle await observes termination | Retain one owner, abort after deadline, then await and classify | `9911d2e` plus config prerequisite `e76080b` |
| No raw source diagnostics in retained task outcomes | RFC forbids directly showing untrusted Close reasons to end users, but does not define a general adapter-redaction policy | Discard raw provider/transport/join text at the retained boundary; expose a typed kind and fixed bounded detail | `9911d2e`; sentinel-secret regression test |
| Public compatibility state cannot claim production authority | No external protocol source can establish Market Squawk execution authority | Describe app-local state as diagnostic, authority-free, single-venue partial coverage, and paper-only | `89b220b`; CLI/MCP contract tests |
| Cleanup failures do not erase primary failures | Tokio ownership semantics require observing owned work, but do not prescribe application error composition | Compose deferred capture-reap failures with the retained primary/source result | `a7be854`; deferred-reap regression test |

The “no raw source diagnostics” rule deserves a precise boundary. It does not suppress local,
bounded operator diagnostics generally. It prevents remote payloads, endpoint strings, arbitrary
error chains, panic payloads, or provider Close reasons from becoming the stable retained
`SourceTaskFailure` detail or public status text. The current stable details identify only source,
join, or lifecycle failure classes. More diagnostic depth requires a separately bounded and
redacted local observability contract; it must not be added by formatting arbitrary source errors.

## Limits and non-claims

- No source reviewed specifies Market Squawk's request-size or shutdown-duration constants.
- “Bounded” refers to the explicit memory and ownership contracts tested by Lane C. It is not a
  throughput, p99 latency, or hard real-time claim.
- Tokio cancellation is cooperative. `abort()` plus await is an ownership guarantee for async tasks
  that yield; it cannot forcibly preempt arbitrary blocking code.
- The MCP oversize response does not make the oversized input valid. It is one valid bounded server
  error followed by deliberate session termination.
- Immediate client transport drop is intentionally not described as clean WebSocket closure.
- The stalled Sink tests prove cancellation at the outbound operation boundary. They do not by
  themselves prove a Pong or Close frame reached the peer.
- The official tungstenite behavior and current advisory about automatic control responses must be
  rechecked on any tokio-tungstenite/tungstenite upgrade.
- Raw diagnostic redaction is Market Squawk policy. RFC 6455 directly supports treating Close reason
  text as unsuitable for end-user display, but does not prescribe the complete source-error model.

## Primary sources

All sources were accessed on 2026-07-16.

1. Model Context Protocol, [2025-11-25 transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports).
2. Model Context Protocol, [2025-11-25 schema reference](https://modelcontextprotocol.io/specification/2025-11-25/schema).
3. Model Context Protocol, [official repository](https://github.com/modelcontextprotocol/modelcontextprotocol) and [2025-11-25 release](https://github.com/modelcontextprotocol/modelcontextprotocol/releases/tag/2025-11-25).
4. Tokio-util, [`CancellationToken` 0.7.18](https://docs.rs/tokio-util/0.7.18/tokio_util/sync/struct.CancellationToken.html).
5. Tokio, [`select!`](https://docs.rs/tokio/latest/tokio/macro.select.html).
6. Tokio, [`time::timeout`](https://docs.rs/tokio/latest/tokio/time/fn.timeout.html).
7. Tokio, [`JoinHandle`](https://docs.rs/tokio/latest/tokio/task/struct.JoinHandle.html) and [task cancellation](https://docs.rs/tokio/latest/tokio/task/index.html#cancellation).
8. Tokio, [official repository](https://github.com/tokio-rs/tokio).
9. tokio-tungstenite, [`WebSocketStream`](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/struct.WebSocketStream.html) and [official repository](https://github.com/snapview/tokio-tungstenite).
10. Tungstenite, [`WebSocket`](https://docs.rs/tungstenite/latest/tungstenite/protocol/struct.WebSocket.html).
11. IETF, [RFC 6455: The WebSocket Protocol](https://www.rfc-editor.org/rfc/rfc6455).
