# HTTP source-client policy decision

Date: 2026-07-16

## Question

Which client behaviors must source adapters make explicit so endpoint allowlists and provider-wide
budgets remain authoritative?

## Decision

Source clients must use a checked profile with these fail-closed defaults:

- Automatic redirects are disabled unless every resolved target is reauthorized against the same
  structural endpoint policy and the redirect count remains bounded.
- Ambient system proxies are disabled. A legitimate proxy requires a separate explicit,
  allowlisted configuration; rotating proxies or using them to evade blocking remains prohibited.
- Implicit client retries are disabled unless every physical attempt reserves the same shared
  provider/account budget and the method is safe under the adapter's idempotency policy.
- Automatic `Referer` generation is disabled for source API clients.
- User information and fragments are rejected. Scheme, host, effective port, path policy, and
  allowlisted query keys are validated structurally rather than with string prefixes.
- Sensitive query/header values are never retained by endpoint-policy values, `Debug`, errors, or
  tracing. Sensitive headers are not forwarded across authority changes.
- Connect, read, total, redirect, record, and response limits are explicit. The response-byte
  limit is enforced while streaming the representation delivered to the parser; `Content-Length`
  is only an early rejection hint and cannot be the sole limit.
- Provider cooldown and fixed-window enforcement use a monotonic runtime clock. An absolute
  HTTP-date `Retry-After` value is converted once using paired wall/monotonic observations; wall
  clock changes cannot reset a request window early.
- The budget authority is minted by one scope-keyed coordinator. A scope structurally binds the
  provider and a non-secret authorization/account reference; workers cannot publicly construct a
  second counter for the same scope. A later existing cooldown always wins over a shorter new
  refusal, and an invalid or over-policy `Retry-After` creates an explicit fail-closed blocking
  state rather than returning an error while leaving the next acquisition available.

Reqwest 0.13.4 documents automatic redirect following by default, with a ten-hop limit, and an
explicit `Policy::none`. Its client builder documentation also states that system proxy environment
variables are used by default and that the default retry behavior includes protocol NACKs. Those
defaults are convenient for general clients but are not safe implicit behavior for an
allowlist-and-budget-enforced source adapter.

RFC 9110 defines `Retry-After` as either an HTTP-date or a non-negative delay in seconds. Both forms
need bounded parsing; neither grants permission to rotate accounts, endpoints, identities, or
proxies.

Rust 1.97 documents `std::time::Instant` as monotonically nondecreasing and opaque. It also warns
that direct instant arithmetic can panic when a result is not representable, while
`checked_add`/`checked_duration_since` expose failure explicitly. Production budget enforcement
therefore uses checked monotonic arithmetic and fails closed on an unrepresentable deadline or
clock-order violation. Tests use an injected deterministic monotonic-clock interface rather than
sleeping or accepting caller-authored timestamps as enforcement truth.

## Implementation refresh: 2026-07-20

Coinbase and Kraken WebSocket handshake refusals now use one sources-layer parser before mutating
their shared provider budget. The parser accepts at most 128 ASCII bytes. Positive decimal seconds
use checked conversion to nanoseconds; standard HTTP dates become an absolute wall-clock
instruction that the budget converts once against its paired monotonic observation. Missing,
invalid, zero, non-ASCII, or representation-overflowing fields use the same capped refusal policy.
A syntactically valid instruction beyond the configured maximum remains fail-closed.

The date parser is pinned exactly to `httpdate` 1.0.3. Its published API returns `SystemTime` and
supports preferred IMF-fixdate plus the legacy HTTP date forms; its source forbids unsafe code and
declares `MIT OR Apache-2.0`, matching this workspace's license policy. The helper exposes no HTTP
client and cannot select alternate accounts, identities, endpoints, or proxies.

## Primary sources

- [Reqwest 0.13.4 redirect module](https://docs.rs/reqwest/0.13.4/reqwest/redirect/)
- [`reqwest::redirect::Policy`](https://docs.rs/reqwest/0.13.4/reqwest/redirect/struct.Policy.html)
- [`reqwest::ClientBuilder`](https://docs.rs/reqwest/0.13.4/reqwest/struct.ClientBuilder.html)
- [Reqwest sensitive-header redirect implementation](https://docs.rs/reqwest/0.13.4/src/reqwest/redirect.rs.html)
- [`url::Url` structured path/query access](https://docs.rs/url/latest/url/struct.Url.html)
- [Rust 1.97 `std::time::Instant`](https://doc.rust-lang.org/1.97.0/std/time/struct.Instant.html)
- [Tokio 1.52.3 paused-time testing](https://docs.rs/tokio/1.52.3/tokio/time/fn.pause.html)
- [RFC 9110, HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110.html)
- [`httpdate` 1.0.3 `parse_http_date`](https://docs.rs/httpdate/1.0.3/httpdate/fn.parse_http_date.html)
- [`httpdate` 1.0.3 published source](https://docs.rs/crate/httpdate/1.0.3/source/)
- [`httpdate` upstream repository](https://github.com/pyfisch/httpdate)

## Required tests for adapters

- Cross-origin and scheme-changing redirects fail unless an explicit rule authorizes the complete
  target; credentials are absent on the redirected attempt.
- `/api` descendant rules do not authorize `/apievil`; encoded traversal and ambiguous authority
  forms fail.
- Unknown, duplicate-forbidden, excessive, or oversized query parameters fail without logging
  values.
- A compressed or chunked response exceeding the delivered-byte ceiling is stopped before parser
  handoff.
- Retry, redirect, and transport-level repeated attempts consume one shared budget per physical
  attempt.
- Two handles requested for the same provider/authorization scope share concurrency, fixed-window,
  refusal-attempt, and cooldown state; a caller cannot mint an independent counter.
- A shorter subsequent cooldown never truncates a later existing cooldown. Invalid or excessive
  `Retry-After` values block acquisition according to explicit fail-closed policy.
- Monotonic checked-add overflow and a simulated backward monotonic reading fail closed without a
  panic.
- Forward/backward wall-clock adjustments do not increase permitted request throughput.
