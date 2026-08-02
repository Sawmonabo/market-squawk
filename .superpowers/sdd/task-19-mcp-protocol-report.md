# Task 19 MCP protocol-contract report

Date: 2026-07-26

Status: implementation complete; root exact-head verification remains the integration refresh gate.

## Audit identity

- Audit base: `998c8d058efbbc709ca8d0f3109db0d55b0b76a1`
- Owned implementation commit:
  `26e8095f9c96976208039359cfa33997048e7691`
- Branch: `feature/control-plane-acceptance`
- Refresh gate: the focused Cargo evidence below predates the final narrow legacy-compatibility
  adjustment and post-dispatch inline-resource normalization. The root lane explicitly owns the
  final exact-head focused gates. This report does not present the earlier evidence as release
  approval for a changed head.

## Outcome

The generic MCP adapter now advertises and enforces descriptor-owned structured-result contracts.
All 62 operations in the production application registry use code-owned, operation-specific data
schemas. The descriptor layer composes those data schemas into the exact MCP
`structuredContent` envelope, validates service data before publication, and exposes the complete
schema through `rmcp` as `outputSchema`.

Once a structurally valid, registered tool call reaches service dispatch, actionable business/API
failures are returned as bounded, redacted MCP tool results with `isError: true`. Protocol
admission failures and exceptional host conditions remain JSON-RPC errors. Tool-error results are
audited as failures rather than successes.

## Owned changes

- `apps/market-squawk/src/application/contracts.rs`
- `apps/market-squawk/src/application/contracts/output.rs`
- `crates/market-squawk-services/src/lib.rs`
- `crates/market-squawk-services/src/output_schema.rs`
- `crates/market-squawk-services/src/response.rs`
- `crates/market-squawk-services/src/traits.rs`
- `crates/market-squawk-mcp/src/framing.rs`
- `crates/market-squawk-mcp/src/server.rs`
- `crates/market-squawk-mcp/tests/hostile_boundaries.rs`
- `crates/market-squawk-mcp/tests/lifecycle_protocol.rs`

No manifest, lockfile, application-composition root, script, release document, or unrelated dirty
path was staged in the implementation commit.

## Structured-result contract

### Central envelope

`market-squawk-services` owns one Draft 2020-12 schema composer and bounded validator.

The inline variant is a closed object with exactly:

```text
{
  data: <operation-specific schema>,
  metadata: {
    completeness,
    returnedItems,
    availableItems,
    sourceCoverage,
    dataQuality
  }
}
```

Descriptors whose result policy permits opaque overflow also advertise a second closed variant:

```text
{
  artifact: {
    id,
    sha256,
    byteCount,
    mediaType
  },
  metadata: <same canonical metadata schema>
}
```

The artifact reference remains path-free. Both variants reject additional envelope fields.
Metadata field names and types match the canonical `TypedToolResult` serialization.

### Production operation families

`application/contracts/output.rs` is the single code-owned registry for all 62 production
operations. It consolidates repeated schema shapes into source, market, research/query, portfolio,
analysis, model, fair-value, bot, and execution families rather than copying a validator per tool.
The registry-completeness test walks every `OPERATION_SPECS` entry and then composes the complete
application capability set.

The review reconciled concrete wire representations that are easy to misstate:

- `Execution.Cancel.cumulativeFees` is the serialized `Money` object with string `amount` and
  string `currency`.
- `observedAt` is the integer wire representation of `Timestamp`.
- price ticks, quantity lots, counts, and sequence values use their transparent integer wire
  representations.
- nullable rows and nullable fields are explicit `oneOf` families.

The supported-schema admission rejects a nonspecific operation root such as
`{"type":"object"}`. It admits only the bounded keyword subset implemented by the runtime
validator, checks required/property consistency, and rejects unknown schema keywords.

### Publication enforcement

The publication path is:

```text
operation registry
  -> ToolDescriptor::try_new_with_output
  -> supported-schema admission and bounded envelope composition
  -> rmcp Tool.outputSchema
  -> TypedToolResult::validate_for(descriptor)
  -> inline envelope or path-free artifact envelope
```

`TypedToolResult::validate_for` now verifies both source-evidence policy and the operation data
schema before either inline or artifact publication. A mismatch is an exceptional invalid service
result and remains JSON-RPC error `-32010`; no artifact is published.

The existing `ToolDescriptor::try_new` constructor remains a compatibility surface for five
diagnostic-only descriptors outside the 62-operation production registry. It now uses JSON
Schema's explicit `true` data sub-schema inside the same closed, bounded canonical envelope. This
preserves their existing non-null diagnostic outputs without weakening production descriptors:
all production operations use `try_new_with_output` and pass the specific-schema admission gate.

## Error boundary

| Boundary/outcome | Protocol representation | Audit result |
| --- | --- | --- |
| Malformed JSON-RPC, unknown method/tool, invalid descriptor arguments | JSON-RPC error | Existing protocol/service-rejected class |
| Dispatched `InvalidRequest` | `CallToolResult`, `isError: true`, static text | `ServiceRejected` |
| Dispatched `NotFound` | `CallToolResult`, `isError: true`, static text | `ServiceRejected` |
| Dispatched `Unauthorized` | `CallToolResult`, `isError: true`, static text | `ServiceRejected` |
| Dispatched `ResourceExhausted`, including inline-only result overflow | `CallToolResult`, `isError: true`, static text | `ServiceRejected` |
| Dispatched `Unavailable` | `CallToolResult`, `isError: true`, static text | `ServiceRejected` |
| Cancellation | Exceptional JSON-RPC cancellation path; existing duplicate-response suppression retained | `Cancelled` |
| Host/service deadline | Exceptional JSON-RPC `-32008` | `DeadlineExceeded` |
| Invalid service result | Exceptional JSON-RPC `-32010` | `ResourceExhausted` under the existing stable audit mapping |
| Internal/host/artifact invariant failure | Exceptional JSON-RPC internal path | Existing exceptional classification |

Tool-error messages are fixed strings selected from the stable `ServiceErrorClass`. Provider text,
raw errors, credentials, filesystem paths, request payloads, and internal diagnostics are never
copied into the protocol response.

The audit classifier checks `CallToolResult.isError == true` before success and artifact
classification, preventing a known service rejection from being recorded as `Succeeded`.

## TDD evidence

### Tool-error boundary

RED:

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test hostile_boundaries \
  dispatched_service_failure_is_a_redacted_tool_error_and_audited_as_failure \
  --locked -- --exact --nocapture
```

Result: exit 101. The known dispatched service failure was still returned as a JSON-RPC error.

GREEN:

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test hostile_boundaries \
  dispatched_service_failure_is_a_redacted_tool_error_and_audited_as_failure \
  --locked -- --exact --nocapture
```

Result: 1 passed. The response had no JSON-RPC `error`, had `isError: true`, contained only the
static text `service is unavailable`, exposed no structured provider/path payload, and the tool
audit contained `ServiceRejected` but not `Succeeded`.

### Output-schema advertisement

RED:

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test lifecycle_protocol \
  duplicate_active_ids_are_rejected_and_cancellation_reaches_the_service \
  --locked -- --exact --nocapture
```

Result: exit 101 at `lifecycle_protocol.rs:952`; the advertised `outputSchema.type` was null
instead of `object`.

GREEN:

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test lifecycle_protocol \
  duplicate_active_ids_are_rejected_and_cancellation_reaches_the_service \
  --locked -- --exact --nocapture
```

Result: 1 passed. Every listed descriptor advertised the closed canonical result variants.

## Focused verification evidence

The following checks passed before the final refresh-gated compatibility adjustment:

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-services --lib --locked
```

Result: 1 passed.

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk --lib \
  application::contracts::output::tests::every_production_operation_has_a_code_owned_data_contract \
  --locked -- --exact --nocapture
```

Result: 1 passed; 47 filtered. The application composed all 62 production descriptors.

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test hostile_boundaries \
  deadline_and_large_output_fail_closed_or_return_an_opaque_artifact \
  --locked -- --exact --nocapture
```

Result: 1 passed, including descriptor-output rejection with JSON-RPC `-32010` and no additional
artifact publication.

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test lifecycle_protocol --locked
```

Result: 5 passed.

```bash
CARGO_INCREMENTAL=0 cargo test -p market-squawk-mcp --test hostile_boundaries --locked
```

Result: 11 passed.

The first full hostile-boundary run passed 10 of 11 tests. The sole failure was the fixed 20 KiB
CRLF fragmentation fixture: adding schemas to the multi-tool capability response correctly made
that unrelated fixture exceed its configured frame ceiling. The fixture was narrowed to the
single-tool `TraceService`, preserving its exact delimiter/framing purpose. The subsequent full
run passed 11 of 11.

```bash
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk-services -p market-squawk-mcp \
  --all-targets --locked -- -D warnings
```

Result: passed.

```bash
rustfmt --edition 2024 --check \
  apps/market-squawk/src/application/contracts.rs \
  apps/market-squawk/src/application/contracts/output.rs \
  crates/market-squawk-services/src/lib.rs \
  crates/market-squawk-services/src/output_schema.rs \
  crates/market-squawk-services/src/response.rs \
  crates/market-squawk-services/src/traits.rs \
  crates/market-squawk-mcp/src/framing.rs \
  crates/market-squawk-mcp/src/server.rs \
  crates/market-squawk-mcp/tests/hostile_boundaries.rs \
  crates/market-squawk-mcp/tests/lifecycle_protocol.rs
```

Result: passed after the final narrow adjustments.

```bash
git diff --check -- \
  apps/market-squawk/src/application/contracts.rs \
  apps/market-squawk/src/application/contracts/output.rs \
  crates/market-squawk-services \
  crates/market-squawk-mcp
```

Result: passed after the final narrow adjustments.

### External dirty-tree blockers observed

The focused application Clippy command:

```bash
CARGO_INCREMENTAL=0 cargo clippy -p market-squawk --lib --locked -- -D warnings
```

failed only on the root-owned existing `apps/market-squawk/src/application.rs:211`
`clippy::too_many_arguments` diagnostic. It emitted no diagnostic for the owned contracts files.

`cargo fmt --all --check` failed only on concurrently root-owned changes in:

- `apps/market-squawk/src/doctor.rs`
- `apps/market-squawk/src/mcp.rs`
- `apps/market-squawk/src/main.rs`
- `apps/market-squawk/tests/harnesses/control_plane.rs`

The root lane acknowledged and owns those formatting changes. The scoped owned-path Rustfmt check
is green.

## Resource discipline

- Shared `target/` before this lane's Cargo phases: 3,845,312 KiB.
- Shared `target/` after the final Cargo phase: 5,240,664 KiB.
- No `CARGO_TARGET_DIR` override was used.
- Every Cargo phase used `CARGO_INCREMENTAL=0`.
- Cargo phases were serialized after checking for an existing Cargo process.
- The target remained below the 10 GiB stop threshold.

## Self-review

- Schema review: all 62 registry names resolve to a specific code-owned schema; canonical envelope
  fields match actual inline and artifact serialization; the concrete Cancel/Money and transparent
  numeric representations were reconciled.
- Response review: operation data is validated before either publication route; known actionable
  failures use fixed bounded text; no raw error is rendered.
- Audit review: `isError` is classified before ordinary success/artifact results.
- Cancellation review: request child-token ownership, biased cancellation selection, progress
  closure, and the existing no-second-response suppression were not weakened.
- Blast-radius review: existing diagnostic-only callers of the legacy descriptor constructor were
  found and preserved through an explicit permissive compatibility sub-schema; production
  descriptor specificity remains mandatory.
- Ownership review: only the ten assigned implementation/test paths were included in
  `26e8095f9c96976208039359cfa33997048e7691`.

No substantiated Critical, Important, or Minor finding remains in the owned implementation. Exact
head approval is deliberately deferred to the root refresh gate identified above.

## Protocol references

- MCP 2025-11-25 tools:
  <https://modelcontextprotocol.io/specification/2025-11-25/server/tools>
- MCP 2025-11-25 schema:
  <https://modelcontextprotocol.io/specification/2025-11-25/schema>

The implementation follows the specification distinction between protocol-level failures and
tool-execution failures, and binds advertised `outputSchema` to returned `structuredContent`.
