# Q2 Task 5 routing and frame-evidence follow-up

Date: 2026-07-16

## Finding

The original current-authority handoff rejected a decoded provider frame containing more than one
`(venue, instrument)` pair. That contradicted the frozen Task 5/7/8 contract: a bounded wire frame
may contain multiple instruments and must be split into homogeneous shard batches after current
scope validation. The handoff also discarded `DecoderEvidence`, so Task 7 could not retain the
exact admitted frame ordinal, receive time, payload digest, binding allocation, and decoder rule.

## Resolution

- `DecodedProviderBatch` remains a bounded representation of one wire frame and permits mixed
  routing scopes while still rejecting empty and over-expanded frames.
- `ValidatedCurrentSourceAuthority::validate_decoded_batch_owned` validates every observation and
  then groups intact current observations by `CurrentBatchKey(venue, instrument)`.
- Grouping preserves first-key order and original wire order within each key.
- `CurrentFrameEvidence` shares the exact receipt-validated `DecoderEvidence` allocation across
  routed observations without exposing a constructor or Serde surface.
- Each routed batch retains compact policy and current-authority leases and conservatively charges
  its provider data, frame allocation, authority allocation, and structural memory for independent
  bounded-queue admission.
- Compile-time assertions keep routed collections and batches `Send`, keep authority envelopes
  non-`Clone`/non-Serde, and keep frame evidence non-Serde.

## Adversarial coverage

The registry integration fixture uses the maximum 4,096-instrument metadata universe and one wire
frame ordered as instrument A, instrument B, instrument A. It proves two homogeneous batches are
returned in first-key order, the A observations remain in their original relative order, retained
memory remains below 128 KiB for the tested A batch, and the exact binding, frame ID, receive time,
payload digest, and decoder rule survive the handoff.

## Verification

```text
cargo fmt --all --check
cargo test -p market-squawk-sources --all-features --locked
cargo clippy -p market-squawk-sources --all-targets --all-features --locked -- -D warnings
git diff --check
```

All commands passed on the documented commit candidate. The workspace-wide locked gates remain a
Task 8 quarter-checkpoint responsibility after the capture bridge and live lanes are integrated.
