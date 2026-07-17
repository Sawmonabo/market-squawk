# Authority lifecycle model-checking decision

Date: 2026-07-17

Status: Accepted for Q2 A3 remediation; implementation and exact-head verification pending

## Context

The restart-durable provider-budget session uses one packed atomic lifecycle word containing a
monotonic phase and the exact number of admitted authority-changing operations. Clean shutdown may
write `Clean` only after winning `Active, 0 -> Closing, 0`; an admitted fatal operation may instead
latch terminal state, and exactly one caller may own terminal persistence.

This state machine has already exposed a real close-versus-terminal race. Deterministic schedule
tests remain required, but scheduler tests alone do not exhaust the atomic interleavings or validate
the checked bit-packing algebra.

## Decision

1. Extract a checked, pure `LifecycleWord` transition kernel from the atomic retry loops.
2. Use `proptest` to compare bounded action sequences against a small reference model.
3. Use Loom 0.7.x through a target-specific `cfg(loom)` dependency and a tiny private atomic-word
   adapter so the Loom tests execute the same transition operations used by production, rather than
   a lookalike state machine.
4. Keep Loom outside the deterministic default suite and run it as an explicit CI/audit job with a
   bounded thread count and preemption/permutation budget. It is a required Q2 authority gate, not a
   substitute for ordinary tests.
5. Retain `Acquire` reads and `AcqRel` state transitions until model evidence and benchmarks justify
   any weakening.

The intended dependency shape follows the upstream project guidance:

```toml
[target.'cfg(loom)'.dependencies]
loom = "0.7"
```

The workspace must register `cfg(loom)` with Cargo/rustc check-cfg so strict `-D warnings` builds do
not rely on a blanket `unexpected_cfgs` exception.

An explicit model command will use this form:

```bash
RUSTFLAGS="--cfg loom" \
LOOM_MAX_PREEMPTIONS=3 \
cargo test \
  -p market-squawk-sources \
  --test authority_lifecycle_loom \
  --release \
  --locked \
  -- \
  --test-threads=1
```

The exact bound must be recorded with the run evidence. A bounded run must never be described as an
exhaustive proof beyond that bound.

## Required properties

The pure transition tests must prove:

- packing and decoding preserve every valid phase and admission count;
- count arithmetic never wraps, truncates, or underflows;
- only `Active` accepts a new admission;
- `Closing` is reachable only from exactly `Active, 0`;
- no state returns to `Active` after leaving it;
- admission release preserves the current phase;
- at most one terminal writer claim succeeds;
- terminal success and failure are stable and never retry persistence;
- `Closed` is reachable only after the exclusive clean-close claim;
- reserved phase encoding `7` is never produced by a valid transition sequence.

The Loom suite must cover at least:

- admitted fatal operation versus clean close;
- admitted normal write versus clean close;
- permit-lifetime admission versus clean close;
- multiple admitted fatal peers contending for one writer claim;
- admission release racing terminal writer claim and completion;
- close winning before a stale admission attempt;
- saturated admission racing an admission release.

## Limits and interpretation

Loom explores permutations only for synchronization operations implemented with its replacement
types. Any standard-library atomic left behind the adapter is invisible to the model. The upstream
documentation also notes that Loom does not implement the complete C11 memory model: sequentially
consistent accesses are treated more weakly, and some load-buffering executions are not explored.
Passing Loom is therefore evidence for the modeled operations and configured bounds, not a universal
concurrency proof.

## Primary sources

- Loom upstream repository and quickstart: <https://github.com/tokio-rs/loom>
- Loom 0.7.2 crate documentation: <https://docs.rs/loom/0.7.2/loom/>
- Loom model builder and exploration bounds: <https://docs.rs/loom/0.7.2/loom/model/struct.Builder.html>
- Loom synchronization replacement types: <https://docs.rs/loom/0.7.2/loom/sync/>
