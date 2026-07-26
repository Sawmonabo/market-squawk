# Changelog

All notable changes to this project are documented here.

## Unreleased

- Established the Rust 1.97.1 Edition 2024 virtual workspace and shared invariant-preserving
  domain crate.
- Rejected Rust 1.97.0 for release evidence after the Rust project classified an x86-64 LLVM
  miscompilation as critical; CI, the pinned toolchain, and the workspace MSRV now require the
  corrective 1.97.1 point release.
- Separated fair-value hierarchy, market depth, data quality, integrity, and provenance contracts.
- Added exact tick, lot, money, and notional conversions with checked arithmetic and property tests.
- Added verified instrument identifiers, network-qualified digital-asset addresses, futures identity,
  venue-scoped symbol changes, and explicit asset-versus-currency denomination.
- Migrated authoritative identity wires to `ExactPayloadEvidence` and
  `RevisionBoundPayloadEvidence`; legacy `source_reference` shapes are rejected, and
  `InstrumentDefinition` now serializes its checked provider state as `provider_identity_registry`
  rather than separate provider-identity and conflict vectors.
- Added generation-bound timing, sequence, checksum, coverage, and live qualification evidence;
  persisted quality assertions require current runtime requalification before executable use.
- Kept live and research provenance independent, including point-in-time publication, revision, and
  unknown-availability semantics for research records.
- Made current journal writes use only `MSJ1/.msj`, retained bounded read-only compatibility for the
  legacy journal format, and made dual-format selection fail closed until explicitly resolved.
- Hardened local and CI verification with exact dependency inventory, exact compatibility-brand
  allowances, immutable GitHub Action commits, strict workspace gates, and bounded offline smokes.
- Added an exact-head, offline all-vertical release demonstration over production live/model/risk/
  paper kernels, Arrow/Parquet/DataFusion and point-in-time storage, Python admission, backtesting,
  portfolio, fair value, CLI, doctor, and the shipping stdio MCP composition; strict release closure
  now requires its immutable evidence and fails closed on stopped paper authority.

## 0.1.0 - 2026-07-15

- Added public Coinbase Exchange Level 2, heartbeat, and match capture.
- Added acknowledged CRC-framed raw journaling with corruption, truncation, and concurrent-writer detection.
- Added fail-closed order-book quality, staleness, reconnect, and crossed-book handling.
- Added fixed-point online features, deterministic paper strategy evaluation, and risk limits.
- Added journal replay through the same Coinbase decoder and engine state transitions.
- Added bounded local MCP tools over stdio with strict arguments and rate limiting.
- Added local CLI, offline mock source, tests, verification scripts, and GitHub Actions CI.
