# Q1 Final Domain Contracts Report

Date: 2026-07-16

Branch: `fix/q1-final-domain-contracts`

Scope: provider/instrument identity, derivatives lifecycle, digital-asset identity, and strict
initial-domain wire contracts.

## Outcome

The final Q1 domain corrections are implemented without changing live classification, operational
scripts, or documentation outside this persisted report.

### Deterministic provider identity ingestion

- `ProviderIdentityRecord` now separates an immutable normalized assertion from sorted, unique
  local `observation_timestamps`.
- The natural key is the typed `ProviderIdentityKey` (`SourceId`, `ProviderInstrumentId`), and
  revision identity uses the shared `MetadataRevision` type.
- Assertions with identical immutable evidence, mapping, effective claim, and revision coalesce
  idempotently while preserving unique local observation timestamps.
- Divergent assertions for the same natural key and revision are retained in a deterministic
  `ProviderIdentityConflict` with
  `ProviderIdentityConflictReason::SameRevisionDivergence`; no input-order winner is selected.
- `ProviderIdentitySupersession` carries a typed predecessor revision plus immutable payload
  evidence. The normalizer rejects missing predecessor evidence, missing predecessors, cycles,
  branches, overlapping successors, and successors of open-ended assertions with typed
  `InstrumentError` variants.
- `InstrumentDefinition` stores accepted assertions and quarantined conflicts separately.
  `provider_identity_at` returns accepted point-in-time mappings only and suppresses resolution for
  any natural key with retained conflict evidence.
- Canonical sorting, observation deduplication, conflict sorting, and graph validation make the
  result invariant to arrival order. Property tests exercise repeated observations and revision
  permutations.
- Direct deserialization reconstructs and revalidates normalized records and conflicts; serialized
  vectors cannot bypass invariants.

### Futures lifecycle

- `first_trade_date` and `settlement_date` are independent optional source claims in
  `FuturesLifecycleDates`, covered by the containing `FuturesContractIdentity` payload reference,
  source timestamp, observation timestamp, and metadata revision.
- No lifecycle field is synthesized from another field. Empty, first-trade-only, and
  settlement-only values round-trip independently.
- Relation-specific errors cover first trade versus last trade/expiration/settlement, last trade
  versus expiration/settlement, expiration/maturity versus settlement, notice ranges, and delivery
  ranges.
- Round-trip tests retain all lifecycle fields while keeping FIX MaturityMonthYear (200),
  MaturityDate (541), LegMaturityMonthYear (610), and LegMaturityDate (611) distinct.

### Protocol-qualified chain identities

- Generic `ChainId` remains a case-sensitive CAIP-2 grammar type and makes no chain-existence claim.
- `EvmChainId` requires namespace `eip155` and a canonical unsigned decimal reference (no sign,
  label, or leading-zero alias).
- `SolanaChainId` requires namespace `solana` and one of the source-registry genesis-hash prefixes
  for mainnet, testnet, or devnet, exposed through `SolanaNetwork`.
- EVM and Solana address constructors accept only their protocol-qualified chain types. The role
  matrix rejects Solana mints on EVM, EVM token-contract roles on Solana, and contract/mint roles on
  Bitcoin.
- EVM address equality and hashing use decoded bytes plus chain/rule/role identity, not submitted
  casing. The original submission remains inspectable in memory; the authoritative wire is
  canonical lowercase hex. Mixed-case input still requires EIP-55 validation.
- Solana decoding remains bounded to 44 input characters and a fixed `[u8; 32]`; Bitcoin retains
  network and address-family validation.
- Authoritative `ChainAddress` deserialization reconstructs through the selected protocol
  constructor, rejecting rule/chain transplants, unsupported roles, unknown fields, and tampering.

### Strict wires

- Added `deny_unknown_fields` to `CryptoPairWire`, `CalendarDateWire`, `EffectiveIntervalWire`,
  symbol/lifecycle/roll wire records, external-identifier and rights-policy records, and the new
  provider identity contracts.
- Strict-wire tests demonstrate that unassigned timezone, inclusivity, inferred-symbol,
  global-symbol, transition, and decoded-address fields are rejected.

## TDD and verification evidence

The work followed explicit red/green cycles:

1. Provider tests first failed with 16 missing API/contract errors; the first implementation then
   exposed and fixed coalescing semantics. Follow-up red tests caught singleton missing-predecessor
   acceptance and active lookup through a later conflict.
2. Protocol tests first failed on absent `EvmChainId`, `SolanaChainId`, `SolanaNetwork`, and
   `InvalidAddressRole`, then passed after typed protocol construction and role enforcement.
3. Lifecycle tests first failed on absent first-trade/settlement fields and typed relation errors,
   then passed after the invariant-preserving constructor changes.

Fresh final verification:

```text
./scripts/verify.sh                                      PASS
cargo fmt --all --check                                 PASS (inside verify)
cargo clippy --workspace --all-targets --all-features
  --locked -- -D warnings                               PASS (inside verify)
cargo test --workspace --all-features --locked          PASS (inside verify)
cargo build --workspace --all-features --release
  --locked                                               PASS (inside verify)
cargo doc --workspace --all-features --no-deps --locked PASS (inside verify)
offline mock smoke (101 events)                          PASS
local stdio MCP smoke                                    PASS
```

Focused final suites include six provider identity tests (including proptest permutation cases),
four protocol chain/address tests, four futures identity tests, and two strict identity-wire tests,
in addition to the inherited domain and application suite.

## Primary research sources

- [CAIP-2 Final blockchain ID syntax and semantics](https://standards.chainagnostic.org/CAIPs/caip-2)
- [ChainAgnostic EIP-155 namespace profile](https://namespaces.chainagnostic.org/eip155/caip2)
- [ChainAgnostic Solana namespace profile and genesis-hash examples](https://namespaces.chainagnostic.org/solana/caip2)
- [EIP-55 mixed-case checksum specification and test vectors](https://eips.ethereum.org/EIPS/eip-55)
- [FIXimate MaturityMonthYear (200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html)
- [FIXimate MaturityDate (541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html)
- [FIXimate LegMaturityMonthYear (610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html)
- [FIXimate LegMaturityDate (611)](https://fiximate.fixtrading.org/en/FIX.Latest/tag611.html)

The provider revision graph and conflict policy are Market Squawk domain decisions rather than a
claim that a provider registry supplies a universal revision graph. Adapters must populate these
contracts from immutable source evidence; the domain does not infer revision relationships.

No floating-point financial representation, unsafe code, identity/account rotation, fingerprint
spoofing, CAPTCHA bypass, proxy rotation, quota evasion, or other concealment behavior was added.
