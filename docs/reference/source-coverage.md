# Source coverage and adapter reference

This page defines Market Squawk's source identities, immutable metadata, declared and runtime
coverage, health evidence, built-in onboarding profiles, rights gates, and currently implemented
adapters.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Audience | Operators, source-adapter authors, research engineers, risk engineers, and auditors |
| Status | Current |
| Last substantive review | 2026-07-25 |
| Reviewed commit | `041175590bd2e4a357ea28d75c675c252d3b3746` |

## Contents

- [Scope](#scope)
- [Source identity and immutable metadata](#source-identity-and-immutable-metadata)
- [Declared coverage](#declared-coverage)
- [Runtime coverage binding](#runtime-coverage-binding)
- [Source health](#source-health)
- [Implemented adapter matrix](#implemented-adapter-matrix)
- [Live market-data adapters](#live-market-data-adapters)
- [Research and local adapters](#research-and-local-adapters)
- [Onboarding, credentials, and rights](#onboarding-credentials-and-rights)
- [Failure and recovery behavior](#failure-and-recovery-behavior)
- [Product query surfaces](#product-query-surfaces)
- [Related documentation and code](#related-documentation-and-code)
- [External sources](#external-sources)

## Scope

Four related records answer different questions:

| Record | Question |
| --- | --- |
| Built-in onboarding profile | May this code-supported provider surface be set up under the reviewed release and rights decision? |
| `SourceMetadata` | What exact source, revision, authorization, coverage, network, capability, protocol, and quality ceiling was admitted? |
| `SourceCoverageRecord` | Does one independently sourced coverage scope match this exact live observation binding, and is it sufficient now? |
| `SourceHealthSnapshot` | Is this exact source session and connection generation live, fresh, authorized, covered, within budget, and integral now? |

These records are not interchangeable. Registering or setting up a profile does not create an
adapter, immutable metadata does not mint execution authority, and a source's quality ceiling does
not classify every observation at that ceiling.

This page documents shipping code at the reviewed commit. It does not provide a step-by-step
provider setup procedure or reproduce every provider response schema. Procedures belong in
[Source operations](../operations/source-operations.md); the only mutable completion state belongs
in the [delivery ledger](../plans/delivery-ledger.md).

## Source identity and immutable metadata

### Identity types

| Type | Contract |
| --- | --- |
| `InstrumentId` | Non-nil UUID, stable across provider symbols |
| `VenueId` | Nonempty string, at most 64 UTF-8 bytes, no whitespace or control character |
| `SourceId` | Nonempty string, at most 128 UTF-8 bytes, no whitespace or control character |
| `SourceIdentifier` | Nonempty string, at most 512 UTF-8 bytes, no whitespace or control character |
| `MetadataRevision` | A bounded `SourceIdentifier`; a label alone does not prove content identity |

`RevisionBoundPayloadEvidence` atomically binds a metadata revision to
`ExactPayloadEvidence`. Exact evidence always carries an algorithm-qualified content digest. It
may also carry a version-pinned locator reference and version, but a locator does not replace the
digest and does not by itself prove that remote content is immutable.

### `SourceMetadata` schema

The current `SourceMetadata` schema version is `1`. Its object is closed and contains:

| Field | Type and authority |
| --- | --- |
| `schema_version` | Supported `SchemaVersion`; unsupported versions are rejected |
| `source_id` | Exact internal `SourceId` |
| `revision_evidence` | Atomic metadata-revision and exact-payload evidence |
| `source_class` | `exchange`, `broker`, `official_agency`, `regulatory_filing`, `local_file`, `portfolio_export`, `licensed_dataset`, or `on_chain` |
| `provider` | Bounded provider `SourceIdentifier` used in metadata and diagnostics |
| `authorization` | Closed `AuthorizationGrant` |
| `coverage` | Closed `SourceCoverage` declaration |
| `quality_ceiling` | One canonical `DataQuality` value; never runtime authority |
| `network` | `denied` or an explicit allowlisted endpoint policy |
| `freshness` | Independent connection-idle, transport-age, source-age, market-age, and clock-skew bounds |
| `budget` | Optional shared provider-budget policy |
| `capabilities` | Closed live/extraction/protocol capability declaration |
| `protocol` | `not_live` or an exact live decoder/integrity-validation profile |

`AuthorizationGrant` contains an `AuthorizationMode`, authorization basis, exact evidence, and a
half-open effective interval `[start, end)`. Modes are:

- `public_interface` for a published public interface used under its declared terms;
- `user_authorized` for a user-owned remote credential or entitlement;
- `licensed` for a configured licensed local dataset; and
- `user_owned_local` for user-owned local input requiring no provider network access.

Local-file and portfolio-export metadata must use `user_owned_local`, deny all network access, and
have no provider budget. Remote exchange, broker, official-agency, regulatory-filing, and on-chain
metadata must carry an endpoint allowlist and a shared provider budget. A budget's provider and
account qualification must match the provider and authorization evidence.

`FreshnessPolicy` has five nanosecond fields:
`max_connection_idle_nanos`, `max_transport_age_nanos`, `max_source_age_nanos`,
`max_market_age_nanos`, and `max_clock_skew_nanos`. The first four must be positive and all five
must fit `i64`; no universal default is supplied by the type because each adapter declares its
policy.

### Capabilities and quality ceilings

`SourceCapabilities` contains:

| Field | Values |
| --- | --- |
| `live` | Boolean |
| `extraction` | Boolean |
| `sequence` | `provided` or `unsupported` |
| `checksum` | `provided` or `unsupported` |
| `historical` | `none`, `historical`, or `revision_preserving` |
| `source_timestamps` | Boolean |

At least one of `live` or `extraction` must be true. A non-live source cannot claim live sequence,
checksum, timestamp, event, or protocol behavior. A historical capability requires extraction.
Capability flags must agree exactly with the protocol profile.

`direct_verified` is a permissible metadata ceiling only for:

- live, real-time coverage with a provided sequence and provider timestamps; and
- an exchange delivered as `direct_venue`, or a user-authorized broker delivered as
  `authorized_broker`.

That constructor rule is necessary, not sufficient, for a current observation to become
`DirectVerified`. Runtime qualification separately checks health, coverage, sequence, checksum,
timing, market state, precision, and integrity. All nine quality values and that qualification
contract are in [Data quality](data-quality.md).

## Declared coverage

`SourceCoverage` is exact-evidence-bound and contains these independent dimensions:

| Field | Contract |
| --- | --- |
| `evidence` | Exact payload evidence for the declaration |
| `effective` | Half-open interval `[start, end)` |
| `domain` | `instruments`, `macroeconomic`, `regulatory_filings`, `portfolio`, `corporate_actions`, or `alternative_data` |
| `asset_classes` | Instrument sources only: nonempty, unique, at most 32 |
| `topology` | Venue topology below |
| `instruments` | Declared instrument universe below |
| `live` | Optional product/channel and event rules |
| `delay` | `real_time` or `delayed` with a positive nanosecond value |
| `delivery` | `direct_venue`, `authorized_broker`, `indirect`, or `unknown` |

### Topology and instrument universe

`CoverageTopology` keeps venue consolidation separate from market depth:

| Kind | Venue requirement |
| --- | --- |
| `single_venue` | Exactly one venue |
| `partial_venues` | Nonempty unique subset |
| `consolidated` | Nonempty unique explicitly consolidated set |
| `not_applicable` | No venues; selected automatically for non-instrument coverage and rejected for live metadata |

Partial and consolidated sets may contain at most 256 venues. Instrument universe kind is
`all_declared`, `partial`, or `enumerated`. The enumerated form requires 1–4,096 unique non-nil
internal instrument UUIDs. `partial` truthfully declines to establish membership; it is not a
positive match.

Non-instrument coverage forces an empty asset-class list, `not_applicable` topology, partial
instrument coverage with no list, and no live declaration.

### Live declarations

A live declaration binds one provider product, one provider channel, and 1–32 unique rules. A rule
binds:

- an event class;
- `top_of_book`, `price_level`, or `order_level` depth for a book event; and
- snapshot applicability.

Book snapshot and delta events require an explicit depth and `required` snapshot initialization.
Non-book events must omit depth and carry metadata-backed `not_applicable` snapshot evidence.
Duplicate event/depth keys are rejected.

Declared coverage is still metadata. Its constructor explicitly does not register a source, prove
a live subscription acknowledgement, or create current execution authority.

## Runtime coverage binding

The live-plane `CoverageScope` repeats the dimensions that must match an observation:

- source and venue identities;
- provider product and channel;
- event class and optional book depth;
- delay and consolidation (`single_venue`, `partial`, or `consolidated`);
- inclusive `effective_from` and optional inclusive `effective_until`; and
- metadata revision.

When half-open metadata coverage is converted to this inclusive runtime scope, the finite end is
reduced by one nanosecond. A zero delay, reversed interval, missing book depth, or unexpected
non-book depth is rejected.

`SourceCoverageRecord` binds that scope to a complete `LiveEvidenceBinding` and records one
`CoverageStatus`:

| Status | Meaning |
| --- | --- |
| `sufficient` | The exact matched scope is sufficient at the queried instant |
| `insufficient` | Coverage is partial, delayed, expired, or otherwise inadequate |
| `unknown` | Positive coverage has not been established |

Construction compares source, venue, product, channel, event class, depth, and metadata revision.
Any transplant across those dimensions fails. `sufficient` cannot be paired with delayed delivery
or partial consolidation. Querying outside the inclusive runtime interval returns
`insufficient`, even if the stored status was sufficient.

## Source health

`SourceHealthSnapshot` belongs to one exact source ID, metadata revision, source session,
nonzero connection generation, and observation instant. Live snapshots constructed inside the
registry also retain a private registry-issued authority binding; serialization deliberately omits
that capability.

Health dimensions remain independent:

| Dimension | States |
| --- | --- |
| Connection liveness | `connecting`, `live`, `stale`, `disconnected` |
| Transport freshness | `uninitialized`, `fresh`, `stale` |
| Market freshness | `uninitialized`, `fresh`, `stale` |
| Provider timestamp freshness | `uninitialized`, `fresh`, `stale` |
| Budget | `available`, `cooling_down`, `unavailable` |
| Authorization | `valid` with exact evidence/deadline, `uninitialized`, `invalid` |
| Runtime coverage | `sufficient` with exact acknowledgement/product/channel/deadline, `uninitialized`, `limited` |
| Stream integrity | `initializing`, `synchronizing`, `validating`, `healthy`, `stale`, `gap_detected`, `checksum_failed`, `divergent`, `quarantined` |
| Capture integrity | `disabled`, `healthy`, `incomplete` |
| Last error | `network`, `authorization`, `decode`, `integrity`, `provider_limit`, `local_backpressure`, or absent |

At most 64 bounded, non-secret coverage limitations may be retained. Future timestamps,
freshness-arithmetic overflow, excessive clock skew, expired runtime authorization/coverage
evidence, already-expired cooldowns, and serialized states inconsistent with their source
timestamps are rejected.

`live_valid_until` exists only when connection, transport, market, and source timestamps are fresh
and authorization and runtime coverage are valid/sufficient. Its deadline is the earliest of all
six dimensions. Heartbeats may refresh connection liveness but never market freshness.

For a ceiling below `DirectVerified`, the current-data deadline calculation may tolerate an
uninitialized provider timestamp while its required connection, market, transport, authorization,
and coverage inputs pass. A stale provider timestamp always fails. A `DirectVerified` ceiling
requires the full `live_valid_until` contract.

## Implemented adapter matrix

“Implemented” means the adapter and its bounded composition exist at the reviewed commit. The
release-state column records the code-owned built-in onboarding gate; it does not assert that an
operator has an active session or that mutable delivery acceptance is complete.

| Surface | Implemented product path | Release state | Authentication/onboarding | Coverage and delivery | Runtime quality ceiling |
| --- | --- | --- | --- | --- | --- |
| `coinbase.public-market-data` | Live Coinbase Exchange WebSocket used by configured paper operation; diagnostic capture also exists | `rights_limited` | Public; no account, key, or contact | Crypto, configured instruments, one venue, real time, delivery `unknown`; not consolidated | `direct_unverified` |
| `coinbase.exchange-direct-market-data` | Authenticated `ws-direct` full-channel and REST level-3 bootstrap selected by `Bot.Start` for risk-enforced paper execution | `rights_limited` | User-owned Coinbase Exchange account and View-only key envelope; exact active onboarding session required | Crypto, configured instruments, one Coinbase Exchange venue, real time, `direct_venue`; not consolidated | `direct_verified`, derived only from current runtime evidence |
| `kraken.spot-public-market-data` | Live Kraken Spot WebSocket v2 used by configured paper operation | `rights_limited` | Public; no account, key, or contact | Crypto, one configured instrument/channel, one venue, real time, `direct_venue`; not consolidated | `direct_unverified` |
| `sec.edgar-public` | SEC EDGAR extraction adapter | `refresh_required` | Public; non-secret organization/admin contact required | Regulatory filings, delayed, non-venue, delivery `unknown`, revision-preserving | `official_delayed` |
| `fred-alfred.api-v1-v2` | FRED/ALFRED extraction and vintage adapter | `rights_blocked` | Provider account and API key; explicit manual secret import | Macroeconomic, delayed, non-venue, delivery `unknown`, revision-preserving | `official_delayed` |
| `bls.v1-unregistered` | BLS public API v1 extraction | `refresh_required` | Public; no account, key, or contact | Macroeconomic, delayed, non-venue, delivery `unknown`, historical | `official_delayed` |
| `bls.v2-registered` | BLS public API v2 extraction | `refresh_required` | Provider registration, API key, and non-secret contact | Macroeconomic, delayed, non-venue, delivery `unknown`, historical | `official_delayed` |
| `treasury.daily-rates-xml` | Treasury daily par-yield XML extraction | `rights_limited` | Public; no account, key, or contact | Macroeconomic, delayed, non-venue, delivery `unknown`, historical | `official_delayed` |
| `treasury.fiscal-data` | Fiscal Data average-interest-rates v2 extraction | `available` | Public; no account, key, or contact | Macroeconomic, delayed, non-venue, delivery `unknown`, historical | `official_delayed` |
| `local.files` | Bounded user-owned file extraction and research ingestion | `available` | Local user-authorized root; no remote credential | Alternative data, delayed, non-venue, network denied, revision-preserving | `direct_unverified` |
| `local.portfolio-imports` | Raw-preserving holdings/transactions import and reconciliation | `available` | Local user-authorized root; no onboarding credential | Portfolio, delayed, non-venue, network denied, revision-preserving | `direct_unverified` |
| `local.paper-execution` | Simulated orders, fills, balances, positions, fees, latency, and slippage | `available` | Local; no external account or key | Execution capability, not a `SourceMetadata` data adapter | `modeled` |

### Static catalog ceiling versus runtime ceiling

The public Coinbase and Kraken onboarding profiles contain a static catalog `qualityCeiling` of
`direct_verified`, describing the highest reviewed capability class that those surfaces could
support. Their actual adapter metadata is stricter and hard-codes `direct_unverified`; the catalog
value cannot promote them. The separate authenticated Coinbase Direct adapter declares
`direct_verified` only as a runtime ceiling. Its constructor, credential probe, connection, or
subscription acknowledgement cannot mint execution authority.

`Source.GetCoverage` can return both the static onboarding declaration and active
`runtimeCoverage`. Readers must not treat the profile field as the quality of a runtime
observation. Public Coinbase and Kraken observations cannot satisfy the default `DirectVerified`
automated-action gate. Coinbase Direct can satisfy it only after its current runtime assessment
proves the complete qualification contract for the exact generation and observation.

`Bot.Start` selects public Coinbase or Kraken directly, or selects `coinbase-direct` with the exact
active onboarding-session UUID. The Direct activation binds the current credential generation,
shared provider-rate and account authority, configured product routes, and central live
qualification before strategy or risk can act. Its scoped rights do not admit research/fair-value
persistence, modeling, export, or redistribution, so that runtime publishes only into the
execution-owned live path.

## Live market-data adapters

### Coinbase Exchange Direct

The authenticated adapter connects only to `wss://ws-direct.exchange.coinbase.com` and the exact
configured `https://api.exchange.coinbase.com/products/<product>` product and level-3 book
endpoints. One active onboarding session supplies a current View-only signing capability. Each
product owner captures authenticated frames before decode, validates the signed `full`
subscription, queues a bounded sequence domain while acquiring the REST snapshot, replays
contiguously, and publishes canonical price-level snapshots and deltas through the ordinary live
qualification pipeline.

The runtime is single-account and bounded: product cardinality, raw capture, replay, orders,
price levels, queues, depth, refresh cadence, and retained bytes are admitted before network
startup. A sequence gap, duplicate/out-of-order frame, stale generation, invalid product status,
precision error, crossed or inconsistent book, queue overflow, capture failure, credential
rotation/revocation, or terminal supervisor exit quarantines/cancels the affected run. Heartbeats do
not refresh market-price freshness. No checksum is claimed because Coinbase does not provide one
for this profile.

### Coinbase Exchange

The current adapter connects only to `wss://ws-feed.exchange.coinbase.com`. Configuration binds
1–100 unique provider products to internal instruments and the `coinbase-exchange` venue, with a
maximum 64-byte product identifier and a 16 KiB encoded subscription.

| Dimension | Current declaration |
| --- | --- |
| Authorization | `public_interface` |
| Source class | `exchange` |
| Channels | `level2`, `matches`, `heartbeat` |
| Live rules | Price-level book snapshot and delta with required snapshot; trade with snapshot not applicable |
| Instrument coverage | Exact configured internal instruments |
| Topology/delay/delivery | Single venue, real time, `unknown` delivery |
| Sequence/checksum | Both `unsupported` in the admitted protocol profile |
| Provider timestamps | Supported |
| Extraction/history | Not supported |
| Ceiling | `direct_unverified` |

Heartbeat activity updates connection liveness only. Level-2 and match decoding, instrument
mapping, exact number handling, source timestamps, capture, and bounded reconnection remain bound
to the admitted metadata revision and connection generation.

### Kraken Spot WebSocket v2

The current adapter connects only to `wss://ws.kraken.com/v2`. One profile binds one ASCII
non-whitespace symbol of at most 64 bytes to one internal instrument and either the trades channel
or a book channel. Book depth must be one of `10`, `25`, `100`, `500`, or `1000`.

| Dimension | Current declaration |
| --- | --- |
| Authorization | `public_interface` |
| Source class | `exchange` |
| Product/venue | `kraken-spot`; single venue `kraken` |
| Live rules | Trades, or price-level book snapshot and delta with required snapshot |
| Topology/delay/delivery | Single venue, real time, `direct_venue` |
| Sequence | `unsupported` |
| Checksum | CRC-32/ISO-HDLC provided for book state under the pinned canonicalization; unsupported for trades |
| Provider timestamps | Supported |
| Extraction/history | Not supported |
| Ceiling | `direct_unverified` |

The book checksum does not compensate for the absent sequence capability. The adapter and compiled
qualification path retain the lower ceiling, so even a healthy checksum-valid Kraken generation
does not become `DirectVerified`.

## Research and local adapters

### Lease-gated research adapters

Research activation requires an active immutable onboarding lease for the exact surface, exact
source/revision binding, and admitted `persist` rights with non-refresh exact evidence. The CLI
activation request is a closed schema-version-2 object, capped at 1 MiB. Its provider kinds are
`sec`, `bls`, `treasury_fiscal`, `treasury_daily_rates`, and `fred_alfred`; each kind has a closed,
provider-specific scope. The loopback portal exposes only SEC, BLS, and Treasury Fiscal activation.

| Adapter | Extracted scope | Important bounded/authority behavior |
| --- | --- | --- |
| SEC EDGAR | Submissions, company facts, and referenced filing/XBRL representations | Exact organization/admin `User-Agent`; endpoint allowlist; shared request budget; raw evidence and representation registry; revision-preserving |
| BLS v1/v2 | Exact selected series and inclusive year range | Tier-specific endpoint and request plan; at most 1,000 series metadata inputs; v2 secret resolved only in explicit foreground work |
| FRED/ALFRED | Exact series metadata, observations, vintage dates, and revision history | API key plus exact per-series rights; revision-preserving; fail-closed durable rights assessment |
| Treasury Fiscal Data | Average Interest Rates v2 for an exact date interval and page size | Exact endpoint/query allowlist; dataset/version provenance |
| Treasury daily XML | Daily par-yield curve for one year | Separate XML surface and evidence; Fiscal Data rights are not inherited |

The research metadata for these adapters uses a positive one-nanosecond `delayed` declaration and
`unknown` delivery rather than claiming real-time or direct delivery.

Durable activation recipes exist for exactly six profile surfaces: SEC; BLS v1 and v2; Treasury
Fiscal and daily XML; and FRED/ALFRED. Recipes are secret-free and bind exact request and evidence
digests. On restart, SEC, BLS v1, and both Treasury surfaces can be reconstructed without a
credential when their authority remains valid. BLS v2 and FRED return
`provider activation requires explicit foreground credential resume` and remain disabled until
that explicit resume. Invalid evidence, authority, or adapter state quarantines the recipe.

At the reviewed commit, release and rights gates have concrete consequences:

- SEC and both BLS profiles require a new admitted evidence refresh revision.
- Treasury daily XML has pending persistence rights; Fiscal Data rights do not transfer to it.
- FRED is rights-blocked, so an API key alone cannot produce an active lease.
- Treasury Fiscal Data is the current built-in official profile whose release and all six rights
  operations are admitted.

### FRED durable-rights boundary

FRED's adapter implementation does not weaken the built-in `rights_blocked` state. When a future
profile revision admits a scope, ephemeral retrieval still requires fresh terms, while durable
persist, cache/archive, display, model-training, export, or redistribution requires:

- an exact grant for each series and requested operation; wildcards are rejected;
- exact owner-authorization evidence effective through its expiry;
- one exact digest-bound terms bundle containing the API terms, services legal terms, and privacy
  policy, each at most 2 MiB;
- a bounded rights artifact of at most 256 KiB and at most 256 grants; and
- the provider API key through the explicit secret boundary.

Every requested series is assessed independently. Missing, expired, mismatched, or insufficient
rights fail closed. A successful key probe is not a rights decision.

### Local files

The local-file adapter supports closed manifest declarations for `csv`, `tsv`, `json`, `ndjson`,
`xml`, `excel`, `parquet`, `sqlite`, `ofx`, and `qfx`. SQLite accepts an exact table, selected
columns, and `order_by` list; it does not accept arbitrary SQL. Input remains beneath a
user-authorized root, while representation state is held under a disjoint controlled root.

Standard extraction limits are:

| Resource | Standard limit |
| --- | ---: |
| Manifest bytes / nesting / objects | 4 MiB / 32 / 4,096 |
| Cumulative manifest mappings / one manifest string | 65,536 / 256 KiB |
| Retained manifest memory | 16 MiB |
| One source object / decompressed bytes / parser-retained memory | 64 MiB / 256 MiB / 256 MiB |
| Output records / fields per record / parser nesting | 100,000 / 1,024 / 64 |
| One text value | 1 MiB |
| Spreadsheet sheets / cells | 256 / 1,000,000 |
| Parquet row groups / tabular columns | 8,192 / 4,096 |
| Archive entries / per-entry compression ratio | 10,000 / 100 |
| Elapsed parser time | 60 seconds |

Limit exhaustion is a typed adapter failure; it never causes an unbounded retry or partial
authority grant.

### Portfolio imports and paper execution

Portfolio import uses user-owned local authority, an 8 MiB manifest ceiling, a controlled
raw-input archive, normalized holdings/transactions, and explicit reconciliation evidence. It is
an extraction source with `portfolio_export` class and `portfolio` coverage.

`local.paper-execution` is an onboarding capability for the local simulated execution adapter. It
has modeled outputs and central risk authority, but it is not a `SourceMetadata` provider and must
not appear as market-data coverage.

## Onboarding, credentials, and rights

### Release states

| State | Meaning at the built-in profile revision |
| --- | --- |
| `available` | Reviewed activation and rights prerequisites encoded by the profile are admitted |
| `rights_limited` | Only the listed rights operations are admitted; broader use remains pending |
| `refresh_required` | Named official evidence must be refreshed and published in a new contiguous profile revision |
| `rights_blocked` | Required rights admission is absent; setup or a credential cannot override it |

Profiles are code-owned and versioned. Runtime onboarding produces bounded sessions, evidence, and
leases; it does not mutate the profile's release decision.

### Exact built-in rights decisions

The six independent operations are retrieve, display, persist, model training, export, and
redistribute.

| Surface | Retrieve | Display | Persist | Model training | Export | Redistribute |
| --- | --- | --- | --- | --- | --- | --- |
| Coinbase public market data | Admitted | Admitted | Pending | Pending | Pending | Pending |
| Kraken Spot public market data | Admitted | Admitted | Pending | Pending | Pending | Pending |
| SEC EDGAR public | Admitted | Admitted | Admitted | Admitted | Admitted | Admitted |
| FRED/ALFRED | Blocked | Blocked | Blocked | Blocked | Blocked | Blocked |
| BLS v1 unregistered | Admitted | Admitted | Admitted | Admitted | Pending | Pending |
| BLS v2 registered | Admitted | Admitted | Admitted | Admitted | Pending | Pending |
| Treasury daily-rates XML | Admitted | Admitted | Pending | Pending | Pending | Pending |
| Treasury Fiscal Data | Admitted | Admitted | Admitted | Admitted | Admitted | Admitted |
| Local files | Admitted | Admitted | Admitted | Admitted | Admitted | Pending |
| Local portfolio imports | Admitted | Admitted | Admitted | Admitted | Admitted | Pending |
| Local paper execution | Admitted | Admitted | Admitted | Admitted | Admitted | Admitted |

For BLS v2, the built-in lifecycle notes annual key renewal and explicit import of the replacement
as a higher local credential generation. The profile records that its reviewed evidence contains
no remote key-revocation API. FRED and BLS v2 secrets are accepted only through write-only/manual
import boundaries and are omitted from status, audit, and durable recipe output.

## Failure and recovery behavior

Source construction and activation fail closed at several independent layers:

- Metadata rejects empty/duplicate/oversized collections, zero delayed duration, inconsistent
  instrument versus non-instrument fields, invalid live rules, impossible quality ceilings,
  capability/protocol conflicts, missing remote network/budget policy, unsafe local networking,
  and provider/budget authorization mismatch.
- Runtime coverage rejects reversed intervals, invalid depth, every binding mismatch, and a
  sufficient status paired with delayed or partial coverage.
- Health rejects future or tampered freshness/liveness, clock skew, stale runtime evidence, an
  expired cooldown, and more than 64 limitations.
- Onboarding refuses blocked, refresh-required, or rights-inadequate activation and never treats
  possession of a key as authorization.
- Network adapters contact only their admitted endpoint and query policies and share bounded
  provider budgets.
- Gaps, checksum failures, divergence, or other critical integrity failures quarantine the affected
  generation. Recovery requires provider-specific reconnect/resnapshot, a new current generation,
  and fresh health and coverage evidence.
- Local extraction cancels or fails on its exact parser, memory, record, decompression, archive, or
  deadline ceiling; no parser fallthrough grants partial authority.

Archived metadata, coverage, health, or a prior `DirectVerified` classification is evidence for
audit/research only. Replaying it cannot recreate current source or execution authority.

## Product query surfaces

The application exposes:

| Operation | Returned view |
| --- | --- |
| `Source.GetStatus` | Bounded code-owned profile, current onboarding session, and runtime active/not-active state |
| `Source.GetCoverage` | Static declared profile coverage plus active runtime coverage when present |
| `Source.GetHealth` | Bounded active runtime connection, freshness, integrity, coverage, and quality view, or explicit `not_active` state |
| `Source.Register` | Confirmed registration of one code-supported profile |
| `Source.Setup` | Confirmed start/resume of bounded local onboarding |

CLI commands are `source status`, `source coverage`, `source health`, `source register`, `source
setup`, and the separate evidence-bound `source activate` request-file command. MCP exposes the
five application operations but not the CLI-owned activation request-file boundary. Full argument
contracts are in the [CLI reference](cli.md) and [MCP reference](mcp.md).

## Related documentation and code

- [Data quality](data-quality.md)
- [Time and provenance](time-and-provenance.md)
- [Configuration reference](configuration.md)
- [CLI reference](cli.md)
- [MCP reference](mcp.md)
- [Live execution plane](../architecture/live-execution-plane.md)
- [Research data plane](../architecture/research-data-plane.md)
- [Data, time, and provenance architecture](../architecture/data-time-and-provenance.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Source operations](../operations/source-operations.md)
- [Source metadata](../../crates/market-squawk-sources/src/metadata.rs)
- [Runtime coverage binding](../../crates/market-squawk-domain/src/classification/coverage.rs)
- [Source health](../../crates/market-squawk-sources/src/health.rs)
- [Built-in onboarding profiles](../../crates/market-squawk-sources/src/onboarding/built_in_profiles.rs)
- [Provider activation contracts](../../apps/market-squawk/src/provider_activation/specs.rs)
- [Research activation and durable recipes](../../apps/market-squawk/src/local_product/cli_provider.rs)
- [Coinbase adapter metadata](../../adapters/market-squawk-adapter-coinbase/src/config.rs)
- [Kraken adapter qualification](../../adapters/market-squawk-adapter-kraken/src/qualification.rs)
- [Local-file extraction contracts](../../adapters/market-squawk-adapter-files/src/contracts.rs)
- [Accepted-head delivery evidence](../plans/delivery-ledger.md)

## External sources

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | `level2`, `matches`, and `heartbeat` provider semantics used by the pinned adapter | 2026-07-23 |
| [Kraken Spot WebSocket v2 book checksum guide](https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/) | Book checksum canonicalization and supported depth semantics implemented by the Kraken adapter | 2026-07-23 |
| [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Public submissions and company-facts API scope | 2026-07-23 |
| [SEC webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | Declared `User-Agent` and aggregate fair-access expectations | 2026-07-23 |
| [BLS public data API](https://www.bls.gov/developers/home.htm) | Unregistered v1 and registered v2 access tiers | 2026-07-23 |
| [BLS API terms of service](https://www.bls.gov/developers/termsOfService.htm) | BLS provenance, representation, limits, and third-party-rights boundary | 2026-07-23 |
| [FRED API documentation](https://fred.stlouisfed.org/docs/api/fred/) | Series, observations, and vintage interfaces implemented by the adapter | 2026-07-23 |
| [FRED API terms of use](https://fred.stlouisfed.org/docs/api/terms_of_use.html) | API access does not override third-party series rights; scope-specific permission may still be required | 2026-07-23 |
| [Treasury daily interest-rate XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | Daily-rate XML is a distinct provider surface | 2026-07-23 |
| [Treasury Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/) | Dataset-specific public API, query, paging, and provenance surface | 2026-07-23 |

External provider pages define upstream interfaces and terms. The reviewed Market Squawk code head
remains authoritative for admitted endpoints, source identities, rights gates, adapter bounds,
coverage truth, quality ceilings, health, and runtime authority.
