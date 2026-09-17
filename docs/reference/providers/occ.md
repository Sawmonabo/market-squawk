# OCC option-reference contract

OCC is a selected no-key reference source for listed option-product/series discovery and operative
contract-event evidence. It does not provide live quotes, trades, Greeks, or market depth.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Admission | Core option-reference target |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Current repository status | Selected and configured in the target template; adapter, frozen batch layout, canonical publication, and workflow composition are absent |

## Role and product workflows

| Evidence | Product use | Boundary |
| --- | --- | --- |
| Directory of Listed Products | Option root/product/series discovery and independent validation of provider contract identity | Reference state only; not proof of a current quote or tradability |
| Information-memo index | Discovery of adjustments, symbol changes, accelerated expirations, settlement events, and follow-up notices | A memo title is not operative economics |
| Full memo and attachments | Source evidence for deliverable, symbol, exercise, expiration, or settlement changes | No canonical mutation until every operative document is retained and parsed |

**VERIFIED PROVIDER FACT:** OCC's Directory of Listed Products page routes to search-batch,
HTTP-download-batch, download-batch, and corresponding record-layout resources. The page is a
reference/discovery surface, not a live market-data feed. See the
[Directory of Listed Products](https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/directory-of-listed-products).

**VERIFIED PROVIDER FACT:** the information-memo search exposes memo number, posting date,
optional effective date, title, and categories that include contract adjustments, expirations,
product/series events, operations, and outages. The full memo and attachments—not the result
title—control any contract-event interpretation. See
[OCC Information Memos](https://infomemo.theocc.com/infomemo/search-memo).

## Authentication and setup

No credential field is selected. The target input contains only:

```text
OCC_OPTIONS_REFERENCE_ENABLED=true
```

**APPLICATION POLICY:** enablement requests a bounded no-key doctor and exact reference jobs. It
does not authorize browser automation, assume the batch layout, or make the Options workspace
Available.

## Exact surfaces and data families

| Surface | Exact reviewed locator | Contract state |
| --- | --- | --- |
| Listed-products directory/router | `https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/directory-of-listed-products` | **VERIFIED PROVIDER FACT:** official entry point and links to batch/layout families |
| Batch-processing router | `https://www.theocc.com/market-data/market-data-reports/other-market-data-info/batch-processing` | Selected discovery route; exact machine request and response layout must be frozen before implementation |
| Information-memo search | `https://infomemo.theocc.com/infomemo/search-memo` | Official event-discovery index; full linked documents remain separate source objects |

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the exact current DLP batch request, record fields, control
records, effective-time rules, pagination/completion signal, and stable downloadable locator were
not frozen in the reviewed source set. No mapper or scheduler may guess them.

## Provenance, clocks, and quality

Retain each DLP response, record-layout document, memo, and attachment as a separate exact object.
Canonicalization must preserve:

- OCC product/root/series identifiers and every provider symbol or alias;
- the DLP schema/layout identity, request coordinates, page/batch identity, response digest, row
  count, and completion state;
- reference effective time when the admitted layout supplies one, local first-observed/receive/
  publish times, and revision/supersession lineage;
- memo number, posting date, optional effective date, category, complete body/attachment digests,
  affected contracts, and the exact interpreted event; and
- explicit unknown, omitted, superseded, conflicting, or unresolved state.

An information-memo posting date and an event effective date are different clocks. A later memo may
update a prior memo and must append a new revision. A format-valid OCC/OSI symbol is not proof that
the contract was listed or active at a decision cutoff.

## Official limits, application admission, and scheduling

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the reviewed official pages publish no numeric request rate,
batch/page ceiling, exact update cadence, checksum contract, retry header, archive completeness,
or correction-retention policy.

**APPLICATION POLICY:** do not assign OCC a recurring numeric capacity until the exact batch
contract is frozen and a bounded controlled acquisition succeeds. Use one shared reference queue,
one content-addressed acquisition per applicable publication, strict response-byte/row/deadline
bounds, and backoff on provider refusal. An unknown limit may lower work; it never becomes an
invented provider ceiling.

**APPLICATION POLICY:** memo discovery is incremental by stable memo identity. Fetch a full memo
and every required attachment once per new or changed digest; never repeatedly poll titles or
reinterpret an earlier event in place.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** on 2026-08-11 the official DLP page was readable through the research
web reader, while a direct generic command-line retrieval received HTTP `403`. No listed-product
data rows, supported machine batch, batch ceiling, or parser schema were proven.

That result makes the current provider doctor a prerequisite. It does not justify a private
endpoint, browser scraper, retry storm, or claim that automated batch access is impossible for an
admitted current route.

## Canonical storage and point-in-time destination

```text
exact DLP batch + frozen layout
    -> provider-native product/series rows
    -> market_squawk.instrument_lifecycle

memo index -> full memo + attachments
    -> closed option-contract lifecycle event
    -> immutable manifest + PIT option identity
```

The target lifecycle record binds option and underlying `InstrumentId`, OCC/provider symbols,
root, expiration, exact strike, side, multiplier/deliverable where established, validity interval,
source object, revision, and predecessor/successor event. Memo-derived economics remain unavailable
until a closed event schema and complete operative documents are admitted. Raw titles or free-form
memo text do not become public application DTOs.

## Repository integration status and seams

Repository inspection found no OCC adapter, built-in source profile, doctor, raw-object job,
canonical option-contract publisher, or typed application read. The future credential-file flag is
also not consumed because the thin importer is not implemented.

The integration must reuse, not duplicate:

- the source metadata, endpoint, budget, capture, and extraction authorities in
  [`market-squawk-sources`](../../../crates/market-squawk-sources/src/lib.rs);
- provider/native identity evidence in
  [`provider_identities`](../../../crates/market-squawk-domain/src/instrument/provider_identities.rs);
- the existing immutable research publication and PIT selectors described by the
  [research data plane](../../architecture/research-data-plane.md); and
- the target `market_squawk.instrument_lifecycle` and option schemas in the
  [provider architecture](../../architecture/market-data-provider-architecture.md).

## Doctor and end-to-end acceptance gates

OCC becomes Available for an exact Options workflow only after:

1. A current first-party batch specification and record layout are retrieved, hashed, reviewed,
   and bound to a code-owned schema.
2. A bounded no-key request proves the exact locator, status, content type, redirect path, bytes,
   row/control-record counts, pagination or terminal completion, and provider messages.
3. The parser rejects malformed, partial, duplicate, unsupported, and unknown required records
   without publishing a partial batch as complete.
4. Full adjustment/expiration/symbol/settlement memos and attachments are retained before any
   contract event is published.
5. Exact raw objects and canonical lifecycle/event rows publish atomically under one manifest.
6. A PIT selector proves contract identity and event state as of a cutoff; ambiguous or missing
   coverage remains explicit.
7. Options discovery/chain validation consumes the typed read, and restart reproduces the same
   manifest-bound result.

## Hard gaps

- The exact DLP machine layout, completion protocol, cadence, request capacity, checksum, and
  archive/revision behavior are not frozen.
- The reviewed direct retrieval received HTTP 403, so unattended acquisition still needs a
  supported-current-route proof.
- Search results do not contain the operative contract economics of an adjustment or settlement
  change; full documents are mandatory.
- OCC reference data does not provide quotes, trades, volume, open interest, IV, Greeks, OPRA, or
  order-book evidence.
- Current and forward snapshots cannot reconstruct comprehensive expired-option history.

## First-party sources

- [OCC Directory of Listed Products](https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/directory-of-listed-products)
- [OCC batch processing](https://www.theocc.com/market-data/market-data-reports/other-market-data-info/batch-processing)
- [OCC Information Memos](https://infomemo.theocc.com/infomemo/search-memo)

Related Market Squawk authorities: [provider architecture](../../architecture/market-data-provider-architecture.md),
[canonical schema and evidence contract](../market-data-canonical-schemas.md),
[shipping source coverage](../source-coverage.md), and the
[provider credential template](../market-squawk-provider-credentials.env.example).
