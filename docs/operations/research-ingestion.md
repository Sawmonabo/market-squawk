# Research ingestion operations

This runbook covers the shipping local procedures for admitting user-owned files and exact objects
from activated research providers into immutable analytical datasets.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local research operators, data stewards, and incident responders |
| Status | Current, with provider limitations called out below |
| Last substantive review | 2026-07-26 |
| Reviewed commit | `50912c18271a0389fb5ac8817555230930dd0506` |

## Contents

- [Scope](#scope)
- [Safety and authority boundaries](#safety-and-authority-boundaries)
- [Preconditions](#preconditions)
- [Choose a supported ingestion path](#choose-a-supported-ingestion-path)
- [Ingest a user-owned file](#ingest-a-user-owned-file)
- [Ingest an exact registered-provider object](#ingest-an-exact-registered-provider-object)
- [Rights, availability, and revisions](#rights-availability-and-revisions)
- [Success evidence](#success-evidence)
- [Idempotency, rollback, and recovery](#idempotency-rollback-and-recovery)
- [Failure modes](#failure-modes)
- [Local state locations](#local-state-locations)
- [Related documentation, code, and evidence](#related-documentation-code-and-evidence)
- [External sources](#external-sources)

## Scope

This page documents:

- `ingest file <MANIFEST> --object <ID> --dataset <ID> --confirm`;
- `ingest source <PROFILE> <OBJECT> --dataset <ID> --confirm`;
- the CSV, JSON, NDJSON, and Parquet forms of the versioned local-file manifest;
- the SEC EDGAR, FRED/ALFRED, BLS, and Treasury adapter identities that the current binary accepts;
- the dedicated portfolio-import boundary to the extent it produces research data; and
- immutable publication, retry, rights, availability, revision, and recovery behavior.

Supported network ingestion enters through a registered and activated source profile, and
supported publication produces an immutable catalog receipt. Direct provider HTTP calls, manual
SQLite insertion, and edits to an admitted Parquet object do not produce that authority. This page
also does not claim that every compiled adapter has a complete first-use CLI workflow. The current
gaps are recorded in
[Choose a supported ingestion path](#choose-a-supported-ingestion-path) and linked to the mutable
[delivery ledger](../plans/delivery-ledger.md).

Ingestion produces research data with record-level provenance. It never creates order authority,
and current results report `executionEligible: false`.

## Safety and authority boundaries

- Every ingestion is a durable mutation and requires `--confirm`.
- Use one explicit `--data-dir` for setup, ingestion, inspection, backup, and recovery. The default
  is `.market-squawk`, but relying on the current working directory during an incident is unsafe.
- `ingest source` accepts an already active **profile identity**, not a provider nickname. The
  built-in research identities are `sec.edgar-public`, `fred-alfred.api-v1-v2`,
  `bls.v1-unregistered`, `bls.v2-registered`, `treasury.daily-rates-xml`, and
  `treasury.fiscal-data`.
- Source activation owns endpoint, credential, terms, and rights evidence. The ingestion command
  cannot widen that authority.
- `ingest file` treats the manifest's parent directory as the user-authorized input root. The
  manifest and referenced data remain confined to that root; the adapter does not turn an arbitrary
  path in the manifest into ambient filesystem authority.
- The local-file manifest is limited to 4 MiB, 4,096 objects, and 1,024 field mappings per object.
  One research extraction is independently limited to 4,096 discovered objects, 100,000 normalized
  records, 64 MiB retained extraction data, and 60 seconds.
- Do not delete a catalog row or content-addressed object to “undo” ingestion. Published
  generations are immutable. Recovery uses an exact retry, a new revision, or the supported
  backup-and-restore procedure.

## Preconditions

Set and initialize the controlled root:

```bash
export DATA_ROOT=/absolute/path/to/market-squawk-data
market-squawk --data-dir "$DATA_ROOT" init
market-squawk --data-dir "$DATA_ROOT" config validate
market-squawk --data-dir "$DATA_ROOT" doctor
```

For a local file:

- keep the manifest and every referenced data file under one user-owned directory;
- use regular files and a real directory rather than symlinks;
- make the manifest's `dataset` and `object_id` exactly match the command arguments; and
- decide and record effective time, publication/first-observation time, revision, units, and any
  instrument binding before ingestion.

For a network provider:

- complete the supported registration, setup, and activation workflow in
  [Source operations](source-operations.md);
- confirm that `source status <PROFILE>` reports the exact active profile;
- retain the source-activation receipt and current rights evidence; and
- obtain an exact adapter object ID through an accepted product evidence path. A digest-bearing ID
  has discovery authority only when that evidence returns it.

Before any FRED or ALFRED persistence, review the tracked
[`fred-rights-decision.json`](../verification/fred-rights-decision.json). At the reviewed commit it
has `series_scope: "exact_service_and_series_grants"` and
`disposition: "service_permission_required"`. Revision 4 keeps persistence and training closed
unless activation binds exact current terms, a written St. Louis Fed permission response matched
byte-for-byte against a fresh reacquisition from its official HTTPS URL, a hash-bound local review,
and independent public-domain or owner-permission evidence for every selected series and operation.

## Choose a supported ingestion path

| Source | Current operator path | Exact dataset/object shape | Current limitation |
| --- | --- | --- | --- |
| User-owned CSV, JSON, NDJSON, or Parquet | `ingest file` | Both identities come from manifest schema version 3 | Complete CLI path |
| SEC submissions | Activate `sec.edgar-public`, then `ingest source` | Dataset `sec.submissions.cik.<10-digit-CIK>`; object `sec.submissions.composite.CIK<10-digit-CIK>` | Available; setup requires a truthful organization/application name, monitored administrative email, and the exact ten-digit registrant CIK |
| SEC company facts | Activate `sec.edgar-public`, then `ingest source` | Dataset `sec.company-facts.cik.<10-digit-CIK>`; object is the exact `https://data.sec.gov/api/xbrl/companyfacts/CIK<10-digit-CIK>.json` locator | Available under the same truthful SEC setup identity and CIK |
| FRED/ALFRED | Portal setup and bounded `source inspect`; durable `ingest source` only after both authority gates pass | Provider selector `fred:series-observations:<SERIES>:<REALTIME_START>:<REALTIME_END>` or the `alfred:` form; local analytical dataset uses the same fields with dots; object `fred-page-v2:<OFFSET>:<LIMIT>:<RETURNED>:<TOTAL>:<TERMINAL>:<PAGE_SHA256>:<METADATA_SHA256>` | Ephemeral inspection is runnable and writes no research dataset; durable use requires current terms, exact written St. Louis Fed service permission with local review, exact-series authority, a zero-fee API key, and explicit foreground credential work |
| BLS public v1 | Portal setup, read the exact dataset identity from activation or `source status`, then `source discover` and `ingest source` | Provider selector `bls:timeseries:public-v1:<PLAN_SHA256>`; local analytical dataset `bls.timeseries.public-v1.<PLAN_SHA256>`; object `bls:<CHUNK_INDEX>:<RESPONSE_SHA256>` | Available without an account or key; terminal release acceptance still requires the unchanged-candidate official-response workflow |
| BLS registered v2 | Portal setup, `source discover`, then `ingest source` after release admission | Provider selector `bls:timeseries:registered-v2:<PLAN_SHA256>`; local analytical dataset `bls.timeseries.registered-v2.<PLAN_SHA256>`; object `bls:<CHUNK_INDEX>:<RESPONSE_SHA256>` | `refresh_required`; this optional higher-limit surface is not currently activatable and cannot replace public v1 in release evidence |
| Treasury Fiscal Data | Portal setup, `source discover`, then `ingest source` | Provider selector `treasury:fiscal-data:average-interest-rates-v2:<QUERY_SHA256>`; local analytical dataset `treasury.fiscal-data.average-interest-rates-v2.<QUERY_SHA256>`; object `treasury-page:fiscal:<PAGE>=1+:<REQUEST_SHA256>:<PAYLOAD_SHA256>` | Complete no-credential local path |
| Treasury daily rates | Portal setup, `source discover`, then `ingest source` | Provider selectors use `treasury:<FAMILY>:<YEAR>`; local analytical datasets use `treasury.<FAMILY>.<YEAR>`; object `treasury-page:daily-rate:0:<REQUEST_SHA256>:<PAYLOAD_SHA256>` | Complete no-credential local path for every configured year across all five families |
| Portfolio export | `portfolio import <PATH> --account <ID> --confirm` | Dedicated portfolio manifest and account authority | Portfolio evidence enters through the dedicated workflow in [Portfolio and paper execution](portfolio-and-paper-execution.md) |

The digest-bearing forms above describe what the adapters validate. They become selectable only
when accepted `source discover` evidence returns the exact identity. Provider selectors and source
provenance retain their provider grammar; analytical publication uses a separate storage-safe
`DatasetId`. Do not substitute one identity for the other.

FRED/ALFRED is the exception for non-persistent first use: `source inspect` accepts the exact
provider selector and active onboarding session directly, then returns a bounded canonical page
and its evidence without creating a selectable research object or manifest.

## Ingest a user-owned file

### 1. Prepare the source manifest

The manifest uses snake_case fields and schema version 3. This single-object CSV example shows the
complete required shape:

```json
{
  "schema_version": 3,
  "objects": [
    {
      "dataset": "alternative-prices",
      "object_id": "prices-2026-07-23",
      "path": "prices.csv",
      "format": {
        "kind": "csv",
        "delimiter": 44
      },
      "effective_at": 1784764800000000000,
      "published_at": 1784768400000000000,
      "revision": "vendor-export-2026-07-23",
      "revision_number": 1,
      "superseded_at": null,
      "record_time": {
        "effective": {
          "schema_version": 2,
          "coordinate": {
            "precision": "exact_timestamp",
            "value": 1784764800000000000
          }
        },
        "published": {
          "schema_version": 2,
          "coordinate": {
            "precision": "exact_timestamp",
            "value": 1784768400000000000
          }
        },
        "superseded": null
      },
      "instrument_binding": {
        "kind": "unscoped"
      },
      "row_policy": {
        "identity_field": "id",
        "fields": [
          {
            "source": "value",
            "field": "price",
            "decimal_scale": 2,
            "unit": "USD"
          }
        ]
      }
    }
  ]
}
```

The integer time values are Unix nanoseconds. A nonzero `revision_number` is mandatory. If the
source has been superseded, `superseded_at` must be later than `effective_at`, and the matching
record-time coordinate must truthfully preserve that source fact.

Use one of these closed `format` objects:

| Input | Manifest value | Input expectation |
| --- | --- | --- |
| CSV | `{"kind":"csv","delimiter":44}` | `delimiter` is the byte value and cannot be NUL, CR, LF, or `"` |
| JSON | `{"kind":"json"}` | Structured JSON records accepted by the bounded file adapter |
| NDJSON | `{"kind":"ndjson"}` | One structured JSON record per line |
| Parquet | `{"kind":"parquet"}` | A bounded regular Parquet file; rows are still mapped by `row_policy` |

`row_policy.identity_field` names the stable source-row identity. Each entry in `fields` maps one
source column to a canonical alternative-data field with a decimal scale and optional unit. Field
names are nonempty, at most 256 bytes, and contain only ASCII letters, digits, `_`, `-`, or `.`.
Duplicate source or output mappings are rejected.

The tracked
[`adapters/market-squawk-adapter-files/fixtures/manifest.json`](../../adapters/market-squawk-adapter-files/fixtures/manifest.json)
is the complete code-owned schema example for every compiled local format.

### 2. Run the confirmed ingestion

Pass the manifest path and select exactly one declared object:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  ingest file /absolute/path/to/source/manifest.json \
  --object prices-2026-07-23 \
  --dataset alternative-prices \
  --confirm
```

The command:

1. opens the manifest once as a bounded no-follow regular file;
2. derives user-ownership evidence for that exact manifest;
3. activates the internal `local.files` profile with network access denied;
4. discovers the exact `(dataset, object_id)` declaration;
5. reads and normalizes the referenced file under the retained root capability;
6. assigns locally observed revision authority;
7. publishes verified Arrow-derived Parquet under the content-addressed artifact root; and
8. atomically records the immutable catalog generation.

The dynamically assigned source ID and representation-state directory are derived from the
manifest SHA-256. Reusing unchanged manifest bytes therefore selects the same local source
identity.

### 3. Inspect the published generation

Use the dataset ID returned by ingestion:

```bash
market-squawk --data-dir "$DATA_ROOT" --output json dataset manifest alternative-prices
```

This reads the latest immutable generation for that dataset. Record its manifest version, schema
fingerprint, content hash, source ID, row count, object count, byte count, and lineage digest in the
operation log.

## Ingest an exact registered-provider object

### 1. Prove the exact profile is active

For example:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  source status sec.edgar-public
```

If the profile is not active in this process, use the registration/setup/activation procedure in
[Source operations](source-operations.md). FRED and registered BLS credentials are deliberately not
restored as plaintext across restart; those profiles require the documented explicit resume or
reactivation step.

### 2. Select an exact object

The coordinator first performs bounded discovery and then requires exactly one discovered object
whose ID equals the supplied argument. A missing match returns not found; two matches fail as an
invalid provider result.

SEC submissions have a deterministic object ID. This is a complete example for Apple CIK
`0000320193`:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  ingest source \
  sec.edgar-public \
  sec.submissions.composite.CIK0000320193 \
  --dataset sec.submissions.cik.0000320193 \
  --confirm
```

SEC company facts use the exact official locator as the object ID:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  ingest source \
  sec.edgar-public \
  https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json \
  --dataset sec.company-facts.cik.0000320193 \
  --confirm
```

The actual syntax has two positional values followed by a required dataset option:

```text
ingest source <PROFILE> <OBJECT> --dataset <DATASET> --confirm
```

Do not use the older three-positional rendering of this command. The frozen CLI implementation
declares `--dataset` as a named option.

For FRED/ALFRED, BLS, or Treasury, run `source discover <PROFILE> --dataset <PROVIDER_DATASET>`
first. Confirmed ingestion must use the complete returned object ID, fresh discovery receipt, same
provider selector, and a profile whose current rights authorize persistence.

## Rights, availability, and revisions

### Rights

Each extraction is bound to the source ID and the exact normalized batch digest. The coordinator
checks that persistence is among the admitted operations and that authorization has not expired at
retrieval time. Expired, absent, or scope-mismatched rights fail before publication.

For local files, the basis is user-owned local evidence derived from the admitted manifest. For
network sources, the basis comes from the active onboarding lease and its provider-specific terms
or ownership evidence. A CLI `--confirm` is consent to execute the already-authorized operation; it
is not a substitute for rights.

FRED is additionally fail-closed through independent service and series gates. Revision 4 admits
persistence or model training only when exact written St. Louis Fed permission reacquired from its
official HTTPS URL and exact-series authority both cover the requested operation and their validity
windows intersect the current terms. API-key validity, email headers, a contact receipt, or
public-domain status alone is insufficient. Unrestricted export and redistribution remain blocked.

### Availability

Point-in-time use distinguishes:

- provider-evidenced publication/availability;
- the first local observation time when the provider supplies no stronger coordinate;
- inferred availability; and
- unknown availability.

The file manifest preserves both legacy exact timestamps and schema-v2 temporal coordinates.
Network adapters retain the strongest provider evidence they can prove. Later dataset construction
uses these coordinates at each cutoff; inferred and unknown availability do not silently become
known.

### Revisions

- SEC, FRED/ALFRED, BLS, and Treasury daily-rate records retain provider-specific revision
  evidence where the provider supplies it.
- Treasury Fiscal Data rows and user-owned file/portfolio rows use locally observed revision
  authority when no truthful provider order exists.
- Revision number zero is invalid.
- Supersession is an additional immutable fact. It does not rewrite or delete the older revision.
- A changed provider response or changed local file is new content. It must become a new immutable
  generation or revision, not an in-place replacement.

See [Time and provenance](../reference/time-and-provenance.md) for the exact selection rules.

## Success evidence

A successful JSON result has:

- `data.manifest.datasetId`, `manifestVersion`, schema identity, and content hash;
- `data.rowCount`, `totalBytes`, `objectCount`, and `lineageDigest`;
- `metadata.sourceCoverage.sourceId`, `provider`, `profile`, `providerDataset`, `objectId`,
  metadata revision, exact payload digest, and the same manifest pin;
- `metadata.dataQuality.qualityCeiling`;
- `metadata.dataQuality.recordLevelProvenance: true`; and
- `metadata.dataQuality.executionEligible: false`.

Treat the operation as successful only when the process exits successfully, this envelope is
complete, and a subsequent `dataset manifest <DATASET>` reports the expected immutable generation.
A network response, source-health success, staged Parquet file, or reserved ingest row by itself is
not commit evidence.

## Idempotency, rollback, and recovery

The coordinator derives the idempotency key from:

- profile;
- provider dataset;
- object ID; and
- exact normalized extraction-batch digest.

The catalog then binds the reservation to source, operation, payload, and rights fingerprint.

### Exact retry

If a command times out, is interrupted, or returns an indeterminate local I/O failure:

1. preserve the catalog, artifact tree, source manifest, rights evidence, and command arguments;
2. inspect `dataset list` and `dataset manifest <DATASET>`;
3. rerun the **identical** confirmed command against unchanged input; and
4. compare the returned manifest and payload digest with the first operation's evidence.

An exact retry reuses the matching reservation. If publication already committed, recovery
reconciles that run to the same manifest and completes it as succeeded. If the same immutable
coordinate points to different bytes, the system returns a conflict instead of overwriting either
side.

### New content or corrected metadata

Do not reuse an old revision identity for changed bytes. Correct the source-owned manifest,
provider activation, rights evidence, or revision declaration and submit the new truthful input.
The resulting publication is a new immutable generation.

### Rollback

There is no destructive ingestion rollback command. To stop downstream use, pin consumers to the
last accepted manifest or publish a truthful superseding revision. To recover lost or corrupt local
state, stop writes and use [Backup and recovery](backup-and-recovery.md); never delete a
content-addressed object or edit `catalog.sqlite3` by hand.

## Failure modes

| Symptom | Meaning | Safe response |
| --- | --- | --- |
| Confirmation required or invalid request | `--confirm` is absent, identities disagree, or the request violates the closed grammar | Correct the command or manifest; do not weaken validation |
| Profile not found | The exact provider profile is not active in this process | Inspect status and perform supported activation/resume |
| Object not found | Discovery did not return the supplied exact object ID | Re-establish accepted discovery evidence; only its returned exact identity has selection authority |
| Unauthorized | Rights are absent, expired, or do not cover persistence | Stop; update rights through onboarding after an independent review |
| FRED persistence rejected | Current terms, exact written Bank service permission, its hash-bound local review, exact-series authority, requested operation, validity intersection, object receipt, or credential generation is missing or stale | Correct the exact rejected authority and rediscover; an API key or contact submission is not durable-use permission |
| Generation resynchronization required | Refetched provider bytes or metadata no longer match the selected object evidence | Treat the earlier object as stale and obtain a newly admitted object identity |
| Deadline, cancellation, or unavailable | Work did not prove a durable terminal before the caller stopped observing it | Preserve state, inspect the manifest, then make one exact retry |
| Replay or generation conflict | The same immutable coordinate is being associated with different inputs | Preserve both evidence sets and create a new truthful version; do not overwrite |
| Catalog, Parquet, or controlled-path failure | Local state cannot be proven consistent | Stop mutations, preserve the root, and follow backup/recovery or troubleshooting |

## Local state locations

All paths are relative to the explicit `DATA_ROOT`:

| Path | Purpose | Operator rule |
| --- | --- | --- |
| `catalog.sqlite3` plus its SQLite WAL/SHM companions while open | Ingest reservations, rights decisions, immutable manifests, object references, revisions, and audit | Back up and restore as one consistency domain |
| `artifacts/objects/sha256/<first-two-hex>/<sha256>.parquet` | Immutable analytical objects | Never rename, edit, or delete manually |
| `control/sources/file-representations/<manifest-sha256>/` | Local-file representation state | Application-owned; preserve with the control root |
| `control/` | Source activation and other durable authority state | Do not inspect for secret values or edit records by hand |

The original user-owned source directory remains outside this managed tree. Preserve it with its
manifest and acquisition evidence according to the operator's retention policy.

## Related documentation, code, and evidence

- [Research data plane](../architecture/research-data-plane.md)
- [Data, time, and provenance](../architecture/data-time-and-provenance.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [CLI reference](../reference/cli.md)
- [Data quality reference](../reference/data-quality.md)
- [Time and provenance reference](../reference/time-and-provenance.md)
- [Datasets and query](datasets-and-query.md)
- [Source operations](source-operations.md)
- [Portfolio and paper execution](portfolio-and-paper-execution.md)
- [Backup and recovery](backup-and-recovery.md)
- [Mutable delivery ledger](../plans/delivery-ledger.md)
- [FRED rights decision](../verification/fred-rights-decision.json)
- [FRED/ALFRED local-first API authority](../research/providers/2026-07-26-fred-alfred-local-first-api-authority.md)
- [`application/research/ingest.rs`](../../apps/market-squawk/src/application/research/ingest.rs)
- [`local_product/cli_transport/files.rs`](../../apps/market-squawk/src/local_product/cli_transport/files.rs)
- [`data/ingest.rs`](../../crates/market-squawk-data/src/ingest.rs)
- [`adapter-files/manifest.rs`](../../adapters/market-squawk-adapter-files/src/manifest.rs)
- [`adapter-sec/extraction.rs`](../../adapters/market-squawk-adapter-sec/src/extraction.rs)
- [`adapter-fred/client/lineage.rs`](../../adapters/market-squawk-adapter-fred/src/client/lineage.rs)
- [`adapter-fred/rights.rs`](../../adapters/market-squawk-adapter-fred/src/rights.rs)
- [`adapter-bls/source.rs`](../../adapters/market-squawk-adapter-bls/src/source.rs)
- [`adapter-treasury/source.rs`](../../adapters/market-squawk-adapter-treasury/src/source.rs)
- [`sources/onboarding/built_in_profiles.rs`](../../crates/market-squawk-sources/src/onboarding/built_in_profiles.rs)

## External sources

External provider and format references were rechecked on 2026-07-26. Market Squawk's accepted
rights evidence and frozen source remain authoritative for what the product may persist.

- [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC developer resources](https://www.sec.gov/about/developer-resources)
- [FRED/ALFRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)
- [FRED series-observations endpoint](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
- [FRED API terms of use](https://fred.stlouisfed.org/docs/api/terms_of_use.html)
- [Current FRED legal terms](https://fred.stlouisfed.org/legal/)
- [FRED permissions contact route](https://fred.stlouisfed.org/contactus/)
- [BLS Public Data API](https://www.bls.gov/developers/home.htm)
- [BLS API signatures](https://www.bls.gov/developers/api_signature.htm)
- [Treasury Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/)
- [Treasury daily interest-rate XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
- [Treasury daily-rates source and rights decision](../research/providers/2026-07-26-treasury-daily-rates-release-authority.md)
- [Apache Arrow format specification](https://arrow.apache.org/docs/format/)
- [Apache Parquet format specification](https://github.com/apache/parquet-format)
