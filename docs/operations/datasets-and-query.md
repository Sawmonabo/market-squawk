# Dataset build and query operations

This runbook covers immutable dataset inspection, phase-one point-in-time feature/label generation,
receipt-admitted feature-product inspection, bounded dataset reads, and the CLI-only read-only SQL
surface.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Local research operators, feature producers, data stewards, and incident responders |
| Status | Current, with analytical-query overflow limitations called out below |
| Last substantive review | 2026-08-12 |
| Review basis | Current phase-one generation and receipt-admitted feature-product contracts; not release approval evidence |

## Contents

- [Scope](#scope)
- [Safety and authority boundaries](#safety-and-authority-boundaries)
- [Preconditions](#preconditions)
- [Inspect datasets and feature contracts](#inspect-datasets-and-feature-contracts)
- [Prepare an exact point-in-time build](#prepare-an-exact-point-in-time-build)
- [Build the phase-one derived generation](#build-the-phase-one-derived-generation)
- [Read a dataset without SQL](#read-a-dataset-without-sql)
- [Run bounded read-only SQL](#run-bounded-read-only-sql)
- [Success evidence](#success-evidence)
- [Idempotency, rollback, and recovery](#idempotency-rollback-and-recovery)
- [Failure modes](#failure-modes)
- [Local state locations](#local-state-locations)
- [Related documentation, code, and evidence](#related-documentation-code-and-evidence)
- [External sources](#external-sources)

## Scope

This page documents the shipping commands:

```text
dataset list
dataset manifest <DATASET>
dataset build <REQUEST> --confirm
feature list
feature build <REQUEST> --confirm
query dataset <DATASET> [--maximum-rows <COUNT>]
query sql --dataset <DATASET> <STATEMENT> [--maximum-rows <COUNT>]
```

It covers exact parent-manifest pins, historical-universe membership, availability and revision
selection, chronological splits, feature/label evidence, corporate-action treatment, output
authorization, caller limits, immutable publication, bounded query results, and recovery.

It does not calculate feature or label values for the caller. The build boundary admits
caller-materialized examples and independently verifies their selectors, source generations,
temporal cutoffs, universe evidence, adjustment evidence, rights, and limits. It also does not
provide arbitrary SQL over the product database, mutate a generation, expose historical manifests
by version through the CLI, or grant order authority. Dataset and query outputs remain research
data and report `executionEligible: false`.

`dataset build` and `feature build` publish the same kind of immutable phase-one derived
generation. They return a reproducible phase-one descriptor digest, but they do not issue a product
receipt, admit a closed feature-dataset product, or create a training-ready Python export. Product
admission is a separate code-owned operation over an exact closed Analysis or Training contract.

## Safety and authority boundaries

- `dataset build` and `feature build` are durable mutations and require `--confirm`. They invoke
  the same phase-one point-in-time dataset builder; neither operation issues a product receipt or
  admission.
- Use one explicit `--data-dir` throughout a workflow. The default is `.market-squawk`, relative to
  the current working directory, so an omitted root can silently address a different catalog.
- Build request files are opened as bounded, user-owned, regular files without following symlinks.
  They may contain at most 8 MiB and one closed top-level JSON object. Unknown fields are rejected.
- A parent is authorized by its complete immutable manifest tuple, not by dataset name alone.
  Preserve `dataset`, `version`, `schema`, `schemaVersion`, `schemaFingerprintSha256`, and
  `contentSha256` exactly.
- The builder checks what was knowable by each example cutoff. Do not replace evidenced
  availability with an earlier timestamp or omit a revision conflict to make a build pass.
- `intendedUse: "train"` requests and verifies source rights for that use. It does not create the
  separately required Training product receipt.
- `query sql` registers exactly one pinned generation as the relation `dataset`. It cannot query
  `catalog.sqlite3`, attach another database, read files, invoke table functions, or mutate state.
- Generic SQL is deliberately absent from MCP. Remote/agent callers use the typed MCP operations
  documented in [MCP reference](../reference/mcp.md); only the local CLI owns the bounded SQL
  surface.
- Do not edit catalog rows, Parquet objects, transient query reservations, terminal artifact
  objects, or Python admission rows by hand. Use exact retries, a new immutable generation, or the
  supported backup-and-restore procedure.

## Preconditions

Set and validate the controlled root:

```bash
export DATA_ROOT=/absolute/path/to/market-squawk-data
market-squawk --data-dir "$DATA_ROOT" config validate
market-squawk --data-dir "$DATA_ROOT" doctor
```

Before building, confirm that:

- every parent generation has already been ingested or published under this root;
- every source in the transitive parent graph authorizes the requested `intendedUse`;
- parent, universe, component, and example evidence was produced together rather than assembled
  from unrelated runs;
- feature cutoffs precede their label cutoffs, and the train, validation, and test ends are strictly
  increasing;
- every component is materialized already, with at least one exact observation-family selector;
  and
- the request file is retained with the publication receipt for reproducibility.

Use [Research ingestion operations](research-ingestion.md) when the required parent generation does
not exist yet.

## Inspect datasets and feature contracts

### List current dataset generations

```bash
market-squawk --data-dir "$DATA_ROOT" --output json dataset list
```

The command returns at most the first 64 dataset identities in catalog order and selects the latest
generation for each identity. A nonempty result has:

- `items`, with one generation receipt per dataset;
- `hasMore`; and
- `nextAfterDataset` when another page exists.

When `hasMore` is true, request the next bounded page with the returned cursor:

```bash
market-squawk --data-dir "$DATA_ROOT" --output json \
  dataset list --after-dataset '<nextAfterDataset>'
```

Continue until `hasMore` is false. The cursor is a dataset identity from the preceding page; do not
construct or modify it. Address a known dataset directly with `dataset manifest` when a full
inventory is unnecessary.

Each listed generation includes:

- `manifest.datasetId`, `manifest.manifestVersion`, schema name/version/fingerprint, and
  `manifest.contentHash`;
- `sourceId` and `generationKind` (`ingest`, `derived`, or an internally produced `compaction`);
- optional `buildSpecDigest`;
- exact parent relations and manifests;
- `rowCount`, `totalBytes`, `lineageDigest`, and `objectCount`.

A derived generation additionally reports `publicationStage: "phase_one_derived_generation"`, a
nullable `phaseOneDescriptorSha256`, and
`productAdmission: "not_established_on_this_surface"`. That last field means this generic
generation read is not product-admission authority; it is not evidence that the same manifest has
never been admitted through a separate receipt-backed product surface.

There is no public compaction command. A listed `generationKind: "compaction"` is descriptive
evidence, not permission to invent a manual compaction procedure.

### Pin one current parent

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  dataset manifest <PARENT_DATASET_ID>
```

Record the complete `manifest` object and the parent generation's `lineageDigest`. The command
returns the latest generation only. It does not accept a manifest version, so it cannot recover an
older pin from a dataset name. Use a previously retained build/ingestion receipt or an accepted
backup/evidence path when an older exact tuple is required.

The manifest read shape differs from the build-result shape:

| Manifest read field | Build request/build result field |
| --- | --- |
| `datasetId` | `dataset` |
| `manifestVersion` | `version` |
| `schema.name` | `schema` |
| `schema.version` | `schemaVersion` |
| `schema.fingerprint` | `schemaFingerprintSha256` |
| `contentHash` | `contentSha256` |

Translate names only; copy the values byte-for-byte.

### Inspect feature contracts

```bash
market-squawk --data-dir "$DATA_ROOT" --output json feature list
```

Local composition returns the code-owned batch feature catalog followed by durable
receipt-admitted Analysis feature datasets from the analytical catalog. A successful generic
`dataset build` or `feature build` is inspectable through `dataset list`, `dataset manifest`, and
the query commands; it does not appear in `feature list` merely because its phase-one build
succeeded. Each receipt-admitted feature-dataset entry includes its exact manifest, build, policy,
universe, split, source, and `pythonExportSha256` identities.

The installed reader is bounded to the closed Analysis contract
`market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.analysis/v1`.
`feature list` is not Training-contract authority. Model training remains unavailable until a
code-owned producer issues the separate
`market-squawk.feature-dataset.price-return-fixed-horizon-forward-return.training/v1` receipt for
the exact generation and export.

When a page reports more entries, continue with the last returned dataset identity:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  feature list \
  --after-dataset <LAST_DATASET_ID>
```

## Prepare an exact point-in-time build

The build JSON is camelCase, except for tagged enum values such as `latest_known`,
`split_adjusted`, and `request_file_ownership`. It has exactly these required top-level fields:

```json
{
  "outputDataset": "...",
  "parents": [],
  "universe": {},
  "componentSpecs": [],
  "examples": [],
  "policy": {},
  "intendedUse": "local_analysis",
  "researchUseLimits": {},
  "outputAuthorization": {},
  "limits": {}
}
```

The empty collections above document structure only and are not a valid request. The builder
requires nonempty parents, universe memberships, component specifications, and examples, including
at least one feature and one label specification.

### 1. Copy every immutable parent pin

Each entry in `parents` has the closed shape:

```json
{
  "dataset": "parent-dataset-id",
  "version": 7,
  "schema": "market_squawk.research_observations",
  "schemaVersion": 1,
  "schemaFingerprintSha256": "<64-lowercase-hex>",
  "contentSha256": "<64-lowercase-nonzero-hex>"
}
```

The schema reference must resolve in the local schema registry. A dataset name with a mismatched
version, schema fingerprint, or content digest is rejected rather than silently upgraded to the
latest generation.

### 2. Bind historical universe membership

`universe` has one stable identity and one or more memberships:

```json
{
  "id": "us-large-cap-research-v1",
  "memberships": [
    {
      "instrumentId": "11111111-1111-4111-8111-111111111111",
      "startsAtUnixNanos": 1704067200000000000,
      "endsAtUnixNanos": null,
      "availability": {
        "kind": "evidenced",
        "availableAtUnixNanos": 1704153600000000000,
        "evidence": "constituent-publication-2024-01-02"
      },
      "sourceManifest": {
        "dataset": "parent-dataset-id",
        "version": 7,
        "schema": "market_squawk.research_observations",
        "schemaVersion": 1,
        "schemaFingerprintSha256": "<64-lowercase-hex>",
        "contentSha256": "<64-lowercase-nonzero-hex>"
      },
      "evidenceSha256": "<64-lowercase-nonzero-hex>"
    }
  ]
}
```

Every membership's `sourceManifest` must be one of `parents`. The admitted availability variants
are:

| `kind` | Required fields | Meaning |
| --- | --- | --- |
| `evidenced` | `availableAtUnixNanos`, `evidence` | The source provides an exact availability fact |
| `local_first_observed` | `observedAtUnixNanos` | The bounded local observation time is the evidence |
| `inferred` | `inferredAtUnixNanos`, `method` | A declared inference supplies availability |
| `unknown` | none | Availability is unknown and cannot be treated as earlier evidence |

All integer timestamps in the build contract are Unix nanoseconds.

### 3. Declare versioned components and materialized examples

Each component specification appears once in `componentSpecs` and is repeated exactly in the
corresponding example component:

```json
{
  "kind": "feature",
  "scope": "instrument",
  "corporateActions": "not_applicable",
  "name": "alternative-score",
  "version": 1
}
```

Closed values are:

- `kind`: `feature` or `label`;
- `scope`: `instrument`, `account`, or `global`; and
- `corporateActions`: `not_applicable` or `requires_adjustment`.

An example has a non-nil instrument UUID, a feature cutoff, a strictly later label cutoff, and one
component for every declared contract:

```json
{
  "exampleId": "example-2024-01-03-instrument-1",
  "instrumentId": "11111111-1111-4111-8111-111111111111",
  "cutoffAtUnixNanos": 1704240000000000000,
  "labelCutoffAtUnixNanos": 1704326400000000000,
  "components": [
    {
      "spec": {
        "kind": "feature",
        "scope": "instrument",
        "corporateActions": "not_applicable",
        "name": "alternative-score",
        "version": 1
      },
      "value": {
        "kind": "float",
        "value": 0.0125,
        "unit": "ratio",
        "currency": null
      },
      "selectors": [
        {
          "kind": "alternative_data",
          "sourceId": "licensed-source-id",
          "instrumentId": "11111111-1111-4111-8111-111111111111",
          "sourceRecord": "vendor-record-2024-01-02",
          "dataset": "alternative-prices",
          "field": "score",
          "effective": {
            "precision": "exact_timestamp",
            "unixNanos": 1704153600000000000
          }
        }
      ],
      "adjustment": {
        "kind": "not_applicable"
      }
    }
  ]
}
```

That fragment illustrates exact field names; it is not a complete request because a valid build
also needs a label component. A component has 1–64 distinct selectors. Selector families are
closed to `filing`, `fundamental`, `macro`, `portfolio_position`, `transaction`,
`corporate_action`, `universe_membership`, and `alternative_data`, with the fields defined by the
request decoder linked below.

Component values are:

- `{"kind":"float","value":<finite-number>,"unit":<string-or-null>,"currency":<code-or-null>}`;
- `{"kind":"decimal","value":"<exact-decimal>","unit":<string-or-null>,"currency":<code-or-null>}`;
  or
- `{"kind":"missing","reason":"<source-identifier>"}`.

Adjustment evidence is `{"kind":"raw"}`, `{"kind":"not_applicable"}`, or:

```json
{
  "kind": "applied",
  "policy": {
    "adjustment": "split_adjusted",
    "version": 1
  },
  "planContentSha256": "<64-lowercase-nonzero-hex>",
  "planAuditSha256": "<64-lowercase-nonzero-hex>",
  "implementationSha256": "<64-lowercase-nonzero-hex>"
}
```

The builder compares supplied component values and selectors with exact parent observations. It
does not derive a return, label, normalization, or corporate-action adjustment on the caller's
behalf.

### 4. Set split, revision, adjustment, and missing-value policy

```json
{
  "split": {
    "trainEndUnixNanos": 1735603200000000000,
    "validationEndUnixNanos": 1743379200000000000,
    "testEndUnixNanos": 1751241600000000000
  },
  "pointInTime": {
    "version": 1,
    "revisionMode": "latest_known"
  },
  "corporateActions": {
    "adjustment": "raw",
    "version": 1
  },
  "missingValues": "reject",
  "implementationRevision": "feature-producer-commit-or-release-id"
}
```

Use this object as `policy`. Split ends are inclusive and strictly increasing. Examples after
`testEndUnixNanos` are not admitted.

`revisionMode` means:

- `latest_known`: choose the latest revision whose availability was at or before the relevant
  cutoff; or
- `all_known`: retain all qualifying known revisions according to the point-in-time selector.

Conflicting eligible revisions are not silently resolved beyond the declared policy. Conflict
counts consume the caller's `maxConflicts` budget; invalid or ambiguous evidence fails closed.

Corporate-action `adjustment` is `raw`, `split_adjusted`, or `total_return`. Non-raw component
values marked `requires_adjustment` need exact applied plan and implementation evidence.
`missingValues` is `reject`, `preserve`, or `drop_example`.

### 5. Authorize use and output

`intendedUse` is one of `display`, `local_analysis`, or `train`. The builder checks that every
transitive source authorizes that use.

Use `request_file_ownership` only when the build request itself is the truthful user-owned
authority:

```json
{
  "sourceId": "local-feature-producer",
  "basis": {
    "kind": "request_file_ownership"
  },
  "authorizationSha256": "<64-lowercase-nonzero-hex>",
  "authorizationExpiresAtUnixNanos": null
}
```

For reviewed provider terms, use:

```json
{
  "sourceId": "licensed-source-id",
  "basis": {
    "kind": "reviewed_terms",
    "url": "https://provider.example/terms",
    "termsSha256": "<64-lowercase-nonzero-hex>"
  },
  "authorizationSha256": "<64-lowercase-nonzero-hex>",
  "authorizationExpiresAtUnixNanos": 1784937600000000000
}
```

These examples are the two accepted JSON shapes, not a substitute for actual ownership or terms
review. An expired or inconsistent authorization fails closed.

### 6. Choose explicit resource ceilings

All limits must be nonzero and no greater than these process ceilings:

| Request field | Maximum |
| --- | ---: |
| `researchUseLimits.maxRoots` | 256 |
| `researchUseLimits.maxNodes` | 100,000 |
| `researchUseLimits.maxEdges` | 400,000 |
| `researchUseLimits.maxSources` | 100,000 |
| `researchUseLimits.maxRetainedBytes` | 64 MiB |
| `researchUseLimits.traversalDeadlineMillis` | 30,000 |
| `researchUseLimits.permitLifetimeMillis` | 300,000 |
| `limits.maxInputRows` | 1,000,000 |
| `limits.maxExamples` | 1,000,000 |
| `limits.maxComponentsPerExample` | 1,024 |
| `limits.maxOutputRows` | 10,000,000 |
| `limits.maxRetainedBytes` | 1 GiB |
| `limits.maxDurationMillis` | 300,000 |
| `limits.pointInTime.maxCandidates` | 1,000,000 |
| `limits.pointInTime.maxFamilies` | 1,000,000 |
| `limits.pointInTime.maxConflicts` | 100,000 |
| `limits.pointInTime.maxResultRows` | 1,000,000 |
| `limits.pointInTime.maxRetainedBytes` | 512 MiB |
| `limits.universe.maxCandidates` | 1,000,000 |
| `limits.universe.maxRetainedBytes` | 512 MiB |
| `limits.corporateActions.maxActions` | 1,000,000 |
| `limits.corporateActions.maxRetainedBytes` | 512 MiB |

Choose the smallest values that admit the reviewed build. These are ceilings, not recommended
defaults. The exact nested JSON field names are:

```json
{
  "researchUseLimits": {
    "maxRoots": 16,
    "maxNodes": 10000,
    "maxEdges": 40000,
    "maxSources": 10000,
    "maxRetainedBytes": 16777216,
    "traversalDeadlineMillis": 30000,
    "permitLifetimeMillis": 300000
  },
  "limits": {
    "maxInputRows": 100000,
    "maxExamples": 10000,
    "maxComponentsPerExample": 64,
    "maxOutputRows": 100000,
    "maxRetainedBytes": 268435456,
    "maxDurationMillis": 300000,
    "pointInTime": {
      "maxCandidates": 100000,
      "maxFamilies": 100000,
      "maxConflicts": 1000,
      "maxResultRows": 100000,
      "maxRetainedBytes": 134217728
    },
    "universe": {
      "maxCandidates": 100000,
      "maxRetainedBytes": 134217728
    },
    "corporateActions": {
      "maxActions": 100000,
      "maxRetainedBytes": 134217728
    }
  }
}
```

## Build the phase-one derived generation

Save the complete closed request as a regular file under a user-owned directory, then run one of:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  dataset build /absolute/path/to/build-request.json \
  --confirm
```

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  feature build /absolute/path/to/build-request.json \
  --confirm
```

On success, retain the complete result:

```json
{
  "publicationStage": "phase_one_derived_generation",
  "productAdmission": "not_admitted_by_phase_one_operation_at_completion",
  "manifest": {
    "dataset": "...",
    "version": 1,
    "schema": "...",
    "schemaVersion": 1,
    "schemaFingerprintSha256": "...",
    "contentSha256": "..."
  },
  "buildSpecSha256": "...",
  "policySha256": "...",
  "universeSha256": "...",
  "phaseOneDescriptorSha256": "...",
  "splitExamples": {
    "train": 6,
    "validation": 2,
    "test": 2
  }
}
```

The numbers and digests above illustrate the output shape. Check a real result against the reviewed
request's expected phase-one split counts.

The builder publishes an immutable Parquet generation, records exact lineage, and returns the
digest of its reproducible phase-one descriptor. Retain `phaseOneDescriptorSha256` with the
complete result; `contentSha256`, `buildSpecSha256`, `policySha256`, and `universeSha256` identify
different objects and are not substitutes. The phase-one descriptor is not a receipt-admitted
Python export, and its digest must not be supplied as `exportSha256` to model training.

Only the code-owned feature-dataset producer and non-duplicable publication authority can bind an
exact closed product contract, rights, point-in-time evidence, and product receipt to a generation.
Analysis and Training are distinct product contracts. Training remains unavailable until the
exact Training receipt exists; no command in this generic build procedure promotes the generation.

## Read a dataset without SQL

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  query dataset <DATASET_ID> \
  --maximum-rows 1000
```

The default maximum is 1,000 rows. The command resolves the latest generation at request start,
pins its exact manifest, and reads canonical observations using `Research.GetHistory`. The CLI
does not expose the typed service's instrument or knowledge-time filters, so reduce
`--maximum-rows` when inspecting an unfamiliar dataset.

This command uses the fixed-template application query path. That path derives its query
inline-byte and complete-result ceilings from the caller's admitted service limits, admits four
partitions, 2,048 syntax-tree nodes, 4,096 plan nodes, and at most 60 seconds, and gives the query
four times its complete-result ceiling in memory within the code-owned clamp. The CLI supplies a
16 MiB inline and complete-result ceiling, so `query dataset` itself has no overflow band: it
returns `rows` plus `arrowIpcBytes` or fails closed at that limit.

The same fixed-template service over production MCP starts with a 64 KiB inline ceiling and a
64 MiB hard complete-result ceiling; the requested `resultLimits.maximumBytes` may narrow the
latter. When the admitted inline ceiling is lower than the complete-result ceiling, the service
verifies an oversized result and republishes it as opaque `application/vnd.apache.parquet` for
retrieval through `Analysis.ReadArtifact`. MCP does not expose general SQL.

For a successful inline result retain:

- the returned manifest;
- source ID;
- object-graph, query-identity, and result digests from `metadata.sourceCoverage`; and
- `metadata.dataQuality`, including `executionEligible: false`.

## Run bounded read-only SQL

Run SQL only through the local CLI:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  query sql \
  --dataset <DATASET_ID> \
  'SELECT * FROM dataset LIMIT 100' \
  --maximum-rows 100
```

The command resolves and pins the latest immutable generation before planning. The sole base
relation is literally named `dataset`. The statement:

- is one UTF-8 statement of at most 64 KiB with no NUL;
- must be a `SELECT`, CTE, subquery, or `EXPLAIN` over `dataset` and/or its CTEs;
- cannot contain mutation, DDL, another relation, a file/object-store locator, or a table function;
  and
- may call only `abs`, `avg`, `coalesce`, `count`, `date_trunc`, `lower`, `max`, `min`, `round`,
  `sum`, `upper`, and the SQL syntax admitted by the pinned DataFusion implementation.

The caller's `--maximum-rows` defaults to 1,000. Execution is additionally bounded to 256 KiB of
inline Arrow IPC, 64 MiB for the complete result, 256 MiB of query memory, four partitions,
2,048 AST nodes, 4,096 plan nodes, and 60 seconds. An inline success includes the pinned manifest,
`relation: "dataset"`, `arrowIpcBytes`, rows, source coverage, and
`executionEligible: false`.

A verified result above 256 KiB and at most 64 MiB is republished into the shared terminal
repository as durable content-addressed Parquet. The command returns one `artifact` object with
`artifactId`, `sha256`, `byteCount`, `mediaType`, and `rowCount`; its media type is exactly
`application/vnd.apache.parquet`. It intentionally contains no internal reservation owner or
expiry. Retrieve bounded chunks using the complete returned identity:

```bash
market-squawk \
  --data-dir "$DATA_ROOT" \
  --output json \
  query artifact \
  --artifact-id <ARTIFACT_ID> \
  --sha256 <SHA256> \
  --byte-count <BYTE_COUNT> \
  --media-type application/vnd.apache.parquet
```

Use `--offset` and `--maximum-bytes` to continue from `nextOffset` until `complete` is true. MCP
clients use the same identity with `Analysis.ReadArtifact`.

## Success evidence

A completed build has all of the following:

1. Exit status `0` and a complete JSON result.
2. A manifest tuple whose dataset identity matches `outputDataset`.
3. `publicationStage: "phase_one_derived_generation"` and
   `productAdmission: "not_admitted_by_phase_one_operation_at_completion"`.
4. Nonzero `buildSpecSha256`, `policySha256`, `universeSha256`, and
   `phaseOneDescriptorSha256`.
5. Split counts that match the reviewed chronological policy and admitted examples.
6. A subsequent `dataset manifest <OUTPUT_DATASET>` result with the same manifest values after
   translating the field names shown above.
7. A bounded `query dataset` or `query sql` result pinned to that exact generation.
8. The original request, output, source/rights evidence, and operator review retained together.

These checks prove a phase-one generation, not product or training admission. Product evidence is a
separate receipt-backed feature-dataset read under its exact closed contract.

A completed read has exit status `0`, an exact returned manifest, bounded row counts, complete
source/result identity, and no execution authority. Empty or truncated-by-policy results are not
evidence that an expected row exists.

## Idempotency, rollback, and recovery

An exact replay of the same valid request is content-addressed. After rights are rechecked, it
reuses the identical derived generation rather than publishing a mutable replacement. A change to
parents, examples, policies, authorization, limits that participate in the build identity, or
producer revision creates a different build specification and, when admitted, a new immutable
generation.

Replaying a generic build never promotes that generation into an Analysis or Training product.

There is no CLI delete, de-publish, historical-version selector, manual compaction, or “make latest”
operation:

- If a command failed before success evidence, inspect the error and current
  `dataset manifest <OUTPUT_DATASET>`. Retry the byte-identical request only when its rights and
  evidence remain valid.
- If the new generation is semantically wrong, preserve it as audit evidence, correct the source
  facts or request, and publish a new generation. Never edit the object or catalog row.
- If latest selection is not the generation needed for a reproducible analysis, use a retained
  exact manifest through a product surface that accepts it; the reviewed CLI name-based query
  commands cannot select that historical version.
- If the catalog/object store is damaged or displaced, stop writers and follow
  [Backup and recovery](backup-and-recovery.md). Do not copy only `catalog.sqlite3` or only the
  Parquet objects.
- If a query returns an artifact reference, retain the complete identity and read verified chunks
  from the terminal repository. If it exceeds the complete-result ceiling, narrow the projection,
  predicate, grouping, or row ceiling and rerun. If latest has changed, the new result is not the
  same reproduction; compare the returned manifest first.

## Failure modes

| Symptom | Meaning | Safe response |
| --- | --- | --- |
| `phase-one derived-generation build requires explicit confirmation` | `--confirm` was omitted | Review the exact request, then rerun with confirmation |
| Request file is not admitted | It is too large, not regular, symlinked, escaped, or unreadable | Move a reviewed copy under one user-owned regular directory; do not weaken path checks |
| Request JSON is malformed or unsupported | Wrong casing, missing field, unknown field, or invalid tagged enum | Compare with the closed decoder and correct the request |
| Point-in-time, rights, or resource invariant failure | Parent pin, availability, selector, split, authorization, or bound is invalid | Correct the source evidence or lower the requested workload; do not alter timestamps/digests to force admission |
| Parent or dataset not found | The named/latest generation is absent under this data root | Confirm `--data-dir`, then inspect the exact parent receipt and catalog |
| Revision/universe/corporate-action conflict | Evidence cannot produce one result under the declared policy or exceeded its conflict budget | Review the competing source records and issue a new truthful policy/request |
| `hasMore: true` from `dataset list` or `feature list` | More identities exist after this stable page | Continue with `--after-dataset <LAST_DATASET_ID>` from the same operation |
| Phase-one generation absent from `feature list` | This is expected until a code-owned producer separately admits the exact Analysis product receipt; a wrong data root or page cursor may also exclude an already admitted product | Inspect the generation through `dataset list` or query; if a product is required, use its code-owned production path and never edit or relabel the phase-one descriptor. Training additionally requires its distinct Training receipt |
| Query limit/resource exhausted | Row, byte, memory, AST, plan, or deadline ceiling was reached | Narrow the read or SQL; do not increase beyond fixed ceilings |
| SQL statement/relation/function rejected | It is outside the read-only allowlist | Rewrite it as one bounded query over `dataset` using admitted functions |
| Dataset or SQL query reports resource exhaustion | The complete-result, row, memory, planning, or deadline ceiling was reached, or terminal publication/readback could not be verified | Reduce projection, predicates, grouping, or rows; never reconstruct a path or weaken artifact verification |

## Local state locations

All paths are relative to the selected data root:

| Path | Purpose | Operator rule |
| --- | --- | --- |
| `catalog.sqlite3` and active `catalog.sqlite3-wal`/`catalog.sqlite3-shm` | Manifests, lineage, phase-one build records, and separately receipt-admitted feature products | Treat as one SQLite consistency domain; never edit manually |
| `artifacts/objects/sha256/<first-two-hex>/<sha256>.parquet` | Immutable ingested and derived dataset objects | Content addressed; never rename, edit, or delete manually |
| `artifacts/mcp/v1/parquet/<first-two-hex>/<sha256>.parquet` | Terminal query-overflow objects retrievable by opaque reference | Durable and content addressed; retain and recover with the data root, and never infer this path from the public ID |
| The retained build request outside the data root | User-owned authority and exact build specification input | Keep with the success receipt and source evidence |

The query engine first uses an internal bounded reservation while producing overflow. The public
composition verifies that object and republishes its exact bytes into the terminal repository.
Internal owner and expiry coordinates govern only that transient handoff; they are not public
fields and do not define terminal repository retention.

## Related documentation, code, and evidence

Documentation:

- [Research ingestion operations](research-ingestion.md)
- [Model training and inference operations](model-inference.md)
- [Backup and recovery](backup-and-recovery.md)
- [Troubleshooting](troubleshooting.md)
- [CLI reference](../reference/cli.md)
- [MCP reference](../reference/mcp.md)
- [Data quality reference](../reference/data-quality.md)
- [Time and provenance reference](../reference/time-and-provenance.md)
- [Research data plane](../architecture/research-data-plane.md)
- [Control plane](../architecture/control-plane.md)
- [Mutable delivery ledger](../plans/delivery-ledger.md)

Reviewed code:

- [`apps/market-squawk/src/local_product/cli_dataset.rs`](../../apps/market-squawk/src/local_product/cli_dataset.rs)
- [`apps/market-squawk/src/local_product/cli_dataset_request.rs`](../../apps/market-squawk/src/local_product/cli_dataset_request.rs)
- [`apps/market-squawk/src/local_product/cli_transport/query.rs`](../../apps/market-squawk/src/local_product/cli_transport/query.rs)
- [`apps/market-squawk/src/application/research.rs`](../../apps/market-squawk/src/application/research.rs)
- [`apps/market-squawk/src/application/analysis.rs`](../../apps/market-squawk/src/application/analysis.rs)
- [`crates/market-squawk-data/src/dataset_builder.rs`](../../crates/market-squawk-data/src/dataset_builder.rs)
- [`crates/market-squawk-data/src/pit.rs`](../../crates/market-squawk-data/src/pit.rs)
- [`crates/market-squawk-data/src/query.rs`](../../crates/market-squawk-data/src/query.rs)
- [`crates/market-squawk-data/src/python_dataset.rs`](../../crates/market-squawk-data/src/python_dataset.rs)

## External sources

Direct upstream sources were rechecked on 2026-07-23:

- [Apache Arrow columnar format](https://arrow.apache.org/docs/format/)
- [Apache Parquet format](https://github.com/apache/parquet-format)
- [Apache DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/)
- [Apache DataFusion `SELECT` syntax](https://datafusion.apache.org/user-guide/sql/select.html)

Upstream documentation describes file formats and the underlying SQL engine. Market Squawk's
tracked schemas, allowlists, immutable-generation pins, rights checks, and resource ceilings remain
the controlling operator contract.
