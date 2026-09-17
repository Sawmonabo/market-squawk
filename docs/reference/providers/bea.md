# Bureau of Economic Analysis provider contract

BEA is Market Squawk's selected direct source for national, regional, industry, income, and
international economic accounts. The upstream API is metadata-driven; a fixed table parser is not
an acceptable provider contract.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Selected target provider; credential and probe evidence exist, adapter and product composition do not |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Credential input | `BEA_ENABLED`, `BEA_USER_ID` |
| Canonical destination | Macro `market_squawk.research_observations` |

## Role and product workflows

**APPLICATION POLICY:** BEA supplies GDP, national accounts, personal income, industry and
regional activity, corporate/economic aggregates, and international-account evidence. These rows
support regime and sector features, forecasts, valuation context, backtests, opportunity screening,
and investment briefs. They do not supply current security prices.

Dataset and parameter selection is discovered from BEA rather than encoded as one permanent list.
Each admitted series is an exact dataset-plus-dimensions coordinate with explicit unit and
multiplier.

## Authentication and setup

**VERIFIED PROVIDER FACT:** registration yields a 36-character `UserID` after the user supplies a
name or organization, a valid non-disposable email, accepts the registration terms, and follows the
activation link. The identifier is sent as the `UserID` GET query parameter. See the
[BEA API guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf) and
[signup page](https://apps.bea.gov/api/signup/).

**APPLICATION POLICY:** `BEA_USER_ID` is imported into protected provider state and redacted from
URLs, logs, traces, errors, receipts, and diagnostics. Endpoint, method, dataset, parameters, and
budgets are code-owned.

## Exact surface and data families

**VERIFIED PROVIDER FACT:** every reviewed request uses
`GET https://apps.bea.gov/api/data` with `UserID` and `Method`. JSON and XML are supported; JSON is
the default response format.

| Method | Admitted use |
| --- | --- |
| `GetDatasetList` | Discover current dataset identities |
| `GetParameterList` | Discover one dataset's required/optional parameters, types, defaults, multi-value behavior, and any all-values marker |
| `GetParameterValues` | Discover admitted values for one parameter |
| `GetParameterValuesFiltered` | Narrow parameter values where that dataset implements the method; API error 34 means it is unsupported |
| `GetData` | Retrieve observations for one exact discovered dataset/parameter request |

**VERIFIED PROVIDER FACT:** `GetData` responses are dataset-specific. `Dimensions` identifies
attributes such as name, ordinal, type, and whether an attribute is the value; attributes are not
guaranteed to arrive in one order. Common semantics include `CL_UNIT`, `UNIT_MULT`, data rows,
`Note`, and `NoteRef`.

## Provenance, clocks, revisions, and corrections

Each raw/canonical record retains the dataset, method, complete sorted parameter set, metadata-
discovery generation, dimension members, exact raw value, unit, multiplier, notes, observation
period, provider response identity, local receipt/availability/publication times, and source
revision relationship.

**VERIFIED PROVIDER FACT:** BEA may incorporate a substantive correction into a regularly
scheduled revision or publish a separate correction notice. Small supporting-table errors can have
cell-specific errata that remain only until the next estimate release. See the
[BEA Correction Policy](https://www.bea.gov/about/policies-and-information/correction).

**APPLICATION POLICY:** metadata, estimate payloads, correction notices, corrected payloads, and
superseding releases are separate immutable evidence. A notice can be observed before the corrected
data, which produces an unresolved-correction state rather than a guessed replacement.

## Official limits and adaptive admission

| Dimension | Contract |
| --- | --- |
| Requests | **VERIFIED PROVIDER FACT:** 100 requests per minute per API user |
| Response bytes | **VERIFIED PROVIDER FACT:** 100 MB retrieved per minute per API user |
| Errors | **VERIFIED PROVIDER FACT:** 30 errors per minute per API user |
| Throttle response | **VERIFIED PROVIDER FACT:** HTTP 429 with `Retry-After`; the timeout is currently one minute but can change dynamically |
| Row/schema ceiling | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no universal maximum row count or common dataset schema is published |

**APPLICATION POLICY:** one shared BEA queue reserves 60 requests, 60 MB, and 10 errors per minute,
normally serializes acquisition, honors `Retry-After`, and rejects or splits broad `ALL`/`X`
selectors before sending them. These are application budgets, not provider limits.

The ledger records every attempt, response bytes, provider/body errors, latency, metadata
generation, returned rows, and retry/cooldown. Desktop, CLI, MCP, and background jobs cannot own
independent counters for the same UserID.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** the configured `GetDatasetList` probe returned HTTP 200 on 2026-08-11.
It proves credential reachability and the metadata envelope only; it does not prove any dataset's
full schema, correction behavior, throughput, or end-to-end product availability.

## Canonical schema, storage, and PIT destination

Raw metadata and data responses are bounded, content-addressed objects. Provider-native parsing is
method- and dataset-aware and addresses attributes by name. Canonical rows map into
`MacroObservation` or a closed extension where an observation needs additional dimensional
coordinates; arbitrary provider JSON is not stored as the canonical payload.

Immutable Parquet generations publish under `market_squawk.research_observations`; SQLite owns the
credential generation, multidimensional budget, discovered datasets/parameters, jobs, checkpoints,
correction state, manifests, and restart recovery. PIT selectors choose only the payload and
metadata generation locally available at the decision cutoff. Derived features bind all parents.

## Scheduling and degradation

**APPLICATION POLICY:** perform metadata discovery on activation, on a reviewed contract refresh,
or when a known dataset changes—not before every data request. Retrieve observations according to
the dataset's actual release cadence and use exact incremental periods where supported.

Expected releases, interactive requests, and correction reconciliation outrank historical
backfills. Byte pressure pauses broad history first. A 429, error-budget pressure, metadata drift,
or unresolved correction degrades only BEA-dependent features; it does not block current-market
workflows.

## Current repository integration seams and status

Repository inspection found no BEA adapter crate, built-in provider profile, canonical mapper,
publication job, typed application read, or Desktop composition. The credential template and
account-setup page describe the target input, and the configured read-only probe succeeded, but
those are not an implemented vertical.

Implementation must reuse the existing provider profile/onboarding, protected-secret,
multidimensional rate authority, raw-object capture, `MacroObservation`, Arrow/Parquet registry,
manifest, PIT selector, job/checkpoint, and typed-operation boundaries. It must not introduce a
separate macro store or generic API client exposed to the frontend.

## Doctor and end-to-end acceptance gates

The provider doctor must:

1. validate/redact the UserID and call `GetDatasetList`;
2. select one code-owned dataset and traverse dataset, parameter, and parameter-value metadata;
3. make one bounded `GetData` request and validate the echoed request, BEAAPI envelope, dimensions,
   unit/multiplier, notes, returned rows, bytes, and provider errors;
4. record `Retry-After`/throttle evidence or its absence and return a redacted readiness receipt.

End-to-end acceptance requires frozen metadata, a complete raw response, closed canonical rows,
immutable publication, PIT selection, correction/supersession behavior, restart recovery, and a
bounded macro or investment-evidence operation consumed by the Console. A metadata-only probe does
not enable the workflow.

## Hard gaps

- Dataset schemas and appendices evolve independently; there is no universal row layout or maximum.
- The correction policy exposes no machine-readable correction API, durable correction ID, fixed
  deadline, or complete affected-series mapping.
- The API contract does not provide ALFRED-style historical-as-known vintages for every dataset.
- No BEA adapter, durable job, PIT read, or product workflow currently exists in the repository.

## First-party sources

- [BEA API for Data Retrieval User Guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf)
- [BEA API signup](https://apps.bea.gov/api/signup/)
- [BEA Correction Policy](https://www.bea.gov/about/policies-and-information/correction)

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
