# FRED and ALFRED API rights decision — 2026-07-22

> Superseded on 2026-07-26 by the
> [FRED/ALFRED local-first API authority](2026-07-26-fred-alfred-local-first-api-authority.md).
> This file remains immutable historical evidence of the earlier blanket-blocked decision.

Status: historical, superseded release decision
Scope: FRED and ALFRED API access, durable local storage, analytical use, and model use  
Review trigger: any change to the cited terms, API-key contract, requested operation, or series owner

This note records the engineering release decision derived from the current official Federal
Reserve Bank of St. Louis pages. It supersedes the narrower FRED/ALFRED interpretation recorded in
the provider table of `2026-07-18-usable-release-dependencies.md`. It does not determine ownership
or permission for an individual third-party series.

## Official facts

- The FRED API provides access to both FRED and ALFRED data. ALFRED exposes historical real-time
  periods and vintages through the same API family.
- API access requires a user account and API key. The current v2 documentation requires an
  application-specific key presented as a bearer credential.
- The current legal terms prohibit using the API in connection with developing or training
  software-based models or algorithms, including machine-learning and generative-AI systems.
- The current legal terms also prohibit using the API in connection with storing, caching, or
  archiving the service or content, including incorporation into a database, compilation, archive,
  or cache.
- Series supplied by third parties may carry additional copyright or use restrictions. Access to
  the API and possession of a key do not establish permission for every series or operation.
- The terms require provider attribution and user-facing notices when an application uses the API.

These findings come from the following official pages, reviewed on 2026-07-22:

- [FRED legal terms](https://fred.stlouisfed.org/legal/terms/)
- [FRED API terms of use](https://fred.stlouisfed.org/docs/api/terms_of_use.html)
- [FRED API v2 key contract](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html)
- [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)

## Market Squawk decision

The FRED/ALFRED adapter remains a supported zero-fee technical adapter, and the onboarding service
may guide a user through obtaining and storing their own API key. That credential proves only
authentication. It must not activate durable ingestion, Parquet publication, DataFusion catalog
publication, feature generation, model training, model inference inputs, export, or AI-facing use.

Those operations remain `RightsBlocked` by default. They may be enabled only through separately
retained authority that identifies the exact operation, series scope, source of permission, review
date, and expiry or refresh condition. A general acknowledgement or successful API request is not
such authority.

The adapter may perform a bounded operation only when that exact operation is admitted under the
current terms and the selected series' rights. Response bytes must not silently enter the durable
research pipeline through logs, caches, retry storage, fixtures, or generic artifact capture.

## Release impact

- The macro research plane remains usable with zero-fee official sources whose exact operations are
  admitted, including BLS and applicable U.S. Treasury Fiscal Data surfaces.
- FRED/ALFRED parsing, pagination, real-time-bound, vintage, revision, and missing-value behavior
  remain production requirements for the adapter; rights enforcement is part of that production
  behavior, not a stub or deferred adapter.
- A clean-machine release demonstration must show the FRED/ALFRED hard gate and must not claim
  durable FRED/ALFRED research or modeling without separately retained permission.
- The product-level requirement for durable FRED/ALFRED use remains externally blocked until the
  necessary scope-specific permission exists or the official terms materially change. Alternative
  macro providers preserve product usability but do not get relabeled as FRED/ALFRED coverage.

## Required implementation invariants

1. Provider account state, credential validity, source availability, rate policy, and data-use
   authority remain independent states.
2. Rights are evaluated before extraction can reserve durable publication or model use.
3. The exact terms reference and review timestamp are retained with every admitted decision.
4. Series-level third-party restrictions can narrow an otherwise admitted provider operation.
5. A terms or permission change invalidates only the affected capability and fails it closed until
   reviewed again.
6. Bounded credential validation must avoid retaining provider content when durable storage is not
   admitted.
