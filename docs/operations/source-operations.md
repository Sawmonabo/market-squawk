# Source registration, onboarding, and status operations

This runbook operates Market Squawk's current code-owned source profiles, durable onboarding
sessions, bounded loopback setup portal, evidence-bound research-adapter activation, and
authority-free source status views.

| Field | Value |
| --- | --- |
| Document type | Operations runbook |
| Audience | Source operators, data-rights reviewers, incident responders, and maintainers |
| Status | Current |
| Last substantive review | 2026-08-12 |
| Review basis | `8fd91dad768affda2126ed91a5b97f1bd1e32209` plus the Wave 8B profile/documentation candidate; not provider-workflow or release acceptance |

## Contents

- [Scope and non-goals](#scope-and-non-goals)
- [Preconditions](#preconditions)
- [Safety and authority](#safety-and-authority)
- [Import the selected provider credential bundle](#import-the-selected-provider-credential-bundle)
- [Current code-owned source surfaces](#current-code-owned-source-surfaces)
- [Read source status, coverage, and health](#read-source-status-coverage-and-health)
- [Register a source profile](#register-a-source-profile)
- [Run the supported Treasury Fiscal Data setup](#run-the-supported-treasury-fiscal-data-setup)
- [Run the supported Treasury daily-rates setup](#run-the-supported-treasury-daily-rates-setup)
- [Understand the source activate boundary](#understand-the-source-activate-boundary)
- [Inspect one FRED or ALFRED page without persistence](#inspect-one-fred-or-alfred-page-without-persistence)
- [Discover and ingest an exact provider object](#discover-and-ingest-an-exact-provider-object)
- [Expected success evidence](#expected-success-evidence)
- [Rollback and recovery](#rollback-and-recovery)
- [Known failure modes](#known-failure-modes)
- [Local logs, data, and artifacts](#local-logs-data-and-artifacts)
- [Related documentation and code](#related-documentation-and-code)
- [Official sources](#official-sources)

## Scope and non-goals

The shipping source CLI is:

```text
source import-credentials <ABSOLUTE_FILE> --confirm
source register <PROVIDER> --confirm
source status [PROVIDER]
source coverage [PROVIDER]
source health [PROVIDER]
source setup <PROVIDER> --confirm
source discover <PROVIDER> --dataset <DATASET>
source inspect <PROVIDER> --onboarding-session-id <UUID> \
  --dataset-identifier <DATASET> [--page-index <0..63>] [--max-records <1..1024>]
source activate <REQUEST> --confirm
```

This page covers those exact commands, the one-time selected-provider credential import, the
bounded FRED/ALFRED inspection path, and the usable Treasury Fiscal Data and five-family
daily-rates portal paths. It explains how the same source surface can have distinct import,
profile, onboarding, extraction, publication, and live-runtime states.

In the current candidate there is no generic:

- `source start`;
- `source stop`;
- `source resynchronize`;
- `source unregister`;
- `source deactivate`;
- cross-process source monitor.

Do not invent those operations or translate bot/capture lifecycle commands into source commands.
Live venue reconnection and book recovery belong to their source runtime; research extraction is
performed by the ingest/dataset workflows after its adapter has been admitted. Local files,
portfolio imports, and paper execution use their own CLI domains rather than portal activation.

Provider availability and external terms can change after this review. The
[delivery ledger](../plans/delivery-ledger.md) is the sole mutable authority for release blockers
and accepted provider-qualification outcomes.

## Preconditions

- Install and bootstrap the complete executable bundle using
  [Installation and local bootstrap](installation-and-bootstrap.md).
- Validate the exact startup layers using
  [Configuration and secrets operations](configuration-and-secrets.md).
- Use the same explicit configuration and data root for every command in one workflow.
- Ensure the data root is backed up or otherwise protected before first use with a new application
  version.
- For `source setup`, use a local browser able to reach an ephemeral IPv4 loopback address and keep
  the launching terminal open.
- Permit outbound HTTPS only to the exact official provider endpoints required by the selected
  code-owned profile. Profile registration and status inspection do not themselves require a
  provider account.
- Have the authority to accept provider terms and data-use duties for the intended organization.
  Market Squawk cannot manufacture that authority.

Examples use:

```bash
MSQ="/exact/CLI/path/printed/by/the/installer/market-squawk"
CONFIG=/absolute/operator-owned/market-squawk/config.toml
```

## Safety and authority

### Keep the evidence layers separate

| Layer | What it proves | What it does not prove |
| --- | --- | --- |
| Credential-bundle disposition | The exact selected provider was disabled, needs a probe, stored an unverified credential, or could not match its code-owned profile | Provider verification, entitlement, activation, collection, publication, product availability, or trading authority |
| Code-owned profile | Reviewed setup requirements, release state, declared coverage, quality ceiling, rights decision, duties, probe, and evidence revision | Current connectivity, active adapter, observed data quality, or execution eligibility |
| Catalog registration | Exact profile revision and canonical bytes are durably retained | Onboarding completed or any runtime started |
| Onboarding session | Provider handoff, public configuration, credential generation when applicable, verification, and rights state progressed through a durable lifecycle | Research adapter registration unless activation evidence also succeeded |
| Research-adapter activation | An immutable lease and exact provider request admitted one extraction adapter and durable restart recipe | A live venue stream, an extracted dataset, or automated-action authority |
| Current runtime status | Same-process live-source evidence for one active route | Cross-process health or future qualification |
| Data quality and action authority | Evidence-derived, short-lived qualification assessed by live and risk authorities | A value implied by profile registration, connectivity, or a successful probe |

Registration is safe to replay, but every mutating command still requires `--confirm`. Never edit
the catalog, activation recipes, evidence objects, or onboarding rows to force a state transition.

### Protect the local portal and credentials

`source setup` may accept only a URL with:

- scheme `http`;
- host exactly `127.0.0.1`;
- an ephemeral port;
- no username, password, query, or fragment.

The portal is bounded to 15 minutes, uses a local session cookie and CSRF token, and limits
requests, connections, time, and body sizes. Keep it on loopback. Do not proxy, publish, bookmark,
or forward its URL.

Provider credentials enter the product through either an admitted write-only local portal or the
exact one-time credential bundle described below. Never put credential material in startup TOML,
the process environment, a CLI argument, an activation request, an issue, a log, or chat. The
bundle path is a CLI argument; its values are not. Do not source the bundle as a shell file. The
current `LocalProduct` uses the OS keyring first and a code-owned encrypted-file fallback that
accepts its unlock only through the explicit foreground loopback portal; see
[Configuration and secrets operations](configuration-and-secrets.md).

### Treat rights as authority, not advice

An activation lease binds the exact session, surface, capability revision and digest, rights
decision and duties, persistence evidence, public-configuration digest, optional credential
generation/reference, issuance time, and optional verification expiry. Callers cannot construct a
lease from a provider name or a profile's quality ceiling.

A successful handoff or network probe never widens `retrieve`, `display`, `persist`,
`model_training`, `export`, or `redistribute` rights. FRED/ALFRED revision 5 admits bounded
ephemeral retrieval. Durable operations require both exact written St. Louis Fed service permission
with a hash-bound local review and independent authority for every exact series and operation.

## Import the selected provider credential bundle

The installed CLI owns one strict import path for the exact
[`market-squawk-provider-credentials/v1`](../reference/market-squawk-provider-credentials.env.example)
contract. The file has 32 ordered fields: one schema identifier, provider enablement/probe intent,
the exact Alpaca `paper` realm, selected credentials, and SEC identifying values. Endpoint URLs,
the Schwab callback (`https://127.0.0.1:8182`), authentication routes, provider limits, application
budgets, batching, pagination, and scheduling are code-owned. They are not accepted from this file.

Use an owner-only regular file outside the repository. On POSIX systems, require mode `0600` and
an owner-only containing directory before import:

```bash
CREDENTIALS=/absolute/operator-owned/market-squawk-provider-credentials.env
chmod 600 "$CREDENTIALS"
"$MSQ" --config "$CONFIG" --output json \
  source import-credentials "$CREDENTIALS" --confirm
```

The command requires the running installed application service. It reads at most 64 KiB through a
no-follow, identity-checked file capability, stages the exact bytes once to that service, strictly
parses the closed V1 key set, and delegates only to the existing onboarding and protected-secret
authorities. Unknown, duplicate, out-of-order, malformed, placeholder, or inconsistent
enabled/credential fields fail closed. The importer does not call a provider, verify entitlement,
activate a runtime, start a scheduler, publish data, or call any brokerage trading endpoint.

The result always contains the 17 selected mappings in this fixed order:

| Receipt provider | Selected profile and revision | Imported input |
| --- | --- | --- |
| `schwab` | `schwab.trader-api-market-data` rev 3 | App key plus app secret |
| `alpaca` | `alpaca.basic-market-data` rev 3 | Paper key ID plus secret key; realm fixed to `paper` |
| `yahoo_finance_experimental` | `yahoo-finance.experimental-enrichment` rev 3 | No secret; enabled intent only |
| `nasdaq_trader_reference` | `nasdaq-trader-symbol-directory-reference` rev 3 | No secret |
| `occ_options_reference` | `occ.options-reference` rev 3 | No secret |
| `cboe_options_reference` | `cboe.options-reference` rev 3 | No secret |
| `iex_hist` | `iex.hist-feed-files` rev 3 | No secret; exact feed/date job intent only |
| `bls` | `bls.v2-registered` rev 3 | Registration key |
| `bea` | `bea.api-data` rev 3 | BEA UserID |
| `census` | `census.data-api` rev 3 | Census API key |
| `eia` | `eia.api-v2` rev 3 | EIA API key |
| `fred_alfred` | `fred-alfred.api-v1-v2` rev 5 | FRED API key |
| `tiingo` | `tiingo.starter-eod-nav` rev 3 | Tiingo token |
| `sec` | `sec.edgar-public` rev 4 | No secret; organization and contact email become public onboarding configuration |
| `treasury_fiscal_data` | `treasury.fiscal-data` rev 5 | No secret |
| `treasury_daily_rates` | `treasury.daily-rates-xml` rev 5 | No secret |
| `federal_reserve_board_direct` | `federal-reserve-board.data-download-program` rev 4 | No secret; exact H.15 doctor intent only |

Each provider row has exactly one secret-free `disposition`:

| Disposition | Meaning |
| --- | --- |
| `disabled` | Neither enablement condition requested this provider; no session was created |
| `credential_stored_unverified` | The enabled credential was stored through the protected secret authority; verification and activation remain required |
| `probe_required` | Enabled no-secret intent created a durable onboarding session; its bounded probe and activation remain required |
| `profile_unavailable` | The selected profile is absent, its revision/release contract no longer matches, or the exact V1 file cannot satisfy it |

`onboardingSessionId` is a UUID only when a durable session was created; otherwise it is `null`. A
stored credential may be described as **Configured**; `probe_required` remains **Probe required**.
Neither is **Available**. Availability requires current entitlement, exact production, durable raw
and canonical publication, a point-in-time typed read, product composition, and focused restart
proof.

## Current code-owned source surfaces

The selected mappings above are current code facts, not claims that their data reaches a product.
The existing application already has substantive Alpaca, Nasdaq reference, SEC, BLS,
FRED/ALFRED, and Treasury foundations. Provider-native core and transport code is also present for
Schwab, Yahoo, IEX HIST, OCC/Cboe reference, BEA, Census, EIA, Tiingo, and the Federal Reserve
Board. Board revision 4 retains an exact bounded no-key H.15 onboarding GET whose response must
pass the adapter's exact 11-series/ten-date parser under the shared one-request-per-minute,
single-flight application budget. Production is a distinct rolling 100-date/1,100-observation
contract; the full-history package is unavailable to the one-batch source until partitioned
resumable extraction exists.

Most of these selected surfaces remain `refresh_required`; Board revision 4 is `available` at the
profile/onboarding-doctor boundary and now has code-owned activation/source construction, shared
rich-capture binding, analytical-dataset registration, and lifecycle serialization/restore.
The scripted installed Board journey proves rolling capture/sealing, durable catalog/Parquet
publication, typed history and macro-dashboard reads, stable same-root restart evidence, and zero
post-restart provider HTTP. An exact-head real-network installed smoke independently proves the
official no-key doctor, durable one-minute rate refusal, rolling retrieval, raw seal, 1,100-row
publication, authenticated dashboard read, and same-root local reread. Native Desktop/package and
frozen-head release acceptance remain separate gates. Read `source status`, `source
coverage`, and the [delivery ledger](../plans/delivery-ledger.md) rather than inferring product
availability from a profile release state or code-path presence.

## Read source status, coverage, and health

Start with all three read-only views:

```bash
"$MSQ" --config "$CONFIG" --output json source status
"$MSQ" --config "$CONFIG" --output json source coverage
"$MSQ" --config "$CONFIG" --output json source health
```

Each command composes the local product, reads the complete code-owned profile registry, current
durable onboarding sessions, and the current in-process live-runtime view, then shuts down. The
Source-domain request is authority-free, but `LocalProduct` composition still prepares controlled
paths and can restore or quarantine a durable activation recipe. Treat the first invocation by a
new application version as a stateful recovery boundary. An optional filter may be a profile
surface ID; an active live source ID can also select its matching runtime row:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source status treasury.fiscal-data
"$MSQ" --config "$CONFIG" --output json \
  source coverage treasury.fiscal-data
"$MSQ" --config "$CONFIG" --output json \
  source health treasury.fiscal-data
```

### Interpret the rows

- `source status` returns the full profile, `currentSession`, and `runtime`.
- `source coverage` returns the profile's `releaseState`, `declaredCoverage`, `qualityCeiling`, and
  `rights` separately from `runtimeCoverage`.
- `source health` returns the durable `onboardingState` separately from `runtimeHealth`.

For an inactive profile:

```json
{
  "runtime": {"state": "not_active"},
  "runtimeCoverage": {"state": "not_established"},
  "runtimeHealth": {"state": "not_active"}
}
```

Those fields are not errors and do not change execution eligibility. The result metadata explicitly
reports `executionEligibilityUnchanged: true`.

A standalone CLI command creates a new `LocalProduct`, so it is not a cross-process monitor for a
different capture, bot, or MCP process. Same-process active live-runtime records are visible to the
long-lived application service, including through MCP. For an activated research provider,
`currentSession.state` and the activation result are the operator-accessible extraction-source
evidence; its live-market `runtime` may correctly remain `not_active`.

## Register a source profile

Register the exact Treasury Fiscal Data profile:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source register treasury.fiscal-data --confirm
```

On first insertion, the result contains:

```json
{
  "outcome": "inserted"
}
```

Run the identical command again to verify idempotence. The result changes to:

```json
{
  "outcome": "replay"
}
```

Both results include the complete secret-free code-owned profile. `replay` means the exact
capability revision and canonical bytes were already retained; it is not an error.

Registration does not:

- contact the provider;
- create a credential;
- activate an adapter;
- run an extraction;
- establish runtime coverage or health;
- improve data quality or execution eligibility.

## Run the supported Treasury Fiscal Data setup

### 1. Start the bounded portal

```bash
"$MSQ" --config "$CONFIG" --output json \
  source setup treasury.fiscal-data --confirm
```

The command first registers or replays the profile, emits its official handoff plus a result such
as:

```json
{
  "portal": {
    "url": "http://127.0.0.1:<ephemeral-port>",
    "expiresInSeconds": 900,
    "secretInput": "local_portal_only"
  }
}
```

It then attempts to open the system browser and keeps the terminal process alive until Ctrl-C or
expiry. If browser launch fails, the loopback portal remains active and stderr records the exact
URL; copy only that emitted URL into a local browser.

### 2. Verify the portal boundary

Before entering anything, confirm that the browser address:

- begins with `http://127.0.0.1:`;
- contains the exact port emitted by this process;
- has no credentials, query, or fragment;
- displays “Market Squawk provider setup.”

Close any page that does not match.

### 3. Activate Treasury Fiscal Data

Before using the Treasury Fiscal Data section, review the profile's full rights, duties, coverage,
and evidence in the `source status`/`source coverage` output retained above. Then, in the portal:

1. Confirm the handoff instruction, `available` release state, and official link match the retained
   code-owned profile.
2. Enter an inclusive first record date.
3. Enter an inclusive last record date no earlier than the first.
4. Enter a page size from `1` through `10000`; the form defaults to `1000`.
5. Select **Set up provider** once.

The portal starts a durable no-credential onboarding session, performs the bounded official probe,
obtains an immutable activation lease, constructs the exact average-interest-rates query, persists
the desired activation recipe, and registers the Treasury extraction adapter. It does not perform
an extraction merely by activating the adapter.

Keep the terminal and browser open until the page displays a successful secret-free activation
result. Preserve that result before closing the page.

### 4. Shut down the portal cleanly

Press Ctrl-C in the terminal after the activation result is captured. The portal stops accepting
requests, the application performs bounded shutdown, and the durable onboarding and adapter recipe
remain under the data root.

### 5. Verify durable status and restart recovery

Run:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source status treasury.fiscal-data
"$MSQ" --config "$CONFIG" --output json \
  source coverage treasury.fiscal-data
"$MSQ" --config "$CONFIG" --output json \
  source health treasury.fiscal-data
```

The current session should be `active` and identify the expected surface. A fresh `LocalProduct`
reconstructs the exact desired research adapter from the durable recipe without broadening rights.
The live-runtime fields may remain `not_active` or `not_established`; Treasury is a research
extraction source, not a live market route.

## Run the supported Treasury daily-rates setup

Start the same bounded portal for the daily-rate surface:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source setup treasury.daily-rates-xml --confirm
```

Verify the loopback address as described above. In the Treasury daily-rates form, select an
inclusive first year from 1990 through the current year and an inclusive last year from 2003
through the current year. The last year must not precede the first. Selecting **Set up provider**
creates a no-credential lease and one durable research configuration containing every official
family available within the range:

- nominal par yield curves from 1990;
- bill rates from 2002;
- long-term and real long-term rates from 2000; and
- real par yield curves from 2003.

The form activates the adapter; it does not download every configured year immediately. Use
`source discover` and `ingest source` with one exact dataset such as
`treasury:daily-bill-rates:2026`. A successful ingestion returns a manifest, row count, object
count, byte count, and lineage digest. Verify the published dataset through
`query dataset <DATASET_ID>` or `Macro.GetObservations`, then stop the portal with Ctrl-C and
confirm `source status`, `source coverage`, and `source health` still report the active recovered
session.

## Understand the source activate boundary

The public command is:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source activate /absolute/authorized/request.json --confirm
```

It is an advanced evidence-bound activation interface for controlled provisioning, not the normal
human Treasury workflow. Use it only when that workflow already owns:

- a current active onboarding session and exact session UUID;
- an exact provider request appropriate to that lease;
- every referenced evidence object under the request file's parent root;
- recorded SHA-256 for each referenced object;
- the authority to use the requested provider scope.

The request must be a nonempty regular file, at most 1 MiB, read without following a final symlink.
Its parent directory becomes the authorized input root. The top-level closed JSON schema is:

```json
{
  "schema_version": 5,
  "session_id": "<uuid>",
  "provider": {
    "kind": "<closed-provider-kind>"
  }
}
```

The closed provider kinds are:

| Kind | Additional authority-bearing input |
| --- | --- |
| `sec` | No request-file evidence; must match an active `sec.edgar-public` lease |
| `bls` | Exact series-metadata file references and hashes, inclusive start/end years, and either BLS surface lease |
| `treasury_fiscal` | Inclusive first/last dates and page size for the exact Fiscal Data query |
| `treasury_daily_rates` | Inclusive first/last years; every official family available in that range is activated |
| `fred_alfred` | Exact current terms artifact; exact official-HTTPS Bank response bytes verified by fresh reacquisition; explicit hash-bound review with reviewer, issuer, grantee, service, exact series, operations, conditions, and revalidation; independent per-series public-domain or owner evidence |
| `federal_reserve_board_h15` | No additional input; must match an active revision-4 Board lease and construct only the exact rolling 100-date H.15 dashboard contract; the full-history contract is rejected until partitioned resumable extraction exists |

Credential bytes are never part of this request. FRED's request shape does not create rights:
without an admitted active lease and both exact authority gates, the command fails closed. A
contact-form submission or receipt is request-route provenance, not permission. Board is admitted
only in schema version 5; older request versions cannot acquire this new surface implicitly.

Request bytes and referenced evidence digests are part of durable recipe identity. Do not
reconstruct or reformat a portal request from status output, and do not use `source activate` after
a portal activation to try to “make it more active.” An exact idempotent replay is accepted;
competing configuration for an already active surface is rejected.

## Inspect one FRED or ALFRED page without persistence

Complete the guided `fred-alfred.api-v1-v2` setup first and retain the active onboarding session
UUID returned by the product. Then request one bounded page:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source inspect fred-alfred.api-v1-v2 \
  --onboarding-session-id "$SESSION_UUID" \
  --dataset-identifier fred:series-observations:UNRATE:2024-01-01:2024-12-31 \
  --page-index 0 \
  --max-records 256
```

The command calls `Source.Inspect`. It reacquires and validates the code-owned current terms,
binds the exact active onboarding session and its foreground credential access, validates the
complete provider pagination contract, refetches the selected page under the same bounded request
authority, and returns only canonical observations plus exact object and payload evidence. The
selected object identifier has the closed form
`fred-page-v2:<OFFSET>:<LIMIT>:<RETURNED>:<TOTAL>:<TERMINAL>:<PAGE_SHA256>:<METADATA_SHA256>`.

This path does not create a research manifest, write provider bytes to Parquet, or mint ingestion
authority. `page-index` is limited to `0..63`; `max-records` is limited to `1..1024` and remains
subject to the application/MCP byte ceiling. A stale session, mismatched dataset, incomplete page
sequence, invalid metadata, deadline, cancellation, or provider refusal fails the request without
partial publication.

## Discover and ingest an exact provider object

After the provider adapter is active and its rights permit the intended operation, list the bounded
objects in one exact provider dataset namespace:

```bash
"$MSQ" --config "$CONFIG" --output json \
  source discover <PROVIDER> --dataset <DATASET>
```

Copy an exact returned object identifier into the confirmed ingestion command:

```bash
"$MSQ" --config "$CONFIG" --output json \
  ingest source <PROVIDER> <OBJECT> --dataset <DATASET> --confirm
```

The listing calls `Source.ListObjects`. It validates registered-adapter metadata, coverage, quality,
rights, and result bounds, but mints no ingestion receipt. The ingestion command independently calls
the confirmed `Source.Discover`, selects the exact provider/dataset/object identity, consumes its
process-local single-use receipt through `Research.IngestSource`, and publishes only under the
retained rights and dataset authority. A listing is therefore useful for object selection but can
never be replayed as ingestion authority.

An MCP client that performs the same workflow calls confirmed `Source.Discover` and passes the
returned `discoveryReceipt` with the exact provider, dataset, and object to confirmed
`Research.IngestSource` in the same application process. Receipts are not cross-process
credentials, cannot authorize a different object, and are revoked when discovery publication
fails.

## Expected success evidence

### Credential import

- The command exits `0` and returns schema `market-squawk-provider-credentials/v1`.
- Exactly 17 provider rows appear in the fixed mapping order.
- Every row has one of the four documented secret-free dispositions.
- No key, secret, token, provider response, endpoint override, account value, or trading authority
  appears in the result.
- Import completion is recorded separately from probe, activation, publication, and workflow
  availability.

### Registration

- Command exits `0`.
- `profile.id` is the exact requested surface.
- `outcome` is `inserted` or idempotent `replay`.
- Returned profile contains the expected code-owned release state, rights, duties, evidence, and
  official handoff.

### Portal setup and activation

- The setup result's portal URL is IPv4 loopback only and has `expiresInSeconds: 900`.
- Treasury's profile is `available`.
- The browser activation result contains:
  - `profile`;
  - `session_id` or `sessionId`, depending on portal versus CLI serialization;
  - `capability_revision`/`capabilityRevision`;
  - capability evidence;
  - rights-decision evidence;
  - persistence-rights evidence;
  - public-configuration evidence;
  - no credential generation for Treasury;
  - issuance time and any verification expiry.
- A new process reports the current Treasury session as active.
- No result contains a credential value.

The activation evidence proves exact adapter admission. It does not prove that a subsequent
extraction completed or that a dataset was published.

### Source status views

- Read commands exit `0` and return bounded rows plus completeness metadata.
- Profile and onboarding state agree with the exact surface.
- Runtime absence is explicit rather than guessed.
- `executionEligibilityUnchanged` remains `true` for status inspection.

### Provider-object discovery and ingestion

- `source discover` returns a bounded exact-object listing with complete source metadata and no
  authority receipt.
- `source inspect` returns one bounded FRED/ALFRED page with exact evidence and no durable
  publication or ingestion receipt.
- For an active, rights-admitted provider, `ingest source` rediscovers and binds the exact selected
  object before immutable publication.
- Provider, dataset, object, metadata, coverage, quality, and receipt identity remain consistent;
  a stale or mismatched selection fails closed.

## Rollback and recovery

- **Credential import was unintended:** import never activates a provider. Preserve the secret-free
  receipt, inspect the created onboarding session, and use the product-owned authority-removal flow
  before deleting the owner-managed input copy. Do not delete keyring entries, fallback-vault
  records, or catalog rows by hand.
- **Registration was unintended:** there is no unregister command. Registration alone activates
  nothing, so leave the exact profile record intact and do not delete catalog state manually.
- **Portal opened but no mutation was intended:** press Ctrl-C or wait for expiry. If no activation
  was submitted, no adapter recipe is created.
- **A session exists but is not active:** inspect `currentSession.state` and `next_action` through
  `source status`. Retry only after the named provider-health, evidence, rights, or deadline
  condition has been corrected. Do not skip states.
- **Exact activation was retried:** an identical active candidate is idempotent. A different
  candidate for the same active provider fails as competing configuration; review the retained
  recipe and intended scope rather than deleting it.
- **Activation fails after recipe publication:** the matching provider recipe is quarantined as
  adapter-rejected. Other providers remain available. Preserve stderr and status evidence, correct
  the provider-specific cause, and create a newly authorized activation.
- **Restart finds invalid, superseded, or authority-invalid state:** recovery quarantines only the
  affected surface. Do not edit the quarantine record or evidence store.
- **Credentialed provider requires restart:** BLS registered and FRED restoration cannot read a
  key in the background. They remain disabled until an explicit foreground resume is both
  release-authorized and requested.
- **Provider terms or rights changed:** stop new use, retain exact lineage and evidence, and wait
  for a reviewed code-owned profile revision. The operator cannot widen rights locally.

There is no generic deactivate or session-cancel CLI. The loopback portal exposes **Remove local
provider authority** for the current session and routes it through product-owned adapter
deregistration, credential cleanup, and durable cancellation. Quiesce provider use first, preserve
the data root, and never remove files or catalog rows by hand.

## Known failure modes

| Symptom | Likely cause | Safe response |
| --- | --- | --- |
| Mutation rejects with confirmation required | `--confirm` omitted | Recheck the exact provider/request, then rerun explicitly |
| Credential import rejects the file | Path/input confinement failed, the file exceeded 64 KiB, or the closed ordered V1 schema/enablement rules failed | Use an owner-only regular file at an absolute path, compare all 32 fields with the repository example, and correct the input without sourcing it |
| Import row is `profile_unavailable` | The selected profile is absent, its revision/release state differs from the exact mapping, or V1 cannot satisfy it | Preserve the receipt and wait for a reviewed mapping/profile correction; do not rename the provider or edit catalog state |
| Unknown provider | Argument is not an exact registered surface ID | Use `source status` without a filter and copy the code-owned ID |
| Portal setup button is disabled | Profile is `refresh_required` or `rights_blocked`, or no exact supported request exists | Read the displayed release state and ledger; setup is unavailable for that surface at this head |
| Browser does not open | Local browser integration failed | Use only the exact loopback URL emitted on stdout/stderr while the command remains running |
| Portal returns expired/unauthorized/CSRF error | Portal lifetime or local session boundary ended | Close the page and run a new confirmed `source setup` |
| Official probe fails | DNS, TLS, endpoint, provider health, rate budget, or policy mismatch | Preserve the bounded error; retry only after the named condition clears |
| SEC or public BLS v1 cannot activate | SEC declared contact or CIK mapping, BLS exact series metadata/year scope, the bounded official probe, or provider health is invalid | Correct the exact rejected input or provider condition; do not weaken the profile or substitute fixture evidence |
| Registered BLS v2 cannot activate | The distinct keyed profile remains `refresh_required`, or its foreground credential is unavailable | Use public v1 within its limits or wait for an admitted registered-v2 revision; never treat v1 authority as v2 authority |
| FRED key/import or use is rejected | API-key format, current terms, exact written Bank permission, hash-bound local review, exact-series authority, requested operations, or validity intersection is missing or mismatched | Correct the exact rejected authority or key generation; a successful key probe or contact receipt cannot replace either rights gate |
| Treasury XML cannot publish durably | Family/query authority, CC0 evidence, official response, or publication integrity is incomplete | Preserve the exact error and rerun only after correcting the named family or authority input; never inherit rights across surfaces |
| Board full-history source construction is rejected | The full-history response cannot satisfy the indivisible one-batch capture/publication contract | Use the code-owned rolling 100-date dashboard source; do not raise bounds or relabel it as full history. Implement reviewed partitioned, checkpointed, resumable ingestion before admitting the full-history research contract |
| Discovery or ingestion rejects an object | Provider activation, rights, exact dataset/object identity, metadata, or the fresh process-local receipt no longer matches | Read current source status, rerun the bounded listing, and retry the exact confirmed ingestion; never fabricate or reuse a receipt |
| Status shows `currentSession: active` and `runtime: not_active` | Research extraction adapter is active but no live market runtime exists in this process | Treat session/activation evidence as extraction status; do not fabricate live health |
| Activation request rejects as invalid | Wrong schema version, unknown field/kind, oversized or symlinked input, surface/session mismatch, missing evidence, or bad hash | Use the exact controlled request and evidence root; do not weaken validation |
| Restart quarantines one provider | Durable recipe/evidence is invalid, superseded, unauthorized, or rejected by its adapter | Preserve quarantine evidence and re-onboard that surface; other providers stay isolated |

## Local logs, data, and artifacts

| Location or stream | Contents |
| --- | --- |
| Owner-managed credential bundle | One-time 32-field input outside the repository; POSIX mode `0600`; not startup configuration and not deleted automatically by import |
| `<data-root>/catalog.sqlite3` | Code-owned profile registrations and durable onboarding sessions; SQLite sidecars may exist while active |
| `<data-root>/control/sources/research-runtime/` | Durable research source-registry and rights authority state |
| `<data-root>/control/sources/provider-activation-v1/recipes/` | Desired or quarantined, secret-free provider activation recipes |
| `<data-root>/control/sources/provider-activation-v1/evidence/` | Digest-addressed exact evidence objects referenced by activation recipes |
| `<data-root>/control/sources/provider-adapters/sec/` | SEC raw-evidence and representation state, created only for admitted SEC activation |
| `<data-root>/control/secrets/provider-credentials/` | Authenticated encrypted fallback vault, used only after explicit foreground unlock when the OS credential service is unavailable |
| `<data-root>/journal/` | Capture/diagnostic journals; not onboarding authority |
| `<data-root>/artifacts/` | Controlled extraction, dataset, model, and other application artifacts |
| OS credential service | Opaque provider credential generations for admitted workflows; never under the data root |
| stdout | Secret-free CLI results and portal location |
| stderr | Local tracing, browser-launch warning, portal lifecycle, recovery, and provider errors; `--json-logs` is supported |

The portal is ephemeral and does not create a separate web-server log file. Market Squawk does not
configure a remote log exporter by default.

## Related documentation and code

- [Installation and local bootstrap](installation-and-bootstrap.md)
- [Configuration and secrets operations](configuration-and-secrets.md)
- [Provider accounts and credential preparation](provider-account-setup.md)
- [Selected provider credential example](../reference/market-squawk-provider-credentials.env.example)
- [Selected provider contracts](../reference/providers/README.md)
- [CLI reference](../reference/cli.md)
- [Configuration reference](../reference/configuration.md)
- [Source coverage and adapter reference](../reference/source-coverage.md)
- [Data quality and live qualification](../reference/data-quality.md)
- [Control-plane architecture](../architecture/control-plane.md)
- [Research data-plane architecture](../architecture/research-data-plane.md)
- [Live execution-plane architecture](../architecture/live-execution-plane.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Delivery ledger](../plans/delivery-ledger.md)
- [Source CLI definitions](../../apps/market-squawk/src/cli.rs)
- [Installed credential import service](../../apps/market-squawk/src/service/provider_credential_import.rs)
- [Strict credential-bundle parser](../../apps/market-squawk/src/provider_onboarding/credential_bundle.rs)
- [Selected credential delegation](../../apps/market-squawk/src/provider_onboarding/credential_bundle_delegation.rs)
- [Source application service and read semantics](../../apps/market-squawk/src/application/source.rs)
- [Source result shapes](../../apps/market-squawk/src/application/source/results.rs)
- [Built-in profile registry](../../crates/market-squawk-sources/src/onboarding/built_in_profiles.rs)
- [Onboarding contracts and activation lease](../../apps/market-squawk/src/provider_onboarding/contracts.rs)
- [Bounded loopback portal](../../apps/market-squawk/src/provider_onboarding/portal.rs)
- [CLI research-provider activation](../../apps/market-squawk/src/local_product/cli_provider.rs)
- [Durable activation recovery state](../../apps/market-squawk/src/local_product/provider_activation_state.rs)

## Official sources

These selected provider sources were reviewed directly through 2026-08-11. They describe upstream
interfaces, limits, and terms; the reviewed Market Squawk profile revision remains the authority
for what the product currently admits.

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Public submissions/XBRL API boundary, no API key for these data APIs, and automated-access policy reference | 2026-07-26 |
| [SEC developer resources](https://www.sec.gov/about/developer-resources) | Aggregate fair-access ceiling of no more than ten requests per second and declared-bot requirement | 2026-07-26 |
| [SEC EDGAR public API authority](../research/providers/sec-edgar-public-api-2026-07-26.md) | Exact zero-fee setup authority and mandatory real-response acceptance boundary | 2026-07-26 |
| [BLS Public Data API FAQ](https://www.bls.gov/developers/api_faqs.htm) | Registered versus unregistered API behavior, limits, and registration lifecycle | 2026-07-26 |
| [BLS API v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm) | Exact registered-v2 request family, series identifiers, years, and registration-key field | 2026-07-23 |
| [BLS terms of service](https://www.bls.gov/developers/termsOfService.htm) | Official API-use duties retained by the BLS profile | 2026-07-26 |
| [BLS Public Data API authority](../research/providers/bls-public-data-api-2026-07-21.md) | Available zero-fee public-v1 decision and distinct registered-v2 boundary | 2026-07-26 |
| [Treasury Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/) | Official Fiscal Service API query and pagination contract | 2026-07-23 |
| [Treasury daily interest-rate XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | Five XML families, year/month selectors, and zero-based 300-row all-history pagination | 2026-07-26 |
| [Treasury daily-rates release authority](../research/providers/2026-07-26-treasury-daily-rates-release-authority.md) | Dataset-level public-access and CC0 evidence for durable local use | 2026-07-26 |
| [Federal Reserve Board Data Download Program](https://www.federalreserve.gov/datadownload/) | No-key DDP entry point and current-definition release boundary | 2026-08-12 |
| [Federal Reserve Board DDP help](https://www.federalreserve.gov/datadownload/help/) | Automated download formats, one-frequency-per-file behavior, labels, units, and multipliers | 2026-08-12 |
| [FRED API terms of use](https://fred.stlouisfed.org/docs/api/terms_of_use.html) | Required API key, mutable terms, usage restrictions, and obligations that prevent implied durable rights | 2026-07-23 |
| [FRED API key documentation](https://fred.stlouisfed.org/docs/api/api_key.html) | Provider-controlled account/key boundary; application users require their own keys | 2026-07-26 |
| [Current FRED legal terms](https://fred.stlouisfed.org/legal/) | Current service and API-specific storage, caching, archival, database, software-development, and model-training prohibitions | 2026-07-26 |
| [FRED permissions contact route](https://fred.stlouisfed.org/contactus/) | Official route for requesting permission; a submission or acknowledgement is not permission | 2026-07-26 |
| [FRED/ALFRED service-use authority](../research/providers/2026-07-26-fred-alfred-self-hosted-api-authority.md) | Current two-gate decision and mandatory real release proof | 2026-07-26 |
