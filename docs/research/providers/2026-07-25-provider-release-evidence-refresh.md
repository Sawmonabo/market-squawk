# Provider release-evidence refresh — 2026-07-25

> Historical audit snapshot. FRED/ALFRED conclusions were superseded on 2026-07-26 by the
> [self-hosted exact-series authority](2026-07-26-fred-alfred-self-hosted-api-authority.md).
> The original audit base, evidence, and conclusions below are retained as recorded.

This report freezes the current official evidence for Market Squawk's zero-mandatory-fee provider
surfaces and reconciles it against the repository at one exact audit base. It is a release decision
record, not an authorized production probe or a substitute for provider-controlled credentials.

| Field | Value |
| --- | --- |
| Document type | Provider release-evidence research |
| Audience | Source, live, research, risk, onboarding, and release owners |
| Status | Current evidence decision; external predicates remain fail-closed |
| Retrieval/as-of date | 2026-07-25 |
| Audit commit | `26be4c920bcc118b7fcc8d6d1bbd0a2c77f751fe` |
| Audit tree | `37421e426d383c274c22bbe59141d9461c812147` |
| Research report digest | SHA-256 `74b43e5e35f247f562540e657acc1a9fd4d8e9f0de0c8a1b63eb4aa36a9a73cf` |
| Evidence-audit verdict | `PASS_WITH_NOTES` |

## Contents

- [Decision summary](#decision-summary)
- [Decision rules](#decision-rules)
- [Provider decisions](#provider-decisions)
- [Rights-operation matrix](#rights-operation-matrix)
- [Official, encoded, and runtime reconciliation](#official-encoded-and-runtime-reconciliation)
- [Capability records requiring reconciliation](#capability-records-requiring-reconciliation)
- [Exact unresolved external predicates](#exact-unresolved-external-predicates)
- [Source matrix](#source-matrix)
- [Method and validation](#method-and-validation)

## Decision summary

Market Squawk retains a credible zero-mandatory-subscription path:

- Coinbase Exchange offers a default ten-WebSocket-subscription tier at `$0`. Its traditional
  feed is available without authentication; its Direct Market Data endpoint and full channel are
  authenticated capabilities. Those two surfaces must not share one ambiguous onboarding profile.
- Kraken expressly describes public real-time L2, trades, ticker, and OHLCV as free and
  unauthenticated. Kraken v2 book data remains `DirectUnverified` because the provider documents a
  top-ten CRC32 state check but no numeric book-update sequence or exact-successor/gap-recovery
  contract.
- SEC EDGAR, BLS v1, and Treasury Fiscal Data require no paid subscription. SEC and BLS remain
  release-evidence/probe gated at the audited commit; research browsing does not clear those gates.
- BLS v2 and FRED require provider-controlled accounts/keys. Their documented registration paths
  do not establish a mandatory subscription fee. BLS can support a scoped local research path;
  current FRED terms cannot support Market Squawk's required durable ingestion and model training.
- Treasury Fiscal Data is a broad open-data surface. Treasury daily-rate XML is a different
  surface and does not inherit Fiscal Data's rights evidence.

The operative release positions are:

| Surface | Zero-mandatory-fee position | Quality or time position | Rights/release position |
| --- | --- | --- | --- |
| Coinbase public Exchange | Default ten-subscription tier is `$0`; public feed accepts unauthenticated subscriptions | Shipping public adapter ceiling `DirectUnverified` | `RightsLimited` |
| Coinbase Exchange Direct/full | Account/API credentials required; bounded `$0` tier evidence exists, but exact direct entitlement still needs an authorized account trace | Potential `DirectVerified` metadata ceiling only after exact qualification | No distinct active onboarding authority at audit base; rights remain limited |
| Kraken Spot public | Public L2/trades/ticker/OHLCV expressly described as free and unauthenticated | `DirectUnverified`; top-ten CRC32 does not replace sequence progression | `RightsLimited` |
| SEC EDGAR | Free, no key; declared organization/admin contact required | `OfficialDelayed`, revision-preserving | Rights-eligible; `RefreshRequired` until exact evidence/probe |
| FRED/ALFRED | Free account and API key | `OfficialDelayed`, vintage-capable | `RightsBlocked` for the required durable/modeling vertical |
| BLS v1 | Unregistered public API | `OfficialDelayed`, historical but not provider-vintaged | Rights-eligible with duties; `RefreshRequired` |
| BLS v2 | Registration, emailed key, annual renewal; no payment step documented | `OfficialDelayed`, higher tier limits | Credential pending plus `RefreshRequired` |
| Treasury Fiscal Data | No account/token; data described as free without restriction | `OfficialDelayed`, versioned dataset/API | `Available`, dataset-scoped rights admitted |
| Treasury daily-rate XML | No credential appears in the documented request grammar; no affirmative fee promise | `OfficialDelayed`, historical feeds | `RightsLimited`; durable operations pending |

## Decision rules

Four independent questions are answered separately:

1. **Zero-mandatory-fee:** can a release-capable path operate without a paid subscription? A free
   provider account or user-created API key is compatible with this condition.
2. **Technical authority:** what exact coverage, rate, ordering, checksum, time, revision, and
   recovery behavior does the provider document?
3. **Rights authority:** which retrieve, display, persist, model-train, export, redistribute, and
   fair-value operations are admitted for the exact surface and content?
4. **Release evidence:** does code own current exact content evidence and an authorized runtime
   receipt for the selected profile revision?

Passing one does not pass the others. A heartbeat is connection health, not market-price
freshness. A checksum of state is not automatically proof of every intermediate update. A free
key is not a durable-use grant. A browser retrieval is not a production-runtime receipt.

`DirectVerified` remains evidence-derived current output, not a label inherited from a catalog
ceiling. ASC 820/IFRS 13 evidence remains independent of execution quality.

## Provider decisions

### Coinbase Exchange

The [Exchange WebSocket overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)
separates unauthenticated `ws-feed` from authenticated `ws-direct`. It documents exact
per-product successor sequence behavior for most market messages, gap/out-of-order handling, and
the need for state repair. The
[authentication page](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication)
requires authentication for full, user, Level 2, Level 3, and RFQ channels and says authenticated
feed messages do not increment sequence numbers. It does not exhaustively enumerate the affected
message classes.

The [channels specification](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)
documents:

- full-channel queue plus REST Level 3 snapshot/replay;
- Level 2 snapshot/update delivery;
- heartbeat sequence and last-trade identifiers;
- matches that may be dropped and require heartbeat/REST repair; and
- provider product/status/precision fields.

No exchange WebSocket checksum contract or numeric freshness/latency service level was found. The
[rate page](https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits) and
[connection page](https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections)
describe distinct IP, client-message, and user-level subscription-request budgets. The connection
page sets ten subscriptions per product/channel at `$0` and optional paid capacity above that
limit.

The [Market Data Terms](https://www.coinbase.com/legal/market_data) grant a limited
personal/internal-entity research license. Without written consent they restrict third-party
display/distribution of Market Data and Derived Works and restrict fair-value prices, valuations,
indexes, fixings, and benchmarks, including internal use. They do not expressly grant or prohibit
ordinary local persistence or model training. Those operations therefore remain pending rather
than inferred.

Decision:

- The public `level2`/`matches` path remains `DirectUnverified`.
- Authenticated Exchange Direct/full can be modeled with a `DirectVerified` metadata ceiling only
  as a distinct user-authorized surface. Current execution authority still requires exact
  authorization, product/venue/instrument coverage, snapshot consistency, sequence handling,
  freshness, status, precision, queue integrity, and unchanged-head runtime qualification.
- Coinbase data cannot support Market Squawk fair-value classification or valuation evidence
  under the reviewed default terms without the required written consent.

### Kraken Spot

Kraken's [v2 book schema](https://docs.kraken.com/api/docs/websocket-v2/book) provides depths 10,
25, 100, 500, and 1,000, RFC 3339 timestamps, snapshots/updates, and CRC32. The
[checksum guide](https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/) requires
message-atomic application, delete-on-zero, subscribed-depth truncation, exact decimal/string
preservation, and CRC construction from the top ten bids and asks after each update.

The provider does not publish a numeric book-update sequence field or exact-successor,
gap-signaling, replay, or recovery contract. At subscribed depth greater than ten, CRC32 does not
cover the entire retained depth. The [trade schema](https://docs.kraken.com/api/docs/websocket-v2/trade/)
calls `trade_id` a sequence unique per book but does not promise contiguous progression or a
complete recovery contract.

Kraken's [WebSocket FAQ](https://support.kraken.com/articles/360022326871-kraken-websocket-api-frequently-asked-questions)
states that public market data is unauthenticated, heartbeats occur when other messages are
absent, and message capacity varies with system load; it publishes no fixed public WebSocket
connection or message ceiling. The [rate guidance](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-)
describes public REST identity scopes and gives one request/second as a safe point, not a universal
exact maximum.

Kraken's [April 2026 product guidance](https://blog.kraken.com/product/api/unlocked-3-the-market-data-feeds-systematic-traders-use)
expressly calls named public real-time feeds free and unauthenticated and describes execution,
historical research, and backtesting use. Its
[downloadable history page](https://support.kraken.com/articles/360047543791-downloadable-historical-market-data-time-and-sales-)
offers complete and quarterly incremental time-and-sales CSV archives and contemplates code,
conversion, and spreadsheet import.

The [current global terms](https://www.kraken.com/legal/global-terms) provide limited, revocable,
own-benefit use and restrict third-party/commercial availability of provider content. Regional
terms can differ. No Spot-API-specific grant for general live-feed persistence, caching, model
training, derived-output publication, or redistribution was located.

Decision:

- Kraken public L2/trades can satisfy the no-fee/direct-venue path.
- The current evidence-derived ceiling is `DirectUnverified`; top-ten CRC32 and wire order do not
  satisfy Market Squawk's required valid-sequence-progression predicate.
- Local persistent/model use beyond the provider's expressly downloadable history and own-benefit
  examples remains rights-limited pending surface- and jurisdiction-specific authority.

### SEC EDGAR

The [EDGAR API page](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
documents unauthenticated, no-key submissions and XBRL company-facts APIs plus nightly bulk
archives. SEC [access guidance](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
provides free download, a ten-requests-per-second aggregate fair-access ceiling, and a declared
automated-client identity. The [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
states that government-created SEC content and public EDGAR filing content are broadly free to
access and reuse, subject to the limited excluded-content boundary.

Acceptance, dissemination, API availability, filing-document availability, and nightly bulk
publication are distinct times. Corrections, removals, and rebuilt bulk data require
revision-preserving point-in-time treatment.

Decision:

- The official evidence supports the encoded no-key, `OfficialDelayed`, revision-preserving,
  rights-eligible profile.
- The audited `RefreshRequired` state remains correct until the code-owned current-content digest
  and bounded production-runtime response are published under the declared organization/contact.

### FRED and ALFRED

FRED documents a [free account](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/)
with API access and requires a distinct
[API key](https://fred.stlouisfed.org/docs/api/api_key.html). Reproducible
[FRED/ALFRED point-in-time requests](https://fred.stlouisfed.org/docs/api/fred/fred_vs_alfred.html)
must set explicit real-time bounds. The
[v2 release-observations endpoint](https://fred.stlouisfed.org/docs/api/fred/v2/release_observations.html)
adds cursor completion, per-series source/copyright metadata, and a mixed-update reprocessing
requirement. The [v2 error page](https://fred.stlouisfed.org/docs/api/fred/v2/errors.html)
documents two requests/second before 429 for v2; that number is not evidence for every API
version.

The current [FRED Terms](https://fred.stlouisfed.org/legal/terms/) expressly prohibit:

- storing, caching, archiving, or incorporating FRED content into a database or compilation; and
- using FRED services/API in development or training of software, machine-learning, or AI systems.

Series owners retain separate copyrights and restrictions.

Decision:

- A free account/key does not cure the rights conflict.
- The integrated durable-ingestion/modeling surface remains `RightsBlocked`.
- Release requires written scope-specific permission or changed binding terms for the exact
  operations, followed by per-series rights admission. Transient API access alone is not the
  Market Squawk product vertical.

### BLS

The [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm) documents:

| Tier | Account/key | Daily queries | Series/query | Years/query | Ten-second limit |
| --- | --- | ---: | ---: | ---: | ---: |
| v1 | Unregistered | 25 | 25 | 10 | 50 |
| v2 | Email/organization registration, CAPTCHA, emailed key, annual renewal | 500 | 50 | 20 | 50 |

The FAQ also contains a later generic ten-year statement that conflicts with the explicit v2
20-year tier table. The explicit tier table is the stronger route, but the provider conflict must
remain observable.

The [BLS API Terms](https://www.bls.gov/developers/termsOfService.htm) say downloaded data should
not carry end-use controls. They require retrieval-date citation, the BLS disclaimer, accurate
representation, quota compliance, and respect for third-party intellectual property. BLS states
that its [published material is public domain](https://www.bls.gov/opub/copyright-information.htm)
except identified previously copyrighted photographs/illustrations; its emblem remains protected.

Decision:

- Verified BLS-authored data supports scoped local retrieval, display, persistence, analytics, and
  model-input/training use with the encoded duties.
- Export/redistribution should remain content-origin scoped rather than globally promoted; the
  current pending state is conservative until the profile can distinguish BLS-authored and
  third-party material.
- v1 and v2 remain `RefreshRequired` until exact code-owned evidence and runtime probes are
  published. v2 additionally requires the user's current emailed key and annual renewal.

### U.S. Treasury

The [Fiscal Data API page](https://fiscaldata.treasury.gov/api-documentation/) states that no
account or token is required and that the data is free without restriction for copying,
adaptation, redistribution, and commercial or noncommercial use. It documents versioned REST
`GET` APIs, schema, pagination, sorting, aggregation, and JSON/CSV/XML representations. Its
generic response table includes an invalid-API-key 403 description despite the no-token access
statement, so an exact unauthenticated endpoint receipt remains operational evidence.

The [Fiscal Service open-data policy](https://fiscaldata.treasury.gov/data/about-us/901-1%20Open%20Data%20Policy.pdf)
supports free use/download and machine-readable storage/analysis. Its scheduled July 2025 review
date is overdue as of this report; the policy does not say that review lateness causes expiration.

The [daily-rate XML page](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
documents five rate datasets, year/month/all-history selection, zero-based paging, and a default
first page of 300 records. It does not publish a surface-specific persistence/training/
redistribution grant, numeric request ceiling, checksum, sequence, or affirmative no-fee
guarantee.

Decision:

- Keep Fiscal Data `Available`, no-token, `OfficialDelayed`, and all-rights-admitted only for exact
  Fiscal Data datasets/versions with retained lineage and a current endpoint receipt.
- Keep daily-rate XML `RightsLimited`; Fiscal Data evidence does not transfer to it.

## Rights-operation matrix

The following is the conservative release position for the audited capability revision. “Scoped”
means only the named surface/content/use and its duties, not all provider content.

| Surface | Retrieve | Local display | Persist/cache | Model training | Export | Redistribute | Special boundary |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Coinbase Exchange | Admitted, scoped | Admitted, internal | Pending | Pending | Pending | Pending | Fair-value/valuation/index/benchmark use restricted without written consent |
| Kraken Spot public | Admitted, scoped | Admitted, own-benefit | Pending | Pending | Pending | Pending | Regional terms and exact surface matter |
| SEC EDGAR | Admitted, scoped | Admitted, scoped | Admitted, scoped | Admitted, content-scoped | Admitted, scoped | Admitted, scoped | Preserve embedded excluded/third-party content boundary |
| FRED/ALFRED integrated vertical | Blocked | Blocked | Blocked | Blocked | Blocked | Blocked | Free key is not admission; exact series rights also required |
| BLS v1/v2 | Admitted, scoped | Admitted, scoped | Admitted, scoped | Admitted, content-scoped | Pending | Pending | Citation, disclaimer, truthful representation, third-party review |
| Treasury Fiscal Data | Admitted, dataset-scoped | Admitted | Admitted | Admitted, dataset-scoped | Admitted | Admitted | Exact dataset/version lineage |
| Treasury daily-rate XML | Admitted | Admitted | Pending | Pending | Pending | Pending | Separate surface-specific authority required |

## Official, encoded, and runtime reconciliation

“Official maximum support” is the strongest capability current provider evidence could permit. An
encoded metadata ceiling remains only a ceiling. “Current output” is what the audited application
can actually admit or qualify.

| Surface | Official maximum support | Encoded metadata ceiling/state at audit base | Currently admitted or qualified output | Mismatch owner and resolution |
| --- | --- | --- | --- | --- |
| Coinbase public Exchange | Unauthenticated traditional feed; public Level 2 delivery; no checksum and no reviewed public-profile sequence contract | `coinbase.public-market-data` is `NoCredential`, `RightsLimited`, static `DirectVerified`, but its evidence/probe names **Advanced Trade**, not Exchange | Shipping Exchange `level2`/`matches` adapter is `DirectUnverified`; no default automated action | **Implementation + evidence:** split provider families, bind Exchange evidence/probe, lower public static ceiling |
| Coinbase Exchange Direct/full | Authenticated `ws-direct`/full plus Level 3 snapshot/replay and per-product sequence; no checksum; authenticated-message caveat | Authenticated direct `SourceMetadata` can encode `DirectVerified`, `UserAuthorized`, sequence provided; no distinct built-in direct onboarding profile | Direct transport/config exists, but audited product coverage documentation and built-in activation do not expose a qualified Direct session; public output remains active ceiling | **Implementation + documentation + evidence:** add distinct credentialed profile/composition and unchanged-head qualification trace |
| Kraken Spot public | Single-venue public L2/trades; top-ten CRC32; no numeric book-update sequence | Built-in static profile says `DirectVerified`; adapter metadata/qualification correctly hard-code `DirectUnverified` | `DirectUnverified`; cannot pass default automated-action quality gate | **Implementation:** lower static catalog ceiling. **Documentation:** replace “could support” wording with evidence-derived maximum |
| SEC EDGAR | No-key, free, ten RPS, revision-preserving, broad scoped reuse | `OfficialDelayed`, `RefreshRequired`, rights admitted, exact-current-digest required | No active lease until evidence refresh/probe | **Evidence:** capture exact official bodies/digests and bounded declared-client response; encoded quality is correct |
| FRED/ALFRED | Free keyed transient access and PIT/vintages; durable storage/training prohibited | `OfficialDelayed`, `RightsBlocked`, all six product operations blocked | No active durable research lease | No capability mismatch. **Evidence/documentation:** pin `/legal/terms/`, current body digest, and exact per-series authority before any future revision |
| BLS v1 | Unregistered historical API; scoped durable/analysis rights | `OfficialDelayed`, `RefreshRequired`, retrieve/display/persist/train admitted; export/redistribute pending | No active lease until exact evidence/probe | **Evidence:** refresh official bodies/digests and probe. **Implementation review:** keep content-origin boundary before changing export/redistribute |
| BLS v2 | Registered keyed API with higher limits and annual renewal | `OfficialDelayed`, `RefreshRequired`, provider-controlled credential, same scoped rights | No active lease until evidence/probe and key import | **Evidence + credential:** current digest/probe and user key. Encoded quality/state are conservative |
| Treasury Fiscal Data | No-token versioned API; broad open reuse | `OfficialDelayed`, `Available`, all six operations admitted, exact dataset lineage | Profile may activate after bounded probe/session evidence; no live-quality claim | No material mismatch. **Evidence:** refresh exact response/schema as datasets change; track overdue policy review |
| Treasury daily-rate XML | Public documented feeds/pagination; no equivalent durable-use grant | `OfficialDelayed`, `RightsLimited`, persistence evidence absent | Retrieve/display only; no durable publication/model authority | **External rights evidence:** obtain surface-applicable authority plus exact response/schema evidence |

## Capability records requiring reconciliation

The following audited records should be reconciled by the integration owner; this research branch
does not change product code:

1. `crates/market-squawk-sources/src/onboarding/built_in_profiles.rs`
   - replace the single Advanced-Trade-backed Coinbase public profile with distinct Coinbase
     Exchange public and authenticated Direct evidence/activation identities;
   - update Coinbase `$0` status for the bounded default tier without implying paid higher tiers;
   - lower Kraken's static `quality_ceiling` to `DirectUnverified`;
   - bind Kraken's current book/checksum/trade/free-feed evidence;
   - pin FRED's current `/legal/terms/` locator and content evidence;
   - publish new review date/report digest only after exact tracked evidence is accepted; and
   - retain SEC/BLS `RefreshRequired`, FRED `RightsBlocked`, and Treasury XML `RightsLimited`
     until their exact predicates are met.
2. `docs/reference/source-coverage.md`
   - add the authenticated Coinbase Direct capability only after product composition/activation
     exists;
   - remove the assertion that Kraken's static `DirectVerified` ceiling is a reviewed capability
     the current provider evidence could support; and
   - retain the distinction between catalog ceilings and current observations.
3. `docs/research/providers/coinbase-direct-market-data-2026-07-22.md`
   - refresh implementation-state statements after exact integration; provider evidence itself
     remains useful.

These are three different owners:

- **Evidence** owns official source locators, content digests, terms scope, and provider predicates.
- **Implementation** owns profile identity, credentials, metadata ceilings, composition, and
  runtime qualification.
- **Documentation** owns truthful current-code descriptions after implementation is integrated.

## Exact unresolved external predicates

| Surface | Predicate that must be satisfied before promotion |
| --- | --- |
| Coinbase Exchange Direct | Successful provider-authorized account/credential trace proving exact `ws-direct`/full entitlement under the selected `$0` tier; explicit handling boundary for non-incrementing authenticated messages; exact snapshot/update/sequence/freshness/status/precision/coverage qualification; written consent for any restricted fair-value or third-party/derived-data use |
| Coinbase durable/model use | Provider terms or written authorization that affirmatively admits the exact persistence/cache/training/export operation; silence is not promotion |
| Kraken `DirectVerified` | Provider-published exact numeric progression/gap-recovery authority for the selected book channel, or a separately reviewed domain-contract change with equivalent evidence; CRC32 alone is insufficient |
| Kraken durable/model/export use | Surface- and jurisdiction-specific authority for the exact operation and output audience |
| SEC release activation | Exact current official bodies and SHA-256 digests; declared organization/admin-contact `User-Agent`; bounded production-runtime response/receipt; preserved correction/revision semantics |
| FRED/ALFRED durable/model use | Written scope-specific provider permission or binding terms change admitting storage/cache/archive/database/training, plus rights evidence for every selected series |
| BLS v1 activation | Exact current FAQ/terms content digests and bounded v1 runtime receipt under the encoded rate plan |
| BLS v2 activation | v1 evidence predicates plus provider-controlled registration, current emailed key, write-only local import, bounded keyed receipt, and annual-renewal handling |
| Treasury Fiscal Data activation/refresh | Exact endpoint response/schema/dataset version and lineage; unauthenticated receipt resolving the generic 403-table ambiguity; later official policy revision if published |
| Treasury daily-rate XML durable use | Surface-applicable persistence/training/export/redistribution authority; exact response and schema/XSD evidence; conservative rate policy because no numeric ceiling is published |

## Source matrix

All pages were retrieved or verified on 2026-07-25.

| Provider | Onboarding/fee | Technical coverage/integrity/time | Rights authority |
| --- | --- | --- | --- |
| Coinbase | [Market Data Connections](https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections) | [Overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview), [authentication](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication), [channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels), [rates](https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits) | [Market Data Terms](https://www.coinbase.com/legal/market_data) |
| Kraken | [Public endpoints](https://support.kraken.com/articles/360000919986-public-endpoint-examples-you-can-try-them-directly-in-a-web-browser-), [free-feed guidance](https://blog.kraken.com/product/api/unlocked-3-the-market-data-feeds-systematic-traders-use) | [Book schema](https://docs.kraken.com/api/docs/websocket-v2/book), [checksum](https://docs.kraken.com/api/docs/guides/spot-ws-book-v2/), [trade](https://docs.kraken.com/api/docs/websocket-v2/trade/), [FAQ](https://support.kraken.com/articles/360022326871-kraken-websocket-api-frequently-asked-questions), [rates](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-), [history](https://support.kraken.com/articles/360047543791-downloadable-historical-market-data-time-and-sales-) | [Global Terms](https://www.kraken.com/legal/global-terms) |
| SEC | No separate account/key page required by public API | [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), [access guidance](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data) | [SEC FAQ/reuse](https://www.sec.gov/about/webmaster-frequently-asked-questions) |
| FRED/ALFRED | [Free account](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/), [API key](https://fred.stlouisfed.org/docs/api/api_key.html) | [FRED vs. ALFRED](https://fred.stlouisfed.org/docs/api/fred/fred_vs_alfred.html), [v2 release observations](https://fred.stlouisfed.org/docs/api/fred/v2/release_observations.html), [v2 errors/rate](https://fred.stlouisfed.org/docs/api/fred/v2/errors.html) | [FRED Terms](https://fred.stlouisfed.org/legal/terms/) |
| BLS | [API FAQ/v1-v2 registration and limits](https://www.bls.gov/developers/api_faqs.htm) | Same FAQ; no provider vintage/sequence/checksum contract located | [API Terms](https://www.bls.gov/developers/termsOfService.htm), [copyright](https://www.bls.gov/opub/copyright-information.htm) |
| Treasury Fiscal Data | [API access](https://fiscaldata.treasury.gov/api-documentation/) | Same API page; versioned schema/pagination/representations | [API reuse statement](https://fiscaldata.treasury.gov/api-documentation/), [Open Data Policy 901-1](https://fiscaldata.treasury.gov/data/about-us/901-1%20Open%20Data%20Policy.pdf) |
| Treasury daily XML | No credential/fee parameter appears in the documented grammar | [Daily interest-rate XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | No surface-specific durable-use authority located |
| Cross-provider training boundary | Not applicable | Not applicable | U.S. Copyright Office, [*Copyright and Artificial Intelligence, Part 3*](https://www.copyright.gov/ai/Copyright-and-Artificial-Intelligence-Part-3-Generative-AI-Training-Report-Pre-Publication-Version.pdf) |

## Method and validation

The full source workflow used four discovery categories, a structured source inventory, ten
bounded deep-dive batches, four category syntheses, a deduplicated final technical report, and a
fresh evidence audit. Academic screening produced a substantive zero-candidate result because
third-party practice cannot establish provider authority.

Artifact checks at report freeze:

- 41 inventoried sources: 33 assigned and eight excluded;
- all ten planned batch reports present;
- every report contains a table of contents and source lineage;
- no internal search-result reference is used as evidence;
- structural validator: pass;
- evidence-audit verdict: `PASS_WITH_NOTES`;
- notes limited to disclosed provider freshness, jurisdiction, mixed-content, and external runtime
  predicates; and
- no Cargo command, product code, test, script, plan, README, ledger, memory, or existing evidence
  record changed in this research branch.
