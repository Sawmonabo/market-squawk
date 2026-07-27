# BLS Public Data API contract — 2026-07-21

Audit base: `069ccc39add6d5f185fefb1d3a51e96505a5773d` (macro adapter lane rebased
onto release `6960e41`).

Last official-source review: 2026-07-26.

Refresh this note before release when BLS changes its API signatures, terms, quota table, or
response schema. Network smoke tests remain opt-in; deterministic fixtures are the default suite.

## 2026-07-26 release decision

The official sources were reviewed again for the V1 provider gate:

- BLS still describes v1 as unregistered and open for public use. It is the required zero-fee
  release path.
- The current FAQ still publishes 25 daily queries, 25 series per query, 10 years per query, and
  50 requests per 10 seconds for v1. Registered v2 remains an optional higher-limit path with 500
  daily queries, 50 series, 20 years, an emailed registration key, and annual renewal.
- The API terms still say downloaded data should not carry end-use controls. They require the
  retrieval date, the BLS post-retrieval disclaimer, truthful representation, and limit
  compliance.
- BLS still states that its publications are public domain except previously copyrighted
  photographs and illustrations, and asks users to cite BLS as the source.
- The API still provides current published historical observations, not a historical-vintage
  interface. Market Squawk must retain direct BLS provenance and locally observed availability and
  not present those responses as reconstructed vintages.

For the unemployment release vertical, the exact source series is `LNS14000000`. Durable
persistence, analytics, and model-input use are admitted for direct BLS-authored observations with
the duties above. A real release run must retrieve the official v1 response, publish one immutable
Arrow/Parquet generation, query it through the analytical service, recover it after restart, and
bind any derived training dataset to that exact BLS parent.

## Provider facts used by the adapter

- Public v1 is unregistered. Registered v2 uses a user-supplied registration key. The key is
  treated as an opaque bounded secret because BLS documents where it belongs but does not publish
  a normative character grammar.
- The JSON POST endpoints are exactly
  `https://api.bls.gov/publicAPI/v1/timeseries/data/` and
  `https://api.bls.gov/publicAPI/v2/timeseries/data/`.
- Series identifiers use uppercase letters and digits and may include `_`, `-`, and `#`.
- V1 permits 25 series, 10 years, and 25 daily queries. Registered v2 permits 50 series,
  20 years, and 500 daily queries. Both tiers publish a 50-requests-per-10-seconds rate limit.
- BLS confirms that the daily allowance resets, but does not document a reset anchor or timezone.
  The implementation therefore treats both limits as conjunctive rolling windows rather than
  guessing a provider calendar boundary that could permit a boundary burst.
- The version-specific v2 signature and the FAQ tier table both state a 20-year v2 window. The
  generic FAQ sentence stating 10 years is applied to v1, not used to double v2 request count.
- The v2 JSON field is `registrationkey` with a lowercase `k`.
- Observation time is a source-authored year/period code such as `2026` plus `M06`; it is not an
  authoritative civil day or UTC instant. Normalization therefore preserves the provider period
  and does not fabricate a first-of-month date or midnight timestamp.
- The API returns published historical observations but no documented historical-vintage query.
  Revisions are therefore retained as locally observed exact response versions, not presented as
  reconstructed BLS publication vintages.
- The observation response does not include a universal unit contract. The optional v2 catalog
  fields are available only for some Data Finder series and do not establish an explicit unit for
  every BLS series. Production configuration therefore requires one exact, bounded, separately
  verified metadata record per requested series, including explicit unit, title, frequency,
  seasonal-adjustment, and measure semantics plus the user's authorization-record reference.

## Implementation decisions

- Endpoint policy, registry-issued shared provider budget, response byte bounds, request deadline,
  cancellation, and exact series/year response binding all fail closed before normalization.
- Registry authority must enforce every applicable window atomically for the same canonical
  public or registered allocation: 25 requests per rolling day plus 50 per 10 seconds for v1, and
  500 requests per rolling day plus 50 per 10 seconds for v2.
- Automatic redirects, ambient proxies, implicit retries, and compression are disabled. The
  adapter requests and accepts identity encoding only, then counts every streamed response byte.
- Partial responses, missing or extra series, duplicates, out-of-window observations, changed
  refetch content, and provider messages/empty series invalidate the normalized batch.
- Raw-response SHA-256, request-plan identity, source/revision metadata, first local observation
  time, preliminary/latest flags, lexical value, decimal value, and footnotes are preserved.
- The request-plan identity binds each series metadata payload digest and authorization reference.
  Missing, duplicate, malformed, mismatched-digest, or schema-unknown metadata fails closed; the
  adapter never derives a unit from the series identifier, title, or observed values.

## Primary sources

- BLS v1 signatures: https://www.bls.gov/developers/api_signature.htm
- BLS v2 signatures and optional catalog fields:
  https://www.bls.gov/developers/api_signature_v2.htm
- BLS API FAQ and quota table: https://www.bls.gov/developers/api_faqs.htm
- BLS API features and daily reset behavior: https://www.bls.gov/bls/api_features.htm
- BLS API terms of service: https://www.bls.gov/developers/termsOfService.htm
- BLS copyright information: https://www.bls.gov/opub/copyright-information.htm
- BLS series `LNS14000000`: https://data.bls.gov/timeseries/LNS14000000
