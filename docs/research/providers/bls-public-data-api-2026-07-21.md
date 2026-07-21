# BLS Public Data API contract — 2026-07-21

Audit base: `d8bc66d3e0b1e3f1971ec83c485882de4aff4d80` (macro adapter lane rebased
onto release `dc3016593edebe8114d792e1268a56dd8c126fa4`).

Refresh this note before release when BLS changes its API signatures, terms, quota table, or
response schema. Network smoke tests remain opt-in; deterministic fixtures are the default suite.

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
- The version-specific v2 signature and the FAQ tier table both state a 20-year v2 window. The
  generic FAQ sentence stating 10 years is applied to v1, not used to double v2 request count.
- The v2 JSON field is `registrationkey` with a lowercase `k`.
- Observation time is a source-authored year/period code such as `2026` plus `M06`; it is not an
  authoritative civil day or UTC instant. Normalization therefore preserves the provider period
  and does not fabricate a first-of-month date or midnight timestamp.
- The API returns published historical observations but no documented historical-vintage query.
  Revisions are therefore retained as locally observed exact response versions, not presented as
  reconstructed BLS publication vintages.

## Implementation decisions

- Endpoint policy, registry-issued shared provider budget, response byte bounds, request deadline,
  cancellation, and exact series/year response binding all fail closed before normalization.
- Because the current shared budget has one window, the adapter requires a conservative 24-hour
  shared window capped at 25 requests for v1 or 50 requests for v2. This single limit satisfies
  both the daily and short-window provider restrictions without identity or account rotation.
- Automatic redirects, ambient proxies, implicit retries, and compression are disabled. The
  adapter requests and accepts identity encoding only, then counts every streamed response byte.
- Partial responses, missing or extra series, duplicates, out-of-window observations, changed
  refetch content, and provider messages/empty series invalidate the normalized batch.
- Raw-response SHA-256, request-plan identity, source/revision metadata, local first-observation
  time, preliminary/latest flags, lexical value, decimal value, and footnotes are preserved.

## Primary sources

- BLS v1 signatures: https://www.bls.gov/developers/api_signature.htm
- BLS v2 signatures: https://www.bls.gov/developers/api_signature_v2.htm
- BLS API FAQ and quota table: https://www.bls.gov/developers/api_faqs.htm
- BLS API terms of service: https://www.bls.gov/developers/termsOfService.htm
