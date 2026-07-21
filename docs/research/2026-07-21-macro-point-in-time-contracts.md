# Macro point-in-time provider contracts

Date assessed: 2026-07-21  
Audit base: `6404e8b865bcfe30c3914325bab3245b9feb126c`  
Refresh gate: re-check when FRED/ALFRED observation semantics, Treasury's Atom feed, or the canonical
research temporal schema changes.

## Decisions

- FRED/ALFRED observation `realtime_start` is retained as the calendar-date publication/vintage
  coordinate for that revision. `realtime_end` is inclusive, so a finite end is converted with
  checked Gregorian arithmetic to the next calendar date before being stored as the canonical
  exclusive supersession coordinate. `9999-12-31` remains open-ended.
- Effective observation time and revision transaction time are independent axes. A historical
  revision may have been superseded before local ingestion, and a forecast may be superseded before
  its future effective date. Only the same-axis invariant `superseded > published` is enforced.
- Treasury daily par-yield Atom `updated` values are RFC 3339 instants, not opaque revision text.
  The parser rejects invalid Atom Date constructs and retains each entry's exact instant as source
  timestamp and canonical publication time. Fiscal Data civil record dates do not acquire invented
  timestamp precision.

## Primary sources

- [ALFRED Download Data Help](https://alfred.stlouisfed.org/help/downloaddata) defines real-time
  start/end as the first/last vintage dates for which a value is the latest revision.
- [FRED API Real-Time Periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
  defines real-time periods as closed/closed dates describing when information was known until it
  changed, with `9999-12-31` representing the open future bound.
- [RFC 4287, Atom Syndication Format](https://www.rfc-editor.org/rfc/rfc4287.html) requires Atom Date
  constructs to conform to RFC 3339 and defines `atom:updated` as the most recent significant
  modification instant.
