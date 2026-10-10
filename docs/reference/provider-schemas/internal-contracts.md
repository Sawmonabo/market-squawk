# Internal contracts — separate from provider responses

This appendix identifies derived/normalized contracts referenced during source-field discovery. They are application or adapter objects, never additional upstream JSON/TSV fields. Their entire schemas remain with the authorities linked below; they are intentionally excluded from provider field tables.

| Internal family | Schema authority | Meaning |
|---|---|---|
| Provider-neutral market quotes/bars/options | [Canonical schemas](../market-data-canonical-schemas.md) | Normalized observations, identity, units, clocks and source lineage |
| Company financial pages/ratios | [financial-page schema](../../../apps/market-squawk/src/application/contracts/output/investment_financials.rs) | Mapped reporting facts/statements/filings and computed ratios |
| Fund products and overlap/concentration | [fund projection](../../../apps/market-squawk/src/application/research/fund_product.rs) | PIT holdings/annual/NAV projections and derived analyses |
| Fund NAV | [FundNav contract](../../../crates/market-squawk-domain/src/research/fund_nav.rs) | Share class, NAV date/value/missing state, availability and revision |
| Tiingo NAV candidate | [Tiingo NAV mapper](../../../adapters/market-squawk-adapter-tiingo/src/nav.rs) | Local eligibility, receive/decode clocks and unavailable provider revision |
| Forecasts / model targets | [output schemas](../../../apps/market-squawk/src/application/contracts/output.rs) | Backend central path, calibrated intervals and target dates |
| Investment decision / valuation / three probabilities | [decision output](../../../apps/market-squawk/src/application/contracts/output.rs) | Independent up/outperformance/after-cost event probabilities, valuation scenarios, entry/add/trim/exit |
| Backtests / harmonic geometry | [analysis output](../../../apps/market-squawk/src/application/contracts/output.rs) | Chronological cost-adjusted evaluation and causal price-pattern evidence |

Internal CalendarDate serializes year/month/day objects; raw Timestamp serialization is an integer Unix-nanosecond value. Served APIs may explicitly format timestamps differently. An internal FundProductValue state/value wrapper is not supplied by SEC or Tiingo. NAV provider revision/publication absence must remain absent, without invented upstream revision keys.
Source: [time.rs](../../../crates/market-squawk-domain/src/time.rs).

This reference does not reproduce selected per-instrument app output fields or consumer/UI status rows. Nothing in it claims that parser support, a retained observation or a derived contract is a complete installed product workflow.
