//! Bounded U.S. Treasury Fiscal Data and daily-rate extraction with strict schema validation.

mod client;
mod daily_rates;
mod fiscal_data;
mod query;
mod rates;
mod source;
mod yield_curve;

pub use daily_rates::{
    TreasuryBillMaturity, TreasuryBillRateMeasure, TreasuryDailyRateFamily,
    TreasuryDailyRateMetric, TreasuryDailyRateObservation, TreasuryDailyRatePage,
    TreasuryDailyRatePageRequest, TreasuryDailyRatePaginationTracker, TreasuryDailyRatePeriod,
    TreasuryDailyRatePeriodKind, TreasuryDailyRatePoint, TreasuryDailyRateQuery,
    TreasuryExtrapolationFactor, TreasuryLongTermRateType, TreasuryMaturity,
};
pub use fiscal_data::{
    FiscalDataPage, FiscalDataParseLimits, FiscalDataRecord, TreasuryPaginationTracker,
    TreasuryProtocolError,
};
pub use query::{TreasuryDatasetProfile, TreasuryFiscalQuery, TreasuryPageRequest};
pub use rates::{AverageInterestRate, TreasuryRateError, TreasuryRateProfile};
pub use source::{
    RetrievedDailyRatePage, RetrievedFiscalDataPage, RetrievedYieldCurvePage,
    TreasuryDailyRatesConfig, TreasuryExtractionOutput, TreasurySource, TreasurySourceConfig,
    TreasurySourceError, TreasurySourceHealth,
};
pub use yield_curve::{
    DailyParYieldCurveObservation, DailyParYieldCurvePage, TreasuryYieldCurvePageRequest,
    TreasuryYieldCurveProfile,
};
