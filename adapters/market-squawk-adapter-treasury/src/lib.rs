//! Bounded U.S. Treasury Fiscal Data extraction with schema and pagination validation.

mod client;
mod fiscal_data;
mod query;
mod rates;
mod source;
mod yield_curve;

pub use fiscal_data::{
    FiscalDataPage, FiscalDataParseLimits, FiscalDataRecord, TreasuryPaginationTracker,
    TreasuryProtocolError,
};
pub use query::{TreasuryDatasetProfile, TreasuryFiscalQuery, TreasuryPageRequest};
pub use rates::{AverageInterestRate, TreasuryRateError, TreasuryRateProfile};
pub use source::{
    RetrievedFiscalDataPage, RetrievedYieldCurvePage, TreasurySource, TreasurySourceConfig,
    TreasurySourceError, TreasurySourceHealth,
};
pub use yield_curve::{
    DailyParYieldCurveObservation, DailyParYieldCurvePage, TreasuryMaturity,
    TreasuryYieldCurvePageRequest, TreasuryYieldCurveProfile,
};
