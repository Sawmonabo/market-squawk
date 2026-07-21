//! Bounded U.S. Treasury Fiscal Data extraction with schema and pagination validation.

mod fiscal_data;
mod rates;

pub use fiscal_data::{
    FiscalDataPage, FiscalDataParseLimits, FiscalDataRecord, TreasuryPaginationTracker,
    TreasuryProtocolError,
};
pub use rates::{AverageInterestRate, TreasuryRateError, TreasuryRateProfile};
