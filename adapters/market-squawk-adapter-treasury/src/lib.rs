//! Bounded U.S. Treasury Average Interest Rates V2 and all five daily-rate XML families.
//!
//! The Fiscal Data surface here is intentionally the selected Average Interest Rates V2 vertical;
//! it does not claim auction, debt, or every Treasury Fiscal Data dataset is implemented.

mod client;
mod daily_rates;
mod fiscal_data;
mod query;
mod rates;
mod source;
mod vertical;
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
    TreasuryAllHistoryAcquisitionCompletion, TreasuryAllHistoryBackfill,
    TreasuryAllHistoryCanonicalPage, TreasuryAllHistoryCheckpoint, TreasuryAllHistoryFetchedPage,
    TreasuryAllHistoryPageAdmission, TreasuryDailyRatesConfig, TreasuryDoctorRun,
    TreasuryDoctorSealError, TreasuryExtractionOutput, TreasuryExtractionSealError,
    TreasurySealedExtraction, TreasurySource, TreasurySourceConfig, TreasurySourceError,
    TreasurySourceHealth,
};
pub use vertical::{
    TreasuryActivationIntent, TreasuryDashboardDatasetRead, TreasuryDashboardReadPlan,
    TreasuryDashboardSeriesMode, TreasuryDatasetCatalog, TreasuryDatasetDescriptor,
    TreasuryDatasetFamily, TreasuryDatasetPeriod, TreasuryDiscoveryAccounting,
    TreasuryDiscoveryCompleteness, TreasuryDiscoveryOutput, TreasuryDoctorObservation,
    TreasuryDoctorPlan, TreasuryDoctorProbe, TreasuryDoctorReceipt, TreasuryExtractionAccounting,
    TreasuryExtractionCommitment, TreasuryPublicationExpectation, TreasuryPublicationMode,
    TreasurySealedDoctorReceipt, TreasurySurface, TreasuryVerticalError,
};
pub use yield_curve::{
    DailyParYieldCurveObservation, DailyParYieldCurvePage, TreasuryYieldCurvePageRequest,
    TreasuryYieldCurveProfile,
};
