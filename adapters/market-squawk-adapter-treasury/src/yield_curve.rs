use market_squawk_domain::{CalendarDate, DataQuality, Timestamp};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    FiscalDataParseLimits, TreasuryDailyRateFamily, TreasuryDailyRateMetric,
    TreasuryDailyRateObservation, TreasuryDailyRatePage, TreasuryDailyRatePageRequest,
    TreasuryDailyRateQuery, TreasuryMaturity, TreasuryProtocolError,
};

const METHODOLOGY_URL: &str = "https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics/treasury-yield-curve-methodology";
const METHODOLOGY_REVISION: &str = "monotone-convex-2021-12-06/reviewed-2025-02-18";

/// Backward-compatible nominal par-yield profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreasuryYieldCurveProfile;

impl TreasuryYieldCurveProfile {
    /// Returns the official nominal par-yield profile.
    pub const fn daily_par_yield_curve() -> Self {
        Self
    }

    /// Returns the source-delivery quality used by the research plane.
    pub const fn quality(self) -> DataQuality {
        TreasuryDailyRateFamily::NominalParYieldCurve.quality()
    }

    /// Returns the exact official feed identity.
    pub const fn source_identity(self) -> &'static str {
        TreasuryDailyRateFamily::NominalParYieldCurve.feed_identity()
    }

    /// Returns Treasury's official nominal par-yield methodology page.
    pub const fn methodology_url(self) -> &'static str {
        METHODOLOGY_URL
    }

    /// Returns the adapter-bound methodology revision.
    pub const fn methodology_revision(self) -> &'static str {
        METHODOLOGY_REVISION
    }

    /// Binds one provider year into the legacy nominal request API.
    ///
    /// # Errors
    ///
    /// Rejects years before 1990, years after 9999, and nonzero pages.
    pub fn page(
        self,
        year: u16,
        page_number: usize,
    ) -> Result<TreasuryYieldCurvePageRequest, TreasuryProtocolError> {
        let inner =
            TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::NominalParYieldCurve, year)?
                .page(page_number)?;
        Ok(TreasuryYieldCurvePageRequest { inner, year })
    }
}

/// Backward-compatible exact nominal par-yield request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryYieldCurvePageRequest {
    inner: TreasuryDailyRatePageRequest,
    year: u16,
}

impl TreasuryYieldCurvePageRequest {
    /// Returns the allowlisted official HTTPS URL.
    pub fn url(&self) -> &str {
        self.inner.url()
    }

    /// Returns the exact requested year.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns zero, the legacy identity sentinel for a complete year response.
    pub const fn page_number(&self) -> usize {
        self.inner.page_number()
    }

    /// Returns the canonical query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.inner.query_digest()
    }

    /// Returns the exact request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.inner.request_digest()
    }

    pub(crate) const fn as_daily_request(&self) -> &TreasuryDailyRatePageRequest {
        &self.inner
    }
}

/// One backward-compatible nominal par-yield observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DailyParYieldCurveObservation {
    inner: TreasuryDailyRateObservation,
}

impl DailyParYieldCurveObservation {
    /// Returns Treasury's stable row identifier.
    pub fn source_record_id(&self) -> &str {
        self.inner.source_record_id()
    }

    /// Returns the exact provider civil date.
    pub const fn record_date(&self) -> CalendarDate {
        self.inner.record_date()
    }

    /// Returns one exact nominal par-yield percentage.
    pub fn rate_percent(&self, maturity: TreasuryMaturity) -> Option<Decimal> {
        self.inner
            .point(TreasuryDailyRateMetric::NominalParYield(maturity))
            .map(crate::TreasuryDailyRatePoint::rate_percent)
    }

    /// Returns the one-month rate when published.
    pub fn one_month_percent(&self) -> Option<Decimal> {
        self.rate_percent(TreasuryMaturity::OneMonth)
    }

    /// Returns the thirty-year rate when published.
    pub fn thirty_year_percent(&self) -> Option<Decimal> {
        self.rate_percent(TreasuryMaturity::ThirtyYears)
    }

    /// Returns the exact Atom entry update instant.
    pub const fn source_published_at(&self) -> Timestamp {
        self.inner.source_published_at()
    }

    /// Returns the canonical provider-row identity.
    pub const fn row_identity(&self) -> [u8; 32] {
        self.inner.row_identity()
    }

    /// Returns the exact containing-payload SHA-256 identity.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.inner.source_payload_digest()
    }
}

/// One backward-compatible nominal par-yield response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DailyParYieldCurvePage {
    inner: TreasuryDailyRatePage,
    observations: Vec<DailyParYieldCurveObservation>,
}

impl DailyParYieldCurvePage {
    /// Parses a bounded nominal par-yield Atom/OData response.
    ///
    /// # Errors
    ///
    /// Rejects malformed XML, source or schema drift, duplicate rows, configured resource
    /// overruns, invalid financial values, and rows outside the requested year.
    pub fn parse(
        bytes: &[u8],
        request: &TreasuryYieldCurvePageRequest,
        limits: FiscalDataParseLimits,
    ) -> Result<Self, TreasuryProtocolError> {
        let inner = TreasuryDailyRatePage::parse(bytes, request.as_daily_request(), limits)?;
        let observations = inner
            .observations()
            .iter()
            .cloned()
            .map(|inner| DailyParYieldCurveObservation { inner })
            .collect();
        Ok(Self {
            inner,
            observations,
        })
    }

    /// Returns whether this response contains no entries.
    pub fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    /// Returns exact normalized nominal observations.
    pub fn observations(&self) -> &[DailyParYieldCurveObservation] {
        &self.observations
    }

    /// Returns the exact Atom feed update instant.
    pub const fn feed_published_at(&self) -> Timestamp {
        self.inner.feed_published_at()
    }

    /// Returns the canonical query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.inner.query_digest()
    }

    /// Returns the exact request digest.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.inner.request_digest()
    }

    /// Returns the exact response SHA-256 identity.
    pub const fn response_payload_digest(&self) -> [u8; 32] {
        self.inner.response_payload_digest()
    }
}
