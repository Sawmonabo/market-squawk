use market_squawk_domain::{DataQuality, SourceIdentifier};
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::TreasuryProtocolError;
use crate::query::update_component;

const FEED_ENDPOINT: &str =
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml";

/// The closed set of official Treasury daily-interest-rate XML datasets.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryDailyRateFamily {
    /// Daily nominal par-yield curve rates.
    NominalParYieldCurve,
    /// Daily Treasury bill rates.
    BillRates,
    /// Daily nominal and real long-term rates.
    LongTermRates,
    /// Daily real par-yield curve rates.
    RealParYieldCurve,
    /// Daily real long-term rate averages.
    RealLongTermRates,
}

impl TreasuryDailyRateFamily {
    /// Every supported family in stable provider order.
    pub const ALL: [Self; 5] = [
        Self::NominalParYieldCurve,
        Self::BillRates,
        Self::LongTermRates,
        Self::RealParYieldCurve,
        Self::RealLongTermRates,
    ];

    /// Returns the exact Treasury `data` parameter.
    pub const fn provider_key(self) -> &'static str {
        match self {
            Self::NominalParYieldCurve => "daily_treasury_yield_curve",
            Self::BillRates => "daily_treasury_bill_rates",
            Self::LongTermRates => "daily_treasury_long_term_rate",
            Self::RealParYieldCurve => "daily_treasury_real_yield_curve",
            Self::RealLongTermRates => "daily_treasury_real_long_term",
        }
    }

    /// Returns the first year Treasury declares available for this dataset.
    pub const fn start_year(self) -> u16 {
        match self {
            Self::NominalParYieldCurve => 1990,
            Self::BillRates => 2002,
            Self::LongTermRates | Self::RealLongTermRates => 2000,
            Self::RealParYieldCurve => 2003,
        }
    }

    /// Returns the exact Atom feed identity.
    pub const fn feed_identity(self) -> &'static str {
        match self {
            Self::NominalParYieldCurve => {
                "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve"
            }
            Self::BillRates => {
                "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_bill_rates"
            }
            Self::LongTermRates => {
                "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_long_term_rate"
            }
            Self::RealParYieldCurve => {
                "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_real_yield_curve"
            }
            Self::RealLongTermRates => {
                "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_real_long_term"
            }
        }
    }

    /// Returns the exact Atom feed title.
    pub const fn feed_title(self) -> &'static str {
        match self {
            Self::NominalParYieldCurve => "DailyTreasuryYieldCurveRateData",
            Self::BillRates => "DailyTreasuryBillRateData",
            Self::LongTermRates => "DailyTreasuryLongTermRateData",
            Self::RealParYieldCurve => "DailyTreasuryRealYieldCurveRateData",
            Self::RealLongTermRates => "DailyTreasuryRealLongTermRateAverageData",
        }
    }

    /// Returns the stable family token used in Market Squawk dataset identities.
    pub const fn dataset_family_token(self) -> &'static str {
        match self {
            Self::NominalParYieldCurve => "daily-par-yield-curve",
            Self::BillRates => "daily-bill-rates",
            Self::LongTermRates => "daily-long-term-rates",
            Self::RealParYieldCurve => "daily-real-par-yield-curve",
            Self::RealLongTermRates => "daily-real-long-term-rates",
        }
    }

    /// Returns the non-execution source-delivery quality.
    pub const fn quality(self) -> DataQuality {
        DataQuality::OfficialDelayed
    }

    pub(crate) const fn schema_revision(self) -> &'static str {
        match self {
            Self::NominalParYieldCurve => "treasury-daily-nominal-par-v2",
            Self::BillRates => "treasury-daily-bills-v1",
            Self::LongTermRates => "treasury-daily-long-term-v1",
            Self::RealParYieldCurve => "treasury-daily-real-par-v1",
            Self::RealLongTermRates => "treasury-daily-real-long-term-v1",
        }
    }
}

/// The kind of official Treasury time-period selector.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TreasuryDailyRatePeriodKind {
    /// One complete calendar year.
    Year,
    /// One complete calendar month.
    Month,
    /// The provider's paginated all-history dataset.
    AllHistory,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum PeriodValue {
    Year(u16),
    Month { year: u16, month: u8 },
    AllHistory,
}

/// A validated Treasury daily-rate period.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TreasuryDailyRatePeriod(PeriodValue);

impl TreasuryDailyRatePeriod {
    fn year(year: u16) -> Self {
        Self(PeriodValue::Year(year))
    }

    fn month(year: u16, month: u8) -> Self {
        Self(PeriodValue::Month { year, month })
    }

    const fn all_history() -> Self {
        Self(PeriodValue::AllHistory)
    }

    /// Returns the selector kind.
    pub const fn kind(self) -> TreasuryDailyRatePeriodKind {
        match self.0 {
            PeriodValue::Year(_) => TreasuryDailyRatePeriodKind::Year,
            PeriodValue::Month { .. } => TreasuryDailyRatePeriodKind::Month,
            PeriodValue::AllHistory => TreasuryDailyRatePeriodKind::AllHistory,
        }
    }

    /// Returns the selected year for year and month queries.
    pub const fn year_value(self) -> Option<u16> {
        match self.0 {
            PeriodValue::Year(year) | PeriodValue::Month { year, .. } => Some(year),
            PeriodValue::AllHistory => None,
        }
    }

    /// Returns the selected month when this is a month query.
    pub const fn month_value(self) -> Option<u8> {
        match self.0 {
            PeriodValue::Month { month, .. } => Some(month),
            PeriodValue::Year(_) | PeriodValue::AllHistory => None,
        }
    }

    /// Returns whether the query uses Treasury's zero-based all-history pagination.
    pub const fn is_all_history(self) -> bool {
        matches!(self.0, PeriodValue::AllHistory)
    }

    fn dataset_suffix(self) -> String {
        match self.0 {
            PeriodValue::Year(year) => year.to_string(),
            PeriodValue::Month { year, month } => format!("{year:04}-{month:02}"),
            PeriodValue::AllHistory => "all".to_owned(),
        }
    }

    fn digest_token(self) -> String {
        match self.0 {
            PeriodValue::Year(year) => format!("year:{year:04}"),
            PeriodValue::Month { year, month } => format!("month:{year:04}{month:02}"),
            PeriodValue::AllHistory => "all-history:zero-based-pages-of-300".to_owned(),
        }
    }
}

/// One immutable Treasury daily-rate query family excluding only page number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDailyRateQuery {
    family: TreasuryDailyRateFamily,
    period: TreasuryDailyRatePeriod,
    dataset: SourceIdentifier,
    analytical_dataset: SourceIdentifier,
    query_digest: [u8; 32],
}

impl TreasuryDailyRateQuery {
    /// Creates a query for one complete provider year.
    ///
    /// # Errors
    ///
    /// Rejects years before the selected family's official start or after year 9999.
    pub fn year(family: TreasuryDailyRateFamily, year: u16) -> Result<Self, TreasuryProtocolError> {
        Self::try_new(family, TreasuryDailyRatePeriod::year(year))
    }

    /// Creates a query for one complete provider month.
    ///
    /// # Errors
    ///
    /// Rejects an invalid month or a year outside the selected family's official range.
    pub fn month(
        family: TreasuryDailyRateFamily,
        year: u16,
        month: u8,
    ) -> Result<Self, TreasuryProtocolError> {
        if !(1..=12).contains(&month) {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        Self::try_new(family, TreasuryDailyRatePeriod::month(year, month))
    }

    /// Creates the provider's paginated all-history query.
    pub fn all_history(family: TreasuryDailyRateFamily) -> Result<Self, TreasuryProtocolError> {
        Self::try_new(family, TreasuryDailyRatePeriod::all_history())
    }

    fn try_new(
        family: TreasuryDailyRateFamily,
        period: TreasuryDailyRatePeriod,
    ) -> Result<Self, TreasuryProtocolError> {
        if period
            .year_value()
            .is_some_and(|year| !(family.start_year()..=9999).contains(&year))
        {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let dataset = SourceIdentifier::try_from(format!(
            "treasury:{}:{}",
            family.dataset_family_token(),
            period.dataset_suffix()
        ))
        .map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        let analytical_dataset = SourceIdentifier::try_from(format!(
            "treasury.{}.{}",
            family.dataset_family_token(),
            period.dataset_suffix()
        ))
        .map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        let query_digest = query_digest(family, period, &dataset);
        Ok(Self {
            family,
            period,
            dataset,
            analytical_dataset,
            query_digest,
        })
    }

    /// Binds a page to this exact query.
    ///
    /// # Errors
    ///
    /// Year and month queries accept only page zero as a local identity sentinel. All-history
    /// queries accept Treasury's documented zero-based page sequence.
    pub fn page(
        &self,
        page_number: usize,
    ) -> Result<TreasuryDailyRatePageRequest, TreasuryProtocolError> {
        if !self.period.is_all_history() && page_number != 0 {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let mut url = Url::parse(FEED_ENDPOINT).map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("data", self.family.provider_key());
            match self.period.0 {
                PeriodValue::Year(year) => {
                    query.append_pair("field_tdr_date_value", &format!("{year:04}"));
                }
                PeriodValue::Month { year, month } => {
                    query.append_pair(
                        "field_tdr_date_value_month",
                        &format!("{year:04}{month:02}"),
                    );
                }
                PeriodValue::AllHistory => {
                    query
                        .append_pair("field_tdr_date_value", "all")
                        .append_pair("page", &page_number.to_string());
                }
            }
        }
        let request_digest = request_digest(self.query_digest, page_number);
        Ok(TreasuryDailyRatePageRequest {
            family: self.family,
            period: self.period,
            dataset: self.dataset.clone(),
            url: url.into(),
            page_number,
            query_digest: self.query_digest,
            request_digest,
        })
    }

    /// Returns the provider dataset family.
    pub const fn family(&self) -> TreasuryDailyRateFamily {
        self.family
    }

    /// Returns the validated provider period.
    pub const fn period(&self) -> TreasuryDailyRatePeriod {
        self.period
    }

    /// Returns whether this query uses all-history pagination.
    pub const fn is_all_history(&self) -> bool {
        self.period.is_all_history()
    }

    /// Returns the canonical provider dataset identity accepted by discovery.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the storage-safe analytical identity for this exact provider dataset.
    pub const fn analytical_dataset(&self) -> &SourceIdentifier {
        &self.analytical_dataset
    }

    /// Returns the exact query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }
}

/// One exact allowlisted Treasury daily-rate request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDailyRatePageRequest {
    family: TreasuryDailyRateFamily,
    period: TreasuryDailyRatePeriod,
    dataset: SourceIdentifier,
    url: String,
    page_number: usize,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
}

impl TreasuryDailyRatePageRequest {
    /// Returns the official HTTPS request URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the provider dataset family.
    pub const fn family(&self) -> TreasuryDailyRateFamily {
        self.family
    }

    /// Returns the validated provider period.
    pub const fn period(&self) -> TreasuryDailyRatePeriod {
        self.period
    }

    /// Returns the canonical provider dataset identity bound to this request.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the zero-based request page.
    pub const fn page_number(&self) -> usize {
        self.page_number
    }

    /// Returns the exact query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the exact request digest including page number.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
}

fn query_digest(
    family: TreasuryDailyRateFamily,
    period: TreasuryDailyRatePeriod,
    dataset: &SourceIdentifier,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-daily-rate-query-v2");
    update_component(&mut digest, FEED_ENDPOINT);
    update_component(&mut digest, family.provider_key());
    update_component(&mut digest, family.feed_identity());
    update_component(&mut digest, family.feed_title());
    update_component(&mut digest, family.schema_revision());
    update_component(&mut digest, dataset.as_str());
    update_component(&mut digest, &period.digest_token());
    digest.finalize().into()
}

fn request_digest(query_digest: [u8; 32], page_number: usize) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-daily-rate-request-v2");
    digest.update(query_digest);
    digest.update((page_number as u128).to_be_bytes());
    digest.finalize().into()
}
