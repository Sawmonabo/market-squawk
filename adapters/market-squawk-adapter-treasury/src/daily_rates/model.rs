use market_squawk_domain::{CalendarDate, Timestamp};
use rust_decimal::Decimal;
use serde::Serialize;

use super::TreasuryDailyRateFamily;

/// A nominal or real par-curve maturity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryMaturity {
    /// One month.
    OneMonth,
    /// One and one-half months.
    OneAndOneHalfMonths,
    /// Two months.
    TwoMonths,
    /// Three months.
    ThreeMonths,
    /// Four months.
    FourMonths,
    /// Six months.
    SixMonths,
    /// One year.
    OneYear,
    /// Two years.
    TwoYears,
    /// Three years.
    ThreeYears,
    /// Five years.
    FiveYears,
    /// Seven years.
    SevenYears,
    /// Ten years.
    TenYears,
    /// Twenty years.
    TwentyYears,
    /// Thirty years.
    ThirtyYears,
}

impl TreasuryMaturity {
    /// Every nominal par-curve maturity supported by the strict provider schema.
    pub const NOMINAL_CURVE: [Self; 14] = [
        Self::OneMonth,
        Self::OneAndOneHalfMonths,
        Self::TwoMonths,
        Self::ThreeMonths,
        Self::FourMonths,
        Self::SixMonths,
        Self::OneYear,
        Self::TwoYears,
        Self::ThreeYears,
        Self::FiveYears,
        Self::SevenYears,
        Self::TenYears,
        Self::TwentyYears,
        Self::ThirtyYears,
    ];

    /// Every real par-curve maturity supported by the strict provider schema.
    pub const REAL_CURVE: [Self; 5] = [
        Self::FiveYears,
        Self::SevenYears,
        Self::TenYears,
        Self::TwentyYears,
        Self::ThirtyYears,
    ];

    /// Returns the stable maturity token used in canonical series identities.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OneMonth => "1m",
            Self::OneAndOneHalfMonths => "1.5m",
            Self::TwoMonths => "2m",
            Self::ThreeMonths => "3m",
            Self::FourMonths => "4m",
            Self::SixMonths => "6m",
            Self::OneYear => "1y",
            Self::TwoYears => "2y",
            Self::ThreeYears => "3y",
            Self::FiveYears => "5y",
            Self::SevenYears => "7y",
            Self::TenYears => "10y",
            Self::TwentyYears => "20y",
            Self::ThirtyYears => "30y",
        }
    }
}

/// A bill maturity published by Treasury's daily-rate feed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryBillMaturity {
    /// Four weeks.
    FourWeeks,
    /// Six weeks.
    SixWeeks,
    /// Eight weeks.
    EightWeeks,
    /// Thirteen weeks.
    ThirteenWeeks,
    /// Seventeen weeks.
    SeventeenWeeks,
    /// Twenty-six weeks.
    TwentySixWeeks,
    /// Fifty-two weeks.
    FiftyTwoWeeks,
}

impl TreasuryBillMaturity {
    /// Every bill maturity supported by the strict provider schema.
    pub const ALL: [Self; 7] = [
        Self::FourWeeks,
        Self::SixWeeks,
        Self::EightWeeks,
        Self::ThirteenWeeks,
        Self::SeventeenWeeks,
        Self::TwentySixWeeks,
        Self::FiftyTwoWeeks,
    ];

    /// Returns the stable maturity token used in canonical series identities.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FourWeeks => "4w",
            Self::SixWeeks => "6w",
            Self::EightWeeks => "8w",
            Self::ThirteenWeeks => "13w",
            Self::SeventeenWeeks => "17w",
            Self::TwentySixWeeks => "26w",
            Self::FiftyTwoWeeks => "52w",
        }
    }
}

/// The provider-authored bill-rate measure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryBillRateMeasure {
    /// Closing bank-discount rate.
    BankDiscount,
    /// Closing coupon-equivalent rate.
    CouponEquivalent,
    /// Treasury's `CS_*_CLOSE_AVG` bank-discount average.
    BankDiscountAverage,
    /// Treasury's `CS_*_YIELD_AVG` coupon-equivalent average.
    CouponEquivalentAverage,
}

impl TreasuryBillRateMeasure {
    /// Every bill measure supported by the strict provider schema.
    pub const ALL: [Self; 4] = [
        Self::BankDiscount,
        Self::CouponEquivalent,
        Self::BankDiscountAverage,
        Self::CouponEquivalentAverage,
    ];

    /// Returns a stable series token.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BankDiscount => "bank-discount",
            Self::CouponEquivalent => "coupon-equivalent",
            Self::BankDiscountAverage => "bank-discount-average",
            Self::CouponEquivalentAverage => "coupon-equivalent-average",
        }
    }
}

/// The exact `RATE_TYPE` values published in Treasury's long-term feed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TreasuryLongTermRateType {
    /// `BC_20year`.
    TwentyYearConstantMaturity,
    /// `Over_10_Years`.
    OverTenYearsAverage,
    /// `Real_Rate`.
    RealRate,
}

impl TreasuryLongTermRateType {
    /// Every long-term rate type supported by the strict provider schema.
    pub const ALL: [Self; 3] = [
        Self::TwentyYearConstantMaturity,
        Self::OverTenYearsAverage,
        Self::RealRate,
    ];

    /// Returns the exact provider token.
    pub const fn provider_key(self) -> &'static str {
        match self {
            Self::TwentyYearConstantMaturity => "BC_20year",
            Self::OverTenYearsAverage => "Over_10_Years",
            Self::RealRate => "Real_Rate",
        }
    }

    /// Returns the stable series token.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TwentyYearConstantMaturity => "20y-constant-maturity",
            Self::OverTenYearsAverage => "over-10y-average",
            Self::RealRate => "real-rate",
        }
    }
}

/// A typed long-term extrapolation factor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TreasuryExtrapolationFactor {
    /// Treasury published `N/A`.
    NotApplicable,
    /// Treasury published an exact decimal factor.
    Exact(Decimal),
}

/// A canonical metric from one of the five Treasury daily-rate datasets.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TreasuryDailyRateMetric {
    /// Nominal par-yield curve.
    NominalParYield(TreasuryMaturity),
    /// One bill rate measure.
    Bill {
        /// Bill maturity.
        maturity: TreasuryBillMaturity,
        /// Published measure.
        measure: TreasuryBillRateMeasure,
    },
    /// One typed long-term rate.
    LongTerm(TreasuryLongTermRateType),
    /// Real par-yield curve.
    RealParYield(TreasuryMaturity),
    /// Real long-term average.
    RealLongTermAverage,
}

impl TreasuryDailyRateMetric {
    /// Returns the first calendar year in which the reviewed provider schema exposes this metric.
    ///
    /// Treasury's documented 2025 additive change introduced the 1.5-month nominal point and all
    /// six-week bill measures. Other metrics use their family's established historical schema.
    pub const fn first_schema_year(self) -> u16 {
        match self {
            Self::NominalParYield(TreasuryMaturity::OneAndOneHalfMonths)
            | Self::Bill {
                maturity: TreasuryBillMaturity::SixWeeks,
                ..
            } => 2025,
            Self::NominalParYield(_)
            | Self::Bill { .. }
            | Self::LongTerm(_)
            | Self::RealParYield(_)
            | Self::RealLongTermAverage => 0,
        }
    }

    /// Returns a stable provider-independent series token.
    pub fn as_series_token(self) -> String {
        match self {
            Self::NominalParYield(maturity) => {
                format!("nominal-par-yield:{}", maturity.as_str())
            }
            Self::Bill { maturity, measure } => {
                format!("bill:{}:{}", maturity.as_str(), measure.as_str())
            }
            Self::LongTerm(rate_type) => format!("long-term:{}", rate_type.as_str()),
            Self::RealParYield(maturity) => {
                format!("real-par-yield:{}", maturity.as_str())
            }
            Self::RealLongTermAverage => "real-long-term-average".to_owned(),
        }
    }

    /// Returns the exact canonical macro-series identity emitted by normalization.
    pub fn canonical_series(self) -> String {
        match self {
            Self::NominalParYield(maturity) => {
                format!("treasury:daily-par-yield-curve:{}", maturity.as_str())
            }
            Self::Bill { maturity, measure } => format!(
                "treasury:daily-bill-rates:{}:{}",
                maturity.as_str(),
                measure.as_str()
            ),
            Self::LongTerm(rate_type) => {
                format!("treasury:daily-long-term-rates:{}", rate_type.as_str())
            }
            Self::RealParYield(maturity) => {
                format!("treasury:daily-real-par-yield-curve:{}", maturity.as_str())
            }
            Self::RealLongTermAverage => "treasury:daily-real-long-term-rates:average".to_owned(),
        }
    }
}

impl TreasuryDailyRateFamily {
    /// Returns the closed canonical series allowlist for one dashboard/read query.
    ///
    /// Each family is deliberately returned separately and contains at most 28 series, below the
    /// analytical latest-known reader's 32-series request ceiling.
    pub fn dashboard_metrics(self) -> Vec<TreasuryDailyRateMetric> {
        match self {
            Self::NominalParYieldCurve => TreasuryMaturity::NOMINAL_CURVE
                .map(TreasuryDailyRateMetric::NominalParYield)
                .into(),
            Self::BillRates => TreasuryBillMaturity::ALL
                .into_iter()
                .flat_map(|maturity| {
                    TreasuryBillRateMeasure::ALL
                        .map(|measure| TreasuryDailyRateMetric::Bill { maturity, measure })
                })
                .collect(),
            Self::LongTermRates => TreasuryLongTermRateType::ALL
                .map(TreasuryDailyRateMetric::LongTerm)
                .into(),
            Self::RealParYieldCurve => TreasuryMaturity::REAL_CURVE
                .map(TreasuryDailyRateMetric::RealParYield)
                .into(),
            Self::RealLongTermRates => vec![TreasuryDailyRateMetric::RealLongTermAverage],
        }
    }
}

/// One exact decimal rate point with provider metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TreasuryDailyRatePoint {
    metric: TreasuryDailyRateMetric,
    rate_percent: Option<Decimal>,
    missing_marker: Option<String>,
    maturity_date: Option<CalendarDate>,
    cusip: Option<String>,
    extrapolation_factor: Option<TreasuryExtrapolationFactor>,
}

impl TreasuryDailyRatePoint {
    pub(super) const fn new(
        metric: TreasuryDailyRateMetric,
        rate_percent: Decimal,
        maturity_date: Option<CalendarDate>,
        cusip: Option<String>,
        extrapolation_factor: Option<TreasuryExtrapolationFactor>,
    ) -> Self {
        Self {
            metric,
            rate_percent: Some(rate_percent),
            missing_marker: None,
            maturity_date,
            cusip,
            extrapolation_factor,
        }
    }

    pub(super) fn missing(
        metric: TreasuryDailyRateMetric,
        marker: &str,
        maturity_date: Option<CalendarDate>,
        cusip: Option<String>,
        extrapolation_factor: Option<TreasuryExtrapolationFactor>,
    ) -> Self {
        Self {
            metric,
            rate_percent: None,
            missing_marker: Some(marker.to_owned()),
            maturity_date,
            cusip,
            extrapolation_factor,
        }
    }

    /// Returns the typed series metric.
    pub const fn metric(&self) -> TreasuryDailyRateMetric {
        self.metric
    }

    /// Returns the exact provider percentage.
    pub const fn rate_percent(&self) -> Option<Decimal> {
        self.rate_percent
    }

    /// Returns the provider-native missing marker when no decimal was reported.
    pub fn missing_marker(&self) -> Option<&str> {
        self.missing_marker.as_deref()
    }

    /// Returns the bill maturity date when supplied.
    pub const fn maturity_date(&self) -> Option<CalendarDate> {
        self.maturity_date
    }

    /// Returns the bill CUSIP when supplied.
    pub fn cusip(&self) -> Option<&str> {
        self.cusip.as_deref()
    }

    /// Returns the long-term extrapolation factor when supplied.
    pub const fn extrapolation_factor(&self) -> Option<TreasuryExtrapolationFactor> {
        self.extrapolation_factor
    }
}

/// One exact daily Treasury source row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TreasuryDailyRateObservation {
    family: TreasuryDailyRateFamily,
    source_record_id: String,
    record_date: CalendarDate,
    points: Vec<TreasuryDailyRatePoint>,
    market_unavailability_reason: Option<String>,
    source_published_at: Timestamp,
    row_identity: [u8; 32],
    source_payload_digest: [u8; 32],
}

impl TreasuryDailyRateObservation {
    #[allow(
        clippy::too_many_arguments,
        reason = "provider row identity and provenance remain explicit"
    )]
    pub(super) fn new(
        family: TreasuryDailyRateFamily,
        source_record_id: String,
        record_date: CalendarDate,
        points: Vec<TreasuryDailyRatePoint>,
        market_unavailability_reason: Option<String>,
        source_published_at: Timestamp,
        row_identity: [u8; 32],
        source_payload_digest: [u8; 32],
    ) -> Self {
        Self {
            family,
            source_record_id,
            record_date,
            points,
            market_unavailability_reason,
            source_published_at,
            row_identity,
            source_payload_digest,
        }
    }

    /// Returns the provider dataset family.
    pub const fn family(&self) -> TreasuryDailyRateFamily {
        self.family
    }

    /// Returns the stable source-row identifier.
    ///
    /// This is Treasury's numeric identifier when supplied. For the date-unique datasets where
    /// Treasury omits redundant identifier metadata, it is the documented official record date
    /// prefixed with `date:`.
    pub fn source_record_id(&self) -> &str {
        &self.source_record_id
    }

    /// Returns the exact provider civil date.
    pub const fn record_date(&self) -> CalendarDate {
        self.record_date
    }

    /// Returns the sorted typed points.
    pub fn points(&self) -> &[TreasuryDailyRatePoint] {
        &self.points
    }

    /// Iterates metric/point pairs without allocation.
    pub fn metric_points(
        &self,
    ) -> impl Iterator<Item = (TreasuryDailyRateMetric, &TreasuryDailyRatePoint)> {
        self.points.iter().map(|point| (point.metric(), point))
    }

    /// Returns one typed point.
    pub fn point(&self, metric: TreasuryDailyRateMetric) -> Option<&TreasuryDailyRatePoint> {
        self.points
            .binary_search_by_key(&metric, TreasuryDailyRatePoint::metric)
            .ok()
            .map(|index| &self.points[index])
    }

    /// Returns Treasury's market-unavailability reason when present.
    pub fn market_unavailability_reason(&self) -> Option<&str> {
        self.market_unavailability_reason.as_deref()
    }

    /// Returns the exact Atom entry update instant.
    pub const fn source_published_at(&self) -> Timestamp {
        self.source_published_at
    }

    /// Returns the canonical provider-row identity.
    pub const fn row_identity(&self) -> [u8; 32] {
        self.row_identity
    }

    /// Returns the exact containing-payload SHA-256 identity.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
}
