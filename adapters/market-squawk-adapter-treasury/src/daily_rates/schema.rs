use std::collections::{BTreeMap, BTreeSet};

use chrono::{Datelike, NaiveDate};
use market_squawk_domain::CalendarDate;
use rust_decimal::Decimal;

use crate::TreasuryProtocolError;
use crate::rates::parse_date;

use super::{
    TreasuryBillMaturity, TreasuryBillRateMeasure, TreasuryDailyRateFamily,
    TreasuryDailyRateMetric, TreasuryDailyRatePoint, TreasuryExtrapolationFactor,
    TreasuryLongTermRateType, TreasuryMaturity,
};

const NOMINAL_RATE_FIELDS: [(&str, TreasuryMaturity); 14] = [
    ("BC_1MONTH", TreasuryMaturity::OneMonth),
    ("BC_1_5MONTH", TreasuryMaturity::OneAndOneHalfMonths),
    ("BC_2MONTH", TreasuryMaturity::TwoMonths),
    ("BC_3MONTH", TreasuryMaturity::ThreeMonths),
    ("BC_4MONTH", TreasuryMaturity::FourMonths),
    ("BC_6MONTH", TreasuryMaturity::SixMonths),
    ("BC_1YEAR", TreasuryMaturity::OneYear),
    ("BC_2YEAR", TreasuryMaturity::TwoYears),
    ("BC_3YEAR", TreasuryMaturity::ThreeYears),
    ("BC_5YEAR", TreasuryMaturity::FiveYears),
    ("BC_7YEAR", TreasuryMaturity::SevenYears),
    ("BC_10YEAR", TreasuryMaturity::TenYears),
    ("BC_20YEAR", TreasuryMaturity::TwentyYears),
    ("BC_30YEAR", TreasuryMaturity::ThirtyYears),
];

const REAL_RATE_FIELDS: [(&str, TreasuryMaturity); 5] = [
    ("TC_5YEAR", TreasuryMaturity::FiveYears),
    ("TC_7YEAR", TreasuryMaturity::SevenYears),
    ("TC_10YEAR", TreasuryMaturity::TenYears),
    ("TC_20YEAR", TreasuryMaturity::TwentyYears),
    ("TC_30YEAR", TreasuryMaturity::ThirtyYears),
];

#[derive(Clone, Copy)]
struct BillFieldSpec {
    maturity: TreasuryBillMaturity,
    discount: &'static str,
    equivalent: &'static str,
    discount_average: &'static str,
    equivalent_average: &'static str,
    maturity_date: &'static str,
    cusip: &'static str,
}

const BILL_FIELDS: [BillFieldSpec; 7] = [
    BillFieldSpec {
        maturity: TreasuryBillMaturity::FourWeeks,
        discount: "ROUND_B1_CLOSE_4WK_2",
        equivalent: "ROUND_B1_YIELD_4WK_2",
        discount_average: "CS_4WK_CLOSE_AVG",
        equivalent_average: "CS_4WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_4WK",
        cusip: "CUSIP_4WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::SixWeeks,
        discount: "ROUND_B1_CLOSE_6WK_2",
        equivalent: "ROUND_B1_YIELD_6WK_2",
        discount_average: "CS_6WK_CLOSE_AVG",
        equivalent_average: "CS_6WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_6WK",
        cusip: "CUSIP_6WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::EightWeeks,
        discount: "ROUND_B1_CLOSE_8WK_2",
        equivalent: "ROUND_B1_YIELD_8WK_2",
        discount_average: "CS_8WK_CLOSE_AVG",
        equivalent_average: "CS_8WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_8WK",
        cusip: "CUSIP_8WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::ThirteenWeeks,
        discount: "ROUND_B1_CLOSE_13WK_2",
        equivalent: "ROUND_B1_YIELD_13WK_2",
        discount_average: "CS_13WK_CLOSE_AVG",
        equivalent_average: "CS_13WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_13WK",
        cusip: "CUSIP_13WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::SeventeenWeeks,
        discount: "ROUND_B1_CLOSE_17WK_2",
        equivalent: "ROUND_B1_YIELD_17WK_2",
        discount_average: "CS_17WK_CLOSE_AVG",
        equivalent_average: "CS_17WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_17WK",
        cusip: "CUSIP_17WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::TwentySixWeeks,
        discount: "ROUND_B1_CLOSE_26WK_2",
        equivalent: "ROUND_B1_YIELD_26WK_2",
        discount_average: "CS_26WK_CLOSE_AVG",
        equivalent_average: "CS_26WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_26WK",
        cusip: "CUSIP_26WK",
    },
    BillFieldSpec {
        maturity: TreasuryBillMaturity::FiftyTwoWeeks,
        discount: "ROUND_B1_CLOSE_52WK_2",
        equivalent: "ROUND_B1_YIELD_52WK_2",
        discount_average: "CS_52WK_CLOSE_AVG",
        equivalent_average: "CS_52WK_YIELD_AVG",
        maturity_date: "MATURITY_DATE_52WK",
        cusip: "CUSIP_52WK",
    },
];

#[derive(Clone, Debug)]
pub(super) struct PropertyValue {
    pub(super) text: Option<String>,
    pub(super) data_type: Option<String>,
    pub(super) is_null: bool,
}

pub(super) struct DecodedRow {
    pub(super) points: Vec<TreasuryDailyRatePoint>,
    pub(super) market_unavailability_reason: Option<String>,
}

pub(super) fn decode_row(
    family: TreasuryDailyRateFamily,
    properties: &BTreeMap<String, PropertyValue>,
    record_date: CalendarDate,
) -> Result<DecodedRow, TreasuryProtocolError> {
    ensure_allowed_fields(family, properties)?;
    let (mut points, market_unavailability_reason) = match family {
        TreasuryDailyRateFamily::NominalParYieldCurve => {
            (nominal_points(properties, record_date)?, None)
        }
        TreasuryDailyRateFamily::BillRates => bill_points(properties, record_date)?,
        TreasuryDailyRateFamily::LongTermRates => (long_term_points(properties)?, None),
        TreasuryDailyRateFamily::RealParYieldCurve => {
            (real_curve_points(properties, record_date)?, None)
        }
        TreasuryDailyRateFamily::RealLongTermRates => (real_long_term_points(properties)?, None),
    };
    points.sort_by_key(TreasuryDailyRatePoint::metric);
    if points.is_empty()
        || points
            .windows(2)
            .any(|pair| pair[0].metric() == pair[1].metric())
    {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(DecodedRow {
        points,
        market_unavailability_reason,
    })
}

fn nominal_points(
    properties: &BTreeMap<String, PropertyValue>,
    record_date: CalendarDate,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    let mut points = Vec::new();
    for (field, maturity) in NOMINAL_RATE_FIELDS {
        let metric = TreasuryDailyRateMetric::NominalParYield(maturity);
        if record_date.year() >= metric.first_schema_year() {
            points.push(rate_point(
                metric,
                decimal_field(properties, field)?,
                None,
                None,
                None,
            ));
        }
    }
    let display = optional_decimal(properties, "BC_30YEARDISPLAY")?;
    let thirty_year = points
        .iter()
        .find(|point| {
            point.metric()
                == TreasuryDailyRateMetric::NominalParYield(TreasuryMaturity::ThirtyYears)
        })
        .and_then(TreasuryDailyRatePoint::rate_percent);
    if display.is_some() && display != thirty_year {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(points)
}

fn real_curve_points(
    properties: &BTreeMap<String, PropertyValue>,
    record_date: CalendarDate,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    let mut points = Vec::new();
    for (field, maturity) in REAL_RATE_FIELDS {
        let metric = TreasuryDailyRateMetric::RealParYield(maturity);
        if record_date.year() >= metric.first_schema_year() {
            points.push(rate_point(
                metric,
                decimal_field(properties, field)?,
                None,
                None,
                None,
            ));
        }
    }
    Ok(points)
}

fn bill_points(
    properties: &BTreeMap<String, PropertyValue>,
    record_date: CalendarDate,
) -> Result<(Vec<TreasuryDailyRatePoint>, Option<String>), TreasuryProtocolError> {
    let civil_date = NaiveDate::from_ymd_opt(
        i32::from(record_date.year()),
        u32::from(record_date.month()),
        u32::from(record_date.day()),
    )
    .ok_or(TreasuryProtocolError::SchemaDrift)?;
    let iso_week = civil_date.iso_week();
    if required_provider_date(properties, "QUOTE_DATE")? != record_date
        || required_untyped(properties, "CF_NEW_DATE")?
            != format!(
                "{:02}/{:02}/{:04}",
                record_date.month(),
                record_date.day(),
                record_date.year()
            )
        || required_typed(properties, "CF_WEEK", "Edm.Int32")?
            != format!("{:04}{:02}", iso_week.year(), iso_week.week())
    {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    let reason = optional_untyped(properties, "BOND_MKT_UNAVAIL_REASON")?
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let mut points = Vec::new();
    for spec in BILL_FIELDS {
        let maturity_date = optional_provider_date(properties, spec.maturity_date)?;
        let cusip = optional_untyped(properties, spec.cusip)?
            .map(validate_cusip)
            .transpose()?;
        let rates = [
            (spec.discount, TreasuryBillRateMeasure::BankDiscount),
            (spec.equivalent, TreasuryBillRateMeasure::CouponEquivalent),
            (
                spec.discount_average,
                TreasuryBillRateMeasure::BankDiscountAverage,
            ),
            (
                spec.equivalent_average,
                TreasuryBillRateMeasure::CouponEquivalentAverage,
            ),
        ];
        let parsed = rates
            .map(|(field, measure)| decimal_field(properties, field).map(|rate| (measure, rate)))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        if parsed
            .iter()
            .any(|(_, rate)| matches!(rate, DecimalField::Observed(_)))
            && (maturity_date.is_none() || cusip.is_none())
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        for (measure, rate) in parsed {
            let metric = TreasuryDailyRateMetric::Bill {
                maturity: spec.maturity,
                measure,
            };
            if record_date.year() >= metric.first_schema_year() {
                points.push(rate_point(metric, rate, maturity_date, cusip.clone(), None));
            }
        }
    }
    Ok((points, reason))
}

fn long_term_points(
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    let rate_type = match required_untyped(properties, "RATE_TYPE")? {
        "BC_20year" => TreasuryLongTermRateType::TwentyYearConstantMaturity,
        "Over_10_Years" => TreasuryLongTermRateType::OverTenYearsAverage,
        "Real_Rate" => TreasuryLongTermRateType::RealRate,
        _ => return Err(TreasuryProtocolError::SchemaDrift),
    };
    let factor = match required_untyped(properties, "EXTRAPOLATION_FACTOR")? {
        "N/A" => TreasuryExtrapolationFactor::NotApplicable,
        value => TreasuryExtrapolationFactor::Exact(parse_decimal(value)?),
    };
    Ok(vec![rate_point(
        TreasuryDailyRateMetric::LongTerm(rate_type),
        decimal_field(properties, "RATE")?,
        None,
        None,
        Some(factor),
    )])
}

fn real_long_term_points(
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    Ok(vec![rate_point(
        TreasuryDailyRateMetric::RealLongTermAverage,
        decimal_field(properties, "RATE")?,
        None,
        None,
        None,
    )])
}

#[derive(Clone, Copy)]
enum DecimalField {
    Observed(Decimal),
    Missing(&'static str),
}

fn decimal_field(
    properties: &BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<DecimalField, TreasuryProtocolError> {
    match properties.get(name) {
        None => Ok(DecimalField::Missing("absent")),
        Some(property) if property.is_null => {
            if property.text.is_some()
                || property
                    .data_type
                    .as_deref()
                    .is_some_and(|value| value != "Edm.Double")
            {
                return Err(TreasuryProtocolError::SchemaDrift);
            }
            Ok(DecimalField::Missing("m:null=true"))
        }
        Some(_) => optional_decimal(properties, name)?
            .map(DecimalField::Observed)
            .ok_or(TreasuryProtocolError::SchemaDrift),
    }
}

fn rate_point(
    metric: TreasuryDailyRateMetric,
    value: DecimalField,
    maturity_date: Option<CalendarDate>,
    cusip: Option<String>,
    extrapolation_factor: Option<TreasuryExtrapolationFactor>,
) -> TreasuryDailyRatePoint {
    match value {
        DecimalField::Observed(value) => {
            TreasuryDailyRatePoint::new(metric, value, maturity_date, cusip, extrapolation_factor)
        }
        DecimalField::Missing(marker) => TreasuryDailyRatePoint::missing(
            metric,
            marker,
            maturity_date,
            cusip,
            extrapolation_factor,
        ),
    }
}

fn ensure_allowed_fields(
    family: TreasuryDailyRateFamily,
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<(), TreasuryProtocolError> {
    let mut allowed = BTreeSet::new();
    match family {
        TreasuryDailyRateFamily::NominalParYieldCurve => {
            allowed.extend(["Id", "NEW_DATE", "BC_30YEARDISPLAY"]);
            allowed.extend(NOMINAL_RATE_FIELDS.map(|(field, _)| field));
        }
        TreasuryDailyRateFamily::BillRates => {
            allowed.extend([
                "DailyTreasuryBillRateDataId",
                "INDEX_DATE",
                "BOND_MKT_UNAVAIL_REASON",
                "QUOTE_DATE",
                "CF_NEW_DATE",
                "CF_WEEK",
            ]);
            for spec in BILL_FIELDS {
                allowed.extend([
                    spec.discount,
                    spec.equivalent,
                    spec.discount_average,
                    spec.equivalent_average,
                    spec.maturity_date,
                    spec.cusip,
                ]);
            }
        }
        TreasuryDailyRateFamily::LongTermRates => {
            allowed.extend([
                "Id",
                "QUOTE_DATE",
                "EXTRAPOLATION_FACTOR",
                "RATE_TYPE",
                "RATE",
            ]);
        }
        TreasuryDailyRateFamily::RealParYieldCurve => {
            allowed.extend(["DailyTreasuryRealYieldCurveRateDataId", "NEW_DATE"]);
            allowed.extend(REAL_RATE_FIELDS.map(|(field, _)| field));
        }
        TreasuryDailyRateFamily::RealLongTermRates => {
            allowed.extend(["QUOTE_DATE", "RATE"]);
        }
    }
    if properties
        .keys()
        .any(|field| !allowed.contains(field.as_str()))
    {
        Err(TreasuryProtocolError::SchemaDrift)
    } else {
        Ok(())
    }
}

pub(super) const fn id_field(family: TreasuryDailyRateFamily) -> Option<&'static str> {
    match family {
        TreasuryDailyRateFamily::NominalParYieldCurve | TreasuryDailyRateFamily::LongTermRates => {
            Some("Id")
        }
        TreasuryDailyRateFamily::BillRates => Some("DailyTreasuryBillRateDataId"),
        TreasuryDailyRateFamily::RealParYieldCurve => Some("DailyTreasuryRealYieldCurveRateDataId"),
        TreasuryDailyRateFamily::RealLongTermRates => None,
    }
}

pub(super) const fn date_field(family: TreasuryDailyRateFamily) -> &'static str {
    match family {
        TreasuryDailyRateFamily::NominalParYieldCurve
        | TreasuryDailyRateFamily::RealParYieldCurve => "NEW_DATE",
        TreasuryDailyRateFamily::BillRates => "INDEX_DATE",
        TreasuryDailyRateFamily::LongTermRates | TreasuryDailyRateFamily::RealLongTermRates => {
            "QUOTE_DATE"
        }
    }
}

fn optional_decimal(
    properties: &BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<Option<Decimal>, TreasuryProtocolError> {
    optional_typed(properties, name, "Edm.Double")?
        .map(parse_decimal)
        .transpose()
}

fn parse_decimal(value: &str) -> Result<Decimal, TreasuryProtocolError> {
    if value.is_empty() || value.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Decimal::from_str_exact(value).map_err(|_| TreasuryProtocolError::SchemaDrift)
}

pub(super) fn required_provider_date(
    properties: &BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<CalendarDate, TreasuryProtocolError> {
    parse_provider_date(required_typed(properties, name, "Edm.DateTime")?)
}

fn optional_provider_date(
    properties: &BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<Option<CalendarDate>, TreasuryProtocolError> {
    optional_typed(properties, name, "Edm.DateTime")?
        .map(parse_provider_date)
        .transpose()
}

fn parse_provider_date(value: &str) -> Result<CalendarDate, TreasuryProtocolError> {
    let date = value
        .strip_suffix("T00:00:00")
        .ok_or(TreasuryProtocolError::SchemaDrift)?;
    parse_date(date).map_err(|_| TreasuryProtocolError::SchemaDrift)
}

pub(super) fn required_typed<'a>(
    properties: &'a BTreeMap<String, PropertyValue>,
    name: &str,
    data_type: &str,
) -> Result<&'a str, TreasuryProtocolError> {
    optional_typed(properties, name, data_type)?.ok_or(TreasuryProtocolError::SchemaDrift)
}

pub(super) fn optional_typed<'a>(
    properties: &'a BTreeMap<String, PropertyValue>,
    name: &str,
    data_type: &str,
) -> Result<Option<&'a str>, TreasuryProtocolError> {
    let Some(property) = properties.get(name) else {
        return Ok(None);
    };
    if property.data_type.as_deref() != Some(data_type)
        && !(property.is_null && property.data_type.is_none())
    {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(property.text.as_deref())
}

fn required_untyped<'a>(
    properties: &'a BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<&'a str, TreasuryProtocolError> {
    optional_untyped(properties, name)?.ok_or(TreasuryProtocolError::SchemaDrift)
}

fn optional_untyped<'a>(
    properties: &'a BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<Option<&'a str>, TreasuryProtocolError> {
    let Some(property) = properties.get(name) else {
        return Ok(None);
    };
    if property.data_type.is_some() {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(property.text.as_deref())
}

fn validate_cusip(value: &str) -> Result<String, TreasuryProtocolError> {
    if value.len() != 9
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(value.to_owned())
}
