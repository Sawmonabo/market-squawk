use std::collections::{BTreeMap, BTreeSet};

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
        TreasuryDailyRateFamily::NominalParYieldCurve => (nominal_points(properties)?, None),
        TreasuryDailyRateFamily::BillRates => bill_points(properties, record_date)?,
        TreasuryDailyRateFamily::LongTermRates => (long_term_points(properties)?, None),
        TreasuryDailyRateFamily::RealParYieldCurve => (real_curve_points(properties)?, None),
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
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    let mut points = Vec::new();
    for (field, maturity) in NOMINAL_RATE_FIELDS {
        if let Some(rate) = optional_decimal(properties, field)? {
            points.push(TreasuryDailyRatePoint::new(
                TreasuryDailyRateMetric::NominalParYield(maturity),
                rate,
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
        .map(TreasuryDailyRatePoint::rate_percent);
    if display.is_some() && display != thirty_year {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    Ok(points)
}

fn real_curve_points(
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    let mut points = Vec::new();
    for (field, maturity) in REAL_RATE_FIELDS {
        if let Some(rate) = optional_decimal(properties, field)? {
            points.push(TreasuryDailyRatePoint::new(
                TreasuryDailyRateMetric::RealParYield(maturity),
                rate,
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
    if required_provider_date(properties, "QUOTE_DATE")? != record_date
        || required_untyped(properties, "CF_NEW_DATE")?
            != format!(
                "{:02}/{:02}/{:04}",
                record_date.month(),
                record_date.day(),
                record_date.year()
            )
        || required_typed(properties, "CF_WEEK", "Edm.Int32")?
            != format!("{:04}{:02}", record_date.year(), record_date.month())
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
            .map(|(field, measure)| optional_decimal(properties, field).map(|rate| (measure, rate)))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        if parsed.iter().any(|(_, rate)| rate.is_some())
            && (maturity_date.is_none() || cusip.is_none())
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        for (measure, rate) in parsed {
            if let Some(rate) = rate {
                points.push(TreasuryDailyRatePoint::new(
                    TreasuryDailyRateMetric::Bill {
                        maturity: spec.maturity,
                        measure,
                    },
                    rate,
                    maturity_date,
                    cusip.clone(),
                    None,
                ));
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
    Ok(vec![TreasuryDailyRatePoint::new(
        TreasuryDailyRateMetric::LongTerm(rate_type),
        required_decimal(properties, "RATE")?,
        None,
        None,
        Some(factor),
    )])
}

fn real_long_term_points(
    properties: &BTreeMap<String, PropertyValue>,
) -> Result<Vec<TreasuryDailyRatePoint>, TreasuryProtocolError> {
    Ok(vec![TreasuryDailyRatePoint::new(
        TreasuryDailyRateMetric::RealLongTermAverage,
        required_decimal(properties, "RATE")?,
        None,
        None,
        None,
    )])
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

fn required_decimal(
    properties: &BTreeMap<String, PropertyValue>,
    name: &str,
) -> Result<Decimal, TreasuryProtocolError> {
    optional_decimal(properties, name)?.ok_or(TreasuryProtocolError::SchemaDrift)
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

fn optional_typed<'a>(
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
