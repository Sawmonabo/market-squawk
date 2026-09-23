//! Exact setup and credential-probe contract for the admitted monthly retail-price family.

use market_squawk_adapter_eia::{
    EiaDataFieldContract, EiaDataFieldContractInput, EiaDataQuery, EiaDataQueryInput,
    EiaDatasetProfile, EiaError, EiaFacetFilter, EiaFacetValue, EiaFieldId, EiaMissingPolicy,
    EiaRoute, EiaSort, EiaSortDirection, EiaUnitSource, EiaValueKind,
};

pub(crate) fn electricity_price_query(
    start_period: String,
    end_period: String,
) -> Result<EiaDataQuery, EiaError> {
    // The bounded setup recipe admits the same 24-month envelope as the retained journey.
    let first = month_index(&start_period)?;
    let last = month_index(&end_period)?;
    if first > last || last - first >= 24 {
        return Err(EiaError::InvalidLimit);
    }
    EiaDataQuery::try_new(EiaDataQueryInput {
        route: EiaRoute::try_from("electricity/retail-sales")?,
        data_fields: vec![field("price")?],
        facets: vec![
            EiaFacetFilter::try_new(field("sectorid")?, vec![EiaFacetValue::try_from("RES")?])?,
            EiaFacetFilter::try_new(field("stateid")?, vec![EiaFacetValue::try_from("US")?])?,
        ],
        frequency: field("monthly")?,
        start: Some(start_period),
        end: Some(end_period),
        sorts: ["period", "stateid", "sectorid"]
        .into_iter()
        .map(|name| Ok(EiaSort::new(field(name)?, EiaSortDirection::Ascending)))
        .collect::<Result<Vec<_>, EiaError>>()?,
        length: 24,
    })
}

pub(crate) fn electricity_price_profile(
    query: EiaDataQuery,
) -> Result<EiaDatasetProfile, EiaError> {
    EiaDatasetProfile::try_for_macro(
        query,
        electricity_price_fields()?,
        electricity_price_descriptors()?,
        Vec::new(),
    )
}

pub(crate) fn electricity_price_fields() -> Result<Vec<EiaDataFieldContract>, EiaError> {
    Ok(vec![EiaDataFieldContract::new(EiaDataFieldContractInput {
        field: field("price")?,
        value_kind: EiaValueKind::Decimal,
        unit_source: EiaUnitSource::RowField,
        missing_policy: EiaMissingPolicy::try_new(["NA".to_owned(), "--".to_owned()], true)?,
    })])
}

pub(crate) fn electricity_price_descriptors() -> Result<Vec<EiaFieldId>, EiaError> {
    Ok(vec![field("stateDescription")?, field("sectorName")?])
}

fn field(value: &str) -> Result<EiaFieldId, EiaError> {
    EiaFieldId::try_from(value)
}

fn month_index(value: &str) -> Result<u32, EiaError> {
    let bytes = value.as_bytes();
    if bytes.len() != 7
        || bytes[4] != b'-'
        || !bytes[..4].iter().chain(&bytes[5..]).all(u8::is_ascii_digit)
    {
        return Err(EiaError::InvalidLimit);
    }
    let year: u32 = value[..4].parse().map_err(|_| EiaError::InvalidLimit)?;
    let month: u32 = value[5..].parse().map_err(|_| EiaError::InvalidLimit)?;
    if year == 0 || !(1..=12).contains(&month) {
        return Err(EiaError::InvalidLimit);
    }
    Ok(year * 12 + month - 1)
}
