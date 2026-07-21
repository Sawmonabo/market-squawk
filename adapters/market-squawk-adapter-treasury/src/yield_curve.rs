use std::collections::{BTreeMap, BTreeSet};

use chrono::DateTime;
use market_squawk_domain::{CalendarDate, DataQuality, Timestamp};
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::TreasuryProtocolError;
use crate::fiscal_data::FiscalDataParseLimits;
use crate::query::update_component;
use crate::rates::parse_date;

const FEED_ENDPOINT: &str =
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml";
const FEED_SOURCE_IDENTITY: &str = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve";
const METHODOLOGY_URL: &str = "https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics/treasury-yield-curve-methodology";
const METHODOLOGY_REVISION: &str = "monotone-convex-2021-12-06/reviewed-2025-02-18";
const PROVIDER_YEAR_RESPONSE_ID: &str = "provider-year-filter-single-response-v1";
const ATOM_NAMESPACE: &[u8] = b"http://www.w3.org/2005/Atom";
const DATA_NAMESPACE: &[u8] = b"http://schemas.microsoft.com/ado/2007/08/dataservices";
const METADATA_NAMESPACE: &[u8] = b"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";

const RATE_FIELDS: [(&str, TreasuryMaturity); 14] = [
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

/// One published constant-maturity point on the daily par yield curve.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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
    /// Returns the stable maturity token used in canonical Treasury series identities.
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

/// Official daily par-yield-curve profile with explicit indicative methodology evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TreasuryYieldCurveProfile;

impl TreasuryYieldCurveProfile {
    /// Returns the supported daily nominal par-yield-curve profile.
    pub const fn daily_par_yield_curve() -> Self {
        Self
    }

    /// Returns the non-executable data-quality ceiling required by the methodology.
    pub const fn quality(self) -> DataQuality {
        DataQuality::Indicative
    }

    /// Returns the exact official feed identity required in each response.
    pub const fn source_identity(self) -> &'static str {
        FEED_SOURCE_IDENTITY
    }

    /// Returns Treasury's official methodology page.
    pub const fn methodology_url(self) -> &'static str {
        METHODOLOGY_URL
    }

    /// Returns the adapter-bound methodology revision.
    pub const fn methodology_revision(self) -> &'static str {
        METHODOLOGY_REVISION
    }

    /// Binds the provider year filter into one exact non-paginated request.
    ///
    /// # Errors
    ///
    /// Rejects years before the official nominal curve begins, nonzero page numbers, or an invalid
    /// URL. Treasury documents pagination only for the separate all-years query.
    pub fn page(
        self,
        year: u16,
        page_number: usize,
    ) -> Result<TreasuryYieldCurvePageRequest, TreasuryProtocolError> {
        if !(1990..=9999).contains(&year) || page_number != 0 {
            return Err(TreasuryProtocolError::InvalidQuery);
        }
        let mut url = Url::parse(FEED_ENDPOINT).map_err(|_| TreasuryProtocolError::InvalidQuery)?;
        url.query_pairs_mut()
            .append_pair("data", "daily_treasury_yield_curve")
            .append_pair("field_tdr_date_value", &year.to_string());
        let query_digest = yield_query_digest(year);
        let mut request_digest = Sha256::new();
        update_component(&mut request_digest, "treasury-yield-curve-page-v1");
        request_digest.update(query_digest);
        request_digest.update((page_number as u128).to_be_bytes());
        Ok(TreasuryYieldCurvePageRequest {
            url: url.into(),
            year,
            page_number,
            query_digest,
            request_digest: request_digest.finalize().into(),
        })
    }
}

/// Exact official yield-curve page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryYieldCurvePageRequest {
    url: String,
    year: u16,
    page_number: usize,
    query_digest: [u8; 32],
    request_digest: [u8; 32],
}

impl TreasuryYieldCurvePageRequest {
    /// Returns the allowlist-ready official HTTPS URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the exact year filter.
    pub const fn year(&self) -> u16 {
        self.year
    }

    /// Returns zero, the internal identity sentinel for one complete year response.
    pub const fn page_number(&self) -> usize {
        self.page_number
    }

    /// Returns the query-family digest, including provider-defined page-size semantics.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the full request digest including page number.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }
}

/// One exact daily Treasury par-yield observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct DailyParYieldCurveObservation {
    source_record_id: String,
    record_date: CalendarDate,
    rates_percent: BTreeMap<TreasuryMaturity, Decimal>,
    source_published_at: Timestamp,
    row_identity: [u8; 32],
    source_payload_digest: [u8; 32],
}

impl DailyParYieldCurveObservation {
    /// Returns Treasury's stable row identifier.
    pub fn source_record_id(&self) -> &str {
        &self.source_record_id
    }

    /// Returns the exact provider civil date without invented time-zone precision.
    pub const fn record_date(&self) -> CalendarDate {
        self.record_date
    }

    /// Returns one exact published percentage when the maturity existed on this date.
    pub fn rate_percent(&self, maturity: TreasuryMaturity) -> Option<Decimal> {
        self.rates_percent.get(&maturity).copied()
    }

    pub(crate) fn rates_percent(&self) -> impl Iterator<Item = (TreasuryMaturity, Decimal)> + '_ {
        self.rates_percent
            .iter()
            .map(|(maturity, value)| (*maturity, *value))
    }

    /// Returns the one-month rate when published.
    pub fn one_month_percent(&self) -> Option<Decimal> {
        self.rate_percent(TreasuryMaturity::OneMonth)
    }

    /// Returns the thirty-year rate when published.
    pub fn thirty_year_percent(&self) -> Option<Decimal> {
        self.rate_percent(TreasuryMaturity::ThirtyYears)
    }

    /// Returns the RFC 3339 Atom instant at which the provider last updated this entry.
    pub const fn source_published_at(&self) -> Timestamp {
        self.source_published_at
    }

    /// Returns the canonical provider row identity.
    pub const fn row_identity(&self) -> [u8; 32] {
        self.row_identity
    }

    /// Returns the SHA-256 identity of the exact response containing the row.
    pub const fn source_payload_digest(&self) -> [u8; 32] {
        self.source_payload_digest
    }
}

/// One bounded, exact-payload-bound official yield-curve response page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DailyParYieldCurvePage {
    request_digest: [u8; 32],
    query_digest: [u8; 32],
    response_payload_digest: [u8; 32],
    feed_published_at: Timestamp,
    observations: Vec<DailyParYieldCurveObservation>,
}

impl DailyParYieldCurvePage {
    /// Parses a bounded official Atom response without treating its civil dates as instants.
    ///
    /// # Errors
    ///
    /// Rejects malformed XML, source identity drift, schema drift, duplicate rows, bounds
    /// violations, or records outside the exact requested year.
    pub fn parse(
        bytes: &[u8],
        request: &TreasuryYieldCurvePageRequest,
        limits: FiscalDataParseLimits,
    ) -> Result<Self, TreasuryProtocolError> {
        if bytes.len() > limits.max_bytes() {
            return Err(TreasuryProtocolError::BodyTooLarge);
        }
        let source_payload_digest = Sha256::digest(bytes).into();
        let mut reader = NsReader::from_reader(bytes);
        reader.config_mut().trim_text(true);
        let mut xml_version = XmlVersion::Implicit1_0;
        let mut target = TextTarget::None;
        let mut feed_title = None;
        let mut feed_id = None;
        let mut feed_published_at = None;
        let mut in_entry = false;
        let mut in_properties = false;
        let mut entry = EntryBuilder::default();
        let mut observations = Vec::new();

        loop {
            match reader.read_resolved_event() {
                Ok((namespace, Event::Start(start))) => {
                    let name = local_name(&start)?;
                    match name.as_str() {
                        "entry" => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            if in_entry {
                                return Err(TreasuryProtocolError::SchemaDrift);
                            }
                            in_entry = true;
                            entry = EntryBuilder::default();
                        }
                        "properties" if in_entry => {
                            require_namespace(&namespace, METADATA_NAMESPACE)?;
                            in_properties = true;
                        }
                        _ if in_properties => {
                            require_namespace(&namespace, DATA_NAMESPACE)?;
                            target = TextTarget::Property(property_target(
                                &reader,
                                xml_version,
                                &start,
                                name,
                            )?);
                        }
                        "title" if !in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            target = TextTarget::FeedTitle;
                        }
                        "id" if in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            target = TextTarget::EntryId;
                        }
                        "id" => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            target = TextTarget::FeedId;
                        }
                        "updated" if in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            target = TextTarget::EntryUpdated;
                        }
                        "updated" => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            target = TextTarget::FeedUpdated;
                        }
                        _ => {}
                    }
                }
                Ok((namespace, Event::Empty(start))) if in_properties => {
                    require_namespace(&namespace, DATA_NAMESPACE)?;
                    let name = local_name(&start)?;
                    let property = property_target(&reader, xml_version, &start, name)?;
                    if !property.is_null {
                        return Err(TreasuryProtocolError::SchemaDrift);
                    }
                    entry.insert(property, None, limits.max_fields())?;
                }
                Ok((_, Event::Text(text))) => {
                    let decoded = text
                        .decode()
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    let value = quick_xml::escape::unescape(&decoded)
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    if value.len() > 64 * 1024 {
                        return Err(TreasuryProtocolError::FieldTooLarge);
                    }
                    append_target(
                        &target,
                        value.as_ref(),
                        &mut feed_title,
                        &mut feed_id,
                        &mut feed_published_at,
                        &mut entry,
                        limits.max_fields(),
                    )?;
                }
                Ok((_, Event::GeneralRef(reference))) => {
                    let reference = reference
                        .decode()
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    let encoded = format!("&{reference};");
                    let value = quick_xml::escape::unescape(&encoded)
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    append_target(
                        &target,
                        value.as_ref(),
                        &mut feed_title,
                        &mut feed_id,
                        &mut feed_published_at,
                        &mut entry,
                        limits.max_fields(),
                    )?;
                }
                Ok((namespace, Event::End(end))) => {
                    let local_name = end.local_name();
                    let name = std::str::from_utf8(local_name.as_ref())
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    match name {
                        "entry" => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            let observation = entry.finish(request, source_payload_digest)?;
                            if observations.len() == limits.max_records() {
                                return Err(TreasuryProtocolError::PaginationLimitExceeded);
                            }
                            observations.push(observation);
                            in_entry = false;
                            in_properties = false;
                            target = TextTarget::None;
                            entry = EntryBuilder::default();
                        }
                        "properties" => {
                            require_namespace(&namespace, METADATA_NAMESPACE)?;
                            in_properties = false;
                            target = TextTarget::None;
                        }
                        _ => target = TextTarget::None,
                    }
                }
                Ok((_, Event::Decl(declaration))) => {
                    xml_version = declaration
                        .xml_version()
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                }
                Ok((_, Event::DocType(_))) => return Err(TreasuryProtocolError::SchemaDrift),
                Ok((_, Event::Eof)) => break,
                Ok(_) => {}
                Err(error) => return Err(TreasuryProtocolError::InvalidXml(error.to_string())),
            }
        }

        if in_entry
            || feed_title.as_deref() != Some("DailyTreasuryYieldCurveRateData")
            || feed_id.as_deref() != Some(FEED_SOURCE_IDENTITY)
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let feed_published_at =
            parse_atom_timestamp(&feed_published_at.ok_or(TreasuryProtocolError::SchemaDrift)?)?;
        if observations
            .iter()
            .any(|observation| observation.source_published_at > feed_published_at)
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let mut identities = BTreeSet::new();
        if observations
            .iter()
            .any(|observation| !identities.insert(observation.row_identity))
        {
            return Err(TreasuryProtocolError::DuplicateRecordIdentity);
        }
        Ok(Self {
            request_digest: request.request_digest,
            query_digest: request.query_digest,
            response_payload_digest: source_payload_digest,
            feed_published_at,
            observations,
        })
    }

    /// Returns whether this page contains no entries and therefore terminates provider paging.
    pub fn is_terminal(&self) -> bool {
        self.observations.is_empty()
    }

    /// Returns exact normalized observations.
    pub fn observations(&self) -> &[DailyParYieldCurveObservation] {
        &self.observations
    }

    /// Returns the RFC 3339 Atom instant at which the provider last updated this feed.
    pub const fn feed_published_at(&self) -> Timestamp {
        self.feed_published_at
    }

    /// Returns the canonical query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the canonical request digest including page number.
    pub const fn request_digest(&self) -> [u8; 32] {
        self.request_digest
    }

    /// Returns the exact response SHA-256 identity.
    pub const fn response_payload_digest(&self) -> [u8; 32] {
        self.response_payload_digest
    }
}

#[derive(Clone, Debug)]
struct PropertyTarget {
    name: String,
    data_type: Option<String>,
    is_null: bool,
}

#[derive(Clone, Debug)]
enum TextTarget {
    None,
    FeedTitle,
    FeedId,
    FeedUpdated,
    EntryId,
    EntryUpdated,
    Property(PropertyTarget),
}

#[derive(Default)]
struct EntryBuilder {
    entry_id: Option<String>,
    published_at: Option<String>,
    properties: BTreeMap<String, (Option<String>, Option<String>)>,
}

impl EntryBuilder {
    fn insert(
        &mut self,
        property: PropertyTarget,
        value: Option<String>,
        max_fields: usize,
    ) -> Result<(), TreasuryProtocolError> {
        if self.properties.len() == max_fields || property.name.len() > 256 {
            return Err(TreasuryProtocolError::FieldTooLarge);
        }
        if self
            .properties
            .insert(property.name, (value, property.data_type))
            .is_some()
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        Ok(())
    }

    fn finish(
        &mut self,
        request: &TreasuryYieldCurvePageRequest,
        source_payload_digest: [u8; 32],
    ) -> Result<DailyParYieldCurveObservation, TreasuryProtocolError> {
        let source_record_id = required_property(&self.properties, "Id", "Edm.Int32")?;
        if !source_record_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let entry_id = self
            .entry_id
            .as_deref()
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        let expected_id = format!("{FEED_SOURCE_IDENTITY}&id={source_record_id}");
        if entry_id != expected_id {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let provider_date = required_property(&self.properties, "NEW_DATE", "Edm.DateTime")?;
        let date_text = provider_date
            .strip_suffix("T00:00:00")
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        let record_date = parse_date(date_text).map_err(|_| TreasuryProtocolError::SchemaDrift)?;
        if record_date.year() != request.year {
            return Err(TreasuryProtocolError::QueryBindingMismatch);
        }
        let mut rates_percent = BTreeMap::new();
        for (field, maturity) in RATE_FIELDS {
            if let Some((value, data_type)) = self.properties.get(field) {
                if data_type.as_deref() != Some("Edm.Double") {
                    return Err(TreasuryProtocolError::SchemaDrift);
                }
                if let Some(value) = value {
                    let decimal = Decimal::from_str_exact(value)
                        .map_err(|_| TreasuryProtocolError::SchemaDrift)?;
                    rates_percent.insert(maturity, decimal);
                }
            }
        }
        if rates_percent.is_empty() {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let allowed = RATE_FIELDS
            .iter()
            .map(|(field, _)| *field)
            .chain(["Id", "NEW_DATE", "BC_30YEARDISPLAY"])
            .collect::<BTreeSet<_>>();
        if self
            .properties
            .keys()
            .any(|field| !allowed.contains(field.as_str()))
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        if let Some((display, data_type)) = self.properties.get("BC_30YEARDISPLAY")
            && (data_type.as_deref() != Some("Edm.Double")
                || display.as_deref()
                    != rates_percent
                        .get(&TreasuryMaturity::ThirtyYears)
                        .map(Decimal::to_string)
                        .as_deref())
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let source_published_at = parse_atom_timestamp(
            self.published_at
                .as_deref()
                .ok_or(TreasuryProtocolError::SchemaDrift)?,
        )?;
        let mut row_identity = Sha256::new();
        update_component(&mut row_identity, "treasury-yield-row-v1");
        update_component(&mut row_identity, &source_record_id);
        update_component(&mut row_identity, date_text);
        Ok(DailyParYieldCurveObservation {
            source_record_id,
            record_date,
            rates_percent,
            source_published_at,
            row_identity: row_identity.finalize().into(),
            source_payload_digest,
        })
    }

    fn append_property(
        &mut self,
        property: &PropertyTarget,
        value: &str,
        max_fields: usize,
    ) -> Result<(), TreasuryProtocolError> {
        if let Some((current, data_type)) = self.properties.get_mut(&property.name) {
            if data_type != &property.data_type || property.is_null {
                return Err(TreasuryProtocolError::SchemaDrift);
            }
            let current = current.as_mut().ok_or(TreasuryProtocolError::SchemaDrift)?;
            if current.len().saturating_add(value.len()) > 64 * 1024 {
                return Err(TreasuryProtocolError::FieldTooLarge);
            }
            current.push_str(value);
            return Ok(());
        }
        self.insert(property.clone(), Some(value.to_owned()), max_fields)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "XML text targets retain distinct source-lineage fields"
)]
fn append_target(
    target: &TextTarget,
    value: &str,
    feed_title: &mut Option<String>,
    feed_id: &mut Option<String>,
    feed_published_at: &mut Option<String>,
    entry: &mut EntryBuilder,
    max_fields: usize,
) -> Result<(), TreasuryProtocolError> {
    match target {
        TextTarget::FeedTitle => append_text(feed_title, value)?,
        TextTarget::FeedId => append_text(feed_id, value)?,
        TextTarget::FeedUpdated => append_text(feed_published_at, value)?,
        TextTarget::EntryId => append_text(&mut entry.entry_id, value)?,
        TextTarget::EntryUpdated => append_text(&mut entry.published_at, value)?,
        TextTarget::Property(property) => entry.append_property(property, value, max_fields)?,
        TextTarget::None => {}
    }
    Ok(())
}

fn append_text(target: &mut Option<String>, value: &str) -> Result<(), TreasuryProtocolError> {
    if let Some(current) = target {
        if current.len().saturating_add(value.len()) > 64 * 1024 {
            return Err(TreasuryProtocolError::FieldTooLarge);
        }
        current.push_str(value);
    } else {
        *target = Some(value.to_owned());
    }
    Ok(())
}

fn local_name(start: &BytesStart<'_>) -> Result<String, TreasuryProtocolError> {
    std::str::from_utf8(start.local_name().as_ref())
        .map(str::to_owned)
        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))
}

fn property_target(
    reader: &NsReader<&[u8]>,
    xml_version: XmlVersion,
    start: &BytesStart<'_>,
    name: String,
) -> Result<PropertyTarget, TreasuryProtocolError> {
    let mut data_type = None;
    let mut is_null = false;
    for attribute in start.attributes() {
        let attribute =
            attribute.map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
        let (namespace, local_name) = reader.resolver().resolve_attribute(attribute.key);
        require_namespace(&namespace, METADATA_NAMESPACE)?;
        let key = std::str::from_utf8(local_name.as_ref())
            .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
        let value = attribute
            .decoded_and_normalized_value(xml_version, reader.decoder())
            .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
        match key {
            "type" => data_type = Some(value.into_owned()),
            "null" if value.as_ref() == "true" => is_null = true,
            _ => return Err(TreasuryProtocolError::SchemaDrift),
        }
    }
    Ok(PropertyTarget {
        name,
        data_type,
        is_null,
    })
}

fn require_namespace(
    actual: &ResolveResult<'_>,
    expected: &[u8],
) -> Result<(), TreasuryProtocolError> {
    if matches!(actual, ResolveResult::Bound(namespace) if namespace.as_ref() == expected) {
        Ok(())
    } else {
        Err(TreasuryProtocolError::SchemaDrift)
    }
}

fn required_property(
    properties: &BTreeMap<String, (Option<String>, Option<String>)>,
    name: &str,
    data_type: &str,
) -> Result<String, TreasuryProtocolError> {
    let (value, actual_type) = properties
        .get(name)
        .ok_or(TreasuryProtocolError::SchemaDrift)?;
    if actual_type.as_deref() != Some(data_type) {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    value.clone().ok_or(TreasuryProtocolError::SchemaDrift)
}

fn yield_query_digest(year: u16) -> [u8; 32] {
    let mut digest = Sha256::new();
    update_component(&mut digest, "treasury-yield-query-v1");
    update_component(&mut digest, FEED_ENDPOINT);
    update_component(&mut digest, FEED_SOURCE_IDENTITY);
    update_component(&mut digest, "daily_treasury_yield_curve");
    for (field, _) in RATE_FIELDS {
        update_component(&mut digest, field);
    }
    update_component(&mut digest, "NEW_DATE:year");
    update_component(&mut digest, "NEW_DATE:ascending");
    update_component(&mut digest, PROVIDER_YEAR_RESPONSE_ID);
    digest.update(year.to_be_bytes());
    digest.finalize().into()
}

fn parse_atom_timestamp(value: &str) -> Result<Timestamp, TreasuryProtocolError> {
    if value.bytes().any(|byte| byte.is_ascii_whitespace())
        || value.as_bytes().get(10) != Some(&b'T')
        || value.ends_with('z')
        || value.ends_with("-00:00")
    {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_| TreasuryProtocolError::SchemaDrift)?;
    let nanos = parsed
        .timestamp()
        .checked_mul(1_000_000_000)
        .and_then(|nanos| nanos.checked_add(i64::from(parsed.timestamp_subsec_nanos())))
        .ok_or(TreasuryProtocolError::SchemaDrift)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
