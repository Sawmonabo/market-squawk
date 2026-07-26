use std::collections::{BTreeMap, BTreeSet};

use chrono::DateTime;
use market_squawk_domain::{CalendarDate, SourceIdentifier, Timestamp};
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::ResolveResult;
use quick_xml::reader::NsReader;
use sha2::{Digest, Sha256};

use crate::TreasuryProtocolError;
use crate::fiscal_data::FiscalDataParseLimits;
use crate::query::update_component;

use super::schema::{
    PropertyValue, date_field, decode_row, id_field, required_provider_date, required_typed,
};
use super::{
    TreasuryDailyRateFamily, TreasuryDailyRateObservation, TreasuryDailyRatePageRequest,
    TreasuryDailyRatePeriod, TreasuryDailyRatePeriodKind,
};

const ATOM_NAMESPACE: &[u8] = b"http://www.w3.org/2005/Atom";
const DATA_NAMESPACE: &[u8] = b"http://schemas.microsoft.com/ado/2007/08/dataservices";
const METADATA_NAMESPACE: &[u8] = b"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
const MAX_TEXT_BYTES: usize = 64 * 1024;

/// One bounded response from an official Treasury daily-rate dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryDailyRatePage {
    family: TreasuryDailyRateFamily,
    period: TreasuryDailyRatePeriod,
    page_number: usize,
    dataset: SourceIdentifier,
    request_digest: [u8; 32],
    query_digest: [u8; 32],
    response_payload_digest: [u8; 32],
    feed_published_at: Timestamp,
    observations: Vec<TreasuryDailyRateObservation>,
}

impl TreasuryDailyRatePage {
    /// Parses one exact bounded Atom/OData response.
    ///
    /// # Errors
    ///
    /// Rejects malformed XML, namespace or schema drift, invalid financial values, source identity
    /// drift, query-period mismatch, duplicate provider rows, and configured resource overrun.
    pub fn parse(
        bytes: &[u8],
        request: &TreasuryDailyRatePageRequest,
        limits: FiscalDataParseLimits,
    ) -> Result<Self, TreasuryProtocolError> {
        if bytes.len() > limits.max_bytes() {
            return Err(TreasuryProtocolError::BodyTooLarge);
        }
        let source_payload_digest = Sha256::digest(bytes).into();
        let mut reader = NsReader::from_reader(bytes);
        reader.config_mut().trim_text(true);
        let mut xml_version = XmlVersion::Implicit1_0;
        let mut depth = 0_usize;
        let mut saw_feed = false;
        let mut closed_feed = false;
        let mut in_entry = false;
        let mut in_properties = false;
        let mut target = TextTarget::None;
        let mut feed_title = None;
        let mut feed_id = None;
        let mut feed_published_at = None;
        let mut entry = EntryBuilder::default();
        let mut observations = Vec::new();

        loop {
            match reader.read_resolved_event() {
                Ok((namespace, Event::Start(start))) => {
                    depth = depth
                        .checked_add(1)
                        .ok_or(TreasuryProtocolError::SchemaDrift)?;
                    let name = local_name(&start)?;
                    match (depth, name.as_str()) {
                        (1, "feed") => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            if saw_feed {
                                return Err(TreasuryProtocolError::SchemaDrift);
                            }
                            saw_feed = true;
                        }
                        (2, "entry") => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            if !saw_feed || in_entry {
                                return Err(TreasuryProtocolError::SchemaDrift);
                            }
                            in_entry = true;
                            entry = EntryBuilder::default();
                        }
                        (2, "title") if !in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            begin_text(&mut feed_title, &mut target, TextTarget::FeedTitle)?;
                        }
                        (2, "id") if !in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            begin_text(&mut feed_id, &mut target, TextTarget::FeedId)?;
                        }
                        (2, "updated") if !in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            begin_text(
                                &mut feed_published_at,
                                &mut target,
                                TextTarget::FeedUpdated,
                            )?;
                        }
                        (3, "id") if in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            begin_text(&mut entry.entry_id, &mut target, TextTarget::EntryId)?;
                        }
                        (3, "updated") if in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            begin_text(
                                &mut entry.published_at,
                                &mut target,
                                TextTarget::EntryUpdated,
                            )?;
                        }
                        (4, "properties") if in_entry => {
                            require_namespace(&namespace, METADATA_NAMESPACE)?;
                            if in_properties {
                                return Err(TreasuryProtocolError::SchemaDrift);
                            }
                            in_properties = true;
                        }
                        (5, _) if in_properties => {
                            require_namespace(&namespace, DATA_NAMESPACE)?;
                            let property =
                                property_target(&reader, xml_version, &start, name, false)?;
                            entry.begin_property(&property, limits.max_fields())?;
                            target = TextTarget::Property(property.name);
                        }
                        _ => {}
                    }
                }
                Ok((namespace, Event::Empty(start))) => {
                    let name = local_name(&start)?;
                    if depth == 1 && name == "entry" {
                        require_namespace(&namespace, ATOM_NAMESPACE)?;
                        return Err(TreasuryProtocolError::SchemaDrift);
                    }
                    if in_properties && depth == 4 {
                        require_namespace(&namespace, DATA_NAMESPACE)?;
                        let property = property_target(&reader, xml_version, &start, name, true)?;
                        entry.begin_property(&property, limits.max_fields())?;
                    }
                }
                Ok((_, Event::Text(text))) => {
                    let decoded = text
                        .decode()
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    let value = quick_xml::escape::unescape(&decoded)
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                    append_target(
                        &target,
                        value.as_ref(),
                        &mut feed_title,
                        &mut feed_id,
                        &mut feed_published_at,
                        &mut entry,
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
                    )?;
                }
                Ok((namespace, Event::End(end))) => {
                    let name = std::str::from_utf8(end.local_name().as_ref())
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?
                        .to_owned();
                    match (depth, name.as_str()) {
                        (5, _) if in_properties => target = TextTarget::None,
                        (4, "properties") if in_properties => {
                            require_namespace(&namespace, METADATA_NAMESPACE)?;
                            in_properties = false;
                            target = TextTarget::None;
                        }
                        (3, "id" | "updated") if in_entry => target = TextTarget::None,
                        (2, "entry") if in_entry => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            if in_properties {
                                return Err(TreasuryProtocolError::SchemaDrift);
                            }
                            let observation = entry.finish(request, source_payload_digest)?;
                            if observations.len() == limits.max_records() {
                                return Err(TreasuryProtocolError::PaginationLimitExceeded);
                            }
                            observations.push(observation);
                            in_entry = false;
                            target = TextTarget::None;
                            entry = EntryBuilder::default();
                        }
                        (2, "title" | "id" | "updated") if !in_entry => {
                            target = TextTarget::None;
                        }
                        (1, "feed") => {
                            require_namespace(&namespace, ATOM_NAMESPACE)?;
                            closed_feed = true;
                        }
                        _ => {}
                    }
                    depth = depth
                        .checked_sub(1)
                        .ok_or(TreasuryProtocolError::SchemaDrift)?;
                }
                Ok((_, Event::Decl(declaration))) => {
                    xml_version = declaration
                        .xml_version()
                        .map_err(|error| TreasuryProtocolError::InvalidXml(error.to_string()))?;
                }
                Ok((_, Event::DocType(_) | Event::CData(_) | Event::PI(_))) => {
                    return Err(TreasuryProtocolError::SchemaDrift);
                }
                Ok((_, Event::Eof)) => break,
                Ok(_) => {}
                Err(error) => return Err(TreasuryProtocolError::InvalidXml(error.to_string())),
            }
        }

        if depth != 0
            || in_entry
            || in_properties
            || !saw_feed
            || !closed_feed
            || feed_title.as_deref() != Some(request.family().feed_title())
            || feed_id.as_deref() != Some(request.family().feed_identity())
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let feed_published_at =
            parse_atom_timestamp(&feed_published_at.ok_or(TreasuryProtocolError::SchemaDrift)?)?;
        if observations
            .iter()
            .any(|observation| observation.source_published_at() > feed_published_at)
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let mut identities = BTreeSet::new();
        if observations
            .iter()
            .any(|observation| !identities.insert(observation.row_identity()))
        {
            return Err(TreasuryProtocolError::DuplicateRecordIdentity);
        }
        Ok(Self {
            family: request.family(),
            period: request.period(),
            page_number: request.page_number(),
            dataset: request.dataset().clone(),
            request_digest: request.request_digest(),
            query_digest: request.query_digest(),
            response_payload_digest: source_payload_digest,
            feed_published_at,
            observations,
        })
    }

    /// Returns whether an all-history page contains no entries and terminates paging.
    pub fn is_terminal(&self) -> bool {
        self.observations.is_empty()
    }

    /// Returns the provider dataset family.
    pub const fn family(&self) -> TreasuryDailyRateFamily {
        self.family
    }

    /// Returns the validated provider period.
    pub const fn period(&self) -> TreasuryDailyRatePeriod {
        self.period
    }

    /// Returns the zero-based provider page number.
    pub const fn page_number(&self) -> usize {
        self.page_number
    }

    /// Returns the canonical Market Squawk dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the normalized provider observations.
    pub fn observations(&self) -> &[TreasuryDailyRateObservation] {
        &self.observations
    }

    /// Returns the exact Atom feed update instant.
    pub const fn feed_published_at(&self) -> Timestamp {
        self.feed_published_at
    }

    /// Returns the exact query-family digest.
    pub const fn query_digest(&self) -> [u8; 32] {
        self.query_digest
    }

    /// Returns the exact request digest.
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
    Property(String),
}

#[derive(Default)]
struct EntryBuilder {
    entry_id: Option<String>,
    published_at: Option<String>,
    properties: BTreeMap<String, PropertyValue>,
}

impl EntryBuilder {
    fn begin_property(
        &mut self,
        property: &PropertyTarget,
        max_fields: usize,
    ) -> Result<(), TreasuryProtocolError> {
        if self.properties.len() == max_fields || property.name.len() > 256 {
            return Err(TreasuryProtocolError::FieldTooLarge);
        }
        let value = PropertyValue {
            text: (!property.is_null).then(String::new),
            data_type: property.data_type.clone(),
            is_null: property.is_null,
        };
        if self
            .properties
            .insert(property.name.clone(), value)
            .is_some()
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        Ok(())
    }

    fn append_property(&mut self, name: &str, value: &str) -> Result<(), TreasuryProtocolError> {
        let property = self
            .properties
            .get_mut(name)
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        if property.is_null {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let text = property
            .text
            .as_mut()
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        append_string(text, value)
    }

    fn finish(
        &self,
        request: &TreasuryDailyRatePageRequest,
        source_payload_digest: [u8; 32],
    ) -> Result<TreasuryDailyRateObservation, TreasuryProtocolError> {
        let family = request.family();
        let entry_id = self
            .entry_id
            .as_deref()
            .ok_or(TreasuryProtocolError::SchemaDrift)?;
        let prefix = format!("{}&id=", family.feed_identity());
        let source_record_id = entry_id
            .strip_prefix(&prefix)
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or(TreasuryProtocolError::SchemaDrift)?
            .to_owned();
        if let Some(id_field) = id_field(family)
            && required_typed(&self.properties, id_field, "Edm.Int32")? != source_record_id
        {
            return Err(TreasuryProtocolError::SchemaDrift);
        }
        let record_date = required_provider_date(&self.properties, date_field(family))?;
        bind_query_period(record_date, request)?;
        let decoded = decode_row(family, &self.properties, record_date)?;
        let source_published_at = parse_atom_timestamp(
            self.published_at
                .as_deref()
                .ok_or(TreasuryProtocolError::SchemaDrift)?,
        )?;
        let mut row_identity = Sha256::new();
        update_component(&mut row_identity, "treasury-daily-rate-row-v2");
        update_component(&mut row_identity, family.provider_key());
        update_component(&mut row_identity, &source_record_id);
        update_component(&mut row_identity, &record_date.to_string());
        Ok(TreasuryDailyRateObservation::new(
            family,
            source_record_id,
            record_date,
            decoded.points,
            decoded.market_unavailability_reason,
            source_published_at,
            row_identity.finalize().into(),
            source_payload_digest,
        ))
    }
}

fn bind_query_period(
    date: CalendarDate,
    request: &TreasuryDailyRatePageRequest,
) -> Result<(), TreasuryProtocolError> {
    let period = request.period();
    let matches = match period.kind() {
        TreasuryDailyRatePeriodKind::Year => period.year_value() == Some(date.year()),
        TreasuryDailyRatePeriodKind::Month => {
            period.year_value() == Some(date.year()) && period.month_value() == Some(date.month())
        }
        TreasuryDailyRatePeriodKind::AllHistory => date.year() >= request.family().start_year(),
    };
    if matches {
        Ok(())
    } else {
        Err(TreasuryProtocolError::QueryBindingMismatch)
    }
}

fn begin_text(
    value: &mut Option<String>,
    target: &mut TextTarget,
    next_target: TextTarget,
) -> Result<(), TreasuryProtocolError> {
    if value.is_some() || !matches!(target, TextTarget::None) {
        return Err(TreasuryProtocolError::SchemaDrift);
    }
    *value = Some(String::new());
    *target = next_target;
    Ok(())
}

fn append_target(
    target: &TextTarget,
    value: &str,
    feed_title: &mut Option<String>,
    feed_id: &mut Option<String>,
    feed_published_at: &mut Option<String>,
    entry: &mut EntryBuilder,
) -> Result<(), TreasuryProtocolError> {
    match target {
        TextTarget::FeedTitle => append_optional(feed_title, value),
        TextTarget::FeedId => append_optional(feed_id, value),
        TextTarget::FeedUpdated => append_optional(feed_published_at, value),
        TextTarget::EntryId => append_optional(&mut entry.entry_id, value),
        TextTarget::EntryUpdated => append_optional(&mut entry.published_at, value),
        TextTarget::Property(name) => entry.append_property(name, value),
        TextTarget::None => Ok(()),
    }
}

fn append_optional(target: &mut Option<String>, value: &str) -> Result<(), TreasuryProtocolError> {
    let target = target.as_mut().ok_or(TreasuryProtocolError::SchemaDrift)?;
    append_string(target, value)
}

fn append_string(target: &mut String, value: &str) -> Result<(), TreasuryProtocolError> {
    if target.len().saturating_add(value.len()) > MAX_TEXT_BYTES {
        return Err(TreasuryProtocolError::FieldTooLarge);
    }
    target.push_str(value);
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
    empty_element: bool,
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
            "type" if data_type.is_none() => data_type = Some(value.into_owned()),
            "null" if value.as_ref() == "true" && !is_null => is_null = true,
            _ => return Err(TreasuryProtocolError::SchemaDrift),
        }
    }
    if empty_element && !is_null {
        return Err(TreasuryProtocolError::SchemaDrift);
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
