use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    CalendarDate, ExactPayloadEvidence, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, ExtractionAuthority, ExtractionSourceError, NetworkPolicyError, PathScope,
    ProviderCaptureMaterial, QueryParameterRule, QuerySensitivity, SourceError,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::FredParseLimits;
use crate::series::{admit_body, parse_date, validate_strings};

use super::http::system_timestamp;
use super::lineage::{evidence_for_payload, map_adapter_error};
use super::{
    FredDataset, FredHttpAuthorization, FredHttpRequest, FredSource, FredSourceError,
    acquire_request_permit, standalone_capture_material,
};

const SERIES_ENDPOINT: &str = "https://api.stlouisfed.org/fred/series";
const MAX_METADATA_STRING_BYTES: usize = 8 * 1024;

/// Builds the exact endpoint-policy rule required by [`FredSource::acquire_series_metadata`].
///
/// The rule fixes the official HTTPS path and admits only the five documented query keys. The API
/// key is explicitly secret so endpoint-policy diagnostics cannot retain its value.
///
/// # Errors
///
/// Returns [`NetworkPolicyError::InvalidRequestBounds`] if an internal bounded identity cannot be
/// represented, and otherwise forwards endpoint-rule invariant failures.
pub fn fred_series_endpoint_rule() -> Result<ApiEndpointRule, NetworkPolicyError> {
    let query_rules = [
        ("api_key", 32, QuerySensitivity::Secret),
        ("series_id", 120, QuerySensitivity::Public),
        ("realtime_start", 10, QuerySensitivity::Public),
        ("realtime_end", 10, QuerySensitivity::Public),
        ("file_type", 4, QuerySensitivity::Public),
    ]
    .into_iter()
    .map(|(key, max_value_bytes, sensitivity)| {
        QueryParameterRule::try_new(
            SourceIdentifier::try_from(key)
                .map_err(|_| NetworkPolicyError::InvalidRequestBounds)?,
            max_value_bytes,
            false,
            sensitivity,
        )
    })
    .collect::<Result<Vec<_>, _>>()?;
    ApiEndpointRule::try_new(SERIES_ENDPOINT, PathScope::Exact, query_rules, 5, 512)
}

/// Provider-authored FRED series semantics, retained without inferred transformations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredSeriesMetadata {
    series_id: SourceIdentifier,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    title: String,
    observation_start: CalendarDate,
    observation_end: CalendarDate,
    frequency: String,
    frequency_short: String,
    units: String,
    units_short: String,
    seasonal_adjustment: String,
    seasonal_adjustment_short: String,
    last_updated: String,
    popularity: u32,
    notes: Option<String>,
}

impl FredSeriesMetadata {
    /// Returns the provider series identity validated against the requested dataset.
    pub const fn series_id(&self) -> &SourceIdentifier {
        &self.series_id
    }

    /// Returns the first civil date in the provider's metadata realtime interval.
    pub const fn realtime_start(&self) -> CalendarDate {
        self.realtime_start
    }

    /// Returns the inclusive final civil date in the provider's metadata realtime interval.
    pub const fn realtime_end(&self) -> CalendarDate {
        self.realtime_end
    }

    /// Returns the provider-authored series title.
    pub fn title(&self) -> &str {
        &self.title
    }

    /// Returns the first provider observation civil date.
    pub const fn observation_start(&self) -> CalendarDate {
        self.observation_start
    }

    /// Returns the final provider observation civil date.
    pub const fn observation_end(&self) -> CalendarDate {
        self.observation_end
    }

    /// Returns the provider's full frequency label.
    pub fn frequency(&self) -> &str {
        &self.frequency
    }

    /// Returns the provider's abbreviated frequency label.
    pub fn frequency_short(&self) -> &str {
        &self.frequency_short
    }

    /// Returns the provider's full unit label.
    pub fn units(&self) -> &str {
        &self.units
    }

    /// Returns the provider's abbreviated unit label.
    pub fn units_short(&self) -> &str {
        &self.units_short
    }

    /// Returns the provider's full seasonal-adjustment label.
    pub fn seasonal_adjustment(&self) -> &str {
        &self.seasonal_adjustment
    }

    /// Returns the provider's abbreviated seasonal-adjustment label.
    pub fn seasonal_adjustment_short(&self) -> &str {
        &self.seasonal_adjustment_short
    }

    /// Returns the provider's exact `last_updated` lexical value without timezone inference.
    pub fn last_updated(&self) -> &str {
        &self.last_updated
    }

    /// Returns the provider's popularity value.
    pub const fn popularity(&self) -> u32 {
        self.popularity
    }

    /// Returns optional provider-authored notes exactly as supplied.
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_deref()
    }

    /// Parses one bounded credential-probe response for an exact code-owned series selector.
    ///
    /// This uses the same strict one-series schema and civil-date consistency checks as normal
    /// extraction without manufacturing a durable dataset.
    ///
    /// # Errors
    ///
    /// Returns [`FredSourceError::Protocol`] when the body, schema, series identity, strings, or
    /// provider realtime interval is invalid.
    pub fn parse_probe_response(
        bytes: &[u8],
        expected_series: &SourceIdentifier,
        limits: FredParseLimits,
    ) -> Result<Self, FredSourceError> {
        parse_series_metadata_for_series(bytes, expected_series.as_str(), limits)
    }
}

/// One exact FRED series-metadata response bound to its source and dataset identities.
#[derive(Debug)]
pub struct FredSeriesMetadataDocument {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    series: FredSeriesMetadata,
    response_bytes: Bytes,
    response_length: u64,
    evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    capture: ProviderCaptureMaterial,
}

impl FredSeriesMetadataDocument {
    /// Returns the registered source identity that acquired this response.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision used for the request.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the complete canonical dataset identity used for the request.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns validated provider-authored series semantics.
    pub const fn series(&self) -> &FredSeriesMetadata {
        &self.series
    }

    /// Returns the exact response bytes used to construct [`Self::series`].
    pub const fn response_bytes(&self) -> &Bytes {
        &self.response_bytes
    }

    /// Returns the checked exact byte length of [`Self::response_bytes`].
    pub const fn response_length(&self) -> u64 {
        self.response_length
    }

    /// Returns digest-backed evidence with the exact secret-free request locator.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns when this process completed receipt of the exact response.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the exact metadata response ready for source-neutral raw sealing.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Consumes the document into its exact metadata response material.
    pub fn into_capture_material(self) -> ProviderCaptureMaterial {
        self.capture
    }

    pub(super) fn into_native_semantics_and_capture(
        self,
    ) -> (FredSeriesMetadata, ProviderCaptureMaterial) {
        (self.series, self.capture)
    }
}

impl FredSource {
    /// Acquires one exact, identity-validated `fred/series` metadata document.
    ///
    /// The API key is authorized and transmitted separately but is never retained in the public
    /// locator, result, or adapter error.
    ///
    /// # Errors
    ///
    /// Fails closed when authority, budget, deadline, transport, response bounds, exact response
    /// schema, civil-date interval, or requested series identity cannot be verified.
    pub async fn acquire_series_metadata(
        &self,
        authority: &ExtractionAuthority,
        dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FredSeriesMetadataDocument, ExtractionSourceError> {
        self.validate_authority(authority)?;
        self.validate_provider_dataset(dataset)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let dataset_identity = FredDataset::parse(dataset)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let now = system_timestamp().map_err(map_adapter_error)?;
        if deadline <= now {
            return Err(ExtractionSourceError::DeadlineExceeded);
        }
        let mut public_url = url::Url::parse(SERIES_ENDPOINT)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        public_url
            .query_pairs_mut()
            .append_pair("series_id", dataset_identity.series_id())
            .append_pair(
                "realtime_start",
                &dataset_identity.realtime_start().to_string(),
            )
            .append_pair("realtime_end", &dataset_identity.realtime_end().to_string())
            .append_pair("file_type", "json");
        let mut authorization_target = public_url.clone();
        authorization_target
            .query_pairs_mut()
            .append_pair("api_key", self.api_key.expose());
        let permit = acquire_request_permit(
            authority,
            authorization_target.as_str(),
            deadline,
            cancellation.clone(),
        )
        .await?;
        let in_flight = permit.authorize_send(authorization_target.as_str())?;
        drop(authorization_target);
        let wall_remaining = deadline
            .unix_nanos()
            .checked_sub(now.unix_nanos())
            .and_then(|nanos| u64::try_from(nanos).ok())
            .map(Duration::from_nanos)
            .ok_or(ExtractionSourceError::DeadlineExceeded)?;
        let timeout = self.request_timeout.min(wall_remaining);
        let response = self
            .transport
            .execute(
                FredHttpRequest {
                    public_url: public_url.clone(),
                    api_key: self.api_key.clone(),
                    authorization: FredHttpAuthorization::QueryParameter,
                },
                self.response_limit,
                timeout,
                cancellation,
            )
            .await
            .map_err(map_adapter_error)?;
        in_flight.validate_response_size(
            u64::try_from(response.body.len())
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
        )?;
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        match response.status {
            200 => {}
            401 | 403 => return Err(ExtractionSourceError::Source(SourceError::Unauthorized)),
            429 | 503 => {
                let deadline =
                    in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
                return Err(ExtractionSourceError::Source(
                    SourceError::BudgetWaitUntil { deadline },
                ));
            }
            _ => return Err(ExtractionSourceError::Source(SourceError::Network)),
        }

        let limits = FredParseLimits::try_new(
            1,
            self.response_limit,
            self.response_limit.min(MAX_METADATA_STRING_BYTES),
        )
        .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let series = parse_series_metadata(&response.body, &dataset_identity, limits)
            .map_err(map_adapter_error)?;
        let response_length = u64::try_from(response.body.len())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let evidence =
            evidence_for_payload(&response.body, &public_url).map_err(map_adapter_error)?;
        let capture =
            standalone_capture_material(&self.metadata, dataset.clone(), &public_url, &response)?;
        in_flight.record_success()?;
        Ok(FredSeriesMetadataDocument {
            source_id: self.metadata.source_id().clone(),
            metadata_revision: self.metadata.revision().clone(),
            dataset: dataset.clone(),
            series,
            response_bytes: response.body,
            response_length,
            evidence,
            received_at: response.received_at,
            capture,
        })
    }
}

fn parse_series_metadata(
    bytes: &[u8],
    dataset: &FredDataset,
    limits: FredParseLimits,
) -> Result<FredSeriesMetadata, FredSourceError> {
    let series = parse_series_metadata_for_series(bytes, dataset.series_id(), limits)?;
    if series.realtime_start != dataset.realtime_start()
        || series.realtime_end != dataset.realtime_end()
    {
        return Err(FredSourceError::Protocol);
    }
    Ok(series)
}

fn parse_series_metadata_for_series(
    bytes: &[u8],
    expected_series: &str,
    limits: FredParseLimits,
) -> Result<FredSeriesMetadata, FredSourceError> {
    admit_body(bytes, limits).map_err(|_| FredSourceError::Protocol)?;
    let wire: SeriesResponseWire =
        serde_json::from_slice(bytes).map_err(|_| FredSourceError::Protocol)?;
    if wire.seriess.len() != 1 {
        return Err(FredSourceError::Protocol);
    }
    let page_start = parse_date(&wire.realtime_start).map_err(|_| FredSourceError::Protocol)?;
    let page_end = parse_date(&wire.realtime_end).map_err(|_| FredSourceError::Protocol)?;
    let mut rows = wire.seriess.into_iter();
    let row = rows.next().ok_or(FredSourceError::Protocol)?;
    let values = [
        row.id.as_str(),
        row.title.as_str(),
        row.frequency.as_str(),
        row.frequency_short.as_str(),
        row.units.as_str(),
        row.units_short.as_str(),
        row.seasonal_adjustment.as_str(),
        row.seasonal_adjustment_short.as_str(),
        row.last_updated.as_str(),
        row.notes.as_deref().unwrap_or_default(),
    ];
    validate_strings(values, limits).map_err(|_| FredSourceError::Protocol)?;
    if values[..9].iter().any(|value| value.is_empty()) || !is_valid_last_updated(&row.last_updated)
    {
        return Err(FredSourceError::Protocol);
    }
    let series_id = SourceIdentifier::try_from(row.id).map_err(|_| FredSourceError::Protocol)?;
    let realtime_start = parse_date(&row.realtime_start).map_err(|_| FredSourceError::Protocol)?;
    let realtime_end = parse_date(&row.realtime_end).map_err(|_| FredSourceError::Protocol)?;
    let observation_start =
        parse_date(&row.observation_start).map_err(|_| FredSourceError::Protocol)?;
    let observation_end =
        parse_date(&row.observation_end).map_err(|_| FredSourceError::Protocol)?;
    if series_id.as_str() != expected_series
        || realtime_start != page_start
        || realtime_end != page_end
        || observation_start > observation_end
    {
        return Err(FredSourceError::Protocol);
    }
    Ok(FredSeriesMetadata {
        series_id,
        realtime_start,
        realtime_end,
        title: row.title,
        observation_start,
        observation_end,
        frequency: row.frequency,
        frequency_short: row.frequency_short,
        units: row.units,
        units_short: row.units_short,
        seasonal_adjustment: row.seasonal_adjustment,
        seasonal_adjustment_short: row.seasonal_adjustment_short,
        last_updated: row.last_updated,
        popularity: row.popularity,
        notes: row.notes,
    })
}

fn is_valid_last_updated(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 22
        || bytes[10] != b' '
        || bytes[13] != b':'
        || bytes[16] != b':'
        || !matches!(bytes[19], b'+' | b'-')
    {
        return false;
    }
    let Some(date) = value.get(..10) else {
        return false;
    };
    parse_date(date).is_ok()
        && two_ascii_digits(bytes, 11).is_some_and(|hour| hour <= 23)
        && two_ascii_digits(bytes, 14).is_some_and(|minute| minute <= 59)
        && two_ascii_digits(bytes, 17).is_some_and(|second| second <= 60)
        && two_ascii_digits(bytes, 20).is_some_and(|offset| offset <= 23)
}

fn two_ascii_digits(bytes: &[u8], start: usize) -> Option<u8> {
    let tens = bytes.get(start)?.checked_sub(b'0')?;
    let ones = bytes.get(start.checked_add(1)?)?.checked_sub(b'0')?;
    if tens > 9 || ones > 9 {
        return None;
    }
    tens.checked_mul(10)?.checked_add(ones)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesResponseWire {
    realtime_start: String,
    realtime_end: String,
    seriess: Vec<SeriesWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesWire {
    id: String,
    realtime_start: String,
    realtime_end: String,
    title: String,
    observation_start: String,
    observation_end: String,
    frequency: String,
    frequency_short: String,
    units: String,
    units_short: String,
    seasonal_adjustment: String,
    seasonal_adjustment_short: String,
    last_updated: String,
    popularity: u32,
    #[serde(default)]
    notes: Option<String>,
}
