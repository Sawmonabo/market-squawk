use std::time::Duration;

use bytes::Bytes;
use market_squawk_domain::{
    CalendarDate, ExactPayloadEvidence, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ApiEndpointRule, ExtractionAuthority, ExtractionSourceError, NetworkPolicyError, PathScope,
    ProviderCaptureMaterial, QueryParameterRule, QuerySensitivity, SourceError,
    SourceMetadataIntervalViolation, SourceMetadataSchemaViolation, SourceProtocolViolation,
};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::FredParseLimits;
use crate::series::{admit_body, parse_date, validate_strings};

use super::http::system_timestamp;
use super::lineage::{evidence_for_payload, map_adapter_error};
use super::{
    FredDataset, FredDiscoveryError, FredHttpAuthorization, FredHttpRequest, FredSource,
    FredSourceError, acquire_request_permit, protocol_violation, standalone_capture_material,
};

const SERIES_ENDPOINT: &str = "https://api.stlouisfed.org/fred/series";
const MAX_METADATA_STRING_BYTES: usize = 8 * 1024;
/// Maximum ordered metadata revisions retained from one bounded series response.
pub const MAX_FRED_SERIES_METADATA_REVISIONS: usize = 4_096;

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
        let revisions =
            parse_series_metadata_for_series(bytes, expected_series.as_str(), None, limits)
                .map_err(|_| FredSourceError::Protocol)?;
        if revisions.len() != 1 {
            return Err(FredSourceError::Protocol);
        }
        revisions.into_vec().pop().ok_or(FredSourceError::Protocol)
    }
}

/// One exact FRED series-metadata response bound to its source and dataset identities.
#[derive(Debug)]
pub struct FredSeriesMetadataDocument {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    series_revisions: Box<[FredSeriesMetadata]>,
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

    /// Returns every validated provider-authored metadata revision in real-time order.
    pub fn series_revisions(&self) -> &[FredSeriesMetadata] {
        &self.series_revisions
    }

    /// Returns the exact response bytes used to construct [`Self::series_revisions`].
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
    ) -> (Box<[FredSeriesMetadata]>, ProviderCaptureMaterial) {
        (self.series_revisions, self.capture)
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
        self.acquire_series_metadata_with_diagnostic(authority, dataset, deadline, cancellation)
            .await
            .map_err(FredDiscoveryError::into_source_error)
    }

    pub(super) async fn acquire_series_metadata_with_diagnostic(
        &self,
        authority: &ExtractionAuthority,
        dataset: &SourceIdentifier,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FredSeriesMetadataDocument, FredDiscoveryError> {
        self.validate_authority(authority)?;
        self.validate_provider_dataset(dataset)?;
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        let dataset_identity = FredDataset::parse(dataset)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let now = system_timestamp().map_err(map_adapter_error)?;
        if deadline <= now {
            return Err(ExtractionSourceError::DeadlineExceeded.into());
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
                .map_err(|_| protocol_violation(SourceProtocolViolation::CaptureBinding))?,
        )?;
        if response
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(protocol_violation(
                SourceProtocolViolation::MetadataEncoding,
            ));
        }
        match response.status {
            200 => {}
            401 | 403 => {
                return Err(ExtractionSourceError::Source(SourceError::Unauthorized).into());
            }
            429 | 503 => {
                let deadline =
                    in_flight.apply_retry_after_header(response.retry_after.as_deref(), 0)?;
                return Err(ExtractionSourceError::Source(SourceError::BudgetWaitUntil {
                    deadline,
                })
                .into());
            }
            _ => return Err(ExtractionSourceError::Source(SourceError::Network).into()),
        }

        let limits = FredParseLimits::try_new(
            MAX_FRED_SERIES_METADATA_REVISIONS,
            self.response_limit,
            self.response_limit.min(MAX_METADATA_STRING_BYTES),
        )
        .map_err(|_| {
            protocol_violation(SourceProtocolViolation::MetadataSchema(
                SourceMetadataSchemaViolation::DocumentShape,
            ))
        })?;
        let series_revisions = parse_series_metadata(&response.body, &dataset_identity, limits)
            .map_err(protocol_violation)?;
        let response_length = u64::try_from(response.body.len())
            .map_err(|_| protocol_violation(SourceProtocolViolation::CaptureBinding))?;
        let evidence = evidence_for_payload(&response.body, &public_url)
            .map_err(|_| protocol_violation(SourceProtocolViolation::CaptureBinding))?;
        let capture =
            standalone_capture_material(&self.metadata, dataset.clone(), &public_url, &response)
                .map_err(|_| protocol_violation(SourceProtocolViolation::CaptureBinding))?;
        in_flight.record_success()?;
        Ok(FredSeriesMetadataDocument {
            source_id: self.metadata.source_id().clone(),
            metadata_revision: self.metadata.revision().clone(),
            dataset: dataset.clone(),
            series_revisions,
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
) -> Result<Box<[FredSeriesMetadata]>, SourceProtocolViolation> {
    let series_revisions = parse_series_metadata_for_series(
        bytes,
        dataset.series_id(),
        Some((dataset.realtime_start(), dataset.realtime_end())),
        limits,
    )
    .map_err(SourceProtocolViolation::MetadataSchema)?;
    let Some(first) = series_revisions.first() else {
        return Err(SourceProtocolViolation::MetadataSchema(
            SourceMetadataSchemaViolation::RecordCardinality,
        ));
    };
    let Some(last) = series_revisions.last() else {
        return Err(SourceProtocolViolation::MetadataSchema(
            SourceMetadataSchemaViolation::RecordCardinality,
        ));
    };
    if first.realtime_start > dataset.realtime_start() || last.realtime_end < dataset.realtime_end()
    {
        return Err(SourceProtocolViolation::MetadataInterval);
    }
    Ok(series_revisions)
}

fn parse_series_metadata_for_series(
    bytes: &[u8],
    expected_series: &str,
    expected_page_interval: Option<(CalendarDate, CalendarDate)>,
    limits: FredParseLimits,
) -> Result<Box<[FredSeriesMetadata]>, SourceMetadataSchemaViolation> {
    admit_body(bytes, limits).map_err(|_| SourceMetadataSchemaViolation::DocumentShape)?;
    let wire: SeriesResponseWire =
        serde_json::from_slice(bytes).map_err(|_| SourceMetadataSchemaViolation::DocumentShape)?;
    if wire.seriess.is_empty() || wire.seriess.len() > limits.max_records {
        return Err(SourceMetadataSchemaViolation::RecordCardinality);
    }
    let page_start = parse_date(&wire.realtime_start)
        .map_err(|_| metadata_interval(SourceMetadataIntervalViolation::ResponseEnvelopeStart))?;
    let page_end = parse_date(&wire.realtime_end)
        .map_err(|_| metadata_interval(SourceMetadataIntervalViolation::ResponseEnvelopeEnd))?;
    if page_start > page_end {
        return Err(metadata_interval(
            SourceMetadataIntervalViolation::ResponseEnvelopeOrder,
        ));
    }
    if expected_page_interval.is_some_and(|expected| expected != (page_start, page_end)) {
        return Err(metadata_interval(
            SourceMetadataIntervalViolation::ResponseEnvelopeBinding,
        ));
    }
    let mut revisions = Vec::new();
    revisions
        .try_reserve_exact(wire.seriess.len())
        .map_err(|_| SourceMetadataSchemaViolation::RecordCardinality)?;
    for row in wire.seriess {
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
        validate_strings(values, limits)
            .map_err(|_| SourceMetadataSchemaViolation::RequiredText)?;
        if values[..9].iter().any(|value| value.is_empty()) {
            return Err(SourceMetadataSchemaViolation::RequiredText);
        }
        if !is_valid_last_updated(&row.last_updated) {
            return Err(SourceMetadataSchemaViolation::UpdateTimestamp);
        }
        let series_id = SourceIdentifier::try_from(row.id)
            .map_err(|_| SourceMetadataSchemaViolation::RecordIdentity)?;
        let realtime_start = parse_date(&row.realtime_start)
            .map_err(|_| metadata_interval(SourceMetadataIntervalViolation::RecordStart))?;
        let realtime_end = parse_date(&row.realtime_end)
            .map_err(|_| metadata_interval(SourceMetadataIntervalViolation::RecordEnd))?;
        let observation_start = parse_date(&row.observation_start)
            .map_err(|_| SourceMetadataSchemaViolation::ObservationInterval)?;
        let observation_end = parse_date(&row.observation_end)
            .map_err(|_| SourceMetadataSchemaViolation::ObservationInterval)?;
        if series_id.as_str() != expected_series {
            return Err(SourceMetadataSchemaViolation::RecordIdentity);
        }
        if realtime_start > realtime_end {
            return Err(metadata_interval(
                SourceMetadataIntervalViolation::RecordOrder,
            ));
        }
        if observation_start > observation_end {
            return Err(SourceMetadataSchemaViolation::ObservationInterval);
        }
        revisions.push(FredSeriesMetadata {
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
        });
    }
    // This endpoint has no ordering selector. Preserve the exact provider array in the raw
    // capture, but canonicalize the semantic timeline before proving closed interval coverage.
    revisions.sort_unstable_by_key(|revision| (revision.realtime_start, revision.realtime_end));
    let first = revisions
        .first()
        .ok_or(SourceMetadataSchemaViolation::RecordCardinality)?;
    let last = revisions
        .last()
        .ok_or(SourceMetadataSchemaViolation::RecordCardinality)?;
    // The response-level dates are the requested envelope. Provider-authored metadata validity
    // can begin before or end after that envelope, so require complete coverage without erasing
    // those exact source intervals.
    if first.realtime_start > page_start {
        return Err(metadata_interval(
            SourceMetadataIntervalViolation::OuterStartCoverage,
        ));
    }
    if last.realtime_end < page_end {
        return Err(metadata_interval(
            SourceMetadataIntervalViolation::OuterEndCoverage,
        ));
    }
    for pair in revisions.windows(2) {
        if let Some(reason) = metadata_interval_discontinuity(&pair[0], &pair[1]) {
            return Err(metadata_interval(reason));
        }
    }
    Ok(revisions.into_boxed_slice())
}

const fn metadata_interval(
    reason: SourceMetadataIntervalViolation,
) -> SourceMetadataSchemaViolation {
    SourceMetadataSchemaViolation::PageRecordInterval(reason)
}

fn metadata_interval_discontinuity(
    previous: &FredSeriesMetadata,
    next: &FredSeriesMetadata,
) -> Option<SourceMetadataIntervalViolation> {
    if previous.realtime_start == next.realtime_start && previous.realtime_end == next.realtime_end
    {
        return Some(SourceMetadataIntervalViolation::DuplicateInterval);
    }
    if next.realtime_start <= previous.realtime_end {
        return Some(SourceMetadataIntervalViolation::Overlap);
    }
    (!closed_intervals_are_contiguous(previous.realtime_end, next.realtime_start))
        .then_some(SourceMetadataIntervalViolation::Gap)
}

fn closed_intervals_are_contiguous(previous_end: CalendarDate, next_start: CalendarDate) -> bool {
    previous_end.days_since_unix_epoch().checked_add(1) == Some(next_start.days_since_unix_epoch())
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
