//! Restart-safe FRED/ALFRED point-in-time application reads.

use std::time::{Duration, Instant};

use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use market_squawk_adapter_fred::FredSource;
use market_squawk_data::{
    AnalyticalMacroLatestKnownRequest, AnalyticalMacroSeriesAllowlist, AnalyticalReadCapability,
    AnalyticalReadError, DatasetId, DatasetManifestRef, QueryError, QueryLimits,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, MacroObservation, PayloadReference,
    SourceId, SourceIdentifier, Timestamp,
};
use serde::Serialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::FRED_SURFACE;

const FRED_DESKTOP_READ_SCHEMA: &str = "market-squawk-fred-alfred-point-in-time/v1";
const FRED_SOURCE_ID: &str = "fred-fred-alfred.api-v1-v2";
const FRED_POINT_IN_TIME_POLICY: &str = "latest_known_by_series_as_of_cutoff_v1";
/// Fixed application operation that returns one manifest-pinned latest-known FRED observation.
pub const FRED_ALFRED_READ_OPERATION: &str = "Macro.GetFredAlfredLatestKnown";
const FRED_QUERY_BYTES: u64 = 8 * 1024 * 1024;
const FRED_QUERY_MEMORY_BYTES: u64 = 32 * 1024 * 1024;
const FRED_QUERY_MAXIMUM_DURATION: Duration = Duration::from_secs(60);

/// One exact configured FRED/ALFRED dataset bound to immutable analytical reads.
///
/// Construction retains no credential, network, mutation, ingestion, or publication authority.
/// The exact configured provider dataset crosses the read boundary so a restarted application can
/// query its immutable manifest without recreating a provider runtime or reopening setup.
#[derive(Clone)]
pub struct FredPointInTimeReadCapability {
    reader: AnalyticalReadCapability,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    analytical_dataset: DatasetId,
    series_id: SourceIdentifier,
    provider_namespace: &'static str,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
}

impl std::fmt::Debug for FredPointInTimeReadCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredPointInTimeReadCapability")
            .field("source_id", &self.source_id)
            .field("provider_dataset", &self.provider_dataset)
            .field("analytical_dataset", &self.analytical_dataset)
            .field("series_id", &self.series_id)
            .finish_non_exhaustive()
    }
}

impl FredPointInTimeReadCapability {
    /// Binds the immutable reader to one exact configured observations dataset.
    ///
    /// The application composition caller must obtain `provider_dataset` from the durable desired
    /// activation recipe rather than a Desktop parameter. Source and series identities are
    /// code-owned, and construction does not require a callable provider runtime.
    pub fn try_new(
        reader: AnalyticalReadCapability,
        provider_dataset: SourceIdentifier,
    ) -> Result<Self, FredPointInTimeReadError> {
        let source_id = SourceId::try_from(FRED_SOURCE_ID)
            .map_err(|_| FredPointInTimeReadError::InvalidBinding)?;
        let analytical_identifier = FredSource::analytical_dataset_identifier(&provider_dataset)
            .map_err(|_| FredPointInTimeReadError::InvalidBinding)?;
        let analytical_dataset = DatasetId::try_from(analytical_identifier.as_str())
            .map_err(|_| FredPointInTimeReadError::InvalidBinding)?;
        let series_id = FredSource::rights_subject_identifier(&provider_dataset)
            .map_err(|_| FredPointInTimeReadError::InvalidBinding)?;
        let (realtime_start, realtime_end) =
            FredSource::dataset_realtime_interval(&provider_dataset)
                .map_err(|_| FredPointInTimeReadError::InvalidBinding)?;
        let provider_namespace = provider_namespace(&provider_dataset)?;
        Ok(Self {
            reader,
            source_id,
            provider_dataset,
            analytical_dataset,
            series_id,
            provider_namespace,
            realtime_start,
            realtime_end,
        })
    }

    /// Returns the exact provider discovery identity retained by this application read.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the exact immutable analytical dataset expected by this application read.
    pub const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    /// Reads one latest-known exact-series observation from one immutable manifest.
    ///
    /// The query is code-owned and manifest-pinned. Provider calls, mutable catalog authority,
    /// raw SQL, and physical paths cannot cross this boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "manifest, point-in-time cutoffs, evaluation time, deadline, and cancellation are independent"
    )]
    pub async fn read_latest_known(
        &self,
        manifest: DatasetManifestRef,
        knowledge_cutoff: Timestamp,
        effective_date_cutoff: CalendarDate,
        evaluated_at: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<FredDesktopPointInTimeReadDto, FredPointInTimeReadError> {
        if cancellation.is_cancelled() {
            return Err(FredPointInTimeReadError::Cancelled);
        }
        if manifest.dataset_id() != &self.analytical_dataset || knowledge_cutoff > evaluated_at {
            return Err(FredPointInTimeReadError::InvalidBinding);
        }
        let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(vec![
            self.series_id.clone(),
        ])?;
        let request = AnalyticalMacroLatestKnownRequest::try_new(
            manifest,
            self.source_id.clone(),
            knowledge_cutoff,
            effective_date_cutoff,
            allowlist,
        )?;
        let limits = fred_query_limits(&request, deadline)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(request, limits, deadline, cancellation)
            .await?;
        let [observation] = output.observations() else {
            return Err(FredPointInTimeReadError::InvalidReadResult);
        };
        if output.source_id() != &self.source_id || observation.series() != &self.series_id {
            return Err(FredPointInTimeReadError::InvalidReadResult);
        }
        let observation = desktop_observation(
            observation,
            &self.source_id,
            &self.series_id,
            self.provider_namespace,
            self.realtime_start,
            self.realtime_end,
            knowledge_cutoff,
            effective_date_cutoff,
        )?;
        let pinned = output.output();
        if pinned.manifest().dataset_id() != &self.analytical_dataset {
            return Err(FredPointInTimeReadError::InvalidReadResult);
        }
        Ok(FredDesktopPointInTimeReadDto {
            schema_identity: FRED_DESKTOP_READ_SCHEMA,
            binding: FredDesktopReadBindingDto {
                provider: FredDesktopProviderBindingDto {
                    surface_id: FRED_SURFACE,
                    source_id: self.source_id.as_str().to_owned(),
                    provider_dataset_id: self.provider_dataset.as_str().to_owned(),
                    analytical_dataset_id: self.analytical_dataset.as_str().to_owned(),
                    series_id: self.series_id.as_str().to_owned(),
                },
                manifest: FredDesktopManifestDto {
                    dataset_id: pinned.manifest().dataset_id().as_str().to_owned(),
                    manifest_version: pinned.manifest().manifest_version().to_string(),
                    schema: FredDesktopSchemaDto {
                        name: pinned.manifest().schema().name().to_owned(),
                        version: pinned.manifest().schema().version().get(),
                        fingerprint: encode_hex(pinned.manifest().schema().fingerprint()),
                    },
                    content_hash: encode_hex(pinned.manifest().content_hash().bytes()),
                },
                object_graph_digest: evidence_hex(pinned.object_graph_digest())?,
                query_identity: evidence_hex(pinned.query_identity())?,
                result_digest: evidence_hex(pinned.result_digest())?,
            },
            selection: FredDesktopSelectionDto {
                policy: FRED_POINT_IN_TIME_POLICY,
                knowledge_cutoff: timestamp_text(knowledge_cutoff),
                effective_date_cutoff: effective_date_cutoff.to_string(),
                evaluated_at: timestamp_text(evaluated_at),
                selection_digest: evidence_hex(output.selection_digest())?,
                complete: true,
            },
            observation,
        })
    }
}

fn fred_query_limits(
    request: &AnalyticalMacroLatestKnownRequest,
    deadline: Instant,
) -> Result<QueryLimits, FredPointInTimeReadError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(FredPointInTimeReadError::DeadlineExceeded);
    }
    QueryLimits::try_new_with_inline_bytes(
        request.required_query_rows(),
        FRED_QUERY_BYTES,
        FRED_QUERY_BYTES,
        FRED_QUERY_MEMORY_BYTES,
        2,
        1_024,
        2_048,
        deadline
            .saturating_duration_since(now)
            .min(FRED_QUERY_MAXIMUM_DURATION),
    )
    .map_err(Into::into)
}

#[allow(
    clippy::too_many_arguments,
    reason = "source, provider interval, and both point-in-time cutoffs are independent invariants"
)]
fn desktop_observation(
    observation: &MacroObservation,
    expected_source: &SourceId,
    expected_series: &SourceIdentifier,
    provider_namespace: &str,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
    knowledge_cutoff: Timestamp,
    effective_cutoff: CalendarDate,
) -> Result<FredDesktopObservationDto, FredPointInTimeReadError> {
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    let effective_date = time
        .effective()
        .calendar_date_value()
        .filter(|date| *date <= effective_cutoff)
        .ok_or(FredPointInTimeReadError::InvalidReadResult)?;
    let knowledge_date = timestamp_calendar_date(knowledge_cutoff)?;
    let published_vintage = time
        .published()
        .and_then(|coordinate| coordinate.calendar_date_value())
        .filter(|date| *date >= realtime_start && *date <= realtime_end && *date <= knowledge_date)
        .ok_or(FredPointInTimeReadError::InvalidReadResult)?;
    let superseded_after = time
        .superseded()
        .map(|coordinate| {
            coordinate
                .calendar_date_value()
                .filter(|date| *date > published_vintage)
                .map(|date| date.to_string())
                .ok_or(FredPointInTimeReadError::InvalidReadResult)
        })
        .transpose()?;
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .filter(|available| *available <= knowledge_cutoff)
        .ok_or(FredPointInTimeReadError::InvalidReadResult)?;
    let received_at = provenance.received_at();
    let ingested_at = provenance.ingested_at();
    if observation.series() != expected_series
        || !observation.unit().as_str().starts_with("fred-unit:v1:")
        || provenance.source_id() != expected_source
        || provenance.instrument_id().is_some()
        || provenance.venue_id().is_some()
        || provenance.source_timestamp().is_some()
        || provenance.quality() != DataQuality::OfficialDelayed
        || available_at != received_at
        || received_at > ingested_at
        || received_at > knowledge_cutoff
        || ingested_at > knowledge_cutoff
        || !source_revision_matches(
            provenance.source_identifier(),
            provider_namespace,
            expected_series,
            effective_date,
            published_vintage,
        )
    {
        return Err(FredPointInTimeReadError::InvalidReadResult);
    }
    let raw_page_digest = match provenance.payload_reference() {
        PayloadReference::ContentHash(hash)
            if hash.algorithm() == DigestAlgorithm::Sha256 && hash.digest() != [0; 32] =>
        {
            encode_hex(hash.digest())
        }
        PayloadReference::ContentHash(_) | PayloadReference::SourceReference(_) => {
            return Err(FredPointInTimeReadError::InvalidReadResult);
        }
    };
    let value = match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(decimal), None) => FredDesktopMacroValueDto::Observed {
            decimal: decimal.normalize().to_string(),
        },
        (None, Some(missing)) => FredDesktopMacroValueDto::Missing {
            marker: missing.marker().as_str().to_owned(),
            reason: missing.reason().map(|reason| reason.as_str().to_owned()),
        },
        (Some(_), Some(_)) | (None, None) => {
            return Err(FredPointInTimeReadError::InvalidReadResult);
        }
    };
    Ok(FredDesktopObservationDto {
        series_id: observation.series().as_str().to_owned(),
        unit_id: observation.unit().as_str().to_owned(),
        effective_date: effective_date.to_string(),
        published_vintage: published_vintage.to_string(),
        superseded_after,
        available_at: timestamp_text(available_at),
        received_at: timestamp_text(received_at),
        ingested_at: timestamp_text(ingested_at),
        revision: time.revision().get(),
        value,
        source_identifier: provenance.source_identifier().as_str().to_owned(),
        raw_page_digest,
        quality: "official_delayed",
    })
}

fn provider_namespace(
    provider_dataset: &SourceIdentifier,
) -> Result<&'static str, FredPointInTimeReadError> {
    match provider_dataset.as_str().split(':').next() {
        Some("fred") => Ok("fred"),
        Some("alfred") => Ok("alfred"),
        Some(_) | None => Err(FredPointInTimeReadError::InvalidBinding),
    }
}

fn source_revision_matches(
    source: &SourceIdentifier,
    namespace: &str,
    series: &SourceIdentifier,
    effective: CalendarDate,
    published: CalendarDate,
) -> bool {
    let effective = effective.to_string();
    let published = published.to_string();
    let mut parts = source.as_str().split(':');
    parts.next() == Some(namespace)
        && parts.next() == Some(series.as_str())
        && parts.next() == Some(effective.as_str())
        && parts.next() == Some(published.as_str())
        && parts.next().is_none()
}

fn evidence_hex(digest: EvidenceDigest) -> Result<String, FredPointInTimeReadError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        return Err(FredPointInTimeReadError::InvalidReadResult);
    }
    Ok(encode_hex(digest.bytes()))
}

fn timestamp_text(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn timestamp_calendar_date(timestamp: Timestamp) -> Result<CalendarDate, FredPointInTimeReadError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| FredPointInTimeReadError::InvalidReadResult)?,
        u8::try_from(date.month()).map_err(|_| FredPointInTimeReadError::InvalidReadResult)?,
        u8::try_from(date.day()).map_err(|_| FredPointInTimeReadError::InvalidReadResult)?,
    )
    .map_err(|_| FredPointInTimeReadError::InvalidReadResult)
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Strict Desktop latest-known point-in-time representation for one exact FRED series.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FredDesktopPointInTimeReadDto {
    schema_identity: &'static str,
    binding: FredDesktopReadBindingDto,
    selection: FredDesktopSelectionDto,
    observation: FredDesktopObservationDto,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopProviderBindingDto {
    surface_id: &'static str,
    source_id: String,
    provider_dataset_id: String,
    analytical_dataset_id: String,
    series_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopReadBindingDto {
    provider: FredDesktopProviderBindingDto,
    manifest: FredDesktopManifestDto,
    object_graph_digest: String,
    query_identity: String,
    result_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopManifestDto {
    dataset_id: String,
    manifest_version: String,
    schema: FredDesktopSchemaDto,
    content_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopSchemaDto {
    name: String,
    version: u16,
    fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopSelectionDto {
    policy: &'static str,
    knowledge_cutoff: String,
    effective_date_cutoff: String,
    evaluated_at: String,
    selection_digest: String,
    complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct FredDesktopObservationDto {
    series_id: String,
    unit_id: String,
    effective_date: String,
    published_vintage: String,
    superseded_after: Option<String>,
    available_at: String,
    received_at: String,
    ingested_at: String,
    revision: u32,
    value: FredDesktopMacroValueDto,
    source_identifier: String,
    raw_page_digest: String,
    quality: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum FredDesktopMacroValueDto {
    Observed {
        decimal: String,
    },
    Missing {
        marker: String,
        reason: Option<String>,
    },
}

/// FRED/ALFRED point-in-time read or DTO construction failure.
#[derive(Debug, Error)]
pub enum FredPointInTimeReadError {
    /// Provider runtime, configured dataset, analytical dataset, and manifest do not agree.
    #[error("invalid FRED/ALFRED point-in-time binding")]
    InvalidBinding,
    /// The caller-supplied operation deadline has already elapsed.
    #[error("FRED/ALFRED point-in-time read deadline elapsed")]
    DeadlineExceeded,
    /// The point-in-time request was cancelled before query admission.
    #[error("FRED/ALFRED point-in-time read cancelled")]
    Cancelled,
    /// The immutable query engine rejected the code-owned execution envelope.
    #[error("FRED/ALFRED point-in-time query failed")]
    Query(#[from] QueryError),
    /// The immutable analytical read boundary rejected the request or retained generation.
    #[error("FRED/ALFRED immutable analytical read failed")]
    Analytical(#[from] AnalyticalReadError),
    /// Typed output diverged from the exact series, source, clocks, or evidence requested.
    #[error("invalid FRED/ALFRED point-in-time read result")]
    InvalidReadResult,
}
