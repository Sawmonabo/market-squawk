//! Reads actual returned market-session entries from their original analytical publication.
use crate::application::market_runtime::MarketRuntimeRegistry;
use crate::{ResearchService, ResearchServiceError};
use chrono::{Datelike, NaiveDate};
use market_squawk_data::{
    AnalyticalObservationReadRequest, AnalyticalObservationTemplate, CommittedDataset,
    DatasetManifestRef, QueryLimits, QueryResult, ResearchArrowBatch, Sha256Digest,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN,
    MarketCalendarCompleteness, MarketCalendarObservation, MarketCalendarPayload,
    ResearchObservation, Timestamp,
};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSessionProduct {
    Equity,
    Option,
    Bond,
    Future,
    Forex,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarketSessionContextRequest {
    product: MarketSessionProduct,
    date: NaiveDate,
}
impl MarketSessionContextRequest {
    pub fn product(&self) -> MarketSessionProduct {
        self.product
    }
    pub fn date(&self) -> NaiveDate {
        self.date
    }
    fn calendar_date(&self) -> Result<CalendarDate, ServiceError> {
        CalendarDate::new(
            u16::try_from(self.date.year()).map_err(|_| ServiceError::InvalidRequest)?,
            self.date.month() as u8,
            self.date.day() as u8,
        )
        .map_err(|_| ServiceError::InvalidRequest)
    }
}
/// Inert commitments only. Reopening verifies the original source request before projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarketSessionContextReference {
    request: MarketSessionContextRequest,
    origin_content_sha256: String,
    capture_binding_sha256: String,
}
pub struct MarketSessionContext {
    reference: MarketSessionContextReference,
    rows: Vec<MarketCalendarObservation>,
}
impl MarketSessionContext {
    /// Financial projection excludes native provider identifiers. Each entry is an actual returned
    /// product; neither omission nor absent session windows establishes closure.
    pub fn projection(&self) -> Result<serde_json::Value, ServiceError> {
        let entries: Vec<_> = self
            .rows
            .chunks_exact(2)
            .enumerate()
            .map(|(index, pair)| {
                let MarketCalendarPayload::SessionDay { day } = pair[1].payload() else {
                    unreachable!()
                };
                let windows: Vec<_> = day
                    .intervals()
                    .iter()
                    .map(|window| {
                        serde_json::json!({
                            "role":window.role,"ordinal":window.native_ordinal,
                            "startUnixNanos":window.start.at().unix_nanos().to_string(),
                            "endUnixNanos":window.end.at().unix_nanos().to_string(),
                            "startUtcOffsetSeconds":window.start.utc_offset_seconds(),
                            "endUtcOffsetSeconds":window.end.utc_offset_seconds()
                        })
                    })
                    .collect();
                serde_json::json!({"entry":index+1,"status":day.status(),
                "sessionPresence":day.session_presence(),"windows":windows})
            })
            .collect();
        Ok(
            serde_json::json!({"reference":self.reference,"product":self.reference.request.product,
            "date":self.reference.request.date,"coverage":"returned_entries_only","entries":entries}),
        )
    }
    pub fn entry_count(&self) -> usize {
        self.rows.len() / 2
    }
}
#[derive(Clone)]
pub struct MarketSessionContextReadCapability {
    research: Arc<ResearchService>,
    runtime: Arc<MarketRuntimeRegistry>,
}
impl MarketSessionContextReadCapability {
    pub fn new(research: Arc<ResearchService>, runtime: Arc<MarketRuntimeRegistry>) -> Self {
        Self { research, runtime }
    }
    pub async fn read_committed(
        &self,
        request: &MarketSessionContextRequest,
        committed: &CommittedDataset,
        binding: EvidenceDigest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketSessionContext, ServiceError> {
        self.read_manifest(
            request,
            committed.manifest(),
            binding,
            deadline,
            cancellation,
        )
        .await
    }
    pub async fn read_reference(
        &self,
        reference: &MarketSessionContextReference,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketSessionContext, ServiceError> {
        check(deadline, &cancellation)?;
        let binding = decode(&reference.capture_binding_sha256)?;
        let content = decode(&reference.origin_content_sha256)?;
        let manifest = self
            .research
            .analytical_reader()
            .provider_capture_origin(
                binding,
                Sha256Digest::new(content.bytes()),
                now()?,
                deadline,
                &cancellation,
            )
            .map_err(|_| controlled(deadline, &cancellation))?
            .ok_or(ServiceError::Unavailable)?;
        let output = self
            .read_manifest(
                &reference.request,
                &manifest,
                binding,
                deadline,
                cancellation,
            )
            .await?;
        if output.reference != *reference {
            return Err(ServiceError::InvalidResult);
        }
        Ok(output)
    }
    async fn read_manifest(
        &self,
        request: &MarketSessionContextRequest,
        manifest: &DatasetManifestRef,
        expected_binding: EvidenceDigest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketSessionContext, ServiceError> {
        check(deadline, &cancellation)?;
        let date = request.calendar_date()?;
        self.runtime
            .verify_market_session_context_reference(
                request,
                manifest,
                expected_binding,
                deadline,
                cancellation.clone(),
            )
            .await?;
        let as_of = now()?;
        let (binding, published_at) = self
            .research
            .read_provider_capture_generation(
                manifest.clone(),
                deadline,
                &cancellation,
                move |generation, _, _, _, _| {
                    if generation.published_at() > as_of
                        || generation.objects().len() != 1
                        || generation.objects()[0].inputs().len() != 1
                    {
                        return Err(ResearchServiceError::IngestAuthorityMismatch);
                    }
                    let binding = generation.objects()[0].inputs()[0].binding();
                    if binding.binding_digest() != expected_binding
                        || binding.record_count() == 0
                        || binding.record_count() > 128
                        || binding.record_count() % 2 != 0
                    {
                        return Err(ResearchServiceError::IngestAuthorityMismatch);
                    }
                    Ok((binding.clone(), generation.published_at()))
                },
            )
            .await
            .map_err(|_| controlled(deadline, &cancellation))?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ServiceError::DeadlineExceeded)?;
        let limits = QueryLimits::try_new_with_inline_bytes(
            128,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            64 * 1024 * 1024,
            2,
            512,
            512,
            remaining.min(Duration::from_secs(60)),
        )
        .map_err(|_| ServiceError::ResourceExhausted)?;
        let read = AnalyticalObservationReadRequest::try_new(
            manifest.clone(),
            AnalyticalObservationTemplate::MarketCalendar,
            Vec::new(),
            None,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let output = self
            .research
            .analytical_reader()
            .read_observations(read, limits, deadline, cancellation.clone())
            .await
            .map_err(|_| controlled(deadline, &cancellation))?;
        let QueryResult::Inline { batches, .. } = output.output().result() else {
            return Err(ServiceError::ResourceExhausted);
        };
        let mut bytes = 8 * 1024 * 1024usize;
        let mut rows = Vec::new();
        let control = ReadControl {
            deadline,
            cancellation: &cancellation,
        };
        for batch in batches {
            check(deadline, &cancellation)?;
            let (records, retained) =
                ResearchArrowBatch::decode_query_capture_binding_rows_bounded(
                    batch.clone(),
                    &binding,
                    bytes,
                    &control,
                )
                .map_err(|_| controlled(deadline, &cancellation))?;
            bytes = bytes
                .checked_sub(retained)
                .ok_or(ServiceError::ResourceExhausted)?;
            for (ordinal, record) in records {
                let ResearchObservation::MarketCalendar(row) = record else {
                    return Err(ServiceError::InvalidResult);
                };
                if rows.len() >= binding.record_count() {
                    return Err(ServiceError::ResourceExhausted);
                }
                rows.push((ordinal, row));
            }
        }
        rows.sort_unstable_by_key(|(ordinal, _)| *ordinal);
        if rows.len() != binding.record_count()
            || rows
                .iter()
                .enumerate()
                .any(|(index, (ordinal, _))| usize::try_from(*ordinal).ok() != Some(index))
        {
            return Err(ServiceError::InvalidResult);
        }
        let rows: Vec<_> = rows.into_iter().map(|(_, row)| row).collect();
        let mut scopes = Vec::new();
        for pair in rows.chunks_exact(2) {
            let MarketCalendarPayload::Coverage {
                completeness,
                reported_day_count,
                reported_days_digest,
                ..
            } = pair[0].payload()
            else {
                return Err(ServiceError::InvalidResult);
            };
            let MarketCalendarPayload::SessionDay { day } = pair[1].payload() else {
                return Err(ServiceError::InvalidResult);
            };
            if *completeness != MarketCalendarCompleteness::ReturnedEntriesOnly
                || *reported_day_count != 1
                || *reported_days_digest != date_digest(date)
                || day.date() != date
                || pair[0].scope() != pair[1].scope()
                || pair[0].scope().date_scope.start_date() != date
                || pair[0].scope().date_scope.end_date() != date
                || scopes.contains(&pair[0].scope())
            {
                return Err(ServiceError::InvalidResult);
            }
            scopes.push(pair[0].scope());
            for row in pair {
                let provenance = row.context().provenance();
                if row.observed_at() > as_of
                    || provenance.received_at() > as_of
                    || provenance.ingested_at() > as_of
                    || provenance.ingested_at() > published_at
                    || provenance
                        .availability()
                        .conservative_available_at()
                        .is_none_or(|at| at > as_of)
                {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        check(deadline, &cancellation)?;
        Ok(MarketSessionContext {
            reference: MarketSessionContextReference {
                request: request.clone(),
                origin_content_sha256: encode(manifest.content_hash().bytes()),
                capture_binding_sha256: encode(expected_binding.bytes()),
            },
            rows,
        })
    }
}
fn date_digest(date: CalendarDate) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(MARKET_CALENDAR_DATE_MEMBERSHIP_DOMAIN);
    hash.update(1u32.to_be_bytes());
    hash.update(date.year().to_be_bytes());
    hash.update([date.month(), date.day()]);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}
fn encode(bytes: [u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode(value: &str) -> Result<EvidenceDigest, ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ServiceError::InvalidRequest)?;
    }
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}
fn now() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Internal)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| ServiceError::Internal)?,
    ))
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
fn controlled(deadline: Instant, cancellation: &CancellationToken) -> ServiceError {
    check(deadline, cancellation)
        .err()
        .unwrap_or(ServiceError::InvalidResult)
}
struct ReadControl<'a> {
    deadline: Instant,
    cancellation: &'a CancellationToken,
}
impl market_squawk_platform::ResearchObjectControl for ReadControl<'_> {
    fn checkpoint(
        &self,
        _: market_squawk_platform::ResearchObjectControlPoint,
    ) -> Result<(), market_squawk_platform::ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(market_squawk_platform::ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(market_squawk_platform::ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}
