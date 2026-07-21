use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    CalendarDate, DataQuality, EffectiveInterval, ExactPayloadEvidence, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthorizationMode, BudgetDecision, CoverageDomain, DiscoveryBatch, DiscoveryRequest,
    ExtractionBatch, ExtractionRequest, ExtractionSource, ExtractionSourceError,
    HistoricalCapability, NetworkAccessPolicy, RegisteredSource, SharedProviderBudget, SourceClass,
    SourceError, SourceMetadata, SourceMetadataProvider, SourceObject, apply_http_retry_after,
    payload_matches_exact_evidence,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

use crate::series::parse_date;
use crate::{
    FredObservationPage, FredOperation, FredParseLimits, FredRightsDisposition, FredRightsPolicy,
};

mod http;
mod lineage;

use http::{
    FredHttpRequest, FredHttpResponse, FredTransport, ReqwestFredTransport, system_timestamp,
};
use lineage::{evidence_for_payload, map_adapter_error, page_object_id, parse_object_id};

const OBSERVATIONS_ENDPOINT: &str = "https://api.stlouisfed.org/fred/series/observations";
const DISCOVERY_PAGE_RECORDS: usize = 10_000;

/// User-owned FRED API credential retained only in zeroizing memory.
#[derive(Clone)]
pub struct FredApiKey(Zeroizing<String>);

impl FredApiKey {
    /// Validates the documented 32-character lower-case alphanumeric key shape.
    pub fn try_new(value: String) -> Result<Self, FredSourceError> {
        if value.len() != 32
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit())
        {
            return Err(FredSourceError::InvalidApiKey);
        }
        Ok(Self(Zeroizing::new(value)))
    }

    fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for FredApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FredApiKey([REDACTED])")
    }
}

/// FRED adapter configuration or protocol failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FredSourceError {
    /// API keys must match the provider's documented exact shape.
    InvalidApiKey,
    /// Dataset identity is outside the bounded FRED/ALFRED observation grammar.
    InvalidDataset,
    /// Provider response crossed the configured byte ceiling.
    BodyTooLarge,
    /// The allowlisted transport failed without retaining sensitive request data.
    Network,
    /// Provider data or canonical normalization violated its exact schema.
    Protocol,
    /// Source metadata or registry authority does not match this adapter.
    InvalidConfiguration,
    /// The bounded operation elapsed before completion.
    DeadlineExceeded,
    /// Caller cancellation interrupted the operation.
    Cancelled,
}

impl std::fmt::Display for FredSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidApiKey => formatter.write_str("invalid FRED API key"),
            Self::InvalidDataset => formatter.write_str("invalid FRED/ALFRED dataset identity"),
            Self::BodyTooLarge => formatter.write_str("FRED response exceeded its byte limit"),
            Self::Network => formatter.write_str("FRED network operation failed"),
            Self::Protocol => formatter.write_str("invalid FRED protocol data"),
            Self::InvalidConfiguration => formatter.write_str("invalid FRED source configuration"),
            Self::DeadlineExceeded => formatter.write_str("FRED operation deadline elapsed"),
            Self::Cancelled => formatter.write_str("FRED operation was cancelled"),
        }
    }
}

impl std::error::Error for FredSourceError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FredNamespace {
    Fred,
    Alfred,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FredDataset {
    namespace: FredNamespace,
    series_id: String,
    realtime_start: CalendarDate,
    realtime_end: CalendarDate,
}

impl FredDataset {
    fn parse(value: &SourceIdentifier) -> Result<Self, FredSourceError> {
        let mut fields = value.as_str().split(':');
        let namespace = match fields.next() {
            Some("fred") => FredNamespace::Fred,
            Some("alfred") => FredNamespace::Alfred,
            _ => return Err(FredSourceError::InvalidDataset),
        };
        if fields.next() != Some("series-observations") {
            return Err(FredSourceError::InvalidDataset);
        }
        let series_id = fields.next().ok_or(FredSourceError::InvalidDataset)?;
        let realtime_start = fields.next().ok_or(FredSourceError::InvalidDataset)?;
        let realtime_end = fields.next().ok_or(FredSourceError::InvalidDataset)?;
        if fields.next().is_some()
            || series_id.is_empty()
            || series_id.len() > 120
            || series_id
                .bytes()
                .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(FredSourceError::InvalidDataset);
        }
        let realtime_start =
            parse_date(realtime_start).map_err(|_| FredSourceError::InvalidDataset)?;
        let realtime_end = parse_date(realtime_end).map_err(|_| FredSourceError::InvalidDataset)?;
        if realtime_start > realtime_end {
            return Err(FredSourceError::InvalidDataset);
        }
        Ok(Self {
            namespace,
            series_id: series_id.to_owned(),
            realtime_start,
            realtime_end,
        })
    }

    fn series_id(&self) -> &str {
        &self.series_id
    }

    const fn realtime_start(&self) -> CalendarDate {
        self.realtime_start
    }

    const fn realtime_end(&self) -> CalendarDate {
        self.realtime_end
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalFredObservation<'a> {
    schema_version: u16,
    provider_namespace: &'static str,
    series_id: &'a str,
    observation_date: String,
    realtime_start: String,
    realtime_end: String,
    raw_value: &'a str,
    value: Option<&'a str>,
    missing: bool,
    received_at_unix_nanos: i64,
    availability: CanonicalAvailability,
    quality: &'static str,
    coverage: &'static str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct CanonicalAvailability {
    kind: &'static str,
    observed_at_unix_nanos: i64,
}

fn canonical_payloads(
    dataset: &FredDataset,
    page: &crate::FredObservationPage,
    received_at: Timestamp,
) -> Result<Vec<Bytes>, FredSourceError> {
    page.observations()
        .iter()
        .map(|observation| {
            let missing = observation.value().is_none();
            serde_json::to_vec(&CanonicalFredObservation {
                schema_version: 1,
                provider_namespace: match dataset.namespace {
                    FredNamespace::Fred => "fred",
                    FredNamespace::Alfred => "alfred",
                },
                series_id: dataset.series_id(),
                observation_date: observation.observation_date().to_string(),
                realtime_start: observation.realtime_start().to_string(),
                realtime_end: observation.realtime_end().to_string(),
                raw_value: observation.raw_value(),
                value: (!missing).then_some(observation.raw_value()),
                missing,
                received_at_unix_nanos: received_at.unix_nanos(),
                availability: CanonicalAvailability {
                    kind: "local_first_observed",
                    observed_at_unix_nanos: received_at.unix_nanos(),
                },
                quality: "official_delayed",
                coverage: "macroeconomic",
            })
            .map(Bytes::from)
            .map_err(|_| FredSourceError::Protocol)
        })
        .collect()
}

/// One exact, request-bound page retrieved for ephemeral inspection.
#[derive(Clone, Debug)]
pub struct FredExtractedPage {
    page_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    canonical_payloads: Vec<Bytes>,
}

impl FredExtractedPage {
    /// Returns exact evidence for the provider page bytes.
    pub const fn page_evidence(&self) -> &ExactPayloadEvidence {
        &self.page_evidence
    }

    /// Returns when this process completed receipt of the exact page.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns canonical observations retaining exact civil-date semantics.
    pub fn canonical_payloads(&self) -> &[Bytes] {
        &self.canonical_payloads
    }
}

/// Registry-bound FRED and ALFRED extraction source.
///
/// Discovery and [`Self::extract_page_ephemeral`] preserve provider civil dates without inventing
/// an instant. The common durable [`ExtractionSource::extract`] entry point fails closed until the
/// shared record contract can represent civil-date effective time and durable rights are granted.
pub struct FredSource {
    metadata: SourceMetadata,
    budget: SharedProviderBudget,
    api_key: FredApiKey,
    rights: FredRightsPolicy,
    transport: Arc<dyn FredTransport>,
    response_limit: usize,
    request_timeout: Duration,
    discovery_page_records: usize,
}

impl std::fmt::Debug for FredSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredSource")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("api_key", &"[REDACTED]")
            .field("response_limit", &self.response_limit)
            .finish_non_exhaustive()
    }
}

impl FredSource {
    /// Builds a production HTTP source from exact registry-issued budget authority.
    pub fn try_new(
        metadata: SourceMetadata,
        registered: &RegisteredSource,
        api_key: FredApiKey,
        rights: FredRightsPolicy,
    ) -> Result<Self, FredSourceError> {
        let bounds = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.request_bounds(),
            NetworkAccessPolicy::Denied => return Err(FredSourceError::InvalidConfiguration),
        };
        let transport = Arc::new(ReqwestFredTransport::try_new(bounds)?);
        Self::try_new_with_transport(
            metadata,
            registered,
            api_key,
            rights,
            transport,
            DISCOVERY_PAGE_RECORDS,
        )
    }

    fn try_new_with_transport(
        metadata: SourceMetadata,
        registered: &RegisteredSource,
        api_key: FredApiKey,
        rights: FredRightsPolicy,
        transport: Arc<dyn FredTransport>,
        discovery_page_records: usize,
    ) -> Result<Self, FredSourceError> {
        if metadata.source_id() != registered.source_id()
            || metadata.revision() != registered.revision()
            || metadata.source_class() != SourceClass::OfficialAgency
            || metadata.provider().as_str() != "fred"
            || metadata.authorization().mode() != AuthorizationMode::UserAuthorized
            || metadata.coverage().domain() != CoverageDomain::Macroeconomic
            || metadata.quality_ceiling() != DataQuality::OfficialDelayed
            || !metadata.capabilities().extraction()
            || metadata.capabilities().historical() != HistoricalCapability::RevisionPreserving
        {
            return Err(FredSourceError::InvalidConfiguration);
        }
        let policy = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy,
            NetworkAccessPolicy::Denied => return Err(FredSourceError::InvalidConfiguration),
        };
        let budget = registered
            .budget()
            .cloned()
            .ok_or(FredSourceError::InvalidConfiguration)?;
        let bounds = policy.request_bounds();
        if discovery_page_records == 0 || discovery_page_records > 100_000 {
            return Err(FredSourceError::InvalidConfiguration);
        }
        let response_limit = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| FredSourceError::InvalidConfiguration)?;
        Ok(Self {
            metadata,
            budget,
            api_key,
            rights,
            transport,
            response_limit,
            request_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
            discovery_page_records,
        })
    }

    /// Refetches and verifies one exact page for ephemeral inspection only.
    pub async fn extract_page_ephemeral(
        &self,
        request: &ExtractionRequest,
        cancellation: CancellationToken,
    ) -> Result<FredExtractedPage, ExtractionSourceError> {
        if request.object().source_id() != self.metadata.source_id()
            || request.object().metadata_revision() != self.metadata.revision()
        {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let dataset = FredDataset::parse(request.object().dataset())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let (offset, limit, expected_digest) = parse_object_id(request.object().object_id())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let fetched = self
            .fetch_page(&dataset, offset, limit, request.deadline(), cancellation)
            .await?;
        if fetched.digest != expected_digest
            || !payload_matches_exact_evidence(&fetched.response.body, request.object().evidence())
            || request
                .object()
                .expected_bytes()
                .is_some_and(|expected| expected != fetched.response.body.len() as u64)
        {
            return Err(ExtractionSourceError::Source(
                SourceError::GenerationResynchronizationRequired,
            ));
        }
        if fetched.page.observations().len() > request.max_records() as usize {
            return Err(ExtractionSourceError::Contract(
                market_squawk_sources::ExtractionError::RecordLimitExceeded {
                    requested: request.max_records(),
                },
            ));
        }
        let canonical_payloads =
            canonical_payloads(&dataset, &fetched.page, fetched.response.received_at)
                .map_err(map_adapter_error)?;
        let total = canonical_payloads.iter().try_fold(0_u64, |total, payload| {
            u64::try_from(payload.len())
                .ok()
                .and_then(|bytes| total.checked_add(bytes))
        });
        if total.is_none_or(|total| total > request.max_bytes()) {
            return Err(ExtractionSourceError::Contract(
                market_squawk_sources::ExtractionError::ByteLimitExceeded {
                    requested: request.max_bytes(),
                },
            ));
        }
        Ok(FredExtractedPage {
            page_evidence: evidence_for_payload(&fetched.response.body, &fetched.public_url)
                .map_err(map_adapter_error)?,
            received_at: fetched.response.received_at,
            canonical_payloads,
        })
    }

    async fn discover_impl(
        &self,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<DiscoveryBatch, ExtractionSourceError> {
        if request.effective_at().is_some() {
            return Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ));
        }
        let dataset = FredDataset::parse(request.dataset())
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let mut objects = Vec::new();
        let mut offset = 0_usize;
        let mut expected_count = None;
        while objects.len() < usize::from(request.max_results()) {
            let fetched = self
                .fetch_page(
                    &dataset,
                    offset,
                    self.discovery_page_records,
                    request.deadline(),
                    cancellation.clone(),
                )
                .await?;
            if fetched.page.offset() != offset
                || fetched.page.limit() != self.discovery_page_records
                || fetched.page.realtime_start() != dataset.realtime_start()
                || fetched.page.realtime_end() != dataset.realtime_end()
                || expected_count.is_some_and(|count| count != fetched.page.count())
            {
                return Err(ExtractionSourceError::Source(
                    SourceError::InvalidProtocolState,
                ));
            }
            expected_count = Some(fetched.page.count());
            let evidence = evidence_for_payload(&fetched.response.body, &fetched.public_url)
                .map_err(map_adapter_error)?;
            let object_id = page_object_id(offset, self.discovery_page_records, fetched.digest)
                .map_err(map_adapter_error)?;
            let effective = EffectiveInterval::new(fetched.response.received_at, None)
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
            objects.push(SourceObject::try_new(
                self.metadata.source_id().clone(),
                self.metadata.revision().clone(),
                &request,
                object_id,
                SourceIdentifier::try_from("application/vnd.market-squawk.fred-page+json")
                    .map_err(|_| {
                        ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                    })?,
                evidence,
                effective,
                None,
                Some(u64::try_from(fetched.response.body.len()).map_err(|_| {
                    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                })?),
            )?);
            let Some(next) = fetched.page.next_offset() else {
                break;
            };
            offset = next;
        }
        DiscoveryBatch::try_new(&request, objects).map_err(ExtractionSourceError::from)
    }

    async fn fetch_page(
        &self,
        dataset: &FredDataset,
        offset: usize,
        limit: usize,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FetchedPage, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let now = system_timestamp().map_err(map_adapter_error)?;
        if deadline <= now {
            return Err(ExtractionSourceError::DeadlineExceeded);
        }
        let rights = self
            .rights
            .assess(
                &SourceIdentifier::try_from(dataset.series_id()).map_err(|_| {
                    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
                })?,
                &[FredOperation::RetrieveEphemeral],
                now,
            )
            .map_err(|_| ExtractionSourceError::Source(SourceError::Unauthorized))?;
        if rights.disposition() != FredRightsDisposition::Permitted {
            return Err(ExtractionSourceError::Source(SourceError::Unauthorized));
        }
        let mut public_url = url::Url::parse(OBSERVATIONS_ENDPOINT)
            .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        public_url
            .query_pairs_mut()
            .append_pair("series_id", dataset.series_id())
            .append_pair("realtime_start", &dataset.realtime_start().to_string())
            .append_pair("realtime_end", &dataset.realtime_end().to_string())
            .append_pair("limit", &limit.to_string())
            .append_pair("offset", &offset.to_string())
            .append_pair("sort_order", "asc")
            .append_pair("order_by", "observation_date")
            .append_pair("output_type", "1")
            .append_pair("file_type", "json");
        let mut authorization_target = public_url.clone();
        authorization_target
            .query_pairs_mut()
            .append_pair("api_key", self.api_key.expose());
        self.metadata
            .network_policy()
            .authorize(authorization_target.as_str())
            .map_err(|_| ExtractionSourceError::Source(SourceError::Network))?;
        drop(authorization_target);

        let _permit = match self.budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            BudgetDecision::WaitUntil(deadline) => {
                return Err(ExtractionSourceError::Source(
                    SourceError::BudgetWaitUntil { deadline },
                ));
            }
            BudgetDecision::Unavailable(reason) => {
                return Err(ExtractionSourceError::Source(
                    SourceError::BudgetUnavailable { reason },
                ));
            }
        };
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
                },
                self.response_limit,
                timeout,
                cancellation,
            )
            .await
            .map_err(map_adapter_error)?;
        match response.status {
            200 => {}
            401 | 403 => return Err(ExtractionSourceError::Source(SourceError::Unauthorized)),
            429 => {
                let decision =
                    apply_http_retry_after(&self.budget, response.retry_after.as_deref(), 0);
                return Err(ExtractionSourceError::Source(
                    SourceError::from_applied_budget_refusal(decision),
                ));
            }
            _ => return Err(ExtractionSourceError::Source(SourceError::Network)),
        }
        let page = FredObservationPage::parse(
            &response.body,
            FredParseLimits::try_new(limit, self.response_limit, 8 * 1024)
                .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?,
        )
        .map_err(|_| ExtractionSourceError::Source(SourceError::InvalidProtocolState))?;
        let digest = Sha256::digest(&response.body).into();
        Ok(FetchedPage {
            response,
            page,
            digest,
            public_url,
        })
    }
}

impl SourceMetadataProvider for FredSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl ExtractionSource for FredSource {
    fn discover(
        &self,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(self.discover_impl(request, cancellation))
    }

    fn extract(
        &self,
        _request: ExtractionRequest,
        _cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        Box::pin(async {
            Err(ExtractionSourceError::Source(
                SourceError::InvalidProtocolState,
            ))
        })
    }
}

struct FetchedPage {
    response: FredHttpResponse,
    page: FredObservationPage,
    digest: [u8; 32],
    public_url: url::Url,
}

#[cfg(test)]
#[path = "client/tests.rs"]
mod tests;
