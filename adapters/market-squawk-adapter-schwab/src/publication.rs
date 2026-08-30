//! Seal-first Schwab REST publication boundaries.

use std::{num::NonZeroU64, sync::Arc};

use bytes::Bytes;
use market_squawk_domain::{
    AvailabilityEvidence as ResearchAvailabilityEvidence, DataQuality, DigestAlgorithm,
    EvidenceDigest, ExactPayloadEvidence, Money, PayloadHash, PayloadReference, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
    SourceId, SourceIdentifier, VenueId,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch, ExtractionRecord,
    ExtractionRequest, ExtractionRevisionPlan, ProviderCaptureSealRequest,
    ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageImplementation, SealedProviderCaptureBinding,
    SealedProviderCaptureMaterial,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

const SCHWAB_DAILY_INTERVAL: &str = "1d";

use crate::canonical::{
    SchwabDailyPriceHistoryCandidateRequest, SchwabPendingPriceHistoryCandidate,
    prepare_price_history_candidate,
};
use crate::transport::SchwabSealedRestResponseParts;
use crate::{
    ExecutedRestResponse, NativeField, NativeNumber, PriceHistoryResponse, ReadOnlyRoute,
    SchwabCanonicalError, SchwabCaptureCoordinates, SchwabOAuthAuthorityReceipt,
    SchwabPriceHistoryCapabilityObservation, SchwabResolvedProviderIdentity,
    SchwabRestCaptureSealRejoin, SchwabRestPayload, SchwabTransportError,
    SchwabUserPreferenceEvidence,
};
use market_squawk_domain::{
    BarTimeSemantics, Currency, InstrumentId, MarketBarAdjustment, MarketBarObservation,
    ProviderInstrumentId, Timestamp,
};

/// Exact delivery-delay evidence attached to a Schwab REST market-data response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "nanoseconds")]
pub enum SchwabRestDelayEvidence {
    /// The current provider evidence explicitly identifies real-time delivery.
    RealTime,
    /// The current provider evidence identifies a positive delivery delay.
    Delayed(NonZeroU64),
    /// The current provider evidence does not establish a numeric delay.
    Unknown,
}

/// Provider/feed/venue/delay evidence for one Schwab daily price-history response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabPriceHistoryMarketDataEvidence {
    venue_id: VenueId,
    feed: SourceIdentifier,
    delay: SchwabRestDelayEvidence,
    qualification_evidence: EvidenceDigest,
}

impl SchwabPriceHistoryMarketDataEvidence {
    /// Constructs explicit market-data evidence. The REST service and route remain code-owned.
    pub fn try_new(
        venue_id: VenueId,
        feed: SourceIdentifier,
        delay: SchwabRestDelayEvidence,
        qualification_evidence: EvidenceDigest,
    ) -> Result<Self, SchwabPriceHistoryPublicationError> {
        if qualification_evidence.algorithm() != DigestAlgorithm::Sha256
            || qualification_evidence.bytes() == [0; 32]
        {
            return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
        }
        Ok(Self {
            venue_id,
            feed,
            delay,
            qualification_evidence,
        })
    }

    /// Exact venue represented by the canonical bars.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Exact provider feed label retained on the canonical bars.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Explicit provider delay state, including honest unknown evidence.
    pub const fn delay(&self) -> SchwabRestDelayEvidence {
        self.delay
    }

    /// Exact external evidence binding the supplied feed, venue, and delay declaration.
    pub const fn qualification_evidence(&self) -> EvidenceDigest {
        self.qualification_evidence
    }

    /// Code-owned REST service identity.
    pub const fn service(&self) -> &'static str {
        "schwab-market-data-rest"
    }

    /// Code-owned read-only route family.
    pub const fn route(&self) -> ReadOnlyRoute {
        ReadOnlyRoute::PriceHistory
    }
}

/// Read-only application-calendar capability for one exact completed daily-history range.
///
/// The adapter deliberately cannot mint this receipt. Its application owner must derive it from
/// retained, current completed-session calendar authority. Publication consumes the exact ordered
/// period set and revalidates the receipt immediately before canonical mapping.
pub trait SchwabDailyPriceHistoryCalendarRangeReceipt: std::fmt::Debug + Send + Sync {
    /// Exact Schwab source generation that owns publication.
    fn publication_source_id(&self) -> &SourceId;

    /// Canonical instrument expected in every returned candle.
    fn instrument_id(&self) -> InstrumentId;

    /// Exact canonical instrument-definition revision admitted by the plan.
    fn instrument_revision_digest(&self) -> EvidenceDigest;

    /// Exact immutable application plan that admitted the request.
    fn admitted_plan_digest(&self) -> EvidenceDigest;

    /// SHA-256 identity of the exact admitted Schwab REST request bytes.
    fn provider_request_digest(&self) -> EvidenceDigest;

    /// Exact venue whose completed sessions were selected.
    fn venue_id(&self) -> &VenueId;

    /// Exact code-owned daily interval retained by the calendar receipt.
    fn interval(&self) -> &SourceIdentifier;

    /// Exact code-owned raw adjustment semantics retained by the calendar receipt.
    fn adjustment(&self) -> MarketBarAdjustment;

    /// Exact half-open start admitted by the calendar selection.
    fn requested_start(&self) -> Timestamp;

    /// Exact half-open end admitted by the calendar selection.
    fn requested_end(&self) -> Timestamp;

    /// Point-in-time knowledge cutoff used by the selection.
    fn knowledge_cutoff(&self) -> Timestamp;

    /// Trusted instant at which the receipt was minted.
    fn evaluated_at(&self) -> Timestamp;

    /// Exclusive expiry bounded by calendar and currentness evidence.
    fn expires_at(&self) -> Timestamp;

    /// Exact provider/calendar evidence that proves the enumerated range is complete.
    fn completeness_evidence(&self) -> EvidenceDigest;

    /// Exact calendar ruleset evidence shared by every returned period.
    fn calendar_evidence(&self) -> EvidenceDigest;

    /// Nonzero digest binding the complete range selection and its retained capture.
    fn receipt_digest(&self) -> EvidenceDigest;

    /// Every expected daily period in strict provider-timestamp order.
    fn periods(&self) -> &[BarTimeSemantics];

    /// Revalidates the exact retained generation/revocation identity at publication time.
    fn validate_current_at(&self, checked_at: Timestamp) -> bool;
}

/// Semantic and application-lineage inputs for one accepted daily price-history response.
#[derive(Debug)]
pub struct SchwabDailyPriceHistoryPublicationRequest<'a> {
    capability: SchwabPriceHistoryCapabilityObservation,
    oauth_authority: SchwabOAuthAuthorityReceipt,
    user_preference: &'a SchwabUserPreferenceEvidence,
    extraction_request: ExtractionRequest,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    identity: SchwabResolvedProviderIdentity,
    market_data: SchwabPriceHistoryMarketDataEvidence,
    currency: Currency,
    calendar_range: Arc<dyn SchwabDailyPriceHistoryCalendarRangeReceipt>,
    ingested_at: Timestamp,
}

impl<'a> SchwabDailyPriceHistoryPublicationRequest<'a> {
    /// Constructs the complete semantic input. Validation occurs against the consumed response.
    #[allow(
        clippy::too_many_arguments,
        reason = "PIT, identity, and market evidence remain explicit"
    )]
    pub fn new(
        capability: SchwabPriceHistoryCapabilityObservation,
        oauth_authority: SchwabOAuthAuthorityReceipt,
        user_preference: &'a SchwabUserPreferenceEvidence,
        extraction_request: ExtractionRequest,
        instrument_id: InstrumentId,
        instrument_revision_digest: EvidenceDigest,
        admitted_plan_digest: EvidenceDigest,
        identity: SchwabResolvedProviderIdentity,
        market_data: SchwabPriceHistoryMarketDataEvidence,
        currency: Currency,
        calendar_range: Arc<dyn SchwabDailyPriceHistoryCalendarRangeReceipt>,
        ingested_at: Timestamp,
    ) -> Self {
        Self {
            capability,
            oauth_authority,
            user_preference,
            extraction_request,
            instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            identity,
            market_data,
            currency,
            calendar_range,
            ingested_at,
        }
    }
}

/// One accepted typed daily-history response awaiting the application-owned physical seal.
pub struct SchwabPendingDailyPriceHistoryPublication {
    rejoin: SchwabRestCaptureSealRejoin,
    candidate: SchwabPendingPriceHistoryCandidate,
    extraction_request: ExtractionRequest,
    market_data: SchwabPriceHistoryMarketDataEvidence,
    calendar_range: Arc<dyn SchwabDailyPriceHistoryCalendarRangeReceipt>,
}

impl std::fmt::Debug for SchwabPendingDailyPriceHistoryPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabPendingDailyPriceHistoryPublication")
            .field("rejoin", &self.rejoin)
            .field("market_data", &self.market_data)
            .field("canonical_records", &self.candidate.bars.len())
            .field("raw_body", &"AWAITING COMMON PHYSICAL SEAL")
            .finish()
    }
}

impl ExecutedRestResponse {
    /// Consumes one typed daily-history response into the sole raw seal request and opaque rejoin.
    pub fn into_pending_daily_price_history_publication(
        self,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
        request: SchwabDailyPriceHistoryPublicationRequest<'_>,
    ) -> Result<
        (
            SchwabPendingDailyPriceHistoryPublication,
            ProviderCaptureSealRequest,
        ),
        SchwabPriceHistoryPublicationError,
    > {
        let SchwabDailyPriceHistoryPublicationRequest {
            capability,
            oauth_authority,
            user_preference,
            extraction_request,
            instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            identity,
            market_data,
            currency,
            calendar_range,
            ingested_at,
        } = request;
        // The admitted REST request is exactly `frequencyType=daily&frequency=1` and exposes no
        // corporate-action adjustment selector. Preserve provider-returned values without
        // allowing callers to relabel their interval or claim a provider-side adjustment.
        let interval = SourceIdentifier::try_from(SCHWAB_DAILY_INTERVAL)
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let adjustment = MarketBarAdjustment::Raw;
        if calendar_range.publication_source_id() != coordinates.source_id()
            || calendar_range.venue_id() != market_data.venue_id()
            || calendar_range.interval() != &interval
            || calendar_range.adjustment() != adjustment
            || calendar_range.provider_request_digest()
                != EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    self.capture().receipt().request_sha256(),
                )
        {
            return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
        }
        validate_preseal_request(
            &extraction_request,
            &coordinates,
            self.capture().receipt(),
            ingested_at,
        )?;
        let candidate = prepare_price_history_candidate(SchwabDailyPriceHistoryCandidateRequest {
            capability,
            oauth_authority,
            user_preference,
            response: &self,
            instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            identity,
            venue_id: market_data.venue_id.clone(),
            feed: market_data.feed.clone(),
            interval,
            adjustment,
            currency,
            calendar_range: calendar_range.as_ref(),
            ingested_at,
        })?;
        let pending = self.into_pending_capture(coordinates, event_id)?;
        let (rejoin, seal_request) = pending.into_sealing_parts();
        Ok((
            SchwabPendingDailyPriceHistoryPublication {
                rejoin,
                candidate,
                extraction_request,
                market_data,
                calendar_range,
            },
            seal_request,
        ))
    }
}

fn validate_preseal_request(
    request: &ExtractionRequest,
    coordinates: &SchwabCaptureCoordinates,
    receipt: &crate::RawRestResponseReceipt,
    ingested_at: Timestamp,
) -> Result<(), SchwabPriceHistoryPublicationError> {
    let received_at = Timestamp::from_unix_nanos(
        i64::try_from(receipt.received_at_unix_millis())
            .ok()
            .and_then(|value| value.checked_mul(1_000_000))
            .ok_or(SchwabPriceHistoryPublicationError::InvalidEvidence)?,
    );
    let object = request.object();
    if receipt.route() != ReadOnlyRoute::PriceHistory
        || object.source_id() != coordinates.source_id()
        || object.metadata_revision() != coordinates.metadata_revision()
        || object.dataset() != coordinates.dataset()
        || object.evidence().content_digest()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, receipt.body_sha256())
        || object.expected_bytes() != Some(receipt.body_bytes())
        || object.effective_interval().starts_at() != received_at
        || object.effective_interval().ends_at().is_some()
        || object.published_at().is_some()
        || object.availability().conservative_available_at() != Some(received_at)
        || request.deadline() <= ingested_at
    {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    }
    Ok(())
}

impl SchwabPendingDailyPriceHistoryPublication {
    /// Rejoins the exact common seal and mints the sole provider publication authority.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedDailyPriceHistoryPublication, SchwabPriceHistoryPublicationError> {
        let sealed_rest = self.rejoin.try_rejoin_whole(sealed)?;
        validate_sealed_rest(&sealed_rest, &self.candidate)?;
        let SchwabRestPayload::PriceHistory(parsed) = &sealed_rest.payload else {
            return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
        };
        let capture = sealed_rest.token.persisted_receipt().capture();
        let canonical_batch = canonical_batch(
            &self.extraction_request,
            &self.candidate,
            sealed_rest.receipt.body_sha256(),
        )?
        .try_bind_provider_capture(capture)
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        validate_request_binding(&canonical_batch, &sealed_rest)?;
        let native_lineage = native_lineage(
            parsed,
            &canonical_batch,
            &sealed_rest,
            &self.candidate,
            &self.market_data,
        )?;
        let revision_plan = ExtractionRevisionPlan::locally_observed_with_native_lineage(
            canonical_batch.records().len(),
        )
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let row_capture_page_ordinals = vec![0; canonical_batch.records().len()];
        let sealed_capture_binding = SealedProviderCaptureBinding::try_whole(
            sealed_rest.token,
            canonical_batch,
            native_lineage,
            row_capture_page_ordinals,
        )
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        sealed_capture_binding
            .validate()
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        Ok(SchwabSealedDailyPriceHistoryPublication {
            market_data: self.market_data,
            revision_plan,
            sealed_capture_binding,
            calendar_range: self.calendar_range,
        })
    }
}

/// Complete non-cloneable Schwab daily-history handoff for one-shot application publication.
#[derive(Debug)]
pub struct SchwabSealedDailyPriceHistoryPublication {
    market_data: SchwabPriceHistoryMarketDataEvidence,
    revision_plan: ExtractionRevisionPlan,
    sealed_capture_binding: SealedProviderCaptureBinding,
    calendar_range: Arc<dyn SchwabDailyPriceHistoryCalendarRangeReceipt>,
}

impl SchwabSealedDailyPriceHistoryPublication {
    /// Exact service/feed/venue/delay evidence persisted in the native-lineage sidecar.
    pub const fn market_data(&self) -> &SchwabPriceHistoryMarketDataEvidence {
        &self.market_data
    }

    /// Bounded local-content revision input aligned with the canonical batch.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Sole sealed raw/canonical/native authority for shared one-shot publication.
    pub const fn sealed_capture_binding(&self) -> &SealedProviderCaptureBinding {
        &self.sealed_capture_binding
    }

    /// Consumes the adapter handoff into the exact shared publication inputs.
    pub fn into_parts(
        self,
        publication_checked_at: Timestamp,
    ) -> Result<
        (ExtractionRevisionPlan, SealedProviderCaptureBinding),
        SchwabPriceHistoryPublicationError,
    > {
        if !self
            .calendar_range
            .validate_current_at(publication_checked_at)
        {
            return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
        }
        Ok((self.revision_plan, self.sealed_capture_binding))
    }
}

fn validate_sealed_rest(
    sealed: &SchwabSealedRestResponseParts,
    candidate: &SchwabPendingPriceHistoryCandidate,
) -> Result<(), SchwabPriceHistoryPublicationError> {
    let capture = sealed.token.persisted_receipt().capture();
    let [page] = capture.pages() else {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    };
    if sealed.receipt.route() != ReadOnlyRoute::PriceHistory
        || sealed.receipt.status() / 100 != 2
        || sealed.accounting.provider_records == 0
        || sealed.accounting.provider_records
            != u64::try_from(candidate.bars.len())
                .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?
        || candidate.response_observation_sha256
            != crate::vertical::rest_receipt_digest_from_parts(&sealed.receipt, sealed.accounting)
        || page.body_digest()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, sealed.receipt.body_sha256())
        || page.body_bytes() != sealed.receipt.body_bytes()
        || page.received_at() != candidate.response_received_at
        || candidate.feed.as_str().is_empty()
        || candidate.mapping_digest.bytes() == [0; 32]
    {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    }
    Ok(())
}

fn validate_request_binding(
    batch: &ExtractionBatch,
    sealed: &SchwabSealedRestResponseParts,
) -> Result<(), SchwabPriceHistoryPublicationError> {
    let object = batch.request().object();
    if object.source_id() != sealed.coordinates.source_id()
        || object.metadata_revision() != sealed.coordinates.metadata_revision()
        || object.dataset() != sealed.coordinates.dataset()
        || object.evidence().content_digest()
            != EvidenceDigest::new(DigestAlgorithm::Sha256, sealed.receipt.body_sha256())
        || object.expected_bytes() != Some(sealed.receipt.body_bytes())
        || object.availability().conservative_available_at()
            != Some(Timestamp::from_unix_nanos(
                i64::try_from(sealed.receipt.received_at_unix_millis())
                    .ok()
                    .and_then(|value| value.checked_mul(1_000_000))
                    .ok_or(SchwabPriceHistoryPublicationError::InvalidEvidence)?,
            ))
    {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    }
    Ok(())
}

fn canonical_batch(
    request: &ExtractionRequest,
    candidate: &SchwabPendingPriceHistoryCandidate,
    response_sha256: [u8; 32],
) -> Result<ExtractionBatch, SchwabPriceHistoryPublicationError> {
    if request.object().source_id().as_str().is_empty()
        || candidate.bars.is_empty()
        || candidate.bars.len()
            > usize::try_from(request.max_records())
                .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?
    {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    }
    let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    let payload_reference =
        PayloadReference::ContentHash(PayloadHash::new(DigestAlgorithm::Sha256, response_sha256));
    let revision =
        RevisionNumber::new(1).map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(candidate.bars.len())
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    for bar in &candidate.bars {
        let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: request.object().source_id().clone(),
            instrument_id: Some(candidate.instrument_id),
            venue_id: Some(candidate.venue_id.clone()),
            source_identifier: bar.source_identifier.clone(),
            source_timestamp: Some(bar.provider_timestamp),
            received_at: candidate.response_received_at,
            ingested_at: candidate.ingested_at,
            quality: DataQuality::Aggregated,
            payload_reference: payload_reference.clone(),
            availability: ResearchAvailabilityEvidence::local_first_observed(
                candidate.response_received_at,
            ),
        })
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let time = ResearchTime::new(bar.provider_timestamp, None, revision, None)
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let context = ResearchContext::new(provenance, time)
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let observation = MarketBarObservation::new(
            context,
            candidate.identity.provider_instrument_id().clone(),
            candidate.feed.clone(),
            candidate.interval.clone(),
            bar.time_semantics.clone(),
            candidate.adjustment,
            Money::new(bar.open, candidate.currency),
            Money::new(bar.high, candidate.currency),
            Money::new(bar.low, candidate.currency),
            Money::new(bar.close, candidate.currency),
            bar.volume,
            None,
            None,
        )
        .map(ResearchObservation::MarketBar)
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let payload = serde_json::to_vec(&observation)
            .map(Bytes::from)
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        let payload_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
        let evidence = ExactPayloadEvidence::from_content_digest(payload_digest);
        let revision = SourceIdentifier::try_from(format!(
            "schwab-history:{}:{}",
            bar.provider_timestamp.unix_nanos(),
            &hex(payload_digest.bytes())[..16]
        ))
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
        records.push(
            ExtractionRecord::try_new(
                request,
                schema.clone(),
                evidence,
                bar.provider_timestamp,
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: candidate.response_received_at,
                },
                revision,
                None,
                payload,
            )
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?,
        );
    }
    ExtractionBatch::try_new(request, records)
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabPriceHistoryNativeRowV1<'a> {
    datetime_millis: u64,
    open: &'a str,
    high: &'a str,
    low: &'a str,
    close: &'a str,
    volume: &'a str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabPriceHistoryNativeSidecarV1<'a> {
    version: u16,
    family: &'static str,
    service: &'static str,
    route: &'static str,
    provider_schema: &'static str,
    provider_schema_version: u16,
    provider_symbol: &'a str,
    request_url: &'a str,
    request_sha256: [u8; 32],
    response_sha256: [u8; 32],
    response_status: u16,
    response_bytes: u64,
    declared_response_bytes: Option<u64>,
    received_at_unix_millis: u64,
    latency_millis: u64,
    response_headers: Vec<SchwabRestHeaderEvidenceV1<'a>>,
    token_generation: u64,
    requested_items: u64,
    returned_items: u64,
    missing_items: u64,
    unexpected_items: u64,
    provider_records: u64,
    venue: &'a str,
    feed: &'a str,
    delay: SchwabRestDelayEvidence,
    qualification_evidence: EvidenceDigest,
    market_data_permission: Option<&'a str>,
    previous_close_state: &'static str,
    previous_close: Option<&'a str>,
    previous_close_date_state: &'static str,
    previous_close_date_millis: Option<u64>,
    unknown_field_count: usize,
    unknown_field_bytes: usize,
    unknown_field_paths: &'a [Box<str>],
    unknown_field_digest: [u8; 32],
    capability_receipt_sha256: [u8; 32],
    user_preference_observation_sha256: [u8; 32],
    response_observation_sha256: [u8; 32],
    oauth_generation: u64,
    oauth_access_issued_at_unix_seconds: u64,
    oauth_access_expires_at_unix_seconds: u64,
    oauth_refresh_authorized_at_unix_seconds: u64,
    oauth_refresh_expires_at_unix_seconds: u64,
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_instrument_id: &'a ProviderInstrumentId,
    resolution_evidence: EvidenceDigest,
    completed_range_receipt_digest: EvidenceDigest,
    range_completeness_evidence: EvidenceDigest,
    completeness_evidence: EvidenceDigest,
    mapping_digest: EvidenceDigest,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct SchwabRestHeaderEvidenceV1<'a> {
    name: &'a str,
    value: &'a [u8],
}

fn native_lineage(
    parsed: &crate::ParsedNative<PriceHistoryResponse>,
    batch: &ExtractionBatch,
    sealed: &SchwabSealedRestResponseParts,
    candidate: &SchwabPendingPriceHistoryCandidate,
    market_data: &SchwabPriceHistoryMarketDataEvidence,
) -> Result<ProviderNativeLineageBatch, SchwabPriceHistoryPublicationError> {
    let history = parsed.value();
    if history.candles().len() != batch.records().len()
        || history.symbol.as_str() != candidate.identity.provider_symbol().as_str()
        || parsed.raw_sha256() != sealed.receipt.body_sha256()
        || candidate.feed != market_data.feed
        || candidate.venue_id != market_data.venue_id
    {
        return Err(SchwabPriceHistoryPublicationError::InvalidEvidence);
    }
    let (previous_close_state, previous_close) = native_number_field(&history.previous_close);
    let (previous_close_date_state, previous_close_date_millis) =
        native_u64_field(&history.previous_close_date_millis);
    let unknown = parsed.unknown_fields();
    let mut response_headers = Vec::new();
    response_headers
        .try_reserve_exact(sealed.receipt.headers().len())
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    response_headers.extend(sealed.receipt.headers().iter().map(|header| {
        SchwabRestHeaderEvidenceV1 {
            name: header.name(),
            value: header.value(),
        }
    }));
    let mut lineage = ProviderNativeLineageBatchBuilder::try_new(
        ProviderNativeLineageImplementation::SchwabRestMarketDataV1,
        batch,
    )
    .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    lineage
        .try_set_batch_sidecar(&SchwabPriceHistoryNativeSidecarV1 {
            version: 1,
            family: "schwab.daily-price-history",
            service: market_data.service(),
            route: "price-history",
            provider_schema: parsed.schema_name(),
            provider_schema_version: parsed.schema_version(),
            provider_symbol: history.symbol.as_str(),
            request_url: sealed.receipt.request_url(),
            request_sha256: sealed.receipt.request_sha256(),
            response_sha256: sealed.receipt.body_sha256(),
            response_status: sealed.receipt.status(),
            response_bytes: sealed.receipt.body_bytes(),
            declared_response_bytes: sealed.receipt.declared_body_bytes(),
            received_at_unix_millis: sealed.receipt.received_at_unix_millis(),
            latency_millis: sealed.receipt.latency_ms(),
            response_headers,
            token_generation: sealed.receipt.token_generation().get(),
            requested_items: sealed.accounting.requested,
            returned_items: sealed.accounting.returned,
            missing_items: sealed.accounting.missing,
            unexpected_items: sealed.accounting.unexpected,
            provider_records: sealed.accounting.provider_records,
            venue: market_data.venue_id.as_str(),
            feed: market_data.feed.as_str(),
            delay: market_data.delay,
            qualification_evidence: market_data.qualification_evidence,
            market_data_permission: candidate.market_data_permission.as_deref(),
            previous_close_state,
            previous_close,
            previous_close_date_state,
            previous_close_date_millis,
            unknown_field_count: unknown.field_count(),
            unknown_field_bytes: unknown.encoded_bytes(),
            unknown_field_paths: unknown.paths(),
            unknown_field_digest: unknown.digest(),
            capability_receipt_sha256: candidate.capability.receipt_sha256(),
            user_preference_observation_sha256: candidate.user_preference_observation_sha256,
            response_observation_sha256: candidate.response_observation_sha256,
            oauth_generation: candidate.oauth_authority.generation().get(),
            oauth_access_issued_at_unix_seconds: candidate
                .oauth_authority
                .access_issued_at_unix_seconds(),
            oauth_access_expires_at_unix_seconds: candidate
                .oauth_authority
                .access_expires_at_unix_seconds(),
            oauth_refresh_authorized_at_unix_seconds: candidate
                .oauth_authority
                .refresh_authorized_at_unix_seconds(),
            oauth_refresh_expires_at_unix_seconds: candidate
                .oauth_authority
                .refresh_expires_at_unix_seconds(),
            requested_start: candidate.requested_start,
            requested_end: candidate.requested_end,
            instrument_revision_digest: candidate.instrument_revision_digest,
            admitted_plan_digest: candidate.admitted_plan_digest,
            provider_instrument_id: candidate.identity.provider_instrument_id(),
            resolution_evidence: candidate.identity.resolution_evidence(),
            completed_range_receipt_digest: candidate.completed_range_receipt_digest,
            range_completeness_evidence: candidate.range_completeness_evidence,
            completeness_evidence: candidate.completeness_evidence,
            mapping_digest: candidate.mapping_digest,
        })
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    for candle in history.candles() {
        lineage
            .try_push(&SchwabPriceHistoryNativeRowV1 {
                datetime_millis: candle.datetime_millis,
                open: candle.open.as_str(),
                high: candle.high.as_str(),
                low: candle.low.as_str(),
                close: candle.close.as_str(),
                volume: candle.volume.as_str(),
            })
            .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)?;
    }
    lineage
        .finish()
        .map_err(|_| SchwabPriceHistoryPublicationError::InvalidEvidence)
}

fn native_number_field(field: &NativeField<NativeNumber>) -> (&'static str, Option<&str>) {
    match field {
        NativeField::Absent => ("absent", None),
        NativeField::Null => ("null", None),
        NativeField::Value(value) => ("value", Some(value.as_str())),
    }
}

fn native_u64_field(field: &NativeField<u64>) -> (&'static str, Option<u64>) {
    match field {
        NativeField::Absent => ("absent", None),
        NativeField::Null => ("null", None),
        NativeField::Value(value) => ("value", Some(*value)),
    }
}

fn hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

/// Closed secret-free daily-history seal/publication failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchwabPriceHistoryPublicationError {
    /// OAuth, capability, identity, calendar, or canonical mapping evidence did not match.
    #[error("Schwab daily-history canonical evidence is invalid")]
    InvalidEvidence,
    /// The accepted response could not move through the common consuming seal boundary.
    #[error("Schwab daily-history physical seal binding failed")]
    Capture,
}

impl From<SchwabCanonicalError> for SchwabPriceHistoryPublicationError {
    fn from(_error: SchwabCanonicalError) -> Self {
        Self::InvalidEvidence
    }
}

impl From<SchwabTransportError> for SchwabPriceHistoryPublicationError {
    fn from(_error: SchwabTransportError) -> Self {
        Self::Capture
    }
}
