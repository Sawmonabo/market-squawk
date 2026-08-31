//! Seal-first Tiingo latest NAV/EOD publication.
//!
//! Metadata and the latest daily-price response are one ordered request graph: metadata is
//! component/page zero and the price response is component/page one. The graph is sealed once,
//! rejoined through its private one-use witness, and consumed into exactly one NAV or EOD
//! publication. Provider-native sidecars retain metadata, request, disposition, gap, and action
//! semantics without copying canonical identities, local clocks, or capture digests.

use bytes::Bytes;
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, FundNavCorrectionState,
    FundNavFinality, FundNavObservation, FundNavObservationInput, FundNavRevisionEvidence,
    MarketBarObservation, PayloadHash, PayloadReference, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, ExtractionBatch,
    ExtractionBatchAccumulator, ExtractionError, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionPlan, ObservedRevisionError, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCaptureSealExpectation, ProviderCaptureSealRequest, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageError, ProviderNativeLineageImplementation, ProviderWholeCaptureToken,
    SealedProviderCaptureBinding, SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt,
    SourceObjectCaptureIdentity,
};
use rust_decimal::Decimal;
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::canonical::{
    TIINGO_LATEST_PUBLICATION_DATASET, latest_publication_request_graph_identity,
};
use crate::{
    TiingoAdapterError, TiingoCaptureMaterialError, TiingoCapturedPage, TiingoCoverage,
    TiingoEndpointFamily, TiingoEodBarCandidate, TiingoEodBarTimeAuthority,
    TiingoEodContractEvidence, TiingoEodInstrumentAuthority, TiingoEodMapError,
    TiingoEodMappingInput, TiingoEodProviderActionEvidence, TiingoEodReceipt, TiingoEodSurface,
    TiingoEodSurfaceGap, TiingoEodSurfaceGapReason, TiingoFundContext,
    TiingoFundNavContractEvidence, TiingoFundNavMapError, TiingoFundNavMappingInput,
    TiingoMetadataReceipt, TiingoNavValueState, TiingoPaginationEvidence, TiingoRequestDisposition,
    TiingoRequestScope, TiingoRequestSpec, map_eod_page_candidate, map_fund_nav_candidate,
    normalize_mutual_fund_row,
};

const TIINGO_CANONICAL_MEDIA_TYPE: &str = "application-json";
const TIINGO_LOCAL_REVISION_PREFIX: &str = "tiingo-local";

/// One-use continuation retained while the common raw journal seals the exact request graph.
#[derive(Debug)]
pub struct TiingoPendingLatestPublication {
    expectation: ProviderCaptureSealExpectation,
    expected_graph: market_squawk_sources::ProviderCaptureSetReceipt,
    metadata: TiingoMetadataReceipt,
    latest: TiingoEodReceipt,
}

/// Exact sealed metadata/latest graph that can enter exactly one NAV or EOD publication path.
#[derive(Debug)]
pub struct TiingoSealedLatestPublication {
    token: ProviderWholeCaptureToken,
    metadata: TiingoMetadataReceipt,
    latest: TiingoEodReceipt,
}

/// Complete canonical FundNav extraction handoff with exclusive sealed-raw authority.
#[derive(Debug)]
pub struct TiingoSealedFundNavPublication {
    revision_plan: ExtractionRevisionPlan,
    binding: SealedProviderCaptureBinding,
}

impl TiingoSealedFundNavPublication {
    /// Returns the common locally observed revision plan aligned to the sole FundNav row.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Returns the canonical/native/raw binding consumed by shared durable publication.
    pub const fn binding(&self) -> &SealedProviderCaptureBinding {
        &self.binding
    }

    /// Consumes this handoff into the exact shared publication parts.
    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderCaptureBinding) {
        (self.revision_plan, self.binding)
    }
}

/// Complete canonical raw/adjusted MarketBar extraction handoff with one sealed graph token.
#[derive(Debug)]
pub struct TiingoSealedEodPublication {
    revision_plan: ExtractionRevisionPlan,
    binding: SealedProviderCaptureBinding,
}

impl TiingoSealedEodPublication {
    /// Returns the common locally observed revision plan aligned to every admitted bar surface.
    pub const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    /// Returns the canonical/native/raw binding consumed by shared durable publication.
    pub const fn binding(&self) -> &SealedProviderCaptureBinding {
        &self.binding
    }

    /// Consumes this handoff into the exact shared publication parts.
    pub fn into_parts(self) -> (ExtractionRevisionPlan, SealedProviderCaptureBinding) {
        (self.revision_plan, self.binding)
    }
}

/// Closed reason a sealed latest graph cannot truthfully publish a canonical row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoLatestUnavailableReason {
    /// Exact metadata did not establish supported coverage for this requested Tiingo instrument.
    UnsupportedMetadataCoverage,
    /// The successful latest response contained no dated provider row.
    EmptyLatestResponse,
    /// Every raw and adjusted EOD surface was incomplete; no cross-surface fill is permitted.
    NoCompleteEodSurface,
}

/// Sealed raw evidence for a truthful unavailable result; this type exposes no reusable token.
#[derive(Debug)]
pub struct TiingoSealedLatestUnavailable {
    reason: TiingoLatestUnavailableReason,
    token: ProviderWholeCaptureToken,
    returned_rows: u32,
    surface_gaps: u32,
}

impl TiingoSealedLatestUnavailable {
    /// Returns the exact closed unavailable reason.
    pub const fn reason(&self) -> TiingoLatestUnavailableReason {
        self.reason
    }

    /// Returns actual accepted provider-native rows, never requested slots.
    pub const fn returned_rows(&self) -> u32 {
        self.returned_rows
    }

    /// Returns exact incomplete raw/adjusted surface count.
    pub const fn surface_gaps(&self) -> u32 {
        self.surface_gaps
    }

    /// Returns cloneable persisted graph evidence without recreating publication authority.
    pub fn persisted_capture(&self) -> &SealedProviderCaptureSetReceipt {
        self.token.persisted_receipt()
    }
}

/// NAV publication or an honest sealed unavailable state.
#[derive(Debug)]
pub enum TiingoLatestFundNavPublicationOutcome {
    /// One exact mutual-fund NAV row is ready for shared durable publication.
    Published(TiingoSealedFundNavPublication),
    /// No dated NAV could be published without fabrication.
    Unavailable(TiingoSealedLatestUnavailable),
}

/// EOD publication or an honest sealed unavailable state.
#[derive(Debug)]
pub enum TiingoLatestEodPublicationOutcome {
    /// At least one exact raw or adjusted equity/ETF EOD bar is ready for publication.
    Published(TiingoSealedEodPublication),
    /// No complete surface could be published without cross-surface substitution.
    Unavailable(TiingoSealedLatestUnavailable),
}

/// Builds one deterministic ordered metadata/latest graph and splits its exclusive seal witness.
#[allow(
    clippy::too_many_arguments,
    reason = "both exact raw-record event identities and their shared connection stay explicit"
)]
pub fn prepare_latest_publication(
    metadata: TiingoCapturedPage<TiingoMetadataReceipt>,
    latest: TiingoCapturedPage<TiingoEodReceipt>,
    metadata_event_id: Uuid,
    latest_event_id: Uuid,
    connection_id: Uuid,
) -> Result<
    (TiingoPendingLatestPublication, ProviderCaptureSealRequest),
    TiingoLatestPublicationError,
> {
    validate_latest_pair(metadata.decoded(), latest.decoded())?;
    let request_graph_identity = latest_publication_request_graph_identity(
        metadata.decoded().evidence().request(),
        latest.decoded().evidence().request(),
    )?;
    let metadata_material = metadata.capture_material(metadata_event_id, connection_id)?;
    let latest_material = latest.capture_material(latest_event_id, connection_id)?;
    let graph = ProviderCaptureMaterial::try_combine_request_graph(
        metadata_material.receipt().source_id().clone(),
        metadata_material.receipt().metadata_revision().clone(),
        identifier(TIINGO_LATEST_PUBLICATION_DATASET)?,
        request_graph_identity,
        vec![metadata_material, latest_material],
    )?;
    let expected_graph = graph.receipt().clone();
    let metadata = metadata.decoded().clone();
    let latest = latest.decoded().clone();
    let (expectation, seal_request) = graph.into_whole_seal_parts();
    Ok((
        TiingoPendingLatestPublication {
            expectation,
            expected_graph,
            metadata,
            latest,
        },
        seal_request,
    ))
}

impl TiingoPendingLatestPublication {
    /// Rejoins only the physical result split from this exact graph and recovers whole authority.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<TiingoSealedLatestPublication, TiingoLatestPublicationError> {
        let token = self.expectation.try_rejoin(sealed)?.try_into_whole()?;
        if token.persisted_receipt().capture() != &self.expected_graph {
            return Err(TiingoLatestPublicationError::CaptureBinding);
        }
        Ok(TiingoSealedLatestPublication {
            token,
            metadata: self.metadata,
            latest: self.latest,
        })
    }
}

impl TiingoSealedLatestPublication {
    /// Consumes an exact `NMFQS` metadata leaf into one canonical FundNav batch.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, source contract, local clocks, and extraction authority remain explicit"
    )]
    pub fn try_into_fund_nav(
        self,
        context: TiingoFundContext,
        contract: &TiingoFundNavContractEvidence,
        extraction_request: ExtractionRequest,
        ingested_at: Timestamp,
        canonical_published_at: Timestamp,
    ) -> Result<TiingoLatestFundNavPublicationOutcome, TiingoLatestPublicationError> {
        let Self {
            token,
            metadata,
            latest,
        } = self;
        validate_fund_nav_authority(&metadata, &context)?;
        if matches!(metadata.metadata().coverage(), TiingoCoverage::Unsupported) {
            return Ok(TiingoLatestFundNavPublicationOutcome::Unavailable(
                unavailable(
                    token,
                    TiingoLatestUnavailableReason::UnsupportedMetadataCoverage,
                    latest.disposition(),
                    0,
                )?,
            ));
        }
        let [row] = latest.rows() else {
            if latest.rows().is_empty() {
                return Ok(TiingoLatestFundNavPublicationOutcome::Unavailable(
                    unavailable(
                        token,
                        TiingoLatestUnavailableReason::EmptyLatestResponse,
                        latest.disposition(),
                        0,
                    )?,
                ));
            }
            return Err(TiingoLatestPublicationError::InvalidLatestResponse);
        };
        let native_row = TiingoNativeDailyRowV1::from_row(row, "nav_raw_close");
        let candidate = normalize_mutual_fund_row(context, &metadata, &latest, 0)?;
        if !matches!(candidate.value(), TiingoNavValueState::Observed(_)) {
            return Err(TiingoLatestPublicationError::InvalidLatestResponse);
        }
        let sealed_capture = token.persisted_receipt();
        let candidate = map_fund_nav_candidate(TiingoFundNavMappingInput {
            candidate: &candidate,
            sealed_capture,
            completed_history: None,
            sealed_metadata_capture: sealed_capture,
            contract,
            ingested_at,
        })?;
        let observation = fund_nav_observation(&candidate, canonical_published_at)?;
        let research = ResearchObservation::FundNav(observation);
        let batch = single_observation_batch(
            &extraction_request,
            &research,
            candidate.effective().clone(),
            candidate.provenance().received_at(),
            ingested_at,
            sealed_capture,
        )?;
        let sidecar = TiingoFundNavSidecarV1::new(&metadata, &latest, candidate.value());
        let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::TiingoFundNavV1,
            &batch,
        )?;
        native_lineage.try_set_batch_sidecar(&sidecar)?;
        native_lineage.try_push(&native_row)?;
        let native_lineage = native_lineage.finish()?;
        let revision_plan =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())?;
        let binding =
            SealedProviderCaptureBinding::try_whole(token, batch, native_lineage, vec![1])?;
        binding.validate()?;
        Ok(TiingoLatestFundNavPublicationOutcome::Published(
            TiingoSealedFundNavPublication {
                revision_plan,
                binding,
            },
        ))
    }

    /// Consumes an externally resolved equity/ETF metadata leaf into raw/adjusted MarketBar rows.
    pub fn try_into_eod(
        self,
        instrument: &TiingoEodInstrumentAuthority,
        contract: &TiingoEodContractEvidence,
        bar_time_authority: &dyn TiingoEodBarTimeAuthority,
        extraction_request: ExtractionRequest,
        ingested_at: Timestamp,
    ) -> Result<TiingoLatestEodPublicationOutcome, TiingoLatestPublicationError> {
        let Self {
            token,
            metadata,
            latest,
        } = self;
        if metadata.metadata().exchange_code() == crate::nav::TIINGO_MUTUAL_FUND_EXCHANGE_CODE {
            return Err(TiingoLatestPublicationError::WrongInstrumentFamily);
        }
        let sealed_capture = token.persisted_receipt();
        let page = map_eod_page_candidate(TiingoEodMappingInput {
            response: &latest,
            metadata: &metadata,
            sealed_capture,
            sealed_metadata_capture: sealed_capture,
            instrument,
            contract,
            bar_time_authority,
            ingested_at,
        })?;
        if page.bars().is_empty() {
            let reason = if latest.rows().is_empty() {
                TiingoLatestUnavailableReason::EmptyLatestResponse
            } else {
                TiingoLatestUnavailableReason::NoCompleteEodSurface
            };
            return Ok(TiingoLatestEodPublicationOutcome::Unavailable(unavailable(
                token,
                reason,
                latest.disposition(),
                page.gaps().len(),
            )?));
        }
        let observations = eod_observations(&page)?;
        let mut records = ExtractionBatchAccumulator::try_new(&extraction_request)?;
        for observation in &observations {
            let ResearchObservation::MarketBar(bar) = observation else {
                return Err(TiingoLatestPublicationError::InvalidLatestResponse);
            };
            records.push(extraction_record(
                &extraction_request,
                observation,
                bar.context().time().effective().clone(),
                bar.context().provenance().received_at(),
            )?)?;
        }
        let batch = records
            .finish()?
            .try_bind_provider_capture(sealed_capture.capture())?;
        validate_extraction_request(
            &extraction_request,
            sealed_capture,
            batch.records().len(),
            ingested_at,
        )?;
        let sidecar = TiingoEodSidecarV1::try_new(&metadata, &latest, &page)?;
        let mut native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
            &batch,
        )?;
        native_lineage.try_set_batch_sidecar(&sidecar)?;
        for bar in page.bars() {
            let row = latest
                .rows()
                .get(
                    usize::try_from(bar.provider_row_index())
                        .map_err(|_| TiingoLatestPublicationError::InvalidLatestResponse)?,
                )
                .ok_or(TiingoLatestPublicationError::InvalidLatestResponse)?;
            native_lineage.try_push(&TiingoNativeDailyRowV1::from_row(
                row,
                surface_name(bar.surface()),
            ))?;
        }
        let native_lineage = native_lineage.finish()?;
        let revision_plan =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())?;
        let row_count = batch.records().len();
        let binding = SealedProviderCaptureBinding::try_whole(
            token,
            batch,
            native_lineage,
            vec![1; row_count],
        )?;
        binding.validate()?;
        Ok(TiingoLatestEodPublicationOutcome::Published(
            TiingoSealedEodPublication {
                revision_plan,
                binding,
            },
        ))
    }
}

pub(crate) fn validate_fund_nav_authority(
    metadata: &TiingoMetadataReceipt,
    context: &TiingoFundContext,
) -> Result<(), TiingoLatestPublicationError> {
    if metadata.metadata().ticker() != context.ticker()
        || metadata.metadata().exchange_code() != crate::nav::TIINGO_MUTUAL_FUND_EXCHANGE_CODE
        || context.provider_exchange_code().as_str() != metadata.metadata().exchange_code()
    {
        return Err(TiingoLatestPublicationError::WrongInstrumentFamily);
    }
    Ok(())
}

fn validate_latest_pair(
    metadata: &TiingoMetadataReceipt,
    latest: &TiingoEodReceipt,
) -> Result<(), TiingoLatestPublicationError> {
    let metadata_evidence = metadata.evidence();
    let latest_evidence = latest.evidence();
    if metadata_evidence.request().endpoint() != TiingoEndpointFamily::Metadata
        || metadata_evidence.request().scope() != &TiingoRequestScope::Metadata
        || latest_evidence.request().endpoint() != TiingoEndpointFamily::LatestDailyPrices
        || latest_evidence.request().scope() != &TiingoRequestScope::Latest
        || latest.pagination() != TiingoPaginationEvidence::NotApplicable
        || metadata.metadata().ticker() != latest_evidence.request().ticker()
        || metadata_evidence.request().ticker() != latest_evidence.request().ticker()
        || metadata_evidence.native_contract_revision()
            != latest_evidence.native_contract_revision()
        || metadata_evidence.entitlement_generation() != latest_evidence.entitlement_generation()
        || metadata_evidence.decoded_at() > latest_evidence.received_at()
        || latest.rows().len() > latest_evidence.request().max_rows()
        || metadata.disposition().returned_rows() != 1
        || metadata.disposition().returned_symbols() != 1
        || metadata.disposition().missing_symbols() != 0
    {
        return Err(TiingoLatestPublicationError::InvalidLatestResponse);
    }
    Ok(())
}

fn unavailable(
    token: ProviderWholeCaptureToken,
    reason: TiingoLatestUnavailableReason,
    disposition: TiingoRequestDisposition,
    surface_gaps: usize,
) -> Result<TiingoSealedLatestUnavailable, TiingoLatestPublicationError> {
    Ok(TiingoSealedLatestUnavailable {
        reason,
        token,
        returned_rows: disposition.returned_rows(),
        surface_gaps: u32::try_from(surface_gaps)
            .map_err(|_| TiingoLatestPublicationError::InvalidLatestResponse)?,
    })
}

fn fund_nav_observation(
    candidate: &crate::TiingoFundNavCanonicalCandidate,
    canonical_published_at: Timestamp,
) -> Result<FundNavObservation, TiingoLatestPublicationError> {
    let context = ResearchContext::new(
        candidate.provenance().clone(),
        ResearchTime::try_new_with_coordinates(
            candidate.effective().clone(),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    FundNavObservation::try_new(FundNavObservationInput {
        context,
        provider_instrument_id: candidate.provider_instrument_id().clone(),
        instrument_reference_revision: candidate.instrument_reference_revision().clone(),
        provider_product: candidate.provider_product().clone(),
        provider_channel: candidate.provider_channel().clone(),
        nav_date: candidate.nav_date(),
        valuation_basis: candidate.valuation_basis(),
        currency: candidate.currency(),
        value: candidate.value(),
        canonical_published_at,
        lineage: candidate.lineage().clone(),
        revision_evidence: FundNavRevisionEvidence::try_new(
            None,
            FundNavCorrectionState::Unspecified,
            FundNavFinality::Unspecified,
            None,
            None,
        )?,
    })
    .map_err(Into::into)
}

fn single_observation_batch(
    request: &ExtractionRequest,
    observation: &ResearchObservation,
    effective: market_squawk_domain::ResearchTemporalCoordinate,
    received_at: Timestamp,
    ingested_at: Timestamp,
    sealed: &SealedProviderCaptureSetReceipt,
) -> Result<ExtractionBatch, TiingoLatestPublicationError> {
    validate_extraction_request(request, sealed, 1, ingested_at)?;
    ExtractionBatch::try_new(
        request,
        vec![extraction_record(
            request,
            observation,
            effective,
            received_at,
        )?],
    )?
    .try_bind_provider_capture(sealed.capture())
    .map_err(Into::into)
}

fn extraction_record(
    request: &ExtractionRequest,
    observation: &ResearchObservation,
    effective: market_squawk_domain::ResearchTemporalCoordinate,
    received_at: Timestamp,
) -> Result<ExtractionRecord, TiingoLatestPublicationError> {
    let payload = serde_json::to_vec(observation)
        .map(Bytes::from)
        .map_err(|_| TiingoLatestPublicationError::CanonicalEncoding)?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    let revision = identifier(&format!(
        "{TIINGO_LOCAL_REVISION_PREFIX}-{}",
        &lower_hex(digest.bytes())[..16]
    ))?;
    ExtractionRecord::try_new_with_time(
        request,
        identifier(CURRENT_RESEARCH_RECORD_SCHEMA)?,
        ExactPayloadEvidence::from_content_digest(digest),
        effective,
        None,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        revision,
        None,
        payload,
    )
    .map_err(Into::into)
}

fn validate_extraction_request(
    request: &ExtractionRequest,
    sealed: &SealedProviderCaptureSetReceipt,
    record_count: usize,
    ingested_at: Timestamp,
) -> Result<(), TiingoLatestPublicationError> {
    let capture = sealed.capture();
    let object = request.object();
    let Some(first_page) = capture.pages().first() else {
        return Err(TiingoLatestPublicationError::InvalidExtractionRequest);
    };
    let Some(last_page) = capture.pages().last() else {
        return Err(TiingoLatestPublicationError::InvalidExtractionRequest);
    };
    if object.source_id() != capture.source_id()
        || object.metadata_revision() != capture.metadata_revision()
        || object.dataset() != capture.dataset()
        || object.media_type().as_str() != TIINGO_CANONICAL_MEDIA_TYPE
        || object.evidence().content_digest() != capture.content_digest()
        || object.capture_identity() != SourceObjectCaptureIdentity::Standalone
        || object.effective_interval().starts_at() != first_page.received_at()
        || object.effective_interval().ends_at().is_some()
        || object.published_at().is_some()
        || object.availability().conservative_available_at() != Some(last_page.received_at())
        || object.expected_bytes() != Some(capture.total_body_bytes())
        || usize::try_from(request.max_records())
            .ok()
            .is_none_or(|max| max < record_count)
        || request.deadline() <= ingested_at
    {
        return Err(TiingoLatestPublicationError::InvalidExtractionRequest);
    }
    Ok(())
}

fn eod_observations(
    page: &crate::TiingoEodPageCandidate,
) -> Result<Vec<ResearchObservation>, TiingoLatestPublicationError> {
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(page.bars().len())
        .map_err(|_| TiingoLatestPublicationError::Allocation)?;
    for bar in page.bars() {
        observations.push(eod_observation(page, bar)?);
    }
    Ok(observations)
}

fn eod_observation(
    page: &crate::TiingoEodPageCandidate,
    bar: &TiingoEodBarCandidate,
) -> Result<ResearchObservation, TiingoLatestPublicationError> {
    let source_identifier = identifier(&format!(
        "tiingo-eod-{}-{}-{}",
        page.instrument().ticker(),
        bar.provider_date(),
        surface_name(bar.surface())
    ))?;
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: page.contract().source_id().clone(),
        instrument_id: Some(bar.instrument_id()),
        venue_id: Some(bar.venue_id().clone()),
        source_identifier,
        source_timestamp: Some(bar.time_semantics().provider_timestamp()),
        received_at: bar.received_at(),
        ingested_at: bar.ingested_at(),
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            bar.provider_row_digest().algorithm(),
            bar.provider_row_digest().bytes(),
        )),
        availability: bar.availability().clone(),
    })?;
    let context = ResearchContext::new(
        provenance,
        ResearchTime::new(
            bar.time_semantics().provider_timestamp(),
            None,
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    MarketBarObservation::new(
        context,
        bar.provider_instrument_id().clone(),
        bar.feed().clone(),
        bar.interval().clone(),
        bar.time_semantics().clone(),
        bar.adjustment(),
        bar.open(),
        bar.high(),
        bar.low(),
        bar.close(),
        bar.volume(),
        None,
        None,
    )
    .map(ResearchObservation::MarketBar)
    .map_err(Into::into)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoNativeDailyRowV1 {
    provider_date: Box<str>,
    selected_surface: &'static str,
    open: Option<String>,
    high: Option<String>,
    low: Option<String>,
    close: Option<String>,
    volume: Option<String>,
    adjusted_open: Option<String>,
    adjusted_high: Option<String>,
    adjusted_low: Option<String>,
    adjusted_close: Option<String>,
    adjusted_volume: Option<String>,
    cash_dividend: Option<String>,
    split_factor: Option<String>,
}

impl TiingoNativeDailyRowV1 {
    fn from_row(row: &crate::TiingoEodRow, selected_surface: &'static str) -> Self {
        let (open, high, low, close) = row.raw_ohlc();
        let (adjusted_open, adjusted_high, adjusted_low, adjusted_close) = row.adjusted_ohlc();
        Self {
            provider_date: row.provider_date().into(),
            selected_surface,
            open: decimal_string(open),
            high: decimal_string(high),
            low: decimal_string(low),
            close: decimal_string(close),
            volume: decimal_string(row.volume()),
            adjusted_open: decimal_string(adjusted_open),
            adjusted_high: decimal_string(adjusted_high),
            adjusted_low: decimal_string(adjusted_low),
            adjusted_close: decimal_string(adjusted_close),
            adjusted_volume: decimal_string(row.adjusted_volume()),
            cash_dividend: decimal_string(row.cash_dividend()),
            split_factor: decimal_string(row.split_factor()),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoMetadataNativeV1<'a> {
    ticker: &'a str,
    name: &'a str,
    exchange_code: &'a str,
    description: Option<&'a str>,
    coverage: TiingoCoverageNativeV1,
}

#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum TiingoCoverageNativeV1 {
    Supported {
        start_date: String,
        end_date: String,
    },
    Unsupported,
}

impl<'a> TiingoMetadataNativeV1<'a> {
    fn from_receipt(receipt: &'a TiingoMetadataReceipt) -> Self {
        let metadata = receipt.metadata();
        let coverage = match metadata.coverage() {
            TiingoCoverage::Supported {
                start_date,
                end_date,
            } => TiingoCoverageNativeV1::Supported {
                start_date: start_date.to_string(),
                end_date: end_date.to_string(),
            },
            TiingoCoverage::Unsupported => TiingoCoverageNativeV1::Unsupported,
        };
        Self {
            ticker: metadata.ticker().as_str(),
            name: metadata.name(),
            exchange_code: metadata.exchange_code(),
            description: metadata.description(),
            coverage,
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoRequestNativeV1<'a> {
    ticker: &'a str,
    endpoint: &'static str,
    scope: &'static str,
    credential_free_url: &'a str,
    max_response_bytes: usize,
    max_rows: usize,
    pagination: &'static str,
}

impl<'a> TiingoRequestNativeV1<'a> {
    fn latest(request: &'a TiingoRequestSpec) -> Self {
        Self {
            ticker: request.ticker().as_str(),
            endpoint: endpoint_name(request.endpoint()),
            scope: scope_name(request.scope()),
            credential_free_url: request.url().as_str(),
            max_response_bytes: request.max_response_bytes(),
            max_rows: request.max_rows(),
            pagination: "not_applicable",
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoDispositionNativeV1 {
    requested_symbols: u16,
    returned_symbols: u16,
    missing_symbols: u16,
    returned_rows: u32,
    response_bytes: u64,
}

impl From<TiingoRequestDisposition> for TiingoDispositionNativeV1 {
    fn from(value: TiingoRequestDisposition) -> Self {
        Self {
            requested_symbols: value.requested_symbols(),
            returned_symbols: value.returned_symbols(),
            missing_symbols: value.missing_symbols(),
            returned_rows: value.returned_rows(),
            response_bytes: value.response_bytes(),
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoFundNavSidecarV1<'a> {
    metadata: TiingoMetadataNativeV1<'a>,
    metadata_request: TiingoRequestNativeV1<'a>,
    latest_request: TiingoRequestNativeV1<'a>,
    metadata_disposition: TiingoDispositionNativeV1,
    latest_disposition: TiingoDispositionNativeV1,
    nav_disposition: &'static str,
}

impl<'a> TiingoFundNavSidecarV1<'a> {
    fn new(
        metadata: &'a TiingoMetadataReceipt,
        latest: &'a TiingoEodReceipt,
        value: market_squawk_domain::FundNavValue,
    ) -> Self {
        Self {
            metadata: TiingoMetadataNativeV1::from_receipt(metadata),
            metadata_request: TiingoRequestNativeV1::latest(metadata.evidence().request()),
            latest_request: TiingoRequestNativeV1::latest(latest.evidence().request()),
            metadata_disposition: metadata.disposition().into(),
            latest_disposition: latest.disposition().into(),
            nav_disposition: match value {
                market_squawk_domain::FundNavValue::Observed(_) => "observed_raw_close",
                market_squawk_domain::FundNavValue::Missing(_) => "missing",
            },
        }
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoEodGapNativeV1 {
    provider_date: String,
    provider_row_index: u32,
    surface: &'static str,
    reason: &'static str,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoEodActionNativeV1 {
    provider_date: String,
    provider_row_index: u32,
    cash_dividend: Option<String>,
    split_factor: Option<String>,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TiingoEodSidecarV1<'a> {
    metadata: TiingoMetadataNativeV1<'a>,
    metadata_request: TiingoRequestNativeV1<'a>,
    latest_request: TiingoRequestNativeV1<'a>,
    metadata_disposition: TiingoDispositionNativeV1,
    latest_disposition: TiingoDispositionNativeV1,
    gaps: Vec<TiingoEodGapNativeV1>,
    provider_actions: Vec<TiingoEodActionNativeV1>,
}

impl<'a> TiingoEodSidecarV1<'a> {
    fn try_new(
        metadata: &'a TiingoMetadataReceipt,
        latest: &'a TiingoEodReceipt,
        page: &crate::TiingoEodPageCandidate,
    ) -> Result<Self, TiingoLatestPublicationError> {
        let mut gaps = Vec::new();
        gaps.try_reserve_exact(page.gaps().len())
            .map_err(|_| TiingoLatestPublicationError::Allocation)?;
        gaps.extend(page.gaps().iter().map(gap_native));
        let mut provider_actions = Vec::new();
        provider_actions
            .try_reserve_exact(page.provider_actions().len())
            .map_err(|_| TiingoLatestPublicationError::Allocation)?;
        provider_actions.extend(page.provider_actions().iter().map(action_native));
        Ok(Self {
            metadata: TiingoMetadataNativeV1::from_receipt(metadata),
            metadata_request: TiingoRequestNativeV1::latest(metadata.evidence().request()),
            latest_request: TiingoRequestNativeV1::latest(latest.evidence().request()),
            metadata_disposition: metadata.disposition().into(),
            latest_disposition: latest.disposition().into(),
            gaps,
            provider_actions,
        })
    }
}

fn gap_native(gap: &TiingoEodSurfaceGap) -> TiingoEodGapNativeV1 {
    TiingoEodGapNativeV1 {
        provider_date: gap.provider_date().to_string(),
        provider_row_index: gap.provider_row_index(),
        surface: surface_name(gap.surface()),
        reason: match gap.reason() {
            TiingoEodSurfaceGapReason::MissingOhlc => "missing_ohlc",
            TiingoEodSurfaceGapReason::MissingVolume => "missing_volume",
        },
    }
}

fn action_native(action: &TiingoEodProviderActionEvidence) -> TiingoEodActionNativeV1 {
    TiingoEodActionNativeV1 {
        provider_date: action.provider_date().to_string(),
        provider_row_index: action.provider_row_index(),
        cash_dividend: decimal_string(action.cash_dividend()),
        split_factor: decimal_string(action.split_factor()),
    }
}

const fn endpoint_name(endpoint: TiingoEndpointFamily) -> &'static str {
    match endpoint {
        TiingoEndpointFamily::Metadata => "metadata",
        TiingoEndpointFamily::LatestDailyPrices => "latest_daily_prices",
        TiingoEndpointFamily::HistoricalDailyPrices => "historical_daily_prices",
    }
}

const fn scope_name(scope: &TiingoRequestScope) -> &'static str {
    match scope {
        TiingoRequestScope::Metadata => "metadata",
        TiingoRequestScope::Latest => "latest",
        TiingoRequestScope::History { .. } => "history",
    }
}

const fn surface_name(surface: TiingoEodSurface) -> &'static str {
    match surface {
        TiingoEodSurface::Raw => "raw",
        TiingoEodSurface::Adjusted => "adjusted",
    }
}

fn decimal_string(value: Option<Decimal>) -> Option<String> {
    value.map(|value| value.normalize().to_string())
}

fn identifier(value: &str) -> Result<SourceIdentifier, TiingoLatestPublicationError> {
    SourceIdentifier::try_from(value)
        .map_err(|_| TiingoLatestPublicationError::InvalidCanonicalIdentity)
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

/// Closed failure to carry one Tiingo latest graph through sealed canonical publication.
#[derive(Debug, Error)]
pub enum TiingoLatestPublicationError {
    /// One standalone raw response could not become exact source-neutral capture material.
    #[error(transparent)]
    CaptureMaterial(#[from] TiingoCaptureMaterialError),
    /// Common request-graph, seal, rejoin, or canonical raw binding failed.
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    /// Tiingo strict NAV classification or row selection failed.
    #[error(transparent)]
    Adapter(#[from] TiingoAdapterError),
    /// Tiingo canonical FundNav mapping rejected the exact evidence.
    #[error(transparent)]
    FundNav(#[from] TiingoFundNavMapError),
    /// Tiingo raw/adjusted EOD mapping rejected the exact evidence.
    #[error(transparent)]
    Eod(#[from] TiingoEodMapError),
    /// Shared extraction construction or revision admission failed.
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    /// Provider-native lineage could not remain bounded and aligned to canonical rows.
    #[error(transparent)]
    NativeLineage(#[from] ProviderNativeLineageError),
    /// Local revision authority could not remain bounded and aligned to canonical rows.
    #[error(transparent)]
    Revision(#[from] ObservedRevisionError),
    /// Canonical research provenance or observation invariants failed.
    #[error(transparent)]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
    /// Canonical research observation invariants failed.
    #[error(transparent)]
    Research(#[from] market_squawk_domain::ResearchError),
    /// Metadata and latest response did not form the exact admitted pair.
    #[error("Tiingo metadata/latest response pair is invalid")]
    InvalidLatestResponse,
    /// The caller routed a mutual fund to EOD or an equity/ETF to NAV.
    #[error("Tiingo latest publication was routed to the wrong instrument family")]
    WrongInstrumentFamily,
    /// The shared extraction request did not identify this exact request graph and clock boundary.
    #[error("Tiingo extraction request does not match the sealed latest graph")]
    InvalidExtractionRequest,
    /// One code-owned dataset, schema, source, or revision identity was invalid.
    #[error("Tiingo canonical publication identity is invalid")]
    InvalidCanonicalIdentity,
    /// Canonical observation serialization failed.
    #[error("Tiingo canonical observation encoding failed")]
    CanonicalEncoding,
    /// Bounded publication allocation failed.
    #[error("Tiingo publication allocation failed")]
    Allocation,
    /// A sealed result did not match the pending graph expectation.
    #[error("Tiingo sealed latest graph does not match its pending publication")]
    CaptureBinding,
}
