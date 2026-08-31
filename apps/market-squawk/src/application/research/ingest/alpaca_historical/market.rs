//! Alpaca-only immutable current-event and indicative option-market publication.
//!
//! The adapter has already joined canonical rows, exact provider-native semantics, and sealed
//! raw coordinates. This boundary binds those non-cloneable inputs to registered source rights,
//! an exact active-account precommit authority, and restart-safe immutable catalog selectors.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_adapter_alpaca::{
    AlpacaError, AlpacaIexDecoder, AlpacaMarketDecodeHandoff, AlpacaOptionChainPublicationRequest,
    AlpacaOptionChainSealRejoin, AlpacaOptionsDecoder,
};
use market_squawk_data::{
    DatasetId, DatasetManifestRef, IngestError, IngestIdentity, IngestPrecommitAuthority,
    OptionMarketPointInTimeRequest, OptionMarketPointInTimeSelection,
    PersistedProviderOptionMarketBindingEvidence, ProviderMarketEventPublicationKind,
    ProviderOptionMarketArrowBatch, RightsError, SourceOperation,
    provider_market_event_publication_digest, provider_option_market_publication_digest,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentDefinition, InstrumentId, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    OptionMarketBatchDisposition, OptionMarketBatchKind, OptionMarketCursorState,
    OptionMarketRequestFilter, ProviderCaptureError, ProviderCaptureSealRequest,
    ProviderNativeLineageImplementation, SealedProviderOptionMarketBinding,
    SealedProviderPublicationBinding, SourceClass, SourceMetadata, SourceMetadataProvider,
    SourceProtocolProfile, ValidatedRawMarketFrame,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::super::{
    MarketEventPointInTimeSelector, MarketEventPublicationReceipt,
    MarketEventSealedReceiptEvidence, ResearchRightsAuthority,
};
use crate::{ResearchService, ResearchServiceError};

const ALPACA_PROVIDER: &str = "alpaca-market-data";
const ALPACA_IEX_PRODUCT: &str = "alpaca-basic-iex-configured-symbols-v1";
const ALPACA_IEX_CHANNEL: &str = "trades+quotes+statuses";
const ALPACA_OPTION_STREAM_PRODUCT: &str = "alpaca-basic-indicative-options-configured-symbols-v1";
const ALPACA_OPTION_STREAM_CHANNEL: &str = "trades+quotes-msgpack";
const ALPACA_OPTION_CHAIN_PRODUCT: &str = "alpaca-basic-indicative-option-snapshots-v1";
const ALPACA_OPTION_CHAIN_CHANNEL: &str = "rest-complete-chain-snapshots";
const ALPACA_IEX_DATASET_PREFIX: &str = "alpaca:iex-market-events:v1:";
const ALPACA_OPTION_EVENT_DATASET_PREFIX: &str = "alpaca:indicative-option-market-events:v1:";
const ALPACA_OPTION_CHAIN_DATASET_PREFIX: &str = "alpaca:indicative-option-chain:v1:";
const ALPACA_OPTION_NATIVE_IMPLEMENTATION: &str = "alpaca_indicative_options_v1";

/// One exact registered Alpaca source and its immutable persistence authority.
pub(crate) struct AlpacaMarketPublicationClosure {
    research: Arc<ResearchService>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
    surface: AlpacaPublicationSurface,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AlpacaPublicationSurface {
    IexLive,
    IndicativeOptionsLive,
    IndicativeOptionChain,
}

impl fmt::Debug for AlpacaMarketPublicationClosure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaMarketPublicationClosure")
            .field("source_id", self.source.source_id())
            .field("metadata_revision", self.source.revision())
            .field("source_registered_at", &self.source_registered_at)
            .field("surface", &self.surface)
            .finish_non_exhaustive()
    }
}

impl AlpacaMarketPublicationClosure {
    /// Binds a registered Alpaca live or complete-chain source without widening its surface.
    pub(crate) fn try_new(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, AlpacaMarketPublicationError> {
        if source.source_id() != rights.source_id()
            || source.provider().as_str() != ALPACA_PROVIDER
            || source.source_class() != SourceClass::Broker
            || !source.is_effective_at(source_registered_at)
        {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        let surface = classify_surface(&source)?;
        Ok(Self {
            research,
            source,
            rights,
            source_registered_at,
            surface,
        })
    }

    /// Runs the closed adapter decoder/preparer used by the production IEX publication lane.
    ///
    /// The application supplies only exact reference definitions and the publication clock. The
    /// adapter consumes the validated provider response/stream frame and remains the sole creator
    /// of the canonical/native prepared material.
    pub(crate) fn decode_iex_for_publication(
        &self,
        decoder: &mut AlpacaIexDecoder,
        frame: &ValidatedRawMarketFrame<'_>,
        definitions: &[InstrumentDefinition],
        ingested_at: Timestamp,
    ) -> Result<AlpacaMarketDecodeHandoff, AlpacaMarketPublicationError> {
        if self.surface != AlpacaPublicationSurface::IexLive
            || decoder.metadata() != &self.source
            || ingested_at < frame.frame().received_at()
        {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        decoder
            .decode_for_publication(frame, definitions, ingested_at)
            .map_err(Into::into)
    }

    /// Runs the closed adapter decoder/preparer used by the production indicative-option stream.
    pub(crate) fn decode_indicative_options_for_publication(
        &self,
        decoder: &mut AlpacaOptionsDecoder,
        frame: &ValidatedRawMarketFrame<'_>,
        definitions: &[InstrumentDefinition],
        ingested_at: Timestamp,
    ) -> Result<AlpacaMarketDecodeHandoff, AlpacaMarketPublicationError> {
        if self.surface != AlpacaPublicationSurface::IndicativeOptionsLive
            || decoder.metadata() != &self.source
            || ingested_at < frame.frame().received_at()
        {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        decoder
            .decode_for_publication(frame, definitions, ingested_at)
            .map_err(Into::into)
    }

    /// Atomically publishes one adapter-sealed IEX response or IEX/indicative stream microbatch.
    pub(crate) async fn publish_market_events(
        &self,
        binding: SealedProviderPublicationBinding,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<MarketEventPublicationReceipt, AlpacaMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        precommit_authority.validate_precommit()?;
        let prepared = self.validate_market_binding(&binding, observed_at)?;
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        require_digest(publication_digest)?;
        let reservation = self
            .reserve(
                publication_digest,
                idempotency_key.into(),
                observed_at,
                &cancellation,
            )
            .await?;
        let committed = self
            .research
            .analytical()
            .ingest_provider_market_events(
                reservation,
                analytical_dataset,
                binding,
                cancellation,
                precommit_authority,
            )
            .await?;
        MarketEventPublicationReceipt::try_new(
            committed.manifest().clone(),
            publication_digest,
            prepared.kind,
            prepared.implementation,
            self.source.source_id().clone(),
            prepared.provider_dataset,
            MarketEventSealedReceiptEvidence::Single(prepared.sealed_receipt),
            prepared.event_count,
        )
        .map_err(Into::into)
    }

    /// Returns the common exact-source current/PIT selector for this Alpaca event surface.
    pub(crate) fn market_event_point_in_time_selector(
        &self,
        analytical_dataset: DatasetId,
    ) -> Result<MarketEventPointInTimeSelector, AlpacaMarketPublicationError> {
        if !matches!(
            self.surface,
            AlpacaPublicationSurface::IexLive | AlpacaPublicationSurface::IndicativeOptionsLive
        ) {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        Ok(MarketEventPointInTimeSelector::new(
            Arc::clone(&self.research),
            analytical_dataset,
            self.source.source_id().clone(),
        ))
    }

    /// Atomically publishes one complete, terminal indicative option-chain snapshot.
    #[allow(
        clippy::too_many_arguments,
        reason = "raw sealing, canonical authority, immutable target, and lifecycle stay explicit"
    )]
    pub(crate) async fn seal_and_publish_option_chain(
        &self,
        rejoin: AlpacaOptionChainSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        publication: AlpacaOptionChainPublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<AlpacaOptionMarketPublicationReceipt, AlpacaMarketPublicationError> {
        if self.surface != AlpacaPublicationSurface::IndicativeOptionChain
            || rejoin.metadata() != &self.source
            || !rejoin
                .dataset()
                .as_str()
                .starts_with(ALPACA_OPTION_CHAIN_DATASET_PREFIX)
        {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        self.validate_current_authority(observed_at)?;
        precommit_authority.validate_precommit()?;
        let binding = rejoin.try_rejoin(sealed, publication)?.try_into_binding()?;
        self.publish_option_market(
            binding,
            analytical_dataset,
            idempotency_key,
            observed_at,
            precommit_authority,
            cancellation,
        )
        .await
    }

    /// Atomically publishes an already sealed complete, terminal indicative option-chain snapshot.
    pub(crate) async fn publish_option_market(
        &self,
        binding: SealedProviderOptionMarketBinding,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<AlpacaOptionMarketPublicationReceipt, AlpacaMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        precommit_authority.validate_precommit()?;
        let prepared = self.validate_option_binding(&binding, observed_at)?;
        let publication_digest = provider_option_market_publication_digest(&binding)?;
        if publication_digest != binding.evidence_digest().evidence() {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        require_digest(publication_digest)?;
        let reservation = self
            .reserve(
                publication_digest,
                idempotency_key.into(),
                observed_at,
                &cancellation,
            )
            .await?;
        let committed = self
            .research
            .analytical()
            .ingest_provider_option_market(
                reservation,
                analytical_dataset,
                binding,
                cancellation,
                precommit_authority,
            )
            .await?;
        Ok(AlpacaOptionMarketPublicationReceipt {
            restart: AlpacaOptionMarketRestartSelector {
                manifest: committed.manifest().clone(),
                publication_digest,
                publication_kind: prepared.publication_kind,
                source_id: self.source.source_id().clone(),
                provider_dataset: prepared.provider_dataset.clone(),
                expected_option_row_count: prepared.option_row_count,
            },
            manifest: committed.manifest().clone(),
            publication_digest,
            provider_dataset: prepared.provider_dataset,
            option_row_count: prepared.option_row_count,
        })
    }

    /// Returns an exact-source whole-batch point-in-time selector for option chains.
    pub(crate) fn option_point_in_time_selector(
        &self,
        analytical_dataset: DatasetId,
    ) -> Result<AlpacaOptionMarketPointInTimeSelector, AlpacaMarketPublicationError> {
        if self.surface != AlpacaPublicationSurface::IndicativeOptionChain {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        Ok(AlpacaOptionMarketPointInTimeSelector {
            research: Arc::clone(&self.research),
            analytical_dataset,
            source_id: self.source.source_id().clone(),
        })
    }

    fn validate_market_binding(
        &self,
        binding: &SealedProviderPublicationBinding,
        observed_at: Timestamp,
    ) -> Result<PreparedMarketEvent, AlpacaMarketPublicationError> {
        let (kind, implementation, source_id, revision, provider_dataset, sealed, count) =
            match binding {
                SealedProviderPublicationBinding::ResponseMarketEvent(response) => {
                    response.validate()?;
                    if response
                        .capture_evidence()
                        .pages()
                        .iter()
                        .any(|page| page.received_at() > observed_at)
                    {
                        return Err(AlpacaMarketPublicationError::FamilyMismatch);
                    }
                    (
                        ProviderMarketEventPublicationKind::ResponseMarketEvent,
                        response.native_lineage().implementation(),
                        response.capture_evidence().source_id(),
                        response.capture_evidence().metadata_revision(),
                        response.capture_evidence().dataset().clone(),
                        response.sealed_receipt_digest(),
                        response.record_count(),
                    )
                }
                SealedProviderPublicationBinding::EventMicrobatch(event) => {
                    event.validate()?;
                    if event
                        .capture_evidence()
                        .frames()
                        .iter()
                        .any(|frame| frame.received_at() > observed_at)
                    {
                        return Err(AlpacaMarketPublicationError::FamilyMismatch);
                    }
                    (
                        ProviderMarketEventPublicationKind::EventMicrobatch,
                        event.native_lineage().implementation(),
                        event.capture_evidence().source_id(),
                        event.capture_evidence().metadata_revision(),
                        event.capture_evidence().dataset().clone(),
                        event.sealed_receipt_digest(),
                        event.record_count(),
                    )
                }
                SealedProviderPublicationBinding::ResponseSet(_)
                | SealedProviderPublicationBinding::CompositeResponseEvent(_) => {
                    return Err(AlpacaMarketPublicationError::FamilyMismatch);
                }
            };
        self.validate_source_binding(source_id, revision)?;
        let expected = match self.surface {
            AlpacaPublicationSurface::IexLive => {
                if !provider_dataset
                    .as_str()
                    .starts_with(ALPACA_IEX_DATASET_PREFIX)
                {
                    return Err(AlpacaMarketPublicationError::FamilyMismatch);
                }
                ProviderNativeLineageImplementation::AlpacaIexMarketDataV1
            }
            AlpacaPublicationSurface::IndicativeOptionsLive => {
                if kind != ProviderMarketEventPublicationKind::EventMicrobatch
                    || !provider_dataset
                        .as_str()
                        .starts_with(ALPACA_OPTION_EVENT_DATASET_PREFIX)
                {
                    return Err(AlpacaMarketPublicationError::FamilyMismatch);
                }
                ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1
            }
            AlpacaPublicationSurface::IndicativeOptionChain => {
                return Err(AlpacaMarketPublicationError::FamilyMismatch);
            }
        };
        if implementation != expected || count == 0 {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        require_digest(sealed)?;
        Ok(PreparedMarketEvent {
            kind,
            implementation,
            provider_dataset,
            sealed_receipt: sealed,
            event_count: count,
        })
    }

    fn validate_option_binding(
        &self,
        binding: &SealedProviderOptionMarketBinding,
        observed_at: Timestamp,
    ) -> Result<PreparedOptionMarket, AlpacaMarketPublicationError> {
        if self.surface != AlpacaPublicationSurface::IndicativeOptionChain {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        binding.validate()?;
        let batch = binding.batch();
        let scope = batch.scope();
        self.validate_source_binding(scope.source_id(), scope.metadata_revision())?;
        if batch.kind() != OptionMarketBatchKind::Snapshots
            || batch.row_count() == 0
            || batch.completeness().disposition() != OptionMarketBatchDisposition::Complete
            || batch.completeness().cursor() != OptionMarketCursorState::Exhausted
            || scope.received_at() > observed_at
            || scope.ingested_at() > observed_at
            || scope.provider_product().as_source_identifier().as_str()
                != ALPACA_OPTION_CHAIN_PRODUCT
            || scope.provider_channel().as_source_identifier().as_str()
                != ALPACA_OPTION_CHAIN_CHANNEL
            || !scope
                .dataset()
                .as_str()
                .starts_with(ALPACA_OPTION_CHAIN_DATASET_PREFIX)
            || binding.native_lineage().schema().implementation()
                != ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1
            || binding.persisted_receipt().capture().source_id() != scope.source_id()
            || binding.persisted_receipt().capture().metadata_revision()
                != scope.metadata_revision()
            || binding.persisted_receipt().capture().dataset() != scope.dataset()
        {
            return Err(AlpacaMarketPublicationError::FamilyMismatch);
        }
        Ok(PreparedOptionMarket {
            publication_kind: batch.kind(),
            provider_dataset: scope.dataset().clone(),
            option_row_count: batch.row_count(),
        })
    }

    async fn reserve(
        &self,
        publication_digest: EvidenceDigest,
        idempotency_key: String,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::IngestReservation, AlpacaMarketPublicationError> {
        let identity = IngestIdentity::try_new(
            self.source.source_id().clone(),
            publication_digest,
            SourceOperation::Persist,
            idempotency_key,
        )?;
        let rights = self.rights.decision(publication_digest, observed_at)?;
        self.research
            .analytical()
            .reserve_source_ingest(
                &self.source,
                self.source_registered_at,
                rights,
                &identity,
                cancellation,
            )
            .await
            .map_err(Into::into)
    }

    fn validate_current_authority(
        &self,
        observed_at: Timestamp,
    ) -> Result<(), AlpacaMarketPublicationError> {
        if observed_at < self.source_registered_at || !self.source.is_effective_at(observed_at) {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        self.rights.validate_at(observed_at)?;
        Ok(())
    }

    fn validate_source_binding(
        &self,
        source_id: &SourceId,
        revision: &market_squawk_domain::MetadataRevision,
    ) -> Result<(), AlpacaMarketPublicationError> {
        if source_id != self.source.source_id() || revision != self.source.revision() {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

struct PreparedMarketEvent {
    kind: ProviderMarketEventPublicationKind,
    implementation: ProviderNativeLineageImplementation,
    provider_dataset: SourceIdentifier,
    sealed_receipt: EvidenceDigest,
    event_count: usize,
}

struct PreparedOptionMarket {
    publication_kind: OptionMarketBatchKind,
    provider_dataset: SourceIdentifier,
    option_row_count: usize,
}

/// Compact exact-generation receipt for one immutable Alpaca option-chain publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AlpacaOptionMarketPublicationReceipt {
    restart: AlpacaOptionMarketRestartSelector,
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    option_row_count: usize,
}

impl AlpacaOptionMarketPublicationReceipt {
    pub(crate) const fn restart_selector(&self) -> &AlpacaOptionMarketRestartSelector {
        &self.restart
    }
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
    pub(crate) const fn option_row_count(&self) -> usize {
        self.option_row_count
    }
}

/// Exact manifest/digest/kind/source selector for an Alpaca complete-chain publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AlpacaOptionMarketRestartSelector {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    expected_option_row_count: usize,
}

impl AlpacaOptionMarketRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
    pub(crate) const fn publication_kind(&self) -> OptionMarketBatchKind {
        self.publication_kind
    }
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Reopens the exact sealed raw/native evidence and typed canonical batch after restart.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<AlpacaOptionMarketRestartReceipt, AlpacaMarketPublicationError> {
        let selector = research
            .analytical()
            .provider_option_market_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(AlpacaMarketPublicationError::RestartInvalid)?;
        let store = research.provider_capture_store();
        let evidence = research
            .analytical()
            .provider_option_market_publication_evidence(
                &self.manifest,
                selector,
                store.as_ref(),
            )?;
        validate_option_restart_evidence(self, &evidence)?;
        let batch = research
            .analytical()
            .read_provider_option_market_publication(
                &self.manifest,
                selector,
                store.as_ref(),
                cancellation,
            )
            .await?;
        if batch.publication_digest() != self.publication_digest
            || batch.publication_kind() != self.publication_kind
            || batch.scope().source_id() != &self.source_id
            || batch.scope().dataset() != &self.provider_dataset
            || batch.snapshots().map(<[_]>::len) != Some(self.expected_option_row_count)
        {
            return Err(AlpacaMarketPublicationError::RestartInvalid);
        }
        Ok(AlpacaOptionMarketRestartReceipt { batch, evidence })
    }
}

/// Restart-verified Alpaca option raw/native evidence and typed canonical rows.
#[derive(Debug)]
pub(crate) struct AlpacaOptionMarketRestartReceipt {
    batch: ProviderOptionMarketArrowBatch,
    evidence: PersistedProviderOptionMarketBindingEvidence,
}

impl AlpacaOptionMarketRestartReceipt {
    pub(crate) const fn batch(&self) -> &ProviderOptionMarketArrowBatch {
        &self.batch
    }
    pub(crate) const fn evidence(&self) -> &PersistedProviderOptionMarketBindingEvidence {
        &self.evidence
    }
}

fn validate_option_restart_evidence(
    expected: &AlpacaOptionMarketRestartSelector,
    evidence: &PersistedProviderOptionMarketBindingEvidence,
) -> Result<(), AlpacaMarketPublicationError> {
    if evidence.binding_digest() != expected.publication_digest
        || evidence.publication_kind() != expected.publication_kind
        || evidence.capture().source_id() != &expected.source_id
        || evidence.capture().dataset() != &expected.provider_dataset
        || evidence.canonical_row_count() != expected.expected_option_row_count
        || evidence.native_lineage().implementation() != ALPACA_OPTION_NATIVE_IMPLEMENTATION
        || evidence.native_lineage().row_count() != expected.expected_option_row_count
    {
        return Err(AlpacaMarketPublicationError::RestartInvalid);
    }
    Ok(())
}

/// Exact-source whole-batch point-in-time selector for indicative Alpaca option chains.
#[derive(Clone)]
pub(crate) struct AlpacaOptionMarketPointInTimeSelector {
    research: Arc<ResearchService>,
    analytical_dataset: DatasetId,
    source_id: SourceId,
}

impl fmt::Debug for AlpacaOptionMarketPointInTimeSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaOptionMarketPointInTimeSelector")
            .field("analytical_dataset", &self.analytical_dataset)
            .field("source_id", &self.source_id)
            .finish_non_exhaustive()
    }
}

impl AlpacaOptionMarketPointInTimeSelector {
    /// Selects the latest complete option batch known by the fixed cutoff for one underlying.
    pub(crate) async fn select_latest(
        &self,
        underlying_instrument_id: InstrumentId,
        filter: OptionMarketRequestFilter,
        knowledge_cutoff: Timestamp,
        maximum_canonical_rows: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<AlpacaOptionMarketPointInTimeReceipt>, AlpacaMarketPublicationError> {
        let request = OptionMarketPointInTimeRequest::try_latest(
            self.analytical_dataset.clone(),
            underlying_instrument_id,
            OptionMarketBatchKind::Snapshots,
            &filter,
            knowledge_cutoff,
            maximum_canonical_rows,
        )?;
        let store = self.research.provider_capture_store();
        let selection = self
            .research
            .analytical()
            .read_provider_option_market_point_in_time(&request, store.as_ref(), cancellation)
            .await?;
        selection
            .map(|selection| {
                AlpacaOptionMarketPointInTimeReceipt::try_new(
                    self,
                    filter.clone(),
                    knowledge_cutoff,
                    maximum_canonical_rows,
                    selection,
                )
            })
            .transpose()
    }

    /// Reopens the originally selected exact manifest and rejects any selection drift.
    pub(crate) async fn verify_restart(
        &self,
        original: &AlpacaOptionMarketPointInTimeReceipt,
        cancellation: CancellationToken,
    ) -> Result<AlpacaOptionMarketPointInTimeReceipt, AlpacaMarketPublicationError> {
        original.validate_selector(self)?;
        let request = OptionMarketPointInTimeRequest::try_exact(
            self.analytical_dataset.clone(),
            original.underlying_instrument_id,
            OptionMarketBatchKind::Snapshots,
            &original.filter,
            original.knowledge_cutoff,
            original.maximum_canonical_rows,
            original.selection.manifest().clone(),
        )?;
        let store = self.research.provider_capture_store();
        let replay = self
            .research
            .analytical()
            .read_provider_option_market_point_in_time(&request, store.as_ref(), cancellation)
            .await?
            .ok_or(AlpacaMarketPublicationError::RestartInvalid)?;
        if replay.selection_digest() != original.selection.selection_digest()
            || replay.batch().publication_digest()
                != original.selection.batch().publication_digest()
        {
            return Err(AlpacaMarketPublicationError::RestartInvalid);
        }
        AlpacaOptionMarketPointInTimeReceipt::try_new(
            self,
            original.filter.clone(),
            original.knowledge_cutoff,
            original.maximum_canonical_rows,
            replay,
        )
    }
}

/// Restart-verifiable exact-source option point-in-time selection.
#[derive(Clone, Debug)]
pub(crate) struct AlpacaOptionMarketPointInTimeReceipt {
    analytical_dataset: DatasetId,
    source_id: SourceId,
    underlying_instrument_id: InstrumentId,
    filter: OptionMarketRequestFilter,
    knowledge_cutoff: Timestamp,
    maximum_canonical_rows: usize,
    selection: OptionMarketPointInTimeSelection,
}

impl AlpacaOptionMarketPointInTimeReceipt {
    fn try_new(
        selector: &AlpacaOptionMarketPointInTimeSelector,
        filter: OptionMarketRequestFilter,
        knowledge_cutoff: Timestamp,
        maximum_canonical_rows: usize,
        selection: OptionMarketPointInTimeSelection,
    ) -> Result<Self, AlpacaMarketPublicationError> {
        let batch = selection.batch();
        if selection.manifest().dataset_id() != &selector.analytical_dataset
            || batch.scope().source_id() != &selector.source_id
            || batch.publication_kind() != OptionMarketBatchKind::Snapshots
            || batch.snapshots().is_none()
        {
            return Err(AlpacaMarketPublicationError::PointInTimeInvalid);
        }
        Ok(Self {
            analytical_dataset: selector.analytical_dataset.clone(),
            source_id: selector.source_id.clone(),
            underlying_instrument_id: batch.scope().underlying_instrument_id(),
            filter,
            knowledge_cutoff,
            maximum_canonical_rows,
            selection,
        })
    }

    pub(crate) const fn selection(&self) -> &OptionMarketPointInTimeSelection {
        &self.selection
    }

    fn validate_selector(
        &self,
        selector: &AlpacaOptionMarketPointInTimeSelector,
    ) -> Result<(), AlpacaMarketPublicationError> {
        if self.analytical_dataset != selector.analytical_dataset
            || self.source_id != selector.source_id
        {
            return Err(AlpacaMarketPublicationError::PointInTimeInvalid);
        }
        Ok(())
    }
}

fn classify_surface(
    source: &SourceMetadata,
) -> Result<AlpacaPublicationSurface, AlpacaMarketPublicationError> {
    if let Some(live) = source.coverage().live() {
        if !source.capabilities().live()
            || !matches!(source.protocol_profile(), SourceProtocolProfile::Live(_))
        {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        let product = live.provider_product().as_source_identifier().as_str();
        let channel = live.provider_channel().as_source_identifier().as_str();
        return match (source.quality_ceiling(), product, channel) {
            (DataQuality::DirectUnverified, ALPACA_IEX_PRODUCT, ALPACA_IEX_CHANNEL) => {
                Ok(AlpacaPublicationSurface::IexLive)
            }
            (
                DataQuality::Indicative,
                ALPACA_OPTION_STREAM_PRODUCT,
                ALPACA_OPTION_STREAM_CHANNEL,
            ) => Ok(AlpacaPublicationSurface::IndicativeOptionsLive),
            _ => Err(AlpacaMarketPublicationError::AuthorityInvalid),
        };
    }
    if source.quality_ceiling() == DataQuality::Indicative
        && source.coverage().live().is_none()
        && !source.capabilities().live()
        && source.capabilities().extraction()
        && source.protocol_profile() == &SourceProtocolProfile::NotLive
    {
        Ok(AlpacaPublicationSurface::IndicativeOptionChain)
    } else {
        Err(AlpacaMarketPublicationError::AuthorityInvalid)
    }
}

fn require_digest(digest: EvidenceDigest) -> Result<(), AlpacaMarketPublicationError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(AlpacaMarketPublicationError::AuthorityInvalid)
    } else {
        Ok(())
    }
}

/// Closed Alpaca immutable-publication and restart failure.
#[derive(Debug, Error)]
pub(crate) enum AlpacaMarketPublicationError {
    #[error("Alpaca market publication authority is invalid or no longer current")]
    AuthorityInvalid,
    #[error("sealed Alpaca evidence does not match the exact selected surface")]
    FamilyMismatch,
    #[error("the exact Alpaca immutable generation failed restart verification")]
    RestartInvalid,
    #[error("the Alpaca option point-in-time selection escaped its exact source surface")]
    PointInTimeInvalid,
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Decode(#[from] market_squawk_sources::DecodeInternalError),
    #[error(transparent)]
    Adapter(#[from] AlpacaError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Rights(#[from] RightsError),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Arrow(#[from] market_squawk_data::ArrowConversionError),
    #[error(transparent)]
    MarketEventRead(#[from] super::super::MarketEventReadError),
}
