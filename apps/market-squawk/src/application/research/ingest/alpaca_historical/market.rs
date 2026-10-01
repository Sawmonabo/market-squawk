//! Alpaca-only immutable current-event and indicative option-market publication.
//!
//! The adapter has already joined canonical rows, exact provider-native semantics, and sealed
//! raw coordinates. This boundary binds those non-cloneable inputs to registered source rights,
//! an exact active-account precommit authority, and restart-safe immutable catalog selectors.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_adapter_alpaca::{
    AlpacaError, AlpacaMarketSealRejoin, AlpacaOptionChainPublicationRequest,
    AlpacaOptionChainSealRejoin,
};
use market_squawk_data::{
    DatasetId, DatasetManifestRef, IngestError, IngestIdentity, IngestPrecommitAuthority,
    OptionMarketPointInTimeRequest, OptionMarketPointInTimeSelection,
    PersistedProviderOptionMarketBindingEvidence, ProviderMarketEventPublicationKind,
    ProviderOptionMarketArrowBatch, ProviderOptionMarketPublicationSelector, RightsError,
    SourceOperation, provider_market_event_publication_digest,
    provider_option_market_publication_digest,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, LiveProvenance,
    MarketDataReference, MarketEvent, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    CatalogProviderIdentityAuthority, CurrentCatalogProviderIdentity, OptionMarketBatchDisposition,
    OptionMarketBatchKind, OptionMarketCursorState, OptionMarketRequestFilter,
    ProviderCaptureError, ProviderCaptureSealRequest, ProviderIdentitySelectionEvidence,
    ProviderMarketEventBatch, ProviderNativeIdentityRequest, ProviderNativeLineageImplementation,
    SealedProviderOptionMarketBinding, SealedProviderPublicationBinding, SourceClass,
    SourceMetadata, SourceProtocolProfile,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::super::{
    MarketEventPointInTimeSelector, MarketEventPublicationReceipt,
    MarketEventSealedReceiptEvidence, ResearchIngestCompositionError, ResearchRightsAuthority,
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

    /// Atomically publishes one adapter-sealed IEX response or IEX/indicative stream microbatch.
    pub(crate) async fn publish_market_events(
        &self,
        binding: SealedProviderPublicationBinding,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketEventPublicationReceipt, AlpacaMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        precommit_authority.validate_precommit()?;
        let prepared = self.validate_market_binding(&binding, observed_at)?;
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        require_digest(publication_digest)?;
        if let Some(compaction) = self
            .research
            .analytical()
            .market_event_compaction_request(&analytical_dataset, 1)?
        {
            let digest = compaction.payload_digest();
            let key = format!(
                "market-compaction-{}",
                digest
                    .bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            );
            let reservation = self
                .reserve(digest, key, observed_at, &cancellation)
                .await?;
            self.research
                .analytical()
                .compact_provider_market_events(
                    reservation,
                    compaction,
                    deadline,
                    cancellation.clone(),
                    Arc::clone(&precommit_authority),
                )
                .await?;
        }
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
        let publication_kind = match self.publication_kind {
            OptionMarketBatchKind::Snapshots => "option_snapshots",
            OptionMarketBatchKind::Expirations => "option_expirations",
        };
        if !research.analytical().has_provider_publication(
            &self.manifest,
            self.publication_digest,
            publication_kind,
        )? {
            return Err(AlpacaMarketPublicationError::RestartInvalid);
        }
        let selector = ProviderOptionMarketPublicationSelector::new(
            self.publication_digest,
            self.publication_kind,
        );
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
        && source.coverage().live_channels().is_empty()
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
    #[error("Alpaca original capture custody failed")]
    Custody(#[source] ResearchServiceError),
    #[error("Alpaca market publication authority is invalid or no longer current")]
    AuthorityInvalid,
    #[error("Alpaca market publication admission failed: {0}")]
    PublicationAdmission(#[source] ResearchIngestCompositionError),
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

/// Exact registered generation, bounded references and neutral durable-read channel.
pub(crate) struct AlpacaPublicationRuntimeInput {
    coordinator: Arc<super::super::ProductionResearchIngestCoordinator>,
    publication: Arc<AlpacaMarketPublicationClosure>,
    generation: super::super::ResearchProviderRuntimeGeneration,
    registration: Arc<AlpacaPublicationRegistration>,
    records: Arc<[market_squawk_data::MarketDataInstrumentRecord]>,
    references: Arc<[MarketDataReference]>,
    identities: Arc<[Arc<dyn CurrentCatalogProviderIdentity>]>,
    currentness: crate::provider_activation::ProviderAccountRuntimeCurrentness,
    writer: super::super::MarketEventDurableReadWriter,
    read: super::super::MarketEventDurableRead,
}

pub(crate) struct AlpacaPublicationRegistration {
    admission: super::super::ResearchProviderAdmission,
    cancellation: CancellationToken,
}
impl Drop for AlpacaPublicationRegistration {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if !self.admission.revoke_if_idle() {
            tracing::error!("Alpaca registration dropped with an undrained publication lease");
        }
    }
}
impl fmt::Debug for AlpacaPublicationRuntimeInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaPublicationRuntimeInput")
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
impl super::super::ProductionResearchIngestCoordinator {
    pub(crate) fn bind_alpaca_publication_runtime(
        self: &Arc<Self>,
        generation: super::super::ResearchProviderRuntimeGeneration,
        bindings: &[crate::provider_activation::MarketDataInstrumentBinding],
        currentness: crate::provider_activation::ProviderAccountRuntimeCurrentness,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<AlpacaPublicationRuntimeInput, AlpacaMarketPublicationError> {
        let now = super::super::system_timestamp()
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        let (source, rights, registered_at, admission) = {
            let authority = self
                .authority
                .lock()
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
            let registered = authority
                .publication_sources
                .get(generation.profile())
                .ok_or(AlpacaMarketPublicationError::AuthorityInvalid)?;
            if registered.generation != generation
                || !registered
                    .admission
                    .admits_generation(&generation)
                    .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?
            {
                return Err(AlpacaMarketPublicationError::AuthorityInvalid);
            }
            (
                registered.metadata.clone(),
                registered.rights.clone(),
                registered.registered_at,
                registered.admission.clone(),
            )
        };
        let registration = Arc::new(AlpacaPublicationRegistration {
            admission,
            cancellation,
        });
        let catalog = self.research.market_data_instruments();
        let mut records = Vec::new();
        let mut references = Vec::new();
        let mut identities = Vec::new();
        records
            .try_reserve_exact(bindings.len())
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        references
            .try_reserve_exact(bindings.len())
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        identities
            .try_reserve_exact(bindings.len())
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        for binding in bindings {
            let record = catalog
                .latest(
                    binding.instrument_id(),
                    deadline,
                    &registration.cancellation,
                )
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?
                .ok_or(AlpacaMarketPublicationError::AuthorityInvalid)?;
            let reference = binding
                .publication_reference(&record, now)
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
            let native = binding
                .native_identity()
                .ok_or(AlpacaMarketPublicationError::AuthorityInvalid)?;
            validate_alpaca_native_route(&source, native, &reference)?;
            let selected = catalog
                .select_current(native, deadline, &registration.cancellation)
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
            if &selected.evidence().native != native {
                return Err(AlpacaMarketPublicationError::AuthorityInvalid);
            }
            references.push(reference);
            identities.push(selected);
            records.push(record);
        }
        if references.is_empty() {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        let publication = Arc::new(AlpacaMarketPublicationClosure::try_new(
            Arc::clone(&self.research),
            source,
            rights,
            registered_at,
        )?);
        let dataset = DatasetId::try_from("market_squawk.market_events")
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        let selector = publication.market_event_point_in_time_selector(dataset)?;
        let (writer, read) = super::super::MarketEventDurableRead::channel(selector);
        Ok(AlpacaPublicationRuntimeInput {
            coordinator: Arc::clone(self),
            publication,
            generation,
            registration,
            records: records.into(),
            references: references.into(),
            identities: identities.into(),
            currentness,
            writer,
            read,
        })
    }
}
impl AlpacaPublicationRuntimeInput {
    pub(crate) fn references(&self) -> Arc<[MarketDataReference]> {
        Arc::clone(&self.references)
    }
    pub(crate) fn durable_read(&self) -> super::super::MarketEventDurableRead {
        self.read.clone()
    }
    pub(crate) fn begin_shutdown(&self) {
        self.registration.admission.revoke();
        self.registration.cancellation.cancel();
    }
    pub(crate) async fn finish_shutdown(&self) {
        self.registration.admission.revoke_and_drain().await;
    }

    /// Custody always precedes account/generation/catalog checks. Revocation cannot discard originals.
    pub(crate) async fn publish(
        &self,
        rejoin: AlpacaMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
        deadline: Instant,
    ) -> Result<AlpacaLivePublicationOutcome, AlpacaMarketPublicationError> {
        let custody = CancellationToken::new();
        let sealed = self
            .publication
            .research
            .seal_provider_capture(seal_request, &custody, deadline)
            .await
            .map_err(AlpacaMarketPublicationError::Custody)?;
        if self.registration.cancellation.is_cancelled() {
            return Ok(AlpacaLivePublicationOutcome::RawRetained);
        }
        // Prepared account startup must reach its atomic activation before canonical publication.
        while !self.currentness.is_active_now() {
            if self.registration.cancellation.is_cancelled() {
                return Ok(AlpacaLivePublicationOutcome::RawRetained);
            }
            let prepared_or_active = tokio::select! {
                biased;
                () = self.registration.cancellation.cancelled() => {
                    return Ok(AlpacaLivePublicationOutcome::RawRetained);
                }
                () = tokio::time::sleep_until(deadline.into()) => {
                    return Err(ServiceError::DeadlineExceeded.into());
                }
                current = self.currentness.is_prepared_or_active() => current,
            };
            if !prepared_or_active {
                return Err(AlpacaMarketPublicationError::AuthorityInvalid);
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let account = tokio::select! {
            biased;
            () = self.registration.cancellation.cancelled() => {
                return Ok(AlpacaLivePublicationOutcome::RawRetained);
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                return Err(ServiceError::DeadlineExceeded.into());
            }
            result = self.currentness.acquire_publication_authority() => {
                result.map_err(|error| {
                    tracing::warn!(%error, "Alpaca publication account lease validation failed");
                    AlpacaMarketPublicationError::AuthorityInvalid
                })?
            }
        };
        let operation = match self
            .coordinator
            .acquire_provider_publication_operation(
                &self.generation,
                self.registration.cancellation.clone(),
                deadline,
            )
            .await
        {
            Ok(operation) => operation,
            // Custody is complete and no ingest reservation exists yet. Coordinator shutdown
            // closes canonical admission without invalidating the retained original capture.
            Err(ResearchIngestCompositionError::ShuttingDown) => {
                return Ok(AlpacaLivePublicationOutcome::RawRetained);
            }
            Err(error) => {
                return Err(AlpacaMarketPublicationError::PublicationAdmission(error));
            }
        };
        let precommit = Arc::new(AlpacaReferencePrecommit {
            account,
            publication: operation.precommit_authority(),
            catalog: self.publication.research.market_data_instruments(),
            records: Arc::clone(&self.records),
            references: Arc::clone(&self.references),
            identities: Arc::clone(&self.identities),
            deadline,
            cancellation: operation.cancellation().clone(),
        });
        let binding = rejoin.try_rejoin(sealed)?;
        // The sealed adapter batch keeps original HTTP response / stream frame row ordinals.
        // Attach each accepted catalog selection at that canonical ordinal before reservation.
        let binding =
            attach_alpaca_selected_identities(binding, &self.references, &self.identities)?;
        let digest = provider_market_event_publication_digest(&binding)?;
        let idempotency = format!(
            "alpaca-current-{}",
            digest
                .bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let receipt = self
            .publication
            .publish_market_events(
                binding,
                DatasetId::try_from("market_squawk.market_events")
                    .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?,
                idempotency,
                observed_at,
                precommit,
                deadline,
                operation.cancellation().clone(),
            )
            .await?;
        if !self.writer.retain(receipt).await? {
            return Err(AlpacaMarketPublicationError::RestartInvalid);
        }
        Ok(AlpacaLivePublicationOutcome::CanonicalPublished)
    }
}

/// Checks the catalog-selected native route against the accepted publication reference.
fn validate_alpaca_native_route(
    source: &SourceMetadata,
    native: &ProviderNativeIdentityRequest,
    reference: &MarketDataReference,
) -> Result<(), AlpacaMarketPublicationError> {
    let venue = match classify_surface(source)? {
        AlpacaPublicationSurface::IexLive => "iex",
        AlpacaPublicationSurface::IndicativeOptionsLive => "alpaca-indicative-options",
        AlpacaPublicationSurface::IndicativeOptionChain => {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
    };
    // The reference retains its independent assigned ticker/OCC proof and subscribed symbol.
    // The opaque catalog selection separately proves the official Alpaca asset/contract ID.
    if native.instrument != reference.instrument_id()
        || native.venue.as_str() != venue
        || native.venue_symbol.as_str() != reference.source_symbol().as_str()
    {
        return Err(AlpacaMarketPublicationError::AuthorityInvalid);
    }
    Ok(())
}

/// Joins one catalog selection to each accepted canonical row in the sealed original order.
fn attach_alpaca_selected_identities(
    binding: SealedProviderPublicationBinding,
    references: &[MarketDataReference],
    identities: &[Arc<dyn CurrentCatalogProviderIdentity>],
) -> Result<SealedProviderPublicationBinding, AlpacaMarketPublicationError> {
    match binding {
        SealedProviderPublicationBinding::ResponseMarketEvent(response) => {
            let selections = alpaca_row_selections(response.batch(), references, identities)?;
            Ok(SealedProviderPublicationBinding::ResponseMarketEvent(
                response.with_provider_identities(selections)?,
            ))
        }
        SealedProviderPublicationBinding::EventMicrobatch(event) => {
            let selections = alpaca_row_selections(event.batch(), references, identities)?;
            Ok(SealedProviderPublicationBinding::EventMicrobatch(
                event.with_provider_identities(selections)?,
            ))
        }
        SealedProviderPublicationBinding::ResponseSet(_)
        | SealedProviderPublicationBinding::CompositeResponseEvent(_) => {
            Err(AlpacaMarketPublicationError::FamilyMismatch)
        }
    }
}

fn alpaca_row_selections(
    batch: &ProviderMarketEventBatch,
    references: &[MarketDataReference],
    identities: &[Arc<dyn CurrentCatalogProviderIdentity>],
) -> Result<Vec<Option<ProviderIdentitySelectionEvidence>>, AlpacaMarketPublicationError> {
    if references.len() != identities.len() {
        return Err(AlpacaMarketPublicationError::AuthorityInvalid);
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(batch.events().len())
        .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
    for event in batch.events() {
        let (provenance, event_reference): (&LiveProvenance, Option<&MarketDataReference>) =
            match event {
                MarketEvent::MarketDataTrade(trade) => {
                    (trade.provenance(), Some(trade.reference()))
                }
                MarketEvent::MarketDataQuote(quote) => {
                    (quote.provenance(), Some(quote.reference()))
                }
                MarketEvent::TradingHalt(halt) => (halt.provenance(), None),
                _ => return Err(AlpacaMarketPublicationError::FamilyMismatch),
            };
        let Some(instrument) = provenance.instrument_id() else {
            // A source-cohort event has no instrument identity and must retain None.
            rows.push(None);
            continue;
        };
        let mut matched = None;
        for (reference, identity) in references.iter().zip(identities.iter()) {
            let native = &identity.evidence().native;
            if native.instrument != instrument
                || provenance.venue_id() != Some(&native.venue)
                || provenance.source_identifier().as_str() != native.venue_symbol.as_str()
                || reference.instrument_id() != instrument
                || reference.source_symbol().as_str() != native.venue_symbol.as_str()
            {
                continue;
            }
            if event_reference.is_some_and(|row| row != reference)
                || matched.is_some()
                || native.knowledge_at > provenance.received_at()
                || native.effective_at > provenance.received_at()
            {
                return Err(AlpacaMarketPublicationError::AuthorityInvalid);
            }
            identity
                .validate_at(provenance.received_at())
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
            matched = Some(identity.evidence().clone());
        }
        rows.push(Some(
            matched.ok_or(AlpacaMarketPublicationError::AuthorityInvalid)?,
        ));
    }
    Ok(rows)
}

#[derive(Debug)]
struct AlpacaReferencePrecommit {
    account: crate::provider_activation::ProviderAccountPublicationAuthority,
    publication: Arc<dyn IngestPrecommitAuthority>,
    catalog: market_squawk_data::MarketDataInstrumentReadCapability,
    records: Arc<[market_squawk_data::MarketDataInstrumentRecord]>,
    references: Arc<[MarketDataReference]>,
    identities: Arc<[Arc<dyn CurrentCatalogProviderIdentity>]>,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl AlpacaReferencePrecommit {
    fn validate_time_and_references(&self) -> Result<(), IngestError> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        let now = super::super::system_timestamp()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        if self.records.len() != self.references.len()
            || self.records.len() != self.identities.len()
        {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        for ((record, reference), identity) in self
            .records
            .iter()
            .zip(self.references.iter())
            .zip(self.identities.iter())
        {
            identity
                .validate_at(now)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
            if record.revision_digest() != reference.definition_digest() {
                return Err(IngestError::PublicationAuthorityRevoked);
            }
            reference
                .validate_definition_at(record.definition(), now)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
}
impl IngestPrecommitAuthority for AlpacaReferencePrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.validate_time_and_references()?;
        self.publication.validate_precommit()?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
    fn validate_catalog_precommit(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), IngestError> {
        self.validate_time_and_references()?;
        self.publication.validate_catalog_precommit(catalog)?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        // All admitted references are checked, including a halt-only batch with no quote/trade row.
        for record in self.records.iter() {
            self.catalog
                .require_current_in_catalog(catalog, record, self.deadline, &self.cancellation)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
}

impl AlpacaPublicationRegistration {
    pub(crate) fn begin_shutdown(&self) {
        self.admission.revoke();
        self.cancellation.cancel();
    }
    pub(crate) async fn finish_shutdown(&self) {
        self.admission.revoke_and_drain().await;
    }
}
impl fmt::Debug for AlpacaPublicationRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AlpacaPublicationRegistration")
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}
impl super::super::ProductionResearchIngestCoordinator {
    pub(crate) fn bind_alpaca_option_chain_runtime(
        &self,
        generation: &super::super::ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<(Arc<ResearchService>, AlpacaPublicationRegistration), AlpacaMarketPublicationError>
    {
        let authority = self
            .authority
            .lock()
            .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?;
        let source = authority
            .publication_sources
            .get(generation.profile())
            .ok_or(AlpacaMarketPublicationError::AuthorityInvalid)?;
        if source.generation != *generation
            || !source
                .admission
                .admits_generation(generation)
                .map_err(|_| AlpacaMarketPublicationError::AuthorityInvalid)?
        {
            return Err(AlpacaMarketPublicationError::AuthorityInvalid);
        }
        Ok((
            Arc::clone(&self.research),
            AlpacaPublicationRegistration {
                admission: source.admission.clone(),
                cancellation,
            },
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AlpacaLivePublicationOutcome {
    RawRetained,
    CanonicalPublished,
}
