//! Application-owned sealing and immutable publication for selected crypto market venues.
//!
//! Coinbase Advanced Trade and Coinbase Exchange Direct enter the common provider-event spine
//! only after the owning live layer supplies already-qualified canonical events. Kraken Spot is
//! sealed here, but its current adapter export stops at provider-normalized observations; those
//! observations therefore remain an explicit sealed-raw unavailable result until the live plane
//! exports qualified `DirectUnverified` events without erasing snapshot/delta semantics.

use std::{fmt, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use market_squawk_adapter_coinbase::{
    CoinbaseDirectSnapshotSealMaterial, CoinbaseEventMicrobatchSealMaterial, CoinbaseMarketFeed,
    CoinbaseMarketHandoff, CoinbaseMarketPublicationContext, CoinbaseMarketPublicationError,
    CoinbaseMarketQualificationOutcome, CoinbaseMarketSealMaterial, CoinbaseMarketSealedTokens,
    CoinbaseSealedMarketPublication, CoinbaseSealedRawMarketPublication,
};
use market_squawk_adapter_kraken::{
    KrakenPendingPublication, KrakenPublicationError, KrakenPublicationEvidence,
    KrakenPublicationUnavailable, KrakenSealedMarketPublicationMaterial,
    KrakenSealedNonMarketPublication, KrakenSealedPublication,
};
use market_squawk_data::{
    CommittedDataset, DatasetId, DatasetManifestRef, IngestError, IngestIdentity,
    IngestPrecommitAuthority, PersistedProviderPublicationEvidence, ProviderMarketEventArrowBatch,
    ProviderMarketEventEffectiveTimeBasis, ProviderMarketEventPointInTimeRequest,
    ProviderMarketEventPointInTimeSelection, ProviderMarketEventPublicationKind,
    ProviderMarketEventSelectionError, RightsError, SourceOperation,
    provider_market_event_publication_digest,
};
use market_squawk_domain::{
    AssetClass, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, LiveEventClass,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{RawCaptureRecord, RawCaptureRecordError};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    InstrumentCoverageMembership, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderEventMicrobatchMaterial, ProviderNativeLineageImplementation,
    ProviderPublicationBindingKind, SealedProviderEventMicrobatchReceipt,
    SealedProviderPublicationBinding, SourceClass, SourceMetadata,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ResearchRightsAuthority;
use crate::{ResearchService, ResearchServiceError};

const COINBASE_ADVANCED_NATIVE_IMPLEMENTATION: &str = "coinbase_advanced_trade_v1";
const COINBASE_DIRECT_NATIVE_IMPLEMENTATION: &str = "coinbase_exchange_direct_v1";
const KRAKEN_PRODUCT: &str = "kraken-spot";
const KRAKEN_VENUE: &str = "kraken";

/// Selected, venue-qualified crypto publication surface.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CryptoMarketSurface {
    CoinbaseAdvancedTrade,
    CoinbaseExchangeDirect,
    KrakenSpot,
}

impl CryptoMarketSurface {
    const fn native_implementation(self) -> Option<ProviderNativeLineageImplementation> {
        match self {
            Self::CoinbaseAdvancedTrade => {
                Some(ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1)
            }
            Self::CoinbaseExchangeDirect => {
                Some(ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1)
            }
            Self::KrakenSpot => None,
        }
    }

    const fn persisted_native_implementation(self) -> Option<&'static str> {
        match self {
            Self::CoinbaseAdvancedTrade => Some(COINBASE_ADVANCED_NATIVE_IMPLEMENTATION),
            Self::CoinbaseExchangeDirect => Some(COINBASE_DIRECT_NATIVE_IMPLEMENTATION),
            Self::KrakenSpot => None,
        }
    }

    const fn publication_kind(self) -> Option<ProviderMarketEventPublicationKind> {
        match self {
            Self::CoinbaseAdvancedTrade => {
                Some(ProviderMarketEventPublicationKind::EventMicrobatch)
            }
            Self::CoinbaseExchangeDirect => {
                Some(ProviderMarketEventPublicationKind::CompositeResponseEvent)
            }
            Self::KrakenSpot => None,
        }
    }
}

/// One source-bound application capability for physical sealing and durable crypto publication.
pub(crate) struct CryptoMarketPublicationClosure {
    research: Arc<ResearchService>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
}

impl fmt::Debug for CryptoMarketPublicationClosure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CryptoMarketPublicationClosure")
            .field("source_id", self.source.source_id())
            .field("metadata_revision", self.source.revision())
            .field("source_registered_at", &self.source_registered_at)
            .finish_non_exhaustive()
    }
}

impl CryptoMarketPublicationClosure {
    /// Binds one registered exchange source and its exact persistence rights to the application
    /// sealer and immutable analytical writer.
    pub(crate) fn try_new(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, CryptoMarketPublicationError> {
        if source.source_id() != rights.source_id()
            || source.source_class() != SourceClass::Exchange
            || source.coverage().asset_classes() != [AssetClass::Crypto]
            || source.coverage().live().is_none()
            || !source.is_effective_at(source_registered_at)
        {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        Ok(Self {
            research,
            source,
            rights,
            source_registered_at,
        })
    }

    /// Seals one exact Coinbase raw handoff and publishes only adapter-validated, caller-supplied
    /// qualified events. Public Advanced Trade and authenticated Exchange Direct remain distinct
    /// publication kinds and never become a consolidated or cross-venue quote.
    #[allow(
        clippy::too_many_arguments,
        reason = "raw context, qualification, immutable target, authority, and deadline remain exact"
    )]
    pub(crate) async fn seal_and_publish_coinbase(
        &self,
        handoff: CoinbaseMarketHandoff,
        context: CoinbaseMarketPublicationContext,
        qualification: CoinbaseMarketQualificationOutcome,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<CoinbaseMarketApplicationOutcome, CryptoMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        precommit_authority.validate_precommit()?;
        let surface = match handoff.evidence().feed() {
            CoinbaseMarketFeed::AdvancedTradePublic => CryptoMarketSurface::CoinbaseAdvancedTrade,
            CoinbaseMarketFeed::ExchangeDirectFull => CryptoMarketSurface::CoinbaseExchangeDirect,
        };
        let decoder_binding = handoff.typed_batch().evidence().binding();
        self.validate_source_binding(
            decoder_binding.source_id(),
            decoder_binding.metadata_revision(),
        )?;
        if surface == CryptoMarketSurface::CoinbaseAdvancedTrade
            && self.source.quality_ceiling() == DataQuality::DirectVerified
        {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }

        let (rejoin, material) = handoff.into_publication_seal_handoff(context)?;
        let tokens = self
            .seal_coinbase_material(
                material,
                precommit_authority.as_ref(),
                &cancellation,
                deadline,
            )
            .await?;
        precommit_authority.validate_precommit()?;
        match rejoin.try_rejoin(tokens, qualification)? {
            CoinbaseSealedMarketPublication::SealedRaw(raw) => {
                Ok(CoinbaseMarketApplicationOutcome::SealedRaw(raw))
            }
            CoinbaseSealedMarketPublication::Published(binding) => {
                let prepared = self.validate_coinbase_binding(&binding, surface)?;
                let publication_digest = provider_market_event_publication_digest(&binding)?;
                if publication_digest != prepared.publication_digest
                    || publication_digest.algorithm() != DigestAlgorithm::Sha256
                    || publication_digest.bytes() == [0; 32]
                {
                    return Err(CryptoMarketPublicationError::AuthorityInvalid);
                }
                let reservation = self
                    .reserve_publication(
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
                Ok(CoinbaseMarketApplicationOutcome::Published(
                    CryptoMarketEventPublicationReceipt {
                        restart: CryptoMarketEventRestartSelector {
                            manifest: committed.manifest().clone(),
                            publication_digest,
                            publication_kind: prepared.publication_kind,
                            surface,
                            source_id: self.source.source_id().clone(),
                            expected_event_count: prepared.event_count,
                        },
                        committed,
                        surface,
                        provider_dataset: prepared.provider_dataset,
                        sealed_receipts: prepared.sealed_receipts,
                        event_count: prepared.event_count,
                    },
                ))
            }
        }
    }

    /// Physically seals one exact Kraken frame. The current public adapter has no already-qualified
    /// canonical output, so market observations are retained as an explicit sealed-raw unavailable
    /// state rather than upgraded to `DirectVerified` or flattened across snapshot/delta classes.
    pub(crate) async fn seal_kraken(
        &self,
        pending: KrakenPendingPublication,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<KrakenMarketApplicationOutcome, CryptoMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        if self.source.quality_ceiling() == DataQuality::DirectVerified {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        precommit_authority.validate_precommit()?;
        let (rejoin, seal_request) = pending.into_sealing_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        precommit_authority.validate_precommit()?;
        match rejoin.try_rejoin(sealed)? {
            KrakenSealedPublication::Market(market) => {
                self.validate_kraken_sealed(
                    market.evidence(),
                    market.persisted_receipt(),
                    observed_at,
                )?;
                if market.native_implementation()
                    != ProviderNativeLineageImplementation::KrakenSpotV1
                    || market.observations().is_empty()
                    || market.native_rows().len() != market.observations().len()
                    || market.native_sidecar().is_empty()
                {
                    return Err(CryptoMarketPublicationError::FamilyMismatch);
                }
                Ok(KrakenMarketApplicationOutcome::CanonicalUnavailable(
                    KrakenSealedRawCanonicalUnavailable {
                        material: market,
                        reason: KrakenPublicationUnavailable::QualifiedCanonicalOutputUnavailable,
                    },
                ))
            }
            KrakenSealedPublication::Abstained(non_market)
            | KrakenSealedPublication::Unavailable(non_market) => {
                self.validate_kraken_sealed(
                    non_market.evidence(),
                    non_market.persisted_receipt(),
                    observed_at,
                )?;
                Ok(KrakenMarketApplicationOutcome::SealedNonMarket(non_market))
            }
        }
    }

    /// Binds provider-event point-in-time reads to this exact registered crypto source and one
    /// canonical analytical dataset. Source ranking remains outside this publication closure.
    pub(crate) fn point_in_time_selector(
        &self,
        analytical_dataset: DatasetId,
    ) -> CryptoMarketPointInTimeSelector {
        CryptoMarketPointInTimeSelector {
            research: Arc::clone(&self.research),
            analytical_dataset,
            source_surface: self.source.source_id().clone(),
        }
    }

    async fn seal_coinbase_material(
        &self,
        material: CoinbaseMarketSealMaterial,
        precommit_authority: &dyn IngestPrecommitAuthority,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<CoinbaseMarketSealedTokens, CryptoMarketPublicationError> {
        match material {
            CoinbaseMarketSealMaterial::AdvancedTrade(material) => {
                let token = self
                    .seal_event_microbatch(material, precommit_authority, cancellation, deadline)
                    .await?;
                Ok(CoinbaseMarketSealedTokens::AdvancedTrade(token))
            }
            CoinbaseMarketSealMaterial::ExchangeDirect { snapshot, replay } => {
                let snapshot = self
                    .seal_direct_snapshot(snapshot, precommit_authority, cancellation, deadline)
                    .await?;
                precommit_authority.validate_precommit()?;
                let replay = self
                    .seal_event_microbatch(replay, precommit_authority, cancellation, deadline)
                    .await?;
                Ok(CoinbaseMarketSealedTokens::ExchangeDirect { snapshot, replay })
            }
        }
    }

    async fn seal_event_microbatch(
        &self,
        material: CoinbaseEventMicrobatchSealMaterial,
        precommit_authority: &dyn IngestPrecommitAuthority,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<market_squawk_sources::ProviderEventMicrobatchToken, CryptoMarketPublicationError>
    {
        self.validate_source_binding(material.source_id(), material.metadata_revision())?;
        let source_id = material.source_id().clone();
        let metadata_revision = material.metadata_revision().clone();
        let dataset = material.dataset().clone();
        let stream_identity = material.stream_identity().clone();
        let records = material
            .into_frames()
            .into_vec()
            .into_iter()
            .map(|frame| {
                RawCaptureRecord::try_new_live(
                    Uuid::from_bytes(frame.event_id()),
                    Arc::<str>::from(source_id.as_str()),
                    Uuid::from_bytes(frame.connection_id()),
                    frame.source_sequence(),
                    frame.exchange_at().map(timestamp_to_utc),
                    timestamp_to_utc(frame.received_at()),
                    frame.into_payload(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let material = ProviderEventMicrobatchMaterial::try_new(
            source_id,
            metadata_revision,
            dataset,
            stream_identity,
            records,
        )?;
        let (expectation, request) = material.into_sealing_parts();
        precommit_authority.validate_precommit()?;
        let sealed = self
            .research
            .seal_provider_capture(request, cancellation, deadline)
            .await?;
        expectation.try_rejoin(sealed).map_err(Into::into)
    }

    async fn seal_direct_snapshot(
        &self,
        material: CoinbaseDirectSnapshotSealMaterial,
        precommit_authority: &dyn IngestPrecommitAuthority,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<market_squawk_sources::ProviderWholeCaptureToken, CryptoMarketPublicationError>
    {
        self.validate_source_binding(material.source_id(), material.metadata_revision())?;
        let source_id = material.source_id().clone();
        let metadata_revision = material.metadata_revision().clone();
        let dataset = material.dataset().clone();
        let request_identity = material.request_identity();
        let status = material.status();
        let received_at = material.received_at();
        let body_digest = material.body_digest();
        let frame = material.into_frame();
        let body_bytes = u64::try_from(frame.payload().len())
            .map_err(|_| CryptoMarketPublicationError::AuthorityInvalid)?;
        let page = ProviderCapturePageReceipt::try_new(
            0,
            request_identity,
            None,
            None,
            status,
            body_bytes,
            body_digest,
            received_at,
        )?;
        let capture = ProviderCaptureSetReceipt::try_new(
            source_id.clone(),
            metadata_revision,
            dataset,
            request_identity,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![page],
        )?;
        let record = RawCaptureRecord::try_new_live(
            Uuid::from_bytes(frame.event_id()),
            Arc::<str>::from(source_id.as_str()),
            Uuid::from_bytes(frame.connection_id()),
            frame.source_sequence(),
            frame.exchange_at().map(timestamp_to_utc),
            timestamp_to_utc(frame.received_at()),
            frame.into_payload(),
        )?;
        let material = ProviderCaptureMaterial::try_new(capture, vec![record])?;
        let (expectation, request) = material.into_whole_seal_parts();
        precommit_authority.validate_precommit()?;
        let sealed = self
            .research
            .seal_provider_capture(request, cancellation, deadline)
            .await?;
        expectation
            .try_rejoin(sealed)?
            .try_into_whole()
            .map_err(Into::into)
    }

    fn validate_coinbase_binding(
        &self,
        binding: &SealedProviderPublicationBinding,
        surface: CryptoMarketSurface,
    ) -> Result<PreparedCryptoPublication, CryptoMarketPublicationError> {
        let expected_implementation = surface
            .native_implementation()
            .ok_or(CryptoMarketPublicationError::FamilyMismatch)?;
        let expected_kind = surface
            .publication_kind()
            .ok_or(CryptoMarketPublicationError::FamilyMismatch)?;
        let publication_digest = provider_market_event_publication_digest(binding)?;
        let (provider_dataset, sealed_receipts, event_count) = match (surface, binding) {
            (
                CryptoMarketSurface::CoinbaseAdvancedTrade,
                SealedProviderPublicationBinding::EventMicrobatch(event),
            ) => {
                event.validate()?;
                self.validate_source_binding(
                    event.capture_evidence().source_id(),
                    event.capture_evidence().metadata_revision(),
                )?;
                if event.native_lineage().implementation() != expected_implementation {
                    return Err(CryptoMarketPublicationError::FamilyMismatch);
                }
                (
                    event.capture_evidence().dataset().clone(),
                    CryptoMarketSealedReceiptEvidence::EventMicrobatch(
                        event.sealed_receipt_digest(),
                    ),
                    event.record_count(),
                )
            }
            (
                CryptoMarketSurface::CoinbaseExchangeDirect,
                SealedProviderPublicationBinding::CompositeResponseEvent(composite),
            ) => {
                composite.response().validate()?;
                composite.event().validate()?;
                self.validate_source_binding(
                    composite.response().capture_evidence().source_id(),
                    composite.response().capture_evidence().metadata_revision(),
                )?;
                self.validate_source_binding(
                    composite.event().capture_evidence().source_id(),
                    composite.event().capture_evidence().metadata_revision(),
                )?;
                if composite.response().native_lineage().implementation() != expected_implementation
                    || composite.event().native_lineage().implementation()
                        != expected_implementation
                    || composite.response().capture_evidence().dataset()
                        != composite.event().capture_evidence().dataset()
                {
                    return Err(CryptoMarketPublicationError::FamilyMismatch);
                }
                (
                    composite.response().capture_evidence().dataset().clone(),
                    CryptoMarketSealedReceiptEvidence::CompositeResponseEvent {
                        response: composite.response().sealed_receipt_digest(),
                        event: composite.event().sealed_receipt_digest(),
                    },
                    composite
                        .response()
                        .record_count()
                        .checked_add(composite.event().record_count())
                        .ok_or(CryptoMarketPublicationError::AuthorityInvalid)?,
                )
            }
            _ => return Err(CryptoMarketPublicationError::FamilyMismatch),
        };
        if binding.kind()
            != match expected_kind {
                ProviderMarketEventPublicationKind::EventMicrobatch => {
                    ProviderPublicationBindingKind::EventMicrobatch
                }
                ProviderMarketEventPublicationKind::CompositeResponseEvent => {
                    ProviderPublicationBindingKind::CompositeResponseEvent
                }
                ProviderMarketEventPublicationKind::ResponseMarketEvent => {
                    ProviderPublicationBindingKind::ResponseMarketEvent
                }
            }
            || event_count == 0
            || sealed_receipts.has_zero_digest()
        {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        Ok(PreparedCryptoPublication {
            publication_digest,
            publication_kind: expected_kind,
            provider_dataset,
            sealed_receipts,
            event_count,
        })
    }

    fn validate_kraken_sealed(
        &self,
        evidence: &KrakenPublicationEvidence,
        receipt: &SealedProviderEventMicrobatchReceipt,
        observed_at: Timestamp,
    ) -> Result<(), CryptoMarketPublicationError> {
        self.validate_source_binding(evidence.source_id(), evidence.metadata_revision())?;
        let live = self
            .source
            .coverage()
            .live()
            .ok_or(CryptoMarketPublicationError::FamilyMismatch)?;
        if evidence.provider_product().as_str() != KRAKEN_PRODUCT
            || evidence.venue().as_str() != KRAKEN_VENUE
            || live.provider_product().as_source_identifier() != evidence.provider_product()
            || live.provider_channel().as_source_identifier() != evidence.feed()
            || self
                .source
                .coverage()
                .instruments()
                .membership(evidence.instrument_id())
                != InstrumentCoverageMembership::Enumerated
            || evidence.received_at() > evidence.available_at()
            || evidence.available_at() > observed_at
            || receipt.capture().source_id() != evidence.source_id()
            || receipt.capture().metadata_revision() != evidence.metadata_revision()
            || receipt.capture().dataset() != evidence.dataset()
            || receipt.capture().stream_identity() != evidence.stream_identity()
        {
            return Err(CryptoMarketPublicationError::FamilyMismatch);
        }
        Ok(())
    }

    async fn reserve_publication(
        &self,
        publication_digest: EvidenceDigest,
        idempotency_key: String,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::IngestReservation, CryptoMarketPublicationError> {
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
    ) -> Result<(), CryptoMarketPublicationError> {
        if observed_at < self.source_registered_at || !self.source.is_effective_at(observed_at) {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        self.rights.validate_at(observed_at)?;
        Ok(())
    }

    fn validate_source_binding(
        &self,
        source_id: &SourceId,
        metadata_revision: &market_squawk_domain::MetadataRevision,
    ) -> Result<(), CryptoMarketPublicationError> {
        if source_id != self.source.source_id() || metadata_revision != self.source.revision() {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

/// Source-bound current/PIT capability over immutable Coinbase or Kraken market publications.
///
/// The selector deliberately fixes one source surface. A later unified resolver may compare the
/// independently returned source receipts, but this layer never ranks or blends venues.
#[derive(Clone)]
pub(crate) struct CryptoMarketPointInTimeSelector {
    research: Arc<ResearchService>,
    analytical_dataset: DatasetId,
    source_surface: SourceId,
}

impl fmt::Debug for CryptoMarketPointInTimeSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CryptoMarketPointInTimeSelector")
            .field("analytical_dataset", &self.analytical_dataset)
            .field("source_surface", &self.source_surface)
            .finish_non_exhaustive()
    }
}

impl CryptoMarketPointInTimeSelector {
    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) const fn source_surface(&self) -> &SourceId {
        &self.source_surface
    }

    /// Selects bounded newest ties for one exact canonical instrument, venue, event kind, clock,
    /// and source surface. An empty result remains distinct from an empty, complete generation.
    #[allow(
        clippy::too_many_arguments,
        reason = "canonical identity, venue, event family, both PIT clocks, basis, and bound stay explicit"
    )]
    pub(crate) async fn select_latest(
        &self,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        event_kind: LiveEventClass,
        as_of_cutoff: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_time_basis: ProviderMarketEventEffectiveTimeBasis,
        maximum_candidates: usize,
        cancellation: CancellationToken,
    ) -> Result<Option<CryptoMarketPointInTimeReceipt>, CryptoMarketPublicationError> {
        let request = ProviderMarketEventPointInTimeRequest::try_latest(
            self.analytical_dataset.clone(),
            instrument_id,
            venue_id,
            event_kind,
            as_of_cutoff,
            knowledge_cutoff,
            effective_time_basis,
            maximum_candidates,
            Some(self.source_surface.clone()),
        )?;
        let store = self.research.provider_capture_store();
        let selection = self
            .research
            .analytical()
            .read_provider_market_event_point_in_time(&request, store.as_ref(), cancellation)
            .await?;
        selection
            .map(|selection| CryptoMarketPointInTimeReceipt::try_new(self, selection))
            .transpose()
    }

    /// Reopens the original selection's exact manifest and rejects any request, source, row,
    /// exclusion, tie, evidence, or selection-digest drift after process restart.
    pub(crate) async fn verify_restart(
        &self,
        original: &CryptoMarketPointInTimeReceipt,
        cancellation: CancellationToken,
    ) -> Result<CryptoMarketPointInTimeReceipt, CryptoMarketPublicationError> {
        original.validate_selector(self)?;
        let store = self.research.provider_capture_store();
        let replay = self
            .research
            .analytical()
            .verify_provider_market_event_point_in_time_restart(
                &original.selection,
                store.as_ref(),
                cancellation,
            )
            .await?;
        CryptoMarketPointInTimeReceipt::try_new(self, replay)
    }
}

/// Exact source-separated PIT selection with enough semantic state for verified restart replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CryptoMarketPointInTimeReceipt {
    analytical_dataset: DatasetId,
    source_surface: SourceId,
    selection: ProviderMarketEventPointInTimeSelection,
}

impl CryptoMarketPointInTimeReceipt {
    fn try_new(
        selector: &CryptoMarketPointInTimeSelector,
        selection: ProviderMarketEventPointInTimeSelection,
    ) -> Result<Self, CryptoMarketPublicationError> {
        if selection.request().dataset() != &selector.analytical_dataset
            || selection.manifest().dataset_id() != &selector.analytical_dataset
            || selection.request().exact_source_surface() != Some(&selector.source_surface)
            || selection
                .sources()
                .iter()
                .any(|source| source.source_surface() != &selector.source_surface)
        {
            return Err(CryptoMarketPublicationError::PointInTimeInvalid);
        }
        Ok(Self {
            analytical_dataset: selector.analytical_dataset.clone(),
            source_surface: selector.source_surface.clone(),
            selection,
        })
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) const fn source_surface(&self) -> &SourceId {
        &self.source_surface
    }

    pub(crate) const fn selection(&self) -> &ProviderMarketEventPointInTimeSelection {
        &self.selection
    }

    fn validate_selector(
        &self,
        selector: &CryptoMarketPointInTimeSelector,
    ) -> Result<(), CryptoMarketPublicationError> {
        if self.analytical_dataset != selector.analytical_dataset
            || self.source_surface != selector.source_surface
        {
            return Err(CryptoMarketPublicationError::PointInTimeInvalid);
        }
        Ok(())
    }
}

/// Coinbase application result after exact physical sealing.
#[derive(Debug)]
pub(crate) enum CoinbaseMarketApplicationOutcome {
    Published(CryptoMarketEventPublicationReceipt),
    SealedRaw(CoinbaseSealedRawMarketPublication),
}

/// Kraken application result. The current bridge deliberately has no published-market variant.
#[derive(Debug)]
pub(crate) enum KrakenMarketApplicationOutcome {
    /// Market observations retain exact snapshot/delta and native semantics while canonical
    /// qualification remains unavailable.
    CanonicalUnavailable(KrakenSealedRawCanonicalUnavailable),
    /// Control traffic or provider/decode unavailability with its exact sealed receipt.
    SealedNonMarket(KrakenSealedNonMarketPublication),
}

/// Sealed Kraken market material retained without creating a canonical `MarketEvent`.
#[derive(Debug)]
pub(crate) struct KrakenSealedRawCanonicalUnavailable {
    material: KrakenSealedMarketPublicationMaterial,
    reason: KrakenPublicationUnavailable,
}

impl KrakenSealedRawCanonicalUnavailable {
    pub(crate) const fn material(&self) -> &KrakenSealedMarketPublicationMaterial {
        &self.material
    }

    pub(crate) const fn reason(&self) -> KrakenPublicationUnavailable {
        self.reason
    }
}

/// Exact immutable raw receipt identities for one Coinbase publication kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CryptoMarketSealedReceiptEvidence {
    EventMicrobatch(EvidenceDigest),
    CompositeResponseEvent {
        response: EvidenceDigest,
        event: EvidenceDigest,
    },
}

impl CryptoMarketSealedReceiptEvidence {
    fn has_zero_digest(self) -> bool {
        match self {
            Self::EventMicrobatch(digest) => digest.bytes() == [0; 32],
            Self::CompositeResponseEvent { response, event } => {
                response.bytes() == [0; 32] || event.bytes() == [0; 32]
            }
        }
    }
}

struct PreparedCryptoPublication {
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    provider_dataset: SourceIdentifier,
    sealed_receipts: CryptoMarketSealedReceiptEvidence,
    event_count: usize,
}

/// Successful Coinbase immutable publication and its exact restart coordinate.
#[derive(Debug)]
pub(crate) struct CryptoMarketEventPublicationReceipt {
    committed: CommittedDataset,
    restart: CryptoMarketEventRestartSelector,
    surface: CryptoMarketSurface,
    provider_dataset: SourceIdentifier,
    sealed_receipts: CryptoMarketSealedReceiptEvidence,
    event_count: usize,
}

impl CryptoMarketEventPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &CryptoMarketEventRestartSelector {
        &self.restart
    }

    pub(crate) const fn surface(&self) -> CryptoMarketSurface {
        self.surface
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn sealed_receipts(&self) -> CryptoMarketSealedReceiptEvidence {
        self.sealed_receipts
    }

    pub(crate) const fn event_count(&self) -> usize {
        self.event_count
    }
}

/// Exact manifest/digest/kind selector for one venue-qualified Coinbase publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CryptoMarketEventRestartSelector {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    surface: CryptoMarketSurface,
    source_id: SourceId,
    expected_event_count: usize,
}

impl CryptoMarketEventRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) const fn publication_kind(&self) -> ProviderMarketEventPublicationKind {
        self.publication_kind
    }

    pub(crate) const fn surface(&self) -> CryptoMarketSurface {
        self.surface
    }

    /// Reopens the exact raw/native evidence and Parquet events after process restart. It never
    /// substitutes a newer generation or another venue.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<CryptoMarketEventRestartReceipt, CryptoMarketPublicationError> {
        let selector = research
            .analytical()
            .provider_market_event_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(CryptoMarketPublicationError::RestartInvalid)?;
        let store = research.provider_capture_store();
        let evidence = research
            .analytical()
            .provider_market_event_publication_evidence(&self.manifest, selector, store.as_ref())?;
        validate_restart_evidence(self, &evidence)?;
        let events = research
            .analytical()
            .read_provider_market_event_publication(
                &self.manifest,
                selector,
                store.as_ref(),
                cancellation,
            )
            .await?;
        if events.publication_digest() != self.publication_digest
            || events.publication_kind() != self.publication_kind.as_str()
            || events.events().len() != self.expected_event_count
        {
            return Err(CryptoMarketPublicationError::RestartInvalid);
        }
        Ok(CryptoMarketEventRestartReceipt { evidence, events })
    }
}

/// Restart-verified raw/native catalog evidence and typed canonical events.
#[derive(Debug)]
pub(crate) struct CryptoMarketEventRestartReceipt {
    evidence: PersistedProviderPublicationEvidence,
    events: ProviderMarketEventArrowBatch,
}

impl CryptoMarketEventRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderPublicationEvidence {
        &self.evidence
    }

    pub(crate) const fn events(&self) -> &ProviderMarketEventArrowBatch {
        &self.events
    }
}

fn validate_restart_evidence(
    expected: &CryptoMarketEventRestartSelector,
    evidence: &PersistedProviderPublicationEvidence,
) -> Result<(), CryptoMarketPublicationError> {
    if evidence.publication_digest() != expected.publication_digest
        || evidence.publication_kind() != expected.publication_kind.as_str()
    {
        return Err(CryptoMarketPublicationError::RestartInvalid);
    }
    let expected_implementation = expected
        .surface
        .persisted_native_implementation()
        .ok_or(CryptoMarketPublicationError::RestartInvalid)?;
    let (source_id, event_count, native_matches) = match (expected.surface, evidence) {
        (
            CryptoMarketSurface::CoinbaseAdvancedTrade,
            PersistedProviderPublicationEvidence::EventMicrobatch(event),
        ) => (
            event.capture().source_id(),
            event.canonical_event_count(),
            event.native_lineage().implementation() == expected_implementation,
        ),
        (
            CryptoMarketSurface::CoinbaseExchangeDirect,
            PersistedProviderPublicationEvidence::CompositeResponseEvent {
                response, event, ..
            },
        ) => (
            response.capture().source_id(),
            response
                .canonical_event_count()
                .checked_add(event.canonical_event_count())
                .ok_or(CryptoMarketPublicationError::RestartInvalid)?,
            response.native_lineage().implementation() == expected_implementation
                && event.native_lineage().implementation() == expected_implementation
                && response.capture().source_id() == event.capture().source_id()
                && response.capture().metadata_revision() == event.capture().metadata_revision()
                && response.capture().dataset() == event.capture().dataset(),
        ),
        _ => return Err(CryptoMarketPublicationError::RestartInvalid),
    };
    if source_id != &expected.source_id
        || event_count != expected.expected_event_count
        || !native_matches
    {
        return Err(CryptoMarketPublicationError::RestartInvalid);
    }
    Ok(())
}

fn timestamp_to_utc(timestamp: Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
}

/// Closed application crypto sealing/publication failure.
#[derive(Debug, Error)]
pub(crate) enum CryptoMarketPublicationError {
    #[error("crypto market publication authority is invalid or no longer current")]
    AuthorityInvalid,
    #[error("sealed crypto evidence does not match the selected source or venue surface")]
    FamilyMismatch,
    #[error("the exact crypto immutable generation failed restart verification")]
    RestartInvalid,
    #[error("the crypto point-in-time selection escaped its exact dataset or source surface")]
    PointInTimeInvalid,
    #[error(transparent)]
    Coinbase(#[from] CoinbaseMarketPublicationError),
    #[error(transparent)]
    Kraken(#[from] KrakenPublicationError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    PointInTime(#[from] ProviderMarketEventSelectionError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    RawCapture(#[from] RawCaptureRecordError),
    #[error(transparent)]
    Rights(#[from] RightsError),
    #[error(transparent)]
    Service(#[from] ServiceError),
}
