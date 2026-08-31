//! Application-owned sealing and immutable market-event publication for selected venues.
//!
//! Coinbase Advanced Trade, Coinbase Exchange Direct, and Kraken Spot enter the common
//! provider-event spine only after the owning live layer supplies already-qualified canonical
//! events. Raw capture, native semantics, venue identity, clocks, quality, and source generation
//! remain attached through publication and point-in-time restart reads.

use std::{fmt, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use market_squawk_adapter_coinbase::{
    CoinbaseDirectSnapshotSealMaterial, CoinbaseEventMicrobatchSealMaterial, CoinbaseMarketFeed,
    CoinbaseMarketHandoff, CoinbaseMarketPublicationContext, CoinbaseMarketPublicationError,
    CoinbaseMarketQualificationOutcome, CoinbaseMarketSealMaterial, CoinbaseMarketSealRejoin,
    CoinbaseMarketSealedTokens, CoinbaseSealedMarketPublication,
    CoinbaseSealedRawMarketPublication,
};
use market_squawk_adapter_kraken::{
    KrakenPendingPublication, KrakenPublicationError, KrakenPublicationEvidence,
    KrakenPublicationUnavailable, KrakenQualifiedMarketPublication,
    KrakenSealedMarketPublicationMaterial, KrakenSealedNonMarketPublication,
    KrakenSealedPublication,
};
use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRegistry, IngestError, IngestIdentity,
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
    ProviderCapturePageReceipt, ProviderCaptureSealRequest, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, ProviderEventMicrobatchMaterial,
    ProviderNativeLineageImplementation, ProviderPublicationBindingKind,
    SealedProviderEventMicrobatchReceipt, SealedProviderPublicationBinding, SourceClass,
    SourceMetadata,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod kraken_rendezvous;

pub(crate) use kraken_rendezvous::{
    CryptoCommittedRowIngress, CryptoPendingFrameIngress, CryptoPublicationRendezvousLimits,
};

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
    const fn native_implementation(self) -> ProviderNativeLineageImplementation {
        match self {
            Self::CoinbaseAdvancedTrade => {
                ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1
            }
            Self::CoinbaseExchangeDirect => {
                ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1
            }
            Self::KrakenSpot => ProviderNativeLineageImplementation::KrakenSpotV1,
        }
    }

    const fn publication_kind(self) -> ProviderMarketEventPublicationKind {
        match self {
            Self::CoinbaseAdvancedTrade => ProviderMarketEventPublicationKind::EventMicrobatch,
            Self::CoinbaseExchangeDirect => {
                ProviderMarketEventPublicationKind::CompositeResponseEvent
            }
            Self::KrakenSpot => ProviderMarketEventPublicationKind::EventMicrobatch,
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

    /// Seals one exact Coinbase Exchange Direct handoff and publishes only adapter-validated,
    /// caller-supplied qualified events. Public Advanced Trade must use the capture-owned physical
    /// frame plus post-commit live-row path below; it cannot enter through this legacy UUID seam.
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
            CoinbaseMarketFeed::AdvancedTradePublic => {
                return Err(CryptoMarketPublicationError::FamilyMismatch);
            }
            CoinbaseMarketFeed::ExchangeDirectFull => CryptoMarketSurface::CoinbaseExchangeDirect,
        };
        let decoder_binding = handoff.typed_batch().evidence().binding();
        self.validate_source_binding(
            decoder_binding.source_id(),
            decoder_binding.metadata_revision(),
        )?;
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
                let prepared = self.validate_publication_binding(&binding, surface)?;
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
                    MarketEventPublicationReceipt::try_new(
                        committed.manifest().clone(),
                        publication_digest,
                        prepared.publication_kind,
                        surface.native_implementation(),
                        self.source.source_id().clone(),
                        prepared.provider_dataset,
                        prepared.sealed_receipts,
                        prepared.event_count,
                    )?,
                ))
            }
        }
    }

    /// Physically seals one exact public Advanced Trade frame while retaining the adapter's opaque
    /// continuation for its post-commit live-row join.
    #[allow(
        clippy::too_many_arguments,
        reason = "raw seal, exact source authority, cancellation, and deadline remain explicit"
    )]
    pub(crate) async fn seal_coinbase_public(
        &self,
        rejoin: CoinbaseMarketSealRejoin,
        seal_request: ProviderCaptureSealRequest,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<CoinbaseMarketSealRejoin, CryptoMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        if self.source.quality_ceiling() == DataQuality::DirectVerified {
            return Err(CryptoMarketPublicationError::AuthorityInvalid);
        }
        self.validate_source_binding(rejoin.source_id(), rejoin.metadata_revision())?;
        precommit_authority.validate_precommit()?;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        precommit_authority.validate_precommit()?;
        let sealed = rejoin.try_rejoin_public_seal(sealed)?;
        let receipt = sealed.persisted_receipt()?;
        self.validate_source_binding(
            receipt.capture().source_id(),
            receipt.capture().metadata_revision(),
        )?;
        Ok(sealed)
    }

    async fn publish_coinbase_public_joined(
        &self,
        material: CoinbaseMarketSealRejoin,
        rows: Vec<market_squawk_live::CommittedResearchMarketObservation>,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseMarketApplicationOutcome, CryptoMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        self.validate_source_binding(material.source_id(), material.metadata_revision())?;
        precommit_authority.validate_precommit()?;
        let binding = material.try_publish_committed(rows)?;
        let prepared = self
            .validate_publication_binding(&binding, CryptoMarketSurface::CoinbaseAdvancedTrade)?;
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        if publication_digest != prepared.publication_digest {
            return Err(CryptoMarketPublicationError::FamilyMismatch);
        }
        let reservation = self
            .reserve_publication(
                publication_digest,
                idempotency_key,
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
            MarketEventPublicationReceipt::try_new(
                committed.manifest().clone(),
                publication_digest,
                prepared.publication_kind,
                CryptoMarketSurface::CoinbaseAdvancedTrade.native_implementation(),
                self.source.source_id().clone(),
                prepared.provider_dataset,
                prepared.sealed_receipts,
                prepared.event_count,
            )?,
        ))
    }

    /// Physically seals one exact Kraken frame. Market observations remain non-canonical until the
    /// bounded publication rendezvous rejoins the same frame with instrument-owned committed rows.
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

    async fn publish_kraken_joined(
        &self,
        material: KrakenSealedMarketPublicationMaterial,
        rows: Vec<market_squawk_live::CommittedResearchMarketObservation>,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<KrakenMarketApplicationOutcome, CryptoMarketPublicationError> {
        self.validate_current_authority(observed_at)?;
        self.validate_kraken_sealed(
            material.evidence(),
            material.persisted_receipt(),
            observed_at,
        )?;
        precommit_authority.validate_precommit()?;
        let binding =
            material.try_publish_qualified(KrakenQualifiedMarketPublication::try_new(rows)?)?;
        let prepared =
            self.validate_publication_binding(&binding, CryptoMarketSurface::KrakenSpot)?;
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        if publication_digest != prepared.publication_digest {
            return Err(CryptoMarketPublicationError::FamilyMismatch);
        }
        let reservation = self
            .reserve_publication(
                publication_digest,
                idempotency_key,
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
        Ok(KrakenMarketApplicationOutcome::Published(
            MarketEventPublicationReceipt::try_new(
                committed.manifest().clone(),
                publication_digest,
                prepared.publication_kind,
                CryptoMarketSurface::KrakenSpot.native_implementation(),
                self.source.source_id().clone(),
                prepared.provider_dataset,
                prepared.sealed_receipts,
                prepared.event_count,
            )?,
        ))
    }

    /// Binds provider-event point-in-time reads to this exact registered source and one
    /// canonical analytical dataset. Source ranking remains outside this publication closure.
    pub(super) fn point_in_time_selector(
        &self,
        analytical_dataset: DatasetId,
    ) -> MarketEventPointInTimeSelector {
        MarketEventPointInTimeSelector::new(
            Arc::clone(&self.research),
            analytical_dataset,
            self.source.source_id().clone(),
        )
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

    fn validate_publication_binding(
        &self,
        binding: &SealedProviderPublicationBinding,
        surface: CryptoMarketSurface,
    ) -> Result<PreparedMarketEventPublication, CryptoMarketPublicationError> {
        let expected_implementation = surface.native_implementation();
        let expected_kind = surface.publication_kind();
        let publication_digest = provider_market_event_publication_digest(binding)?;
        let (provider_dataset, sealed_receipts, event_count) = match (surface, binding) {
            (
                CryptoMarketSurface::CoinbaseAdvancedTrade,
                SealedProviderPublicationBinding::EventMicrobatch(event),
            )
            | (
                CryptoMarketSurface::KrakenSpot,
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
                    MarketEventSealedReceiptEvidence::Single(event.sealed_receipt_digest()),
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
                    MarketEventSealedReceiptEvidence::CompositeResponseEvent {
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
        Ok(PreparedMarketEventPublication {
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

/// Source-bound current/PIT capability over immutable canonical market-event publications.
///
/// The selector deliberately fixes one source surface. A later unified resolver may compare the
/// independently returned source receipts, but this layer never ranks or blends venues.
#[derive(Clone)]
pub(crate) struct MarketEventPointInTimeSelector {
    research: Arc<ResearchService>,
    analytical_dataset: DatasetId,
    source_surface: SourceId,
}

impl fmt::Debug for MarketEventPointInTimeSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketEventPointInTimeSelector")
            .field("analytical_dataset", &self.analytical_dataset)
            .field("source_surface", &self.source_surface)
            .finish_non_exhaustive()
    }
}

impl MarketEventPointInTimeSelector {
    pub(crate) fn new(
        research: Arc<ResearchService>,
        analytical_dataset: DatasetId,
        source_surface: SourceId,
    ) -> Self {
        Self {
            research,
            analytical_dataset,
            source_surface,
        }
    }

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
    ) -> Result<Option<MarketEventPointInTimeReceipt>, MarketEventReadError> {
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
            .map(|selection| MarketEventPointInTimeReceipt::try_new(self, selection))
            .transpose()
    }

    /// Reopens the original selection's exact manifest and rejects any request, source, row,
    /// exclusion, tie, evidence, or selection-digest drift after process restart.
    pub(crate) async fn verify_restart(
        &self,
        original: &MarketEventPointInTimeReceipt,
        cancellation: CancellationToken,
    ) -> Result<MarketEventPointInTimeReceipt, MarketEventReadError> {
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
        MarketEventPointInTimeReceipt::try_new(self, replay)
    }
}

/// Bounded internal handoff from one live source into provider-neutral market selection.
///
/// The current-process receipt is an optimization and diagnostic coordinate only. An empty value
/// after restart does not override the durable catalog, which remains authoritative through the
/// source-bound point-in-time selector.
#[derive(Clone, Debug)]
pub(crate) struct MarketEventDurableRead {
    point_in_time: MarketEventPointInTimeSelector,
    latest: Arc<Mutex<Option<Arc<MarketEventPublicationReceipt>>>>,
}

impl MarketEventDurableRead {
    pub(super) fn channel(
        point_in_time: MarketEventPointInTimeSelector,
    ) -> (MarketEventDurableReadWriter, Self) {
        let latest = Arc::new(Mutex::new(None));
        let writer = MarketEventDurableReadWriter {
            analytical_dataset: point_in_time.analytical_dataset.clone(),
            source_surface: point_in_time.source_surface.clone(),
            latest: Arc::clone(&latest),
        };
        (
            writer,
            Self {
                point_in_time,
                latest,
            },
        )
    }

    pub(crate) const fn point_in_time_selector(&self) -> &MarketEventPointInTimeSelector {
        &self.point_in_time
    }

    pub(crate) async fn latest_publication(&self) -> Option<Arc<MarketEventPublicationReceipt>> {
        self.latest.lock().await.clone()
    }
}

/// Sole writer for the compact latest-publication coordinate retained by one runtime lane.
#[derive(Clone, Debug)]
pub(crate) struct MarketEventDurableReadWriter {
    analytical_dataset: DatasetId,
    source_surface: SourceId,
    latest: Arc<Mutex<Option<Arc<MarketEventPublicationReceipt>>>>,
}

impl MarketEventDurableReadWriter {
    /// Retains only a monotonic latest immutable generation. Out-of-order completion cannot move
    /// the runtime backward; a conflicting identity at one generation is terminal.
    pub(crate) async fn retain(
        &self,
        receipt: MarketEventPublicationReceipt,
    ) -> Result<bool, MarketEventReadError> {
        if receipt.manifest.dataset_id() != &self.analytical_dataset
            || !is_canonical_market_event_manifest(&receipt.manifest)
            || receipt.restart.manifest() != &receipt.manifest
            || &receipt.restart.source_id != &self.source_surface
        {
            return Err(MarketEventReadError::DurableGenerationInvalid);
        }

        let candidate_version = receipt.manifest.manifest_version();
        let mut latest = self.latest.lock().await;
        match latest.as_ref() {
            None => {
                *latest = Some(Arc::new(receipt));
                Ok(true)
            }
            Some(current) => match candidate_version.cmp(&current.manifest.manifest_version()) {
                std::cmp::Ordering::Greater => {
                    *latest = Some(Arc::new(receipt));
                    Ok(true)
                }
                std::cmp::Ordering::Less => Ok(false),
                std::cmp::Ordering::Equal if current.as_ref() == &receipt => Ok(false),
                std::cmp::Ordering::Equal => Err(MarketEventReadError::DurableReadConflict),
            },
        }
    }
}

/// Exact source-separated PIT selection with enough semantic state for verified restart replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketEventPointInTimeReceipt {
    analytical_dataset: DatasetId,
    source_surface: SourceId,
    selection: ProviderMarketEventPointInTimeSelection,
}

impl MarketEventPointInTimeReceipt {
    fn try_new(
        selector: &MarketEventPointInTimeSelector,
        selection: ProviderMarketEventPointInTimeSelection,
    ) -> Result<Self, MarketEventReadError> {
        if selection.request().dataset() != &selector.analytical_dataset
            || selection.manifest().dataset_id() != &selector.analytical_dataset
            || selection.request().exact_source_surface() != Some(&selector.source_surface)
            || selection
                .sources()
                .iter()
                .any(|source| source.source_surface() != &selector.source_surface)
        {
            return Err(MarketEventReadError::PointInTimeInvalid);
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
        selector: &MarketEventPointInTimeSelector,
    ) -> Result<(), MarketEventReadError> {
        if self.analytical_dataset != selector.analytical_dataset
            || self.source_surface != selector.source_surface
        {
            return Err(MarketEventReadError::PointInTimeInvalid);
        }
        Ok(())
    }
}

/// Coinbase application result after exact physical sealing.
#[derive(Debug)]
pub(crate) enum CoinbaseMarketApplicationOutcome {
    Published(MarketEventPublicationReceipt),
    SealedRaw(CoinbaseSealedRawMarketPublication),
}

/// Kraken application result across raw sealing and joined canonical publication.
#[derive(Debug)]
pub(crate) enum KrakenMarketApplicationOutcome {
    /// Exact sealed material and committed live rows were published through the common spine.
    Published(MarketEventPublicationReceipt),
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

    /// Transfers the exact sealed frame into the bounded qualification rendezvous.
    pub(crate) fn into_material(self) -> KrakenSealedMarketPublicationMaterial {
        self.material
    }
}

/// Exact immutable raw receipt identities for one canonical market-event publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketEventSealedReceiptEvidence {
    Single(EvidenceDigest),
    CompositeResponseEvent {
        response: EvidenceDigest,
        event: EvidenceDigest,
    },
}

impl MarketEventSealedReceiptEvidence {
    fn has_zero_digest(self) -> bool {
        match self {
            Self::Single(digest) => digest.bytes() == [0; 32],
            Self::CompositeResponseEvent { response, event } => {
                response.bytes() == [0; 32] || event.bytes() == [0; 32]
            }
        }
    }
}

struct PreparedMarketEventPublication {
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    provider_dataset: SourceIdentifier,
    sealed_receipts: MarketEventSealedReceiptEvidence,
    event_count: usize,
}

/// Successful source-qualified market-event publication and its exact restart coordinate.
///
/// This receipt intentionally retains only compact immutable identities. The catalog owns the
/// cumulative pinned generation graph; a continuously running source must not retain that graph
/// once publication commits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketEventPublicationReceipt {
    manifest: DatasetManifestRef,
    restart: MarketEventRestartSelector,
    provider_dataset: SourceIdentifier,
    sealed_receipts: MarketEventSealedReceiptEvidence,
    event_count: usize,
}

impl MarketEventPublicationReceipt {
    #[allow(
        clippy::too_many_arguments,
        reason = "immutable generation, publication, lineage, source, and capture identities stay explicit"
    )]
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        publication_digest: EvidenceDigest,
        publication_kind: ProviderMarketEventPublicationKind,
        native_implementation: ProviderNativeLineageImplementation,
        source_id: SourceId,
        provider_dataset: SourceIdentifier,
        sealed_receipts: MarketEventSealedReceiptEvidence,
        event_count: usize,
    ) -> Result<Self, MarketEventReadError> {
        let receipt_shape_matches = matches!(
            (publication_kind, sealed_receipts),
            (
                ProviderMarketEventPublicationKind::CompositeResponseEvent,
                MarketEventSealedReceiptEvidence::CompositeResponseEvent { .. }
            ) | (
                ProviderMarketEventPublicationKind::EventMicrobatch
                    | ProviderMarketEventPublicationKind::ResponseMarketEvent,
                MarketEventSealedReceiptEvidence::Single(_)
            )
        );
        if publication_digest.algorithm() != DigestAlgorithm::Sha256
            || publication_digest.bytes() == [0; 32]
            || !is_canonical_market_event_manifest(&manifest)
            || persisted_market_event_implementation(native_implementation).is_none()
            || sealed_receipts.has_zero_digest()
            || event_count == 0
            || !receipt_shape_matches
        {
            return Err(MarketEventReadError::DurableGenerationInvalid);
        }
        Ok(Self {
            restart: MarketEventRestartSelector {
                manifest: manifest.clone(),
                publication_digest,
                publication_kind,
                native_implementation,
                source_id,
                expected_event_count: event_count,
            },
            manifest,
            provider_dataset,
            sealed_receipts,
            event_count,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn restart_selector(&self) -> &MarketEventRestartSelector {
        &self.restart
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn sealed_receipts(&self) -> MarketEventSealedReceiptEvidence {
        self.sealed_receipts
    }

    pub(crate) const fn event_count(&self) -> usize {
        self.event_count
    }
}

fn is_canonical_market_event_manifest(manifest: &DatasetManifestRef) -> bool {
    DatasetSchemaRegistry::local()
        .canonical_market_events()
        .is_ok_and(|registered| manifest.schema() == &registered)
}

/// Exact manifest/digest/kind/lineage selector for one source-qualified market publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketEventRestartSelector {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    native_implementation: ProviderNativeLineageImplementation,
    source_id: SourceId,
    expected_event_count: usize,
}

impl MarketEventRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) const fn publication_kind(&self) -> ProviderMarketEventPublicationKind {
        self.publication_kind
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Reopens the exact raw/native evidence and Parquet events after process restart. It never
    /// substitutes a newer generation or another venue.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<MarketEventRestartReceipt, MarketEventReadError> {
        let selector = research
            .analytical()
            .provider_market_event_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(MarketEventReadError::RestartInvalid)?;
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
            return Err(MarketEventReadError::RestartInvalid);
        }
        Ok(MarketEventRestartReceipt { evidence, events })
    }
}

/// Restart-verified raw/native catalog evidence and typed canonical events.
#[derive(Debug)]
pub(crate) struct MarketEventRestartReceipt {
    evidence: PersistedProviderPublicationEvidence,
    events: ProviderMarketEventArrowBatch,
}

impl MarketEventRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderPublicationEvidence {
        &self.evidence
    }

    pub(crate) const fn events(&self) -> &ProviderMarketEventArrowBatch {
        &self.events
    }
}

fn validate_restart_evidence(
    expected: &MarketEventRestartSelector,
    evidence: &PersistedProviderPublicationEvidence,
) -> Result<(), MarketEventReadError> {
    if evidence.publication_digest() != expected.publication_digest
        || evidence.publication_kind() != expected.publication_kind.as_str()
    {
        return Err(MarketEventReadError::RestartInvalid);
    }
    let expected_implementation =
        persisted_market_event_implementation(expected.native_implementation)
            .ok_or(MarketEventReadError::RestartInvalid)?;
    let (source_id, event_count, native_matches) = match (expected.publication_kind, evidence) {
        (
            ProviderMarketEventPublicationKind::EventMicrobatch,
            PersistedProviderPublicationEvidence::EventMicrobatch(event),
        ) => (
            event.capture().source_id(),
            event.canonical_event_count(),
            event.native_lineage().implementation() == expected_implementation,
        ),
        (
            ProviderMarketEventPublicationKind::ResponseMarketEvent,
            PersistedProviderPublicationEvidence::ResponseMarketEvent(response),
        ) => (
            response.capture().source_id(),
            response.canonical_event_count(),
            response.native_lineage().implementation() == expected_implementation,
        ),
        (
            ProviderMarketEventPublicationKind::CompositeResponseEvent,
            PersistedProviderPublicationEvidence::CompositeResponseEvent {
                response, event, ..
            },
        ) => (
            response.capture().source_id(),
            response
                .canonical_event_count()
                .checked_add(event.canonical_event_count())
                .ok_or(MarketEventReadError::RestartInvalid)?,
            response.native_lineage().implementation() == expected_implementation
                && event.native_lineage().implementation() == expected_implementation
                && response.capture().source_id() == event.capture().source_id()
                && response.capture().metadata_revision() == event.capture().metadata_revision()
                && response.capture().dataset() == event.capture().dataset(),
        ),
        _ => return Err(MarketEventReadError::RestartInvalid),
    };
    if source_id != &expected.source_id
        || event_count != expected.expected_event_count
        || !native_matches
    {
        return Err(MarketEventReadError::RestartInvalid);
    }
    Ok(())
}

const fn persisted_market_event_implementation(
    implementation: ProviderNativeLineageImplementation,
) -> Option<&'static str> {
    match implementation {
        ProviderNativeLineageImplementation::AlpacaIexMarketDataV1 => {
            Some("alpaca_iex_market_data_v1")
        }
        ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1 => {
            Some("alpaca_indicative_options_v1")
        }
        ProviderNativeLineageImplementation::CoinbaseAdvancedTradeV1 => {
            Some(COINBASE_ADVANCED_NATIVE_IMPLEMENTATION)
        }
        ProviderNativeLineageImplementation::CoinbaseExchangeDirectV1 => {
            Some(COINBASE_DIRECT_NATIVE_IMPLEMENTATION)
        }
        ProviderNativeLineageImplementation::KrakenSpotV1 => Some("kraken_spot_v1"),
        ProviderNativeLineageImplementation::SchwabRestMarketDataV1 => {
            Some("schwab_rest_market_data_v1")
        }
        ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1 => {
            Some("schwab_streamer_market_data_v1")
        }
        ProviderNativeLineageImplementation::YahooEnrichmentV1 => Some("yahoo_enrichment_v1"),
        _ => None,
    }
}

fn timestamp_to_utc(timestamp: Timestamp) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
}

/// Closed provider-neutral durable market-event read failure.
#[derive(Debug, Error)]
pub(crate) enum MarketEventReadError {
    #[error("the durable market-event generation escaped its exact dataset or source surface")]
    DurableGenerationInvalid,
    #[error("the exact market-event generation failed restart verification")]
    RestartInvalid,
    #[error("the market-event point-in-time selection escaped its exact dataset or source surface")]
    PointInTimeInvalid,
    #[error("one market-event generation has conflicting durable publication identities")]
    DurableReadConflict,
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    PointInTime(#[from] ProviderMarketEventSelectionError),
}

/// Closed application crypto sealing/publication failure.
#[derive(Debug, Error)]
pub(crate) enum CryptoMarketPublicationError {
    #[error("crypto market publication authority is invalid or no longer current")]
    AuthorityInvalid,
    #[error("sealed crypto evidence does not match the selected source or venue surface")]
    FamilyMismatch,
    #[error("the bounded crypto publication rendezvous rejected or expired exact material")]
    RendezvousUnavailable,
    #[error(transparent)]
    Coinbase(#[from] CoinbaseMarketPublicationError),
    #[error(transparent)]
    Kraken(#[from] KrakenPublicationError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    RawCapture(#[from] RawCaptureRecordError),
    #[error(transparent)]
    Rights(#[from] RightsError),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    MarketEventRead(#[from] MarketEventReadError),
}

#[cfg(test)]
mod tests {
    use market_squawk_data::{DatasetSchemaRegistry, Sha256Digest};

    use super::*;

    #[tokio::test]
    async fn durable_read_rejects_reversed_and_conflicting_generation() {
        let dataset = DatasetId::try_from("market_squawk.market_events").expect("valid dataset");
        let source = SourceId::try_from("crypto-test-source").expect("valid source");
        let latest = Arc::new(Mutex::new(None));
        let writer = MarketEventDurableReadWriter {
            analytical_dataset: dataset.clone(),
            source_surface: source.clone(),
            latest: Arc::clone(&latest),
        };

        assert!(
            writer
                .retain(receipt(dataset.clone(), source.clone(), 2, 2))
                .await
                .expect("newer generation retained")
        );
        assert!(
            !writer
                .retain(receipt(dataset.clone(), source.clone(), 1, 1))
                .await
                .expect("older completion ignored")
        );
        assert_eq!(
            latest
                .lock()
                .await
                .as_ref()
                .expect("latest receipt")
                .manifest()
                .manifest_version(),
            2
        );
        assert!(matches!(
            writer.retain(receipt(dataset, source, 2, 3)).await,
            Err(MarketEventReadError::DurableReadConflict)
        ));
    }

    fn receipt(
        dataset: DatasetId,
        source_id: SourceId,
        manifest_version: u64,
        marker: u8,
    ) -> MarketEventPublicationReceipt {
        let manifest = DatasetManifestRef::try_new_with_schema(
            dataset,
            manifest_version,
            DatasetSchemaRegistry::local()
                .canonical_market_events()
                .expect("canonical schema"),
            Sha256Digest::new([marker; 32]),
        )
        .expect("valid manifest");
        let publication_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [marker; 32]);
        MarketEventPublicationReceipt::try_new(
            manifest,
            publication_digest,
            ProviderMarketEventPublicationKind::EventMicrobatch,
            ProviderNativeLineageImplementation::KrakenSpotV1,
            source_id,
            SourceIdentifier::try_from("crypto-test-events").expect("valid provider dataset"),
            MarketEventSealedReceiptEvidence::Single(publication_digest),
            1,
        )
        .expect("valid durable generation")
    }
}
