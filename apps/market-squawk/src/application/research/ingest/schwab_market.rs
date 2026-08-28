//! Application-owned sealing and immutable publication for read-only Schwab market data.
//!
//! This boundary accepts only daily price history, quotes, option chains/expirations, and
//! Level-One Streamer market events. It has no account, position, transaction, order, preview,
//! replacement, cancellation, or money-movement surface.

use std::{
    fmt,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_schwab::{
    AccessTokenGeneration, ConnectionGeneration, ExecutedRestResponse, ParseBounds, ReadOnlyRoute,
    SchwabCaptureCoordinates, SchwabDailyPriceHistoryPublicationRequest,
    SchwabOAuthAuthorityReceipt, SchwabPriceHistoryMarketDataEvidence,
    SchwabPriceHistoryPublicationError, SchwabRestOptionDisposition,
    SchwabRestOptionMarketDataEvidence, SchwabRestOptionPublicationError,
    SchwabRestOptionPublicationOutcome, SchwabRestOptionPublicationRequest,
    SchwabRestQuoteDisposition, SchwabRestQuotePublicationError, SchwabRestQuotePublicationOutcome,
    SchwabRestQuotePublicationRequest, SchwabSealedRawRestOptionPublication,
    SchwabSealedRawRestQuotePublication, SchwabSealedRawStreamerPublication,
    SchwabSealedRestResponse, SchwabSealedStreamerCapture, SchwabStreamerPublicationError,
    SchwabStreamerQuotePublicationOutcome, SchwabStreamerQuotePublicationRequest,
    SchwabStreamerRecordDisposition, SchwabTransportError, StreamerMicrobatch,
};
use market_squawk_data::{
    CommittedDataset, DatasetId, IngestError, IngestIdentity, IngestPrecommitAuthority,
    PersistedProviderCaptureBindingEvidence, PersistedProviderOptionMarketBindingEvidence,
    PersistedProviderPublicationEvidence, ProviderMarketEventArrowBatch,
    ProviderMarketEventPublicationKind, ProviderOptionMarketArrowBatch, RightsError,
    SourceOperation, extraction_provider_payload_digest, provider_market_event_publication_digest,
    provider_option_market_publication_digest,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    OptionMarketBatchKind, ProviderCaptureError, ProviderNativeLineageImplementation,
    RuntimeCapabilityDisposition, SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily,
    SealedProviderEventMicrobatchBinding, SealedProviderPublicationBinding,
    SealedProviderResponseMarketEventBinding,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ResearchIngestCompositionError, ResearchProviderPublicationLease,
    ResearchProviderRuntimeGeneration, ResearchRightsAuthority,
};
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// Exact-generation runtime authority required by every Schwab publication.
///
/// The coordinator implements this beside `ResearchProviderAdmission`; this provider-specific
/// module cannot manufacture an admission or publication lease.
pub(super) trait SchwabMarketRuntimeAdmission: fmt::Debug + Send + Sync + 'static {
    fn generation_digest(&self) -> Option<EvidenceDigest>;

    fn ensure_live(&self) -> Result<(), ResearchIngestCompositionError>;

    /// Revalidates the exact protected OAuth receipt against the process-local current epoch.
    ///
    /// A receipt's provider timestamps are insufficient after token rotation or revocation. The
    /// runtime implementation must bind this check to the same protected authority that supplied
    /// transient access tokens.
    fn validate_oauth_current(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), ResearchIngestCompositionError>;

    fn cancellation(&self) -> &CancellationToken;

    fn acquire_publication_lease(
        &self,
    ) -> BoxFuture<'_, Result<ResearchProviderPublicationLease, ResearchIngestCompositionError>>;

    fn revoke(&self);

    fn revoke_and_drain(&self) -> BoxFuture<'_, ()>;

    fn revocation_drained(&self) -> bool;
}

/// One exact-generation application capability for sealing and publishing Schwab market data.
pub(crate) struct SchwabMarketPublicationClosure {
    research: Arc<ResearchService>,
    generation: ResearchProviderRuntimeGeneration,
    rights: ResearchRightsAuthority,
    doctor: SchwabMarketDataDoctorReceiptV1,
    admission: Arc<dyn SchwabMarketRuntimeAdmission>,
}

impl fmt::Debug for SchwabMarketPublicationClosure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabMarketPublicationClosure")
            .field("profile", self.generation.profile())
            .field("source_id", self.generation.metadata().source_id())
            .field("runtime_session_id", &self.generation.session_id())
            .field("doctor_receipt_sha256", &self.doctor.receipt_sha256())
            .finish_non_exhaustive()
    }
}

impl SchwabMarketPublicationClosure {
    /// Binds the sole application sealer and analytical writer to one coordinator-owned runtime.
    pub(super) fn try_new(
        research: Arc<ResearchService>,
        generation: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
        doctor: SchwabMarketDataDoctorReceiptV1,
        admission: Arc<dyn SchwabMarketRuntimeAdmission>,
    ) -> Result<Self, SchwabMarketPublicationError> {
        let rebuilt = ResearchProviderRuntimeGeneration::try_new(
            generation.profile().clone(),
            generation.session_id(),
            generation.capability_revision(),
            generation.capability_digest(),
            generation.credential_generation(),
            generation.secret_reference().cloned(),
            generation.authority_effective_at(),
            generation.metadata().clone(),
            rights.clone(),
        )?;
        let generation_digest = generation.generation_digest()?;
        if generation.profile().as_str() != market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
            || generation.metadata().source_id() != rights.source_id()
            || !generation.rights_admits(SourceOperation::Persist)
            || rebuilt.generation_digest()? != generation_digest
            || admission.generation_digest() != Some(generation_digest)
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        validate_static_doctor(&generation, &doctor)?;
        validate_current_doctor(&generation, &doctor, trusted_now()?)?;
        admission.ensure_live()?;
        Ok(Self {
            research,
            generation,
            rights,
            doctor,
            admission,
        })
    }

    /// Seals, maps, reserves, and atomically publishes one REST quote response.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, publication, and authority coordinates remain exact"
    )]
    pub(crate) async fn seal_and_publish_rest_quotes(
        &self,
        response: ExecutedRestResponse,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
        request: SchwabRestQuotePublicationRequest,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabRestQuoteApplicationOutcome, SchwabMarketPublicationError> {
        self.validate_rest_input(
            &response,
            &coordinates,
            &[ReadOnlyRoute::Quotes, ReadOnlyRoute::SingleQuote],
            SchwabMarketDataFamily::Quotes,
            oauth,
            observed_at,
        )?;
        let lease = self
            .acquire_publication_lease(oauth, observed_at, &cancellation)
            .await?;
        let sealed = self
            .seal_rest_response(response, coordinates, event_id, &cancellation, deadline)
            .await?;
        match sealed.into_quote_publication(request)? {
            SchwabRestQuotePublicationOutcome::SealedRaw(raw) => {
                Ok(SchwabRestQuoteApplicationOutcome::SealedRaw(raw))
            }
            SchwabRestQuotePublicationOutcome::Published(publication) => {
                publication.binding().validate()?;
                self.validate_response_binding(publication.binding())?;
                if publication.binding().native_lineage().implementation()
                    != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
                {
                    return Err(SchwabMarketPublicationError::AuthorityInvalid);
                }
                let dispositions = publication.dispositions().to_vec().into_boxed_slice();
                let binding = publication.into_binding();
                let generation = self
                    .publish_market_events(
                        binding.into(),
                        ProviderMarketEventPublicationKind::ResponseMarketEvent,
                        analytical_dataset,
                        idempotency_key,
                        observed_at,
                        lease,
                        cancellation,
                    )
                    .await?;
                Ok(SchwabRestQuoteApplicationOutcome::Published(
                    SchwabRestQuotePublicationReceipt {
                        generation,
                        dispositions,
                    },
                ))
            }
        }
    }

    /// Seals, rejoins, and publishes one exact daily-history response through the common provider
    /// publication path.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, publication, and authority coordinates remain exact"
    )]
    pub(crate) async fn seal_and_publish_daily_price_history(
        &self,
        response: ExecutedRestResponse,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
        request: SchwabDailyPriceHistoryPublicationRequest<'_>,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabPriceHistoryPublicationReceipt, SchwabMarketPublicationError> {
        self.validate_rest_input(
            &response,
            &coordinates,
            &[ReadOnlyRoute::PriceHistory],
            SchwabMarketDataFamily::PriceHistory,
            oauth,
            observed_at,
        )?;
        let lease = self
            .acquire_publication_lease(oauth, observed_at, &cancellation)
            .await?;
        let (pending, seal_request) = response.into_pending_daily_price_history_publication(
            coordinates,
            event_id,
            request,
        )?;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        let publication = pending.try_rejoin(sealed)?;
        let market_data = publication.market_data().clone();
        let (revisions, binding) = publication.into_parts();
        binding.validate()?;
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )?;
        if binding.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = self
            .rights
            .decision(payload_digest, observed_at)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let precommit: Arc<dyn IngestPrecommitAuthority> = lease;
        let ingest = ResearchIngestRequest::with_provider_publication(
            self.generation.metadata().clone(),
            rights,
            analytical_dataset,
            binding,
            revisions,
        )?
        .with_precommit_authority(precommit);
        let committed = self.research.ingest(ingest, cancellation).await?;
        Ok(SchwabPriceHistoryPublicationReceipt {
            committed,
            binding_digest,
            market_data,
        })
    }

    /// Seals and atomically publishes one option-chain or expiration response through the common
    /// immutable option-market spine.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, and authority coordinates remain exact"
    )]
    pub(crate) async fn seal_and_publish_rest_options(
        &self,
        response: ExecutedRestResponse,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
        request: SchwabRestOptionPublicationRequest,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabRestOptionApplicationOutcome, SchwabMarketPublicationError> {
        let family = match response.capture().receipt().route() {
            ReadOnlyRoute::Chains => SchwabMarketDataFamily::OptionChains,
            ReadOnlyRoute::ExpirationChain => SchwabMarketDataFamily::ExpirationChains,
            _ => return Err(SchwabMarketPublicationError::FamilyMismatch),
        };
        self.validate_rest_input(
            &response,
            &coordinates,
            &[ReadOnlyRoute::Chains, ReadOnlyRoute::ExpirationChain],
            family,
            oauth,
            observed_at,
        )?;
        let lease = self
            .acquire_publication_lease(oauth, observed_at, &cancellation)
            .await?;
        let sealed = self
            .seal_rest_response(response, coordinates, event_id, &cancellation, deadline)
            .await?;
        match sealed.into_option_publication(request)? {
            SchwabRestOptionPublicationOutcome::SealedRaw(raw) => {
                Ok(SchwabRestOptionApplicationOutcome::SealedRaw(raw))
            }
            SchwabRestOptionPublicationOutcome::Published(publication) => {
                publication.binding().validate()?;
                self.validate_capture_binding(
                    publication
                        .binding()
                        .persisted_receipt()
                        .capture()
                        .source_id(),
                    publication
                        .binding()
                        .persisted_receipt()
                        .capture()
                        .metadata_revision(),
                )?;
                if publication
                    .binding()
                    .native_lineage()
                    .schema()
                    .implementation()
                    != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
                {
                    return Err(SchwabMarketPublicationError::AuthorityInvalid);
                }
                let binding_digest = publication.binding().evidence_digest().evidence();
                let market_data = publication.market_data().clone();
                let dispositions = publication.dispositions().to_vec().into_boxed_slice();
                let (revisions, binding) = publication.into_parts();
                if revisions.len() != binding.batch().row_count()
                    || !revisions.is_locally_observed()
                    || !revisions.native_lineage_required()
                {
                    return Err(SchwabMarketPublicationError::AuthorityInvalid);
                }
                let publication_digest = provider_option_market_publication_digest(&binding)?;
                if publication_digest != binding_digest {
                    return Err(SchwabMarketPublicationError::AuthorityInvalid);
                }
                let publication_kind = binding.batch().kind();
                let provider_dataset = binding.batch().scope().dataset().clone();
                let option_row_count = binding.batch().row_count();
                let reservation = self
                    .reserve_event(
                        publication_digest,
                        idempotency_key,
                        observed_at,
                        &cancellation,
                    )
                    .await?;
                let precommit: Arc<dyn IngestPrecommitAuthority> = lease;
                let committed = self
                    .research
                    .analytical()
                    .ingest_provider_option_market(
                        reservation,
                        analytical_dataset,
                        binding,
                        cancellation,
                        precommit,
                    )
                    .await?;
                Ok(SchwabRestOptionApplicationOutcome::Published(
                    SchwabRestOptionPublicationReceipt {
                        restart: SchwabOptionMarketRestartSelector {
                            manifest: committed.manifest().clone(),
                            publication_digest,
                            publication_kind,
                            source_id: self.generation.metadata().source_id().clone(),
                            expected_option_row_count: option_row_count,
                        },
                        committed,
                        binding_digest,
                        provider_dataset,
                        market_data,
                        dispositions,
                    },
                ))
            }
        }
    }

    /// Seals, maps, reserves, and atomically publishes one Streamer microbatch.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, publication, and authority coordinates remain exact"
    )]
    pub(crate) async fn seal_and_publish_streamer_quotes(
        &self,
        microbatch: StreamerMicrobatch,
        event_ids: Vec<Uuid>,
        parse_bounds: ParseBounds,
        request: SchwabStreamerQuotePublicationRequest,
        family: SchwabMarketDataFamily,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabStreamerApplicationOutcome, SchwabMarketPublicationError> {
        let receipt = microbatch.receipt();
        let coordinates = microbatch.connection().coordinates();
        self.validate_coordinates(coordinates)?;
        self.validate_doctor_family(family, observed_at)?;
        validate_streamer_family(family)?;
        self.validate_doctor_oauth(oauth, observed_at)?;
        if receipt.token_generation() != oauth.generation()
            || timestamp_from_unix_millis(receipt.last_received_at_unix_millis())? > observed_at
            || microbatch
                .frames()
                .iter()
                .any(|frame| frame.generation() != receipt.generation())
        {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        let connection_generation = receipt.generation();
        let token_generation = receipt.token_generation();
        let frame_count = receipt.frame_count();
        let stream_identity = microbatch.connection().stream_identity().clone();
        let lease = self
            .acquire_publication_lease(oauth, observed_at, &cancellation)
            .await?;
        let (pending, seal_request) = microbatch.into_pending_capture(event_ids, parse_bounds)?;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        let sealed: SchwabSealedStreamerCapture = pending.try_rejoin(sealed)?;
        self.validate_coordinates(sealed.coordinates())?;
        if sealed.streamer_receipt().generation() != connection_generation
            || sealed.streamer_receipt().token_generation() != token_generation
            || sealed.streamer_receipt().frame_count() != frame_count
            || sealed.stream_identity() != &stream_identity
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        match sealed.into_level_one_quote_publication(request)? {
            SchwabStreamerQuotePublicationOutcome::SealedRaw(raw) => {
                Ok(SchwabStreamerApplicationOutcome::SealedRaw(raw))
            }
            SchwabStreamerQuotePublicationOutcome::Published(publication) => {
                publication.binding().validate()?;
                self.validate_event_binding(publication.binding())?;
                if publication.binding().native_lineage().implementation()
                    != ProviderNativeLineageImplementation::SchwabStreamerMarketDataV1
                {
                    return Err(SchwabMarketPublicationError::AuthorityInvalid);
                }
                let dispositions = publication.dispositions().to_vec().into_boxed_slice();
                let binding = publication.into_binding();
                let generation = self
                    .publish_market_events(
                        binding.into(),
                        ProviderMarketEventPublicationKind::EventMicrobatch,
                        analytical_dataset,
                        idempotency_key,
                        observed_at,
                        lease,
                        cancellation,
                    )
                    .await?;
                Ok(SchwabStreamerApplicationOutcome::Published(
                    SchwabStreamerPublicationReceipt {
                        generation,
                        connection_generation,
                        token_generation,
                        stream_identity,
                        frame_count,
                        dispositions,
                    },
                ))
            }
        }
    }

    /// Reopens the exact sealed history binding retained by one committed immutable generation.
    pub(crate) fn read_price_history_capture_evidence(
        &self,
        receipt: &SchwabPriceHistoryPublicationReceipt,
    ) -> Result<PersistedProviderCaptureBindingEvidence, SchwabMarketPublicationError> {
        let store = self.research.provider_capture_store();
        self.research
            .analytical()
            .provider_capture_binding_evidence(
                receipt.committed().manifest(),
                receipt.binding_digest(),
                store.as_ref(),
            )
            .map_err(Into::into)
    }

    pub(crate) fn begin_revocation(&self) {
        self.admission.revoke();
    }

    pub(crate) async fn finish_revocation_drain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.begin_revocation();
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(SchwabMarketPublicationError::Cancelled),
            () = self.admission.revoke_and_drain() => Ok(()),
        }
    }

    pub(crate) fn revocation_drained(&self) -> bool {
        self.admission.revocation_drained()
    }

    async fn seal_rest_response(
        &self,
        response: ExecutedRestResponse,
        coordinates: SchwabCaptureCoordinates,
        event_id: Uuid,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabSealedRestResponse, SchwabMarketPublicationError> {
        let pending = response.into_pending_capture(coordinates, event_id)?;
        let (rejoin, seal_request) = pending.into_sealing_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, cancellation, deadline)
            .await?;
        rejoin.try_rejoin(sealed).map_err(Into::into)
    }

    async fn acquire_publication_lease(
        &self,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Arc<SchwabMarketPublicationLease>, SchwabMarketPublicationError> {
        self.validate_doctor_oauth(oauth, observed_at)?;
        self.admission.ensure_live()?;
        self.admission.validate_oauth_current(oauth)?;
        let generation = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(SchwabMarketPublicationError::Cancelled),
            () = self.admission.cancellation().cancelled() => {
                return Err(SchwabMarketPublicationError::AuthorityRevoked);
            }
            generation = self.admission.acquire_publication_lease() => generation?,
        };
        self.admission.ensure_live()?;
        self.admission.validate_oauth_current(oauth)?;
        Ok(Arc::new(SchwabMarketPublicationLease {
            generation: Arc::new(generation),
            generation_digest: self.generation.generation_digest()?,
            oauth,
            admission: Arc::clone(&self.admission),
            exclusive_expires_at: exact_exclusive_expiry(&self.generation, &self.doctor, oauth)?,
        }))
    }

    async fn publish_market_events(
        &self,
        binding: SealedProviderPublicationBinding,
        kind: ProviderMarketEventPublicationKind,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        lease: Arc<SchwabMarketPublicationLease>,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketEventPublicationReceipt, SchwabMarketPublicationError> {
        lease.validate_precommit_exact()?;
        let runtime_generation_digest = self.generation.generation_digest()?;
        if lease.generation_digest() != runtime_generation_digest {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let oauth_generation = lease.oauth_generation();
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        if publication_digest.algorithm() != DigestAlgorithm::Sha256
            || publication_digest.bytes() == [0; 32]
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let (sealed_receipt_digest, provider_dataset, event_count) = match (&binding, kind) {
            (
                SealedProviderPublicationBinding::ResponseMarketEvent(binding),
                ProviderMarketEventPublicationKind::ResponseMarketEvent,
            ) => (
                binding.sealed_receipt_digest(),
                binding.capture_evidence().dataset().clone(),
                binding.record_count(),
            ),
            (
                SealedProviderPublicationBinding::EventMicrobatch(binding),
                ProviderMarketEventPublicationKind::EventMicrobatch,
            ) => (
                binding.sealed_receipt_digest(),
                binding.capture_evidence().dataset().clone(),
                binding.record_count(),
            ),
            _ => return Err(SchwabMarketPublicationError::FamilyMismatch),
        };
        if event_count == 0 || sealed_receipt_digest.bytes() == [0; 32] {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let reservation = self
            .reserve_event(
                publication_digest,
                idempotency_key,
                observed_at,
                &cancellation,
            )
            .await?;
        let precommit: Arc<dyn IngestPrecommitAuthority> = lease;
        let committed = self
            .research
            .analytical()
            .ingest_provider_market_events(
                reservation,
                analytical_dataset,
                binding,
                cancellation,
                precommit,
            )
            .await?;
        Ok(SchwabMarketEventPublicationReceipt {
            restart: SchwabMarketEventRestartSelector {
                manifest: committed.manifest().clone(),
                publication_digest,
                publication_kind: kind,
                source_id: self.generation.metadata().source_id().clone(),
                expected_event_count: event_count,
            },
            committed,
            sealed_receipt_digest,
            provider_dataset,
            event_count,
            runtime_generation_digest,
            oauth_generation,
        })
    }

    async fn reserve_event(
        &self,
        payload_digest: EvidenceDigest,
        idempotency_key: impl Into<String>,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::IngestReservation, SchwabMarketPublicationError> {
        let identity = IngestIdentity::try_new(
            self.generation.metadata().source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            idempotency_key,
        )?;
        let rights = self
            .rights
            .decision(payload_digest, observed_at)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        self.research
            .analytical()
            .reserve_source_ingest(
                self.generation.metadata(),
                self.generation.authority_effective_at(),
                rights,
                &identity,
                cancellation,
            )
            .await
            .map_err(Into::into)
    }

    fn validate_rest_input(
        &self,
        response: &ExecutedRestResponse,
        coordinates: &SchwabCaptureCoordinates,
        routes: &[ReadOnlyRoute],
        family: SchwabMarketDataFamily,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.validate_coordinates(coordinates)?;
        let receipt = response.capture().receipt();
        self.validate_doctor_family(family, observed_at)?;
        self.validate_doctor_oauth(oauth, observed_at)?;
        if !routes.contains(&receipt.route())
            || receipt.token_generation() != oauth.generation()
            || timestamp_from_unix_millis(receipt.received_at_unix_millis())? > observed_at
        {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        Ok(())
    }

    fn validate_doctor_family(
        &self,
        family: SchwabMarketDataFamily,
        observed_at: Timestamp,
    ) -> Result<(), SchwabMarketPublicationError> {
        validate_current_doctor(&self.generation, &self.doctor, observed_at)?;
        let admitted = self
            .doctor
            .observation()
            .families
            .iter()
            .find(|evidence| evidence.family == family)
            .is_some_and(|evidence| {
                matches!(
                    evidence.disposition,
                    RuntimeCapabilityDisposition::Available
                        | RuntimeCapabilityDisposition::Degraded
                )
            });
        if !admitted {
            return Err(SchwabMarketPublicationError::FamilyUnavailable);
        }
        Ok(())
    }

    fn validate_doctor_oauth(
        &self,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
    ) -> Result<(), SchwabMarketPublicationError> {
        validate_oauth_receipt(oauth, observed_at)?;
        validate_doctor_oauth_binding(&self.generation, &self.doctor, oauth, observed_at)
    }

    fn validate_coordinates(
        &self,
        coordinates: &SchwabCaptureCoordinates,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.validate_capture_binding(coordinates.source_id(), coordinates.metadata_revision())
    }

    fn validate_response_binding(
        &self,
        binding: &SealedProviderResponseMarketEventBinding,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )
    }

    fn validate_event_binding(
        &self,
        binding: &SealedProviderEventMicrobatchBinding,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )
    }

    fn validate_capture_binding(
        &self,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
    ) -> Result<(), SchwabMarketPublicationError> {
        if source_id != self.generation.metadata().source_id()
            || metadata_revision != self.generation.metadata().revision()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

/// Revocable exact-generation lease retained through physical sealing and durable precommit.
pub(crate) struct SchwabMarketPublicationLease {
    generation: Arc<ResearchProviderPublicationLease>,
    generation_digest: EvidenceDigest,
    oauth: SchwabOAuthAuthorityReceipt,
    admission: Arc<dyn SchwabMarketRuntimeAdmission>,
    exclusive_expires_at: Timestamp,
}

impl fmt::Debug for SchwabMarketPublicationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabMarketPublicationLease")
            .field("generation", &self.generation)
            .field("generation_digest", &self.generation_digest)
            .field("oauth_generation", &self.oauth.generation().get())
            .field("exclusive_expires_at", &self.exclusive_expires_at)
            .finish_non_exhaustive()
    }
}

impl SchwabMarketPublicationLease {
    pub(crate) const fn generation_digest(&self) -> EvidenceDigest {
        self.generation_digest
    }

    pub(crate) const fn oauth_generation(&self) -> AccessTokenGeneration {
        self.oauth.generation()
    }

    fn validate_precommit_exact(&self) -> Result<(), SchwabMarketPublicationError> {
        self.generation
            .validate_precommit()
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        self.admission
            .ensure_live()
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        self.admission
            .validate_oauth_current(self.oauth)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        if trusted_now()? >= self.exclusive_expires_at {
            return Err(SchwabMarketPublicationError::AuthorityExpired);
        }
        Ok(())
    }
}

impl IngestPrecommitAuthority for SchwabMarketPublicationLease {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.validate_precommit_exact()
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)
    }
}

/// Exact generation-owned selector for one durable Schwab quote or Streamer publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchwabMarketEventRestartSelector {
    manifest: market_squawk_data::DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    source_id: SourceId,
    expected_event_count: usize,
}

impl SchwabMarketEventRestartSelector {
    pub(crate) const fn manifest(&self) -> &market_squawk_data::DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) const fn publication_kind(&self) -> ProviderMarketEventPublicationKind {
        self.publication_kind
    }

    /// Reopens the exact kind-qualified raw evidence and typed Parquet rows after restart.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketEventRestartReceipt, SchwabMarketPublicationError> {
        let selector = research
            .analytical()
            .provider_market_event_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(SchwabMarketPublicationError::RestartInvalid)?;
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
            return Err(SchwabMarketPublicationError::RestartInvalid);
        }
        Ok(SchwabMarketEventRestartReceipt { events, evidence })
    }
}

#[derive(Debug)]
pub(crate) struct SchwabMarketEventRestartReceipt {
    events: ProviderMarketEventArrowBatch,
    evidence: PersistedProviderPublicationEvidence,
}

impl SchwabMarketEventRestartReceipt {
    pub(crate) const fn events(&self) -> &ProviderMarketEventArrowBatch {
        &self.events
    }

    pub(crate) const fn evidence(&self) -> &PersistedProviderPublicationEvidence {
        &self.evidence
    }
}

#[derive(Debug)]
pub(crate) struct SchwabMarketEventPublicationReceipt {
    committed: CommittedDataset,
    restart: SchwabMarketEventRestartSelector,
    sealed_receipt_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    event_count: usize,
    runtime_generation_digest: EvidenceDigest,
    oauth_generation: AccessTokenGeneration,
}

impl SchwabMarketEventPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SchwabMarketEventRestartSelector {
        &self.restart
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.restart.publication_digest()
    }

    pub(crate) const fn sealed_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_receipt_digest
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn event_count(&self) -> usize {
        self.event_count
    }

    pub(crate) const fn runtime_generation_digest(&self) -> EvidenceDigest {
        self.runtime_generation_digest
    }

    pub(crate) const fn oauth_generation(&self) -> AccessTokenGeneration {
        self.oauth_generation
    }
}

#[derive(Debug)]
pub(crate) enum SchwabRestQuoteApplicationOutcome {
    Published(SchwabRestQuotePublicationReceipt),
    SealedRaw(Box<SchwabSealedRawRestQuotePublication>),
}

#[derive(Debug)]
pub(crate) struct SchwabRestQuotePublicationReceipt {
    generation: SchwabMarketEventPublicationReceipt,
    dispositions: Box<[SchwabRestQuoteDisposition]>,
}

impl SchwabRestQuotePublicationReceipt {
    pub(crate) const fn generation(&self) -> &SchwabMarketEventPublicationReceipt {
        &self.generation
    }

    pub(crate) const fn dispositions(&self) -> &[SchwabRestQuoteDisposition] {
        &self.dispositions
    }
}

#[derive(Debug)]
pub(crate) enum SchwabStreamerApplicationOutcome {
    Published(SchwabStreamerPublicationReceipt),
    SealedRaw(Box<SchwabSealedRawStreamerPublication>),
}

#[derive(Debug)]
pub(crate) struct SchwabStreamerPublicationReceipt {
    generation: SchwabMarketEventPublicationReceipt,
    connection_generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    stream_identity: SourceIdentifier,
    frame_count: u64,
    dispositions: Box<[SchwabStreamerRecordDisposition]>,
}

impl SchwabStreamerPublicationReceipt {
    pub(crate) const fn generation(&self) -> &SchwabMarketEventPublicationReceipt {
        &self.generation
    }

    pub(crate) const fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation
    }

    pub(crate) const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }

    pub(crate) const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    pub(crate) const fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub(crate) const fn dispositions(&self) -> &[SchwabStreamerRecordDisposition] {
        &self.dispositions
    }
}

#[derive(Debug)]
pub(crate) struct SchwabPriceHistoryPublicationReceipt {
    committed: CommittedDataset,
    binding_digest: EvidenceDigest,
    market_data: SchwabPriceHistoryMarketDataEvidence,
}

impl SchwabPriceHistoryPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(crate) const fn market_data(&self) -> &SchwabPriceHistoryMarketDataEvidence {
        &self.market_data
    }
}

#[derive(Debug)]
pub(crate) enum SchwabRestOptionApplicationOutcome {
    Published(SchwabRestOptionPublicationReceipt),
    SealedRaw(Box<SchwabSealedRawRestOptionPublication>),
}

#[derive(Debug)]
pub(crate) struct SchwabRestOptionPublicationReceipt {
    committed: CommittedDataset,
    restart: SchwabOptionMarketRestartSelector,
    binding_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    market_data: SchwabRestOptionMarketDataEvidence,
    dispositions: Box<[SchwabRestOptionDisposition]>,
}

impl SchwabRestOptionPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &SchwabOptionMarketRestartSelector {
        &self.restart
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn market_data(&self) -> &SchwabRestOptionMarketDataEvidence {
        &self.market_data
    }

    pub(crate) const fn dispositions(&self) -> &[SchwabRestOptionDisposition] {
        &self.dispositions
    }
}

/// Exact generation-owned selector for one immutable Schwab option response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchwabOptionMarketRestartSelector {
    manifest: market_squawk_data::DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
    source_id: SourceId,
    expected_option_row_count: usize,
}

impl SchwabOptionMarketRestartSelector {
    pub(crate) const fn manifest(&self) -> &market_squawk_data::DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) const fn publication_kind(&self) -> OptionMarketBatchKind {
        self.publication_kind
    }

    /// Reopens the exact sealed evidence and typed option batch after process restart.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<SchwabOptionMarketRestartReceipt, SchwabMarketPublicationError> {
        let selector = research
            .analytical()
            .provider_option_market_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(SchwabMarketPublicationError::RestartInvalid)?;
        let store = research.provider_capture_store();
        let evidence = research
            .analytical()
            .provider_option_market_publication_evidence(
                &self.manifest,
                selector,
                store.as_ref(),
            )?;
        if evidence.binding_digest() != self.publication_digest
            || evidence.publication_kind() != self.publication_kind
            || evidence.capture().source_id() != &self.source_id
            || evidence.canonical_row_count() != self.expected_option_row_count
        {
            return Err(SchwabMarketPublicationError::RestartInvalid);
        }
        let batch = research
            .analytical()
            .read_provider_option_market_publication(
                &self.manifest,
                selector,
                store.as_ref(),
                cancellation,
            )
            .await?;
        let option_row_count = match self.publication_kind {
            OptionMarketBatchKind::Snapshots => batch
                .snapshots()
                .map(<[_]>::len)
                .ok_or(SchwabMarketPublicationError::RestartInvalid)?,
            OptionMarketBatchKind::Expirations => batch
                .expirations()
                .map(<[_]>::len)
                .ok_or(SchwabMarketPublicationError::RestartInvalid)?,
        };
        if batch.publication_digest() != self.publication_digest
            || batch.publication_kind() != self.publication_kind
            || batch.scope().source_id() != &self.source_id
            || option_row_count != self.expected_option_row_count
        {
            return Err(SchwabMarketPublicationError::RestartInvalid);
        }
        Ok(SchwabOptionMarketRestartReceipt { batch, evidence })
    }
}

#[derive(Debug)]
pub(crate) struct SchwabOptionMarketRestartReceipt {
    batch: ProviderOptionMarketArrowBatch,
    evidence: PersistedProviderOptionMarketBindingEvidence,
}

impl SchwabOptionMarketRestartReceipt {
    pub(crate) const fn batch(&self) -> &ProviderOptionMarketArrowBatch {
        &self.batch
    }

    pub(crate) const fn evidence(&self) -> &PersistedProviderOptionMarketBindingEvidence {
        &self.evidence
    }
}

fn validate_restart_evidence(
    expected: &SchwabMarketEventRestartSelector,
    evidence: &PersistedProviderPublicationEvidence,
) -> Result<(), SchwabMarketPublicationError> {
    if evidence.publication_digest() != expected.publication_digest
        || evidence.publication_kind() != expected.publication_kind.as_str()
    {
        return Err(SchwabMarketPublicationError::RestartInvalid);
    }
    let (source_id, event_count) = match (expected.publication_kind, evidence) {
        (
            ProviderMarketEventPublicationKind::ResponseMarketEvent,
            PersistedProviderPublicationEvidence::ResponseMarketEvent(response),
        ) => (
            response.capture().source_id(),
            response.canonical_event_count(),
        ),
        (
            ProviderMarketEventPublicationKind::EventMicrobatch,
            PersistedProviderPublicationEvidence::EventMicrobatch(event),
        ) => (event.capture().source_id(), event.canonical_event_count()),
        _ => return Err(SchwabMarketPublicationError::RestartInvalid),
    };
    if source_id != &expected.source_id || event_count != expected.expected_event_count {
        return Err(SchwabMarketPublicationError::RestartInvalid);
    }
    Ok(())
}

fn timestamp_from_unix_millis(
    milliseconds: u64,
) -> Result<Timestamp, SchwabMarketPublicationError> {
    let nanos = milliseconds
        .checked_mul(NANOS_PER_MILLISECOND)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn timestamp_from_unix_seconds(seconds: u64) -> Result<Timestamp, SchwabMarketPublicationError> {
    let nanos = seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn validate_oauth_receipt(
    oauth: SchwabOAuthAuthorityReceipt,
    observed_at: Timestamp,
) -> Result<(), SchwabMarketPublicationError> {
    let access_issued_at = timestamp_from_unix_seconds(oauth.access_issued_at_unix_seconds())?;
    let access_expires_at = timestamp_from_unix_seconds(oauth.access_expires_at_unix_seconds())?;
    let refresh_authorized_at =
        timestamp_from_unix_seconds(oauth.refresh_authorized_at_unix_seconds())?;
    let refresh_expires_at = timestamp_from_unix_seconds(oauth.refresh_expires_at_unix_seconds())?;
    if observed_at < access_issued_at
        || observed_at >= access_expires_at
        || observed_at < refresh_authorized_at
        || observed_at >= refresh_expires_at
    {
        return Err(SchwabMarketPublicationError::AuthorityInvalid);
    }
    Ok(())
}

fn validate_static_doctor(
    generation: &ResearchProviderRuntimeGeneration,
    doctor: &SchwabMarketDataDoctorReceiptV1,
) -> Result<(), SchwabMarketPublicationError> {
    let exact_session = generation.session_id().to_string();
    if doctor.surface_id().as_str() != market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
        || doctor.session_identifier().as_str() != exact_session
        || generation.credential_generation() != Some(doctor.application_credential_generation())
        || generation.capability_revision() != doctor.capability_revision()
        || generation.capability_digest() != doctor.capability_digest()
        || generation.parent_rights_authorization_evidence() != doctor.rights_decision_digest()
        || doctor.market_data_principal_sha256().bytes() == [0; 32]
        || doctor.receipt_sha256().bytes() == [0; 32]
        || !doctor.admits_source_start()
    {
        return Err(SchwabMarketPublicationError::AuthorityInvalid);
    }
    Ok(())
}

fn validate_current_doctor(
    generation: &ResearchProviderRuntimeGeneration,
    doctor: &SchwabMarketDataDoctorReceiptV1,
    observed_at: Timestamp,
) -> Result<(), SchwabMarketPublicationError> {
    let mut expiry = doctor.exclusive_expires_at();
    if let Some(rights_expiry) = generation.rights_authorization_expires_at() {
        expiry = expiry.min(rights_expiry);
    }
    if !doctor.is_current_at(observed_at)
        || !generation.metadata().is_effective_at(observed_at)
        || observed_at < generation.authority_effective_at()
        || observed_at >= expiry
    {
        return Err(SchwabMarketPublicationError::AuthorityExpired);
    }
    Ok(())
}

fn validate_doctor_oauth_binding(
    generation: &ResearchProviderRuntimeGeneration,
    doctor: &SchwabMarketDataDoctorReceiptV1,
    oauth: SchwabOAuthAuthorityReceipt,
    observed_at: Timestamp,
) -> Result<(), SchwabMarketPublicationError> {
    validate_current_doctor(generation, doctor, observed_at)?;
    let observation = doctor.observation();
    if doctor.access_token_generation() != oauth.generation().get()
        || observation.access_issued_at
            != timestamp_from_unix_seconds(oauth.access_issued_at_unix_seconds())?
        || observation.access_expires_at
            != timestamp_from_unix_seconds(oauth.access_expires_at_unix_seconds())?
        || observation.refresh_authorized_at
            != timestamp_from_unix_seconds(oauth.refresh_authorized_at_unix_seconds())?
        || observation.refresh_expires_at
            != timestamp_from_unix_seconds(oauth.refresh_expires_at_unix_seconds())?
        || observed_at >= exact_exclusive_expiry(generation, doctor, oauth)?
    {
        return Err(SchwabMarketPublicationError::AuthorityInvalid);
    }
    Ok(())
}

fn exact_exclusive_expiry(
    generation: &ResearchProviderRuntimeGeneration,
    doctor: &SchwabMarketDataDoctorReceiptV1,
    oauth: SchwabOAuthAuthorityReceipt,
) -> Result<Timestamp, SchwabMarketPublicationError> {
    let mut expiry = doctor
        .exclusive_expires_at()
        .min(timestamp_from_unix_seconds(
            oauth.access_expires_at_unix_seconds(),
        )?)
        .min(timestamp_from_unix_seconds(
            oauth.refresh_expires_at_unix_seconds(),
        )?);
    if let Some(rights_expiry) = generation.rights_authorization_expires_at() {
        expiry = expiry.min(rights_expiry);
    }
    Ok(expiry)
}

fn validate_streamer_family(
    family: SchwabMarketDataFamily,
) -> Result<(), SchwabMarketPublicationError> {
    if matches!(
        family,
        SchwabMarketDataFamily::LevelOneEquities
            | SchwabMarketDataFamily::LevelOneOptions
            | SchwabMarketDataFamily::LevelOneFutures
            | SchwabMarketDataFamily::LevelOneFuturesOptions
            | SchwabMarketDataFamily::LevelOneForex
    ) {
        Ok(())
    } else {
        Err(SchwabMarketPublicationError::FamilyMismatch)
    }
}

fn trusted_now() -> Result<Timestamp, SchwabMarketPublicationError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

#[derive(Debug, Error)]
pub(crate) enum SchwabMarketPublicationError {
    #[error("Schwab market-data authority is structurally invalid")]
    AuthorityInvalid,
    #[error("Schwab market-data authority was revoked")]
    AuthorityRevoked,
    #[error("Schwab market-data doctor, rights, or OAuth authority expired")]
    AuthorityExpired,
    #[error("the requested Schwab market-data family is not currently admitted")]
    FamilyUnavailable,
    #[error("sealed Schwab market data does not match the requested read-only family")]
    FamilyMismatch,
    #[error("Schwab market-data publication was cancelled")]
    Cancelled,
    #[error("Schwab provider-event generation failed exact immutable restart verification")]
    RestartInvalid,
    #[error(transparent)]
    Runtime(#[from] ResearchIngestCompositionError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Rights(#[from] RightsError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Transport(#[from] SchwabTransportError),
    #[error(transparent)]
    Quote(#[from] SchwabRestQuotePublicationError),
    #[error(transparent)]
    History(#[from] SchwabPriceHistoryPublicationError),
    #[error(transparent)]
    Option(#[from] SchwabRestOptionPublicationError),
    #[error(transparent)]
    Streamer(#[from] SchwabStreamerPublicationError),
}
