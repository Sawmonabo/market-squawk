//! Application-owned sealing and immutable publication for read-only Schwab market data.
//!
//! This boundary accepts only daily price history, quotes, option chains/expirations, and
//! Level-One Streamer market events. It has no account, position, transaction, order, preview,
//! replacement, cancellation, or money-movement surface.

mod history;
mod market_hours;
pub(crate) use market_hours::SchwabMarketHoursPublicationReceipt;

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_schwab::{
    AccessTokenGeneration, ConnectionGeneration, ExecutedRestResponse, ParseBounds, ReadOnlyRoute,
    SchwabAdapterError, SchwabCaptureCoordinates, SchwabDailyPriceHistoryPublicationRequest,
    SchwabOAuthAuthorityReceipt, SchwabPriceHistoryMarketDataEvidence,
    SchwabPriceHistoryPublicationError, SchwabRestOptionDisposition,
    SchwabRestOptionMarketDataEvidence, SchwabRestOptionPublicationError,
    SchwabRestOptionPublicationOutcome, SchwabRestOptionPublicationRequest,
    SchwabRestQuoteDisposition, SchwabRestQuotePublicationError,
    SchwabSealedRawRestOptionPublication, SchwabSealedRawStreamerPublication,
    SchwabSealedRestQuotePublication, SchwabSealedRestResponse, SchwabSealedStreamerCapture,
    SchwabStreamerPublicationError, SchwabStreamerQuotePublicationOutcome,
    SchwabStreamerQuotePublicationRequest, SchwabStreamerRecordDisposition, SchwabTransportError,
    StreamerMicrobatch,
};
use market_squawk_data::{
    CommittedDataset, DatasetId, IngestError, IngestIdentity, IngestPrecommitAuthority,
    MarketEventCommitRef, PersistedProviderCaptureBindingEvidence,
    PersistedProviderOptionMarketBindingEvidence, PersistedProviderPublicationEvidence,
    ProviderMarketEventArrowBatch, ProviderMarketEventPublicationKind,
    ProviderMarketEventPublicationSelector, ProviderOptionMarketArrowBatch,
    ProviderOptionMarketPublicationSelector, RightsError, SourceOperation,
    extraction_provider_payload_digest, provider_market_event_publication_digest,
    provider_option_market_publication_digest,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    OptionMarketBatchKind, ProviderCaptureError, ProviderCaptureSealRequest,
    ProviderNativeLineageImplementation, RuntimeCapabilityDisposition,
    SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily, SealedProviderCaptureMaterial,
    SealedProviderEventMicrobatchBinding, SealedProviderPublicationBinding,
    SealedProviderResponseMarketEventBinding, SourceMetadata,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ResearchIngestCompositionError, ResearchProviderPublicationLease,
    ResearchProviderRuntimeGeneration, ResearchRightsAuthority,
};
use crate::provider_activation::SchwabQuotePublicationSelection;
use crate::provider_onboarding::SchwabOAuthPublicationEpoch;
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

const NANOS_PER_MILLISECOND: u64 = 1_000_000;
const NANOS_PER_SECOND: u64 = 1_000_000_000;
pub(super) const SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET: &str = "market_squawk.market_events";

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

/// Opaque exact-generation authority consumed by the Schwab REST quote sink.
///
/// Source metadata, provider/analytical datasets, capture coordinates, and the durable
/// publication closure are derived once from the admitted research generation. The transport
/// runtime cannot replace any of them with caller-authored identity strings or UUIDs.
pub(crate) struct SchwabRestQuoteGenerationAuthority {
    closure: Arc<SchwabMarketPublicationClosure>,
    coordinates: SchwabCaptureCoordinates,
    session_identifier: SourceIdentifier,
    analytical_dataset: DatasetId,
    operation_timeout: Duration,
    latest_source_health: Mutex<Option<SchwabRestQuoteSourceHealthOutcome>>,
}

impl fmt::Debug for SchwabRestQuoteGenerationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabRestQuoteGenerationAuthority")
            .field("source_id", self.closure.generation.metadata().source_id())
            .field("provider_dataset", self.coordinates.dataset())
            .field("analytical_dataset", &self.analytical_dataset)
            .field("session_identifier", &self.session_identifier)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

/// Generation-local source-health evidence retained after the raw body crosses the sole sealer.
///
/// This is a bounded single-outcome health surface, not a provider-data store. Raw bytes and exact
/// physical evidence remain solely in the research capture store. A later live-runtime owner may
/// consume this outcome when updating registered source health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuoteSourceHealthOutcome {
    ProviderRejected {
        status: u16,
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: EvidenceDigest,
    },
    InvalidPayload {
        status: u16,
        error: SchwabAdapterError,
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: EvidenceDigest,
    },
    AllRowsAbstained {
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: EvidenceDigest,
        dispositions: Box<[SchwabRestQuoteDisposition]>,
    },
    PostSealPublicationUnavailable {
        payload_digest: EvidenceDigest,
        sealed_receipt_digest: Option<EvidenceDigest>,
        reason: SchwabRestQuotePostSealFailure,
    },
    DurablePublishedCurrentUnavailable {
        publication_digest: EvidenceDigest,
        sealed_receipt_digest: EvidenceDigest,
        event_count: usize,
        dispositions: Box<[SchwabRestQuoteDisposition]>,
    },
}

/// Closed source-health classification for a canonical response retained raw but not published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchwabRestQuotePostSealFailure {
    Deadline,
    ShutdownOrRevocation,
    AuthorityOrBinding,
    StorageUnavailable,
}

impl SchwabRestQuoteGenerationAuthority {
    pub(crate) fn metadata(&self) -> &SourceMetadata {
        self.closure.generation.metadata()
    }

    pub(crate) fn coordinates(&self) -> SchwabCaptureCoordinates {
        self.coordinates.clone()
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        self.coordinates.dataset()
    }

    pub(crate) const fn session_identifier(&self) -> &SourceIdentifier {
        &self.session_identifier
    }

    pub(crate) const fn operation_timeout(&self) -> Duration {
        self.operation_timeout
    }

    pub(crate) async fn seal_capture(
        &self,
        request: ProviderCaptureSealRequest,
        deadline: Instant,
    ) -> Result<SealedProviderCaptureMaterial, SchwabMarketPublicationError> {
        // A completed provider response must remain sealable during runtime shutdown. The finite
        // deadline bounds the worker; a late physical object is recovered by the existing startup
        // quarantine/recovery path.
        let sealing_cancellation = CancellationToken::new();
        self.closure
            .research
            .seal_provider_capture(request, &sealing_cancellation, deadline)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn publish_sealed_rest_quotes(
        &self,
        publication: Box<SchwabSealedRestQuotePublication>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: Option<crate::provider_activation::ProviderAccountPublicationAuthority>,
        selection: SchwabQuotePublicationSelection,
        observed_at: Timestamp,
        idempotency_key: String,
        deadline: Instant,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabMarketPublicationError> {
        self.closure
            .publish_already_sealed_rest_quotes(
                publication,
                oauth_epoch,
                account,
                selection,
                observed_at,
                self.analytical_dataset.clone(),
                idempotency_key,
                deadline,
            )
            .await
    }

    pub(crate) fn record_source_health(
        &self,
        outcome: SchwabRestQuoteSourceHealthOutcome,
    ) -> Result<(), SchwabMarketPublicationError> {
        let mut latest = self
            .latest_source_health
            .lock()
            .map_err(|_error| SchwabMarketPublicationError::SourceHealthUnavailable)?;
        *latest = Some(outcome);
        Ok(())
    }

    pub(crate) fn latest_source_health(
        &self,
    ) -> Result<Option<SchwabRestQuoteSourceHealthOutcome>, SchwabMarketPublicationError> {
        self.latest_source_health
            .lock()
            .map(|latest| latest.clone())
            .map_err(|_error| SchwabMarketPublicationError::SourceHealthUnavailable)
    }

    pub(crate) fn begin_revocation(&self) {
        self.closure.begin_revocation();
    }

    pub(crate) async fn finish_revocation_drain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), SchwabMarketPublicationError> {
        self.closure.finish_revocation_drain(cancellation).await
    }
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

impl SchwabRestQuoteGenerationAuthority {
    #[cfg(test)]
    pub(crate) fn bind_test_rest_quote_sink(
        research: Arc<ResearchService>,
        generation: ResearchProviderRuntimeGeneration,
        rights: ResearchRightsAuthority,
        doctor: SchwabMarketDataDoctorReceiptV1,
        oauth: crate::provider_onboarding::SchwabOAuthMarketAuthority,
        oauth_receipt: SchwabOAuthAuthorityReceipt,
        operation_timeout: Duration,
    ) -> Result<Arc<SchwabRestQuoteGenerationAuthority>, SchwabMarketPublicationError> {
        let admission = super::provider_runtime::test_schwab_composite_market_runtime_admission(
            &generation,
            oauth.receipt_currentness(),
            oauth_receipt,
        )?;
        Arc::new(SchwabMarketPublicationClosure::try_new(
            research, generation, rights, doctor, admission,
        )?)
        .bind_rest_quote_sink(
            operation_timeout,
            DatasetId::try_from(SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET)
                .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?,
        )
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
        if !(generation.profile().as_str() == market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
            || generation.profile().as_str() == SCHWAB_STREAMER_PROFILE
                && generation.metadata().source_id().as_str() == SCHWAB_STREAMER_SOURCE
            || generation.profile().as_str() == SCHWAB_MARKET_HOURS_PROFILE
                && generation.metadata().source_id().as_str() == SCHWAB_MARKET_HOURS_SOURCE
                && generation.metadata().coverage().domain()
                    == market_squawk_sources::CoverageDomain::MarketCalendar)
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

    /// Derives the sole quote sink authority from one exactly scoped research generation.
    ///
    /// Quote production is intentionally unavailable for source-wide or multi-subject rights.
    /// The activation owner must bind this runtime generation to exactly one provider capture
    /// dataset. Canonical observations publish separately into the one explicit neutral market
    /// event dataset; provider capture identity never names the analytical dataset.
    pub(crate) fn bind_rest_quote_sink(
        self: &Arc<Self>,
        operation_timeout: Duration,
        analytical_dataset: DatasetId,
    ) -> Result<Arc<SchwabRestQuoteGenerationAuthority>, SchwabMarketPublicationError> {
        let subjects = self
            .generation
            .rights_exact_subjects()
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?;
        if self.generation.profile().as_str()
            != market_squawk_sources::SCHWAB_MARKET_DATA_SURFACE_ID
            || subjects.len() != 1
            || operation_timeout.is_zero()
            || analytical_dataset.as_str() != SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let provider_dataset = subjects
            .iter()
            .next()
            .cloned()
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?;
        self.rights
            .validate_subject(Some(&provider_dataset))
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let session_id = self.generation.session_id();
        let session_identifier = SourceIdentifier::try_from(session_id.to_string().as_str())
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let coordinates = SchwabCaptureCoordinates::try_new(
            self.generation.metadata().source_id().clone(),
            self.generation.metadata().revision().clone(),
            provider_dataset,
            session_id,
        )?;
        Ok(Arc::new(SchwabRestQuoteGenerationAuthority {
            closure: Arc::clone(self),
            coordinates,
            session_identifier,
            analytical_dataset,
            operation_timeout,
            latest_source_health: Mutex::new(None),
        }))
    }

    /// Publishes one quote response that has already crossed the sole physical raw sealer.
    ///
    /// This is the generation-bound continuation used by the REST runtime sink. It deliberately
    /// acquires the revocable publication lease after physical sealing, so expiry or shutdown can
    /// prevent analytical publication without losing the recoverable sealed provider response.
    pub(crate) async fn publish_already_sealed_rest_quotes(
        &self,
        publication: Box<SchwabSealedRestQuotePublication>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: Option<crate::provider_activation::ProviderAccountPublicationAuthority>,
        selection: SchwabQuotePublicationSelection,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        deadline: Instant,
    ) -> Result<SchwabRestQuotePublicationReceipt, SchwabMarketPublicationError> {
        let oauth = oauth_epoch.receipt();
        oauth_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        if idempotency_key.is_empty() {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        #[cfg(not(test))]
        if account.is_none() {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        if let Some(account) = &account {
            account
                .require_current()
                .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        }
        if Instant::now() >= deadline {
            return Err(SchwabMarketPublicationError::Deadline);
        }
        publication.binding().validate()?;
        self.validate_response_binding(publication.binding())?;
        self.rights
            .validate_subject(Some(publication.binding().capture_evidence().dataset()))
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        if publication.binding().native_lineage().implementation()
            != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        self.validate_doctor_family(SchwabMarketDataFamily::Quotes, observed_at)?;
        self.validate_doctor_oauth(oauth, observed_at)?;

        let publication_cancellation = CancellationToken::new();
        let mut lease = tokio::select! {
            biased;
            () = self.admission.cancellation().cancelled() => {
                return Err(SchwabMarketPublicationError::AuthorityRevoked);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(SchwabMarketPublicationError::Deadline);
            }
            lease = self.acquire_attempt_publication_lease(
                oauth_epoch,
                observed_at,
                &publication_cancellation,
            ) => lease?,
        };

        selection
            .validate_at(observed_at)
            .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        Arc::get_mut(&mut lease)
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?
            .account = account;
        Arc::get_mut(&mut lease)
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?
            .selected = Some(selection);
        let dispositions = publication.dispositions().to_vec().into_boxed_slice();
        let binding = publication.into_binding();
        let publish_cancellation = publication_cancellation.clone();
        let publication = self.publish_market_events(
            binding.into(),
            ProviderMarketEventPublicationKind::ResponseMarketEvent,
            analytical_dataset,
            idempotency_key,
            observed_at,
            lease,
            publish_cancellation,
        );
        tokio::pin!(publication);
        let generation = tokio::select! {
            biased;
            result = &mut publication => result?,
            () = self.admission.cancellation().cancelled() => {
                publication_cancellation.cancel();
                return Err(SchwabMarketPublicationError::AuthorityRevoked);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                publication_cancellation.cancel();
                return Err(SchwabMarketPublicationError::Deadline);
            }
        };
        Ok(SchwabRestQuotePublicationReceipt {
            generation,
            dispositions,
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

    /// Publishes an original already-sealed Streamer batch under the retained account/OAuth epoch.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, publication, and authority coordinates remain exact"
    )]
    pub(crate) async fn publish_already_sealed_streamer_quotes(
        &self,
        sealed: SchwabSealedStreamerCapture,
        request: SchwabStreamerQuotePublicationRequest<'_>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: crate::provider_activation::ProviderAccountPublicationAuthority,
        references: crate::provider_activation::SchwabQuoteReferencePrecommit,
        selection: SchwabQuotePublicationSelection,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        idempotency_key: impl Into<String>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabStreamerApplicationOutcome, SchwabMarketPublicationError> {
        let oauth = oauth_epoch.receipt();
        let receipt = sealed.streamer_receipt();
        self.validate_coordinates(sealed.coordinates())?;
        if sealed.coordinates().connection_id() != self.generation.session_id() {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        self.rights
            .validate_subject(Some(sealed.coordinates().dataset()))
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        validate_current_doctor(&self.generation, &self.doctor, observed_at)?;
        request.validate_current_authority(&self.doctor, oauth, observed_at)?;
        self.validate_doctor_oauth(oauth, observed_at)?;
        if receipt.token_generation() != oauth.generation()
            || timestamp_from_unix_millis(receipt.last_received_at_unix_millis())? > observed_at
            || cancellation.is_cancelled()
            || Instant::now() >= deadline
        {
            return Err(SchwabMarketPublicationError::AuthorityRevoked);
        }
        let connection_generation = receipt.generation();
        let token_generation = receipt.token_generation();
        let frame_count = receipt.frame_count();
        let stream_identity = sealed.stream_identity().clone();
        account
            .require_current()
            .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        let mut lease = self
            .acquire_attempt_publication_lease(oauth_epoch, observed_at, &cancellation)
            .await?;
        Arc::get_mut(&mut lease)
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?
            .account = Some(account);
        Arc::get_mut(&mut lease)
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?
            .references = Some(references);
        selection
            .validate_at(observed_at)
            .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        Arc::get_mut(&mut lease)
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?
            .selected = Some(selection.clone());
        match sealed.into_level_one_quote_publication(request)? {
            SchwabStreamerQuotePublicationOutcome::SealedRaw(raw) => {
                Ok(SchwabStreamerApplicationOutcome::SealedRaw(raw))
            }
            SchwabStreamerQuotePublicationOutcome::Published(publication) => {
                let identities = selection
                    .for_events(publication.binding().batch().events(), observed_at)
                    .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
                let publication = publication.with_provider_identities(identities)?;
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
        let generation = self
            .acquire_publication_generation(oauth, observed_at, cancellation)
            .await?;
        Ok(Arc::new(SchwabMarketPublicationLease {
            generation: Arc::new(generation),
            generation_digest: self.generation.generation_digest()?,
            oauth,
            oauth_epoch: None,
            account: None,
            references: None,
            selected: None,
            admission: Arc::clone(&self.admission),
            exclusive_expires_at: exact_exclusive_expiry(&self.generation, &self.doctor, oauth)?,
        }))
    }

    async fn acquire_attempt_publication_lease(
        &self,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<Arc<SchwabMarketPublicationLease>, SchwabMarketPublicationError> {
        let oauth = oauth_epoch.receipt();
        oauth_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        let generation = self
            .acquire_publication_generation(oauth, observed_at, cancellation)
            .await?;
        oauth_epoch
            .validate_current(oauth)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        Ok(Arc::new(SchwabMarketPublicationLease {
            generation: Arc::new(generation),
            generation_digest: self.generation.generation_digest()?,
            oauth,
            oauth_epoch: Some(oauth_epoch),
            account: None,
            references: None,
            selected: None,
            admission: Arc::clone(&self.admission),
            exclusive_expires_at: exact_exclusive_expiry(&self.generation, &self.doctor, oauth)?,
        }))
    }

    async fn acquire_publication_generation(
        &self,
        oauth: SchwabOAuthAuthorityReceipt,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<ResearchProviderPublicationLease, SchwabMarketPublicationError> {
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
        Ok(generation)
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
        if matches!(
            kind,
            ProviderMarketEventPublicationKind::ResponseMarketEvent
                | ProviderMarketEventPublicationKind::EventMicrobatch
        ) && lease.oauth_epoch.is_none()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
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
                commit: committed.clone(),
                publication_digest,
                publication_kind: kind,
                source_id: self.generation.metadata().source_id().clone(),
                expected_event_count: event_count,
            },
            commit: committed,
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
        binding
            .batch()
            .validate_source_metadata(self.generation.metadata())?;
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )
    }

    fn validate_event_binding(
        &self,
        binding: &SealedProviderEventMicrobatchBinding,
    ) -> Result<(), SchwabMarketPublicationError> {
        binding
            .batch()
            .validate_source_metadata(self.generation.metadata())?;
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
    oauth_epoch: Option<SchwabOAuthPublicationEpoch>,
    account: Option<crate::provider_activation::ProviderAccountPublicationAuthority>,
    references: Option<crate::provider_activation::SchwabQuoteReferencePrecommit>,
    selected: Option<SchwabQuotePublicationSelection>,
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
        if let Some(account) = &self.account {
            account
                .require_current()
                .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        }
        if let Some(selected) = &self.selected {
            selected
                .validate_at(trusted_now()?)
                .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        }
        self.generation
            .validate_precommit()
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        self.admission
            .ensure_live()
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        self.admission
            .validate_oauth_current(self.oauth)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        if let Some(oauth_epoch) = self.oauth_epoch.as_ref() {
            oauth_epoch
                .validate_current(self.oauth)
                .map_err(|_error| SchwabMarketPublicationError::AuthorityRevoked)?;
        }
        if trusted_now()? >= self.exclusive_expires_at {
            return Err(SchwabMarketPublicationError::AuthorityExpired);
        }
        Ok(())
    }
}

impl IngestPrecommitAuthority for SchwabMarketPublicationLease {
    fn validate_catalog_precommit(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), IngestError> {
        self.validate_precommit()?;
        self.generation.validate_catalog_precommit(catalog)?;
        if let Some(references) = &self.references {
            references.validate_catalog(catalog)?;
        }
        if let Some(account) = &self.account {
            account
                .require_catalog_current(catalog)
                .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.validate_precommit_exact()
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)
    }
}

/// Exact generation-owned selector for one durable Schwab quote or Streamer publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SchwabMarketEventRestartSelector {
    commit: MarketEventCommitRef,
    publication_digest: EvidenceDigest,
    publication_kind: ProviderMarketEventPublicationKind,
    source_id: SourceId,
    expected_event_count: usize,
}

impl SchwabMarketEventRestartSelector {
    pub(crate) const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) const fn publication_kind(&self) -> ProviderMarketEventPublicationKind {
        self.publication_kind
    }

    /// Reopens the exact kind-qualified raw evidence and committed rows after restart.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketEventRestartReceipt, SchwabMarketPublicationError> {
        if !research
            .analytical()
            .has_provider_market_event_publication(
                &self.commit,
                self.publication_digest,
                self.publication_kind,
            )?
        {
            return Err(SchwabMarketPublicationError::RestartInvalid);
        }
        let selector = ProviderMarketEventPublicationSelector::new(
            self.publication_digest,
            self.publication_kind,
        );
        let store = research.provider_capture_store();
        let evidence = research
            .analytical()
            .provider_market_event_publication_evidence(&self.commit, selector, store.as_ref())?;
        validate_restart_evidence(self, &evidence)?;
        let events = research
            .analytical()
            .read_provider_market_event_publication(
                &self.commit,
                selector,
                store,
                deadline,
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
    commit: MarketEventCommitRef,
    restart: SchwabMarketEventRestartSelector,
    sealed_receipt_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    event_count: usize,
    runtime_generation_digest: EvidenceDigest,
    oauth_generation: AccessTokenGeneration,
}

impl SchwabMarketEventPublicationReceipt {
    pub(crate) const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
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
        let publication_kind = match self.publication_kind {
            OptionMarketBatchKind::Snapshots => "option_snapshots",
            OptionMarketBatchKind::Expirations => "option_expirations",
        };
        if !research.analytical().has_provider_publication(
            &self.manifest,
            self.publication_digest,
            publication_kind,
        )? {
            return Err(SchwabMarketPublicationError::RestartInvalid);
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
    if !oauth.matches_market_data_authorization(doctor)
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
    #[error("Schwab market-data post-seal publication deadline elapsed")]
    Deadline,
    #[error("Schwab quote source-health outcome could not be retained")]
    SourceHealthUnavailable,
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
    MarketHours(#[from] market_squawk_adapter_schwab::SchwabMarketHoursPublicationError),
    #[error(transparent)]
    Option(#[from] SchwabRestOptionPublicationError),
    #[error(transparent)]
    Streamer(#[from] SchwabStreamerPublicationError),
}

/// Exact independent calendar family; this never relabels the quote profile or venue topology.
pub(crate) const SCHWAB_MARKET_HOURS_PROFILE: &str = "schwab.trader-api-market-data.market-hours";
pub(crate) const SCHWAB_MARKET_HOURS_SOURCE: &str = "schwab-trader-api-market-hours";
pub(crate) const SCHWAB_MARKET_HOURS_DATASET: &str = "schwab.market-hours";

/// The registered calendar generation owns both raw sealing and the sole canonical publisher.
#[derive(Debug)]
pub(crate) struct SchwabMarketHoursGenerationAuthority {
    closure: Arc<SchwabMarketPublicationClosure>,
    coordinates: SchwabCaptureCoordinates,
    analytical_dataset: DatasetId,
}
impl SchwabMarketHoursGenerationAuthority {
    pub(super) fn try_new(
        closure: Arc<SchwabMarketPublicationClosure>,
    ) -> Result<Self, SchwabMarketPublicationError> {
        if closure.generation.profile().as_str() != SCHWAB_MARKET_HOURS_PROFILE
            || closure.generation.metadata().source_id().as_str() != SCHWAB_MARKET_HOURS_SOURCE
            || closure.generation.metadata().coverage().domain()
                != market_squawk_sources::CoverageDomain::MarketCalendar
        {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        let dataset = SourceIdentifier::try_from(SCHWAB_MARKET_HOURS_DATASET)
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        if !closure
            .generation
            .rights_exact_subjects()
            .is_some_and(|subjects| subjects.len() == 1 && subjects.contains(&dataset))
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        closure
            .rights
            .validate_subject(Some(&dataset))
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        let coordinates = SchwabCaptureCoordinates::try_new(
            closure.generation.metadata().source_id().clone(),
            closure.generation.metadata().revision().clone(),
            dataset,
            closure.generation.session_id(),
        )?;
        let analytical_dataset = DatasetId::try_from(SCHWAB_MARKET_HOURS_DATASET)
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        Ok(Self {
            closure,
            coordinates,
            analytical_dataset,
        })
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        self.closure.admission.cancellation()
    }

    /// Completed accepted, rejected and malformed responses all enter the same physical store.
    /// Cancellation or stale authority cannot erase an already completed native response.
    pub(crate) async fn seal_outcome(
        &self,
        outcome: market_squawk_adapter_schwab::RestExecutionOutcome,
        deadline: Instant,
    ) -> Result<Option<SchwabSealedRestResponse>, SchwabMarketPublicationError> {
        use market_squawk_adapter_schwab::RestExecutionOutcome;
        let cancellation = CancellationToken::new();
        match outcome {
            RestExecutionOutcome::Accepted(response) => self
                .closure
                .seal_rest_response(
                    response,
                    self.coordinates.clone(),
                    Uuid::new_v4(),
                    &cancellation,
                    deadline,
                )
                .await
                .map(Some),
            RestExecutionOutcome::ProviderRejected(capture)
            | RestExecutionOutcome::InvalidPayload { capture, .. } => {
                let pending =
                    capture.into_pending_capture(self.coordinates.clone(), Uuid::new_v4())?;
                let (rejoin, request) = pending.into_sealing_parts();
                let sealed = self
                    .closure
                    .research
                    .seal_provider_capture(request, &cancellation, deadline)
                    .await?;
                let _retained = rejoin.try_rejoin(sealed)?;
                Ok(None)
            }
            _ => Err(SchwabMarketPublicationError::FamilyMismatch),
        }
    }

    pub(crate) async fn publish(
        &self,
        sealed: SchwabSealedRestResponse,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: crate::provider_activation::ProviderAccountPublicationAuthority,
        max_canonical_bytes: std::num::NonZeroU64,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabMarketHoursPublicationReceipt, SchwabMarketPublicationError> {
        self.closure
            .publish_already_sealed_market_hours(
                sealed,
                max_canonical_bytes,
                oauth_epoch,
                account,
                self.analytical_dataset.clone(),
                cancellation,
                deadline,
            )
            .await
    }
}

/// Both original account mutation ownership and OAuth/generation proof survive supervised ingest.
#[derive(Debug)]
struct SchwabCalendarPrecommit {
    lease: Arc<SchwabMarketPublicationLease>,
    account: crate::provider_activation::ProviderAccountPublicationAuthority,
}
impl IngestPrecommitAuthority for SchwabCalendarPrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.lease.validate_precommit()?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
    fn validate_catalog_precommit(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), IngestError> {
        self.lease.validate_catalog_precommit(catalog)?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
}

pub(super) const SCHWAB_STREAMER_PROFILE: &str = "schwab.trader-api-market-data.streamer";
pub(super) const SCHWAB_STREAMER_SOURCE: &str = "schwab-streamer-market-data";
const SCHWAB_STREAMER_DATASET: &str = "schwab.streamer.market-data";

/// Actual source generation for the sole current Streamer publisher. No constructor accepts
/// caller-authored currentness or a reconstructed archival proof.
#[derive(Debug)]
pub(crate) struct SchwabStreamerGenerationAuthority {
    closure: Arc<SchwabMarketPublicationClosure>,
    coordinates: SchwabCaptureCoordinates,
}
impl SchwabStreamerGenerationAuthority {
    pub(super) fn try_new(
        closure: Arc<SchwabMarketPublicationClosure>,
    ) -> Result<Self, SchwabMarketPublicationError> {
        let subjects = closure
            .generation
            .rights_exact_subjects()
            .ok_or(SchwabMarketPublicationError::AuthorityInvalid)?;
        if closure.generation.profile().as_str() != SCHWAB_STREAMER_PROFILE
            || closure.generation.metadata().source_id().as_str() != SCHWAB_STREAMER_SOURCE
            || subjects.len() != 1
            || subjects
                .iter()
                .next()
                .is_none_or(|subject| subject.as_str() != SCHWAB_STREAMER_DATASET)
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let coordinates = SchwabCaptureCoordinates::try_new(
            closure.generation.metadata().source_id().clone(),
            closure.generation.metadata().revision().clone(),
            SourceIdentifier::try_from(SCHWAB_STREAMER_DATASET)
                .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?,
            closure.generation.session_id(),
        )?;
        Ok(Self {
            closure,
            coordinates,
        })
    }
    pub(crate) fn metadata(&self) -> &SourceMetadata {
        self.closure.generation.metadata()
    }
    pub(crate) fn coordinates(&self) -> SchwabCaptureCoordinates {
        self.coordinates.clone()
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        self.closure.admission.cancellation()
    }
    pub(crate) async fn seal_microbatch(
        &self,
        microbatch: StreamerMicrobatch,
        parse: ParseBounds,
    ) -> Result<SchwabSealedStreamerCapture, SchwabMarketPublicationError> {
        // The native owner bounds every microbatch. Completed frames drain even after cancellation.
        let event_ids = microbatch.frames().iter().map(|_| Uuid::new_v4()).collect();
        let (pending, request) = microbatch.into_pending_capture(event_ids, parse)?;
        let cleanup = CancellationToken::new();
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(20))
            .ok_or(SchwabMarketPublicationError::Deadline)?;
        let physical = self
            .closure
            .research
            .seal_provider_capture(request, &cleanup, deadline)
            .await?;
        let sealed = pending.try_rejoin(physical)?;
        if sealed.coordinates() != &self.coordinates {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        Ok(sealed)
    }
    #[allow(
        clippy::too_many_arguments,
        reason = "original capture, account, OAuth and publication clocks remain explicit"
    )]
    pub(crate) async fn publish(
        &self,
        sealed: SchwabSealedStreamerCapture,
        request: SchwabStreamerQuotePublicationRequest<'_>,
        epoch: SchwabOAuthPublicationEpoch,
        account: crate::provider_activation::ProviderAccountPublicationAuthority,
        references: crate::provider_activation::SchwabQuoteReferencePrecommit,
        selection: SchwabQuotePublicationSelection,
        observed_at: Timestamp,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabStreamerApplicationOutcome, SchwabMarketPublicationError> {
        if sealed.coordinates() != &self.coordinates {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let key = format!(
            "schwab-streamer-{}-{}-{}",
            sealed.streamer_receipt().generation().get(),
            sealed.streamer_receipt().first_ordinal(),
            sealed.streamer_receipt().last_ordinal()
        );
        self.closure
            .publish_already_sealed_streamer_quotes(
                sealed,
                request,
                epoch,
                account,
                references,
                selection,
                observed_at,
                DatasetId::try_from(SCHWAB_MARKET_EVENT_ANALYTICAL_DATASET)
                    .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?,
                key,
                cancellation,
                deadline,
            )
            .await
    }
    pub(crate) fn begin_revocation(&self) {
        self.closure.begin_revocation();
    }
    pub(crate) async fn finish_revocation_drain(&self) -> Result<(), SchwabMarketPublicationError> {
        self.closure
            .finish_revocation_drain(&CancellationToken::new())
            .await
    }
}
