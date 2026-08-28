//! Application-owned sealing and immutable publication for explicit-demand Yahoo enrichment.
//!
//! This leaf performs no network request and owns no background runtime. It consumes exactly one
//! adapter-produced network response, seals the raw body through the application store, and then
//! routes only quote, history, or option families into their closed shared publication paths.
//! Reference, fund, and lookup responses remain typed provider hints and never create canonical
//! identity.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_yahoo::{
    YAHOO_SOURCE_ID, YahooEnrichment, YahooFundData, YahooHistoricalPublicationRequest,
    YahooLookupHint, YahooOptionAbstention, YahooOptionPublicationOutcome,
    YahooOptionPublicationRequest, YahooParsedResponse, YahooPendingPublication,
    YahooPublicationBridgeError, YahooQuoteAbstention, YahooQuotePublicationOutcome,
    YahooQuotePublicationRequest, YahooRawReceipt, YahooReference, YahooReturnedDisposition,
    YahooSealedPublication, YahooSealedPublicationFamily,
};
use market_squawk_data::{
    AnalyticalMarketBarOutput, AnalyticalMarketBarReadRequest, AnalyticalReadError,
    CommittedDataset, DatasetId, DatasetManifestRef, IngestError, IngestIdentity,
    IngestPrecommitAuthority, OptionMarketPointInTimeRequest, OptionMarketPointInTimeSelection,
    PersistedProviderCaptureBindingEvidence, PersistedProviderOptionMarketBindingEvidence,
    PersistedProviderPublicationEvidence, ProviderMarketEventArrowBatch,
    ProviderMarketEventPublicationKind, ProviderOptionMarketArrowBatch, QueryLimits, RightsError,
    SourceOperation, extraction_provider_payload_digest, provider_market_event_publication_digest,
    provider_option_market_publication_digest,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    ExtractionRevisionPlan, OptionMarketBatchKind, ProviderCaptureError,
    ProviderNativeLineageImplementation, SealedProviderPublicationBinding, SourceMetadata,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::ResearchRightsAuthority;
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

/// One closed application request paired with the authority required by its publication family.
///
/// Canonical publication always carries a caller-supplied precommit authority. Hint requests carry
/// no such authority because they do not enter an immutable canonical generation.
#[derive(Debug)]
pub(crate) enum YahooEnrichmentPublicationRequest {
    Historical {
        canonical: YahooHistoricalPublicationRequest,
        analytical_dataset: DatasetId,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    },
    Quotes {
        canonical: YahooQuotePublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    },
    Options {
        canonical: YahooOptionPublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
    },
    ProviderHint,
}

impl YahooEnrichmentPublicationRequest {
    fn precommit_authority(&self) -> Option<&dyn IngestPrecommitAuthority> {
        match self {
            Self::Historical {
                precommit_authority,
                ..
            }
            | Self::Quotes {
                precommit_authority,
                ..
            }
            | Self::Options {
                precommit_authority,
                ..
            } => Some(precommit_authority.as_ref()),
            Self::ProviderHint => None,
        }
    }
}

/// One application-owned Yahoo sealing and publication closure.
pub(crate) struct YahooEnrichmentPublicationClosure {
    research: Arc<ResearchService>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
}

impl std::fmt::Debug for YahooEnrichmentPublicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("YahooEnrichmentPublicationClosure")
            .field("source_id", self.source.source_id())
            .field("metadata_revision", self.source.revision())
            .field("source_registered_at", &self.source_registered_at)
            .finish_non_exhaustive()
    }
}

impl YahooEnrichmentPublicationClosure {
    /// Binds one registered Yahoo source and its exact persistence rights to the sole application
    /// sealer and analytical writer.
    pub(crate) fn try_new(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, YahooEnrichmentPublicationError> {
        if source.source_id().as_str() != YAHOO_SOURCE_ID
            || source.source_id() != rights.source_id()
            || !source.is_effective_at(source_registered_at)
        {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        Ok(Self {
            research,
            source,
            rights,
            source_registered_at,
        })
    }

    /// Seals and consumes one exact Yahoo network response into its selected family.
    ///
    /// The injected precommit authority is checked before physical sealing and retained through
    /// the final catalog/Parquet commit. The response is never refetched or remapped through a
    /// generic provider API.
    pub(crate) async fn seal_and_publish(
        &self,
        pending: YahooPendingPublication,
        request: YahooEnrichmentPublicationRequest,
        observed_at: Timestamp,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<YahooEnrichmentApplicationOutcome, YahooEnrichmentPublicationError> {
        self.validate_current_authority(observed_at)?;
        if let Some(precommit) = request.precommit_authority() {
            precommit.validate_precommit()?;
        }
        let (rejoin, seal_request) = pending.into_sealing_parts();
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, deadline)
            .await?;
        let sealed = rejoin.try_rejoin(sealed)?;
        self.validate_sealed_response(&sealed, observed_at)?;
        let family = sealed.family();

        match (request, family) {
            (
                YahooEnrichmentPublicationRequest::Historical {
                    canonical,
                    analytical_dataset,
                    precommit_authority,
                },
                YahooSealedPublicationFamily::HistoricalBars,
            ) => self
                .publish_history(
                    sealed,
                    canonical,
                    analytical_dataset,
                    observed_at,
                    precommit_authority,
                    cancellation,
                )
                .await
                .map(YahooEnrichmentApplicationOutcome::Historical),
            (
                YahooEnrichmentPublicationRequest::Quotes {
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                    precommit_authority,
                },
                YahooSealedPublicationFamily::CurrentQuotes,
            ) => self
                .publish_quotes(
                    sealed,
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                    observed_at,
                    precommit_authority,
                    cancellation,
                )
                .await
                .map(YahooEnrichmentApplicationOutcome::Quotes),
            (
                YahooEnrichmentPublicationRequest::Options {
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                    precommit_authority,
                },
                YahooSealedPublicationFamily::Options,
            ) => self
                .publish_options(
                    sealed,
                    canonical,
                    analytical_dataset,
                    idempotency_key,
                    observed_at,
                    precommit_authority,
                    cancellation,
                )
                .await
                .map(YahooEnrichmentApplicationOutcome::Options),
            (YahooEnrichmentPublicationRequest::ProviderHint, family)
                if matches!(
                    family,
                    YahooSealedPublicationFamily::ReferenceHint
                        | YahooSealedPublicationFamily::FundHint
                        | YahooSealedPublicationFamily::LookupHint
                ) =>
            {
                YahooSealedProviderHint::try_from_sealed(sealed)
                    .map(YahooEnrichmentApplicationOutcome::ProviderHint)
            }
            _ => Err(YahooEnrichmentPublicationError::FamilyMismatch),
        }
    }

    /// Executes the common whole-batch option PIT selector without adding Yahoo coordinates to
    /// the provider-neutral request or result.
    pub(crate) async fn read_provider_neutral_option_point_in_time(
        &self,
        request: &OptionMarketPointInTimeRequest,
        cancellation: CancellationToken,
    ) -> Result<Option<OptionMarketPointInTimeSelection>, YahooEnrichmentPublicationError> {
        let store = self.research.provider_capture_store();
        self.research
            .analytical()
            .read_provider_option_market_point_in_time(request, store.as_ref(), cancellation)
            .await
            .map_err(Into::into)
    }

    async fn publish_history(
        &self,
        sealed: YahooSealedPublication,
        canonical: YahooHistoricalPublicationRequest,
        analytical_dataset: DatasetId,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<YahooHistoricalPublicationReceipt, YahooEnrichmentPublicationError> {
        let publication = sealed.into_historical_publication(canonical)?;
        let (revisions, binding) = publication.into_parts();
        binding.validate()?;
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )?;
        if binding.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::YahooEnrichmentV1
            || revisions.len() != binding.batch().records().len()
            || !revisions.is_locally_observed()
            || !revisions.native_lineage_required()
        {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let provider_dataset = binding.capture_evidence().dataset().clone();
        let expected_record_count = binding.batch().records().len();
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = self.rights.decision(payload_digest, observed_at)?;
        let ingest = ResearchIngestRequest::with_provider_publication(
            self.source.clone(),
            rights,
            analytical_dataset,
            binding,
            revisions,
        )?
        .with_precommit_authority(precommit_authority);
        let committed = self.research.ingest(ingest, cancellation).await?;
        Ok(YahooHistoricalPublicationReceipt {
            restart: YahooHistoricalRestartSelector {
                manifest: committed.manifest().clone(),
                binding_digest,
                source_id: self.source.source_id().clone(),
                expected_record_count,
            },
            committed,
            binding_digest,
            provider_dataset,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical rows, immutable dataset, idempotency, clocks, and authority stay explicit"
    )]
    async fn publish_quotes(
        &self,
        sealed: YahooSealedPublication,
        canonical: YahooQuotePublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<YahooQuoteApplicationOutcome, YahooEnrichmentPublicationError> {
        match sealed.into_quote_publication(canonical)? {
            YahooQuotePublicationOutcome::SealedRaw {
                response,
                abstentions,
            } => Ok(YahooQuoteApplicationOutcome::SealedRaw {
                response,
                abstentions,
            }),
            YahooQuotePublicationOutcome::Published(publication) => {
                publication.binding().validate()?;
                self.validate_capture_binding(
                    publication.binding().capture_evidence().source_id(),
                    publication.binding().capture_evidence().metadata_revision(),
                )?;
                if publication.binding().native_lineage().implementation()
                    != ProviderNativeLineageImplementation::YahooEnrichmentV1
                {
                    return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
                }
                let abstentions = publication.abstentions().to_vec().into_boxed_slice();
                let binding = SealedProviderPublicationBinding::ResponseMarketEvent(
                    publication.into_binding(),
                );
                let prepared = self
                    .publish_market_events(
                        binding,
                        analytical_dataset,
                        idempotency_key,
                        observed_at,
                        precommit_authority,
                        cancellation,
                    )
                    .await?;
                Ok(YahooQuoteApplicationOutcome::Published(
                    YahooQuotePublicationReceipt {
                        committed: prepared.committed,
                        restart: prepared.restart,
                        sealed_receipt_digest: prepared.sealed_receipt_digest,
                        provider_dataset: prepared.provider_dataset,
                        event_count: prepared.event_count,
                        abstentions,
                    },
                ))
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "canonical rows, immutable dataset, idempotency, clocks, and authority stay explicit"
    )]
    async fn publish_options(
        &self,
        sealed: YahooSealedPublication,
        canonical: YahooOptionPublicationRequest,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<YahooOptionApplicationOutcome, YahooEnrichmentPublicationError> {
        match sealed.into_option_publication(canonical)? {
            YahooOptionPublicationOutcome::SealedRaw {
                response,
                abstentions,
            } => Ok(YahooOptionApplicationOutcome::SealedRaw {
                response,
                abstentions,
            }),
            YahooOptionPublicationOutcome::Published(publication) => {
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
                    != ProviderNativeLineageImplementation::YahooEnrichmentV1
                {
                    return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
                }
                let abstentions = publication.abstentions().to_vec().into_boxed_slice();
                let (revision_plan, binding) = publication.into_parts();
                if revision_plan.len() != binding.batch().row_count()
                    || !revision_plan.is_locally_observed()
                    || !revision_plan.native_lineage_required()
                {
                    return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
                }
                let binding_digest = binding.evidence_digest().evidence();
                let publication_digest = provider_option_market_publication_digest(&binding)?;
                if publication_digest != binding_digest {
                    return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
                }
                let publication_kind = binding.batch().kind();
                let provider_dataset = binding.batch().scope().dataset().clone();
                let expected_option_row_count = binding.batch().row_count();
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
                    .ingest_provider_option_market(
                        reservation,
                        analytical_dataset,
                        binding,
                        cancellation,
                        precommit_authority,
                    )
                    .await?;
                Ok(YahooOptionApplicationOutcome::Published(
                    YahooOptionPublicationReceipt {
                        restart: YahooOptionRestartSelector {
                            manifest: committed.manifest().clone(),
                            publication_digest,
                            publication_kind,
                            source_id: self.source.source_id().clone(),
                            expected_option_row_count,
                        },
                        committed,
                        binding_digest,
                        provider_dataset,
                        revision_plan,
                        abstentions,
                    },
                ))
            }
        }
    }

    async fn publish_market_events(
        &self,
        binding: SealedProviderPublicationBinding,
        analytical_dataset: DatasetId,
        idempotency_key: String,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<YahooPreparedMarketEventPublication, YahooEnrichmentPublicationError> {
        let publication_digest = provider_market_event_publication_digest(&binding)?;
        if publication_digest.algorithm() != DigestAlgorithm::Sha256
            || publication_digest.bytes() == [0; 32]
        {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        let (sealed_receipt_digest, provider_dataset, event_count) = match &binding {
            SealedProviderPublicationBinding::ResponseMarketEvent(binding) => (
                binding.sealed_receipt_digest(),
                binding.capture_evidence().dataset().clone(),
                binding.record_count(),
            ),
            _ => return Err(YahooEnrichmentPublicationError::FamilyMismatch),
        };
        if event_count == 0 || sealed_receipt_digest.bytes() == [0; 32] {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
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
        Ok(YahooPreparedMarketEventPublication {
            restart: YahooMarketEventRestartSelector {
                manifest: committed.manifest().clone(),
                publication_digest,
                source_id: self.source.source_id().clone(),
                expected_event_count: event_count,
            },
            committed,
            sealed_receipt_digest,
            provider_dataset,
            event_count,
        })
    }

    async fn reserve_publication(
        &self,
        payload_digest: EvidenceDigest,
        idempotency_key: String,
        observed_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::IngestReservation, YahooEnrichmentPublicationError> {
        let identity = IngestIdentity::try_new(
            self.source.source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            idempotency_key,
        )?;
        let rights = self.rights.decision(payload_digest, observed_at)?;
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
    ) -> Result<(), YahooEnrichmentPublicationError> {
        if observed_at < self.source_registered_at || !self.source.is_effective_at(observed_at) {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        Ok(())
    }

    fn validate_sealed_response(
        &self,
        sealed: &YahooSealedPublication,
        observed_at: Timestamp,
    ) -> Result<(), YahooEnrichmentPublicationError> {
        self.validate_capture_binding(
            sealed.publication_binding().source_id(),
            sealed.publication_binding().metadata_revision(),
        )?;
        let available_at = timestamp_from_unix_millis(sealed.raw_receipt().available_at_unix_ms)?;
        if available_at > observed_at {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        Ok(())
    }

    fn validate_capture_binding(
        &self,
        source_id: &SourceId,
        metadata_revision: &market_squawk_domain::MetadataRevision,
    ) -> Result<(), YahooEnrichmentPublicationError> {
        if source_id != self.source.source_id() || metadata_revision != self.source.revision() {
            return Err(YahooEnrichmentPublicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct YahooPreparedMarketEventPublication {
    committed: CommittedDataset,
    restart: YahooMarketEventRestartSelector,
    sealed_receipt_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    event_count: usize,
}

/// Exact family result for one consumed Yahoo network response.
#[derive(Debug)]
pub(crate) enum YahooEnrichmentApplicationOutcome {
    Historical(YahooHistoricalPublicationReceipt),
    Quotes(YahooQuoteApplicationOutcome),
    Options(YahooOptionApplicationOutcome),
    ProviderHint(YahooSealedProviderHint),
}

/// Typed sealed hints that remain provider evidence rather than canonical identity.
#[derive(Debug)]
pub(crate) enum YahooSealedProviderHint {
    Reference(YahooSealedPublication),
    Fund(YahooSealedPublication),
    Search(YahooSealedPublication),
}

impl YahooSealedProviderHint {
    fn try_from_sealed(
        sealed: YahooSealedPublication,
    ) -> Result<Self, YahooEnrichmentPublicationError> {
        let family = sealed.family();
        let parsed_matches = matches!(
            (family, sealed.parsed_response()),
            (
                YahooSealedPublicationFamily::ReferenceHint,
                YahooParsedResponse::Reference(_)
            ) | (
                YahooSealedPublicationFamily::FundHint,
                YahooParsedResponse::Fund(_)
            ) | (
                YahooSealedPublicationFamily::LookupHint,
                YahooParsedResponse::Lookup(_)
            )
        );
        if !parsed_matches {
            return Err(YahooEnrichmentPublicationError::FamilyMismatch);
        }
        Ok(match family {
            YahooSealedPublicationFamily::ReferenceHint => Self::Reference(sealed),
            YahooSealedPublicationFamily::FundHint => Self::Fund(sealed),
            YahooSealedPublicationFamily::LookupHint => Self::Search(sealed),
            _ => return Err(YahooEnrichmentPublicationError::FamilyMismatch),
        })
    }

    pub(crate) fn raw_receipt(&self) -> &YahooRawReceipt {
        self.sealed().raw_receipt()
    }

    pub(crate) fn reference(&self) -> Option<&YahooEnrichment<YahooReference>> {
        match self.sealed().parsed_response() {
            YahooParsedResponse::Reference(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn fund(&self) -> Option<&YahooEnrichment<YahooFundData>> {
        match self.sealed().parsed_response() {
            YahooParsedResponse::Fund(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn search(&self) -> Option<&YahooReturnedDisposition<YahooLookupHint>> {
        match self.sealed().parsed_response() {
            YahooParsedResponse::Lookup(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn sealed_capture_receipt(
        &self,
    ) -> &market_squawk_sources::SealedProviderCaptureSetReceipt {
        self.sealed().sealed_capture_receipt()
    }

    fn sealed(&self) -> &YahooSealedPublication {
        match self {
            Self::Reference(sealed) | Self::Fund(sealed) | Self::Search(sealed) => sealed,
        }
    }
}

fn timestamp_from_unix_millis(
    milliseconds: i64,
) -> Result<Timestamp, YahooEnrichmentPublicationError> {
    milliseconds
        .checked_mul(1_000_000)
        .map(Timestamp::from_unix_nanos)
        .ok_or(YahooEnrichmentPublicationError::AuthorityInvalid)
}

#[derive(Debug)]
pub(crate) struct YahooHistoricalPublicationReceipt {
    committed: CommittedDataset,
    restart: YahooHistoricalRestartSelector,
    binding_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
}

impl YahooHistoricalPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &YahooHistoricalRestartSelector {
        &self.restart
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
}

#[derive(Debug)]
pub(crate) enum YahooQuoteApplicationOutcome {
    Published(YahooQuotePublicationReceipt),
    SealedRaw {
        response: YahooSealedPublication,
        abstentions: Box<[YahooQuoteAbstention]>,
    },
}

#[derive(Debug)]
pub(crate) struct YahooQuotePublicationReceipt {
    committed: CommittedDataset,
    restart: YahooMarketEventRestartSelector,
    sealed_receipt_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    event_count: usize,
    abstentions: Box<[YahooQuoteAbstention]>,
}

impl YahooQuotePublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &YahooMarketEventRestartSelector {
        &self.restart
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

    pub(crate) const fn abstentions(&self) -> &[YahooQuoteAbstention] {
        &self.abstentions
    }
}

#[derive(Debug)]
pub(crate) enum YahooOptionApplicationOutcome {
    Published(YahooOptionPublicationReceipt),
    SealedRaw {
        response: YahooSealedPublication,
        abstentions: Box<[YahooOptionAbstention]>,
    },
}

#[derive(Debug)]
pub(crate) struct YahooOptionPublicationReceipt {
    committed: CommittedDataset,
    restart: YahooOptionRestartSelector,
    binding_digest: EvidenceDigest,
    provider_dataset: SourceIdentifier,
    revision_plan: ExtractionRevisionPlan,
    abstentions: Box<[YahooOptionAbstention]>,
}

impl YahooOptionPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &YahooOptionRestartSelector {
        &self.restart
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    pub(crate) const fn revision_plan(&self) -> &ExtractionRevisionPlan {
        &self.revision_plan
    }

    pub(crate) const fn abstentions(&self) -> &[YahooOptionAbstention] {
        &self.abstentions
    }
}

/// Exact immutable history generation and raw binding required for restart verification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct YahooHistoricalRestartSelector {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    expected_record_count: usize,
}

impl YahooHistoricalRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Reopens exact raw/native evidence and an exact-manifest typed bar request after restart.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        request: AnalyticalMarketBarReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<YahooHistoricalRestartReceipt, YahooEnrichmentPublicationError> {
        if request.manifest() != &self.manifest {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
        let store = research.provider_capture_store();
        let evidence = research.analytical().provider_capture_binding_evidence(
            &self.manifest,
            self.binding_digest,
            store.as_ref(),
        )?;
        if evidence.binding_digest() != self.binding_digest
            || evidence.capture().source_id() != &self.source_id
            || evidence.record_count() != self.expected_record_count
        {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
        let bars = research
            .analytical_reader()
            .read_market_bars(request, limits, deadline, cancellation)
            .await?;
        if bars.source_id() != &self.source_id || bars.bars().len() != self.expected_record_count {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
        Ok(YahooHistoricalRestartReceipt { evidence, bars })
    }
}

#[derive(Debug)]
pub(crate) struct YahooHistoricalRestartReceipt {
    evidence: PersistedProviderCaptureBindingEvidence,
    bars: AnalyticalMarketBarOutput,
}

impl YahooHistoricalRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.evidence
    }

    pub(crate) const fn bars(&self) -> &AnalyticalMarketBarOutput {
        &self.bars
    }
}

/// Exact immutable quote generation and kind-qualified response selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct YahooMarketEventRestartSelector {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    source_id: SourceId,
    expected_event_count: usize,
}

impl YahooMarketEventRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<YahooMarketEventRestartReceipt, YahooEnrichmentPublicationError> {
        let selector = research
            .analytical()
            .provider_market_event_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind()
                        == ProviderMarketEventPublicationKind::ResponseMarketEvent
            })
            .ok_or(YahooEnrichmentPublicationError::RestartInvalid)?;
        let store = research.provider_capture_store();
        let evidence = research
            .analytical()
            .provider_market_event_publication_evidence(&self.manifest, selector, store.as_ref())?;
        let PersistedProviderPublicationEvidence::ResponseMarketEvent(response) = &evidence else {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        };
        if response.binding_digest() != self.publication_digest
            || response.capture().source_id() != &self.source_id
            || response.canonical_event_count() != self.expected_event_count
        {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
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
            || events.publication_kind()
                != ProviderMarketEventPublicationKind::ResponseMarketEvent.as_str()
            || events.events().len() != self.expected_event_count
        {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
        Ok(YahooMarketEventRestartReceipt { evidence, events })
    }
}

#[derive(Debug)]
pub(crate) struct YahooMarketEventRestartReceipt {
    evidence: PersistedProviderPublicationEvidence,
    events: ProviderMarketEventArrowBatch,
}

impl YahooMarketEventRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderPublicationEvidence {
        &self.evidence
    }

    pub(crate) const fn events(&self) -> &ProviderMarketEventArrowBatch {
        &self.events
    }
}

/// Exact immutable option generation and kind-qualified response selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct YahooOptionRestartSelector {
    manifest: DatasetManifestRef,
    publication_digest: EvidenceDigest,
    publication_kind: OptionMarketBatchKind,
    source_id: SourceId,
    expected_option_row_count: usize,
}

impl YahooOptionRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        cancellation: CancellationToken,
    ) -> Result<YahooOptionRestartReceipt, YahooEnrichmentPublicationError> {
        let selector = research
            .analytical()
            .provider_option_market_publications(&self.manifest)?
            .into_iter()
            .find(|selector| {
                selector.publication_digest() == self.publication_digest
                    && selector.publication_kind() == self.publication_kind
            })
            .ok_or(YahooEnrichmentPublicationError::RestartInvalid)?;
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
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
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
        let row_count = match self.publication_kind {
            OptionMarketBatchKind::Snapshots => batch
                .snapshots()
                .map(<[_]>::len)
                .ok_or(YahooEnrichmentPublicationError::RestartInvalid)?,
            OptionMarketBatchKind::Expirations => batch
                .expirations()
                .map(<[_]>::len)
                .ok_or(YahooEnrichmentPublicationError::RestartInvalid)?,
        };
        if batch.publication_digest() != self.publication_digest
            || batch.publication_kind() != self.publication_kind
            || batch.scope().source_id() != &self.source_id
            || row_count != self.expected_option_row_count
        {
            return Err(YahooEnrichmentPublicationError::RestartInvalid);
        }
        Ok(YahooOptionRestartReceipt { evidence, batch })
    }
}

#[derive(Debug)]
pub(crate) struct YahooOptionRestartReceipt {
    evidence: PersistedProviderOptionMarketBindingEvidence,
    batch: ProviderOptionMarketArrowBatch,
}

impl YahooOptionRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderOptionMarketBindingEvidence {
        &self.evidence
    }

    pub(crate) const fn batch(&self) -> &ProviderOptionMarketArrowBatch {
        &self.batch
    }
}

#[derive(Debug, Error)]
pub(crate) enum YahooEnrichmentPublicationError {
    #[error("Yahoo enrichment publication authority is invalid or no longer current")]
    AuthorityInvalid,
    #[error("the sealed Yahoo response does not match the requested publication family")]
    FamilyMismatch,
    #[error("the exact Yahoo immutable generation failed restart verification")]
    RestartInvalid,
    #[error(transparent)]
    Bridge(#[from] YahooPublicationBridgeError),
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Rights(#[from] RightsError),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Read(#[from] AnalyticalReadError),
}
