//! MarketHours uses the same generation lease, physical capture and research publisher as history.

use super::*;
use market_squawk_adapter_schwab::{
    SchwabMarketDataQualification, SchwabMarketHoursPublicationRequest,
};
use market_squawk_sources::{CoverageDomain, DiscoveryRequest, ExtractionRequest};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

/// One actual immutable calendar generation. Consumers use its manifest through normal research
/// selectors; returned-entry coverage cannot authorize an omitted market or date as closed.
#[derive(Debug)]
pub(crate) struct SchwabMarketHoursPublicationReceipt {
    pub(crate) committed: CommittedDataset,
    pub(crate) binding_digest: EvidenceDigest,
    pub(crate) returned_products: usize,
}

impl SchwabMarketPublicationClosure {
    /// Publishes an already physically sealed MarketHours response. The governed caller seals
    /// every completed native outcome before currentness/cancellation can reject publication.
    /// Both original OAuth epoch and account authority move into supervised precommit ownership.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, capture, extraction and publication authority remain explicit"
    )]
    pub(crate) async fn publish_already_sealed_market_hours(
        &self,
        sealed: SchwabSealedRestResponse,
        max_canonical_bytes: NonZeroU64,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: crate::provider_activation::ProviderAccountPublicationAuthority,
        analytical_dataset: DatasetId,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<super::SchwabMarketHoursPublicationReceipt, SchwabMarketPublicationError> {
        let oauth = oauth_epoch.receipt();
        let observed_at = timestamp_from_unix_millis(sealed.receipt().received_at_unix_millis())?;
        self.validate_capture_binding(
            sealed.persisted_receipt().capture().source_id(),
            sealed.persisted_receipt().capture().metadata_revision(),
        )?;
        self.validate_doctor_family(SchwabMarketDataFamily::MarketHours, observed_at)?;
        self.validate_doctor_oauth(oauth, observed_at)?;
        if sealed.family() != market_squawk_adapter_schwab::SchwabRestFamily::MarketHours
            || !matches!(
                sealed.route(),
                ReadOnlyRoute::Markets | ReadOnlyRoute::SingleMarket
            )
            || sealed.receipt().token_generation() != oauth.generation()
            || sealed.receipt().credential_authority() != oauth.credential_authority()
        {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        account
            .require_current()
            .map_err(|_| SchwabMarketPublicationError::AuthorityRevoked)?;
        if self.generation.metadata().coverage().domain() != CoverageDomain::MarketCalendar {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        self.generation
            .metadata()
            .network_policy()
            .authorize(sealed.receipt().request_url())
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketPublicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SchwabMarketPublicationError::Deadline);
        }
        let qualification = SchwabMarketDataQualification::try_from_doctor_receipt(
            &self.doctor,
            SchwabMarketDataFamily::MarketHours,
            observed_at,
            oauth,
        )
        .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        let lease = self
            .acquire_attempt_publication_lease(oauth_epoch, observed_at, &cancellation)
            .await?;
        let ingested_at = trusted_now()?;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(SchwabMarketPublicationError::Deadline)?;
        let extraction_deadline = ingested_at
            .checked_add_nanos(
                i64::try_from(remaining.as_nanos())
                    .map_err(|_| SchwabMarketPublicationError::Deadline)?,
            )
            .map_err(|_| SchwabMarketPublicationError::Deadline)?;
        let discovery = DiscoveryRequest::try_new(
            sealed.persisted_receipt().capture().dataset().clone(),
            None,
            NonZeroU16::new(1).ok_or(SchwabMarketPublicationError::AuthorityInvalid)?,
            extraction_deadline,
        )
        .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        let extraction = ExtractionRequest::try_new(
            sealed.market_hours_source_object(&discovery)?,
            NonZeroU32::new(128).ok_or(SchwabMarketPublicationError::AuthorityInvalid)?,
            max_canonical_bytes,
            extraction_deadline,
        )
        .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        let publication = sealed.into_market_hours_publication(
            SchwabMarketHoursPublicationRequest::new(extraction, qualification, ingested_at),
        )?;
        let returned_products = publication.returned_products();
        let (revisions, binding) = publication.into_parts();
        binding.validate()?;
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )?;
        if binding.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
            || binding.batch().records().len() != returned_products * 2
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = self
            .rights
            .decision(payload_digest, observed_at)
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        let precommit: Arc<dyn IngestPrecommitAuthority> =
            Arc::new(SchwabCalendarPrecommit { lease, account });
        let request = ResearchIngestRequest::with_provider_publication(
            self.generation.metadata().clone(),
            rights,
            analytical_dataset,
            binding,
            revisions,
        )?
        .with_precommit_authority(precommit);
        if cancellation.is_cancelled() {
            return Err(SchwabMarketPublicationError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(SchwabMarketPublicationError::Deadline);
        }
        let committed = self.research.ingest(request, cancellation).await?;
        Ok(SchwabMarketHoursPublicationReceipt {
            committed,
            binding_digest,
            returned_products,
        })
    }
}
