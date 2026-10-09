//! Source-backed canonical reference publication from the existing sealed Instruments FIFO.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_schwab::{
    RestExecutionOutcome, SchwabCanonicalField, SchwabCaptureCoordinates,
    SchwabSealedInstrumentReference, SchwabTransportError,
};
use market_squawk_data::{
    CatalogAuthority, IngestError, IngestPrecommitAuthority, ListingReferenceRecord,
    MarketDataInstrumentCatalogError, MarketDataInstrumentRecord,
    MarketDataInstrumentSourceReferenceInput,
    OfficialIssuerInstrumentReference as IssuerInstrumentReference, SourceOperation,
};
use market_squawk_domain::{
    AssignmentVerification, DigestAlgorithm, EffectiveInterval, ExternalIdentifier,
    ExternalIdentifierRecord, ExternalIdentifierRecordInput, IdentifierEntitlement,
    IdentifierRightsPolicyReference, MarketDataDisplayName, MetadataRevision, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    ProductionResearchIngestCoordinator, ResearchProviderPublicationOperation,
    ResearchProviderRuntimeGeneration,
};
use crate::ResearchService;
use crate::provider_activation::{
    ProviderAccountPublicationAuthority, ProviderAccountRuntimeCurrentness,
};
use crate::provider_onboarding::{SchwabOAuthPublicationEpoch, SchwabOAuthReceiptCurrentness};

pub(crate) const SCHWAB_INSTRUMENT_REFERENCE_PROFILE: &str =
    "schwab.trader-api-market-data.instruments";
pub(crate) const SCHWAB_INSTRUMENT_REFERENCE_SOURCE: &str = "schwab-trader-api-instruments";
pub(crate) const SCHWAB_INSTRUMENT_REFERENCE_DATASET: &str = "schwab.instruments.detail";

/// One bounded Instruments operation, independent of quote coverage and execution metadata.
pub(crate) struct SchwabInstrumentReferencePublicationAuthority {
    research: Arc<ResearchService>,
    operation: ResearchProviderPublicationOperation,
    coordinates: SchwabCaptureCoordinates,
    oauth: SchwabOAuthReceiptCurrentness,
    receipt: market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt,
}

impl ProductionResearchIngestCoordinator {
    pub(crate) async fn acquire_schwab_instrument_reference_publication(
        &self,
        generation: &ResearchProviderRuntimeGeneration,
        oauth: SchwabOAuthReceiptCurrentness,
        receipt: market_squawk_adapter_schwab::SchwabOAuthAuthorityReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<
        Arc<SchwabInstrumentReferencePublicationAuthority>,
        SchwabInstrumentReferencePublicationError,
    > {
        if generation.profile().as_str() != SCHWAB_INSTRUMENT_REFERENCE_PROFILE
            || generation.metadata().source_id().as_str() != SCHWAB_INSTRUMENT_REFERENCE_SOURCE
            || generation.metadata().capabilities().live()
            || !generation.metadata().coverage().live_channels().is_empty()
            || generation.session_id() != oauth.session_id()
            || generation.secret_reference().is_none()
            || generation.credential_generation() != Some(receipt.credential_authority().application_credential_generation())
            || market_squawk_adapter_schwab::SchwabCredentialAuthorityBinding::try_from_application_credential(
                generation.secret_reference().ok_or(ServiceError::Unauthorized)?
            )? != receipt.credential_authority()
        {
            return Err(ServiceError::Unauthorized.into());
        }
        let coordinates = SchwabCaptureCoordinates::try_new(
            generation.metadata().source_id().clone(),
            generation.metadata().revision().clone(),
            SourceIdentifier::try_from(SCHWAB_INSTRUMENT_REFERENCE_DATASET)
                .map_err(|_| ServiceError::Internal)?,
            generation.session_id(),
        )?;
        let operation = self
            .acquire_provider_publication_operation(generation, cancellation, deadline)
            .await?;
        operation
            .rights()
            .validate_subject(Some(coordinates.dataset()))?;
        let result = Arc::new(SchwabInstrumentReferencePublicationAuthority {
            research: Arc::clone(&self.research),
            operation,
            coordinates,
            oauth,
            receipt,
        });
        result.require_current(trusted_now()?)?;
        Ok(result)
    }
}

impl SchwabInstrumentReferencePublicationAuthority {
    fn metadata(&self) -> &SourceMetadata {
        self.operation.source()
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        self.operation.cancellation()
    }

    fn require_current(&self, now: Timestamp) -> Result<(), ServiceError> {
        self.operation
            .validate_precommit()
            .map_err(|_| ServiceError::Unauthorized)?;
        self.oauth
            .validate_current_authorization(self.receipt)
            .map_err(|_| ServiceError::Unauthorized)?;
        if !self.metadata().is_effective_at(now) {
            return Err(ServiceError::Unauthorized);
        }
        self.operation.rights().validate_at(now)
    }

    /// Every completed body crosses the sole physical sealer, including refusals and bad JSON.
    pub(crate) async fn seal_detail_outcome(
        &self,
        outcome: RestExecutionOutcome,
        expected_cusip: &market_squawk_domain::Cusip,
        cleanup_deadline: Instant,
    ) -> Result<SchwabSealedInstrumentReference, SchwabInstrumentReferencePublicationError> {
        let cancellation = CancellationToken::new();
        match outcome {
            RestExecutionOutcome::Accepted(response) => {
                let pending = response
                    .into_pending_capture(self.coordinates.clone(), uuid::Uuid::new_v4())?;
                let (rejoin, request) = pending.into_sealing_parts();
                let sealed = self
                    .research
                    .seal_provider_capture(request, &cancellation, cleanup_deadline)
                    .await?;
                Ok(SchwabSealedInstrumentReference::try_from_detail(
                    rejoin.try_rejoin(sealed)?,
                    expected_cusip,
                )?)
            }
            RestExecutionOutcome::ProviderRejected(capture)
            | RestExecutionOutcome::InvalidPayload { capture, .. } => {
                let pending =
                    capture.into_pending_capture(self.coordinates.clone(), uuid::Uuid::new_v4())?;
                let (rejoin, request) = pending.into_sealing_parts();
                let sealed = self
                    .research
                    .seal_provider_capture(request, &cancellation, cleanup_deadline)
                    .await?;
                let _sealed = rejoin.try_rejoin(sealed)?;
                Err(ServiceError::Unavailable.into())
            }
            _ => Err(ServiceError::InvalidResult.into()),
        }
    }
}

impl SchwabInstrumentReferencePublicationAuthority {
    /// Uses the actual admitted generation, rights and sealed capture. Account and exact listing
    /// guards are acquired after normalization and retained by the existing supervised publisher.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact source and publication authorities remain explicit"
    )]
    pub(crate) async fn publish_instrument_reference(
        self: &Arc<Self>,
        reference: SchwabSealedInstrumentReference,
        issuer: &IssuerInstrumentReference,
        official_listing: ListingReferenceRecord,
        expected_current: Option<MarketDataInstrumentRecord>,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        currentness: ProviderAccountRuntimeCurrentness,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, SchwabInstrumentReferencePublicationError> {
        check_operation(deadline, &cancellation)?;
        let observed_at = reference.received_at();
        let now = trusted_now()?;
        self.require_current(now)?;
        oauth_epoch
            .validate_current(oauth_epoch.receipt())
            .map_err(|_| ServiceError::Unauthorized)?;
        if reference.receipt().token_generation() != oauth_epoch.receipt().generation()
            || reference.receipt().credential_authority()
                != oauth_epoch.receipt().credential_authority()
        {
            return Err(ServiceError::InvalidResult.into());
        }
        if observed_at.unix_nanos() <= 0 || observed_at > now {
            return Err(ServiceError::InvalidResult.into());
        }
        if reference.coordinates() != &self.coordinates {
            return Err(ServiceError::InvalidResult.into());
        }
        self.operation.rights().validate_at(now)?;
        self.operation
            .rights()
            .validate_subject(Some(self.coordinates.dataset()))?;
        issuer
            .validate_listing(&official_listing, observed_at)
            .map_err(|_| ServiceError::Unavailable)?;

        let native = reference.candidate();
        if reference.requested_cusip() != issuer.cusip()
            || !matches!(&native.cusip, SchwabCanonicalField::Value(value) if value.as_ref() == issuer.cusip().as_str())
            || !matches!(&native.symbol, SchwabCanonicalField::Value(value) if value.as_ref() == issuer.symbol().as_str())
        {
            return Err(ServiceError::InvalidResult.into());
        }
        // The source's exact exchange and asset_type states remain in the sealed raw capture.
        // Their uninterpreted text cannot override the admitted official listing/issuer proof.
        let SchwabCanonicalField::Value(description) = &native.description else {
            return Err(ServiceError::Unavailable.into());
        };
        let payload = reference.evidence();
        let rights = self
            .operation
            .rights()
            .decision(payload.content_digest(), observed_at)?;
        if !rights
            .permitted_operations
            .contains(&SourceOperation::Display)
            || !rights
                .permitted_operations
                .contains(&SourceOperation::Persist)
        {
            return Err(ServiceError::Unauthorized.into());
        }
        // This is the exact existing personal/internal-use grant. No redistribution permission
        // is inferred from successful HTTP, source names, or a provider's displayable quote.
        let rights_policy = IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from(
                format!(
                    "authorization-sha256:{}",
                    lower_hex(&rights.authorization_evidence.bytes()),
                )
                .as_str(),
            )
            .map_err(|_| ServiceError::Internal)?,
            IdentifierEntitlement::LicensedInternalUse,
            SourceIdentifier::try_from(rights.basis.reference())
                .map_err(|_| ServiceError::InvalidResult)?,
        );
        let source = self.metadata().clone();
        let validity =
            EffectiveInterval::new(observed_at, None).map_err(|_| ServiceError::InvalidResult)?;
        let display_name = MarketDataDisplayName::try_new(
            description.as_ref(),
            source.source_id().clone(),
            payload.clone(),
            rights_policy.clone(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let identifiers = [
            ExternalIdentifier::Cusip(issuer.cusip().clone()),
            ExternalIdentifier::Ticker(issuer.symbol().clone()),
        ]
        .map(|identifier| {
            ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
                identifier,
                assignment_verification: AssignmentVerification::VerifiedAssigned,
                source_id: source.source_id().clone(),
                source_evidence: payload.clone(),
                source_timestamp: None,
                observed_at,
                validity,
                rights_policy: rights_policy.clone(),
            })
        });
        let provider_instrument_id = ProviderInstrumentId::try_from(issuer.symbol().as_str())
            .map_err(|_| ServiceError::InvalidResult)?;
        let revision = reference_revision(&reference, &official_listing, issuer)?;
        let authorization_expires_at = rights.authorization_expires_at;
        let input = MarketDataInstrumentSourceReferenceInput {
            source,
            rights,
            capture: reference.into_capture_token(),
            official_listing,
            expected_current,
            reference_evidence: RevisionBoundPayloadEvidence::new(revision, payload),
            effective_interval: validity,
            display_name,
            quote_currency: issuer.quote_currency(),
            quote_currency_evidence: issuer.currency_evidence().clone(),
            issuer_identity_evidence: issuer.identity_evidence().clone(),
            identifiers,
            provider_instrument_id,
        };

        check_operation(deadline, &cancellation)?;
        let account = currentness
            .try_acquire_publication_authority()
            .map_err(|_| ServiceError::Unauthorized)?;
        let precommit: Arc<dyn IngestPrecommitAuthority> = Arc::new(InstrumentReferencePrecommit {
            authority: Arc::clone(self),
            account,
            oauth_epoch,
            authorization_expires_at,
            deadline,
            cancellation: cancellation.clone(),
        });
        self.research
            .publish_market_data_source_reference(input, precommit, deadline, cancellation)
            .await
            .map_err(Into::into)
    }
}

fn reference_revision(
    reference: &SchwabSealedInstrumentReference,
    listing: &ListingReferenceRecord,
    issuer: &IssuerInstrumentReference,
) -> Result<MetadataRevision, ServiceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/source-reference-revision/v1\0");
    for digest in [
        reference.persisted_receipt().receipt_digest(),
        listing.generation().generation_digest(),
        listing.record_payload_evidence().content_digest(),
        issuer.currency_evidence().content_digest(),
        issuer.identity_evidence().content_digest(),
    ] {
        if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        hash.update(digest.bytes());
    }
    let digest: [u8; 32] = hash.finalize().into();
    Ok(MetadataRevision::new(
        SourceIdentifier::try_from(
            format!("source-reference-sha256:{}", lower_hex(&digest),).as_str(),
        )
        .map_err(|_| ServiceError::Internal)?,
    ))
}

struct InstrumentReferencePrecommit {
    authority: Arc<SchwabInstrumentReferencePublicationAuthority>,
    account: ProviderAccountPublicationAuthority,
    oauth_epoch: SchwabOAuthPublicationEpoch,
    authorization_expires_at: Option<Timestamp>,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl std::fmt::Debug for InstrumentReferencePrecommit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstrumentReferencePrecommit")
            .finish_non_exhaustive()
    }
}

impl InstrumentReferencePrecommit {
    fn require_operation(&self) -> Result<(), IngestError> {
        if self.cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(IngestError::DeadlineExceeded);
        }
        let now = trusted_now().map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        if self
            .authorization_expires_at
            .is_some_and(|expiry| now >= expiry)
        {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        Ok(())
    }
}

impl IngestPrecommitAuthority for InstrumentReferencePrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.require_operation()?;
        self.authority
            .require_current(trusted_now().map_err(|_| IngestError::PublicationAuthorityRevoked)?)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.oauth_epoch
            .validate_current(self.oauth_epoch.receipt())
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.require_operation()
    }

    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.require_operation()?;
        self.authority
            .require_current(trusted_now().map_err(|_| IngestError::PublicationAuthorityRevoked)?)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.oauth_epoch
            .validate_current(self.oauth_epoch.receipt())
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.require_operation()
    }
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}

/// Preserves exact source, cancellation, resource and publication failures for the existing caller.
#[derive(Debug, Error)]
pub(crate) enum SchwabInstrumentReferencePublicationError {
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Composition(#[from] super::ResearchIngestCompositionError),
    #[error(transparent)]
    Transport(#[from] SchwabTransportError),
    #[error(transparent)]
    Reference(#[from] market_squawk_adapter_schwab::SchwabInstrumentReferenceError),
    #[error(transparent)]
    Research(#[from] crate::ResearchServiceError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Catalog(#[from] MarketDataInstrumentCatalogError),
}

fn trusted_now() -> Result<Timestamp, ServiceError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = i64::try_from(elapsed.as_nanos()).map_err(|_| ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn lower_hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(64);
    for byte in bytes {
        result.push(char::from(HEX[usize::from(byte >> 4)]));
        result.push(char::from(HEX[usize::from(byte & 15)]));
    }
    result
}
