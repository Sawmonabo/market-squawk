//! Generation-bound live SEC fund capture and publication orchestration.
//!
//! The activated SEC client owns the only network client and SEC raw-evidence store. This leaf
//! retains that exact client together with its registry authority and provider-generation
//! admission, captures one bounded quarterly N-PORT or N-CEN archive/readme graph, and closes a
//! matching one-use adapter preparation through [`SecFundApplicationBridge`] without creating a
//! second SEC client or raw store.

use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_sec::{
    SecAuthoritativeIdentifierNamespace, SecBulkError, SecBulkLayoutManifest, SecBulkParseLimits,
    SecBulkSelection, SecClientError, SecEdgarSource, SecFundIdentityAuthority,
    SecFundPartitionAdmissions, SecFundPublicationScope, SecGovernedIdentityReceipt,
    SecNportHoldingRow, SecNportIdentifierRow, SecPreparedFundLogicalPublication,
};
use market_squawk_data::{
    DatasetId, IngestError, IngestPrecommitAuthority, SecFundJobCatalogError, SecFundJobCommit,
    SecFundJobCoordinate, SecFundJobFamily, SecFundJobRecovery, SourceOperation,
};
use market_squawk_domain::{
    Cusip, DigestAlgorithm, EvidenceDigest, FundEvidenceRecord, FundHoldingSecurityIdentity,
    FundMissingState, FundReportedValue, FundSecurityIdentifier, FundShareClassIdentity,
    InstrumentId, Isin, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::SealedResearchJournalStore;
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    ExtractionAuthority, ExtractionAuthorityError, LogicalObjectRole, SourceMetadata,
    SourceMetadataProvider,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::provider_runtime::SecLiveFundCoordinatorSeal;
use super::{
    ResearchIngestCompositionError, ResearchProviderAdmission, ResearchProviderRuntimeGeneration,
    ResearchRightsAuthority, SecFundApplicationBridge, SecFundApplicationError,
    SecFundPublicationReceipt,
};
use crate::ResearchService;
use crate::application::research::{
    SecFundJobCommitAuthority, SecFundProductFamily, SecFundProductRequestFactory,
};

pub(super) const SEC_PROFILE: &str = "sec.edgar-public";
pub(super) const SEC_RUNTIME_SOURCE_ID: &str = "us-sec-edgar";
const SEC_CANONICAL_FUND_SOURCE_ID: &str = "sec-edgar";

/// One bounded exact SEC fund-family request.
#[derive(Clone, Debug)]
pub(crate) struct SecLiveFundRequest {
    selection: SecBulkSelection,
    scope: SecFundPublicationScope,
    analytical_dataset: DatasetId,
    parse_limits: SecBulkParseLimits,
    partition_admissions: SecFundPartitionAdmissions,
    deadline: Timestamp,
}

impl SecLiveFundRequest {
    /// Binds one filing/fund scope to the exact quarterly family and physical publication bounds.
    pub(crate) fn try_new(
        selection: SecBulkSelection,
        scope: SecFundPublicationScope,
        analytical_dataset: DatasetId,
        parse_limits: SecBulkParseLimits,
        partition_admissions: SecFundPartitionAdmissions,
        deadline: Timestamp,
    ) -> Result<Self, SecLiveFundApplicationError> {
        if selection.family() != scope.family() {
            return Err(SecLiveFundApplicationError::RequestMismatch);
        }
        Ok(Self {
            selection,
            scope,
            analytical_dataset,
            parse_limits,
            partition_admissions,
            deadline,
        })
    }

    pub(crate) const fn selection(&self) -> &SecBulkSelection {
        &self.selection
    }

    pub(crate) const fn scope(&self) -> &SecFundPublicationScope {
        &self.scope
    }

    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    pub(crate) const fn parse_limits(&self) -> SecBulkParseLimits {
        self.parse_limits
    }

    pub(crate) const fn partition_admissions(&self) -> SecFundPartitionAdmissions {
        self.partition_admissions
    }

    pub(crate) const fn deadline(&self) -> Timestamp {
        self.deadline
    }
}

/// Exact source, registry, rights, and application composition for live SEC fund work.
pub(crate) struct SecLiveFundSource {
    source: Arc<SecEdgarSource>,
    extraction: ExtractionAuthority,
    generation: ResearchProviderRuntimeGeneration,
    generation_digest: EvidenceDigest,
    admission: ResearchProviderAdmission,
    rights: ResearchRightsAuthority,
    research: Arc<ResearchService>,
    bridge: SecFundApplicationBridge,
    identity_authority_source_id: SourceId,
}

impl std::fmt::Debug for SecLiveFundSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecLiveFundSource")
            .field("profile", self.generation.profile())
            .field("source_id", self.source.metadata().source_id())
            .field("generation_digest", &self.generation_digest)
            .field(
                "identity_authority_source_id",
                &self.identity_authority_source_id,
            )
            .finish_non_exhaustive()
    }
}

impl SecLiveFundSource {
    /// Composes only values retained by one exact active research-provider runtime.
    ///
    /// This constructor is parent-module-visible so the coordinator can supply its private
    /// [`ResearchProviderAdmission`]. Callers outside the application authority cannot assemble a
    /// substitute generation lease.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, registry, runtime, rights, storage, and identity authorities are independent"
    )]
    pub(super) fn from_coordinator(
        _seal: SecLiveFundCoordinatorSeal,
        source: Arc<SecEdgarSource>,
        extraction: ExtractionAuthority,
        generation: ResearchProviderRuntimeGeneration,
        admission: ResearchProviderAdmission,
        rights: ResearchRightsAuthority,
        research: Arc<ResearchService>,
        identity_authority_source_id: SourceId,
    ) -> Result<Self, SecLiveFundApplicationError> {
        let metadata = source.metadata();
        let rejoined_generation = ResearchProviderRuntimeGeneration::try_new(
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
        admission.ensure_live()?;
        if generation.profile().as_str() != SEC_PROFILE
            || metadata.source_id().as_str() != SEC_RUNTIME_SOURCE_ID
            || extraction.metadata() != metadata
            || generation.metadata() != metadata
            || rejoined_generation != generation
            || !generation.rights_admits(SourceOperation::Retrieve)
            || !generation.rights_admits(SourceOperation::Persist)
            || !metadata.is_effective_at(generation.authority_effective_at())
        {
            return Err(SecLiveFundApplicationError::AuthorityMismatch);
        }
        extraction.validate_current()?;
        if !digest_is_valid(generation_digest) {
            return Err(SecLiveFundApplicationError::AuthorityMismatch);
        }
        let bridge = SecFundApplicationBridge::try_new(
            Arc::clone(&research),
            metadata.clone(),
            rights.clone(),
            generation.authority_effective_at(),
        )?;
        Ok(Self {
            source,
            extraction,
            generation,
            generation_digest,
            admission,
            rights,
            research,
            bridge,
            identity_authority_source_id,
        })
    }

    /// Completes one live bounded SEC fund graph through durable application publication.
    ///
    /// The same activated source, generation lease, absolute deadline, application journal, and
    /// governed identity authority are retained from network capture through the adapter's
    /// one-use preparation and the family-specific restart selector returned by the bridge.
    pub(crate) async fn acquire_and_publish(
        &self,
        request: SecLiveFundRequest,
        cancellation: CancellationToken,
    ) -> Result<SecFundPublicationReceipt, SecLiveFundApplicationError> {
        self.acquire_and_publish_inner(request, cancellation, None)
            .await
    }

    /// Executes the same exact live path while retaining one common-job terminal authority.
    pub(crate) async fn acquire_and_publish_for_job(
        &self,
        request: SecLiveFundRequest,
        cancellation: CancellationToken,
        commit: Arc<dyn SecFundJobCommitAuthority>,
    ) -> Result<SecFundPublicationReceipt, SecLiveFundApplicationError> {
        self.acquire_and_publish_inner(request, cancellation, Some(commit))
            .await
    }

    async fn acquire_and_publish_inner(
        &self,
        request: SecLiveFundRequest,
        cancellation: CancellationToken,
        job_commit: Option<Arc<dyn SecFundJobCommitAuthority>>,
    ) -> Result<SecFundPublicationReceipt, SecLiveFundApplicationError> {
        let started_at = system_timestamp()?;
        self.validate_request_authority(&request, started_at)?;
        let operation = SecLiveFundOperation::try_new(
            cancellation,
            self.admission.cancellation().clone(),
            request.deadline(),
            started_at,
        )?;
        let operation_token = operation.cancellation();
        let result = async {
            let captured = self.capture(request, operation_token.clone()).await?;
            captured.publication.validate_precommit()?;

            let identity_authority = SecLiveFundIdentityAuthority::new(
                Arc::clone(&captured.source),
                captured.identity_authority_source_id.clone(),
            );
            let prepared = captured
                .source
                .prepare_fund_logical_publication(
                    captured.manifest.clone(),
                    captured.request.scope().clone(),
                    captured.request.parse_limits(),
                    captured.request.partition_admissions(),
                    captured.captured_at,
                    captured.request.deadline(),
                    identity_authority,
                    Arc::clone(&captured.application_store),
                    operation_token.clone(),
                )
                .await?;
            captured.publication.validate_precommit()?;
            self.publish_captured(captured, prepared, operation_token, job_commit)
                .await
        }
        .await;
        operation.classify(result)
    }

    /// Reopens only one exact common-job coordinate from the sole durable catalog.
    pub(crate) fn recover_job_publication(
        &self,
        coordinate: SecFundJobCoordinate,
    ) -> Result<SecFundJobRecovery, SecLiveFundApplicationError> {
        self.research
            .analytical()
            .sec_fund_job_catalog()
            .recover_exact(coordinate)
            .map_err(Into::into)
    }

    /// Retrieves, seals in the existing SEC raw store, and inspects one exact quarterly graph.
    ///
    /// The provider-generation publication lease is retained in the returned value. Replacement
    /// or revocation can cancel the work, and publication cannot outlive that same generation.
    async fn capture(
        &self,
        request: SecLiveFundRequest,
        cancellation: CancellationToken,
    ) -> Result<SecLiveFundCapturedGraph, SecLiveFundApplicationError> {
        self.validate_current()?;
        let publication = Arc::new(self.admission.acquire_publication_lease().await?);
        publication.validate_precommit()?;
        let operation = cancellation.child_token();
        let manifest = self
            .source
            .fetch_and_inspect_bulk(
                &self.extraction,
                request.selection(),
                request.parse_limits(),
                request.deadline(),
                operation.clone(),
            )
            .await?;
        publication.validate_precommit()?;
        self.extraction.validate_current()?;
        validate_manifest(&manifest, &request)?;
        let captured_at = system_timestamp()?;
        if captured_at >= request.deadline() {
            return Err(SecLiveFundApplicationError::DeadlineExceeded);
        }
        Ok(SecLiveFundCapturedGraph {
            source: Arc::clone(&self.source),
            source_generation_digest: self.generation_digest,
            identity_authority_source_id: self.identity_authority_source_id.clone(),
            application_store: self.research.provider_capture_store(),
            publication,
            request,
            manifest,
            captured_at,
        })
    }

    /// Publishes only a preparation proven to represent this exact captured ZIP/PDF graph.
    async fn publish_captured(
        &self,
        captured: SecLiveFundCapturedGraph,
        prepared: SecPreparedFundLogicalPublication,
        cancellation: CancellationToken,
        job_commit: Option<Arc<dyn SecFundJobCommitAuthority>>,
    ) -> Result<SecFundPublicationReceipt, SecLiveFundApplicationError> {
        let observed_at = system_timestamp()?;
        if observed_at >= captured.request.deadline() {
            return Err(SecLiveFundApplicationError::DeadlineExceeded);
        }
        if observed_at < captured.captured_at
            || !Arc::ptr_eq(&self.source, &captured.source)
            || captured.source_generation_digest != self.generation_digest
        {
            return Err(SecLiveFundApplicationError::CapturedGraphMismatch);
        }
        self.validate_current()?;
        captured.publication.validate_precommit()?;
        validate_prepared_graph(&captured, &prepared)?;
        let analytical_dataset = captured.request.analytical_dataset().clone();
        let expected_scope = captured.request.scope().clone();
        let provider_precommit = SecLiveFundPrecommitAuthority {
            generation: captured.publication,
            source: self.generation.metadata().clone(),
            rights: self.rights.clone(),
            cancellation: cancellation.clone(),
            deadline: captured.request.deadline(),
        };
        let precommit: Arc<dyn IngestPrecommitAuthority> = match job_commit {
            Some(job) => Arc::new(SecLiveFundJobPrecommitAuthority::try_new(
                provider_precommit,
                job,
                &captured.request,
                &prepared,
            )?),
            None => Arc::new(provider_precommit),
        };
        precommit.validate_precommit()?;
        let receipt = self
            .bridge
            .publish(
                prepared,
                analytical_dataset.clone(),
                observed_at,
                precommit,
                cancellation,
            )
            .await?;
        validate_publication_receipt(&receipt, &expected_scope, &analytical_dataset)?;
        Ok(receipt)
    }

    fn validate_current(&self) -> Result<(), SecLiveFundApplicationError> {
        self.admission.ensure_live()?;
        self.extraction.validate_current()?;
        if self.source.metadata() != self.generation.metadata()
            || self.generation.generation_digest()? != self.generation_digest
        {
            return Err(SecLiveFundApplicationError::AuthorityMismatch);
        }
        Ok(())
    }

    fn validate_request_authority(
        &self,
        request: &SecLiveFundRequest,
        observed_at: Timestamp,
    ) -> Result<(), SecLiveFundApplicationError> {
        self.validate_current()?;
        self.rights.validate_at(observed_at)?;
        if self.generation.rights_exact_subjects().is_some() {
            return Err(SecLiveFundApplicationError::ScopedRightsUnavailable);
        }
        self.rights.validate_subject(None)?;
        if request.selection().family() != request.scope().family() {
            return Err(SecLiveFundApplicationError::RequestMismatch);
        }
        if request.deadline() <= observed_at {
            return Err(SecLiveFundApplicationError::DeadlineExceeded);
        }
        Ok(())
    }
}

const OPERATION_ACTIVE: u8 = 0;
const OPERATION_RUNTIME_REVOKED: u8 = 1;
const OPERATION_DEADLINE_EXCEEDED: u8 = 2;

/// Links caller cancellation, the exact provider generation, and one absolute wall deadline.
struct SecLiveFundOperation {
    cancellation: CancellationToken,
    deadline: Timestamp,
    terminal_reason: Arc<AtomicU8>,
    linker: tokio::task::JoinHandle<()>,
}

impl SecLiveFundOperation {
    fn try_new(
        caller: CancellationToken,
        runtime: CancellationToken,
        deadline: Timestamp,
        started_at: Timestamp,
    ) -> Result<Self, SecLiveFundApplicationError> {
        let remaining = remaining_until(deadline, started_at)?;
        let cancellation = caller.child_token();
        let worker_cancellation = cancellation.clone();
        let terminal_reason = Arc::new(AtomicU8::new(OPERATION_ACTIVE));
        let worker_reason = Arc::clone(&terminal_reason);
        let linker = tokio::spawn(async move {
            tokio::select! {
                biased;
                () = runtime.cancelled() => {
                    worker_reason.store(OPERATION_RUNTIME_REVOKED, Ordering::Release);
                    worker_cancellation.cancel();
                }
                () = tokio::time::sleep(remaining) => {
                    worker_reason.store(OPERATION_DEADLINE_EXCEEDED, Ordering::Release);
                    worker_cancellation.cancel();
                }
                () = worker_cancellation.cancelled() => {}
            }
        });
        Ok(Self {
            cancellation,
            deadline,
            terminal_reason,
            linker,
        })
    }

    fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    fn classify<T>(
        &self,
        result: Result<T, SecLiveFundApplicationError>,
    ) -> Result<T, SecLiveFundApplicationError> {
        if result.is_ok() {
            return result;
        }
        match self.terminal_reason.load(Ordering::Acquire) {
            OPERATION_RUNTIME_REVOKED => {
                return Err(SecLiveFundApplicationError::RuntimeUnavailable);
            }
            OPERATION_DEADLINE_EXCEEDED => {
                return Err(SecLiveFundApplicationError::DeadlineExceeded);
            }
            OPERATION_ACTIVE => {}
            _ => return result,
        }
        if matches!(
            result.as_ref(),
            Err(SecLiveFundApplicationError::Client(
                SecClientError::DeadlineExceeded
            )) | Err(SecLiveFundApplicationError::Preparation(
                SecBulkError::DeadlineExceeded
                    | SecBulkError::Client(SecClientError::DeadlineExceeded)
            )) | Err(SecLiveFundApplicationError::Publication(
                SecFundApplicationError::Ingest(IngestError::DeadlineExceeded)
            ))
        ) {
            return Err(SecLiveFundApplicationError::DeadlineExceeded);
        }
        if system_timestamp().is_ok_and(|observed_at| observed_at >= self.deadline) {
            return Err(SecLiveFundApplicationError::DeadlineExceeded);
        }
        result
    }
}

impl Drop for SecLiveFundOperation {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.linker.abort();
    }
}

/// Exact-generation and operation-lifetime authority checked at the atomic catalog precommit.
#[derive(Debug)]
struct SecLiveFundPrecommitAuthority {
    generation: Arc<super::ResearchProviderPublicationLease>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    cancellation: CancellationToken,
    deadline: Timestamp,
}

impl IngestPrecommitAuthority for SecLiveFundPrecommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.generation
            .validate_precommit()
            .map_err(|_error| IngestError::PublicationAuthorityRevoked)?;
        let observed_at =
            system_timestamp().map_err(|_error| IngestError::PublicationAuthorityRevoked)?;
        if !self.source.is_effective_at(observed_at)
            || self.rights.validate_at(observed_at).is_err()
        {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        if observed_at >= self.deadline {
            return Err(IngestError::DeadlineExceeded);
        }
        if self.cancellation.is_cancelled() {
            return Err(IngestError::Cancelled);
        }
        Ok(())
    }
}

/// Exact provider-generation precommit plus one one-shot common-job terminal claim.
struct SecLiveFundJobPrecommitAuthority {
    provider: SecLiveFundPrecommitAuthority,
    job: Arc<dyn SecFundJobCommitAuthority>,
    coordinate: SecFundJobCoordinate,
    family: SecFundJobFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    fund_id: Option<SourceIdentifier>,
    preparation_digest: EvidenceDigest,
    fund_instrument_id: InstrumentId,
    row_count: u64,
    logical_object_bytes: u64,
    logical_object_count: usize,
}

impl SecLiveFundJobPrecommitAuthority {
    fn try_new(
        provider: SecLiveFundPrecommitAuthority,
        job: Arc<dyn SecFundJobCommitAuthority>,
        request: &SecLiveFundRequest,
        prepared: &SecPreparedFundLogicalPublication,
    ) -> Result<Self, SecLiveFundApplicationError> {
        let execution = job.execution_coordinate();
        let coordinate = SecFundJobCoordinate::try_new(
            execution.job_id().as_uuid(),
            execution.generation().get(),
            execution.admitted_request_digest(),
        )
        .map_err(|_| SecLiveFundApplicationError::AuthorityMismatch)?;
        let product = SecFundProductRequestFactory
            .validate_live_execution(request, coordinate.admitted_request_digest())
            .map_err(|_| SecLiveFundApplicationError::RequestMismatch)?;
        let family = match product.family() {
            SecFundProductFamily::Nport => SecFundJobFamily::Nport,
            SecFundProductFamily::Ncen => SecFundJobFamily::Ncen,
        };
        let fund_instrument_id = prepared_fund_instrument_id(prepared)?;
        let row_count = prepared.terminal().total_canonical_rows;
        let logical_object_bytes = prepared.terminal().total_logical_object_bytes;
        let logical_object_count = prepared.objects().len();
        if row_count == 0 || logical_object_bytes == 0 || logical_object_count == 0 {
            return Err(SecLiveFundApplicationError::PreparedGraphMismatch);
        }
        Ok(Self {
            provider,
            job,
            coordinate,
            family,
            year: product.year(),
            quarter: product.quarter(),
            accession: product.accession().clone(),
            fund_id: product.fund_id().cloned(),
            preparation_digest: prepared.preparation_digest(),
            fund_instrument_id,
            row_count,
            logical_object_bytes,
            logical_object_count,
        })
    }
}

impl IngestPrecommitAuthority for SecLiveFundJobPrecommitAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.provider.validate_precommit()
    }

    fn claim_sec_fund_job_commit(
        &self,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<SecFundJobCommit>, IngestError> {
        self.provider.validate_precommit()?;
        let commit = SecFundJobCommit::try_new(
            self.coordinate,
            binding_digest,
            self.preparation_digest,
            self.family,
            self.year,
            self.quarter,
            self.accession.clone(),
            self.fund_id.clone(),
            self.fund_instrument_id,
            self.row_count,
            self.logical_object_bytes,
            self.logical_object_count,
        )
        .map_err(|_| IngestError::ProviderLogicalFundRequired)?;
        // The common terminal permit is deliberately the final fallible authority claim before
        // the catalog stages this exact binding and enters the existing manifest transaction.
        self.job.validate_precommit()?;
        Ok(Some(commit))
    }
}

impl std::fmt::Debug for SecLiveFundJobPrecommitAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecLiveFundJobPrecommitAuthority")
            .field("coordinate", &self.coordinate)
            .field("family", &self.family)
            .field("year", &self.year)
            .field("quarter", &self.quarter)
            .field("accession", &self.accession)
            .field("fund_id", &self.fund_id)
            .field("preparation_digest", &self.preparation_digest)
            .field("fund_instrument_id", &self.fund_instrument_id)
            .finish_non_exhaustive()
    }
}

/// Captured SEC raw graph plus the exact still-live provider publication lease.
///
/// This value is intentionally noncloneable. Dropping it releases the provider-generation read
/// lease without publishing any analytical generation.
struct SecLiveFundCapturedGraph {
    source: Arc<SecEdgarSource>,
    source_generation_digest: EvidenceDigest,
    identity_authority_source_id: SourceId,
    application_store: Arc<SealedResearchJournalStore>,
    publication: Arc<super::ResearchProviderPublicationLease>,
    request: SecLiveFundRequest,
    manifest: SecBulkLayoutManifest,
    captured_at: Timestamp,
}

impl std::fmt::Debug for SecLiveFundCapturedGraph {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecLiveFundCapturedGraph")
            .field("selection", self.request.selection())
            .field("scope", self.request.scope())
            .field("manifest_evidence", &self.manifest.evidence())
            .field("captured_at", &self.captured_at)
            .finish_non_exhaustive()
    }
}

/// Exact governed SEC-series and holding-security resolution for canonical fund preparation.
struct SecLiveFundIdentityAuthority {
    source: Arc<SecEdgarSource>,
    authority_source_id: SourceId,
}

impl SecLiveFundIdentityAuthority {
    fn new(source: Arc<SecEdgarSource>, authority_source_id: SourceId) -> Self {
        Self {
            source,
            authority_source_id,
        }
    }

    fn resolve_holding_identifier(
        &self,
        namespace: SecAuthoritativeIdentifierNamespace,
        source_identifier: &SourceIdentifier,
        canonical_identifier: FundSecurityIdentifier,
        cutoff: Timestamp,
    ) -> Result<Option<ResolvedHoldingIdentifier>, SecBulkError> {
        match self.source.resolve_bulk_identity(
            namespace,
            &self.authority_source_id,
            source_identifier,
            cutoff,
        ) {
            Ok(receipt) if receipt.available_at() <= cutoff && receipt.observed_at() <= cutoff => {
                Ok(Some(ResolvedHoldingIdentifier {
                    receipt,
                    canonical_identifier,
                }))
            }
            Ok(_) => Ok(None),
            Err(SecBulkError::UnresolvedIdentity) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl SecFundIdentityAuthority for SecLiveFundIdentityAuthority {
    fn resolve_share_class(
        &mut self,
        series_id: &SourceIdentifier,
        cutoff: Timestamp,
    ) -> Result<FundShareClassIdentity, SecBulkError> {
        let authority = self.source.resolve_bulk_identity(
            SecAuthoritativeIdentifierNamespace::SecSeriesId,
            &self.authority_source_id,
            series_id,
            cutoff,
        )?;
        if authority.available_at() > cutoff || authority.observed_at() > cutoff {
            return Err(SecBulkError::UnresolvedIdentity);
        }
        FundShareClassIdentity::try_new(
            authority.instrument_id(),
            series_id.clone(),
            authority.authority_source_id().clone(),
            MetadataRevision::new(authority.authority_revision().clone()),
            authority.evidence().clone(),
            authority.available_at(),
            authority.observed_at(),
        )
        .map_err(|_| SecBulkError::UnresolvedIdentity)
    }

    fn resolve_holding_security(
        &mut self,
        holding: &SecNportHoldingRow,
        identifiers: &[SecNportIdentifierRow],
        cutoff: Timestamp,
    ) -> Result<FundHoldingSecurityIdentity, SecBulkError> {
        let mut resolved = Vec::new();
        let isins = identifiers
            .iter()
            .filter_map(|row| row.isin.clone())
            .collect::<BTreeSet<_>>();
        resolved
            .try_reserve_exact(
                isins
                    .len()
                    .saturating_add(usize::from(holding.cusip.is_some())),
            )
            .map_err(|_| SecBulkError::AllocationFailed)?;
        for source_identifier in isins {
            let Ok(isin) = Isin::try_from(source_identifier.as_str()) else {
                continue;
            };
            if let Some(candidate) = self.resolve_holding_identifier(
                SecAuthoritativeIdentifierNamespace::Isin,
                &source_identifier,
                FundSecurityIdentifier::Isin(isin),
                cutoff,
            )? {
                resolved.push(candidate);
            }
        }
        if let Some(source_identifier) = holding.cusip.as_ref() {
            if let Ok(cusip) = Cusip::try_from(source_identifier.as_str()) {
                if let Some(candidate) = self.resolve_holding_identifier(
                    SecAuthoritativeIdentifierNamespace::Cusip,
                    source_identifier,
                    FundSecurityIdentifier::Cusip(cusip),
                    cutoff,
                )? {
                    resolved.push(candidate);
                }
            }
        }
        let Some(first) = resolved.first() else {
            return FundHoldingSecurityIdentity::unresolved(FundMissingState::UnresolvedIdentity)
                .map_err(|_| SecBulkError::UnresolvedIdentity);
        };
        if resolved
            .iter()
            .any(|candidate| candidate.receipt.instrument_id() != first.receipt.instrument_id())
        {
            return FundHoldingSecurityIdentity::try_ambiguous(holding_conflict_digest(&resolved)?)
                .map_err(|_| SecBulkError::UnresolvedIdentity);
        }
        FundHoldingSecurityIdentity::try_exact(
            first.receipt.instrument_id(),
            first.canonical_identifier.clone(),
            first.receipt.authority_source_id().clone(),
            MetadataRevision::new(first.receipt.authority_revision().clone()),
            first.receipt.evidence().clone(),
            FundReportedValue::Missing(FundMissingState::SourceAbsent),
            first.receipt.available_at(),
            first.receipt.observed_at(),
        )
        .map_err(|_| SecBulkError::UnresolvedIdentity)
    }
}

struct ResolvedHoldingIdentifier {
    receipt: SecGovernedIdentityReceipt,
    canonical_identifier: FundSecurityIdentifier,
}

fn holding_conflict_digest(
    candidates: &[ResolvedHoldingIdentifier],
) -> Result<EvidenceDigest, SecBulkError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/application/sec-fund-holding-identity-conflict/v1\0");
    digest.update(
        u64::try_from(candidates.len())
            .map_err(|_| SecBulkError::QueryLimitExceeded)?
            .to_be_bytes(),
    );
    for candidate in candidates {
        let encoded = serde_json::to_vec(&candidate.receipt)
            .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
        digest.update(
            u64::try_from(encoded.len())
                .map_err(|_| SecBulkError::QueryLimitExceeded)?
                .to_be_bytes(),
        );
        digest.update(encoded);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn validate_manifest(
    manifest: &SecBulkLayoutManifest,
    request: &SecLiveFundRequest,
) -> Result<(), SecLiveFundApplicationError> {
    if manifest.capture().selection() != request.selection()
        || manifest.official_readme_capture().selection() != request.selection()
        || request.scope().family() != request.selection().family()
        || manifest.capture().transport().body_received_at() > request.deadline()
        || manifest
            .official_readme_capture()
            .transport()
            .body_received_at()
            > request.deadline()
    {
        return Err(SecLiveFundApplicationError::CapturedGraphMismatch);
    }
    if digest_is_valid(manifest.evidence()) {
        Ok(())
    } else {
        Err(SecLiveFundApplicationError::CapturedGraphMismatch)
    }
}

fn validate_prepared_graph(
    captured: &SecLiveFundCapturedGraph,
    prepared: &SecPreparedFundLogicalPublication,
) -> Result<(), SecLiveFundApplicationError> {
    let archive = captured.manifest.capture();
    let readme = captured.manifest.official_readme_capture();
    let objects = prepared.objects();
    let expected_bytes = archive
        .size_bytes()
        .checked_add(readme.size_bytes())
        .ok_or(SecLiveFundApplicationError::CapturedGraphMismatch)?;
    if prepared.scope() != captured.request.scope()
        || prepared.terminal().source_id.as_str() != SEC_CANONICAL_FUND_SOURCE_ID
        || prepared.terminal().total_decoded_events != 0
        || prepared.terminal().total_logical_object_bytes != expected_bytes
        || prepared.canonical_partitions().is_empty()
        || prepared.partitions().is_empty()
        || objects.len() != 2
        || objects[0].role() != LogicalObjectRole::ProviderPayload
        || objects[0].ordinal() != 0
        || objects[0].object().content_digest() != archive.evidence()
        || objects[0].object().size_bytes() != archive.size_bytes()
        || objects[1].role() != LogicalObjectRole::ProviderComponent
        || objects[1].ordinal() != 1
        || objects[1].object().content_digest() != readme.evidence()
        || objects[1].object().size_bytes() != readme.size_bytes()
    {
        return Err(SecLiveFundApplicationError::PreparedGraphMismatch);
    }
    if digest_is_valid(prepared.preparation_digest())
        && digest_is_valid(prepared.terminal().source_revision_digest)
        && digest_is_valid(prepared.terminal().provider_terminal_evidence_digest)
    {
        Ok(())
    } else {
        Err(SecLiveFundApplicationError::PreparedGraphMismatch)
    }
}

fn prepared_fund_instrument_id(
    prepared: &SecPreparedFundLogicalPublication,
) -> Result<InstrumentId, SecLiveFundApplicationError> {
    let mut instrument_id = None;
    for record in prepared
        .canonical_partitions()
        .iter()
        .flat_map(|partition| partition.records())
    {
        let current = match record {
            FundEvidenceRecord::Report(value) => value.filing().fund().instrument_id(),
            FundEvidenceRecord::ShareClass(value) => value.filing().fund().instrument_id(),
            FundEvidenceRecord::PortfolioHolding(value) => value.filing().fund().instrument_id(),
        };
        match instrument_id {
            Some(expected) if expected != current => {
                return Err(SecLiveFundApplicationError::PreparedGraphMismatch);
            }
            Some(_) => {}
            None => instrument_id = Some(current),
        }
    }
    instrument_id.ok_or(SecLiveFundApplicationError::PreparedGraphMismatch)
}

fn validate_publication_receipt(
    receipt: &SecFundPublicationReceipt,
    expected_scope: &SecFundPublicationScope,
    expected_dataset: &DatasetId,
) -> Result<(), SecLiveFundApplicationError> {
    let matches = match (receipt, expected_scope) {
        (
            SecFundPublicationReceipt::Nport(receipt),
            SecFundPublicationScope::Nport { accession },
        ) => {
            receipt.restart_selector().accession() == accession
                && receipt.committed().manifest().dataset_id() == expected_dataset
        }
        (
            SecFundPublicationReceipt::Ncen(receipt),
            SecFundPublicationScope::Ncen { accession, fund_id },
        ) => {
            receipt.restart_selector().accession() == accession
                && receipt.restart_selector().fund_id() == fund_id
                && receipt.committed().manifest().dataset_id() == expected_dataset
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(SecLiveFundApplicationError::PublicationReceiptMismatch)
    }
}

fn digest_is_valid(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes().iter().any(|byte| *byte != 0)
}

fn system_timestamp() -> Result<Timestamp, SecLiveFundApplicationError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SecLiveFundApplicationError::TrustedTimeUnavailable)?
        .as_nanos();
    let nanos =
        i64::try_from(nanos).map_err(|_| SecLiveFundApplicationError::TrustedTimeUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn remaining_until(
    deadline: Timestamp,
    started_at: Timestamp,
) -> Result<Duration, SecLiveFundApplicationError> {
    let nanos = deadline
        .unix_nanos()
        .checked_sub(started_at.unix_nanos())
        .filter(|remaining| *remaining > 0)
        .and_then(|remaining| u64::try_from(remaining).ok())
        .ok_or(SecLiveFundApplicationError::DeadlineExceeded)?;
    Ok(Duration::from_nanos(nanos))
}

/// Failure before exact SEC capture, raw/logical closure, analytical publication, or restart
/// selector retention.
#[derive(Debug, Error)]
pub(crate) enum SecLiveFundApplicationError {
    #[error("SEC live fund request family and filing scope do not match")]
    RequestMismatch,
    #[error("SEC live fund source, registry, rights, or runtime generation do not match")]
    AuthorityMismatch,
    #[error("scoped SEC fund rights cannot be matched to a closed code-owned request subject")]
    ScopedRightsUnavailable,
    #[error("SEC live fund provider generation is no longer callable")]
    RuntimeUnavailable,
    #[error("SEC live fund operation deadline was exceeded")]
    DeadlineExceeded,
    #[error("SEC live fund captured archive/readme graph does not match the request")]
    CapturedGraphMismatch,
    #[error("SEC live fund one-use preparation does not match the captured graph")]
    PreparedGraphMismatch,
    #[error("SEC live fund publication receipt does not match the requested filing scope")]
    PublicationReceiptMismatch,
    #[error("trusted SEC live fund operation time is unavailable")]
    TrustedTimeUnavailable,
    #[error("SEC provider runtime authority is unavailable")]
    Runtime(#[from] ResearchIngestCompositionError),
    #[error("SEC extraction registry authority is unavailable")]
    ExtractionAuthority(#[from] ExtractionAuthorityError),
    #[error("SEC fund request rights are unavailable or expired")]
    Rights(#[from] ServiceError),
    #[error("SEC fund exact publication precommit validation failed")]
    Precommit(#[from] IngestError),
    #[error("SEC live fund acquisition failed")]
    Client(#[from] SecClientError),
    #[error("SEC fund raw/logical preparation failed")]
    Preparation(#[from] SecBulkError),
    #[error("SEC fund analytical publication failed")]
    Publication(#[from] SecFundApplicationError),
    #[error("SEC fund durable job catalog recovery failed")]
    JobCatalog(#[from] SecFundJobCatalogError),
}
