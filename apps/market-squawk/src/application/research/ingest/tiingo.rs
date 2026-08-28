//! Application-owned Tiingo latest NAV/EOD sealing, publication, and exact PIT reads.
//!
//! This leaf performs no network request and owns no quota or credential state. It consumes the
//! adapter's one-use metadata/latest request graph, seals it through the sole application raw
//! store, and routes the graph into exactly one canonical family. Mutual-fund NAV and equity/ETF
//! EOD bars remain separate typed operations, immutable generations, and restart selectors.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_tiingo::{
    TiingoEodBarTimeAuthority, TiingoEodContractEvidence, TiingoEodInstrumentAuthority,
    TiingoFundContext, TiingoFundNavContractEvidence, TiingoLatestEodPublicationOutcome,
    TiingoLatestFundNavPublicationOutcome, TiingoLatestPublicationError,
    TiingoLatestUnavailableReason, TiingoPendingLatestPublication, TiingoSealedLatestPublication,
    TiingoSealedLatestUnavailable,
};
use market_squawk_data::{
    AnalyticalFundNavOutput, AnalyticalFundNavReadRequest, AnalyticalMarketBarOutput,
    AnalyticalMarketBarReadRequest, AnalyticalReadError, CommittedDataset, DatasetId,
    DatasetManifestRef, IngestError, IngestPrecommitAuthority,
    PersistedProviderCaptureBindingEvidence, QueryLimits, extraction_provider_payload_digest,
};
use market_squawk_domain::{EvidenceDigest, SourceId, SourceIdentifier, Timestamp};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    ExtractionRequest, ExtractionRevisionPlan, ProviderCaptureError, ProviderCaptureSealRequest,
    ProviderNativeLineageImplementation, SealedProviderCaptureBinding, SourceMetadata,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::ResearchRightsAuthority;
use crate::{ResearchIngestRequest, ResearchService, ResearchServiceError};

/// Fixed typed operation for exact-manifest Tiingo mutual-fund NAV reads.
pub(crate) const TIINGO_FUND_NAV_POINT_IN_TIME_OPERATION: &str =
    "Research.GetTiingoFundNavPointInTime";

/// Fixed typed operation for exact-manifest Tiingo equity/ETF EOD bar reads.
pub(crate) const TIINGO_EOD_MARKET_BAR_POINT_IN_TIME_OPERATION: &str =
    "Markets.GetTiingoEodMarketBarsPointInTime";

/// Optional Tiingo application composition without a fake enabled-but-unusable state.
#[derive(Debug)]
pub(crate) enum TiingoLatestApplicationState {
    /// The exact source, rights, raw store, and analytical authority are composed.
    Enabled(TiingoLatestApplicationClosure),
    /// Tiingo is intentionally disabled before any provider request is admitted.
    Unavailable(TiingoLatestUnavailableDto),
}

impl TiingoLatestApplicationState {
    /// Constructs the pre-acquisition disabled state. No pending capture is accepted or dropped.
    pub(crate) fn disabled() -> Self {
        Self::Unavailable(TiingoLatestUnavailableDto::disabled())
    }

    /// Enables Tiingo only when source metadata and persistence rights name one current source.
    pub(crate) fn try_enabled(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, TiingoLatestApplicationError> {
        TiingoLatestApplicationClosure::try_new(research, source, rights, source_registered_at)
            .map(Self::Enabled)
    }

    /// Returns the enabled producer-to-consumer closure, when configured.
    pub(crate) const fn enabled(&self) -> Option<&TiingoLatestApplicationClosure> {
        match self {
            Self::Enabled(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }

    /// Returns the exact typed disabled result, when acquisition is not admitted.
    pub(crate) const fn unavailable(&self) -> Option<&TiingoLatestUnavailableDto> {
        match self {
            Self::Unavailable(value) => Some(value),
            Self::Enabled(_) => None,
        }
    }
}

/// Application-owned Tiingo physical sealing and immutable analytical publication boundary.
pub(crate) struct TiingoLatestApplicationClosure {
    research: Arc<ResearchService>,
    source: SourceMetadata,
    rights: ResearchRightsAuthority,
    source_registered_at: Timestamp,
}

impl std::fmt::Debug for TiingoLatestApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TiingoLatestApplicationClosure")
            .field("source_id", self.source.source_id())
            .field("metadata_revision", self.source.revision())
            .field("source_registered_at", &self.source_registered_at)
            .finish_non_exhaustive()
    }
}

impl TiingoLatestApplicationClosure {
    fn try_new(
        research: Arc<ResearchService>,
        source: SourceMetadata,
        rights: ResearchRightsAuthority,
        source_registered_at: Timestamp,
    ) -> Result<Self, TiingoLatestApplicationError> {
        if source.source_id() != rights.source_id() || !source.is_effective_at(source_registered_at)
        {
            return Err(TiingoLatestApplicationError::AuthorityInvalid);
        }
        Ok(Self {
            research,
            source,
            rights,
            source_registered_at,
        })
    }

    /// Seals and publishes one exact Tiingo mutual-fund latest response as canonical FundNav.
    ///
    /// Unsupported metadata and an empty latest response return a sealed typed unavailable result;
    /// they never become a fabricated zero NAV or a MarketBar.
    #[allow(
        clippy::too_many_arguments,
        reason = "canonical NAV identity, clocks, immutable target, and authority remain explicit"
    )]
    pub(crate) async fn seal_and_publish_fund_nav(
        &self,
        pending: TiingoPendingLatestPublication,
        seal_request: ProviderCaptureSealRequest,
        context: TiingoFundContext,
        contract: &TiingoFundNavContractEvidence,
        extraction_request: ExtractionRequest,
        analytical_dataset: DatasetId,
        observed_at: Timestamp,
        ingested_at: Timestamp,
        canonical_published_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<TiingoFundNavApplicationOutcome, TiingoLatestApplicationError> {
        self.validate_before_seal(observed_at, precommit_authority.as_ref())?;
        let sealed = self
            .seal_latest(pending, seal_request, &cancellation, deadline)
            .await?;
        match sealed.try_into_fund_nav(
            context,
            contract,
            extraction_request,
            ingested_at,
            canonical_published_at,
        )? {
            TiingoLatestFundNavPublicationOutcome::Published(publication) => {
                let (revisions, binding) = publication.into_parts();
                let prepared = self
                    .publish_binding(
                        binding,
                        revisions,
                        ProviderNativeLineageImplementation::TiingoFundNavV1,
                        analytical_dataset,
                        observed_at,
                        precommit_authority,
                        cancellation,
                    )
                    .await?;
                if prepared.expected_record_count != 1 {
                    return Err(TiingoLatestApplicationError::FamilyMismatch);
                }
                Ok(TiingoFundNavApplicationOutcome::Published(
                    TiingoFundNavPublicationReceipt {
                        restart: TiingoFundNavRestartSelector {
                            binding: prepared.restart_binding(),
                        },
                        committed: prepared.committed,
                        provider_dataset: prepared.provider_dataset,
                    },
                ))
            }
            TiingoLatestFundNavPublicationOutcome::Unavailable(unavailable) => {
                Ok(TiingoFundNavApplicationOutcome::Unavailable(
                    TiingoLatestUnavailableDto::from_sealed(unavailable),
                ))
            }
        }
    }

    /// Seals and publishes one exact Tiingo equity/ETF latest response as canonical MarketBar.
    ///
    /// Raw and adjusted surfaces remain independent rows. Missing fields cannot be filled from the
    /// other surface; an empty response or all-gap response is returned as typed unavailable.
    #[allow(
        clippy::too_many_arguments,
        reason = "EOD identity, session authority, clocks, immutable target, and authority stay explicit"
    )]
    pub(crate) async fn seal_and_publish_eod(
        &self,
        pending: TiingoPendingLatestPublication,
        seal_request: ProviderCaptureSealRequest,
        instrument: &TiingoEodInstrumentAuthority,
        contract: &TiingoEodContractEvidence,
        bar_time_authority: &dyn TiingoEodBarTimeAuthority,
        extraction_request: ExtractionRequest,
        analytical_dataset: DatasetId,
        observed_at: Timestamp,
        ingested_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<TiingoEodApplicationOutcome, TiingoLatestApplicationError> {
        self.validate_before_seal(observed_at, precommit_authority.as_ref())?;
        let sealed = self
            .seal_latest(pending, seal_request, &cancellation, deadline)
            .await?;
        match sealed.try_into_eod(
            instrument,
            contract,
            bar_time_authority,
            extraction_request,
            ingested_at,
        )? {
            TiingoLatestEodPublicationOutcome::Published(publication) => {
                let (revisions, binding) = publication.into_parts();
                let prepared = self
                    .publish_binding(
                        binding,
                        revisions,
                        ProviderNativeLineageImplementation::TiingoEodMarketBarV1,
                        analytical_dataset,
                        observed_at,
                        precommit_authority,
                        cancellation,
                    )
                    .await?;
                Ok(TiingoEodApplicationOutcome::Published(
                    TiingoEodPublicationReceipt {
                        restart: TiingoEodRestartSelector {
                            binding: prepared.restart_binding(),
                        },
                        committed: prepared.committed,
                        provider_dataset: prepared.provider_dataset,
                    },
                ))
            }
            TiingoLatestEodPublicationOutcome::Unavailable(unavailable) => {
                Ok(TiingoEodApplicationOutcome::Unavailable(
                    TiingoLatestUnavailableDto::from_sealed(unavailable),
                ))
            }
        }
    }

    async fn seal_latest(
        &self,
        pending: TiingoPendingLatestPublication,
        seal_request: ProviderCaptureSealRequest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<TiingoSealedLatestPublication, TiingoLatestApplicationError> {
        let sealed = self
            .research
            .seal_provider_capture(seal_request, cancellation, deadline)
            .await?;
        pending.try_rejoin(sealed).map_err(Into::into)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exclusive binding, revision, family, immutable target, clocks, and precommit authority stay explicit"
    )]
    async fn publish_binding(
        &self,
        binding: SealedProviderCaptureBinding,
        revisions: ExtractionRevisionPlan,
        expected_implementation: ProviderNativeLineageImplementation,
        analytical_dataset: DatasetId,
        observed_at: Timestamp,
        precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<PreparedTiingoPublication, TiingoLatestApplicationError> {
        self.validate_before_seal(observed_at, precommit_authority.as_ref())?;
        binding.validate()?;
        self.validate_capture_binding(&binding)?;
        let native_schema = binding.native_lineage().schema();
        let expected_record_count = binding.batch().records().len();
        if expected_record_count == 0
            || native_schema.implementation() != expected_implementation
            || revisions.len() != expected_record_count
            || !revisions.is_locally_observed()
            || !revisions.native_lineage_required()
        {
            return Err(TiingoLatestApplicationError::FamilyMismatch);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let provider_dataset = binding.capture_evidence().dataset().clone();
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
        let prepared = PreparedTiingoPublication {
            committed,
            binding_digest,
            source_id: self.source.source_id().clone(),
            provider_dataset,
            expected_record_count,
            native_schema_version: native_schema.version(),
            native_schema_fingerprint: native_schema.fingerprint(),
        };
        prepared.verify_persisted_binding(self.research.as_ref())?;
        Ok(prepared)
    }

    fn validate_before_seal(
        &self,
        observed_at: Timestamp,
        precommit_authority: &dyn IngestPrecommitAuthority,
    ) -> Result<(), TiingoLatestApplicationError> {
        if observed_at < self.source_registered_at || !self.source.is_effective_at(observed_at) {
            return Err(TiingoLatestApplicationError::AuthorityInvalid);
        }
        precommit_authority.validate_precommit()?;
        Ok(())
    }

    fn validate_capture_binding(
        &self,
        binding: &SealedProviderCaptureBinding,
    ) -> Result<(), TiingoLatestApplicationError> {
        let capture = binding.capture_evidence();
        if capture.source_id() != self.source.source_id()
            || capture.metadata_revision() != self.source.revision()
        {
            return Err(TiingoLatestApplicationError::AuthorityInvalid);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PreparedTiingoPublication {
    committed: CommittedDataset,
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    expected_record_count: usize,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
}

impl PreparedTiingoPublication {
    fn restart_binding(&self) -> TiingoLatestRestartBinding {
        TiingoLatestRestartBinding {
            manifest: self.committed.manifest().clone(),
            binding_digest: self.binding_digest,
            source_id: self.source_id.clone(),
            expected_record_count: self.expected_record_count,
            native_schema_version: self.native_schema_version,
            native_schema_fingerprint: self.native_schema_fingerprint,
        }
    }

    fn verify_persisted_binding(
        &self,
        research: &ResearchService,
    ) -> Result<(), TiingoLatestApplicationError> {
        self.restart_binding()
            .verify_persisted_binding(research)
            .map(|_evidence| ())
    }
}

/// Exact immutable NAV generation and raw/native selector returned after publication.
#[derive(Debug)]
pub(crate) struct TiingoFundNavPublicationReceipt {
    committed: CommittedDataset,
    restart: TiingoFundNavRestartSelector,
    provider_dataset: SourceIdentifier,
}

impl TiingoFundNavPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &TiingoFundNavRestartSelector {
        &self.restart
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
}

/// Exact immutable EOD generation and raw/native selector returned after publication.
#[derive(Debug)]
pub(crate) struct TiingoEodPublicationReceipt {
    committed: CommittedDataset,
    restart: TiingoEodRestartSelector,
    provider_dataset: SourceIdentifier,
}

impl TiingoEodPublicationReceipt {
    pub(crate) const fn committed(&self) -> &CommittedDataset {
        &self.committed
    }

    pub(crate) const fn restart_selector(&self) -> &TiingoEodRestartSelector {
        &self.restart
    }

    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
}

/// Tiingo mutual-fund NAV publication without cross-family fallback.
#[derive(Debug)]
pub(crate) enum TiingoFundNavApplicationOutcome {
    Published(TiingoFundNavPublicationReceipt),
    Unavailable(TiingoLatestUnavailableDto),
}

/// Tiingo equity/ETF EOD publication without NAV substitution.
#[derive(Debug)]
pub(crate) enum TiingoEodApplicationOutcome {
    Published(TiingoEodPublicationReceipt),
    Unavailable(TiingoLatestUnavailableDto),
}

/// Closed application reason a Tiingo latest operation has no publishable canonical result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TiingoLatestApplicationUnavailableReason {
    Disabled,
    UnsupportedMetadataCoverage,
    EmptyLatestResponse,
    NoCompleteEodSurface,
}

/// Typed unavailable state retaining sealed raw evidence when a request actually completed.
#[derive(Debug)]
pub(crate) struct TiingoLatestUnavailableDto {
    reason: TiingoLatestApplicationUnavailableReason,
    sealed: Option<TiingoSealedLatestUnavailable>,
}

impl TiingoLatestUnavailableDto {
    fn disabled() -> Self {
        Self {
            reason: TiingoLatestApplicationUnavailableReason::Disabled,
            sealed: None,
        }
    }

    fn from_sealed(sealed: TiingoSealedLatestUnavailable) -> Self {
        let reason = match sealed.reason() {
            TiingoLatestUnavailableReason::UnsupportedMetadataCoverage => {
                TiingoLatestApplicationUnavailableReason::UnsupportedMetadataCoverage
            }
            TiingoLatestUnavailableReason::EmptyLatestResponse => {
                TiingoLatestApplicationUnavailableReason::EmptyLatestResponse
            }
            TiingoLatestUnavailableReason::NoCompleteEodSurface => {
                TiingoLatestApplicationUnavailableReason::NoCompleteEodSurface
            }
        };
        Self {
            reason,
            sealed: Some(sealed),
        }
    }

    pub(crate) const fn reason(&self) -> TiingoLatestApplicationUnavailableReason {
        self.reason
    }

    /// Returns exact sealed raw graph evidence only when a provider response existed.
    pub(crate) fn sealed_capture(
        &self,
    ) -> Option<&market_squawk_sources::SealedProviderCaptureSetReceipt> {
        self.sealed
            .as_ref()
            .map(|sealed| sealed.persisted_capture())
    }

    pub(crate) fn returned_rows(&self) -> u32 {
        self.sealed
            .as_ref()
            .map_or(0, TiingoSealedLatestUnavailable::returned_rows)
    }

    pub(crate) fn surface_gaps(&self) -> u32 {
        self.sealed
            .as_ref()
            .map_or(0, TiingoSealedLatestUnavailable::surface_gaps)
    }
}

/// Common exact immutable generation and raw/native evidence hidden behind family-specific reads.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TiingoLatestRestartBinding {
    manifest: DatasetManifestRef,
    binding_digest: EvidenceDigest,
    source_id: SourceId,
    expected_record_count: usize,
    native_schema_version: u16,
    native_schema_fingerprint: EvidenceDigest,
}

impl TiingoLatestRestartBinding {
    const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    fn verify_persisted_binding(
        &self,
        research: &ResearchService,
    ) -> Result<PersistedProviderCaptureBindingEvidence, TiingoLatestApplicationError> {
        let store = research.provider_capture_store();
        let evidence = research.analytical().provider_capture_binding_evidence(
            &self.manifest,
            self.binding_digest,
            store.as_ref(),
        )?;
        if evidence.binding_digest() != self.binding_digest
            || evidence.capture().source_id() != &self.source_id
            || evidence.record_count() != self.expected_record_count
            || evidence.native_lineage().version() != self.native_schema_version
            || evidence.native_lineage().fingerprint() != self.native_schema_fingerprint
        {
            return Err(TiingoLatestApplicationError::RestartInvalid);
        }
        Ok(evidence)
    }
}

/// Exact immutable Tiingo FundNav generation and its sole raw/native restart coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TiingoFundNavRestartSelector {
    binding: TiingoLatestRestartBinding,
}

impl TiingoFundNavRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    /// Reopens the exact immutable FundNav generation, raw claims, and fixed PIT result.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        request: AnalyticalFundNavReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TiingoFundNavRestartReceipt, TiingoLatestApplicationError> {
        if request.manifest() != self.binding.manifest() || self.binding.expected_record_count != 1
        {
            return Err(TiingoLatestApplicationError::RestartInvalid);
        }
        let evidence = self.binding.verify_persisted_binding(research)?;
        let nav = research
            .analytical_reader()
            .read_fund_nav_history(request, limits, deadline, cancellation)
            .await?;
        if nav.source_id() != &self.binding.source_id
            || nav.output().manifest() != self.binding.manifest()
            || nav.observations().len() != self.binding.expected_record_count
        {
            return Err(TiingoLatestApplicationError::RestartInvalid);
        }
        Ok(TiingoFundNavRestartReceipt { evidence, nav })
    }
}

/// Exact immutable Tiingo MarketBar generation and its sole raw/native restart coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TiingoEodRestartSelector {
    binding: TiingoLatestRestartBinding,
}

impl TiingoEodRestartSelector {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        self.binding.manifest()
    }

    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding.binding_digest()
    }

    /// Reopens the exact immutable MarketBar generation, raw claims, and fixed PIT result.
    pub(crate) async fn reopen(
        &self,
        research: &ResearchService,
        request: AnalyticalMarketBarReadRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<TiingoEodRestartReceipt, TiingoLatestApplicationError> {
        if request.manifest() != self.binding.manifest() {
            return Err(TiingoLatestApplicationError::RestartInvalid);
        }
        let evidence = self.binding.verify_persisted_binding(research)?;
        let bars = research
            .analytical_reader()
            .read_market_bars(request, limits, deadline, cancellation)
            .await?;
        if bars.source_id() != &self.binding.source_id
            || bars.output().manifest() != self.binding.manifest()
            || bars.bars().len() != self.binding.expected_record_count
        {
            return Err(TiingoLatestApplicationError::RestartInvalid);
        }
        Ok(TiingoEodRestartReceipt { evidence, bars })
    }
}

/// Exact raw/native and typed FundNav evidence reopened from one immutable manifest.
#[derive(Debug)]
pub(crate) struct TiingoFundNavRestartReceipt {
    evidence: PersistedProviderCaptureBindingEvidence,
    nav: AnalyticalFundNavOutput,
}

impl TiingoFundNavRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.evidence
    }

    pub(crate) const fn nav(&self) -> &AnalyticalFundNavOutput {
        &self.nav
    }
}

/// Exact raw/native and typed MarketBar evidence reopened from one immutable manifest.
#[derive(Debug)]
pub(crate) struct TiingoEodRestartReceipt {
    evidence: PersistedProviderCaptureBindingEvidence,
    bars: AnalyticalMarketBarOutput,
}

impl TiingoEodRestartReceipt {
    pub(crate) const fn evidence(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.evidence
    }

    pub(crate) const fn bars(&self) -> &AnalyticalMarketBarOutput {
        &self.bars
    }
}

/// Failure before or during Tiingo sealing, publication, or exact-manifest PIT restart.
#[derive(Debug, Error)]
pub(crate) enum TiingoLatestApplicationError {
    #[error("Tiingo source or rights authority does not match the exact capture")]
    AuthorityInvalid,
    #[error("Tiingo NAV/EOD family or native-lineage evidence does not match")]
    FamilyMismatch,
    #[error("Tiingo exact restart selector did not reproduce its immutable generation")]
    RestartInvalid,
    #[error("Tiingo adapter rejected latest canonical publication")]
    Adapter(#[from] TiingoLatestPublicationError),
    #[error("Tiingo sealed provider binding is invalid")]
    Capture(#[from] ProviderCaptureError),
    #[error("Tiingo persistence precommit authority or catalog publication failed")]
    Ingest(#[from] IngestError),
    #[error("Tiingo payload-specific persistence rights are unavailable")]
    Rights(#[from] ServiceError),
    #[error("Tiingo application research composition failed")]
    Research(#[from] ResearchServiceError),
    #[error("Tiingo exact-manifest typed read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
}
