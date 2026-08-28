//! Application-owned BEA activation, atomic macro publication, and provider-period reads.
//!
//! BEA acquisition runs only through a registry-minted extraction authority. The adapter reserves
//! the shared durable response-byte and provider-error claims before every send and consumes the
//! exact in-flight permit when each response terminates. This boundary therefore never performs a
//! second settlement or creates a local counter, credential path, raw store, or publication
//! authority.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_bea::{
    BeaDoctorAdmissionEvidence, BeaProviderQuotaDeclaration, BeaPublicationCandidate,
    BeaPublicationError, BeaRequiredSharedSettlement, BeaSource, BeaSourceError,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, DatasetId, IngestError,
    IngestPrecommitAuthority, IngestReservation, PinnedDataset, ProviderMacroPlanChunkInput,
    ProviderMacroPlanPublicationInput, ProviderMacroPlanPublicationReceipt,
    ProviderMacroPlanRestartSelector, ProviderMacroPlanSemantics, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceId, SourceIdentifier, Timestamp};
use market_squawk_sources::{
    ExtractionAuthority, ExtractionSourceError, ProviderNativeLineageImplementation,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation that later Desktop, CLI, and MCP composition must register.
pub(crate) const BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetBeaProviderPeriodLatestKnown";

const BEA_PROVIDER_SEMANTICS_SCHEMA: &str = "bea-regional-provider-semantics-v1";

/// Application-owned BEA physical sealing, publication, and typed-read coordinator.
#[derive(Clone)]
pub(crate) struct BeaMacroApplicationClosure {
    research: Arc<ResearchService>,
}

impl std::fmt::Debug for BeaMacroApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BeaMacroApplicationClosure")
            .field("research", &"[APPLICATION-OWNED RESEARCH SERVICE]")
            .finish()
    }
}

impl BeaMacroApplicationClosure {
    /// Binds BEA to the sole application-owned physical sealer and analytical authority.
    pub(crate) fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    /// Reports protected setup or an invalid code-owned quota contract without network I/O.
    pub(crate) fn acquisition_state(source: Option<&BeaSource>) -> BeaMacroCapabilityState {
        let Some(source) = source else {
            return BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
                kind: BeaSetupRequiredKind::ProtectedCredential,
            });
        };
        if !complete_shared_quota_declaration(source.quota_declaration()) {
            return BeaMacroCapabilityState::Unavailable(BeaUnavailableDto::invalid_quota(
                source.quota_declaration(),
            ));
        }
        BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
            kind: BeaSetupRequiredKind::DoctorActivation,
        })
    }

    /// Acquires, physically seals, rejoins, and activates one protected BEA doctor run.
    ///
    /// The registry-minted extraction authority is the only request path. The BEA adapter uses it
    /// to reserve the worst-case weighted claim before every transport send and consuming-settles
    /// every complete response with exact bytes, error class, and Retry-After evidence. Transport
    /// abandonment charges the reserved maximum through the common authority. No credential
    /// material or independent quota state is retained in the result.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn acquire_seal_and_activate_doctor(
        &self,
        source: &BeaSource,
        authority: &ExtractionAuthority,
        provider_dataset: &SourceIdentifier,
        acquisition_deadline: Timestamp,
        seal_deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaDoctorActivationState, BeaMacroApplicationError> {
        let quota = source.quota_declaration();
        if !complete_shared_quota_declaration(quota) {
            return Ok(BeaDoctorActivationState::Unavailable(
                BeaUnavailableDto::invalid_quota(quota),
            ));
        }
        let doctor = source
            .doctor(
                authority,
                provider_dataset,
                acquisition_deadline,
                cancellation.clone(),
            )
            .await?;
        let doctor_receipt_digest = doctor.receipt().receipt_digest();

        let (pending, seal_request) = doctor.into_sealing_parts()?;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, seal_deadline)
            .await?;
        let admission = Arc::new(pending.try_rejoin(source.source_binding(), sealed)?);
        if admission.quota_declaration_digest() != quota.declaration_digest()
            || admission.doctor_receipt_digest() != doctor_receipt_digest
        {
            return Err(BeaMacroApplicationError::DoctorAuthorityMismatch);
        }
        source.activate_doctor(Arc::clone(&admission))?;
        Ok(BeaDoctorActivationState::Available(
            BeaDoctorActivationDto { admission },
        ))
    }

    /// Publishes one already sealed BEA candidate through the shared atomic macro-plan authority.
    ///
    /// The shared contract admits only the exact BEA native lineage and metadata-first whole
    /// capture shape. Invalid evidence fails closed; no alternate store or partial generation is
    /// created.
    pub(crate) async fn publish_candidate(
        &self,
        candidate: BeaPublicationCandidate,
        persist_reservation: IngestReservation,
        application_precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<BeaMacroPlanPublication, BeaMacroApplicationError> {
        application_precommit_authority.validate_precommit()?;
        let publication_input = try_into_provider_macro_plan_publication_input(candidate)?;
        let pending = self
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(persist_reservation, publication_input)?;
        let receipt = pending
            .commit(
                self.research.analytical(),
                cancellation,
                application_precommit_authority,
            )
            .await?;
        let restart_selector = receipt.restart_selector();
        let reopened = self
            .research
            .analytical()
            .verify_provider_macro_plan_restart(&restart_selector)?;
        if reopened.manifest() != receipt.manifest() {
            return Err(BeaMacroApplicationError::RestartVerificationMismatch);
        }
        Ok(BeaMacroPlanPublication { receipt, reopened })
    }

    /// Reopens one exact generation and performs the fixed provider-period PIT read.
    ///
    /// This boundary accepts only [`ResearchPeriod`]. It never converts a provider period to a
    /// calendar date and never substitutes a latest manifest.
    pub(crate) async fn read_provider_period_latest_known(
        &self,
        request: BeaProviderPeriodLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaMacroCapabilityState, BeaMacroApplicationError> {
        let BeaProviderPeriodLatestKnownRequest {
            restart_selector,
            analytical,
        } = request;
        let reopened = self
            .research
            .analytical()
            .verify_provider_macro_plan_restart(&restart_selector)?;
        if reopened.manifest() != restart_selector.manifest()
            || analytical.manifest() != restart_selector.manifest()
            || analytical.source_series().source_id() != restart_selector.source_id()
        {
            return Err(BeaMacroApplicationError::RestartVerificationMismatch);
        }
        let output = self
            .research
            .analytical_reader()
            .read_macro_provider_period_latest_known_snapshot(
                analytical,
                limits,
                deadline,
                cancellation,
            )
            .await?;
        if output.source_id() != restart_selector.source_id()
            || output.output().manifest() != restart_selector.manifest()
        {
            return Err(BeaMacroApplicationError::InvalidReadResult);
        }
        Ok(BeaMacroCapabilityState::Available(
            BeaProviderPeriodLatestKnownDto {
                restart_selector,
                reopened,
                output,
            },
        ))
    }
}

fn complete_shared_quota_declaration(declaration: &BeaProviderQuotaDeclaration) -> bool {
    let requirements = declaration.required_shared_settlements();
    requirements.as_slice()
        == [
            BeaRequiredSharedSettlement::ResponseBytes,
            BeaRequiredSharedSettlement::ProviderErrors,
        ]
        && declaration.shared_declaration().validate().is_ok()
}

/// Consumes one BEA canonical/native/raw handoff into the shared atomic macro-plan input.
fn try_into_provider_macro_plan_publication_input(
    candidate: BeaPublicationCandidate,
) -> Result<ProviderMacroPlanPublicationInput, IngestError> {
    candidate
        .validate()
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let coordinates = candidate.rejoin_coordinates().clone();
    let completion_digest = candidate.candidate_digest();
    let expected_total_rows = coordinates.row_count();
    let analytical_dataset = DatasetId::try_from(coordinates.analytical_dataset_id().as_str())
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let (_, revisions, sealed_capture) = candidate.into_shared_publication_parts().into_parts();
    let native_lineage = sealed_capture.native_lineage();
    if native_lineage.schema().implementation()
        != ProviderNativeLineageImplementation::BeaRegionalV1
    {
        return Err(IngestError::InvalidProviderMacroPlan);
    }
    let sidecar = native_lineage
        .batch_sidecar()
        .ok_or(IngestError::InvalidProviderMacroPlan)?;
    let semantics_schema = SourceIdentifier::try_from(BEA_PROVIDER_SEMANTICS_SCHEMA)
        .map_err(|_error| IngestError::InvalidProviderMacroPlan)?;
    let semantics = ProviderMacroPlanSemantics::try_new(
        semantics_schema,
        native_lineage.schema().fingerprint(),
        sidecar.semantic_payload_digest(),
        sidecar.semantic_payload().to_vec().into_boxed_slice(),
    )?;
    let chunk = ProviderMacroPlanChunkInput::try_new(
        0,
        1,
        coordinates.candidate_digest(),
        coordinates.source_binding_digest(),
        semantics,
        sealed_capture,
        revisions,
    )?;
    ProviderMacroPlanPublicationInput::try_new(
        analytical_dataset,
        completion_digest,
        expected_total_rows,
        vec![chunk],
    )
}

/// Protected doctor result after shared settlement, physical sealing, and process activation.
#[derive(Debug)]
pub(crate) enum BeaDoctorActivationState {
    /// The exact doctor admission is active without retaining its protected credential.
    Available(BeaDoctorActivationDto),
    /// The code-owned shared weighted declaration is invalid.
    Unavailable(BeaUnavailableDto),
}

/// Bounded non-secret doctor activation evidence.
#[derive(Debug)]
pub(crate) struct BeaDoctorActivationDto {
    admission: Arc<BeaDoctorAdmissionEvidence>,
}

impl BeaDoctorActivationDto {
    /// Returns the exact sealed process activation used by subsequent adapter operations.
    pub(crate) fn admission(&self) -> &Arc<BeaDoctorAdmissionEvidence> {
        &self.admission
    }

    /// Returns the complete request/byte/error policy identity that was settled.
    pub(crate) fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.admission.quota_declaration_digest()
    }

    /// Returns the exact successful page/byte receipt bound to consuming shared settlements.
    pub(crate) fn doctor_receipt_digest(&self) -> EvidenceDigest {
        self.admission.doctor_receipt_digest()
    }
}

/// Exact immutable BEA macro-plan generation proven readable after commit.
#[derive(Debug)]
pub(crate) struct BeaMacroPlanPublication {
    receipt: ProviderMacroPlanPublicationReceipt,
    reopened: PinnedDataset,
}

impl BeaMacroPlanPublication {
    /// Returns the atomic publication receipt.
    pub(crate) const fn receipt(&self) -> &ProviderMacroPlanPublicationReceipt {
        &self.receipt
    }

    /// Returns the exact immutable generation reopened immediately after commit.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Reconstructs the only selector accepted by restart and typed PIT reads.
    pub(crate) fn restart_selector(&self) -> ProviderMacroPlanRestartSelector {
        self.receipt.restart_selector()
    }
}

/// Exact generation-bound request for the fixed BEA provider-period operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BeaProviderPeriodLatestKnownRequest {
    restart_selector: ProviderMacroPlanRestartSelector,
    analytical: AnalyticalMacroProviderPeriodLatestKnownRequest,
}

impl BeaProviderPeriodLatestKnownRequest {
    /// Pins a source-qualified allowlist and exact provider-period cutoffs to one generation.
    pub(crate) fn try_new(
        restart_selector: ProviderMacroPlanRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
    ) -> Result<Self, BeaMacroApplicationError> {
        let source_series = AnalyticalMacroSourceQualifiedSeries::new(
            restart_selector.source_id().clone(),
            series_allowlist,
        );
        let analytical = AnalyticalMacroProviderPeriodLatestKnownRequest::try_new(
            restart_selector.manifest().clone(),
            source_series,
            knowledge_cutoff,
            effective_period_cutoff,
        )?;
        Ok(Self {
            restart_selector,
            analytical,
        })
    }

    /// Returns the fixed application operation identity.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact immutable selector retained by this request.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the fixed typed analytical request; no SQL or physical path is exposed.
    pub(crate) const fn analytical_request(
        &self,
    ) -> &AnalyticalMacroProviderPeriodLatestKnownRequest {
        &self.analytical
    }

    /// Returns the minimum bounded row envelope needed to retain tied revisions.
    pub(crate) fn required_query_rows(&self) -> u64 {
        self.analytical.required_query_rows()
    }
}

/// Fixed BEA operation state suitable for later Desktop, CLI, and MCP registration.
#[derive(Debug)]
pub(crate) enum BeaMacroCapabilityState {
    /// An exact generation-bound provider-period read completed.
    Available(BeaProviderPeriodLatestKnownDto),
    /// Protected source construction or doctor activation is still required.
    SetupRequired(BeaSetupRequiredDto),
    /// A shared authority or exact immutable generation is unavailable.
    Unavailable(BeaUnavailableDto),
}

impl BeaMacroCapabilityState {
    /// Returns the successful typed output, when available.
    pub(crate) const fn available(&self) -> Option<&BeaProviderPeriodLatestKnownDto> {
        match self {
            Self::Available(value) => Some(value),
            Self::SetupRequired(_) | Self::Unavailable(_) => None,
        }
    }

    /// Returns the bounded setup requirement, when user/setup action is needed.
    pub(crate) const fn setup_required(&self) -> Option<&BeaSetupRequiredDto> {
        match self {
            Self::SetupRequired(value) => Some(value),
            Self::Available(_) | Self::Unavailable(_) => None,
        }
    }

    /// Returns the explicit infrastructure/data blocker, when unavailable.
    pub(crate) const fn unavailable(&self) -> Option<&BeaUnavailableDto> {
        match self {
            Self::Unavailable(value) => Some(value),
            Self::Available(_) | Self::SetupRequired(_) => None,
        }
    }
}

/// Closed user-actionable setup requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BeaSetupRequiredDto {
    kind: BeaSetupRequiredKind,
}

impl BeaSetupRequiredDto {
    /// Returns the exact missing setup step without exposing credential values.
    pub(crate) const fn kind(&self) -> BeaSetupRequiredKind {
        self.kind
    }
}

/// Closed BEA setup states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaSetupRequiredKind {
    /// A protected BEA credential/source instance has not been activated.
    ProtectedCredential,
    /// The configured source has not completed a current sealed doctor activation.
    DoctorActivation,
}

/// Bounded non-user-actionable unavailable result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BeaUnavailableDto {
    reason: BeaUnavailableReason,
    quota_declaration_digest: Option<EvidenceDigest>,
}

impl BeaUnavailableDto {
    fn invalid_quota(declaration: &BeaProviderQuotaDeclaration) -> Self {
        Self {
            reason: BeaUnavailableReason::InvalidQuotaDeclaration,
            quota_declaration_digest: Some(declaration.declaration_digest()),
        }
    }

    /// Returns the exact closed blocker.
    pub(crate) const fn reason(&self) -> BeaUnavailableReason {
        self.reason
    }

    /// Returns the BEA declaration identity when its shared weighted contract is invalid.
    pub(crate) const fn quota_declaration_digest(&self) -> Option<EvidenceDigest> {
        self.quota_declaration_digest
    }
}

/// Closed reasons the fixed BEA operation cannot currently run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaUnavailableReason {
    /// The adapter declaration does not retain both mandatory BEA settlement dimensions.
    InvalidQuotaDeclaration,
    /// No exact restart selector is available for the fixed read.
    ManifestRequired,
}

/// Typed successful result for the fixed BEA provider-period operation.
#[derive(Debug)]
pub(crate) struct BeaProviderPeriodLatestKnownDto {
    restart_selector: ProviderMacroPlanRestartSelector,
    reopened: PinnedDataset,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
}

impl BeaProviderPeriodLatestKnownDto {
    /// Returns the fixed application operation that produced this DTO.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact selector revalidated immediately before the read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the exact immutable generation reopened immediately before the read.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Returns exact provider-period Macro rows with revision and clock evidence.
    pub(crate) const fn output(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        &self.output
    }

    /// Returns the sole source-rights owner for the fixed selection.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.output.source_id()
    }
}

/// Failure before or during BEA physical sealing, atomic publication, or exact PIT selection.
#[derive(Debug, Error)]
pub(crate) enum BeaMacroApplicationError {
    /// The BEA adapter rejected source, activation, or canonical handoff evidence.
    #[error("BEA adapter rejected application composition")]
    Adapter(#[from] BeaSourceError),
    /// The BEA doctor could not rejoin its exact physical seal.
    #[error("BEA doctor physical seal could not be rejoined")]
    Doctor(#[from] market_squawk_adapter_bea::BeaDoctorError),
    /// The BEA canonical publication candidate is invalid.
    #[error("BEA canonical publication candidate is invalid")]
    Publication(#[from] BeaPublicationError),
    /// Registry-authorized BEA acquisition or shared weighted response settlement failed.
    #[error("BEA registry-authorized acquisition failed")]
    Extraction(#[from] ExtractionSourceError),
    /// The application-owned research service rejected physical capture sealing.
    #[error("BEA application-owned physical capture sealing failed")]
    ResearchService(#[from] ResearchServiceError),
    /// Shared atomic publication, reservation, commit, or restart verification failed.
    #[error("BEA atomic macro publication failed")]
    Ingest(#[from] IngestError),
    /// The exact provider-period analytical capability rejected the bounded read.
    #[error("BEA provider-period analytical read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
    /// Exact publication evidence did not reopen the same immutable generation.
    #[error("BEA exact restart verification changed generation identity")]
    RestartVerificationMismatch,
    /// Doctor sealing changed quota or successful-response receipt identity.
    #[error("BEA doctor activation changed shared authority evidence")]
    DoctorAuthorityMismatch,
    /// The typed read did not retain the exact source and immutable generation.
    #[error("BEA provider-period read returned invalid binding evidence")]
    InvalidReadResult,
}
