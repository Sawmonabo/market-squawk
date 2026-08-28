//! Application-owned BEA activation, atomic macro publication, and provider-period reads.
//!
//! BEA declares response-byte and provider/body-error budgets in addition to the shared request
//! windows. The current shared provider-rate authority cannot settle those post-response
//! dimensions. This boundary therefore refuses acquisition before network I/O unless an injected
//! shared authority proves and performs both settlements. It never creates a second counter,
//! credential path, raw store, or publication authority.

use std::{sync::Arc, time::Instant};

use market_squawk_adapter_bea::{
    BeaDoctorAdmissionEvidence, BeaDoctorRun, BeaProviderQuotaDeclaration, BeaPublicationCandidate,
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
use market_squawk_sources::ProviderNativeLineageImplementation;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation that later Desktop, CLI, and MCP composition must register.
pub(crate) const BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetBeaProviderPeriodLatestKnown";

const BEA_PROVIDER_SEMANTICS_SCHEMA: &str = "bea-regional-provider-semantics-v1";

/// Shared durable settlement seam required before any BEA response-producing operation.
///
/// An implementation must be backed by the product-wide provider-rate authority. It must not be
/// adapter-local or process-only. The preflight covers both successful and failed responses; the
/// settlement method atomically charges the exact successful response evidence before it can
/// authorize activation.
pub(crate) trait BeaSharedQuotaSettlementAuthority: std::fmt::Debug + Send + Sync {
    /// Proves that the exact BEA declaration and both post-response dimensions are durable.
    fn validate_complete_declaration(
        &self,
        declaration: &BeaProviderQuotaDeclaration,
    ) -> Result<(), BeaSharedQuotaSettlementFailure>;

    /// Settles one successful bounded operation into the shared durable byte/error windows.
    fn settle_success(
        &self,
        declaration: &BeaProviderQuotaDeclaration,
        request_count: u32,
        response_bytes: u64,
    ) -> Result<EvidenceDigest, BeaSharedQuotaSettlementFailure>;
}

/// Closed shared-rate failure retained as an unavailable product state, not an adapter retry.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BeaSharedQuotaSettlementFailure {
    /// The shared authority does not durably settle every required BEA response dimension.
    #[error("shared BEA response-byte and provider-error settlement is unavailable")]
    Unsupported,
    /// The shared authority is stale, unavailable, or could not durably commit settlement.
    #[error("shared BEA quota settlement authority is unavailable")]
    AuthorityUnavailable,
    /// The operation evidence does not match the registered declaration or durable window.
    #[error("shared BEA quota settlement evidence does not match")]
    EvidenceMismatch,
}

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

    /// Reports setup or the exact shared-authority blocker without performing network I/O.
    pub(crate) fn acquisition_state(
        source: Option<&BeaSource>,
        settlement: Option<&dyn BeaSharedQuotaSettlementAuthority>,
    ) -> BeaMacroCapabilityState {
        let Some(source) = source else {
            return BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
                kind: BeaSetupRequiredKind::ProtectedCredential,
            });
        };
        let Some(settlement) = settlement else {
            return BeaMacroCapabilityState::Unavailable(BeaUnavailableDto::shared_quota(
                source.quota_declaration(),
                BeaSharedQuotaSettlementFailure::Unsupported,
            ));
        };
        match settlement.validate_complete_declaration(source.quota_declaration()) {
            Ok(()) => BeaMacroCapabilityState::SetupRequired(BeaSetupRequiredDto {
                kind: BeaSetupRequiredKind::DoctorActivation,
            }),
            Err(reason) => BeaMacroCapabilityState::Unavailable(BeaUnavailableDto::shared_quota(
                source.quota_declaration(),
                reason,
            )),
        }
    }

    /// Settles, physically seals, rejoins, and activates one successful protected doctor run.
    ///
    /// The caller must route provider failures through the same shared settlement authority. A
    /// missing or incomplete authority returns `Unavailable` before this method mutates source
    /// activation. No credential material is retained in the result.
    pub(crate) async fn settle_seal_and_activate_doctor(
        &self,
        source: &BeaSource,
        doctor: BeaDoctorRun,
        settlement: Option<&dyn BeaSharedQuotaSettlementAuthority>,
        seal_deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BeaDoctorActivationState, BeaMacroApplicationError> {
        let quota = source.quota_declaration();
        let Some(settlement) = settlement else {
            return Ok(BeaDoctorActivationState::Unavailable(
                BeaUnavailableDto::shared_quota(
                    quota,
                    BeaSharedQuotaSettlementFailure::Unsupported,
                ),
            ));
        };
        if let Err(reason) = settlement.validate_complete_declaration(quota) {
            return Ok(BeaDoctorActivationState::Unavailable(
                BeaUnavailableDto::shared_quota(quota, reason),
            ));
        }
        let settlement_digest = match settlement.settle_success(
            quota,
            doctor.receipt().request_count(),
            doctor.receipt().total_response_bytes(),
        ) {
            Ok(digest) if digest.bytes() != [0; 32] => digest,
            Ok(_) => {
                return Ok(BeaDoctorActivationState::Unavailable(
                    BeaUnavailableDto::shared_quota(
                        quota,
                        BeaSharedQuotaSettlementFailure::EvidenceMismatch,
                    ),
                ));
            }
            Err(reason) => {
                return Ok(BeaDoctorActivationState::Unavailable(
                    BeaUnavailableDto::shared_quota(quota, reason),
                ));
            }
        };

        let (pending, seal_request) = doctor.into_sealing_parts()?;
        let sealed = self
            .research
            .seal_provider_capture(seal_request, &cancellation, seal_deadline)
            .await?;
        let admission = Arc::new(pending.try_rejoin(source.source_binding(), sealed)?);
        source.activate_doctor(Arc::clone(&admission))?;
        Ok(BeaDoctorActivationState::Available(
            BeaDoctorActivationDto {
                admission,
                quota_declaration_digest: quota.declaration_digest(),
                settlement_digest,
            },
        ))
    }

    /// Publishes one already sealed BEA candidate through the shared atomic macro-plan authority.
    ///
    /// The shared input currently rejects BEA native lineage and its metadata-first whole capture
    /// shape. That exact rejection is retained as a typed unavailable state; no alternate store or
    /// partial generation is created. Once the shared contract admits the handoff, this same path
    /// performs Persist/precommit, atomic commit, and exact restart verification.
    pub(crate) async fn publish_candidate(
        &self,
        candidate: BeaPublicationCandidate,
        persist_reservation: IngestReservation,
        application_precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<BeaCandidatePublicationState, BeaMacroApplicationError> {
        application_precommit_authority.validate_precommit()?;
        let publication_input = match try_into_provider_macro_plan_publication_input(candidate) {
            Ok(input) => input,
            Err(IngestError::InvalidProviderMacroPlan) => {
                return Ok(BeaCandidatePublicationState::Unavailable(
                    BeaUnavailableDto::shared_macro_publication(),
                ));
            }
            Err(error) => return Err(error.into()),
        };
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
        Ok(BeaCandidatePublicationState::Published(
            BeaMacroPlanPublication { receipt, reopened },
        ))
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
    /// Shared durable quota authority could not authorize activation.
    Unavailable(BeaUnavailableDto),
}

/// Bounded non-secret doctor activation evidence.
#[derive(Debug)]
pub(crate) struct BeaDoctorActivationDto {
    admission: Arc<BeaDoctorAdmissionEvidence>,
    quota_declaration_digest: EvidenceDigest,
    settlement_digest: EvidenceDigest,
}

impl BeaDoctorActivationDto {
    /// Returns the exact sealed process activation used by subsequent adapter operations.
    pub(crate) fn admission(&self) -> &Arc<BeaDoctorAdmissionEvidence> {
        &self.admission
    }

    /// Returns the complete request/byte/error policy identity that was settled.
    pub(crate) const fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.quota_declaration_digest
    }

    /// Returns the shared durable settlement receipt without exposing a rate mutation API.
    pub(crate) const fn settlement_digest(&self) -> EvidenceDigest {
        self.settlement_digest
    }
}

/// BEA candidate publication result; unavailable never represents an empty generation.
#[derive(Debug)]
pub(crate) enum BeaCandidatePublicationState {
    /// One atomic immutable generation passed exact restart verification.
    Published(BeaMacroPlanPublication),
    /// The shared macro authority does not yet admit the exact BEA handoff.
    Unavailable(BeaUnavailableDto),
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
    fn shared_quota(
        declaration: &BeaProviderQuotaDeclaration,
        failure: BeaSharedQuotaSettlementFailure,
    ) -> Self {
        let requirements = declaration.required_shared_settlements();
        let complete_requirement = requirements
            .contains(&BeaRequiredSharedSettlement::ResponseBytes)
            && requirements.contains(&BeaRequiredSharedSettlement::ProviderErrors);
        let reason = if complete_requirement {
            BeaUnavailableReason::SharedQuotaSettlement(failure)
        } else {
            BeaUnavailableReason::InvalidQuotaDeclaration
        };
        Self {
            reason,
            quota_declaration_digest: Some(declaration.declaration_digest()),
        }
    }

    fn shared_macro_publication() -> Self {
        Self {
            reason: BeaUnavailableReason::SharedMacroPublicationContract,
            quota_declaration_digest: None,
        }
    }

    /// Returns the exact closed blocker.
    pub(crate) const fn reason(&self) -> BeaUnavailableReason {
        self.reason
    }

    /// Returns the BEA declaration identity when quota settlement caused unavailability.
    pub(crate) const fn quota_declaration_digest(&self) -> Option<EvidenceDigest> {
        self.quota_declaration_digest
    }
}

/// Closed reasons the fixed BEA operation cannot currently run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BeaUnavailableReason {
    /// Shared durable response-byte/provider-error settlement is incomplete or unavailable.
    SharedQuotaSettlement(BeaSharedQuotaSettlementFailure),
    /// The adapter declaration does not retain both mandatory BEA settlement dimensions.
    InvalidQuotaDeclaration,
    /// Shared atomic macro publication does not yet admit BEA lineage/capture topology.
    SharedMacroPublicationContract,
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
    /// The typed read did not retain the exact source and immutable generation.
    #[error("BEA provider-period read returned invalid binding evidence")]
    InvalidReadResult,
}
