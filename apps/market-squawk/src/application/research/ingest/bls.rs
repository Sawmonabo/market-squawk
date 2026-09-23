//! Application-owned BLS sealing, atomic publication, and provider-period reads.
//!
//! The adapter closes one bounded request plan and retains BLS-native semantics. This leaf keeps
//! [`ResearchService`] as the sole physical sealer, converts the complete one-use handoff into the
//! shared atomic macro-plan input, and exposes only exact-manifest provider-period reads. It does
//! not register a Desktop operation or claim that Desktop composition is complete.

use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use market_squawk_adapter_bls::{BlsCompletePublicationPlanHandoff, BlsSource, BlsSourceError};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, DatasetId, IngestError,
    IngestPrecommitAuthority, IngestReservation, PinnedDataset, ProviderMacroPlanChunkInput,
    ProviderMacroPlanPublicationInput, ProviderMacroPlanPublicationReceipt,
    ProviderMacroPlanRestartSelector, ProviderMacroPlanSemantics, QueryLimits,
};
use market_squawk_domain::{EvidenceDigest, ResearchPeriod, SourceId, Timestamp};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionAuthority, ExtractionError, ExtractionRequest,
    ExtractionSourceError,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{ResearchService, ResearchServiceError};

/// Fixed typed operation that shared application/CLI/MCP/Desktop dispatch must register.
pub(crate) const BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION: &str =
    "Macro.GetBlsProviderPeriodLatestKnown";

/// Bounded application limits applied to every chunk in one BLS request plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BlsSealFirstExtractionLimits {
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
}

impl BlsSealFirstExtractionLimits {
    /// Retains explicit record and deep-byte ceilings for every deterministic request chunk.
    pub(crate) const fn new(max_records: NonZeroU32, max_bytes: NonZeroU64) -> Self {
        Self {
            max_records,
            max_bytes,
        }
    }
}

/// Application-owned physical sealer and atomic BLS publication/read coordinator.
#[derive(Clone)]
pub(crate) struct BlsMacroApplicationClosure {
    research: Arc<ResearchService>,
}

impl std::fmt::Debug for BlsMacroApplicationClosure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlsMacroApplicationClosure")
            .field("research", &"[APPLICATION-OWNED RESEARCH SERVICE]")
            .finish()
    }
}

impl BlsMacroApplicationClosure {
    /// Binds BLS completion to the sole application-owned sealed journal and analytical writer.
    pub(crate) fn new(research: Arc<ResearchService>) -> Self {
        Self { research }
    }

    /// Acquires, physically seals, extracts, and closes every chunk in one exact BLS plan.
    ///
    /// The doctor response and discovery graph are sealed before they authorize their next
    /// operation. Extraction reuses the sealed discovery evidence and makes no second provider
    /// request. The result remains non-publishing until consumed by
    /// [`BlsWholePlanApplicationHandoff::try_prepare`].
    #[allow(
        clippy::too_many_arguments,
        reason = "provider authority, independent deadlines, bounds, and cancellation stay explicit"
    )]
    pub(crate) async fn acquire_complete_plan(
        &self,
        source: &BlsSource,
        authority: ExtractionAuthority,
        doctor_deadline: Timestamp,
        discovery: DiscoveryRequest,
        limits: BlsSealFirstExtractionLimits,
        seal_deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BlsWholePlanApplicationHandoff, BlsMacroApplicationError> {
        let doctor = source
            .doctor(authority.clone(), doctor_deadline, cancellation.clone())
            .await?;
        let (pending_doctor, doctor_seal) = doctor.into_sealing_parts();
        // The provider response is already official evidence. Preserve it under the physical-seal
        // deadline even when caller cancellation races completion, then recheck caller authority
        // before the sealed response can activate another provider transition.
        let raw_seal = CancellationToken::new();
        let sealed_doctor = self
            .research
            .seal_provider_capture(doctor_seal, &raw_seal, seal_deadline)
            .await?;
        ensure_not_cancelled(&cancellation)?;
        let activation = source.activation_candidate(pending_doctor, sealed_doctor)?;

        let extraction_deadline = discovery.deadline();
        let discovered = source
            .discover_with_activation(
                authority.clone(),
                discovery,
                &activation,
                cancellation.clone(),
            )
            .await?;
        let (pending_discovery, discovery_seal) = discovered.into_sealing_parts()?;
        let raw_seal = CancellationToken::new();
        let sealed_discovery = self
            .research
            .seal_provider_capture(discovery_seal, &raw_seal, seal_deadline)
            .await?;
        ensure_not_cancelled(&cancellation)?;
        let admissions = source
            .admit_sealed_discovery(pending_discovery, sealed_discovery, &activation)?
            .into_objects();

        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(admissions.len())
            .map_err(|_error| BlsMacroApplicationError::Capacity)?;
        for admission in admissions.into_vec() {
            let request = ExtractionRequest::try_new(
                admission.object().clone(),
                limits.max_records,
                limits.max_bytes,
                extraction_deadline,
            )?;
            let output = source
                .extract_sealed_discovery(
                    authority.clone(),
                    request,
                    admission,
                    &activation,
                    cancellation.clone(),
                )
                .await?;
            let candidate = source.publication_candidate(output, &activation)?;
            source.validate_publication_candidate(&candidate, &activation)?;
            candidates.push(candidate);
        }

        BlsCompletePublicationPlanHandoff::try_new(candidates)
            .map(BlsWholePlanApplicationHandoff::new)
            .map_err(Into::into)
    }

    /// Consumes one prepared complete plan through commit and exact restart verification.
    ///
    /// The caller supplies the persist reservation whose payload must equal the input's computed
    /// publication digest and the application precommit authority that must remain valid through
    /// the final catalog commit. No per-chunk generation can escape this operation.
    pub(crate) async fn commit_prepared_plan(
        &self,
        prepared: BlsPreparedMacroPlan,
        persist_reservation: IngestReservation,
        application_precommit_authority: Arc<dyn IngestPrecommitAuthority>,
        cancellation: CancellationToken,
    ) -> Result<BlsMacroPlanPublication, BlsMacroApplicationError> {
        application_precommit_authority.validate_precommit()?;
        let BlsPreparedMacroPlan { input, .. } = prepared;
        let pending = self
            .research
            .analytical()
            .prepare_provider_macro_plan_publication(persist_reservation, input)?;
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
            return Err(BlsMacroApplicationError::RestartVerificationMismatch);
        }
        Ok(BlsMacroPlanPublication { receipt, reopened })
    }

    /// Revalidates the exact plan generation and performs the fixed provider-period PIT read.
    ///
    /// `Available` is returned only after the atomic generation has been reopened through its
    /// durable plan selector and the typed latest-known read succeeds. Calendar dates are not
    /// accepted or derived by this boundary.
    pub(crate) async fn read_provider_period_latest_known(
        &self,
        request: BlsProviderPeriodLatestKnownRequest,
        limits: QueryLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<BlsMacroCapabilityState, BlsMacroApplicationError> {
        let BlsProviderPeriodLatestKnownRequest {
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
            return Err(BlsMacroApplicationError::RestartVerificationMismatch);
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
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        Ok(BlsMacroCapabilityState::Available(
            BlsProviderPeriodLatestKnownDto {
                restart_selector,
                reopened,
                output,
            },
        ))
    }
}

/// One-use application handoff for the adapter's complete ordered BLS request plan.
#[derive(Debug)]
pub(crate) struct BlsWholePlanApplicationHandoff {
    handoff: BlsCompletePublicationPlanHandoff,
}

impl BlsWholePlanApplicationHandoff {
    fn new(handoff: BlsCompletePublicationPlanHandoff) -> Self {
        Self { handoff }
    }

    /// Consumes the complete application handoff into a reservation-ready non-cloneable plan.
    pub(crate) fn try_prepare(self) -> Result<BlsPreparedMacroPlan, BlsMacroApplicationError> {
        BlsPreparedMacroPlan::try_from_complete_handoff(self.handoff)
    }
}

/// Reservation-ready BLS plan that owns the exact non-cloneable shared publication input.
///
/// The exposed fields are the checked values needed to construct the matching persist reservation;
/// provider serialization, closure validation, and publication-digest derivation occur once inside
/// the shared input.
#[derive(Debug)]
pub(crate) struct BlsPreparedMacroPlan {
    analytical_dataset: DatasetId,
    input: ProviderMacroPlanPublicationInput,
}

impl BlsPreparedMacroPlan {
    /// Consumes a direct adapter handoff into the same reservation-ready application boundary.
    pub(crate) fn try_from_complete_handoff(
        handoff: BlsCompletePublicationPlanHandoff,
    ) -> Result<Self, BlsMacroApplicationError> {
        let analytical_dataset = DatasetId::try_from(handoff.analytical_dataset().as_str())
            .map_err(|_error| BlsMacroApplicationError::InvalidAnalyticalDataset)?;
        let completion_digest = handoff.completion_digest();
        let expected_total_rows = handoff.canonical_record_count();
        let expected_total_chunks = handoff.total_chunks();
        let mut chunks = Vec::new();
        chunks
            .try_reserve_exact(usize::from(expected_total_chunks))
            .map_err(|_error| BlsMacroApplicationError::Capacity)?;

        for candidate in handoff.into_candidates().into_vec() {
            let chunk_index = candidate.chunk_index();
            let total_chunks = candidate.total_chunks();
            let candidate_digest = candidate.candidate_digest();
            let source_generation_digest = candidate.source_generation_digest();
            let revisions = candidate.revision_plan()?;
            let (provider_semantics, sealed_capture) = candidate.into_root_publication_parts();
            let semantics = ProviderMacroPlanSemantics::try_new(
                provider_semantics
                    .schema_requirement()
                    .provider_semantics_schema()
                    .clone(),
                provider_semantics.schema_requirement().requirement_digest(),
                provider_semantics.semantics_digest(),
                serde_json::to_vec(&provider_semantics)
                    .map_err(|_error| BlsMacroApplicationError::ProviderSemanticsEncoding)?
                    .into_boxed_slice(),
            )?;
            chunks.push(ProviderMacroPlanChunkInput::try_new(
                chunk_index,
                total_chunks,
                candidate_digest,
                source_generation_digest,
                semantics,
                sealed_capture,
                revisions,
            )?);
        }

        let input = ProviderMacroPlanPublicationInput::try_new(
            analytical_dataset.clone(),
            completion_digest,
            expected_total_rows,
            chunks,
        )?;
        Ok(Self {
            analytical_dataset,
            input,
        })
    }

    /// Returns the exact checked digest the persist reservation must bind as its payload.
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.input.publication_digest()
    }

    /// Returns the sole checked source-rights namespace the persist reservation must bind.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.input.source_id()
    }

    /// Returns the exact analytical dataset that the atomic generation will append.
    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }

    /// Returns the exact checked canonical row count that must commit atomically.
    pub(crate) const fn total_rows(&self) -> u64 {
        self.input.total_rows()
    }
}

/// Exact immutable BLS macro-plan generation proven readable after commit.
#[derive(Debug)]
pub(crate) struct BlsMacroPlanPublication {
    receipt: ProviderMacroPlanPublicationReceipt,
    reopened: PinnedDataset,
}

impl BlsMacroPlanPublication {
    /// Returns the all-or-nothing publication and durable catalog receipt.
    pub(crate) const fn receipt(&self) -> &ProviderMacroPlanPublicationReceipt {
        &self.receipt
    }

    /// Returns the exact immutable generation reopened immediately after commit.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Reconstructs the exact selector needed by restart and provider-period reads.
    pub(crate) fn restart_selector(&self) -> ProviderMacroPlanRestartSelector {
        self.receipt.restart_selector()
    }
}

/// Exact generation-bound request for the fixed BLS provider-period operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BlsProviderPeriodLatestKnownRequest {
    restart_selector: ProviderMacroPlanRestartSelector,
    analytical: AnalyticalMacroProviderPeriodLatestKnownRequest,
}

impl BlsProviderPeriodLatestKnownRequest {
    /// Pins source-qualified series and provider-period cutoffs to one durable plan selector.
    ///
    /// This constructor is activation-independent: startup recovery may rebuild the typed read
    /// directly from catalog-retained selector coordinates without a live registered credential.
    pub(crate) fn try_new(
        restart_selector: ProviderMacroPlanRestartSelector,
        series_allowlist: AnalyticalMacroSeriesAllowlist,
        knowledge_cutoff: Timestamp,
        effective_period_cutoff: ResearchPeriod,
    ) -> Result<Self, BlsMacroApplicationError> {
        if restart_selector.total_chunks() == 0
            || restart_selector.total_rows() == 0
            || restart_selector.completion_digest().bytes() == [0; 32]
            || restart_selector.publication_digest().bytes() == [0; 32]
            || restart_selector.catalog_receipt_digest().bytes() == [0; 32]
            || restart_selector.source_generation_digest().bytes() == [0; 32]
        {
            return Err(BlsMacroApplicationError::RestartVerificationMismatch);
        }
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

    /// Returns the fixed typed application operation identity.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact immutable plan selector retained by this read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the fixed shared typed read request without exposing raw SQL or physical paths.
    pub(crate) const fn analytical_request(
        &self,
    ) -> &AnalyticalMacroProviderPeriodLatestKnownRequest {
        &self.analytical
    }

    /// Returns the minimum query row envelope needed for the complete tied candidate set.
    pub(crate) fn required_query_rows(&self) -> u64 {
        self.analytical.required_query_rows()
    }
}

/// The BLS macro capability exposed to shared application composition.
#[derive(Debug)]
pub(crate) enum BlsMacroCapabilityState {
    /// A manifest-bound latest-known read completed after exact restart verification.
    Available(BlsProviderPeriodLatestKnownDto),
    /// The capability is intentionally absent rather than represented by empty data.
    Unavailable(BlsMacroUnavailableReason),
}

impl BlsMacroCapabilityState {
    /// Reports that no current BLS activation has produced a complete sealed plan.
    pub(crate) const fn activation_required() -> Self {
        Self::Unavailable(BlsMacroUnavailableReason::ActivationRequired)
    }

    /// Reports that activation exists but no atomic immutable plan generation is selected.
    pub(crate) const fn manifest_required() -> Self {
        Self::Unavailable(BlsMacroUnavailableReason::ManifestRequired)
    }

    /// Returns the successful exact provider-period read, when available.
    pub(crate) const fn available(&self) -> Option<&BlsProviderPeriodLatestKnownDto> {
        match self {
            Self::Available(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }

    /// Returns the explicit setup/data blocker, when unavailable.
    pub(crate) const fn unavailable_reason(&self) -> Option<BlsMacroUnavailableReason> {
        match self {
            Self::Available(_) => None,
            Self::Unavailable(reason) => Some(*reason),
        }
    }
}

/// Closed reasons the fixed BLS provider-period operation cannot currently run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlsMacroUnavailableReason {
    /// No current activation has produced the adapter's complete sealed request-plan handoff.
    ActivationRequired,
    /// No exact atomic generation/manifest selector is available for the typed read.
    ManifestRequired,
}

/// Typed successful result for the fixed BLS provider-period operation.
#[derive(Debug)]
pub(crate) struct BlsProviderPeriodLatestKnownDto {
    restart_selector: ProviderMacroPlanRestartSelector,
    reopened: PinnedDataset,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
}

impl BlsProviderPeriodLatestKnownDto {
    /// Returns the fixed application operation that produced this DTO.
    pub(crate) const fn operation_identity(&self) -> &'static str {
        BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION
    }

    /// Returns the exact plan selector revalidated immediately before the read.
    pub(crate) const fn restart_selector(&self) -> &ProviderMacroPlanRestartSelector {
        &self.restart_selector
    }

    /// Returns the exact immutable generation reopened immediately before the read.
    pub(crate) const fn reopened(&self) -> &PinnedDataset {
        &self.reopened
    }

    /// Returns typed Macro observations with exact SourcePeriod, missingness, and clock evidence.
    pub(crate) const fn output(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        &self.output
    }

    /// Returns the sole source-rights owner for the fixed series selection.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.output.source_id()
    }
}

fn ensure_not_cancelled(cancellation: &CancellationToken) -> Result<(), BlsMacroApplicationError> {
    if cancellation.is_cancelled() {
        Err(BlsMacroApplicationError::Extraction(
            ExtractionSourceError::Cancelled,
        ))
    } else {
        Ok(())
    }
}

/// Failure before or during BLS atomic publication and exact provider-period selection.
#[derive(Debug, Error)]
pub(crate) enum BlsMacroApplicationError {
    /// The BLS adapter rejected provider content or a provider-local publication invariant.
    #[error("BLS adapter rejected the sealed complete plan")]
    Adapter(#[from] BlsSourceError),
    /// A bounded provider acquisition operation failed.
    #[error("BLS bounded acquisition failed")]
    Extraction(#[from] ExtractionSourceError),
    /// An application extraction request exceeded a shared contract ceiling.
    #[error("BLS extraction request is invalid")]
    ExtractionContract(#[from] ExtractionError),
    /// The application-owned research service rejected physical capture sealing.
    #[error("BLS application-owned physical capture sealing failed")]
    ResearchService(#[from] ResearchServiceError),
    /// The atomic data authority rejected preparation, commit, or exact restart verification.
    #[error("BLS atomic macro-plan publication failed")]
    Ingest(#[from] IngestError),
    /// The exact provider-period analytical capability rejected the bounded read.
    #[error("BLS provider-period analytical read failed")]
    AnalyticalRead(#[from] AnalyticalReadError),
    /// The complete plan could not fit its declared bounded application representation.
    #[error("BLS complete plan exceeds application capacity")]
    Capacity,
    /// The adapter's analytical dataset cannot be represented by the shared dataset identity.
    #[error("BLS analytical dataset identity is invalid")]
    InvalidAnalyticalDataset,
    /// The exact adapter-authored provider semantic document could not be serialized.
    #[error("BLS provider semantics could not be encoded")]
    ProviderSemanticsEncoding,
    /// Exact whole-plan receipt evidence did not reopen the same immutable generation.
    #[error("BLS exact restart verification changed generation identity")]
    RestartVerificationMismatch,
    /// The typed read did not retain the exact source and immutable generation.
    #[error("BLS provider-period read returned invalid binding evidence")]
    InvalidReadResult,
}
