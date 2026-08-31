//! Application-owned BLS sealing, atomic publication, and provider-period reads.
//!
//! The adapter closes one bounded request plan and retains BLS-native semantics. This leaf keeps
//! [`ResearchService`] as the sole physical sealer, converts the complete one-use handoff into the
//! shared atomic macro-plan input, and exposes only exact-manifest provider-period reads. It does
//! not register a Desktop operation or claim that Desktop composition is complete.

use std::{
    cmp::Ordering,
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
    time::Instant,
};

use futures_util::future::BoxFuture;
use market_squawk_adapter_bls::{
    BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION, BlsCanonicalObservationSemantics,
    BlsCanonicalProviderSemantics, BlsCompletePublicationPlanHandoff, BlsSource, BlsSourceError,
    BlsTimeseriesNativeLineageRowV1,
};
use market_squawk_data::{
    AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalReadError, DatasetId, DatasetManifestRef,
    IngestError, IngestPrecommitAuthority, IngestReservation, PinnedDataset,
    ProviderMacroPlanChunkInput, ProviderMacroPlanPublicationInput,
    ProviderMacroPlanPublicationReceipt, ProviderMacroPlanRestartSelector,
    ProviderMacroPlanSemantics, QueryLimits,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MacroObservation, ResearchPeriod, SourceId, Timestamp,
};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionAuthority, ExtractionError, ExtractionRequest,
    ExtractionSourceError,
};
use sha2::Digest as _;
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

/// Explicit bounds for the root-owned join from selected canonical rows to provider evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct BlsSelectedRowEvidenceLimits {
    max_selected_rows: NonZeroU32,
    max_opaque_bytes: NonZeroU64,
}

impl BlsSelectedRowEvidenceLimits {
    fn try_for_output(
        output: &AnalyticalMacroProviderPeriodLatestKnownOutput,
        query_limits: QueryLimits,
    ) -> Result<Self, BlsMacroApplicationError> {
        let max_selected_rows = u32::try_from(output.observations().len())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(BlsMacroApplicationError::Capacity)?;
        let max_opaque_bytes =
            NonZeroU64::new(query_limits.max_bytes()).ok_or(BlsMacroApplicationError::Capacity)?;
        Ok(Self {
            max_selected_rows,
            max_opaque_bytes,
        })
    }

    /// Returns the exact maximum selected canonical row count.
    pub(super) const fn max_selected_rows(self) -> NonZeroU32 {
        self.max_selected_rows
    }

    /// Returns the maximum aggregate provider-native and sidecar payload bytes.
    pub(super) const fn max_opaque_bytes(self) -> NonZeroU64 {
        self.max_opaque_bytes
    }
}

/// Root-composed capability for an exact, bounded selected-row provider-evidence join.
///
/// The shared data owner must implement this without giving the BLS leaf a catalog or raw-store
/// handle. Its data method must consume the analytical selector's retained canonical-payload
/// identities, constrain matches to `restart_selector.manifest()`, verify the sealed physical
/// claims, and return only the uniquely joined rows in [`BlsSelectedRowEvidenceJoinReceipt`]. It
/// must enforce `limits`, `deadline`, and `cancellation` during catalog and raw-evidence work. A
/// cumulative manifest-lineage scan or a generation-wide application fallback is not admissible.
///
/// Root integration must add one provider-neutral data receipt that retains the already-decoded
/// selector's ordered `payload_sha256` values (currently private inside the analytical decoder),
/// source, manifest, and selection digest. The corresponding shared method must join those exact
/// payload identities directly to provider-capture row mappings for that manifest and return:
/// binding digest; native schema version/implementation/original row count/batch digest; one
/// sidecar and its digest per matched binding; and only the selected row ordinal, canonical digest,
/// native bytes/digest, and receipt clock. The method must verify existing sealed claims before
/// returning and apply the count, aggregate opaque-byte, deadline, and cancellation bounds here.
pub(super) trait BlsSelectedRowEvidenceJoin: Send + Sync {
    /// Reopens only the provider-native rows selected by the exact analytical output.
    fn reopen_selected_rows<'a>(
        &'a self,
        restart_selector: &'a ProviderMacroPlanRestartSelector,
        output: &'a AnalyticalMacroProviderPeriodLatestKnownOutput,
        limits: BlsSelectedRowEvidenceLimits,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<BlsSelectedRowEvidenceJoinReceipt, BlsSelectedRowEvidenceJoinError>>;
}

/// Closed failures emitted by the root-selected evidence capability.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum BlsSelectedRowEvidenceJoinError {
    /// The operation was cancelled before the verified receipt completed.
    #[error("selected-row evidence join was cancelled")]
    Cancelled,
    /// The explicit evidence-join deadline elapsed.
    #[error("selected-row evidence join exceeded its deadline")]
    DeadlineExceeded,
    /// Shared data could not produce one exact verified row per analytical selection.
    #[error("selected-row evidence join was rejected")]
    Rejected,
}

/// Opaque common-owned receipt for the exact selected canonical rows.
///
/// Batches retain only the native bytes and digests that the BLS adapter must decode. Catalog
/// coordinates, SQL, raw-store handles, cumulative lineage, and unselected native rows stay in the
/// shared data implementation.
#[derive(Debug)]
pub(super) struct BlsSelectedRowEvidenceJoinReceipt {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    selection_digest: EvidenceDigest,
    batches: Box<[BlsSelectedRowEvidenceBatch]>,
}

impl BlsSelectedRowEvidenceJoinReceipt {
    /// Builds the closed receipt after a root-owned implementation has verified physical evidence.
    pub(super) fn try_new(
        manifest: DatasetManifestRef,
        source_id: SourceId,
        selection_digest: EvidenceDigest,
        batches: Vec<BlsSelectedRowEvidenceBatch>,
        limits: BlsSelectedRowEvidenceLimits,
    ) -> Result<Self, BlsSelectedRowEvidenceJoinError> {
        if batches.is_empty()
            || selection_digest.algorithm() != DigestAlgorithm::Sha256
            || selection_digest.bytes() == [0; 32]
        {
            return Err(BlsSelectedRowEvidenceJoinError::Rejected);
        }
        let mut selected_rows = 0_u32;
        let mut opaque_bytes = 0_u64;
        for (batch_index, batch) in batches.iter().enumerate() {
            if batch.rows.is_empty()
                || batch.implementation.is_empty()
                || batch.original_row_count == 0
                || batch.binding_digest.algorithm() != DigestAlgorithm::Sha256
                || batch.binding_digest.bytes() == [0; 32]
                || batch.batch_digest.algorithm() != DigestAlgorithm::Sha256
                || batch.batch_digest.bytes() == [0; 32]
                || batch.sidecar_digest.algorithm() != DigestAlgorithm::Sha256
                || batch.sidecar_digest.bytes() == [0; 32]
                || digest_bytes(&batch.sidecar) != batch.sidecar_digest
                || batches[..batch_index]
                    .iter()
                    .any(|prior| prior.binding_digest == batch.binding_digest)
            {
                return Err(BlsSelectedRowEvidenceJoinError::Rejected);
            }
            selected_rows = selected_rows
                .checked_add(
                    u32::try_from(batch.rows.len())
                        .map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?,
                )
                .ok_or(BlsSelectedRowEvidenceJoinError::Rejected)?;
            opaque_bytes = checked_add_opaque_bytes(opaque_bytes, batch.implementation.len())?;
            opaque_bytes = checked_add_opaque_bytes(opaque_bytes, batch.sidecar.len())?;
            for (row_index, row) in batch.rows.iter().enumerate() {
                if usize::try_from(row.canonical_row_ordinal)
                    .ok()
                    .is_none_or(|ordinal| ordinal >= batch.original_row_count)
                    || row.native_semantic_payload.is_empty()
                    || row.canonical_row_digest.algorithm() != DigestAlgorithm::Sha256
                    || row.canonical_row_digest.bytes() == [0; 32]
                    || row.native_semantic_digest.algorithm() != DigestAlgorithm::Sha256
                    || row.native_semantic_digest.bytes() == [0; 32]
                    || digest_bytes(&row.native_semantic_payload) != row.native_semantic_digest
                    || batch.rows[..row_index].iter().any(|prior| {
                        prior.canonical_row_ordinal == row.canonical_row_ordinal
                            || prior.canonical_row_digest == row.canonical_row_digest
                    })
                {
                    return Err(BlsSelectedRowEvidenceJoinError::Rejected);
                }
                opaque_bytes =
                    checked_add_opaque_bytes(opaque_bytes, row.native_semantic_payload.len())?;
            }
        }
        if selected_rows != limits.max_selected_rows.get()
            || opaque_bytes == 0
            || opaque_bytes > limits.max_opaque_bytes.get()
        {
            return Err(BlsSelectedRowEvidenceJoinError::Rejected);
        }
        Ok(Self {
            manifest,
            source_id,
            selection_digest,
            batches: batches.into_boxed_slice(),
        })
    }
}

/// One provider binding containing one or more selected native rows and one shared sidecar.
#[derive(Debug)]
pub(super) struct BlsSelectedRowEvidenceBatch {
    binding_digest: EvidenceDigest,
    native_schema_version: u16,
    implementation: Box<str>,
    original_row_count: usize,
    batch_digest: EvidenceDigest,
    sidecar: Box<[u8]>,
    sidecar_digest: EvidenceDigest,
    rows: Box<[BlsSelectedRowNativeEvidence]>,
}

impl BlsSelectedRowEvidenceBatch {
    /// Retains verified common-owned bytes without interpreting provider semantics.
    #[allow(
        clippy::too_many_arguments,
        reason = "every native schema, sidecar, batch, and selected-row coordinate stays explicit"
    )]
    pub(super) fn new(
        binding_digest: EvidenceDigest,
        native_schema_version: u16,
        implementation: Box<str>,
        original_row_count: usize,
        batch_digest: EvidenceDigest,
        sidecar: Box<[u8]>,
        sidecar_digest: EvidenceDigest,
        rows: Vec<BlsSelectedRowNativeEvidence>,
    ) -> Self {
        Self {
            binding_digest,
            native_schema_version,
            implementation,
            original_row_count,
            batch_digest,
            sidecar,
            sidecar_digest,
            rows: rows.into_boxed_slice(),
        }
    }
}

/// Opaque common-owned evidence for one selected canonical/native row.
#[derive(Debug)]
pub(super) struct BlsSelectedRowNativeEvidence {
    canonical_row_ordinal: u32,
    canonical_row_digest: EvidenceDigest,
    native_semantic_payload: Box<[u8]>,
    native_semantic_digest: EvidenceDigest,
    received_at: Timestamp,
}

impl BlsSelectedRowNativeEvidence {
    /// Retains the exact verified row coordinate and provider-native payload.
    pub(super) fn new(
        canonical_row_ordinal: u32,
        canonical_row_digest: EvidenceDigest,
        native_semantic_payload: Box<[u8]>,
        native_semantic_digest: EvidenceDigest,
        received_at: Timestamp,
    ) -> Self {
        Self {
            canonical_row_ordinal,
            canonical_row_digest,
            native_semantic_payload,
            native_semantic_digest,
            received_at,
        }
    }
}

fn checked_add_opaque_bytes(
    retained: u64,
    bytes: usize,
) -> Result<u64, BlsSelectedRowEvidenceJoinError> {
    retained
        .checked_add(u64::try_from(bytes).map_err(|_| BlsSelectedRowEvidenceJoinError::Rejected)?)
        .ok_or(BlsSelectedRowEvidenceJoinError::Rejected)
}

fn digest_bytes(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, sha2::Sha256::digest(bytes).into())
}

/// Application-owned physical sealer and atomic BLS publication/read coordinator.
#[derive(Clone)]
pub(crate) struct BlsMacroApplicationClosure {
    research: Arc<ResearchService>,
    selected_row_evidence: Option<Arc<dyn BlsSelectedRowEvidenceJoin>>,
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
        Self {
            research,
            selected_row_evidence: None,
        }
    }

    /// Binds the root-owned selected-row evidence capability without giving BLS a raw store.
    pub(super) fn with_selected_row_evidence(
        research: Arc<ResearchService>,
        selected_row_evidence: Arc<dyn BlsSelectedRowEvidenceJoin>,
    ) -> Self {
        Self {
            research,
            selected_row_evidence: Some(selected_row_evidence),
        }
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
        let retained_request = analytical.clone();
        let output = match self
            .research
            .analytical_reader()
            .read_macro_provider_period_latest_known_snapshot(
                analytical,
                limits,
                deadline,
                cancellation.clone(),
            )
            .await
        {
            Ok(output) => output,
            Err(AnalyticalReadError::MacroSnapshotIncomplete) => {
                return Ok(BlsMacroCapabilityState::Unavailable(
                    BlsMacroUnavailableReason::IncompleteSeriesAtCutoff,
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if !validate_provider_period_consumer_read(&restart_selector, &retained_request, &output)? {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        let evidence_join = self
            .selected_row_evidence
            .as_deref()
            .ok_or(BlsMacroApplicationError::SelectedRowEvidenceJoinUnavailable)?;
        let evidence_limits = BlsSelectedRowEvidenceLimits::try_for_output(&output, limits)?;
        let semantic_evidence = reopen_provider_period_semantic_evidence(
            evidence_join,
            &restart_selector,
            &output,
            evidence_limits,
            deadline,
            cancellation,
        )
        .await?;
        Ok(BlsMacroCapabilityState::Available(
            BlsProviderPeriodLatestKnownDto {
                restart_selector,
                reopened,
                analytical_request: retained_request,
                output,
                semantic_evidence,
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
    /// The exact cutoff did not yield one explicit observed-or-missing row for every series.
    IncompleteSeriesAtCutoff,
}

/// Typed successful result and provider-neutral consumer handoff for the fixed BLS operation.
///
/// The handoff retains the exact shared analytical request alongside its shared typed output, so
/// macro context, feature, forecast, and backtest consumers can inherit one immutable manifest,
/// source-qualified allowlist, knowledge cutoff, provider-period cutoff, and selection digest. It
/// is produced only for a complete series set; provider-native missing rows remain data while an
/// absent row yields [`BlsMacroUnavailableReason::IncompleteSeriesAtCutoff`].
#[derive(Debug)]
pub(crate) struct BlsProviderPeriodLatestKnownDto {
    restart_selector: ProviderMacroPlanRestartSelector,
    reopened: PinnedDataset,
    analytical_request: AnalyticalMacroProviderPeriodLatestKnownRequest,
    output: AnalyticalMacroProviderPeriodLatestKnownOutput,
    semantic_evidence: BlsProviderPeriodSemanticEvidence,
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

    /// Returns the exact shared PIT request that produced the consumer handoff.
    pub(crate) const fn analytical_request(
        &self,
    ) -> &AnalyticalMacroProviderPeriodLatestKnownRequest {
        &self.analytical_request
    }

    /// Returns typed Macro observations with exact SourcePeriod, missingness, and clock evidence.
    pub(crate) const fn output(&self) -> &AnalyticalMacroProviderPeriodLatestKnownOutput {
        &self.output
    }

    /// Returns exact reopened native/companion evidence aligned to the selected canonical rows.
    pub(crate) fn semantic_observations(&self) -> &[BlsProviderPeriodObservationSemanticEvidence] {
        &self.semantic_evidence.observations
    }

    /// Returns the sole source-rights owner for the fixed series selection.
    pub(crate) const fn source_id(&self) -> &SourceId {
        self.output.source_id()
    }
}

/// Provider-local durable evidence for the selected canonical rows.
///
/// This value remains below provider-neutral product composition. It proves the native/companion
/// join without adding provider fields to Desktop, CLI, MCP, feature, or forecast DTOs.
#[derive(Debug)]
pub(crate) struct BlsProviderPeriodSemanticEvidence {
    observations: Box<[BlsProviderPeriodObservationSemanticEvidence]>,
}

/// One selected canonical observation rejoined to exact persisted BLS semantics and digests.
#[derive(Clone, Debug)]
pub(crate) struct BlsProviderPeriodObservationSemanticEvidence {
    companion: BlsCanonicalObservationSemantics,
    native: BlsTimeseriesNativeLineageRowV1,
    binding_digest: EvidenceDigest,
    canonical_row_digest: EvidenceDigest,
    native_semantic_digest: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    native_sidecar_digest: EvidenceDigest,
    provider_semantics_digest: EvidenceDigest,
}

impl BlsProviderPeriodObservationSemanticEvidence {
    /// Returns the full persisted companion observation, including distinct local clocks.
    pub(crate) const fn companion(&self) -> &BlsCanonicalObservationSemantics {
        &self.companion
    }

    /// Returns the exact decoded value-only native row aligned by canonical row ordinal.
    pub(crate) const fn native(&self) -> &BlsTimeseriesNativeLineageRowV1 {
        &self.native
    }

    /// Returns the common-owned exact provider binding identity.
    pub(crate) const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    /// Returns the exact extraction-row digest bound into persisted native lineage.
    pub(crate) const fn canonical_row_digest(&self) -> EvidenceDigest {
        self.canonical_row_digest
    }

    /// Returns SHA-256 of this row's exact persisted native semantic bytes.
    pub(crate) const fn native_semantic_digest(&self) -> EvidenceDigest {
        self.native_semantic_digest
    }

    /// Returns the complete persisted native batch identity.
    pub(crate) const fn native_batch_digest(&self) -> EvidenceDigest {
        self.native_batch_digest
    }

    /// Returns SHA-256 of the complete persisted BLS companion sidecar.
    pub(crate) const fn native_sidecar_digest(&self) -> EvidenceDigest {
        self.native_sidecar_digest
    }

    /// Returns the adapter-authored semantic identity decoded from that sidecar.
    pub(crate) const fn provider_semantics_digest(&self) -> EvidenceDigest {
        self.provider_semantics_digest
    }

    fn matches_selected(&self, observation: &MacroObservation) -> bool {
        let companion = &self.companion;
        let provenance = observation.context().provenance();
        let value_matches = match (
            observation.value().observed_value(),
            observation.value().missing_value(),
            companion.value(),
        ) {
            (Some(selected), None, Some(provider)) => selected == provider,
            (None, Some(missing), None) => missing.marker().as_str() == companion.raw_value(),
            (Some(_), Some(_), _)
            | (None, None, _)
            | (Some(_), None, None)
            | (None, Some(_), Some(_)) => false,
        };
        companion.series_id() == observation.series()
            && companion.effective_time() == observation.context().time().effective()
            && companion.canonical_revision() == provenance.source_identifier()
            && companion.locally_available_at() == provenance.received_at()
            && companion.canonical_ingested_at() == provenance.ingested_at()
            && provenance.availability().conservative_available_at()
                == Some(companion.locally_available_at())
            && self.native.series().unit() == observation.unit()
            && value_matches
    }
}

async fn reopen_provider_period_semantic_evidence(
    evidence_join: &dyn BlsSelectedRowEvidenceJoin,
    restart_selector: &ProviderMacroPlanRestartSelector,
    output: &AnalyticalMacroProviderPeriodLatestKnownOutput,
    limits: BlsSelectedRowEvidenceLimits,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<BlsProviderPeriodSemanticEvidence, BlsMacroApplicationError> {
    let receipt = evidence_join
        .reopen_selected_rows(restart_selector, output, limits, deadline, cancellation)
        .await
        .map_err(BlsMacroApplicationError::from_selected_row_evidence_join)?;
    if &receipt.manifest != restart_selector.manifest()
        || &receipt.source_id != restart_selector.source_id()
        || receipt.selection_digest != output.selection_digest()
    {
        return Err(BlsMacroApplicationError::InvalidReadResult);
    }
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(output.observations().len())
        .map_err(|_| BlsMacroApplicationError::Capacity)?;
    for batch in receipt.batches {
        if batch.implementation.as_ref() != BLS_TIMESERIES_NATIVE_LINEAGE_IMPLEMENTATION {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        let semantics =
            BlsCanonicalProviderSemantics::try_decode_persisted_native_sidecar(&batch.sidecar)?;
        if semantics.observations().len() != batch.original_row_count
            || semantics.semantics_digest().algorithm() != DigestAlgorithm::Sha256
            || semantics.semantics_digest().bytes() == [0; 32]
        {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        for row in batch.rows {
            let native = BlsTimeseriesNativeLineageRowV1::try_decode_persisted(
                batch.native_schema_version,
                &batch.implementation,
                &row.native_semantic_payload,
            )?;
            let companion =
                semantics.validate_persisted_native_row(row.canonical_row_ordinal, &native)?;
            if companion.canonical_payload_digest() != row.canonical_row_digest
                || companion.locally_available_at() != row.received_at
                || retained
                    .iter()
                    .any(|prior: &BlsProviderPeriodObservationSemanticEvidence| {
                        prior.canonical_row_digest == row.canonical_row_digest
                            || (prior.binding_digest == batch.binding_digest
                                && prior.companion.record_ordinal() == row.canonical_row_ordinal)
                    })
            {
                return Err(BlsMacroApplicationError::InvalidReadResult);
            }
            retained.push(BlsProviderPeriodObservationSemanticEvidence {
                companion: companion.clone(),
                native,
                binding_digest: batch.binding_digest,
                canonical_row_digest: row.canonical_row_digest,
                native_semantic_digest: row.native_semantic_digest,
                native_batch_digest: batch.batch_digest,
                native_sidecar_digest: batch.sidecar_digest,
                provider_semantics_digest: semantics.semantics_digest(),
            });
        }
    }

    let mut selected = Vec::new();
    selected
        .try_reserve_exact(output.observations().len())
        .map_err(|_| BlsMacroApplicationError::Capacity)?;
    for observation in output.observations() {
        let mut matches = retained
            .iter()
            .filter(|evidence| evidence.matches_selected(observation));
        let evidence = matches
            .next()
            .ok_or(BlsMacroApplicationError::InvalidReadResult)?;
        if matches.next().is_some() {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        selected.push(evidence.clone());
    }
    if selected.len() != output.observations().len() {
        return Err(BlsMacroApplicationError::InvalidReadResult);
    }
    Ok(BlsProviderPeriodSemanticEvidence {
        observations: selected.into_boxed_slice(),
    })
}

/// Validates that one shared analytical read is safe to hand to every downstream macro consumer.
///
/// Incompleteness is emitted only by the analytical reader's exact `MacroSnapshotIncomplete`
/// result. Any malformed or partial successful output is an error rather than another absence.
fn validate_provider_period_consumer_read(
    restart_selector: &ProviderMacroPlanRestartSelector,
    request: &AnalyticalMacroProviderPeriodLatestKnownRequest,
    output: &AnalyticalMacroProviderPeriodLatestKnownOutput,
) -> Result<bool, BlsMacroApplicationError> {
    if request.manifest() != restart_selector.manifest()
        || request.source_series().source_id() != restart_selector.source_id()
        || output.source_id() != restart_selector.source_id()
        || output.output().manifest() != restart_selector.manifest()
        || output.period_scheme() != request.effective_period_cutoff().scheme()
    {
        return Err(BlsMacroApplicationError::InvalidReadResult);
    }

    let expected_series = request.source_series().series_allowlist().series();
    let observations = output.observations();
    if observations.len() > expected_series.len() {
        return Err(BlsMacroApplicationError::InvalidReadResult);
    }

    for (index, observation) in observations.iter().enumerate() {
        if expected_series.binary_search(observation.series()).is_err()
            || observations[..index]
                .iter()
                .any(|prior| prior.series() == observation.series())
        {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }

        let context = observation.context();
        let provenance = context.provenance();
        let time = context.time();
        let Some(effective_period) = time.effective().source_period_value() else {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        };
        if effective_period.scheme() != output.period_scheme()
            || !matches!(
                effective_period.partial_cmp(request.effective_period_cutoff()),
                Some(Ordering::Less | Ordering::Equal)
            )
            || time.published().is_some()
            || time.superseded().is_some()
            || provenance.source_id() != restart_selector.source_id()
            || provenance.instrument_id().is_some()
            || provenance.venue_id().is_some()
            || provenance.source_timestamp().is_some()
            || provenance.received_at() > request.knowledge_cutoff()
            || provenance.ingested_at() > request.knowledge_cutoff()
        {
            return Err(BlsMacroApplicationError::InvalidReadResult);
        }
        match provenance.availability().conservative_available_at() {
            Some(available_at) if available_at <= request.knowledge_cutoff() => {}
            Some(_) | None => return Err(BlsMacroApplicationError::InvalidReadResult),
        }
        match (
            observation.value().observed_value(),
            observation.value().missing_value(),
        ) {
            (Some(_), None) | (None, Some(_)) => {}
            (Some(_), Some(_)) | (None, None) => {
                return Err(BlsMacroApplicationError::InvalidReadResult);
            }
        }
    }

    Ok(observations.len() == expected_series.len())
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
    /// Shared application composition did not install the selected-row evidence capability.
    #[error("BLS selected-row evidence join is unavailable")]
    SelectedRowEvidenceJoinUnavailable,
    /// Caller cancellation won while the root-owned selected-row evidence join was active.
    #[error("BLS selected-row evidence join was cancelled")]
    SelectedRowEvidenceJoinCancelled,
    /// The root-owned selected-row evidence join exhausted its explicit deadline.
    #[error("BLS selected-row evidence join exceeded its deadline")]
    SelectedRowEvidenceJoinDeadlineExceeded,
    /// Shared data rejected the exact selected-row evidence join.
    #[error("BLS selected-row evidence join was rejected")]
    SelectedRowEvidenceJoinRejected,
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

impl BlsMacroApplicationError {
    fn from_selected_row_evidence_join(error: BlsSelectedRowEvidenceJoinError) -> Self {
        match error {
            BlsSelectedRowEvidenceJoinError::Cancelled => Self::SelectedRowEvidenceJoinCancelled,
            BlsSelectedRowEvidenceJoinError::DeadlineExceeded => {
                Self::SelectedRowEvidenceJoinDeadlineExceeded
            }
            BlsSelectedRowEvidenceJoinError::Rejected => Self::SelectedRowEvidenceJoinRejected,
        }
    }
}
