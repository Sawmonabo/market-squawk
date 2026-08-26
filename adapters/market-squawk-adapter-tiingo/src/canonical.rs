use std::num::NonZeroU64;

use market_squawk_domain::{
    CalendarDate, Currency, DataQuality, EvidenceDigest, ExactPayloadEvidence, FundNavCompleteness,
    FundNavDisposition, FundNavEntitlementEvidence, FundNavLineage, FundNavMissingState,
    FundNavNativeSchema, FundNavValuationBasis, FundNavValue, MetadataRevision, PayloadHash,
    PayloadReference, ProviderChannel, ProviderInstrumentId, ProviderProduct, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use market_squawk_sources::{ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    TiingoCompletedHistoryCapture, TiingoEndpointFamily, TiingoNavObservationCandidate,
    TiingoNavValueState, TiingoPaginationEvidence, TiingoProviderRevisionEvidence,
    TiingoRequestScope,
};

const TIINGO_PROVIDER_PRODUCT: &str = "starter";
const TIINGO_PROVIDER_CHANNEL: &str = "daily-eod";
const TIINGO_NAV_NATIVE_SCHEMA: &str = "tiingo.daily-prices.eod-row";
const TIINGO_SOURCE_ID: &str = "tiingo-starter";

/// Exact policy/schema/entitlement evidence required by canonical Tiingo NAV mapping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TiingoFundNavContractEvidence {
    source_id: SourceId,
    source_contract_revision: MetadataRevision,
    source_contract_evidence: ExactPayloadEvidence,
    native_schema_revision: SourceIdentifier,
    native_schema_evidence: ExactPayloadEvidence,
    entitlement_generation: NonZeroU64,
    entitlement_generation_identity: SourceIdentifier,
    entitlement_evidence: EvidenceDigest,
}

impl TiingoFundNavContractEvidence {
    /// Binds the activated source contract, reviewed native schema, and gated token generation.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, schema, and protected entitlement identities remain explicit"
    )]
    pub fn try_new(
        source_id: SourceId,
        source_contract_revision: MetadataRevision,
        source_contract_evidence: ExactPayloadEvidence,
        native_schema_revision: SourceIdentifier,
        native_schema_evidence: ExactPayloadEvidence,
        entitlement_generation: NonZeroU64,
        entitlement_generation_identity: SourceIdentifier,
        entitlement_evidence: EvidenceDigest,
    ) -> Result<Self, TiingoFundNavMapError> {
        if source_id.as_str() != TIINGO_SOURCE_ID
            || [
                source_contract_evidence.content_digest(),
                native_schema_evidence.content_digest(),
                entitlement_evidence,
            ]
            .into_iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(TiingoFundNavMapError::InvalidContractEvidence);
        }
        Ok(Self {
            source_id,
            source_contract_revision,
            source_contract_evidence,
            native_schema_revision,
            native_schema_evidence,
            entitlement_generation,
            entitlement_generation_identity,
            entitlement_evidence,
        })
    }

    /// Returns the exact activated Tiingo source namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact activated source-contract revision.
    pub const fn source_contract_revision(&self) -> &MetadataRevision {
        &self.source_contract_revision
    }

    /// Returns the exact source-contract payload evidence.
    pub const fn source_contract_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_contract_evidence
    }

    /// Returns the reviewed Tiingo native-schema payload evidence.
    pub const fn native_schema_evidence(&self) -> &ExactPayloadEvidence {
        &self.native_schema_evidence
    }

    /// Returns the exact reviewed provider-native decoder contract revision.
    pub const fn native_schema_revision(&self) -> &SourceIdentifier {
        &self.native_schema_revision
    }

    /// Returns the nonzero protected-credential generation used for retrieval.
    pub const fn entitlement_generation(&self) -> NonZeroU64 {
        self.entitlement_generation
    }

    /// Returns the exact source-local identity for that protected-token generation.
    pub const fn entitlement_generation_identity(&self) -> &SourceIdentifier {
        &self.entitlement_generation_identity
    }

    /// Returns exact admission evidence for that entitlement generation.
    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.entitlement_evidence
    }
}

/// Complete pure-mapping input for one sealed Tiingo daily NAV result.
#[derive(Debug)]
pub struct TiingoFundNavMappingInput<'a> {
    /// Strict provider-native NAV candidate.
    pub candidate: &'a TiingoNavObservationCandidate,
    /// Exact raw response already sealed into the shared `MSJ1` journal.
    pub sealed_capture: &'a SealedProviderCaptureSetReceipt,
    /// Terminal exact request-graph evidence, required only for a historical response.
    pub completed_history: Option<&'a TiingoCompletedHistoryCapture>,
    /// Exact per-ticker metadata admission response sealed into the same journal authority.
    pub sealed_metadata_capture: &'a SealedProviderCaptureSetReceipt,
    /// Activated source, native-schema, and gated-entitlement evidence.
    pub contract: &'a TiingoFundNavContractEvidence,
    /// Time canonical ingestion completed locally.
    pub ingested_at: Timestamp,
}

/// One latest-only Tiingo NAV row awaiting the common publication transaction.
///
/// The future common transaction must consume this handoff and bind its exact provider handoff
/// identity, source, FundNav family and semantic payload to a trusted canonical publication clock,
/// locally assigned observed revision, predecessor/successor evidence, and final domain value.
/// This adapter supplies none of those common storage or point-in-time facts.
#[derive(Debug)]
pub struct TiingoPendingLatestFundNavPublication {
    candidate: TiingoFundNavCanonicalCandidate,
}

impl TiingoPendingLatestFundNavPublication {
    /// Consumes the closed handoff into its exact provider-local canonical candidate.
    pub fn into_candidate(self) -> TiingoFundNavCanonicalCandidate {
        self.candidate
    }
}

/// Validated provider-local canonical NAV fields awaiting shared revision/publication authority.
///
/// Only a consuming latest or complete-history transition may turn this value into a pending
/// publication handoff. The adapter never accepts or chooses revision, canonical publication,
/// correction, manifest, catalog, or point-in-time authority.
#[derive(Debug, Eq, PartialEq)]
pub struct TiingoFundNavCanonicalCandidate {
    provenance: ResearchProvenance,
    effective: ResearchTemporalCoordinate,
    provider_instrument_id: ProviderInstrumentId,
    instrument_reference_revision: MetadataRevision,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    nav_date: CalendarDate,
    currency: Currency,
    value: FundNavValue,
    lineage: FundNavLineage,
    sealed_capture_receipt: EvidenceDigest,
    sealed_metadata_capture_receipt: EvidenceDigest,
    response_request_identity: EvidenceDigest,
    provider_row_index: Option<u32>,
    provider_row_digest: Option<EvidenceDigest>,
    history_page_identity: Option<EvidenceDigest>,
    history_completion_identity: Option<EvidenceDigest>,
    handoff_identity: EvidenceDigest,
}

impl TiingoFundNavCanonicalCandidate {
    /// Returns canonical provenance finalized up to the local ingestion boundary.
    pub const fn provenance(&self) -> &ResearchProvenance {
        &self.provenance
    }

    /// Returns the exact date-valued effective coordinate; no midnight instant is invented.
    pub const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }

    /// Returns the exact Tiingo provider instrument.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the external instrument-definition revision used by mapping.
    pub const fn instrument_reference_revision(&self) -> &MetadataRevision {
        &self.instrument_reference_revision
    }

    /// Returns the code-owned Tiingo product identity.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    /// Returns the code-owned Tiingo daily-EOD channel identity.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    /// Returns the exact source valuation date.
    pub const fn nav_date(&self) -> CalendarDate {
        self.nav_date
    }

    /// Returns the only truthful valuation basis for this Tiingo mutual-fund mapping.
    pub const fn valuation_basis(&self) -> FundNavValuationBasis {
        FundNavValuationBasis::PerShare
    }

    /// Returns externally resolved fund/share-class currency.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the exact observed or closed missing NAV value.
    pub const fn value(&self) -> FundNavValue {
        self.value
    }

    /// Returns exact source/schema/entitlement/raw/page/seal lineage.
    pub const fn lineage(&self) -> &FundNavLineage {
        &self.lineage
    }

    /// Returns evidence binding the exact raw response to immutable physical storage.
    pub const fn sealed_capture_receipt(&self) -> EvidenceDigest {
        self.sealed_capture_receipt
    }

    /// Returns evidence binding the exact metadata admission response to immutable storage.
    pub const fn sealed_metadata_capture_receipt(&self) -> EvidenceDigest {
        self.sealed_metadata_capture_receipt
    }

    /// Returns the exact latest or historical source-response request identity.
    pub const fn response_request_identity(&self) -> EvidenceDigest {
        self.response_request_identity
    }

    /// Returns the exact zero-based provider row ordinal when a row was returned.
    pub const fn provider_row_index(&self) -> Option<u32> {
        self.provider_row_index
    }

    /// Returns the exact provider-native row identity when a row was returned.
    pub const fn provider_row_digest(&self) -> Option<EvidenceDigest> {
        self.provider_row_digest
    }

    /// Returns the exact sealed decoded history-page identity for historical mapping.
    pub const fn history_page_identity(&self) -> Option<EvidenceDigest> {
        self.history_page_identity
    }

    /// Returns the exact terminal raw request-graph identity for historical mapping.
    pub const fn history_completion_identity(&self) -> Option<EvidenceDigest> {
        self.history_completion_identity
    }

    /// Returns the exact provider-local NAV handoff identity consumed by shared publication.
    pub const fn handoff_identity(&self) -> EvidenceDigest {
        self.handoff_identity
    }

    /// Consumes a latest-only row into the closed pending-publication capability.
    ///
    /// A row carrying either historical coordinate is returned unchanged so the caller can place
    /// it only into the terminal whole-history reconciliation path.
    pub fn try_into_latest_pending_publication(
        self,
    ) -> Result<TiingoPendingLatestFundNavPublication, Self> {
        if self.history_page_identity.is_some() || self.history_completion_identity.is_some() {
            return Err(self);
        }
        Ok(TiingoPendingLatestFundNavPublication { candidate: self })
    }
}

/// Honest financial-date coverage state of a completed provider request graph.
///
/// Tiingo's returned rows and application-created date windows do not identify every date on
/// which the resolved fund was expected to publish NAV. Only a separate shared calendar/fund
/// policy authority may reconcile that expectation and upgrade the later published generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TiingoFundNavHistoryFinancialCoverage {
    /// No authoritative expected-financial-date set was supplied at this provider boundary.
    ExpectedFinancialDatesUnavailable,
}

/// Entire terminal Tiingo NAV history graph awaiting one common publication transaction.
///
/// The future common transaction must consume all parts together and bind the complete provider
/// handoff identity and each ordered FundNav family/semantic payload to trusted publication,
/// observed-revision, predecessor/successor, catalog, and point-in-time authority. The provider
/// adapter neither accepts nor constructs those facts.
#[derive(Debug)]
pub struct TiingoPendingFundNavHistoryPublication {
    completed_capture: TiingoCompletedHistoryCapture,
    observations: Box<[TiingoFundNavCanonicalCandidate]>,
    returned_provider_rows: u64,
    financial_coverage: TiingoFundNavHistoryFinancialCoverage,
    handoff_identity: EvidenceDigest,
}

impl TiingoPendingFundNavHistoryPublication {
    /// Consumes the closed whole-history capability and transfers every retained part together.
    pub fn into_parts(
        self,
    ) -> (
        TiingoCompletedHistoryCapture,
        Box<[TiingoFundNavCanonicalCandidate]>,
        u64,
        TiingoFundNavHistoryFinancialCoverage,
        EvidenceDigest,
    ) {
        (
            self.completed_capture,
            self.observations,
            self.returned_provider_rows,
            self.financial_coverage,
            self.handoff_identity,
        )
    }
}

/// Completed provider-local NAV-history handoff awaiting shared publication and PIT authority.
///
/// Construction consumes the terminal raw request graph and proves that every returned native
/// row has exactly one mapped canonical candidate. It deliberately does not claim that every
/// financially expected NAV date was returned.
#[derive(Debug, Eq, PartialEq)]
pub struct TiingoCompletedFundNavHistoryCandidate {
    completed_capture: TiingoCompletedHistoryCapture,
    observations: Box<[TiingoFundNavCanonicalCandidate]>,
    returned_provider_rows: u64,
    financial_coverage: TiingoFundNavHistoryFinancialCoverage,
    handoff_identity: EvidenceDigest,
}

impl TiingoCompletedFundNavHistoryCandidate {
    /// Reconciles the exact terminal raw request graph with all mapped returned NAV rows.
    pub fn try_new(
        completed_capture: TiingoCompletedHistoryCapture,
        mut observations: Vec<TiingoFundNavCanonicalCandidate>,
    ) -> Result<Self, TiingoFundNavMapError> {
        validate_completed_nav_history(&completed_capture, &observations)?;
        sort_completed_nav_history_rows(&mut observations);
        let returned_provider_rows = completed_capture.total_rows();
        let financial_coverage =
            TiingoFundNavHistoryFinancialCoverage::ExpectedFinancialDatesUnavailable;
        let handoff_identity = completed_nav_history_handoff_identity(
            &completed_capture,
            &observations,
            financial_coverage,
        )?;
        Ok(Self {
            completed_capture,
            observations: observations.into_boxed_slice(),
            returned_provider_rows,
            financial_coverage,
            handoff_identity,
        })
    }

    /// Returns the exact number of provider-native rows mapped once.
    pub const fn returned_provider_rows(&self) -> u64 {
        self.returned_provider_rows
    }

    /// Returns the explicit absence of authoritative expected-financial-date coverage.
    pub const fn financial_coverage(&self) -> TiingoFundNavHistoryFinancialCoverage {
        self.financial_coverage
    }

    /// Returns the provider-local complete-history handoff identity.
    pub const fn handoff_identity(&self) -> EvidenceDigest {
        self.handoff_identity
    }

    /// Infallibly consumes the already reconciled and deterministically ordered history graph.
    pub fn into_pending_publication(self) -> TiingoPendingFundNavHistoryPublication {
        let Self {
            completed_capture,
            observations,
            returned_provider_rows,
            financial_coverage,
            handoff_identity,
        } = self;
        TiingoPendingFundNavHistoryPublication {
            completed_capture,
            observations,
            returned_provider_rows,
            financial_coverage,
            handoff_identity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TiingoNavHistoryRowCoordinate {
    page_identity: [u8; 32],
    row_index: u32,
    row_digest: [u8; 32],
}

fn sort_completed_nav_history_rows(observations: &mut [TiingoFundNavCanonicalCandidate]) {
    observations.sort_unstable_by_key(|observation| {
        (
            observation.nav_date(),
            observation
                .history_page_identity()
                .map(|identity| identity.bytes())
                .unwrap_or([0; 32]),
            observation.provider_row_index().unwrap_or(u32::MAX),
            observation
                .provider_row_digest()
                .map(|identity| identity.bytes())
                .unwrap_or([0; 32]),
            observation.handoff_identity().bytes(),
        )
    });
}

fn validate_completed_nav_history(
    completed: &TiingoCompletedHistoryCapture,
    observations: &[TiingoFundNavCanonicalCandidate],
) -> Result<(), TiingoFundNavMapError> {
    let expected_row_count = usize::try_from(completed.total_rows())
        .map_err(|_| TiingoFundNavMapError::Allocation)?;
    let maximum_observations = expected_row_count
        .checked_add(completed.pages().len())
        .ok_or(TiingoFundNavMapError::Allocation)?;
    if observations.len() > maximum_observations {
        return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
    }
    if let Some(first) = observations.first() {
        if observations.iter().any(|observation| {
            observation.provenance().instrument_id() != first.provenance().instrument_id()
                || observation.provider_instrument_id() != first.provider_instrument_id()
                || observation.instrument_reference_revision()
                    != first.instrument_reference_revision()
                || observation.provider_product() != first.provider_product()
                || observation.provider_channel() != first.provider_channel()
                || observation.currency() != first.currency()
                || observation.sealed_metadata_capture_receipt()
                    != first.sealed_metadata_capture_receipt()
        }) {
            return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
        }
    }
    let mut expected_rows = Vec::new();
    expected_rows
        .try_reserve_exact(expected_row_count)
        .map_err(|_| TiingoFundNavMapError::Allocation)?;
    for page in completed.pages() {
        for (row_index, row_digest) in page.row_digests().iter().copied().enumerate() {
            expected_rows.push(TiingoNavHistoryRowCoordinate {
                page_identity: page.page_identity().bytes(),
                row_index: u32::try_from(row_index)
                    .map_err(|_| TiingoFundNavMapError::Allocation)?,
                row_digest: row_digest.bytes(),
            });
        }
    }

    let mut actual_rows = Vec::new();
    actual_rows
        .try_reserve_exact(expected_row_count)
        .map_err(|_| TiingoFundNavMapError::Allocation)?;
    let mut empty_page_observations = Vec::new();
    empty_page_observations
        .try_reserve_exact(completed.pages().len())
        .map_err(|_| TiingoFundNavMapError::Allocation)?;
    let mut resolved_instrument = None;
    for observation in observations {
        let Some(page_identity) = observation.history_page_identity() else {
            return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
        };
        if observation.history_completion_identity() != Some(completed.completion_identity())
            || observation.lineage().page_identity() != Some(page_identity)
            || observation.lineage().checkpoint_identity() != completed.completion_identity()
            || observation.lineage().completeness() != FundNavCompleteness::Complete
            || observation.provider_instrument_id().as_str()
                != completed.plan().ticker().as_str()
            || observation.provenance().source_id() != completed.source_id()
        {
            return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
        }
        let instrument_id = observation
            .provenance()
            .instrument_id()
            .ok_or(TiingoFundNavMapError::IncompleteHistoryMapping)?;
        if resolved_instrument.replace(instrument_id).is_some_and(|prior| prior != instrument_id) {
            return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
        }
        let page = completed
            .pages()
            .iter()
            .find(|page| page.page_identity() == page_identity)
            .ok_or(TiingoFundNavMapError::IncompleteHistoryMapping)?;
        if page.request().request_identity() != observation.response_request_identity()
            || page.sealed_capture_receipt() != observation.sealed_capture_receipt()
        {
            return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
        }
        match (
            observation.provider_row_index(),
            observation.provider_row_digest(),
        ) {
            (Some(row_index), Some(row_digest)) => {
                if actual_rows.len() == expected_row_count {
                    return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
                }
                actual_rows.push(TiingoNavHistoryRowCoordinate {
                    page_identity: page_identity.bytes(),
                    row_index,
                    row_digest: row_digest.bytes(),
                });
            }
            (None, None)
                if page.row_digests().is_empty()
                    && matches!(
                        observation.value(),
                        FundNavValue::Missing(
                            FundNavMissingState::Unsupported
                                | FundNavMissingState::SourceMissing
                        )
                    ) =>
            {
                if empty_page_observations.len() == completed.pages().len() {
                    return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
                }
                empty_page_observations.push(page_identity.bytes());
            }
            _ => return Err(TiingoFundNavMapError::IncompleteHistoryMapping),
        }
    }
    expected_rows.sort_unstable();
    actual_rows.sort_unstable();
    empty_page_observations.sort_unstable();
    if expected_rows != actual_rows
        || empty_page_observations
            .windows(2)
            .any(|window| window[0] == window[1])
    {
        return Err(TiingoFundNavMapError::IncompleteHistoryMapping);
    }
    Ok(())
}

fn completed_nav_history_handoff_identity(
    completed: &TiingoCompletedHistoryCapture,
    observations: &[TiingoFundNavCanonicalCandidate],
    financial_coverage: TiingoFundNavHistoryFinancialCoverage,
) -> Result<EvidenceDigest, TiingoFundNavMapError> {
    let mut hasher = Sha256::new();
    append_field(
        &mut hasher,
        b"market-squawk/tiingo/completed-fund-nav-history-candidate/v2",
    );
    for identity in [
        completed.plan().request_set_identity(),
        completed.checkpoint_receipt_identity(),
        completed.completion_identity(),
    ] {
        append_field(&mut hasher, &identity.bytes());
    }
    append_field(&mut hasher, &completed.total_rows().to_be_bytes());
    append_field(&mut hasher, &completed.total_response_bytes().to_be_bytes());
    append_field(
        &mut hasher,
        &u64::try_from(observations.len())
            .map_err(|_| TiingoFundNavMapError::Allocation)?
            .to_be_bytes(),
    );
    for observation in observations {
        append_field(&mut hasher, &observation.handoff_identity().bytes());
    }
    append_field(
        &mut hasher,
        match financial_coverage {
            TiingoFundNavHistoryFinancialCoverage::ExpectedFinancialDatesUnavailable => {
                b"expected-financial-dates-unavailable"
            }
        },
    );
    Ok(EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

/// Maps one sealed strict Tiingo result into a pre-publication canonical FundNav candidate.
///
/// Equity/ETF rows cannot reach this function through `normalize_mutual_fund_row`, which admits
/// only exact `MF` metadata. The mapper additionally revalidates the sealed request/body/clock and
/// disposition bindings. It never turns adjusted OHLC into NAV and never accepts publication or
/// revision facts from a provider-layer caller.
pub fn map_fund_nav_candidate(
    input: TiingoFundNavMappingInput<'_>,
) -> Result<TiingoFundNavCanonicalCandidate, TiingoFundNavMapError> {
    validate_contract_binding(&input)?;
    validate_capture(&input)?;
    let history_binding = validate_history_binding(&input)?;
    validate_chronology(&input)?;
    validate_disposition(input.candidate)?;

    let source_identifier = SourceIdentifier::try_from(format!(
        "tiingo-nav:{}:{}",
        input.candidate.context().ticker(),
        input.candidate.nav_date()
    ))
    .map_err(|_| TiingoFundNavMapError::InvalidContractIdentity)?;
    // When no provider row exists, the exact complete response body is the raw evidence for the
    // closed absence state; it is not reinterpreted as a fabricated row or zero NAV.
    let payload_digest = input
        .candidate
        .provider_row_digest()
        .unwrap_or(input.candidate.raw_object_digest());
    let provenance = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: input.contract.source_id().clone(),
        instrument_id: Some(input.candidate.context().instrument_id()),
        venue_id: None,
        source_identifier,
        source_timestamp: None,
        received_at: input.candidate.clocks().received_at(),
        ingested_at: input.ingested_at,
        quality: DataQuality::Aggregated,
        payload_reference: PayloadReference::ContentHash(PayloadHash::new(
            payload_digest.algorithm(),
            payload_digest.bytes(),
        )),
        availability: input.candidate.clocks().availability().clone(),
    })?;
    let native_schema = FundNavNativeSchema::new(
        input.contract.source_contract_revision().clone(),
        input.contract.source_contract_evidence().clone(),
        identifier(TIINGO_NAV_NATIVE_SCHEMA)?,
        MetadataRevision::new(input.candidate.context().native_schema_revision().clone()),
        input.contract.native_schema_evidence().clone(),
    );
    let raw_object = ExactPayloadEvidence::with_version_pinned_locator(
        input.candidate.raw_object_digest(),
        VersionPinnedSourceLocator::new(
            identifier(&format!(
                "tiingo-response:{}",
                input.candidate.context().ticker()
            ))?,
            input.candidate.context().entitlement_generation().clone(),
        ),
    );
    let raw_row = ExactPayloadEvidence::with_version_pinned_locator(
        payload_digest,
        VersionPinnedSourceLocator::new(
            identifier(&format!(
                "tiingo-nav-row:{}:{}",
                input.candidate.context().ticker(),
                input.candidate.nav_date()
            ))?,
            input
                .candidate
                .context()
                .mutual_fund_classification()
                .metadata_revision()
                .as_source_identifier()
                .clone(),
        ),
    );
    let (value, completeness, disposition) = canonical_value(input.candidate.value());
    let page_identity = history_binding.map(|binding| binding.page_identity);
    let checkpoint_identity = history_binding.map_or_else(
        || input.sealed_capture.receipt_digest(),
        |binding| binding.completion_identity,
    );
    let lineage = FundNavLineage::try_new(
        native_schema,
        FundNavEntitlementEvidence::Gated {
            generation: input.contract.entitlement_generation(),
            evidence: input.contract.entitlement_evidence(),
        },
        input.candidate.request_identity(),
        raw_object,
        raw_row,
        page_identity,
        checkpoint_identity,
        completeness,
        disposition,
    )?;
    let sealed_capture_receipt = input.sealed_capture.receipt_digest();
    let sealed_metadata_capture_receipt = input.sealed_metadata_capture.receipt_digest();
    let handoff_identity = nav_handoff_identity(
        &input,
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
        history_binding,
    );
    Ok(TiingoFundNavCanonicalCandidate {
        provenance,
        effective: ResearchTemporalCoordinate::calendar_date(input.candidate.nav_date()),
        provider_instrument_id: input.candidate.context().provider_instrument_id().clone(),
        instrument_reference_revision: input
            .candidate
            .context()
            .instrument_definition()
            .metadata_revision()
            .clone(),
        provider_product: ProviderProduct::new(identifier(TIINGO_PROVIDER_PRODUCT)?),
        provider_channel: ProviderChannel::new(identifier(TIINGO_PROVIDER_CHANNEL)?),
        nav_date: input.candidate.nav_date(),
        currency: input.candidate.context().currency(),
        value,
        lineage,
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
        response_request_identity: input.candidate.request_identity(),
        provider_row_index: input.candidate.provider_row_index(),
        provider_row_digest: input.candidate.provider_row_digest(),
        history_page_identity: history_binding.map(|binding| binding.page_identity),
        history_completion_identity: history_binding
            .map(|binding| binding.completion_identity),
        handoff_identity,
    })
}

#[derive(Clone, Copy)]
struct TiingoNavHistoryBinding {
    page_identity: EvidenceDigest,
    completion_identity: EvidenceDigest,
}

fn validate_contract_binding(
    input: &TiingoFundNavMappingInput<'_>,
) -> Result<(), TiingoFundNavMapError> {
    if input.candidate.context().native_schema_revision()
        != input.contract.native_schema_revision()
        || input.candidate.context().entitlement_generation()
            != input.contract.entitlement_generation_identity()
    {
        return Err(TiingoFundNavMapError::InvalidContractEvidence);
    }
    Ok(())
}

fn validate_history_binding(
    input: &TiingoFundNavMappingInput<'_>,
) -> Result<Option<TiingoNavHistoryBinding>, TiingoFundNavMapError> {
    match (
        input.candidate.response_endpoint(),
        input.candidate.pagination(),
        input.completed_history,
    ) {
        (TiingoEndpointFamily::LatestDailyPrices, TiingoPaginationEvidence::NotApplicable, None)
            if input.candidate.provider_row_index().is_some()
                && input.candidate.provider_row_digest().is_some() =>
        {
            Ok(None)
        }
        (
            TiingoEndpointFamily::HistoricalDailyPrices,
            TiingoPaginationEvidence::ApplicationDateWindow(expected_application_page),
            Some(completed),
        ) => {
            if completed.source_id() != input.contract.source_id()
                || completed.source_contract_revision()
                    != input.contract.source_contract_revision()
                || completed.native_contract_revision()
                    != input.contract.native_schema_revision()
                || completed.entitlement_generation()
                    != input.contract.entitlement_generation_identity()
                || completed.plan().ticker() != input.candidate.context().ticker()
            {
                return Err(TiingoFundNavMapError::HistoryCaptureMismatch);
            }
            let page = completed
                .pages()
                .iter()
                .find(|page| {
                    page.request().request_identity() == input.candidate.request_identity()
                })
                .ok_or(TiingoFundNavMapError::HistoryCaptureMismatch)?;
            let TiingoRequestScope::History {
                start_date,
                end_date,
                page: actual_application_page,
            } = page.request().scope()
            else {
                return Err(TiingoFundNavMapError::HistoryCaptureMismatch);
            };
            if *actual_application_page != expected_application_page
                || input.candidate.nav_date() < *start_date
                || input.candidate.nav_date() > *end_date
                || page.response_body_digest() != input.candidate.raw_object_digest()
                || page.response_status() != input.candidate.response_status()
                || page.response_bytes()
                    != input.candidate.request_disposition().response_bytes()
                || page.received_at() != input.candidate.clocks().received_at()
                || page.decoded_at() != input.candidate.clocks().decoded_at()
                || page.sealed_capture_receipt() != input.sealed_capture.receipt_digest()
            {
                return Err(TiingoFundNavMapError::HistoryCaptureMismatch);
            }
            let no_row_candidate = input.candidate.provider_row_index().is_none()
                && input.candidate.provider_row_digest().is_none();
            if no_row_candidate
                && (*start_date != input.candidate.nav_date()
                    || *end_date != input.candidate.nav_date())
            {
                return Err(TiingoFundNavMapError::HistoryCaptureMismatch);
            }
            match (
                input.candidate.provider_row_index(),
                input.candidate.provider_row_digest(),
            ) {
                (Some(index), Some(digest)) => {
                    let index = usize::try_from(index)
                        .map_err(|_| TiingoFundNavMapError::HistoryCaptureMismatch)?;
                    if page.row_digests().get(index).copied() != Some(digest) {
                        return Err(TiingoFundNavMapError::HistoryCaptureMismatch);
                    }
                }
                (None, None)
                    if page.row_digests().is_empty()
                        && matches!(
                            input.candidate.value(),
                            TiingoNavValueState::Unsupported | TiingoNavValueState::SourceMissing
                        )
                        && start_date == end_date => {}
                _ => return Err(TiingoFundNavMapError::HistoryCaptureMismatch),
            }
            Ok(Some(TiingoNavHistoryBinding {
                page_identity: page.page_identity(),
                completion_identity: completed.completion_identity(),
            }))
        }
        _ => Err(TiingoFundNavMapError::HistoryCaptureMismatch),
    }
}

fn nav_handoff_identity(
    input: &TiingoFundNavMappingInput<'_>,
    sealed_capture_receipt: EvidenceDigest,
    sealed_metadata_capture_receipt: EvidenceDigest,
    history_binding: Option<TiingoNavHistoryBinding>,
) -> EvidenceDigest {
    let mut hasher = Sha256::new();
    append_field(&mut hasher, b"market-squawk/tiingo/nav-canonical-candidate/v3");
    for digest in [
        input.candidate.family_identity(),
        input.candidate.payload_identity(),
        input.candidate.provenance_identity(),
        input.candidate.request_identity(),
        input.candidate.raw_object_digest(),
        input.candidate.metadata_request_identity(),
        input.candidate.metadata_raw_object_digest(),
        input.contract.source_contract_evidence().content_digest(),
        input.contract.native_schema_evidence().content_digest(),
        input.contract.entitlement_evidence(),
        sealed_capture_receipt,
        sealed_metadata_capture_receipt,
    ] {
        append_field(&mut hasher, &digest.bytes());
    }
    if let Some(history_binding) = history_binding {
        append_field(&mut hasher, b"completed-history");
        append_field(&mut hasher, &history_binding.page_identity.bytes());
        append_field(
            &mut hasher,
            &history_binding.completion_identity.bytes(),
        );
    } else {
        append_field(&mut hasher, b"latest-response");
    }
    match (
        input.candidate.provider_row_index(),
        input.candidate.provider_row_digest(),
    ) {
        (Some(index), Some(digest)) => {
            append_field(&mut hasher, &index.to_be_bytes());
            append_field(&mut hasher, &digest.bytes());
        }
        _ => append_field(&mut hasher, b"no-provider-row"),
    }
    append_field(
        &mut hasher,
        input.contract.source_id().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        input
            .contract
            .source_contract_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    append_field(
        &mut hasher,
        &input.contract.entitlement_generation().get().to_be_bytes(),
    );
    append_field(
        &mut hasher,
        input.contract.native_schema_revision().as_str().as_bytes(),
    );
    append_field(
        &mut hasher,
        input
            .contract
            .entitlement_generation_identity()
            .as_str()
            .as_bytes(),
    );
    append_field(&mut hasher, TIINGO_PROVIDER_PRODUCT.as_bytes());
    append_field(&mut hasher, TIINGO_PROVIDER_CHANNEL.as_bytes());
    append_field(&mut hasher, b"per-share");
    append_field(&mut hasher, &input.ingested_at.unix_nanos().to_be_bytes());
    EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    )
}

fn append_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn validate_capture(input: &TiingoFundNavMappingInput<'_>) -> Result<(), TiingoFundNavMapError> {
    let capture = input.sealed_capture.capture();
    let Some(page) = capture.pages().first() else {
        return Err(TiingoFundNavMapError::CaptureMismatch);
    };
    let metadata_capture = input.sealed_metadata_capture.capture();
    let Some(metadata_page) = metadata_capture.pages().first() else {
        return Err(TiingoFundNavMapError::CaptureMismatch);
    };
    let expected_dataset = match input.candidate.response_endpoint() {
        crate::TiingoEndpointFamily::LatestDailyPrices => "tiingo-daily-latest",
        crate::TiingoEndpointFamily::HistoricalDailyPrices => "tiingo-daily-history-window",
        crate::TiingoEndpointFamily::Metadata => return Err(TiingoFundNavMapError::CaptureMismatch),
    };
    if capture.pages().len() != 1
        || capture.source_id() != input.contract.source_id()
        || capture.metadata_revision() != input.contract.source_contract_revision()
        || input.candidate.provider_revision() != TiingoProviderRevisionEvidence::NotSupplied
        || capture.dataset().as_str() != expected_dataset
        || capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
        || capture.request_set_identity() != input.candidate.request_identity()
        || capture.total_body_bytes() != input.candidate.request_disposition().response_bytes()
        || page.request_identity() != input.candidate.request_identity()
        || page.http_status() != input.candidate.response_status()
        || page.body_bytes() != input.candidate.request_disposition().response_bytes()
        || page.body_digest() != input.candidate.raw_object_digest()
        || page.received_at() != input.candidate.clocks().received_at()
        || metadata_capture.pages().len() != 1
        || metadata_capture.source_id() != input.contract.source_id()
        || metadata_capture.metadata_revision() != input.contract.source_contract_revision()
        || metadata_capture.dataset().as_str() != "tiingo-daily-metadata"
        || metadata_capture.terminal()
            != ProviderCaptureTerminalDisposition::StandaloneResponse
        || metadata_capture.request_set_identity() != input.candidate.metadata_request_identity()
        || metadata_capture.total_body_bytes() != input.candidate.metadata_response_bytes()
        || metadata_page.request_identity() != input.candidate.metadata_request_identity()
        || metadata_page.http_status() != input.candidate.metadata_response_status()
        || metadata_page.body_bytes() != input.candidate.metadata_response_bytes()
        || metadata_page.body_digest() != input.candidate.metadata_raw_object_digest()
        || metadata_page.received_at() != input.candidate.metadata_received_at()
    {
        return Err(TiingoFundNavMapError::CaptureMismatch);
    }
    Ok(())
}

fn validate_chronology(input: &TiingoFundNavMappingInput<'_>) -> Result<(), TiingoFundNavMapError> {
    let clocks = input.candidate.clocks();
    if clocks.received_at() > clocks.decoded_at()
        || clocks.decoded_at() > input.ingested_at
    {
        return Err(TiingoFundNavMapError::InvalidChronology);
    }
    Ok(())
}

fn validate_disposition(
    candidate: &TiingoNavObservationCandidate,
) -> Result<(), TiingoFundNavMapError> {
    let disposition = candidate.request_disposition();
    if disposition.requested_symbols() != 1
        || disposition.returned_symbols() + disposition.missing_symbols() != 1
        || disposition.response_bytes() == 0
        || !(200..300).contains(&candidate.response_status())
        || !(200..300).contains(&candidate.metadata_response_status())
        || candidate.metadata_response_bytes() == 0
        || candidate.metadata_received_at() > candidate.clocks().received_at()
    {
        return Err(TiingoFundNavMapError::InvalidDisposition);
    }
    let returned_row = match (
        candidate.provider_row_index(),
        candidate.provider_row_digest(),
    ) {
        (Some(index), Some(_)) if index < disposition.returned_rows() => true,
        (None, None) => false,
        _ => return Err(TiingoFundNavMapError::InvalidDisposition),
    };
    let row_count = disposition.returned_rows();
    let consistent = match candidate.value() {
        TiingoNavValueState::Observed(_) | TiingoNavValueState::Invalid(_) => {
            returned_row
                && row_count > 0
                && disposition.returned_symbols() == 1
                && disposition.missing_symbols() == 0
        }
        TiingoNavValueState::SourceMissing => {
            returned_row == (row_count > 0)
                && disposition.returned_symbols() == u16::from(row_count > 0)
                && disposition.missing_symbols() == u16::from(row_count == 0)
        }
        TiingoNavValueState::NotYetPublished
        | TiingoNavValueState::Unsupported
        | TiingoNavValueState::Unavailable => {
            !returned_row
                && row_count == 0
                && disposition.returned_symbols() == 0
                && disposition.missing_symbols() == 1
        }
    };
    if consistent {
        Ok(())
    } else {
        Err(TiingoFundNavMapError::InvalidDisposition)
    }
}

fn canonical_value(
    value: TiingoNavValueState,
) -> (FundNavValue, FundNavCompleteness, FundNavDisposition) {
    match value {
        TiingoNavValueState::Observed(money) => (
            FundNavValue::Observed(money),
            FundNavCompleteness::Complete,
            FundNavDisposition::Returned,
        ),
        TiingoNavValueState::NotYetPublished => (
            FundNavValue::Missing(FundNavMissingState::NotYetPublished),
            FundNavCompleteness::Complete,
            FundNavDisposition::NotYetPublished,
        ),
        TiingoNavValueState::Unsupported => (
            FundNavValue::Missing(FundNavMissingState::Unsupported),
            FundNavCompleteness::Complete,
            FundNavDisposition::Unsupported,
        ),
        TiingoNavValueState::SourceMissing => (
            FundNavValue::Missing(FundNavMissingState::SourceMissing),
            FundNavCompleteness::Complete,
            FundNavDisposition::SourceMissing,
        ),
        TiingoNavValueState::Invalid(_) => (
            FundNavValue::Missing(FundNavMissingState::Invalid),
            FundNavCompleteness::Complete,
            FundNavDisposition::Invalid,
        ),
        TiingoNavValueState::Unavailable => (
            FundNavValue::Missing(FundNavMissingState::Unavailable),
            FundNavCompleteness::Incomplete,
            FundNavDisposition::Unavailable,
        ),
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, TiingoFundNavMapError> {
    SourceIdentifier::try_from(value).map_err(|_| TiingoFundNavMapError::InvalidContractIdentity)
}

/// Closed failure to construct exact canonical Tiingo fund-NAV evidence.
#[derive(Debug, Error)]
pub enum TiingoFundNavMapError {
    /// Source, schema, or entitlement evidence was empty or belonged to another source.
    #[error("Tiingo NAV contract evidence is invalid")]
    InvalidContractEvidence,
    /// The sealed source-neutral receipt does not bind the candidate request/body/clock exactly.
    #[error("sealed Tiingo capture does not match the NAV candidate")]
    CaptureMismatch,
    /// A historical candidate did not bind the exact terminal plan and sealed decoded page.
    #[error("completed Tiingo history capture does not match the NAV candidate")]
    HistoryCaptureMismatch,
    /// The completed NAV-history handoff did not map every returned provider row exactly once.
    #[error("completed Tiingo NAV history mapping is incomplete")]
    IncompleteHistoryMapping,
    /// Decode or local-ingestion clocks regressed.
    #[error("Tiingo NAV pre-publication chronology is invalid")]
    InvalidChronology,
    /// Requested, returned, missing, row, byte, and NAV-state evidence disagree.
    #[error("Tiingo NAV request disposition is inconsistent")]
    InvalidDisposition,
    /// A code-owned source/product/channel/schema identity could not satisfy domain bounds.
    #[error("Tiingo NAV canonical contract identity is invalid")]
    InvalidContractIdentity,
    /// Bounded NAV-history evidence allocation failed.
    #[error("Tiingo NAV history evidence allocation failed")]
    Allocation,
    /// Canonical provenance or time invariants rejected the supplied evidence.
    #[error(transparent)]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
    /// Canonical FundNav invariants rejected the supplied evidence.
    #[error(transparent)]
    Research(#[from] market_squawk_domain::ResearchError),
}
