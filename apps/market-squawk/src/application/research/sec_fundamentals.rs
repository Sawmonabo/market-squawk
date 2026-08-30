//! Exact-generation SEC facts and filings research status.
//!
//! This leaf owns no provider, catalog, raw store, or decoder. It composes the common exact-origin
//! SEC point-in-time reader with the authoritative company/security identity reader and reports
//! facts, filings, and ratio availability without ticker inference or derived-value fabrication.

use std::sync::Arc;
use std::time::Instant;

use market_squawk_data::{
    CompanySecurityIdentityCatalogError, CompanySecurityIdentityReadCapability, DatasetManifestRef,
    MAX_SEC_RESEARCH_OBJECT_BYTES, PointInTimeLimits, PointInTimeRevisionMode,
    SecFundamentalIdentityAvailability, SecFundamentalIdentityQuery,
    SecFundamentalIdentitySelection, SecResearchDisposition, SecResearchFamily,
    SecResearchReadError, SecResearchReadRequest, SecResearchSelection,
};
use market_squawk_domain::{
    CalendarDate, CompanyIdentitySurface, DigestAlgorithm, EvidenceDigest, FundamentalPeriod,
    InstrumentId, ResearchContext, ResearchObservation, ResearchTemporalCoordinate, SourceId,
    SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::ResearchService;

/// Fixed read operation consumed by future CLI/MCP/Desktop composition.
pub(crate) const SEC_FUNDAMENTALS_RESEARCH_STATUS_OPERATION: &str =
    "Research.GetSecFundamentalsStatus";

/// Exact durable coordinates for one SEC observation family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecResearchFamilyBinding {
    manifest: DatasetManifestRef,
    family: SecResearchFamily,
    provider_binding_digest: EvidenceDigest,
    company_observation_digest: EvidenceDigest,
}

impl SecResearchFamilyBinding {
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        family: SecResearchFamily,
        provider_binding_digest: EvidenceDigest,
        company_observation_digest: EvidenceDigest,
    ) -> Result<Self, SecFundamentalsResearchError> {
        if [provider_binding_digest, company_observation_digest]
            .into_iter()
            .any(|digest| {
                digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
            })
            || manifest.content_hash().bytes() == [0; 32]
            || manifest.schema().fingerprint() == [0; 32]
        {
            return Err(SecFundamentalsResearchError::InvalidRequest);
        }
        Ok(Self {
            manifest,
            family,
            provider_binding_digest,
            company_observation_digest,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn family(&self) -> SecResearchFamily {
        self.family
    }

    pub(crate) const fn provider_binding_digest(&self) -> EvidenceDigest {
        self.provider_binding_digest
    }

    pub(crate) const fn company_observation_digest(&self) -> EvidenceDigest {
        self.company_observation_digest
    }
}

/// Fixed SEC research request with all four knowledge clocks enforced downstream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SecFundamentalsResearchRequest {
    facts: Option<SecResearchFamilyBinding>,
    filings: Option<SecResearchFamilyBinding>,
    filing_xbrl: Option<SecResearchFamilyBinding>,
    knowledge_at: Timestamp,
    identity_effective_at: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    revision_mode: PointInTimeRevisionMode,
    point_in_time_limits: PointInTimeLimits,
    maximum_object_bytes: usize,
}

impl SecFundamentalsResearchRequest {
    #[allow(
        clippy::too_many_arguments,
        reason = "exact generations, identity/economic clocks, PIT policy, and bounds stay explicit"
    )]
    pub(crate) fn try_new(
        facts: Option<SecResearchFamilyBinding>,
        filings: Option<SecResearchFamilyBinding>,
        filing_xbrl: Option<SecResearchFamilyBinding>,
        knowledge_at: Timestamp,
        identity_effective_at: Timestamp,
        effective_cutoff: ResearchTemporalCoordinate,
        revision_mode: PointInTimeRevisionMode,
        point_in_time_limits: PointInTimeLimits,
        maximum_object_bytes: usize,
    ) -> Result<Self, SecFundamentalsResearchError> {
        if facts.is_none() && filings.is_none() && filing_xbrl.is_none()
            || facts
                .as_ref()
                .is_some_and(|binding| binding.family != SecResearchFamily::CompanyFacts)
            || filings
                .as_ref()
                .is_some_and(|binding| binding.family != SecResearchFamily::Submissions)
            || filing_xbrl
                .as_ref()
                .is_some_and(|binding| binding.family != SecResearchFamily::FilingXbrl)
            || maximum_object_bytes == 0
            || maximum_object_bytes > MAX_SEC_RESEARCH_OBJECT_BYTES
        {
            return Err(SecFundamentalsResearchError::InvalidRequest);
        }
        Ok(Self {
            facts,
            filings,
            filing_xbrl,
            knowledge_at,
            identity_effective_at,
            effective_cutoff,
            revision_mode,
            point_in_time_limits,
            maximum_object_bytes,
        })
    }

    pub(crate) const fn facts(&self) -> Option<&SecResearchFamilyBinding> {
        self.facts.as_ref()
    }

    pub(crate) const fn filings(&self) -> Option<&SecResearchFamilyBinding> {
        self.filings.as_ref()
    }

    pub(crate) const fn filing_xbrl(&self) -> Option<&SecResearchFamilyBinding> {
        self.filing_xbrl.as_ref()
    }

    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn identity_effective_at(&self) -> Timestamp {
        self.identity_effective_at
    }

    pub(crate) const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }
}

/// Read-only composition over the repository's sole analytical and identity authorities.
#[derive(Clone)]
pub(crate) struct SecFundamentalsResearchOperation {
    research: Arc<ResearchService>,
    identities: CompanySecurityIdentityReadCapability,
}

impl std::fmt::Debug for SecFundamentalsResearchOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecFundamentalsResearchOperation")
            .field("operation", &SEC_FUNDAMENTALS_RESEARCH_STATUS_OPERATION)
            .finish_non_exhaustive()
    }
}

impl SecFundamentalsResearchOperation {
    pub(crate) const fn new(
        research: Arc<ResearchService>,
        identities: CompanySecurityIdentityReadCapability,
    ) -> Self {
        Self {
            research,
            identities,
        }
    }

    /// Reads each supplied exact generation, verifies a restart replay, and then requires the exact
    /// company observation to resolve through authoritative reference evidence.
    pub(crate) async fn read(
        &self,
        request: SecFundamentalsResearchRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecFundamentalsResearchStatus, SecFundamentalsResearchError> {
        check_operation(deadline, &cancellation)?;
        let facts = self
            .read_family(
                request.facts.as_ref(),
                &request,
                deadline,
                cancellation.child_token(),
            )
            .await?;
        let filings = self
            .read_family(
                request.filings.as_ref(),
                &request,
                deadline,
                cancellation.child_token(),
            )
            .await?;
        let filing_xbrl = self
            .read_family(
                request.filing_xbrl.as_ref(),
                &request,
                deadline,
                cancellation.child_token(),
            )
            .await?;
        check_operation(deadline, &cancellation)?;
        let identity = combined_identity(&facts, &filings, &filing_xbrl)?;
        Ok(SecFundamentalsResearchStatus {
            source_id: exact_source(&facts, &filings, &filing_xbrl)?,
            cik: exact_cik(&facts, &filings, &filing_xbrl)?,
            knowledge_at: request.knowledge_at,
            identity_effective_at: request.identity_effective_at,
            effective_cutoff: request.effective_cutoff,
            identity,
            facts,
            filings,
            filing_xbrl,
            ratios: SecRatioResearchStatus::Unavailable(
                SecRatioUnavailableReason::NoTypedRatioEvidence,
            ),
        })
    }

    async fn read_family(
        &self,
        binding: Option<&SecResearchFamilyBinding>,
        request: &SecFundamentalsResearchRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<SecFamilyResearchStatus, SecFundamentalsResearchError> {
        let Some(binding) = binding else {
            return Ok(SecFamilyResearchStatus::Missing);
        };
        check_operation(deadline, &cancellation)?;
        let read_request = SecResearchReadRequest::try_new(
            binding.manifest.clone(),
            binding.family,
            binding.provider_binding_digest,
            binding.company_observation_digest,
            request.knowledge_at,
            request.effective_cutoff.clone(),
            request.revision_mode,
            request.point_in_time_limits,
            request.maximum_object_bytes,
        )?;
        let reader = self.research.analytical().sec_research_reader();
        let raw_store = self.research.provider_capture_store();
        let selection = reader
            .select(
                read_request,
                raw_store.as_ref(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        reader
            .verify_restart(
                &selection,
                raw_store.as_ref(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        let identity = resolve_identity(
            &self.identities,
            &selection,
            request.identity_effective_at,
            request.knowledge_at,
            deadline,
            &cancellation,
        )?;
        classify_family(selection, identity)
    }
}

/// Combined exact SEC research result for one company/security identity.
#[derive(Debug)]
pub(crate) struct SecFundamentalsResearchStatus {
    source_id: SourceId,
    cik: SourceIdentifier,
    knowledge_at: Timestamp,
    identity_effective_at: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    identity: SecCombinedIdentityStatus,
    facts: SecFamilyResearchStatus,
    filings: SecFamilyResearchStatus,
    filing_xbrl: SecFamilyResearchStatus,
    ratios: SecRatioResearchStatus,
}

impl SecFundamentalsResearchStatus {
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }

    pub(crate) const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn identity_effective_at(&self) -> Timestamp {
        self.identity_effective_at
    }

    pub(crate) const fn effective_cutoff(&self) -> &ResearchTemporalCoordinate {
        &self.effective_cutoff
    }

    pub(crate) const fn identity(&self) -> &SecCombinedIdentityStatus {
        &self.identity
    }

    pub(crate) const fn facts(&self) -> &SecFamilyResearchStatus {
        &self.facts
    }

    pub(crate) const fn filings(&self) -> &SecFamilyResearchStatus {
        &self.filings
    }

    pub(crate) const fn filing_xbrl(&self) -> &SecFamilyResearchStatus {
        &self.filing_xbrl
    }

    pub(crate) const fn ratios(&self) -> SecRatioResearchStatus {
        self.ratios
    }

    /// Projects the exact internal evidence into provider-neutral product data. Provider names,
    /// manifests, raw receipts, retry state, and source-native object coordinates remain behind the
    /// research boundary and cannot leak into ordinary product pages through this type.
    pub(crate) fn product_data(&self) -> Result<CompanyResearchData, SecFundamentalsResearchError> {
        let instrument_id = match self.identity {
            SecCombinedIdentityStatus::Available { instrument_id, .. } => Some(instrument_id),
            SecCombinedIdentityStatus::Pending | SecCombinedIdentityStatus::Unavailable => None,
            SecCombinedIdentityStatus::Conflict => None,
        };
        let availability = product_availability(&self.facts, &self.filings, &self.filing_xbrl);
        let mut fundamentals = Vec::new();
        append_product_fundamentals(&mut fundamentals, &self.facts)?;
        append_product_fundamentals(&mut fundamentals, &self.filing_xbrl)?;
        let mut filings = Vec::new();
        append_product_filings(&mut filings, &self.filings)?;
        if matches!(availability, ResearchDataAvailability::Conflict) {
            fundamentals.clear();
            filings.clear();
        }
        Ok(CompanyResearchData {
            instrument_id,
            as_of: self.knowledge_at,
            availability,
            fundamentals: fundamentals.into_boxed_slice(),
            filings: filings.into_boxed_slice(),
            latest_known_at: latest_product_knowledge([
                &self.facts,
                &self.filings,
                &self.filing_xbrl,
            ]),
        })
    }
}

/// Provider-neutral company research data consumed by product composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyResearchData {
    instrument_id: Option<InstrumentId>,
    as_of: Timestamp,
    availability: ResearchDataAvailability,
    fundamentals: Box<[CompanyFundamentalData]>,
    filings: Box<[CompanyFilingData]>,
    latest_known_at: Option<Timestamp>,
}

impl CompanyResearchData {
    pub(crate) const fn instrument_id(&self) -> Option<InstrumentId> {
        self.instrument_id
    }
    pub(crate) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
    pub(crate) const fn availability(&self) -> ResearchDataAvailability {
        self.availability
    }
    pub(crate) fn fundamentals(&self) -> &[CompanyFundamentalData] {
        &self.fundamentals
    }
    pub(crate) fn filings(&self) -> &[CompanyFilingData] {
        &self.filings
    }
    pub(crate) const fn latest_known_at(&self) -> Option<Timestamp> {
        self.latest_known_at
    }
}

/// Product-facing availability without provider or ingestion implementation details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResearchDataAvailability {
    Complete,
    Partial,
    Unavailable,
    Conflict,
}

/// One exact point-in-time fundamental value stripped of provider plumbing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyFundamentalData {
    metric: SourceIdentifier,
    value: Decimal,
    unit: SourceIdentifier,
    period: FundamentalPeriod,
    filed_on: Option<CalendarDate>,
    effective: ResearchTemporalCoordinate,
    known_at: Timestamp,
}

impl CompanyFundamentalData {
    pub(crate) const fn metric(&self) -> &SourceIdentifier {
        &self.metric
    }
    pub(crate) const fn value(&self) -> Decimal {
        self.value
    }
    pub(crate) const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
    pub(crate) const fn period(&self) -> FundamentalPeriod {
        self.period
    }
    pub(crate) const fn filed_on(&self) -> Option<CalendarDate> {
        self.filed_on
    }
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }
    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

/// One point-in-time filing event stripped of provider identifiers and raw coordinates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompanyFilingData {
    form: SourceIdentifier,
    effective: ResearchTemporalCoordinate,
    published: Option<ResearchTemporalCoordinate>,
    known_at: Timestamp,
}

impl CompanyFilingData {
    pub(crate) const fn form(&self) -> &SourceIdentifier {
        &self.form
    }
    pub(crate) const fn effective(&self) -> &ResearchTemporalCoordinate {
        &self.effective
    }
    pub(crate) const fn published(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published.as_ref()
    }
    pub(crate) const fn known_at(&self) -> Timestamp {
        self.known_at
    }
}

/// Family-level availability retaining the exact origin, selection, exclusions, and conflicts.
#[derive(Debug)]
pub(crate) enum SecFamilyResearchStatus {
    Missing,
    Available(SecAvailableResearchFamily),
    Unavailable(SecUnavailableResearchFamily),
    Conflict(SecConflictResearchFamily),
}

impl SecFamilyResearchStatus {
    fn selection(&self) -> Option<&SecResearchSelection> {
        match self {
            Self::Missing => None,
            Self::Available(value) => Some(&value.selection),
            Self::Unavailable(value) => Some(&value.selection),
            Self::Conflict(value) => Some(&value.selection),
        }
    }

    fn identity(&self) -> Option<&SecFundamentalIdentitySelection> {
        match self {
            Self::Missing => None,
            Self::Available(value) => Some(&value.identity),
            Self::Unavailable(value) => Some(&value.identity),
            Self::Conflict(value) => Some(&value.identity),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SecAvailableResearchFamily {
    selection: SecResearchSelection,
    identity: SecFundamentalIdentitySelection,
    clocks: SecFourClockStatus,
}

impl SecAvailableResearchFamily {
    pub(crate) const fn selection(&self) -> &SecResearchSelection {
        &self.selection
    }

    pub(crate) const fn identity(&self) -> &SecFundamentalIdentitySelection {
        &self.identity
    }

    pub(crate) const fn clocks(&self) -> SecFourClockStatus {
        self.clocks
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecFamilyUnavailableReason {
    NoPointInTimeRows,
    CompanyIdentityPending,
    CompanyIdentityUnavailable,
}

#[derive(Debug)]
pub(crate) struct SecUnavailableResearchFamily {
    reason: SecFamilyUnavailableReason,
    selection: SecResearchSelection,
    identity: SecFundamentalIdentitySelection,
    clocks: SecFourClockStatus,
}

impl SecUnavailableResearchFamily {
    pub(crate) const fn reason(&self) -> SecFamilyUnavailableReason {
        self.reason
    }

    pub(crate) const fn selection(&self) -> &SecResearchSelection {
        &self.selection
    }

    pub(crate) const fn identity(&self) -> &SecFundamentalIdentitySelection {
        &self.identity
    }

    pub(crate) const fn clocks(&self) -> SecFourClockStatus {
        self.clocks
    }
}

#[derive(Debug)]
pub(crate) struct SecConflictResearchFamily {
    selection: SecResearchSelection,
    identity: SecFundamentalIdentitySelection,
    clocks: SecFourClockStatus,
}

impl SecConflictResearchFamily {
    pub(crate) const fn selection(&self) -> &SecResearchSelection {
        &self.selection
    }

    pub(crate) const fn identity(&self) -> &SecFundamentalIdentitySelection {
        &self.identity
    }

    pub(crate) const fn clocks(&self) -> SecFourClockStatus {
        self.clocks
    }
}

/// Explicit coverage of availability, receipt, ingestion, and generation-completion clocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SecFourClockStatus {
    knowledge_at: Timestamp,
    latest_selected_availability: Option<Timestamp>,
    latest_selected_receipt: Option<Timestamp>,
    latest_selected_ingestion: Option<Timestamp>,
    generation_completed_at: Timestamp,
    availability_exclusions: usize,
    receipt_exclusions: usize,
    ingestion_exclusions: usize,
    generation_completion_exclusions: usize,
}

impl SecFourClockStatus {
    pub(crate) const fn knowledge_at(self) -> Timestamp {
        self.knowledge_at
    }

    pub(crate) const fn generation_completed_at(self) -> Timestamp {
        self.generation_completed_at
    }

    pub(crate) const fn latest_selected_availability(self) -> Option<Timestamp> {
        self.latest_selected_availability
    }

    pub(crate) const fn latest_selected_receipt(self) -> Option<Timestamp> {
        self.latest_selected_receipt
    }

    pub(crate) const fn latest_selected_ingestion(self) -> Option<Timestamp> {
        self.latest_selected_ingestion
    }

    pub(crate) const fn availability_exclusions(self) -> usize {
        self.availability_exclusions
    }

    pub(crate) const fn receipt_exclusions(self) -> usize {
        self.receipt_exclusions
    }

    pub(crate) const fn ingestion_exclusions(self) -> usize {
        self.ingestion_exclusions
    }

    pub(crate) const fn generation_completion_exclusions(self) -> usize {
        self.generation_completion_exclusions
    }
}

/// Cross-family identity outcome. `Available` is possible only for one authoritative common
/// `InstrumentId` and one exact reference generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecCombinedIdentityStatus {
    Available {
        instrument_id: InstrumentId,
        market_reference_digest: EvidenceDigest,
    },
    Pending,
    Unavailable,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecRatioResearchStatus {
    Unavailable(SecRatioUnavailableReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecRatioUnavailableReason {
    /// No typed, exact-manifest ratio evidence or calculation receipt exists at this layer.
    NoTypedRatioEvidence,
}

fn product_availability(
    facts: &SecFamilyResearchStatus,
    filings: &SecFamilyResearchStatus,
    filing_xbrl: &SecFamilyResearchStatus,
) -> ResearchDataAvailability {
    let families = [facts, filings, filing_xbrl];
    if families
        .iter()
        .any(|family| matches!(family, SecFamilyResearchStatus::Conflict(_)))
    {
        return ResearchDataAvailability::Conflict;
    }
    let available = families
        .iter()
        .filter(|family| matches!(family, SecFamilyResearchStatus::Available(_)))
        .count();
    if available >= 2
        && matches!(facts, SecFamilyResearchStatus::Available(_))
        && matches!(filings, SecFamilyResearchStatus::Available(_))
    {
        ResearchDataAvailability::Complete
    } else if available > 0 {
        ResearchDataAvailability::Partial
    } else {
        ResearchDataAvailability::Unavailable
    }
}

fn append_product_fundamentals(
    output: &mut Vec<CompanyFundamentalData>,
    family: &SecFamilyResearchStatus,
) -> Result<(), SecFundamentalsResearchError> {
    let SecFamilyResearchStatus::Available(available) = family else {
        return Ok(());
    };
    for selected in available.selection.selected() {
        let row = usize::try_from(selected.row().row_ordinal())
            .map_err(|_| SecFundamentalsResearchError::CountOverflow)?;
        let observation = available
            .selection
            .decoded_rows()
            .get(row)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        let ResearchObservation::Fundamental(fundamental) = observation else {
            continue;
        };
        let provenance = fundamental.context().provenance();
        output.push(CompanyFundamentalData {
            metric: fundamental.concept().clone(),
            value: fundamental.value(),
            unit: fundamental.unit().clone(),
            period: fundamental.fact_context().period(),
            filed_on: fundamental.fact_context().filed_on(),
            effective: fundamental.context().time().effective().clone(),
            known_at: provenance
                .availability()
                .conservative_available_at()
                .ok_or(SecFundamentalsResearchError::IdentityMismatch)?,
        });
    }
    Ok(())
}

fn append_product_filings(
    output: &mut Vec<CompanyFilingData>,
    family: &SecFamilyResearchStatus,
) -> Result<(), SecFundamentalsResearchError> {
    let SecFamilyResearchStatus::Available(available) = family else {
        return Ok(());
    };
    for selected in available.selection.selected() {
        let row = usize::try_from(selected.row().row_ordinal())
            .map_err(|_| SecFundamentalsResearchError::CountOverflow)?;
        let observation = available
            .selection
            .decoded_rows()
            .get(row)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        let ResearchObservation::Filing(filing) = observation else {
            continue;
        };
        let provenance = filing.context().provenance();
        output.push(CompanyFilingData {
            form: filing.form_type().clone(),
            effective: filing.context().time().effective().clone(),
            published: filing.context().time().published().cloned(),
            known_at: provenance
                .availability()
                .conservative_available_at()
                .ok_or(SecFundamentalsResearchError::IdentityMismatch)?,
        });
    }
    Ok(())
}

fn latest_product_knowledge(families: [&SecFamilyResearchStatus; 3]) -> Option<Timestamp> {
    families
        .into_iter()
        .filter_map(|family| match family {
            SecFamilyResearchStatus::Available(value) => {
                value.clocks.latest_selected_availability()
            }
            SecFamilyResearchStatus::Unavailable(value) => {
                value.clocks.latest_selected_availability()
            }
            SecFamilyResearchStatus::Conflict(value) => value.clocks.latest_selected_availability(),
            SecFamilyResearchStatus::Missing => None,
        })
        .max()
}

fn classify_family(
    selection: SecResearchSelection,
    identity: SecFundamentalIdentitySelection,
) -> Result<SecFamilyResearchStatus, SecFundamentalsResearchError> {
    let clocks = four_clock_status(&selection)?;
    match (selection.disposition(), identity.availability()) {
        (SecResearchDisposition::Conflict, _)
        | (_, SecFundamentalIdentityAvailability::Conflict) => Ok(
            SecFamilyResearchStatus::Conflict(SecConflictResearchFamily {
                selection,
                identity,
                clocks,
            }),
        ),
        (SecResearchDisposition::Unavailable, _) => Ok(SecFamilyResearchStatus::Unavailable(
            SecUnavailableResearchFamily {
                reason: SecFamilyUnavailableReason::NoPointInTimeRows,
                selection,
                identity,
                clocks,
            },
        )),
        (SecResearchDisposition::Selected, SecFundamentalIdentityAvailability::IdentityPending) => {
            Ok(SecFamilyResearchStatus::Unavailable(
                SecUnavailableResearchFamily {
                    reason: SecFamilyUnavailableReason::CompanyIdentityPending,
                    selection,
                    identity,
                    clocks,
                },
            ))
        }
        (SecResearchDisposition::Selected, SecFundamentalIdentityAvailability::Unavailable) => Ok(
            SecFamilyResearchStatus::Unavailable(SecUnavailableResearchFamily {
                reason: SecFamilyUnavailableReason::CompanyIdentityUnavailable,
                selection,
                identity,
                clocks,
            }),
        ),
        (SecResearchDisposition::Selected, SecFundamentalIdentityAvailability::Available) => {
            validate_selected_instrument(&selection, &identity)?;
            Ok(SecFamilyResearchStatus::Available(
                SecAvailableResearchFamily {
                    selection,
                    identity,
                    clocks,
                },
            ))
        }
    }
}

fn resolve_identity(
    reader: &CompanySecurityIdentityReadCapability,
    selection: &SecResearchSelection,
    effective_at: Timestamp,
    knowledge_at: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<SecFundamentalIdentitySelection, SecFundamentalsResearchError> {
    let company = selection.company_identity();
    let observation = company.observation();
    let expected_surface = match selection.request().family() {
        SecResearchFamily::Submissions => CompanyIdentitySurface::SecSubmissions,
        SecResearchFamily::CompanyFacts => CompanyIdentitySurface::SecCompanyFacts,
        SecResearchFamily::FilingXbrl => CompanyIdentitySurface::SecSubmissions,
    };
    if observation.source_id() != selection.origin().source_id()
        || observation.surface() != expected_surface
        || company.observation_digest() != selection.request().company_observation_digest()
        || company.provider_binding_digest() != Some(selection.request().provider_binding_digest())
    {
        return Err(SecFundamentalsResearchError::IdentityMismatch);
    }
    let query = SecFundamentalIdentityQuery::try_new(
        selection.origin().source_id().clone(),
        observation.provider_company_id().clone(),
        expected_surface,
        company.observation_digest(),
        effective_at,
        knowledge_at,
    )?;
    let identity = reader.sec_fundamental_identity_as_of(&query, deadline, cancellation)?;
    if identity.company_observation_digest() != company.observation_digest()
        || identity.query_digest().bytes() == [0; 32]
        || identity.receipt_digest().bytes() == [0; 32]
    {
        return Err(SecFundamentalsResearchError::IdentityMismatch);
    }
    Ok(identity)
}

fn validate_selected_instrument(
    selection: &SecResearchSelection,
    identity: &SecFundamentalIdentitySelection,
) -> Result<(), SecFundamentalsResearchError> {
    let instrument = identity
        .instrument_id()
        .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
    if identity.market_instrument_revision_digest().is_none() || identity.relationship().is_none() {
        return Err(SecFundamentalsResearchError::IdentityMismatch);
    }
    for selected in selection.selected() {
        let observation = selection
            .decoded_rows()
            .get(
                usize::try_from(selected.row().row_ordinal())
                    .map_err(|_| SecFundamentalsResearchError::IdentityMismatch)?,
            )
            .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
        if observation_context(observation)
            .provenance()
            .instrument_id()
            != Some(instrument)
        {
            return Err(SecFundamentalsResearchError::IdentityMismatch);
        }
    }
    Ok(())
}

fn combined_identity(
    facts: &SecFamilyResearchStatus,
    filings: &SecFamilyResearchStatus,
    filing_xbrl: &SecFamilyResearchStatus,
) -> Result<SecCombinedIdentityStatus, SecFundamentalsResearchError> {
    let identities = [facts.identity(), filings.identity(), filing_xbrl.identity()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if identities.is_empty() {
        return Err(SecFundamentalsResearchError::IdentityMismatch);
    }
    if identities
        .iter()
        .any(|identity| identity.availability() == SecFundamentalIdentityAvailability::Conflict)
    {
        return Ok(SecCombinedIdentityStatus::Conflict);
    }
    if identities
        .iter()
        .any(|identity| identity.availability() == SecFundamentalIdentityAvailability::Unavailable)
    {
        return Ok(SecCombinedIdentityStatus::Unavailable);
    }
    if identities.iter().any(|identity| {
        identity.availability() == SecFundamentalIdentityAvailability::IdentityPending
    }) {
        return Ok(SecCombinedIdentityStatus::Pending);
    }
    let first = identities
        .first()
        .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
    let instrument_id = first
        .instrument_id()
        .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
    let market_reference_digest = first
        .market_instrument_revision_digest()
        .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
    if identities.iter().any(|identity| {
        identity.instrument_id() != Some(instrument_id)
            || identity.market_instrument_revision_digest() != Some(market_reference_digest)
    }) {
        return Ok(SecCombinedIdentityStatus::Conflict);
    }
    Ok(SecCombinedIdentityStatus::Available {
        instrument_id,
        market_reference_digest,
    })
}

fn exact_source(
    facts: &SecFamilyResearchStatus,
    filings: &SecFamilyResearchStatus,
    filing_xbrl: &SecFamilyResearchStatus,
) -> Result<SourceId, SecFundamentalsResearchError> {
    let sources = [
        facts.selection(),
        filings.selection(),
        filing_xbrl.selection(),
    ]
    .into_iter()
    .flatten()
    .map(|selection| selection.origin().source_id())
    .collect::<Vec<_>>();
    let first = sources
        .first()
        .ok_or(SecFundamentalsResearchError::InvalidRequest)?;
    if sources.iter().any(|source| *source != *first) {
        return Err(SecFundamentalsResearchError::SourceMismatch);
    }
    Ok((*first).clone())
}

fn exact_cik(
    facts: &SecFamilyResearchStatus,
    filings: &SecFamilyResearchStatus,
    filing_xbrl: &SecFamilyResearchStatus,
) -> Result<SourceIdentifier, SecFundamentalsResearchError> {
    let ciks = [
        facts.selection(),
        filings.selection(),
        filing_xbrl.selection(),
    ]
    .into_iter()
    .flatten()
    .map(|selection| {
        selection
            .company_identity()
            .observation()
            .provider_company_id()
    })
    .collect::<Vec<_>>();
    let first = ciks
        .first()
        .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
    if ciks.iter().any(|cik| *cik != *first) {
        return Err(SecFundamentalsResearchError::IdentityMismatch);
    }
    Ok((*first).clone())
}

fn four_clock_status(
    selection: &SecResearchSelection,
) -> Result<SecFourClockStatus, SecFundamentalsResearchError> {
    let mut latest_selected_availability = None;
    let mut latest_selected_receipt = None;
    let mut latest_selected_ingestion = None;
    for selected in selection.selected() {
        let row = usize::try_from(selected.row().row_ordinal())
            .map_err(|_| SecFundamentalsResearchError::CountOverflow)?;
        let observation = selection
            .decoded_rows()
            .get(row)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        let provenance = observation_context(observation).provenance();
        let available = provenance
            .availability()
            .conservative_available_at()
            .ok_or(SecFundamentalsResearchError::IdentityMismatch)?;
        latest_selected_availability = Some(
            latest_selected_availability
                .map_or(available, |current: Timestamp| current.max(available)),
        );
        latest_selected_receipt = Some(
            latest_selected_receipt.map_or(provenance.received_at(), |current: Timestamp| {
                current.max(provenance.received_at())
            }),
        );
        latest_selected_ingestion = Some(
            latest_selected_ingestion.map_or(provenance.ingested_at(), |current: Timestamp| {
                current.max(provenance.ingested_at())
            }),
        );
    }
    let mut availability_exclusions = 0_usize;
    let mut receipt_exclusions = 0_usize;
    let mut ingestion_exclusions = 0_usize;
    let mut generation_completion_exclusions = 0_usize;
    for exclusion in selection.exclusions() {
        availability_exclusions = availability_exclusions
            .checked_add(exclusion.knowledge().available_after_cutoff() as usize)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        receipt_exclusions = receipt_exclusions
            .checked_add(exclusion.knowledge().received_after_cutoff() as usize)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        ingestion_exclusions = ingestion_exclusions
            .checked_add(exclusion.knowledge().ingested_after_cutoff() as usize)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
        generation_completion_exclusions = generation_completion_exclusions
            .checked_add(exclusion.knowledge().generation_completed_after_cutoff() as usize)
            .ok_or(SecFundamentalsResearchError::CountOverflow)?;
    }
    Ok(SecFourClockStatus {
        knowledge_at: selection.request().knowledge_at(),
        latest_selected_availability,
        latest_selected_receipt,
        latest_selected_ingestion,
        generation_completed_at: selection.origin().generation_completed_at(),
        availability_exclusions,
        receipt_exclusions,
        ingestion_exclusions,
        generation_completion_exclusions,
    })
}

fn observation_context(observation: &ResearchObservation) -> &ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        _ => unreachable!("common SEC reader admits only its requested closed family"),
    }
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecFundamentalsResearchError> {
    if cancellation.is_cancelled() {
        Err(SecFundamentalsResearchError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SecFundamentalsResearchError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub(crate) enum SecFundamentalsResearchError {
    #[error("SEC fundamentals research request is invalid")]
    InvalidRequest,
    #[error("SEC facts and filings do not belong to the same SEC source")]
    SourceMismatch,
    #[error("SEC company/security identity does not match the exact research generation")]
    IdentityMismatch,
    #[error("SEC research status count overflowed")]
    CountOverflow,
    #[error("SEC fundamentals research was cancelled")]
    Cancelled,
    #[error("SEC fundamentals research deadline elapsed")]
    DeadlineExceeded,
    #[error(transparent)]
    Read(#[from] SecResearchReadError),
    #[error(transparent)]
    Identity(#[from] CompanySecurityIdentityCatalogError),
}
