//! Provider-neutral, point-in-time option context for research and volatility features.
//!
//! The leaf reads only already-canonical option observations and the official option-reference
//! catalog. Provider, venue, manifest, source, and entitlement coordinates remain in the private
//! evidence receipt; the product-facing result contains stable instrument identity, economic
//! terms, exact clocks, component availability, and bounded quality only.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU16,
    sync::Arc,
    time::Instant,
};

use market_squawk_data::{
    DatasetId, DatasetManifestRef, IngestError, OfficialOptionsReferenceCanonicalResolution,
    OfficialOptionsReferenceCatalogReadCapability, OfficialOptionsReferenceCatalogResolution,
    OfficialOptionsReferenceError, OfficialOptionsReferenceGenerationSelection,
    OfficialOptionsReferenceIdentityQuery, OfficialOptionsReferenceIdentityResolution,
    OfficialOptionsReferenceRecordValue, OptionMarketPointInTimeRequest,
    OptionMarketPointInTimeSelection,
};
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId, MetadataRevision, Money,
    OccOptionIdentity, OptionComponent, OptionComponentState, OptionExerciseStyle, OptionKind,
    OptionSettlementKind, OptionSnapshotObservation, ProviderChannel, ProviderProduct,
    QuantityLots, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    OptionExpirationRange, OptionMarketBatchDisposition, OptionMarketBatchKind,
    OptionMarketCompleteness, OptionMarketCursorState, OptionMarketRequestFilter,
    OptionStrikeRange,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::ResearchService;

const MAX_OPTIONS_CONTEXT_DATASETS: usize = 16;
const MAX_OPTIONS_CONTEXT_CONTRACTS: usize = 512;
const MAX_OPTIONS_CONTEXT_CANDIDATES: usize = 4_096;
const OPTIONS_CONTEXT_COMPONENTS: u16 = 16;
const OPTIONS_CONTEXT_QUERY_DOMAIN: &[u8] = b"market-squawk/options-context/query/v1";
const OPTIONS_CONTEXT_RECEIPT_DOMAIN: &[u8] = b"market-squawk/options-context/receipt/v1";

/// Fixed provider-neutral request for one underlying's option research context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContextRequest {
    underlying_instrument_id: InstrumentId,
    valuation_at: Timestamp,
    knowledge_cutoff: Timestamp,
    effective_cutoff: Timestamp,
    expiration_range: OptionExpirationRange,
    strike_range: OptionStrikeRange,
    maximum_contracts: NonZeroU16,
}

impl OptionsContextRequest {
    /// Constructs a look-ahead-safe, bounded option-context request.
    #[allow(
        clippy::too_many_arguments,
        reason = "identity, three clocks, two economic windows, and the result bound are independent"
    )]
    pub(crate) fn try_new(
        underlying_instrument_id: InstrumentId,
        valuation_at: Timestamp,
        knowledge_cutoff: Timestamp,
        effective_cutoff: Timestamp,
        expiration_range: OptionExpirationRange,
        strike_range: OptionStrikeRange,
        maximum_contracts: NonZeroU16,
    ) -> Result<Self, OptionsContextError> {
        if effective_cutoff > knowledge_cutoff
            || knowledge_cutoff > valuation_at
            || usize::from(maximum_contracts.get()) > MAX_OPTIONS_CONTEXT_CONTRACTS
        {
            return Err(OptionsContextError::InvalidRequest);
        }
        Ok(Self {
            underlying_instrument_id,
            valuation_at,
            knowledge_cutoff,
            effective_cutoff,
            expiration_range,
            strike_range,
            maximum_contracts,
        })
    }

    pub(crate) const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    pub(crate) const fn valuation_at(&self) -> Timestamp {
        self.valuation_at
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn effective_cutoff(&self) -> Timestamp {
        self.effective_cutoff
    }

    pub(crate) const fn expiration_range(&self) -> OptionExpirationRange {
        self.expiration_range
    }

    pub(crate) const fn strike_range(&self) -> OptionStrikeRange {
        self.strike_range
    }

    pub(crate) const fn maximum_contracts(&self) -> NonZeroU16 {
        self.maximum_contracts
    }
}

/// Startup truth for the canonical observation route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OptionsObservationReadAvailability {
    SetupRequired,
    EntitlementUnavailable,
    Ready(Box<[DatasetId]>),
}

impl OptionsObservationReadAvailability {
    /// Binds a bounded, duplicate-free set of canonical option datasets.
    pub(crate) fn try_ready(datasets: Vec<DatasetId>) -> Result<Self, OptionsContextError> {
        if datasets.is_empty() || datasets.len() > MAX_OPTIONS_CONTEXT_DATASETS {
            return Err(OptionsContextError::InvalidRequest);
        }
        let mut unique = BTreeSet::new();
        if datasets
            .iter()
            .any(|dataset| !unique.insert(dataset.as_str()))
        {
            return Err(OptionsContextError::InvalidRequest);
        }
        Ok(Self::Ready(datasets.into_boxed_slice()))
    }
}

/// Startup truth for official contract-reference corroboration.
#[derive(Clone)]
pub(crate) enum OptionsReferenceReadAvailability {
    Unavailable,
    Ready(OfficialOptionsReferenceCatalogReadCapability),
}

impl fmt::Debug for OptionsReferenceReadAvailability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "OptionsReferenceReadAvailability::Unavailable",
            Self::Ready(_) => "OptionsReferenceReadAvailability::Ready([SEALED])",
        })
    }
}

/// Cloneable, read-only application capability with no acquisition or publication authority.
#[derive(Clone)]
pub(crate) struct OptionsContextReadCapability {
    research: Arc<ResearchService>,
    reference: OptionsReferenceReadAvailability,
    observations: OptionsObservationReadAvailability,
}

impl fmt::Debug for OptionsContextReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptionsContextReadCapability")
            .field("research", &"[SEALED ANALYTICAL READ AUTHORITY]")
            .field("reference", &self.reference)
            .field("observations", &"[SEALED OBSERVATION ROUTES]")
            .finish()
    }
}

impl OptionsContextReadCapability {
    pub(crate) const fn new(
        research: Arc<ResearchService>,
        reference: OptionsReferenceReadAvailability,
        observations: OptionsObservationReadAvailability,
    ) -> Self {
        Self {
            research,
            reference,
            observations,
        }
    }

    /// Reads, validates, deduplicates, and reference-corroborates one bounded option context.
    pub(crate) async fn read(
        &self,
        request: &OptionsContextRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OptionsContext, OptionsContextError> {
        check_control(deadline, cancellation)?;
        match &self.observations {
            OptionsObservationReadAvailability::SetupRequired => {
                return OptionsContext::unavailable(
                    request,
                    OptionsContextUnavailableReason::SetupRequired,
                );
            }
            OptionsObservationReadAvailability::EntitlementUnavailable => {
                return OptionsContext::unavailable(
                    request,
                    OptionsContextUnavailableReason::EntitlementUnavailable,
                );
            }
            OptionsObservationReadAvailability::Ready(_) => {}
        }

        let filter = OptionMarketRequestFilter::try_new(
            Some(request.expiration_range),
            Some(request.strike_range),
            None,
            Vec::new(),
        )
        .map_err(|_error| OptionsContextError::InvalidRequest)?;
        let OptionsObservationReadAvailability::Ready(datasets) = &self.observations else {
            return Err(OptionsContextError::InvalidEvidence);
        };
        let store = self.research.provider_capture_store();
        let mut batch_evidence = Vec::new();
        batch_evidence
            .try_reserve_exact(datasets.len())
            .map_err(|_error| OptionsContextError::CapacityExceeded)?;
        let mut groups = BTreeMap::<InstrumentId, CandidateGroup>::new();
        let mut selected_batches = 0_usize;
        let mut candidate_count = 0_usize;

        for dataset in datasets {
            check_control(deadline, cancellation)?;
            let selection_request = OptionMarketPointInTimeRequest::try_latest(
                dataset.clone(),
                request.underlying_instrument_id,
                OptionMarketBatchKind::Snapshots,
                &filter,
                request.knowledge_cutoff,
                usize::from(request.maximum_contracts.get()),
            )
            .map_err(|_error| OptionsContextError::InvalidRequest)?;
            let Some(selection) = self
                .research
                .analytical()
                .read_provider_option_market_point_in_time(
                    &selection_request,
                    store.as_ref(),
                    cancellation.clone(),
                )
                .await
                .map_err(map_ingest_error)?
            else {
                continue;
            };
            check_control(deadline, cancellation)?;
            let batch_index = u16::try_from(batch_evidence.len())
                .map_err(|_error| OptionsContextError::CapacityExceeded)?;
            validate_selection(request, &filter, &selection)?;
            let batch = selection.batch();
            let snapshots = batch
                .snapshots()
                .ok_or(OptionsContextError::InvalidEvidence)?;
            batch_evidence.push(PrivateBatchEvidence::from_selection(dataset, &selection));
            selected_batches = selected_batches
                .checked_add(1)
                .ok_or(OptionsContextError::CapacityExceeded)?;

            for snapshot in snapshots {
                candidate_count = candidate_count
                    .checked_add(1)
                    .ok_or(OptionsContextError::CapacityExceeded)?;
                if candidate_count > MAX_OPTIONS_CONTEXT_CANDIDATES {
                    return Err(OptionsContextError::CapacityExceeded);
                }
                let candidate = Candidate::try_new(
                    snapshot,
                    batch.scope().available_at(),
                    batch.scope().received_at(),
                    batch.scope().ingested_at(),
                    request.effective_cutoff,
                    batch.completeness(),
                    selection.selection_digest(),
                    batch_index,
                )?;
                match groups.entry(candidate.contract.instrument_id) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(CandidateGroup::new(candidate));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().try_observe(candidate)?;
                    }
                }
            }
        }

        if selected_batches == 0 {
            return OptionsContext::unavailable(
                request,
                OptionsContextUnavailableReason::NoDataAtCutoff,
            );
        }
        if groups.is_empty() {
            return OptionsContext::unavailable(
                request,
                OptionsContextUnavailableReason::NoContractsInWindow,
            );
        }

        let mut groups = groups.into_values().collect::<Vec<_>>();
        groups.sort_by(CandidateGroup::output_order);
        let maximum_contracts = usize::from(request.maximum_contracts.get());
        let has_more = groups.len() > maximum_contracts;
        groups.truncate(maximum_contracts);

        let mut contracts = Vec::new();
        let mut contract_evidence = Vec::new();
        contracts
            .try_reserve_exact(groups.len())
            .map_err(|_error| OptionsContextError::CapacityExceeded)?;
        contract_evidence
            .try_reserve_exact(groups.len())
            .map_err(|_error| OptionsContextError::CapacityExceeded)?;
        let mut confirmed = 0_u16;
        let mut complete = 0_u16;
        let mut observed_components = 0_u32;
        let mut limited = has_more;

        for mut group in groups {
            check_control(deadline, cancellation)?;
            let (reference_status, reference_evidence) = self.resolve_reference(
                group.occ_identity.as_ref(),
                group.selected.contract.instrument_id,
                request,
                deadline,
                cancellation,
            )?;
            if reference_status == OptionsReferenceStatus::Confirmed {
                confirmed = confirmed
                    .checked_add(1)
                    .ok_or(OptionsContextError::CapacityExceeded)?;
            } else {
                limited = true;
            }
            if group.selected.contract.quality.batch == OptionsBatchQuality::Complete {
                complete = complete
                    .checked_add(1)
                    .ok_or(OptionsContextError::CapacityExceeded)?;
            } else {
                limited = true;
            }
            observed_components = observed_components
                .checked_add(u32::from(
                    group.selected.contract.quality.observed_components,
                ))
                .ok_or(OptionsContextError::CapacityExceeded)?;
            if group.selected.contract.quality.observed_components != OPTIONS_CONTEXT_COMPONENTS {
                limited = true;
            }
            group.selected.contract.quality.reference = reference_status;
            contract_evidence.push(PrivateContractEvidence {
                instrument_id: group.selected.contract.instrument_id,
                selected_batch: group.selected.batch_index,
                observed_batches: group.observed_batches.into_iter().collect(),
                option_definition_revision: group.selected.option_definition_revision,
                underlying_definition_revision: group.selected.underlying_definition_revision,
                identity_batch: group.identity_batch,
                reference: reference_evidence,
            });
            contracts.push(group.selected.contract);
        }

        let returned_contracts = u16::try_from(contracts.len())
            .map_err(|_error| OptionsContextError::CapacityExceeded)?;
        let possible_components = u32::from(returned_contracts)
            .checked_mul(u32::from(OPTIONS_CONTEXT_COMPONENTS))
            .ok_or(OptionsContextError::CapacityExceeded)?;
        let quality = OptionsContextQuality {
            returned_contracts,
            complete_batch_contracts: complete,
            confirmed_reference_contracts: confirmed,
            observed_components,
            possible_components,
            has_more,
        };
        let availability = if limited {
            OptionsContextAvailability::Limited
        } else {
            OptionsContextAvailability::Available
        };
        let evidence = OptionsContextEvidenceReceipt::try_new(
            request,
            availability,
            has_more,
            batch_evidence,
            contract_evidence,
        )?;
        Ok(OptionsContext {
            underlying_instrument_id: request.underlying_instrument_id,
            valuation_at: request.valuation_at,
            knowledge_cutoff: request.knowledge_cutoff,
            effective_cutoff: request.effective_cutoff,
            expiration_range: request.expiration_range,
            strike_range: request.strike_range,
            availability,
            contracts: contracts.into_boxed_slice(),
            quality,
            evidence,
        })
    }

    fn resolve_reference(
        &self,
        identity: Option<&OccOptionIdentity>,
        expected_instrument: InstrumentId,
        request: &OptionsContextRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(OptionsReferenceStatus, PrivateReferenceEvidence), OptionsContextError> {
        let Some(identity) = identity else {
            return Ok((
                OptionsReferenceStatus::NotAvailable,
                PrivateReferenceEvidence::not_available(),
            ));
        };
        let OptionsReferenceReadAvailability::Ready(reference) = &self.reference else {
            return Ok((
                OptionsReferenceStatus::Unavailable,
                PrivateReferenceEvidence::unavailable(),
            ));
        };
        let resolution = match reference.resolve(
            OfficialOptionsReferenceGenerationSelection::AsOf {
                knowledge_at: request.knowledge_cutoff,
                effective_at: request.effective_cutoff,
            },
            OfficialOptionsReferenceIdentityQuery::Osi(identity.clone()),
            deadline,
            cancellation,
        ) {
            Ok(OfficialOptionsReferenceCatalogResolution::Unavailable) => {
                return Ok((
                    OptionsReferenceStatus::Unavailable,
                    PrivateReferenceEvidence::unavailable(),
                ));
            }
            Ok(OfficialOptionsReferenceCatalogResolution::Ambiguous { .. }) => {
                return Ok((
                    OptionsReferenceStatus::Ambiguous,
                    PrivateReferenceEvidence::ambiguous_catalog(),
                ));
            }
            Ok(OfficialOptionsReferenceCatalogResolution::Selected(resolution)) => resolution,
            Err(
                OfficialOptionsReferenceError::SourceUnavailable
                | OfficialOptionsReferenceError::AuthorityUnavailable,
            ) => {
                return Ok((
                    OptionsReferenceStatus::Unavailable,
                    PrivateReferenceEvidence::unavailable(),
                ));
            }
            Err(error) => return Err(map_reference_error(error)),
        };
        map_reference_resolution(identity, expected_instrument, resolution)
    }
}

/// Whether the requested context is usable without exposing a provider route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsContextAvailability {
    Available,
    Limited,
    Unavailable(OptionsContextUnavailableReason),
}

/// Honest terminal reason no canonical option context can be returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsContextUnavailableReason {
    SetupRequired,
    EntitlementUnavailable,
    NoDataAtCutoff,
    NoContractsInWindow,
}

/// Official contract-identity corroboration without source or catalog plumbing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsReferenceStatus {
    Confirmed,
    Missing,
    Ambiguous,
    Unavailable,
    NotAvailable,
}

/// Completeness of the exact canonical batch supplying the selected contract observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsBatchQuality {
    Complete,
    Partial,
}

impl OptionsBatchQuality {
    const fn rank(self) -> u8 {
        match self {
            Self::Complete => 1,
            Self::Partial => 0,
        }
    }
}

/// Product-neutral reason one independently meaningful option component is unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsValueUnavailableReason {
    NotReported,
    NotApplicable,
    Invalid,
    Unresolved,
    OutsideEffectiveWindow,
}

/// One independently timed value or explicit unavailable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OptionsContextValue<T> {
    Observed {
        value: T,
        observed_at: Option<Timestamp>,
    },
    Unavailable {
        reason: OptionsValueUnavailableReason,
        observed_at: Option<Timestamp>,
    },
}

impl<T> OptionsContextValue<T> {
    pub(crate) const fn value(&self) -> Option<&T> {
        match self {
            Self::Observed { value, .. } => Some(value),
            Self::Unavailable { .. } => None,
        }
    }

    pub(crate) const fn unavailable_reason(&self) -> Option<OptionsValueUnavailableReason> {
        match self {
            Self::Observed { .. } => None,
            Self::Unavailable { reason, .. } => Some(*reason),
        }
    }

    pub(crate) const fn observed_at(&self) -> Option<Timestamp> {
        match self {
            Self::Observed { observed_at, .. } | Self::Unavailable { observed_at, .. } => {
                *observed_at
            }
        }
    }

    const fn is_observed(&self) -> bool {
        matches!(self, Self::Observed { .. })
    }
}

/// Source-neutral exercise style; unrecognized source labels remain intentionally opaque.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsExerciseStyle {
    American,
    European,
    Bermudan,
    Other,
}

/// Source-neutral settlement kind; unrecognized source labels remain intentionally opaque.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OptionsSettlementKind {
    Physical,
    Cash,
    Other,
}

/// Stable contract identity and exact economic terms used by option analytics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContractTerms {
    expiration: CalendarDate,
    strike: Money,
    right: OptionKind,
    multiplier: Decimal,
    exercise_style: OptionsContextValue<OptionsExerciseStyle>,
    settlement: OptionsContextValue<OptionsSettlementKind>,
}

impl OptionsContractTerms {
    pub(crate) const fn expiration(&self) -> CalendarDate {
        self.expiration
    }

    pub(crate) const fn strike(&self) -> Money {
        self.strike
    }

    pub(crate) const fn right(&self) -> OptionKind {
        self.right
    }

    pub(crate) const fn multiplier(&self) -> Decimal {
        self.multiplier
    }

    pub(crate) const fn exercise_style(&self) -> &OptionsContextValue<OptionsExerciseStyle> {
        &self.exercise_style
    }

    pub(crate) const fn settlement(&self) -> &OptionsContextValue<OptionsSettlementKind> {
        &self.settlement
    }
}

/// Exact outer clocks for the canonical batch supplying one selected contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OptionsObservationClocks {
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
}

impl OptionsObservationClocks {
    pub(crate) const fn available_at(self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn received_at(self) -> Timestamp {
        self.received_at
    }

    pub(crate) const fn ingested_at(self) -> Timestamp {
        self.ingested_at
    }
}

/// Market and volatility components for one canonical option contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContractMetrics {
    pub(crate) bid_price: OptionsContextValue<Money>,
    pub(crate) bid_size: OptionsContextValue<QuantityLots>,
    pub(crate) ask_price: OptionsContextValue<Money>,
    pub(crate) ask_size: OptionsContextValue<QuantityLots>,
    pub(crate) last_price: OptionsContextValue<Money>,
    pub(crate) last_size: OptionsContextValue<QuantityLots>,
    pub(crate) mark_price: OptionsContextValue<Money>,
    pub(crate) volume: OptionsContextValue<u64>,
    pub(crate) open_interest: OptionsContextValue<u64>,
    pub(crate) implied_volatility: OptionsContextValue<Decimal>,
    pub(crate) delta: OptionsContextValue<Decimal>,
    pub(crate) gamma: OptionsContextValue<Decimal>,
    pub(crate) theta: OptionsContextValue<Decimal>,
    pub(crate) vega: OptionsContextValue<Decimal>,
    pub(crate) rho: OptionsContextValue<Decimal>,
    pub(crate) underlying_price: OptionsContextValue<Money>,
}

impl OptionsContractMetrics {
    fn observed_component_count(&self) -> u16 {
        [
            self.bid_price.is_observed(),
            self.bid_size.is_observed(),
            self.ask_price.is_observed(),
            self.ask_size.is_observed(),
            self.last_price.is_observed(),
            self.last_size.is_observed(),
            self.mark_price.is_observed(),
            self.volume.is_observed(),
            self.open_interest.is_observed(),
            self.implied_volatility.is_observed(),
            self.delta.is_observed(),
            self.gamma.is_observed(),
            self.theta.is_observed(),
            self.vega.is_observed(),
            self.rho.is_observed(),
            self.underlying_price.is_observed(),
        ]
        .into_iter()
        .map(u16::from)
        .sum()
    }
}

/// Bounded structural quality for one selected contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContractQuality {
    batch: OptionsBatchQuality,
    reference: OptionsReferenceStatus,
    observed_components: u16,
    possible_components: u16,
}

impl OptionsContractQuality {
    pub(crate) const fn batch(self) -> OptionsBatchQuality {
        self.batch
    }

    pub(crate) const fn reference(self) -> OptionsReferenceStatus {
        self.reference
    }

    pub(crate) const fn observed_components(self) -> u16 {
        self.observed_components
    }

    pub(crate) const fn possible_components(self) -> u16 {
        self.possible_components
    }
}

/// One provider-neutral canonical option contract and the best admitted observation at cutoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContextContract {
    instrument_id: InstrumentId,
    underlying_instrument_id: InstrumentId,
    terms: OptionsContractTerms,
    metrics: OptionsContractMetrics,
    clocks: OptionsObservationClocks,
    quality: OptionsContractQuality,
}

impl OptionsContextContract {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    pub(crate) const fn terms(&self) -> &OptionsContractTerms {
        &self.terms
    }

    pub(crate) const fn metrics(&self) -> &OptionsContractMetrics {
        &self.metrics
    }

    pub(crate) const fn clocks(&self) -> OptionsObservationClocks {
        self.clocks
    }

    pub(crate) const fn quality(&self) -> OptionsContractQuality {
        self.quality
    }
}

/// Aggregate structural coverage without a model-derived confidence score.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OptionsContextQuality {
    returned_contracts: u16,
    complete_batch_contracts: u16,
    confirmed_reference_contracts: u16,
    observed_components: u32,
    possible_components: u32,
    has_more: bool,
}

impl OptionsContextQuality {
    pub(crate) const fn returned_contracts(self) -> u16 {
        self.returned_contracts
    }

    pub(crate) const fn complete_batch_contracts(self) -> u16 {
        self.complete_batch_contracts
    }

    pub(crate) const fn confirmed_reference_contracts(self) -> u16 {
        self.confirmed_reference_contracts
    }

    pub(crate) const fn observed_components(self) -> u32 {
        self.observed_components
    }

    pub(crate) const fn possible_components(self) -> u32 {
        self.possible_components
    }

    pub(crate) const fn has_more(self) -> bool {
        self.has_more
    }
}

/// Provider-neutral option context. Exact plumbing is retained only in `evidence`.
#[derive(Clone)]
pub(crate) struct OptionsContext {
    underlying_instrument_id: InstrumentId,
    valuation_at: Timestamp,
    knowledge_cutoff: Timestamp,
    effective_cutoff: Timestamp,
    expiration_range: OptionExpirationRange,
    strike_range: OptionStrikeRange,
    availability: OptionsContextAvailability,
    contracts: Box<[OptionsContextContract]>,
    quality: OptionsContextQuality,
    evidence: OptionsContextEvidenceReceipt,
}

impl fmt::Debug for OptionsContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OptionsContext")
            .field("underlying_instrument_id", &self.underlying_instrument_id)
            .field("valuation_at", &self.valuation_at)
            .field("knowledge_cutoff", &self.knowledge_cutoff)
            .field("effective_cutoff", &self.effective_cutoff)
            .field("expiration_range", &self.expiration_range)
            .field("strike_range", &self.strike_range)
            .field("availability", &self.availability)
            .field("contracts", &self.contracts)
            .field("quality", &self.quality)
            .field("evidence", &"[OPAQUE VERIFIED RECEIPT]")
            .finish()
    }
}

impl OptionsContext {
    fn unavailable(
        request: &OptionsContextRequest,
        reason: OptionsContextUnavailableReason,
    ) -> Result<Self, OptionsContextError> {
        let availability = OptionsContextAvailability::Unavailable(reason);
        let evidence = OptionsContextEvidenceReceipt::try_new(
            request,
            availability,
            false,
            Vec::new(),
            Vec::new(),
        )?;
        Ok(Self {
            underlying_instrument_id: request.underlying_instrument_id,
            valuation_at: request.valuation_at,
            knowledge_cutoff: request.knowledge_cutoff,
            effective_cutoff: request.effective_cutoff,
            expiration_range: request.expiration_range,
            strike_range: request.strike_range,
            availability,
            contracts: Box::new([]),
            quality: OptionsContextQuality {
                returned_contracts: 0,
                complete_batch_contracts: 0,
                confirmed_reference_contracts: 0,
                observed_components: 0,
                possible_components: 0,
                has_more: false,
            },
            evidence,
        })
    }

    pub(crate) const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    pub(crate) const fn valuation_at(&self) -> Timestamp {
        self.valuation_at
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn effective_cutoff(&self) -> Timestamp {
        self.effective_cutoff
    }

    pub(crate) const fn expiration_range(&self) -> OptionExpirationRange {
        self.expiration_range
    }

    pub(crate) const fn strike_range(&self) -> OptionStrikeRange {
        self.strike_range
    }

    pub(crate) const fn availability(&self) -> OptionsContextAvailability {
        self.availability
    }

    pub(crate) fn contracts(&self) -> &[OptionsContextContract] {
        &self.contracts
    }

    pub(crate) const fn quality(&self) -> OptionsContextQuality {
        self.quality
    }

    /// Returns only the opaque binding identity; provider and storage coordinates stay private.
    pub(crate) fn evidence_digest(&self) -> Result<EvidenceDigest, OptionsContextError> {
        self.evidence.verify()
    }
}

#[derive(Clone, Debug)]
struct Candidate {
    contract: OptionsContextContract,
    reference_identity: Option<OccOptionIdentity>,
    option_definition_revision: EvidenceDigest,
    underlying_definition_revision: EvidenceDigest,
    selection_digest: EvidenceDigest,
    batch_index: u16,
}

impl Candidate {
    #[allow(
        clippy::too_many_arguments,
        reason = "one snapshot and its exact three batch clocks, quality, identity, and coordinate stay explicit"
    )]
    fn try_new(
        snapshot: &OptionSnapshotObservation,
        available_at: Timestamp,
        received_at: Timestamp,
        ingested_at: Timestamp,
        effective_cutoff: Timestamp,
        completeness: OptionMarketCompleteness,
        selection_digest: EvidenceDigest,
        batch_index: u16,
    ) -> Result<Self, OptionsContextError> {
        let terms = snapshot.terms();
        let metrics = OptionsContractMetrics {
            bid_price: map_component(snapshot.bid_price(), effective_cutoff),
            bid_size: map_component(snapshot.bid_size(), effective_cutoff),
            ask_price: map_component(snapshot.ask_price(), effective_cutoff),
            ask_size: map_component(snapshot.ask_size(), effective_cutoff),
            last_price: map_component(snapshot.last_price(), effective_cutoff),
            last_size: map_component(snapshot.last_size(), effective_cutoff),
            mark_price: map_component(snapshot.mark_price(), effective_cutoff),
            volume: map_component(snapshot.volume(), effective_cutoff),
            open_interest: map_component(snapshot.open_interest(), effective_cutoff),
            implied_volatility: map_component(snapshot.implied_volatility(), effective_cutoff),
            delta: map_component(snapshot.delta(), effective_cutoff),
            gamma: map_component(snapshot.gamma(), effective_cutoff),
            theta: map_component(snapshot.theta(), effective_cutoff),
            vega: map_component(snapshot.vega(), effective_cutoff),
            rho: map_component(snapshot.rho(), effective_cutoff),
            underlying_price: map_component(snapshot.underlying().price(), effective_cutoff),
        };
        let observed_components = metrics.observed_component_count();
        Ok(Self {
            contract: OptionsContextContract {
                instrument_id: terms.option_instrument_id(),
                underlying_instrument_id: terms.underlying_instrument_id(),
                terms: OptionsContractTerms {
                    expiration: terms.expiration(),
                    strike: terms.strike(),
                    right: terms.kind(),
                    multiplier: terms.multiplier(),
                    exercise_style: map_component_with(
                        terms.exercise_style(),
                        effective_cutoff,
                        map_exercise_style,
                    ),
                    settlement: map_component_with(
                        terms.settlement(),
                        effective_cutoff,
                        map_settlement,
                    ),
                },
                metrics,
                clocks: OptionsObservationClocks {
                    available_at,
                    received_at,
                    ingested_at,
                },
                quality: OptionsContractQuality {
                    batch: if completeness.disposition() == OptionMarketBatchDisposition::Complete {
                        OptionsBatchQuality::Complete
                    } else {
                        OptionsBatchQuality::Partial
                    },
                    reference: OptionsReferenceStatus::NotAvailable,
                    observed_components,
                    possible_components: OPTIONS_CONTEXT_COMPONENTS,
                },
            },
            reference_identity: terms.occ_identity().cloned(),
            option_definition_revision: terms.option_definition_revision(),
            underlying_definition_revision: terms.underlying_definition_revision(),
            selection_digest,
            batch_index,
        })
    }

    fn preference(&self, other: &Self) -> Ordering {
        self.contract
            .quality
            .batch
            .rank()
            .cmp(&other.contract.quality.batch.rank())
            .then_with(|| {
                self.contract
                    .quality
                    .observed_components
                    .cmp(&other.contract.quality.observed_components)
            })
            .then_with(|| {
                self.contract
                    .clocks
                    .available_at
                    .cmp(&other.contract.clocks.available_at)
            })
            .then_with(|| {
                self.contract
                    .clocks
                    .received_at
                    .cmp(&other.contract.clocks.received_at)
            })
            .then_with(|| {
                self.contract
                    .clocks
                    .ingested_at
                    .cmp(&other.contract.clocks.ingested_at)
            })
            .then_with(|| {
                other
                    .selection_digest
                    .bytes()
                    .cmp(&self.selection_digest.bytes())
            })
    }
}

#[derive(Debug)]
struct CandidateGroup {
    selected: Candidate,
    occ_identity: Option<OccOptionIdentity>,
    identity_batch: Option<u16>,
    observed_batches: BTreeSet<u16>,
}

impl CandidateGroup {
    fn new(candidate: Candidate) -> Self {
        let occ_identity = candidate.reference_identity.clone();
        let identity_batch = occ_identity.as_ref().map(|_| candidate.batch_index);
        let observed_batches = BTreeSet::from([candidate.batch_index]);
        Self {
            selected: candidate,
            occ_identity,
            identity_batch,
            observed_batches,
        }
    }

    fn try_observe(&mut self, candidate: Candidate) -> Result<(), OptionsContextError> {
        if !core_terms_match(&self.selected.contract, &candidate.contract)
            || self
                .occ_identity
                .as_ref()
                .zip(candidate.reference_identity.as_ref())
                .is_some_and(|(left, right)| left != right)
        {
            return Err(OptionsContextError::ConflictingCanonicalTerms);
        }
        self.observed_batches.insert(candidate.batch_index);
        if self.occ_identity.is_none()
            && let Some(identity) = candidate.reference_identity.clone()
        {
            self.occ_identity = Some(identity);
            self.identity_batch = Some(candidate.batch_index);
        }
        if candidate.preference(&self.selected).is_gt() {
            self.selected = candidate;
        }
        Ok(())
    }

    fn output_order(left: &Self, right: &Self) -> Ordering {
        left.selected
            .contract
            .terms
            .expiration
            .cmp(&right.selected.contract.terms.expiration)
            .then_with(|| {
                left.selected
                    .contract
                    .terms
                    .strike
                    .currency()
                    .cmp(&right.selected.contract.terms.strike.currency())
            })
            .then_with(|| {
                left.selected
                    .contract
                    .terms
                    .strike
                    .amount()
                    .cmp(&right.selected.contract.terms.strike.amount())
            })
            .then_with(|| {
                option_kind_rank(left.selected.contract.terms.right)
                    .cmp(&option_kind_rank(right.selected.contract.terms.right))
            })
            .then_with(|| {
                left.selected
                    .contract
                    .instrument_id
                    .cmp(&right.selected.contract.instrument_id)
            })
    }
}

fn validate_selection(
    request: &OptionsContextRequest,
    filter: &OptionMarketRequestFilter,
    selection: &OptionMarketPointInTimeSelection,
) -> Result<(), OptionsContextError> {
    let batch = selection.batch();
    let scope = batch.scope();
    if batch.publication_kind() != OptionMarketBatchKind::Snapshots
        || scope.underlying_instrument_id() != request.underlying_instrument_id
        || scope.filter() != filter
        || scope.available_at() > request.knowledge_cutoff
        || scope.received_at() > request.knowledge_cutoff
        || scope.ingested_at() > request.knowledge_cutoff
    {
        return Err(OptionsContextError::InvalidEvidence);
    }
    let snapshots = batch
        .snapshots()
        .ok_or(OptionsContextError::InvalidEvidence)?;
    if u64::try_from(snapshots.len()).ok() != Some(batch.completeness().returned_records()) {
        return Err(OptionsContextError::InvalidEvidence);
    }
    for snapshot in snapshots {
        let terms = snapshot.terms();
        let expiration = terms.expiration();
        let strike = terms.strike();
        if terms.underlying_instrument_id() != request.underlying_instrument_id
            || expiration < request.expiration_range.start()
            || expiration > request.expiration_range.end()
            || strike.currency() != request.strike_range.minimum().currency()
            || strike.amount() < request.strike_range.minimum().amount()
            || strike.amount() > request.strike_range.maximum().amount()
        {
            return Err(OptionsContextError::InvalidEvidence);
        }
    }
    Ok(())
}

fn core_terms_match(left: &OptionsContextContract, right: &OptionsContextContract) -> bool {
    left.instrument_id == right.instrument_id
        && left.underlying_instrument_id == right.underlying_instrument_id
        && left.terms.expiration == right.terms.expiration
        && left.terms.strike == right.terms.strike
        && left.terms.right == right.terms.right
        && left.terms.multiplier == right.terms.multiplier
}

fn map_component<T: Clone>(
    component: &OptionComponent<T>,
    effective_cutoff: Timestamp,
) -> OptionsContextValue<T> {
    map_component_with(component, effective_cutoff, Clone::clone)
}

fn map_component_with<T, U>(
    component: &OptionComponent<T>,
    effective_cutoff: Timestamp,
    map: impl FnOnce(&T) -> U,
) -> OptionsContextValue<U> {
    match component {
        OptionComponent::Observed {
            source_at: Some(source_at),
            ..
        } if *source_at > effective_cutoff => OptionsContextValue::Unavailable {
            reason: OptionsValueUnavailableReason::OutsideEffectiveWindow,
            observed_at: Some(*source_at),
        },
        OptionComponent::Observed { value, source_at } => OptionsContextValue::Observed {
            value: map(value),
            observed_at: *source_at,
        },
        OptionComponent::Unavailable { reason, source_at } => OptionsContextValue::Unavailable {
            reason: match reason {
                OptionComponentState::ProviderAbsent
                | OptionComponentState::ProviderNull
                | OptionComponentState::Omitted => OptionsValueUnavailableReason::NotReported,
                OptionComponentState::NotApplicable => OptionsValueUnavailableReason::NotApplicable,
                OptionComponentState::Invalid => OptionsValueUnavailableReason::Invalid,
                OptionComponentState::Unresolved => OptionsValueUnavailableReason::Unresolved,
            },
            observed_at: *source_at,
        },
    }
}

const fn map_exercise_style(value: &OptionExerciseStyle) -> OptionsExerciseStyle {
    match value {
        OptionExerciseStyle::American => OptionsExerciseStyle::American,
        OptionExerciseStyle::European => OptionsExerciseStyle::European,
        OptionExerciseStyle::Bermudan => OptionsExerciseStyle::Bermudan,
        OptionExerciseStyle::Other(_) => OptionsExerciseStyle::Other,
    }
}

const fn map_settlement(value: &OptionSettlementKind) -> OptionsSettlementKind {
    match value {
        OptionSettlementKind::Physical => OptionsSettlementKind::Physical,
        OptionSettlementKind::Cash => OptionsSettlementKind::Cash,
        OptionSettlementKind::Other(_) => OptionsSettlementKind::Other,
    }
}

const fn option_kind_rank(value: OptionKind) -> u8 {
    match value {
        OptionKind::Call => 0,
        OptionKind::Put => 1,
    }
}

fn map_reference_resolution(
    requested_identity: &OccOptionIdentity,
    expected_instrument: InstrumentId,
    resolution: OfficialOptionsReferenceIdentityResolution,
) -> Result<(OptionsReferenceStatus, PrivateReferenceEvidence), OptionsContextError> {
    match resolution {
        OfficialOptionsReferenceIdentityResolution::Missing { generation } => Ok((
            OptionsReferenceStatus::Missing,
            PrivateReferenceEvidence::missing(
                generation.map(|generation| generation.generation_digest()),
            ),
        )),
        OfficialOptionsReferenceIdentityResolution::Ambiguous {
            generation,
            ambiguity,
        } => {
            let details = ambiguity
                .records()
                .iter()
                .map(|record| record.record_digest())
                .chain(
                    ambiguity
                        .conflicts()
                        .iter()
                        .map(|conflict| conflict.digest()),
                )
                .collect::<Vec<_>>();
            Ok((
                OptionsReferenceStatus::Ambiguous,
                PrivateReferenceEvidence::ambiguous(generation.generation_digest(), details),
            ))
        }
        OfficialOptionsReferenceIdentityResolution::Exact {
            generation,
            identity,
        } => {
            if identity.records().is_empty()
                || identity
                    .records()
                    .iter()
                    .any(|record| match record.value() {
                        OfficialOptionsReferenceRecordValue::CboeSeries(series) => {
                            series.osi() != requested_identity
                        }
                        OfficialOptionsReferenceRecordValue::OccProduct(_) => true,
                    })
            {
                return Err(OptionsContextError::InvalidEvidence);
            }
            let status = match identity.canonical() {
                OfficialOptionsReferenceCanonicalResolution::Exact(candidate)
                    if candidate.instrument_id() == expected_instrument =>
                {
                    OptionsReferenceStatus::Confirmed
                }
                OfficialOptionsReferenceCanonicalResolution::Exact(_) => {
                    return Err(OptionsContextError::ConflictingCanonicalIdentity);
                }
                OfficialOptionsReferenceCanonicalResolution::Ambiguous { .. }
                | OfficialOptionsReferenceCanonicalResolution::Truncated { .. } => {
                    OptionsReferenceStatus::Ambiguous
                }
                OfficialOptionsReferenceCanonicalResolution::Missing
                | OfficialOptionsReferenceCanonicalResolution::ProviderProductOnly => {
                    OptionsReferenceStatus::Missing
                }
            };
            Ok((
                status,
                PrivateReferenceEvidence::exact(
                    status,
                    generation.generation_digest(),
                    identity.receipt_digest(),
                ),
            ))
        }
    }
}

#[derive(Clone)]
struct PrivateBatchEvidence {
    route_dataset: DatasetId,
    manifest: DatasetManifestRef,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    provider_dataset: SourceIdentifier,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    venue_id: Option<VenueId>,
    selection_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    observation_identity: EvidenceDigest,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
    completeness: OptionMarketCompleteness,
}

impl PrivateBatchEvidence {
    fn from_selection(dataset: &DatasetId, selection: &OptionMarketPointInTimeSelection) -> Self {
        let batch = selection.batch();
        let scope = batch.scope();
        Self {
            route_dataset: dataset.clone(),
            manifest: selection.manifest().clone(),
            source_id: scope.source_id().clone(),
            metadata_revision: scope.metadata_revision().clone(),
            provider_dataset: scope.dataset().clone(),
            provider_product: scope.provider_product().clone(),
            provider_channel: scope.provider_channel().clone(),
            venue_id: scope.venue_id().cloned(),
            selection_digest: selection.selection_digest(),
            publication_digest: batch.publication_digest(),
            observation_identity: scope.observation_identity(),
            entitlement_evidence: scope.entitlement_evidence(),
            capability_evidence: scope.capability_evidence(),
            available_at: scope.available_at(),
            received_at: scope.received_at(),
            ingested_at: scope.ingested_at(),
            completeness: batch.completeness(),
        }
    }
}

#[derive(Clone, Debug)]
struct PrivateContractEvidence {
    instrument_id: InstrumentId,
    selected_batch: u16,
    observed_batches: Vec<u16>,
    option_definition_revision: EvidenceDigest,
    underlying_definition_revision: EvidenceDigest,
    identity_batch: Option<u16>,
    reference: PrivateReferenceEvidence,
}

#[derive(Clone, Debug)]
struct PrivateReferenceEvidence {
    state: u8,
    generation_digest: Option<EvidenceDigest>,
    receipt_digest: Option<EvidenceDigest>,
    detail_digests: Box<[EvidenceDigest]>,
}

impl PrivateReferenceEvidence {
    fn unavailable() -> Self {
        Self::new(0, None, None, Vec::new())
    }

    fn not_available() -> Self {
        Self::new(1, None, None, Vec::new())
    }

    fn missing(generation_digest: Option<EvidenceDigest>) -> Self {
        Self::new(2, generation_digest, None, Vec::new())
    }

    fn ambiguous(generation_digest: EvidenceDigest, details: Vec<EvidenceDigest>) -> Self {
        Self::new(3, Some(generation_digest), None, details)
    }

    fn ambiguous_catalog() -> Self {
        Self::new(3, None, None, Vec::new())
    }

    fn exact(
        status: OptionsReferenceStatus,
        generation_digest: EvidenceDigest,
        receipt_digest: EvidenceDigest,
    ) -> Self {
        Self::new(
            reference_status_tag(status),
            Some(generation_digest),
            Some(receipt_digest),
            Vec::new(),
        )
    }

    fn new(
        state: u8,
        generation_digest: Option<EvidenceDigest>,
        receipt_digest: Option<EvidenceDigest>,
        details: Vec<EvidenceDigest>,
    ) -> Self {
        Self {
            state,
            generation_digest,
            receipt_digest,
            detail_digests: details.into_boxed_slice(),
        }
    }
}

#[derive(Clone)]
struct OptionsContextEvidenceReceipt {
    request_digest: EvidenceDigest,
    availability: OptionsContextAvailability,
    has_more: bool,
    batches: Box<[PrivateBatchEvidence]>,
    contracts: Box<[PrivateContractEvidence]>,
    receipt_digest: EvidenceDigest,
}

impl OptionsContextEvidenceReceipt {
    fn try_new(
        request: &OptionsContextRequest,
        availability: OptionsContextAvailability,
        has_more: bool,
        batches: Vec<PrivateBatchEvidence>,
        contracts: Vec<PrivateContractEvidence>,
    ) -> Result<Self, OptionsContextError> {
        let request_digest = request_digest(request)?;
        let receipt_digest =
            receipt_digest(request_digest, availability, has_more, &batches, &contracts)?;
        Ok(Self {
            request_digest,
            availability,
            has_more,
            batches: batches.into_boxed_slice(),
            contracts: contracts.into_boxed_slice(),
            receipt_digest,
        })
    }

    fn verify(&self) -> Result<EvidenceDigest, OptionsContextError> {
        let recomputed = receipt_digest(
            self.request_digest,
            self.availability,
            self.has_more,
            &self.batches,
            &self.contracts,
        )?;
        if recomputed != self.receipt_digest {
            return Err(OptionsContextError::InvalidEvidence);
        }
        Ok(self.receipt_digest)
    }
}

fn request_digest(request: &OptionsContextRequest) -> Result<EvidenceDigest, OptionsContextError> {
    let mut digest = Sha256::new();
    digest.update(OPTIONS_CONTEXT_QUERY_DOMAIN);
    digest.update(request.underlying_instrument_id.as_uuid().as_bytes());
    digest.update(request.valuation_at.unix_nanos().to_be_bytes());
    digest.update(request.knowledge_cutoff.unix_nanos().to_be_bytes());
    digest.update(request.effective_cutoff.unix_nanos().to_be_bytes());
    hash_date(&mut digest, request.expiration_range.start());
    hash_date(&mut digest, request.expiration_range.end());
    hash_money(&mut digest, request.strike_range.minimum())?;
    hash_money(&mut digest, request.strike_range.maximum())?;
    digest.update(request.maximum_contracts.get().to_be_bytes());
    Ok(sha256_evidence(digest))
}

fn receipt_digest(
    request_digest: EvidenceDigest,
    availability: OptionsContextAvailability,
    has_more: bool,
    batches: &[PrivateBatchEvidence],
    contracts: &[PrivateContractEvidence],
) -> Result<EvidenceDigest, OptionsContextError> {
    let mut digest = Sha256::new();
    digest.update(OPTIONS_CONTEXT_RECEIPT_DOMAIN);
    hash_evidence(&mut digest, request_digest);
    digest.update([availability_tag(availability)]);
    digest.update([u8::from(has_more)]);
    hash_length(&mut digest, batches.len())?;
    for batch in batches {
        hash_bytes(&mut digest, batch.route_dataset.as_str().as_bytes())?;
        hash_bytes(&mut digest, batch.manifest.dataset_id().as_str().as_bytes())?;
        hash_bytes(
            &mut digest,
            batch.manifest.manifest_version().to_string().as_bytes(),
        )?;
        hash_bytes(&mut digest, batch.manifest.schema().name().as_bytes())?;
        digest.update(batch.manifest.schema_version().get().to_be_bytes());
        digest.update(batch.manifest.schema().fingerprint());
        hash_evidence(
            &mut digest,
            EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                batch.manifest.content_hash().bytes(),
            ),
        );
        hash_bytes(&mut digest, batch.source_id.as_str().as_bytes())?;
        hash_bytes(
            &mut digest,
            batch
                .metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_bytes(&mut digest, batch.provider_dataset.as_str().as_bytes())?;
        hash_bytes(
            &mut digest,
            batch
                .provider_product
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_bytes(
            &mut digest,
            batch
                .provider_channel
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_optional_bytes(
            &mut digest,
            batch
                .venue_id
                .as_ref()
                .map(|venue| venue.as_str().as_bytes()),
        )?;
        for evidence in [
            batch.selection_digest,
            batch.publication_digest,
            batch.observation_identity,
            batch.entitlement_evidence,
            batch.capability_evidence,
        ] {
            hash_evidence(&mut digest, evidence);
        }
        digest.update(batch.available_at.unix_nanos().to_be_bytes());
        digest.update(batch.received_at.unix_nanos().to_be_bytes());
        digest.update(batch.ingested_at.unix_nanos().to_be_bytes());
        hash_completeness(&mut digest, batch.completeness);
    }
    hash_length(&mut digest, contracts.len())?;
    for contract in contracts {
        digest.update(contract.instrument_id.as_uuid().as_bytes());
        digest.update(contract.selected_batch.to_be_bytes());
        hash_length(&mut digest, contract.observed_batches.len())?;
        for batch in &contract.observed_batches {
            digest.update(batch.to_be_bytes());
        }
        hash_evidence(&mut digest, contract.option_definition_revision);
        hash_evidence(&mut digest, contract.underlying_definition_revision);
        match contract.identity_batch {
            Some(batch) => {
                digest.update([1]);
                digest.update(batch.to_be_bytes());
            }
            None => digest.update([0]),
        }
        digest.update([contract.reference.state]);
        hash_optional_evidence(&mut digest, contract.reference.generation_digest);
        hash_optional_evidence(&mut digest, contract.reference.receipt_digest);
        hash_length(&mut digest, contract.reference.detail_digests.len())?;
        for detail in &contract.reference.detail_digests {
            hash_evidence(&mut digest, *detail);
        }
    }
    Ok(sha256_evidence(digest))
}

fn hash_completeness(digest: &mut Sha256, completeness: OptionMarketCompleteness) {
    match completeness.expected_records() {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(completeness.returned_records().to_be_bytes());
    digest.update(completeness.missing_records().to_be_bytes());
    digest.update(completeness.unexpected_records().to_be_bytes());
    match completeness.provider_reported_records() {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest.update(completeness.page_count().get().to_be_bytes());
    digest.update([cursor_tag(completeness.cursor())]);
    digest.update([disposition_tag(completeness.disposition())]);
}

const fn availability_tag(availability: OptionsContextAvailability) -> u8 {
    match availability {
        OptionsContextAvailability::Available => 1,
        OptionsContextAvailability::Limited => 2,
        OptionsContextAvailability::Unavailable(OptionsContextUnavailableReason::SetupRequired) => {
            3
        }
        OptionsContextAvailability::Unavailable(
            OptionsContextUnavailableReason::EntitlementUnavailable,
        ) => 4,
        OptionsContextAvailability::Unavailable(
            OptionsContextUnavailableReason::NoDataAtCutoff,
        ) => 5,
        OptionsContextAvailability::Unavailable(
            OptionsContextUnavailableReason::NoContractsInWindow,
        ) => 6,
    }
}

const fn reference_status_tag(status: OptionsReferenceStatus) -> u8 {
    match status {
        OptionsReferenceStatus::Unavailable => 0,
        OptionsReferenceStatus::NotAvailable => 1,
        OptionsReferenceStatus::Missing => 2,
        OptionsReferenceStatus::Ambiguous => 3,
        OptionsReferenceStatus::Confirmed => 4,
    }
}

const fn cursor_tag(cursor: OptionMarketCursorState) -> u8 {
    match cursor {
        OptionMarketCursorState::NotApplicable => 0,
        OptionMarketCursorState::Exhausted => 1,
        OptionMarketCursorState::Incomplete => 2,
    }
}

const fn disposition_tag(disposition: OptionMarketBatchDisposition) -> u8 {
    match disposition {
        OptionMarketBatchDisposition::Complete => 0,
        OptionMarketBatchDisposition::Unavailable => 1,
    }
}

fn hash_date(digest: &mut Sha256, date: CalendarDate) {
    digest.update(date.year().to_be_bytes());
    digest.update([date.month(), date.day()]);
}

fn hash_money(digest: &mut Sha256, money: Money) -> Result<(), OptionsContextError> {
    hash_bytes(digest, money.currency().as_str().as_bytes())?;
    hash_bytes(digest, money.amount().normalize().to_string().as_bytes())
}

fn hash_optional_bytes(
    digest: &mut Sha256,
    value: Option<&[u8]>,
) -> Result<(), OptionsContextError> {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_bytes(digest, value)
        }
        None => {
            digest.update([0]);
            Ok(())
        }
    }
}

fn hash_optional_evidence(digest: &mut Sha256, value: Option<EvidenceDigest>) {
    match value {
        Some(value) => {
            digest.update([1]);
            hash_evidence(digest, value);
        }
        None => digest.update([0]),
    }
}

fn hash_evidence(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(evidence.bytes());
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) -> Result<(), OptionsContextError> {
    hash_length(digest, bytes.len())?;
    digest.update(bytes);
    Ok(())
}

fn hash_length(digest: &mut Sha256, length: usize) -> Result<(), OptionsContextError> {
    digest.update(
        u64::try_from(length)
            .map_err(|_error| OptionsContextError::CapacityExceeded)?
            .to_be_bytes(),
    );
    Ok(())
}

fn sha256_evidence(digest: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OptionsContextError> {
    if cancellation.is_cancelled() {
        Err(OptionsContextError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OptionsContextError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_ingest_error(error: IngestError) -> OptionsContextError {
    match error {
        IngestError::Cancelled => OptionsContextError::Cancelled,
        IngestError::DeadlineExceeded => OptionsContextError::DeadlineExceeded,
        _ => OptionsContextError::AnalyticalEvidenceUnavailable,
    }
}

fn map_reference_error(error: OfficialOptionsReferenceError) -> OptionsContextError {
    match error {
        OfficialOptionsReferenceError::Cancelled => OptionsContextError::Cancelled,
        OfficialOptionsReferenceError::DeadlineExceeded => OptionsContextError::DeadlineExceeded,
        _ => OptionsContextError::ReferenceEvidenceUnavailable,
    }
}

/// Provider-neutral request, evidence, or availability failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum OptionsContextError {
    #[error("option-context request is invalid")]
    InvalidRequest,
    #[error("option-context bounded capacity was exceeded")]
    CapacityExceeded,
    #[error("option-context canonical evidence is invalid")]
    InvalidEvidence,
    #[error("option-context canonical observations disagree on contract terms")]
    ConflictingCanonicalTerms,
    #[error("option-context official and canonical identities conflict")]
    ConflictingCanonicalIdentity,
    #[error("option-context analytical evidence is unavailable")]
    AnalyticalEvidenceUnavailable,
    #[error("option-context reference evidence is unavailable")]
    ReferenceEvidenceUnavailable,
    #[error("option-context read was cancelled")]
    Cancelled,
    #[error("option-context read exceeded its deadline")]
    DeadlineExceeded,
}
