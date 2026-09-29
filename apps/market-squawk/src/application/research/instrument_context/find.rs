//! Complete canonical discovery for genuine broad financial screening.
//!
//! Catalog admission and an official directory are distinct coverage scopes. Deep financial work
//! never changes this population. Deep analysis breadth is applied only after real ScreenRun ranking.

use std::{
    mem::size_of,
    time::{SystemTime, UNIX_EPOCH},
};

use market_squawk_data::{
    CurrentListedPopulation, CurrentListedPopulationAdmission,
    CurrentListedPopulationScope, CurrentPopulationError, CurrentPopulationExclusionReason,
    MarketDataInstrumentPopulationQuery,
    MarketDataInstrumentRecord, Sha256Digest,
};
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::*;
use crate::application::{
    analytical_profile::{SupportedInvestmentPolicy, ValidatedAnalyticalProfile},
    market_selection::product::{MAXIMUM_PRODUCT_MARKET_POPULATION, individual_selection_token},
};

const PAGE_ROWS: usize = 256;
const MAX_RETAINED_BYTES: usize = 64 * 1024 * 1024;
const MAX_DIRECTORY_PAGES: usize = MAX_LISTING_REFERENCE_RECORDS.div_ceil(PAGE_ROWS);

/// Completeness of the admitted canonical catalog, independent of requested deep-work breadth.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindPopulationCoverage {
    Complete,
    Saturated,
}

/// Exact discovery limitation retained before financial outcomes are requested.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FindPopulationExclusionReason {
    NoEffectiveCanonicalDefinition,
    OutsideProfileAssetScope,
    MissingOfficialListing,
    AmbiguousOfficialListing,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindPopulationExclusion {
    instrument_id: InstrumentId,
    reason: FindPopulationExclusionReason,
}

impl FindPopulationExclusion {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub(crate) const fn reason(&self) -> FindPopulationExclusionReason {
        self.reason
    }
}

/// Restart recipe only. Deserialization does not create candidate/population authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindPopulationReference {
    source_cutoff: Timestamp,
    financial_profile_digest: String,
    maximum_deep_analyses: usize,
    coverage: FindPopulationCoverage,
    canonical_scanned: usize,
    source_population_digest: [u8; 32],
    official_directory_digest: Option<[u8; 32]>,
    official_directory_count: Option<usize>,
    canonical_scope_count: usize,
    selected_count: usize,
    selection_digest: [u8; 32],
}

impl FindPopulationReference {
    pub(crate) const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    pub(crate) fn financial_profile_digest(&self) -> &str {
        &self.financial_profile_digest
    }
    pub(crate) const fn maximum_deep_analyses(&self) -> usize {
        self.maximum_deep_analyses
    }
    pub(crate) const fn coverage(&self) -> FindPopulationCoverage {
        self.coverage
    }
    pub(crate) const fn canonical_scanned(&self) -> usize {
        self.canonical_scanned
    }
    /// Count of all admitted canonical identities, only when the complete scan was exhausted.
    pub(crate) fn canonical_population_count(&self) -> Option<usize> {
        (self.coverage == FindPopulationCoverage::Complete).then_some(self.canonical_scanned)
    }
    /// All canonical equity/fund candidates before exact listing qualification.
    pub(crate) const fn canonical_scope_count(&self) -> usize {
        self.canonical_scope_count
    }
    pub(crate) const fn selected_count(&self) -> usize {
        self.selected_count
    }
    pub(crate) const fn selection_digest(&self) -> [u8; 32] {
        self.selection_digest
    }
    pub(crate) const fn source_population_digest(&self) -> [u8; 32] {
        self.source_population_digest
    }
    pub(crate) const fn official_directory_count(&self) -> Option<usize> {
        self.official_directory_count
    }
    /// This scope is the admitted canonical catalog, never the whole investment market.
    pub(crate) const fn coverage_scope(&self) -> &'static str {
        "admitted_canonical_catalog"
    }
}

/// One actual canonical identity qualified through the existing source-owned listing join.
#[derive(Debug)]
pub(crate) struct PreparedFindCandidate {
    context: InstrumentContext,
    population: CurrentListedPopulation,
    member_index: usize,
    selection_token: Box<str>,
}

impl PreparedFindCandidate {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.context.instrument_id()
    }
    pub(crate) fn selection_token(&self) -> &str {
        &self.selection_token
    }
    pub(crate) const fn context(&self) -> &InstrumentContext {
        &self.context
    }
    pub(crate) fn canonical_record(&self) -> &MarketDataInstrumentRecord {
        self.population.members()[self.member_index].canonical_record()
    }
    fn listing(&self) -> &ListingReferenceRecord {
        self.population.members()[self.member_index].listing_record()
    }
}

/// Sealed source population and selected candidates; no public caller-vector constructor exists.
#[derive(Debug)]
pub(crate) struct PreparedFindPopulation {
    reference: FindPopulationReference,
    candidates: Box<[PreparedFindCandidate]>,
    exclusions: Box<[FindPopulationExclusion]>,
    current_population: Option<CurrentListedPopulation>,
    validated_at: Timestamp,
    retained_bytes: usize,
}

impl PreparedFindPopulation {
    pub(crate) fn current_population(&self) -> Option<&CurrentListedPopulation> {
        self.current_population.as_ref()
    }

    pub(crate) const fn reference(&self) -> &FindPopulationReference {
        &self.reference
    }
    pub(crate) const fn source_cutoff(&self) -> Timestamp {
        self.reference.source_cutoff
    }
    pub(crate) fn candidates(&self) -> &[PreparedFindCandidate] {
        &self.candidates
    }
    pub(crate) fn exclusions(&self) -> &[FindPopulationExclusion] {
        &self.exclusions
    }
    pub(crate) const fn validated_at(&self) -> Timestamp {
        self.validated_at
    }
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub(crate) fn complete(&self) -> bool {
        self.reference.coverage == FindPopulationCoverage::Complete
    }
}

impl InstrumentContextReadCapability {
    /// Synchronous bounded source preparation. Composition runs it in its existing owned worker.
    /// The financial profile is constructor-admitted before this method; breadth is work only.
    pub(crate) fn prepare_find_population(
        &self,
        profile: &ValidatedAnalyticalProfile,
        source_cutoff: Timestamp,
        maximum_deep_analyses: usize,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFindPopulation, ServiceError> {
        if !matches!(maximum_deep_analyses, 8 | 16 | 32)
            || source_cutoff.unix_nanos() <= 0
            || source_cutoff > current_time()?
        {
            return Err(ServiceError::InvalidRequest);
        }
        ensure_live(deadline, cancellation)?;
        let policy = profile
            .resolution()
            .configuration
            .supported_investment_policy;
        let mut admission = CurrentListedPopulationAdmission::try_new(
            source_cutoff,
            match policy {
                SupportedInvestmentPolicy::ListedEquitiesV1 => {
                    CurrentListedPopulationScope::ListedEquities
                }
                SupportedInvestmentPolicy::ListedEquitiesAndEtfsV1 => {
                    CurrentListedPopulationScope::ListedEquitiesAndEtfs
                }
            },
            profile_digest(&profile.resolution().configuration_digest)?,
        )
        .map_err(map_population_error)?;
        let mut cursor = None;
        let mut scanned = 0usize;
        let mut partial_hash = Sha256::new();
        partial_hash.update(b"market-squawk/find-saturated-canonical-scan/v1\0");
        loop {
            ensure_live(deadline, cancellation)?;
            let page = self
                .identity
                .instruments
                .enumerate_as_of(
                    source_cutoff,
                    source_cutoff,
                    cursor.as_ref(),
                    PAGE_ROWS,
                    deadline,
                    cancellation,
                )
                .map_err(map_instrument_service_error)?;
            scanned = scanned
                .checked_add(page.instrument_ids().len())
                .ok_or(ServiceError::ResourceExhausted)?;
            partial_hash.update(page.receipt_digest().bytes());
            let complete = page.complete();
            cursor = page.next_cursor().cloned();
            admission
                .consume_canonical_page(page)
                .map_err(map_population_error)?;
            if complete {
                break;
            }
            if scanned == MAXIMUM_PRODUCT_MARKET_POPULATION {
                return finish(
                    FindPopulationReference {
                        source_cutoff,
                        financial_profile_digest: profile.resolution().configuration_digest.clone(),
                        maximum_deep_analyses,
                        coverage: FindPopulationCoverage::Saturated,
                        canonical_scanned: scanned,
                        source_population_digest: partial_hash.finalize().into(),
                        official_directory_digest: None,
                        official_directory_count: None,
                        canonical_scope_count: 0,
                        selected_count: 0,
                        selection_digest: [0; 32],
                    },
                    Vec::new(),
                    Vec::new(),
                    None,
                    size_of::<PreparedFindPopulation>(),
                );
            }
        }
        let population =
            self.finish_current_population(admission, source_cutoff, deadline, cancellation)?;
        let mut retained_bytes = population
            .retained_bytes()
            .checked_add(population.instrument_ids().len() * size_of::<PreparedFindCandidate>())
            .and_then(|bytes| {
                bytes.checked_add(
                    population.canonical_scanned() * size_of::<FindPopulationExclusion>(),
                )
            })
            .filter(|bytes| *bytes <= MAX_RETAINED_BYTES)
            .ok_or(ServiceError::ResourceExhausted)?;
        let exclusions = population
            .exclusions()
            .iter()
            .map(|value| FindPopulationExclusion {
                instrument_id: value.instrument_id(),
                reason: match value.reason() {
                    CurrentPopulationExclusionReason::NoEffectiveCanonicalDefinition => {
                        FindPopulationExclusionReason::NoEffectiveCanonicalDefinition
                    }
                    CurrentPopulationExclusionReason::OutsideProfileAssetScope => {
                        FindPopulationExclusionReason::OutsideProfileAssetScope
                    }
                    CurrentPopulationExclusionReason::MissingOfficialListing => {
                        FindPopulationExclusionReason::MissingOfficialListing
                    }
                    CurrentPopulationExclusionReason::AmbiguousOfficialListing => {
                        FindPopulationExclusionReason::AmbiguousOfficialListing
                    }
                },
            })
            .collect::<Vec<_>>();
        let mut candidates = Vec::new();
        candidates
            .try_reserve_exact(population.instrument_ids().len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (batch_index, batch) in population.members().chunks(PAGE_ROWS).enumerate() {
            for (index, member) in batch.iter().enumerate() {
                ensure_live(deadline, cancellation)?;
                let id = member.instrument_id();
                let record = member.canonical_record();
                let listing = member.listing_record();
                let mut context = InstrumentContext::try_new(
                    InstrumentContextRequest::try_new(id, source_cutoff, source_cutoff)
                        .map_err(map_context_error)?,
                    record.definition(),
                    listing,
                )
                .map_err(map_context_error)?;
                context.display_name =
                    try_boxed_text(listing.display_name()).map_err(map_context_error)?;
                if !profile.admits_investment(context.asset_class(), context.exchange_traded_fund())
                {
                    return Err(ServiceError::InvalidResult);
                }
                let selection_token = individual_selection_token(record)?;
                retained_bytes = retained_bytes
                    .checked_add(
                        selection_token.len()
                            + context.display_name().len()
                            + context.listed_symbol().len(),
                    )
                    .filter(|v| *v <= MAX_RETAINED_BYTES)
                    .ok_or(ServiceError::ResourceExhausted)?;
                candidates.push(PreparedFindCandidate {
                    context,
                    population: population.clone(),
                    member_index: batch_index * PAGE_ROWS + index,
                    selection_token,
                });
            }
        }
        let reference = FindPopulationReference {
            source_cutoff,
            financial_profile_digest: profile.resolution().configuration_digest.clone(),
            maximum_deep_analyses,
            coverage: FindPopulationCoverage::Complete,
            canonical_scanned: population.canonical_scanned(),
            source_population_digest: population.source_population_digest().bytes(),
            official_directory_digest: Some(population.official_directory_digest().bytes()),
            official_directory_count: Some(population.official_directory_count()),
            canonical_scope_count: population.canonical_scope_count(),
            selected_count: candidates.len(),
            selection_digest: [0; 32],
        };
        finish(
            reference,
            candidates,
            exclusions,
            Some(population),
            retained_bytes,
        )
    }

    /// Reopens original source clocks and exact population; current rights are independently read.
    pub(crate) fn read_find_population(
        &self,
        profile: &ValidatedAnalyticalProfile,
        expected: &FindPopulationReference,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFindPopulation, ServiceError> {
        if profile.resolution().configuration_digest != expected.financial_profile_digest {
            return Err(ServiceError::InvalidRequest);
        }
        let actual = self.prepare_find_population(
            profile,
            expected.source_cutoff,
            expected.maximum_deep_analyses,
            deadline,
            cancellation,
        )?;
        if actual.reference != *expected {
            return Err(ServiceError::Unavailable);
        }
        Ok(actual)
    }

    fn finish_current_population(
        &self,
        mut admission: CurrentListedPopulationAdmission,
        source_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CurrentListedPopulation, ServiceError> {
        let mut cursor = None;
        for _ in 0..MAX_DIRECTORY_PAGES {
            ensure_live(deadline, cancellation)?;
            let page = self
                .listings
                .memberships(
                    ListingReferenceGenerationSelection::AsOf(source_cutoff),
                    cursor.as_ref(),
                    PAGE_ROWS,
                    deadline,
                    cancellation,
                )
                .map_err(|error| map_context_error(map_listing_error(error)))?;
            let complete = page.state() == ListingReferenceMembershipPageState::Complete;
            cursor = page.next_cursor().cloned();
            admission
                .consume_listing_page(page)
                .map_err(map_population_error)?;
            if complete {
                let original = admission.finish().map_err(map_population_error)?;
                return self
                    .identity
                    .instruments
                    .revalidate_current_listed_population(
                        &original,
                        source_cutoff,
                        deadline,
                        cancellation,
                    )
                    .map_err(map_population_error);
            }
        }
        Err(ServiceError::ResourceExhausted)
    }
}

fn finish(
    mut reference: FindPopulationReference,
    candidates: Vec<PreparedFindCandidate>,
    mut exclusions: Vec<FindPopulationExclusion>,
    current_population: Option<CurrentListedPopulation>,
    retained_bytes: usize,
) -> Result<PreparedFindPopulation, ServiceError> {
    if reference.coverage == FindPopulationCoverage::Complete {
        if candidates.len().checked_add(exclusions.len()) != Some(reference.canonical_scanned) {
            return Err(ServiceError::InvalidResult);
        }
        let mut seen = std::collections::BTreeSet::new();
        for id in candidates
            .iter()
            .map(PreparedFindCandidate::instrument_id)
            .chain(
                exclusions
                    .iter()
                    .map(FindPopulationExclusion::instrument_id),
            )
        {
            if !seen.insert(id) {
                return Err(ServiceError::InvalidResult);
            }
        }
    }
    exclusions.sort_unstable_by_key(|value| (value.instrument_id, value.reason as u8));
    if retained_bytes
        .checked_add(
            reference
                .canonical_scanned
                .checked_mul(256)
                .ok_or(ServiceError::ResourceExhausted)?
                + 4096,
        )
        .is_none_or(|bytes| bytes > MAX_RETAINED_BYTES)
    {
        return Err(ServiceError::ResourceExhausted);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/find-pre-outcome-selection/v1\0");
    hash.update(serde_json::to_vec(&reference).map_err(|_| ServiceError::Internal)?);
    for candidate in &candidates {
        hash.update(candidate.instrument_id().as_uuid().as_bytes());
        hash.update(candidate.canonical_record().revision_digest().bytes());
        hash.update(
            candidate
                .listing()
                .record_payload_evidence()
                .content_digest()
                .bytes(),
        );
        hash.update(candidate.selection_token.as_bytes());
    }
    hash.update(serde_json::to_vec(&exclusions).map_err(|_| ServiceError::Internal)?);
    reference.selection_digest = hash.finalize().into();
    Ok(PreparedFindPopulation {
        reference,
        candidates: candidates.into_boxed_slice(),
        exclusions: exclusions.into_boxed_slice(),
        current_population,
        validated_at: current_time()?,
        retained_bytes,
    })
}

fn current_time() -> Result<Timestamp, ServiceError> {
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ServiceError::Internal)?
                .as_nanos(),
        )
        .map_err(|_| ServiceError::Internal)?,
    ))
}
fn ensure_live(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    check_operation(deadline, cancellation).map_err(map_context_error)
}
fn map_context_error(error: InstrumentContextReadError) -> ServiceError {
    match error {
        InstrumentContextReadError::Cancelled => ServiceError::Cancelled,
        InstrumentContextReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        InstrumentContextReadError::ResourceExhausted => ServiceError::ResourceExhausted,
        InstrumentContextReadError::InvalidRequest => ServiceError::InvalidRequest,
        InstrumentContextReadError::EvidenceConflict
        | InstrumentContextReadError::RestartConflict => ServiceError::InvalidResult,
        InstrumentContextReadError::AuthorityUnavailable => ServiceError::Unavailable,
    }
}
fn map_instrument_service_error(error: MarketDataInstrumentCatalogError) -> ServiceError {
    crate::application::research::map_market_definition_read_error(error)
}
/// Runs source preparation in ResearchService's original single owned I/O lane.
#[allow(
    clippy::too_many_arguments,
    reason = "immutable profile and exact source clocks remain explicit"
)]
pub(crate) async fn prepare_find_population(
    service: &crate::ResearchService,
    identities: std::sync::Arc<InstrumentContextReadCapability>,
    profile: ValidatedAnalyticalProfile,
    source_cutoff: Timestamp,
    maximum_deep_analyses: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PreparedFindPopulation, ServiceError> {
    service
        .run_owned_research_io(deadline, cancellation, move |operation_cancellation| {
            identities.prepare_find_population(
                &profile,
                source_cutoff,
                maximum_deep_analyses,
                deadline,
                &operation_cancellation,
            )
        })
        .await
        .map_err(map_owned_read_error)?
}

/// Reopens exactly the retained current-source population on the same owned I/O lane.
pub(crate) async fn read_find_population(
    service: &crate::ResearchService,
    identities: std::sync::Arc<InstrumentContextReadCapability>,
    profile: ValidatedAnalyticalProfile,
    expected: FindPopulationReference,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PreparedFindPopulation, ServiceError> {
    service
        .run_owned_research_io(deadline, cancellation, move |operation_cancellation| {
            identities.read_find_population(
                &profile,
                &expected,
                deadline,
                &operation_cancellation,
            )
        })
        .await
        .map_err(map_owned_read_error)?
}

fn map_owned_read_error(error: crate::ResearchServiceError) -> ServiceError {
    match error {
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::Cancelled) => {
            ServiceError::Cancelled
        }
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::DeadlineExceeded) => {
            ServiceError::DeadlineExceeded
        }
        // Domain results are the worker's nested output; this outer failure means the
        // owned lane could not be admitted/joined or failed to return its result.
        _ => ServiceError::Internal,
    }
}

fn profile_digest(value: &str) -> Result<Sha256Digest, ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0; 32];
    for (out, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let digit = |v: u8| -> Result<u8, ServiceError> {
            match v {
                b'0'..=b'9' => Ok(v - b'0'),
                b'a'..=b'f' => Ok(v - b'a' + 10),
                _ => Err(ServiceError::InvalidRequest),
            }
        };
        *out = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    Ok(Sha256Digest::new(bytes))
}
fn map_population_error(error: CurrentPopulationError) -> ServiceError {
    super::super::map_current_population_error(error)
}
/// Reuses the same source admission for an explicitly declared native fiscal issuer cohort.
#[allow(
    clippy::too_many_arguments,
    reason = "source clocks and owned worker remain explicit"
)]
pub(crate) async fn prepare_fixed_current_population(
    service: &crate::ResearchService,
    identities: std::sync::Arc<InstrumentContextReadCapability>,
    instrument_ids: Vec<InstrumentId>,
    financial_profile_digest: Sha256Digest,
    source_cutoff: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CurrentListedPopulation, ServiceError> {
    service
        .run_owned_research_io(deadline, cancellation, move |token| {
            let query = MarketDataInstrumentPopulationQuery::try_new(
                instrument_ids,
                source_cutoff,
                source_cutoff,
            )
            .map_err(map_instrument_service_error)?;
            let selected = identities
                .identity
                .instruments
                .pin_population_as_of(query, deadline, &token)
                .map_err(map_instrument_service_error)?;
            let admission = CurrentListedPopulationAdmission::try_new_fixed_cohort(
                selected,
                financial_profile_digest,
            )
            .map_err(map_population_error)?;
            identities.finish_current_population(admission, source_cutoff, deadline, &token)
        })
        .await
        .map_err(map_owned_read_error)?
}
