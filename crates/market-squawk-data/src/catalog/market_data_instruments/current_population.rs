//! Source-owned current listing membership; this never asserts historical survivorship.

use super::*;
use crate::{
    ListingReferenceGenerationReceipt, ListingReferenceGenerationSelection,
    ListingReferenceMembershipCursor, ListingReferenceMembershipPage,
    ListingReferenceMembershipPageState, ListingReferenceMembershipSelectionReceipt,
    ListingReferenceRightsState, Sha256Digest, UniverseId,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Maximum complete canonical population; enumeration remains bounded to256 rows per page.
pub const MAX_CURRENT_LISTED_POPULATION_MEMBERS: usize = 65_536;
const MAX_MEMBERS: usize = MAX_CURRENT_LISTED_POPULATION_MEMBERS;
const MAX_DIRECTORY_ROWS: usize = 65_536;
const MAX_BYTES: usize = 64 * 1024 * 1024;

/// Profile-selected asset scope, fixed before any financial values are requested.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrentListedPopulationScope {
    ListedEquities,
    ListedEquitiesAndEtfs,
}
/// Exhaustive catalog qualification and a declared analyzed cohort have different coverage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentListedPopulationSourceScope {
    CompleteQualifiedCatalog,
    DeclaredFixedCohort,
}
/// Source exclusions are retained before financial input preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CurrentPopulationExclusionReason {
    NoEffectiveCanonicalDefinition,
    OutsideProfileAssetScope,
    MissingOfficialListing,
    AmbiguousOfficialListing,
}
/// A source exclusion cannot remove an admitted member by caller assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentPopulationExclusion {
    instrument_id: InstrumentId,
    reason: CurrentPopulationExclusionReason,
}
impl CurrentPopulationExclusion {
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn reason(&self) -> CurrentPopulationExclusionReason {
        self.reason
    }
}
/// Original canonical and official directory authorities for one admitted current member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentListedPopulationMember {
    canonical: Arc<MarketDataInstrumentRecord>,
    listing: ListingReferenceRecord,
}
impl CurrentListedPopulationMember {
    pub fn instrument_id(&self) -> InstrumentId {
        self.canonical.definition().instrument_id()
    }
    pub fn canonical_record(&self) -> &MarketDataInstrumentRecord {
        &self.canonical
    }
    pub const fn listing_record(&self) -> &ListingReferenceRecord {
        &self.listing
    }
}
/// Opaque current source admission. No deserializer or public scalar constructor exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentListedPopulation {
    evidence: Arc<CurrentListedPopulationEvidence>,
    membership_as_of: Timestamp,
    research_uses: [Option<crate::research_use::RetainedSourceUseGrant>; 2],
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentListedPopulationEvidence {
    source_cutoff: Timestamp,
    scope: CurrentListedPopulationSourceScope,
    asset_scope: CurrentListedPopulationScope,
    profile_digest: Sha256Digest,
    universe_id: UniverseId,
    instrument_ids: Box<[InstrumentId]>,
    members: Box<[CurrentListedPopulationMember]>,
    exclusions: Box<[CurrentPopulationExclusion]>,
    // Every original in-scope definition remains retained, including ambiguous/missing listings.
    source_records: Box<[Arc<MarketDataInstrumentRecord>]>,
    directory_generation: ListingReferenceGenerationReceipt,
    directory_receipts: Box<[ListingReferenceMembershipSelectionReceipt]>,
    canonical_scanned: usize,
    canonical_scope_count: usize,
    directory_count: usize,
    source_population_digest: Sha256Digest,
    directory_digest: Sha256Digest,
    content_digest: Sha256Digest,
    audit_digest: Sha256Digest,
    retained_bytes: usize,
}
impl CurrentListedPopulation {
    pub fn instrument_ids(&self) -> &[InstrumentId] {
        &self.evidence.instrument_ids
    }
    pub fn members(&self) -> &[CurrentListedPopulationMember] {
        &self.evidence.members
    }
    pub fn exclusions(&self) -> &[CurrentPopulationExclusion] {
        &self.evidence.exclusions
    }
    pub fn source_cutoff(&self) -> Timestamp {
        self.evidence.source_cutoff
    }
    pub fn membership_as_of(&self) -> Timestamp {
        self.membership_as_of
    }
    pub fn source_scope(&self) -> CurrentListedPopulationSourceScope {
        self.evidence.scope
    }
    pub fn universe_id(&self) -> &UniverseId {
        &self.evidence.universe_id
    }
    pub fn content_digest(&self) -> Sha256Digest {
        self.evidence.content_digest
    }
    pub fn audit_digest(&self) -> Sha256Digest {
        self.evidence.audit_digest
    }
    pub fn source_population_digest(&self) -> Sha256Digest {
        self.evidence.source_population_digest
    }
    pub fn financial_profile_digest(&self) -> Sha256Digest {
        self.evidence.profile_digest
    }
    pub fn official_directory_digest(&self) -> Sha256Digest {
        self.evidence.directory_digest
    }
    pub fn canonical_scanned(&self) -> usize {
        self.evidence.canonical_scanned
    }
    pub fn canonical_scope_count(&self) -> usize {
        self.evidence.canonical_scope_count
    }
    pub fn official_directory_count(&self) -> usize {
        self.evidence.directory_count
    }
    pub fn retained_bytes(&self) -> usize {
        self.evidence.retained_bytes
    }
    pub(crate) fn contains(&self, id: InstrumentId) -> bool {
        self.evidence.instrument_ids.binary_search(&id).is_ok()
    }

    pub(crate) fn has_research_use(&self, requested: crate::ResearchUse) -> bool {
        use_index(requested).is_some_and(|index| self.research_uses[index].is_some())
    }
    pub(crate) fn research_use_digest(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/current-population-source-use/v1\0");
        hash.update(self.evidence.directory_generation.rights_id());
        for grant in &self.research_uses {
            hash.update([u8::from(grant.is_some())]);
            if let Some(grant) = grant {
                hash.update(grant.research_grant_id);
                hash.update(grant.rights_basis_digest);
                hash.update(grant.authorization_evidence.bytes());
                hash.update(grant.grant_evidence.bytes());
                for end in [grant.rights_expires_at, grant.grant_expires_at] {
                    hash.update([u8::from(end.is_some())]);
                    if let Some(at) = end {
                        hash.update(at.unix_nanos().to_be_bytes());
                    }
                }
            }
        }
        Sha256Digest::new(hash.finalize().into())
    }
    pub(crate) fn validate_research_use_in_catalog(
        &self,
        authority: &CatalogAuthority,
        requested: crate::ResearchUse,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CurrentPopulationError> {
        self.validate_in_catalog(authority, deadline, cancellation)?;
        let index = use_index(requested).ok_or(CurrentPopulationError::ResearchUseUnavailable)?;
        let expected = self.research_uses[index]
            .as_ref()
            .ok_or(CurrentPopulationError::ResearchUseUnavailable)?;
        let actual = read_population_use(
            authority,
            &self.evidence.directory_generation,
            requested,
            Some(expected.research_grant_id),
            deadline,
            cancellation,
        )?;
        if actual.as_ref() != Some(expected) {
            return Err(CurrentPopulationError::ResearchUseUnavailable);
        }
        Ok(())
    }

    /// Rechecks immutable selected source revisions under the existing borrowed writer.
    pub(crate) fn validate_in_catalog(
        &self,
        authority: &CatalogAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), CurrentPopulationError> {
        check_operation(deadline, cancellation)?;
        let connection = &authority.catalog().connection;
        let busy: u32 = connection
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .map_err(MarketDataInstrumentCatalogError::from)?;
        connection
            .busy_timeout(std::time::Duration::ZERO)
            .map_err(MarketDataInstrumentCatalogError::from)?;
        let result = (|| {
            crate::ListingReferenceReadCapability::require_retained_generation_in_catalog(
                authority,
                &self.evidence.directory_generation,
                deadline,
                cancellation,
            )?;
            for expected in &self.evidence.source_records {
                require_current_market_data_instrument(
                    authority,
                    expected,
                    deadline,
                    cancellation,
                )?;
            }
            Ok(())
        })();
        let restore = connection
            .busy_timeout(std::time::Duration::from_millis(u64::from(busy)))
            .map_err(MarketDataInstrumentCatalogError::from);
        check_operation(deadline, cancellation)?;
        restore?;
        result
    }
}

/// Inert original directory-use evidence retained by the existing dataset receipt.
/// Deserialization does not issue a source-population admission or a research permit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetPopulationSourceUse {
    source_id: SourceId,
    directory_generation: [u8; 32],
    rights_id: [u8; 32],
    requested_use: PopulationResearchUse,
    grant: crate::research_use::RetainedSourceUseGrant,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PopulationResearchUse {
    LocalAnalysis,
    Train,
}
impl DatasetPopulationSourceUse {
    pub const fn requested_use(&self) -> crate::ResearchUse {
        match self.requested_use {
            PopulationResearchUse::LocalAnalysis => crate::ResearchUse::LocalAnalysis,
            PopulationResearchUse::Train => crate::ResearchUse::Train,
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.source_id.as_str().len()
    }
    pub(crate) fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/dataset-population-source-use/v1\0");
        hash.update((self.source_id.as_str().len() as u64).to_be_bytes());
        hash.update(self.source_id.as_str().as_bytes());
        hash.update(self.directory_generation);
        hash.update(self.rights_id);
        hash.update([self.requested_use as u8]);
        hash.update(self.grant.research_grant_id);
        hash.update(self.grant.rights_basis_digest);
        hash.update(self.grant.authorization_evidence.bytes());
        hash.update(self.grant.grant_evidence.bytes());
        for at in [self.grant.rights_expires_at, self.grant.grant_expires_at] {
            hash.update([u8::from(at.is_some())]);
            if let Some(at) = at {
                hash.update(at.unix_nanos().to_be_bytes());
            }
        }
        hash.finalize().into()
    }
    pub(crate) fn validate(
        &self,
        required: crate::ResearchUse,
    ) -> Result<(), crate::ResearchUseCatalogError> {
        if self.requested_use() != required
            || self.directory_generation == [0; 32]
            || self.rights_id == [0; 32]
            || self.grant.research_grant_id == [0; 32]
        {
            return Err(crate::ResearchUseCatalogError::CorruptCatalog);
        }
        Ok(())
    }
    /// The caller owns the existing bounded catalog read snapshot and SQLite progress handler.
    pub(crate) fn validate_current(
        &self,
        connection: &rusqlite::Connection,
        required: crate::ResearchUse,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), crate::ResearchUseCatalogError> {
        self.validate(required)?;
        if cancellation.is_cancelled() {
            return Err(crate::ResearchUseCatalogError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(crate::ResearchUseCatalogError::DeadlineExceeded);
        }
        let result = (|| {
            let now = super::super::storage::now_timestamp()
                .map_err(crate::ResearchUseCatalogError::Catalog)?;
            let frontier = crate::research_use::source_use_frontier(connection, now)?;
            let actual = crate::research_use::select_source_use_grant(
                connection,
                self.rights_id,
                &self.source_id,
                required,
                now,
                frontier,
                Some(self.grant.research_grant_id),
                cancellation,
                deadline,
            )?;
            match actual {
                crate::research_use::SourceGrantSelection::Selected(grant)
                    if grant == self.grant =>
                {
                    Ok(())
                }
                crate::research_use::SourceGrantSelection::Denied(
                    crate::ResearchUseDenialReason::Revoked,
                ) => Err(crate::ResearchUseCatalogError::Revoked),
                crate::research_use::SourceGrantSelection::Denied(
                    crate::ResearchUseDenialReason::Expired,
                ) => Err(crate::ResearchUseCatalogError::Expired),
                _ => Err(crate::ResearchUseCatalogError::InvalidGrant),
            }
        })();
        if cancellation.is_cancelled() {
            return Err(crate::ResearchUseCatalogError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(crate::ResearchUseCatalogError::DeadlineExceeded);
        }
        result
    }
}
impl CurrentListedPopulation {
    pub(crate) fn source_use(
        &self,
        requested: crate::ResearchUse,
    ) -> Result<DatasetPopulationSourceUse, CurrentPopulationError> {
        let index = use_index(requested).ok_or(CurrentPopulationError::ResearchUseUnavailable)?;
        let grant = self.research_uses[index]
            .clone()
            .ok_or(CurrentPopulationError::ResearchUseUnavailable)?;
        Ok(DatasetPopulationSourceUse {
            source_id: self.evidence.directory_generation.source_id().clone(),
            directory_generation: self
                .evidence
                .directory_generation
                .generation_digest()
                .bytes(),
            rights_id: self.evidence.directory_generation.rights_id(),
            requested_use: match requested {
                crate::ResearchUse::LocalAnalysis => PopulationResearchUse::LocalAnalysis,
                crate::ResearchUse::Train => PopulationResearchUse::Train,
                crate::ResearchUse::Display => {
                    return Err(CurrentPopulationError::ResearchUseUnavailable);
                }
            },
            grant,
        })
    }
}

/// Inert exact partition recipe; only the original opaque source seal mints admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetPopulationPartition {
    full_population_digest: [u8; 32],
    ordinal: usize,
    partition_count: usize,
    member_ids: Box<[InstrumentId]>,
    partition_digest: [u8; 32],
}
impl DatasetPopulationPartition {
    pub const fn full_population_digest(&self) -> [u8; 32] {
        self.full_population_digest
    }
    pub const fn ordinal(&self) -> usize {
        self.ordinal
    }
    pub const fn partition_count(&self) -> usize {
        self.partition_count
    }
    pub fn member_ids(&self) -> &[InstrumentId] {
        &self.member_ids
    }
    pub const fn partition_digest(&self) -> [u8; 32] {
        self.partition_digest
    }
    /// Checks the inert partition recipe and digest, without granting source membership authority.
    pub fn validate(&self, full_count: usize) -> Result<(), crate::DatasetBuildError> {
        if full_count == 0
            || full_count > MAX_MEMBERS
            || self.partition_count < full_count.div_ceil(128)
            || self.partition_count > full_count
            || self.ordinal >= self.partition_count
            || self.member_ids.is_empty()
            || self.member_ids.len() > full_count.min(128)
            || self.member_ids.windows(2).any(|v| v[0] >= v[1])
            || self.full_population_digest == [0; 32]
            || self.digest() != self.partition_digest
        {
            return Err(crate::DatasetBuildError::InvalidRequest);
        }
        Ok(())
    }
    fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/current-population-partition/v1\0");
        hash.update(self.full_population_digest);
        hash.update((self.ordinal as u64).to_be_bytes());
        hash.update((self.partition_count as u64).to_be_bytes());
        hash.update((self.member_ids.len() as u64).to_be_bytes());
        for id in &self.member_ids {
            hash.update(id.as_uuid().as_bytes());
        }
        hash.finalize().into()
    }
}
/// A deterministic member range shares the original complete source seal by Arc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CurrentListedPopulationPartition {
    population: CurrentListedPopulation,
    descriptor: DatasetPopulationPartition,
}
impl CurrentListedPopulationPartition {
    pub const fn population(&self) -> &CurrentListedPopulation {
        &self.population
    }
    pub fn instrument_ids(&self) -> &[InstrumentId] {
        self.descriptor.member_ids()
    }
    pub const fn ordinal(&self) -> usize {
        self.descriptor.ordinal()
    }
    pub const fn partition_count(&self) -> usize {
        self.descriptor.partition_count()
    }
    pub const fn descriptor(&self) -> &DatasetPopulationPartition {
        &self.descriptor
    }
    pub(crate) fn contains(&self, id: InstrumentId) -> bool {
        self.instrument_ids().binary_search(&id).is_ok()
    }
}
impl CurrentListedPopulation {
    /// Fixed source partitions are chosen before any financial feature or outcome is requested.
    pub fn partitions(
        &self,
    ) -> Result<Box<[CurrentListedPopulationPartition]>, CurrentPopulationError> {
        let ends = (1..=self.instrument_ids().len()).filter(|end| end % 128 == 0 || *end == self.instrument_ids().len()).collect::<Vec<_>>();
        self.partitions_with_ends(&ends)
    }

    /// Applies an inert storage schedule to this original complete source seal. Every source
    /// member appears once, in source order; schedule boundaries carry no economic authority.
    pub fn partitions_with_ends(
        &self, ends: &[usize],
    ) -> Result<Box<[CurrentListedPopulationPartition]>, CurrentPopulationError> {
        let count = self.instrument_ids().len();
        if ends.len() > count || ends.last().copied().unwrap_or(0) != count {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let mut previous = 0;
        let mut values = Vec::new();
        values.try_reserve_exact(ends.len()).map_err(|_| CurrentPopulationError::LimitExceeded)?;
        for (ordinal, end) in ends.iter().copied().enumerate() {
            if end <= previous || end > count || end - previous > 128 {
                return Err(CurrentPopulationError::InvalidInput);
            }
            let mut descriptor = DatasetPopulationPartition {
                full_population_digest: self.content_digest().bytes(), ordinal,
                partition_count: ends.len(), member_ids: self.instrument_ids()[previous..end].to_vec().into_boxed_slice(),
                partition_digest: [0; 32],
            };
            descriptor.partition_digest = descriptor.digest();
            values.push(CurrentListedPopulationPartition { population: self.clone(), descriptor });
            previous = end;
        }
        Ok(values.into_boxed_slice())
    }
}

/// Bounded admission over original opaque pages, never caller-authored membership rows.
#[derive(Debug)]
pub struct CurrentListedPopulationAdmission {
    source_cutoff: Timestamp,
    asset_scope: CurrentListedPopulationScope,
    scope: CurrentListedPopulationSourceScope,
    profile_digest: Sha256Digest,
    canonical_cursor: Option<MarketDataInstrumentEnumerationCursor>,
    canonical_started: bool,
    canonical_complete: bool,
    records: Vec<MarketDataInstrumentRecord>,
    exclusions: Vec<CurrentPopulationExclusion>,
    canonical_scanned: usize,
    canonical_hash: Sha256,
    directory_hash: Sha256,
    listing_cursor: Option<ListingReferenceMembershipCursor>,
    listing_complete: bool,
    directory_generation: Option<ListingReferenceGenerationReceipt>,
    receipts: Vec<ListingReferenceMembershipSelectionReceipt>,
    directory_count: usize,
    index: BTreeMap<(VenueId, String), Vec<usize>>,
    matches: Vec<(usize, Option<ListingReferenceRecord>)>,
    retained_bytes: usize,
}
impl CurrentListedPopulationAdmission {
    pub fn try_new(
        source_cutoff: Timestamp,
        asset_scope: CurrentListedPopulationScope,
        profile_digest: Sha256Digest,
    ) -> Result<Self, CurrentPopulationError> {
        if source_cutoff.unix_nanos() <= 0 || profile_digest.bytes() == [0; 32] {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let mut canonical_hash = Sha256::new();
        canonical_hash.update(b"market-squawk/current-population-canonical/v1\0");
        canonical_hash.update(source_cutoff.unix_nanos().to_be_bytes());
        let mut directory_hash = Sha256::new();
        directory_hash.update(b"market-squawk/current-population-directory/v1\0");
        directory_hash.update(source_cutoff.unix_nanos().to_be_bytes());
        Ok(Self {
            source_cutoff,
            asset_scope,
            scope: CurrentListedPopulationSourceScope::CompleteQualifiedCatalog,
            profile_digest,
            canonical_cursor: None,
            canonical_started: false,
            canonical_complete: false,
            records: Vec::new(),
            exclusions: Vec::new(),
            canonical_scanned: 0,
            canonical_hash,
            directory_hash,
            listing_cursor: None,
            listing_complete: false,
            directory_generation: None,
            receipts: Vec::new(),
            directory_count: 0,
            index: BTreeMap::new(),
            matches: Vec::new(),
            retained_bytes: size_of::<Self>(),
        })
    }
    pub fn try_new_fixed_cohort(
        canonical: MarketDataInstrumentPopulationSelection,
        profile_digest: Sha256Digest,
    ) -> Result<Self, CurrentPopulationError> {
        if canonical.query().effective_at() != canonical.query().knowledge_at()
            || canonical.disposition() != MarketDataInstrumentPopulationDisposition::Complete
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let mut value = Self::try_new(
            canonical.query().knowledge_at(),
            CurrentListedPopulationScope::ListedEquitiesAndEtfs,
            profile_digest,
        )?;
        value.scope = CurrentListedPopulationSourceScope::DeclaredFixedCohort;
        value
            .canonical_hash
            .update(canonical.receipt_digest().bytes());
        value.canonical_scanned = canonical.query().instrument_ids().len();
        for record in canonical.records {
            value.admit_record(record)?;
        }
        if !value.exclusions.is_empty() {
            return Err(CurrentPopulationError::Unavailable);
        }
        value.canonical_started = true;
        value.canonical_complete = true;
        value.prepare_index()?;
        Ok(value)
    }
    pub fn consume_canonical_page(
        &mut self,
        page: MarketDataInstrumentEnumerationPage,
    ) -> Result<(), CurrentPopulationError> {
        if self.canonical_complete
            || self.directory_generation.is_some()
            || page.knowledge_at() != self.source_cutoff
            || page.effective_at() != self.source_cutoff
            || page.requested_cursor != self.canonical_cursor
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        self.reserve(page.retained_bytes())?;
        self.canonical_scanned = self
            .canonical_scanned
            .checked_add(page.instrument_ids().len())
            .ok_or(CurrentPopulationError::LimitExceeded)?;
        if self.canonical_scanned > MAX_MEMBERS {
            return Err(CurrentPopulationError::LimitExceeded);
        }
        self.canonical_hash.update(page.receipt_digest().bytes());
        self.canonical_cursor = page.next_cursor.clone();
        self.canonical_complete = page.complete();
        self.canonical_started = true;
        if let Some(population) = page.population {
            self.charge(population.exclusions.len() * size_of::<CurrentPopulationExclusion>())?;
            self.exclusions
                .try_reserve_exact(population.exclusions.len())
                .map_err(|_| CurrentPopulationError::LimitExceeded)?;
            for excluded in population.exclusions {
                self.exclusions
                    .try_reserve_exact(1)
                    .map_err(|_| CurrentPopulationError::LimitExceeded)?;
                self.exclusions.push(CurrentPopulationExclusion {
                    instrument_id: excluded.instrument_id(),
                    reason: CurrentPopulationExclusionReason::NoEffectiveCanonicalDefinition,
                });
            }
            for record in population.records {
                self.admit_record(record)?;
            }
        }
        if self.canonical_complete {
            self.prepare_index()?;
        }
        Ok(())
    }
    fn admit_record(
        &mut self,
        record: MarketDataInstrumentRecord,
    ) -> Result<(), CurrentPopulationError> {
        let asset = record.definition().asset_class();
        let admitted = asset == AssetClass::Equity
            || (self.asset_scope == CurrentListedPopulationScope::ListedEquitiesAndEtfs
                && asset == AssetClass::Fund);
        if !admitted {
            self.charge(size_of::<CurrentPopulationExclusion>())?;
            self.exclusions
                .try_reserve_exact(1)
                .map_err(|_| CurrentPopulationError::LimitExceeded)?;
            self.exclusions.push(CurrentPopulationExclusion {
                instrument_id: record.definition().instrument_id(),
                reason: CurrentPopulationExclusionReason::OutsideProfileAssetScope,
            });
            return Ok(());
        }
        if record.published_at() > self.source_cutoff {
            return Err(CurrentPopulationError::InvalidInput);
        }
        self.charge(record.retained_bytes()?)?;
        self.records
            .try_reserve_exact(1)
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        self.records.push(record);
        Ok(())
    }
    fn prepare_index(&mut self) -> Result<(), CurrentPopulationError> {
        if self
            .records
            .windows(2)
            .any(|v| v[0].definition().instrument_id() >= v[1].definition().instrument_id())
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        for index in 0..self.records.len() {
            for mapping in self.records[index].definition().venue_mappings() {
                let extra =
                    mapping.venue_id().as_str().len() + mapping.venue_symbol().as_str().len() + 256;
                self.retained_bytes = self
                    .retained_bytes
                    .checked_add(extra)
                    .filter(|v| *v <= MAX_BYTES)
                    .ok_or(CurrentPopulationError::LimitExceeded)?;
                self.index
                    .entry((
                        mapping.venue_id().clone(),
                        mapping.venue_symbol().as_str().to_owned(),
                    ))
                    .or_default()
                    .push(index);
            }
        }
        self.charge(self.records.len() * size_of::<(usize, Option<ListingReferenceRecord>)>())?;
        self.matches
            .try_reserve_exact(self.records.len())
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        self.matches.resize_with(self.records.len(), || (0, None));
        Ok(())
    }
    pub fn consume_listing_page(
        &mut self,
        page: ListingReferenceMembershipPage,
    ) -> Result<(), CurrentPopulationError> {
        if !self.canonical_started
            || !self.canonical_complete
            || self.listing_complete
            || page.records().len() > 256
            || page.receipt().requested_cursor() != self.listing_cursor.as_ref()
            || page.receipt().selection()
                != ListingReferenceGenerationSelection::AsOf(self.source_cutoff)
            || page.receipt().requested_knowledge_at() != self.source_cutoff
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        self.reserve(256 * 16_384)?;
        let generation = page
            .generation()
            .ok_or(CurrentPopulationError::Unavailable)?;
        if generation.published_at() > self.source_cutoff
            || self
                .directory_generation
                .as_ref()
                .is_some_and(|v| v != generation)
            || page.receipt().rights_id() != Some(generation.rights_id())
            || page.receipt().source_revision_digest() != Some(generation.source_revision_digest())
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        self.directory_generation = Some(generation.clone());
        self.directory_hash
            .update(generation.generation_digest().bytes());
        self.directory_hash
            .update(page.receipt().ordered_rows_digest().bytes());
        self.directory_hash.update(generation.rights_id());
        self.directory_hash
            .update(generation.source_revision_digest().bytes());
        self.directory_count = self
            .directory_count
            .checked_add(page.records().len())
            .filter(|v| *v <= MAX_DIRECTORY_ROWS)
            .ok_or(CurrentPopulationError::LimitExceeded)?;
        for listing in page.records() {
            if listing.generation() != generation
                || listing.source_file().available_at() > self.source_cutoff
                || listing.effective_at() > self.source_cutoff
            {
                return Err(CurrentPopulationError::InvalidInput);
            }
            let mut indices = BTreeSet::new();
            for symbol in [
                Some(listing.provider_symbol()),
                listing.cqs_symbol(),
                listing.nasdaq_symbol(),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(values) = self
                    .index
                    .get(&(listing.listing_venue().clone(), symbol.to_owned()))
                {
                    indices.extend(values.iter().copied());
                }
            }
            for index in indices {
                let definition = self.records[index].definition();
                if listing.is_test_issue()
                    || listing.generation().rights_state()
                        != ListingReferenceRightsState::AdmittedScoped
                    || listing.is_etf() != (definition.asset_class() == AssetClass::Fund)
                {
                    continue;
                }
                self.matches[index].0 = self.matches[index]
                    .0
                    .checked_add(1)
                    .ok_or(CurrentPopulationError::LimitExceeded)?;
                if self.matches[index].1.is_none() {
                    self.charge(listing_retained_bytes(listing)?)?;
                    self.matches[index].1 = Some(listing.clone());
                }
            }
        }
        self.charge(2048)?;
        self.receipts
            .try_reserve_exact(1)
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        self.receipts.push(page.receipt().clone());
        self.listing_complete = page.state() == ListingReferenceMembershipPageState::Complete;
        self.listing_cursor = page.next_cursor().cloned();
        if self.listing_complete == self.listing_cursor.is_some() {
            return Err(CurrentPopulationError::InvalidInput);
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<CurrentListedPopulation, CurrentPopulationError> {
        if !self.canonical_complete || !self.listing_complete {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let generation = self
            .directory_generation
            .take()
            .ok_or(CurrentPopulationError::Unavailable)?;
        let source_population_digest = Sha256Digest::new(self.canonical_hash.finalize().into());
        self.directory_hash
            .update((self.directory_count as u64).to_be_bytes());
        let directory_digest = Sha256Digest::new(self.directory_hash.finalize().into());
        let finish_bytes = self
            .records
            .len()
            .checked_mul(
                size_of::<CurrentListedPopulationMember>()
                    + size_of::<InstrumentId>()
                    + 2 * size_of::<Arc<MarketDataInstrumentRecord>>()
                    + 2 * size_of::<usize>()
                    + size_of::<CurrentPopulationExclusion>(),
            )
            .and_then(|v| v.checked_add(size_of::<CurrentListedPopulationEvidence>() + 512))
            .ok_or(CurrentPopulationError::LimitExceeded)?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(finish_bytes)
            .filter(|v| *v <= MAX_BYTES)
            .ok_or(CurrentPopulationError::LimitExceeded)?;
        let mut members = Vec::new();
        let mut ids = Vec::new();
        members
            .try_reserve_exact(self.records.len())
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        ids.try_reserve_exact(self.records.len())
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        let canonical_scope_count = self.records.len();
        let mut source_records = Vec::new();
        source_records
            .try_reserve_exact(canonical_scope_count)
            .map_err(|_| CurrentPopulationError::LimitExceeded)?;
        for (record, (count, listing)) in self.records.into_iter().zip(self.matches) {
            let record = Arc::new(record);
            source_records.push(Arc::clone(&record));
            let id = record.definition().instrument_id();
            if count != 1 {
                self.exclusions
                    .try_reserve_exact(1)
                    .map_err(|_| CurrentPopulationError::LimitExceeded)?;
                self.exclusions.push(CurrentPopulationExclusion {
                    instrument_id: id,
                    reason: if count == 0 {
                        CurrentPopulationExclusionReason::MissingOfficialListing
                    } else {
                        CurrentPopulationExclusionReason::AmbiguousOfficialListing
                    },
                });
                continue;
            }
            let listing = listing.ok_or(CurrentPopulationError::InvalidInput)?;
            ids.push(id);
            members.push(CurrentListedPopulationMember {
                canonical: record,
                listing,
            });
        }
        if self.scope == CurrentListedPopulationSourceScope::DeclaredFixedCohort
            && !self.exclusions.is_empty()
        {
            return Err(CurrentPopulationError::Unavailable);
        }
        self.exclusions.sort_unstable_by_key(|v| v.instrument_id);
        if ids.len() + self.exclusions.len() != self.canonical_scanned {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let mut content = Sha256::new();
        content.update(b"market-squawk/current-listed-population/v1\0");
        content.update([self.scope as u8, self.asset_scope as u8]);
        content.update(self.profile_digest.bytes());
        content.update(source_population_digest.bytes());
        content.update(directory_digest.bytes());
        for member in &members {
            content.update(member.instrument_id().as_uuid().as_bytes());
            content.update(member.canonical.revision_digest().bytes());
            content.update(
                member
                    .listing
                    .record_payload_evidence()
                    .content_digest()
                    .bytes(),
            );
        }
        let content_digest = Sha256Digest::new(content.finalize().into());
        let mut audit = Sha256::new();
        audit.update(b"market-squawk/current-listed-population-audit/v1\0");
        audit.update(content_digest.bytes());
        for value in &self.exclusions {
            audit.update(value.instrument_id.as_uuid().as_bytes());
            audit.update([value.reason as u8]);
        }
        let audit_digest = Sha256Digest::new(audit.finalize().into());
        let mut name = String::from("current-listed-");
        for byte in content_digest.bytes() {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").map_err(|_| CurrentPopulationError::LimitExceeded)?;
        }
        let universe_id =
            UniverseId::try_from(name).map_err(|_| CurrentPopulationError::InvalidInput)?;
        Ok(CurrentListedPopulation {
            membership_as_of: self.source_cutoff,
            research_uses: [None, None],
            evidence: Arc::new(CurrentListedPopulationEvidence {
                source_cutoff: self.source_cutoff,
                scope: self.scope,
                asset_scope: self.asset_scope,
                profile_digest: self.profile_digest,
                universe_id,
                instrument_ids: ids.into_boxed_slice(),
                members: members.into_boxed_slice(),
                exclusions: self.exclusions.into_boxed_slice(),
                canonical_scope_count,
                source_records: source_records.into_boxed_slice(),
                directory_generation: generation,
                directory_receipts: self.receipts.into_boxed_slice(),
                canonical_scanned: self.canonical_scanned,
                directory_count: self.directory_count,
                source_population_digest,
                directory_digest,
                content_digest,
                audit_digest,
                retained_bytes: self.retained_bytes,
            }),
        })
    }
    fn reserve(&self, bytes: usize) -> Result<(), CurrentPopulationError> {
        self.retained_bytes
            .checked_add(bytes)
            .filter(|v| *v <= MAX_BYTES)
            .ok_or(CurrentPopulationError::LimitExceeded)
            .map(|_| ())
    }
    fn charge(&mut self, bytes: usize) -> Result<(), CurrentPopulationError> {
        self.reserve(bytes)?;
        self.retained_bytes += bytes;
        Ok(())
    }
}
impl MarketDataInstrumentReadCapability {
    pub fn revalidate_current_listed_population(
        &self,
        expected: &CurrentListedPopulation,
        analytical_cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CurrentListedPopulation, CurrentPopulationError> {
        check_operation(deadline, cancellation)?;
        if analytical_cutoff < expected.source_cutoff()
            || analytical_cutoff
                > super::super::storage::now_timestamp()
                    .map_err(MarketDataInstrumentCatalogError::SourceAuthority)?
        {
            return Err(CurrentPopulationError::InvalidInput);
        }
        let authority = self
            .authority
            .try_lock()
            .map_err(|_| CurrentPopulationError::AuthorityUnavailable)?;
        expected.validate_in_catalog(&authority, deadline, cancellation)?;
        let mut actual = expected.clone();
        actual.membership_as_of = analytical_cutoff;
        for (index, requested) in [crate::ResearchUse::LocalAnalysis, crate::ResearchUse::Train]
            .into_iter()
            .enumerate()
        {
            actual.research_uses[index] = read_population_use(
                &authority,
                &actual.evidence.directory_generation,
                requested,
                None,
                deadline,
                cancellation,
            )?;
        }
        Ok(actual)
    }
}
fn use_index(requested: crate::ResearchUse) -> Option<usize> {
    match requested {
        crate::ResearchUse::LocalAnalysis => Some(0),
        crate::ResearchUse::Train => Some(1),
        crate::ResearchUse::Display => None,
    }
}
fn read_population_use(
    authority: &CatalogAuthority,
    generation: &ListingReferenceGenerationReceipt,
    requested: crate::ResearchUse,
    expected: Option<[u8; 32]>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<crate::research_use::RetainedSourceUseGrant>, CurrentPopulationError> {
    check_operation(deadline, cancellation)?;
    let connection = &authority.catalog().connection;
    let busy: u32 = connection
        .pragma_query_value(None, "busy_timeout", |row| row.get(0))
        .map_err(MarketDataInstrumentCatalogError::from)?;
    connection
        .busy_timeout(std::time::Duration::ZERO)
        .map_err(MarketDataInstrumentCatalogError::from)?;
    let install = install_progress_handler(connection, deadline, cancellation);
    let result = (|| {
        install?;
        let now = super::super::storage::now_timestamp()
            .map_err(MarketDataInstrumentCatalogError::SourceAuthority)?;
        let frontier = crate::research_use::source_use_frontier(connection, now)?;
        match crate::research_use::select_source_use_grant(
            connection,
            generation.rights_id(),
            generation.source_id(),
            requested,
            now,
            frontier,
            expected,
            cancellation,
            deadline,
        )? {
            crate::research_use::SourceGrantSelection::Selected(grant) => Ok(Some(grant)),
            crate::research_use::SourceGrantSelection::Denied(_) => Ok(None),
        }
    })();
    let progress_cleanup = clear_progress_handler(connection);
    let busy_cleanup = connection
        .busy_timeout(std::time::Duration::from_millis(u64::from(busy)))
        .map_err(MarketDataInstrumentCatalogError::from);
    check_operation(deadline, cancellation)?;
    progress_cleanup?;
    busy_cleanup?;
    result
}

fn listing_retained_bytes(value: &ListingReferenceRecord) -> Result<usize, CurrentPopulationError> {
    let file = value.source_file();
    let strings = [
        value.provider_symbol(),
        value.security_name(),
        value.listing_venue().as_str(),
        value.cqs_symbol().unwrap_or(""),
        value.nasdaq_symbol().unwrap_or(""),
        value.record_revision().as_str(),
        file.source_object_id().as_str(),
        file.source_reference().as_str(),
        file.file_creation_time(),
    ];
    strings
        .into_iter()
        .try_fold(size_of::<ListingReferenceRecord>() + 4096, |n, s| {
            n.checked_add(s.len() * 2)
                .ok_or(CurrentPopulationError::LimitExceeded)
        })
}
/// Source admission failures preserve ordinary unavailability, resources and catalog errors.
#[derive(Debug, Error)]
pub enum CurrentPopulationError {
    #[error("current population source use is not authorized")]
    ResearchUseUnavailable,
    #[error("current population source-use registry failed: {0}")]
    ResearchUse(#[from] crate::ResearchUseCatalogError),
    #[error("current population source input is invalid")]
    InvalidInput,
    #[error("current population source coverage is unavailable")]
    Unavailable,
    #[error("current population source revisions changed")]
    Superseded,
    #[error("current population exceeds its bounded resources")]
    LimitExceeded,
    #[error("current population catalog is busy")]
    AuthorityUnavailable,
    #[error("current population canonical source failed: {0}")]
    Canonical(#[from] MarketDataInstrumentCatalogError),
    #[error("current population directory source failed: {0}")]
    Listing(#[from] crate::ListingReferenceError),
}
