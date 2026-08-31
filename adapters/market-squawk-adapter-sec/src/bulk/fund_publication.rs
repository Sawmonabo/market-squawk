//! Bounded, accession-scoped SEC fund evidence preparation.
//!
//! Quarterly N-PORT and N-CEN objects can contain hundreds of millions of rows. This module does
//! not pretend that an entire archive fits in a canonical batch. It selects one exact filing
//! accession (and, for N-CEN, one exact `FUND_ID`) while the complete archive is still validated,
//! retains at most the domain's 100,000 source-row ceiling, maps that selection to the dedicated
//! provider-neutral fund contract, and physically seals aligned provider-native and row-map
//! partitions. The application/data layer remains the sole owner of the registered Arrow schema,
//! canonical Parquet staging, and final atomic logical-publication binding.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroU64};

use chrono::{Datelike, NaiveDate};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, Currency, DigestAlgorithm, EvidenceDigest,
    FUND_HOLDINGS_SCHEMA_NAME, FUND_HOLDINGS_SCHEMA_VERSION, FundAmendmentState,
    FundCurrencyAmount, FundEtfMechanics, FundEvidenceRecord, FundExchangeAssociation,
    FundFilingChronology, FundFilingIdentity, FundHoldingAssociations, FundHoldingQuantity,
    FundHoldingSecurityIdentity, FundHoldingSupplementEvidence, FundHoldingUnit,
    FundLineageRowRange, FundMissingState, FundPortfolioHoldingAttributes,
    FundPortfolioHoldingEvidence, FundReleaseCoverage, FundReportAttributes, FundReportEvidence,
    FundReportedDecimal, FundReportedValue, FundRevisionEvidence, FundRevisionLink,
    FundRevisionStatus, FundShareClassAttributes, FundShareClassEvidence, FundShareClassIdentity,
    FundSourceFamily, FundSourceLineage, FundSourceRowEvidence, FundSourceTable, FundSourceText,
    FundSupplementDisposition, Isin, MAX_FUND_SOURCE_ROWS, SchemaVersion, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{ResearchObjectControl, SealedResearchJournalStore};
use market_squawk_sources::{
    LogicalItemRange, LogicalPartitionFamily, LogicalPartitionSetAdmission,
    PendingLogicalPartitionSet, ProviderLogicalTerminalInput, SEC_EDGAR_SOURCE_ID,
    SealedLogicalObjectInput, SealedLogicalPartitionInput, StagedLogicalItemCoordinate,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::model::SecExactNumber;
use super::{
    SecBulkCoverage, SecBulkError, SecBulkFamily, SecBulkJoinDomain,
    SecBulkLogicalPublicationHandoff, SecBulkLogicalRow, SecBulkPendingLogicalRowSink,
    SecBulkProjectionDisposition, SecBulkProviderProjection, SecBulkStagedLogicalPublication,
    SecBulkTableKind, SecBulkTypedValue, SecNcenEtfRow, SecNcenFundRow, SecNcenRegistrantRow,
    SecNcenSubmissionRow, SecNportFundRow, SecNportHoldingRow, SecNportIdentifierRow,
    SecNportRegistrantRow, SecNportSubmissionRow,
};

const SEC_FUND_NATIVE_SCHEMA_DOMAIN: &[u8] = b"market-squawk/sec-fund/provider-native-envelope/v1";
const SEC_FUND_ROW_MAP_SCHEMA_DOMAIN: &[u8] = b"market-squawk/sec-fund/canonical-row-map/v1";
const SEC_FUND_CANONICAL_PARTITION_DOMAIN: &[u8] = b"market-squawk/sec-fund/canonical-partition/v1";
const SEC_FUND_SOURCE_REVISION_DOMAIN: &[u8] = b"market-squawk/sec-fund/source-revision/v1";
const SEC_FUND_PREPARATION_DOMAIN: &[u8] = b"market-squawk/sec-fund/preparation/v1";
const MAX_SELECTED_NATIVE_BYTES: u64 = 512 * 1024 * 1024;
const SELECTED_ROW_ACCOUNTING_OVERHEAD: u64 = 4 * 1024;

/// One bounded canonical publication scope inside a completely validated quarterly object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum SecFundPublicationScope {
    /// One exact N-PORT accession and every holding owned by it.
    Nport {
        /// Dashed EDGAR accession.
        accession: SourceIdentifier,
    },
    /// One exact N-CEN accession and fund identifier.
    Ncen {
        /// Dashed EDGAR accession.
        accession: SourceIdentifier,
        /// Exact provider `FUND_ID` scoped by the accession.
        fund_id: SourceIdentifier,
    },
}

impl SecFundPublicationScope {
    /// Selects one N-PORT filing without widening to a quarter-wide in-memory projection.
    pub fn try_nport(accession: SourceIdentifier) -> Result<Self, SecBulkError> {
        validate_accession(&accession)?;
        Ok(Self::Nport { accession })
    }

    /// Selects one N-CEN fund record without treating fund name or ticker as identity.
    pub fn try_ncen(
        accession: SourceIdentifier,
        fund_id: SourceIdentifier,
    ) -> Result<Self, SecBulkError> {
        validate_accession(&accession)?;
        if fund_id.as_str().is_empty() {
            return Err(SecBulkError::InvalidCanonicalMapping);
        }
        Ok(Self::Ncen { accession, fund_id })
    }

    /// Returns the selected form family.
    pub const fn family(&self) -> SecBulkFamily {
        match self {
            Self::Nport { .. } => SecBulkFamily::Nport,
            Self::Ncen { .. } => SecBulkFamily::Ncen,
        }
    }

    /// Returns the exact filing accession.
    pub const fn accession(&self) -> &SourceIdentifier {
        match self {
            Self::Nport { accession } | Self::Ncen { accession, .. } => accession,
        }
    }

    /// Returns the exact N-CEN fund identifier, when applicable.
    pub const fn fund_id(&self) -> Option<&SourceIdentifier> {
        match self {
            Self::Nport { .. } => None,
            Self::Ncen { fund_id, .. } => Some(fund_id),
        }
    }
}

/// Separately governed identity authority used during SEC canonical mapping.
///
/// Implementations may return explicit unresolved/ambiguous held-security states. Fund identity
/// itself must be exact because report/share-class rows cannot exist without a governed fund.
pub trait SecFundIdentityAuthority {
    /// Resolves exact SEC `SERIES_ID` to an already governed canonical share-class identity.
    fn resolve_share_class(
        &mut self,
        series_id: &SourceIdentifier,
        cutoff: Timestamp,
    ) -> Result<FundShareClassIdentity, SecBulkError>;

    /// Resolves only authoritative CUSIP/ISIN evidence; names and tickers are associations only.
    fn resolve_holding_security(
        &mut self,
        holding: &SecNportHoldingRow,
        identifiers: &[SecNportIdentifierRow],
        cutoff: Timestamp,
    ) -> Result<FundHoldingSecurityIdentity, SecBulkError>;
}

/// Caller-supplied physical partition ceilings for one bounded accession publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecFundPartitionAdmissions {
    native: LogicalPartitionSetAdmission,
    row_map: LogicalPartitionSetAdmission,
}

impl SecFundPartitionAdmissions {
    /// Uses independent byte ceilings while retaining an exact one-row-to-one-row alignment.
    pub const fn new(
        native: LogicalPartitionSetAdmission,
        row_map: LogicalPartitionSetAdmission,
    ) -> Self {
        Self { native, row_map }
    }

    /// Returns provider-native partition admission.
    pub const fn native(self) -> LogicalPartitionSetAdmission {
        self.native
    }

    /// Returns canonical row-map partition admission.
    pub const fn row_map(self) -> LogicalPartitionSetAdmission {
        self.row_map
    }
}

/// Bounded selection sink used directly by the complete archive verification pass.
#[derive(Debug)]
pub struct SecFundPendingLogicalRows {
    scope: SecFundPublicationScope,
    rows: Vec<SecBulkLogicalRow>,
    nport_holding_ids: BTreeSet<String>,
    accounted_native_bytes: u64,
    aborted: bool,
}

impl SecFundPendingLogicalRows {
    /// Begins one bounded filing/fund selection.
    pub const fn new(scope: SecFundPublicationScope) -> Self {
        Self {
            scope,
            rows: Vec::new(),
            nport_holding_ids: BTreeSet::new(),
            accounted_native_bytes: 0,
            aborted: false,
        }
    }

    /// Returns exact selected canonical scope.
    pub const fn scope(&self) -> &SecFundPublicationScope {
        &self.scope
    }

    fn accepts(&mut self, row: &SecBulkLogicalRow) -> Result<bool, SecBulkError> {
        let accession_matches = has_join(
            row,
            SecBulkJoinDomain::Accession,
            self.scope.accession().as_str(),
        );
        match &self.scope {
            SecFundPublicationScope::Nport { .. } => {
                if row.table().family() != SecBulkFamily::Nport
                    || fund_source_table(row.table()).is_none()
                {
                    return Ok(false);
                }
                match row.table() {
                    SecBulkTableKind::NportSubmission
                    | SecBulkTableKind::NportRegistrant
                    | SecBulkTableKind::NportFundReportedInfo => Ok(accession_matches),
                    SecBulkTableKind::NportFundReportedHolding => {
                        if !accession_matches {
                            return Ok(false);
                        }
                        let holding_id = join_value(row, SecBulkJoinDomain::Holding)
                            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
                        if !self.nport_holding_ids.insert(holding_id.to_owned()) {
                            return Err(SecBulkError::InvalidCanonicalMapping);
                        }
                        Ok(true)
                    }
                    // The closed manifest order places holdings before identifiers/supplements,
                    // and archive validation proves HOLDING_ID uniqueness and referential
                    // integrity across the complete quarter before this selection pass begins.
                    SecBulkTableKind::NportIdentifiers => {
                        Ok(join_value(row, SecBulkJoinDomain::Holding)
                            .is_some_and(|holding_id| self.nport_holding_ids.contains(holding_id)))
                    }
                    table if is_canonical_holding_supplement(table) => {
                        Ok(join_value(row, SecBulkJoinDomain::Holding)
                            .is_some_and(|holding_id| self.nport_holding_ids.contains(holding_id)))
                    }
                    _ => Ok(false),
                }
            }
            SecFundPublicationScope::Ncen { fund_id, .. } => {
                if row.table().family() != SecBulkFamily::Ncen
                    || fund_source_table(row.table()).is_none()
                {
                    return Ok(false);
                }
                Ok(match row.table() {
                    SecBulkTableKind::NcenSubmission | SecBulkTableKind::NcenRegistrant => {
                        accession_matches
                    }
                    SecBulkTableKind::NcenFundReportedInfo => {
                        accession_matches
                            && has_join(row, SecBulkJoinDomain::Fund, fund_id.as_str())
                    }
                    SecBulkTableKind::NcenEtf | SecBulkTableKind::NcenSecurityExchange => {
                        has_join(row, SecBulkJoinDomain::Fund, fund_id.as_str())
                    }
                    _ => false,
                })
            }
        }
    }
}

impl SecBulkPendingLogicalRowSink for SecFundPendingLogicalRows {
    fn stage(&mut self, row: SecBulkLogicalRow) -> Result<(), SecBulkError> {
        if self.aborted {
            return Err(SecBulkError::PublicationNotReady);
        }
        if self.accepts(&row)? {
            if self.rows.len() == MAX_FUND_SOURCE_ROWS {
                return Err(SecBulkError::QueryLimitExceeded);
            }
            let encoded_bytes = u64::try_from(serde_json::to_vec(row.provider_row())?.len())
                .map_err(|_| SecBulkError::QueryLimitExceeded)?;
            let accounted_bytes = encoded_bytes
                .checked_add(SELECTED_ROW_ACCOUNTING_OVERHEAD)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            self.accounted_native_bytes = self
                .accounted_native_bytes
                .checked_add(accounted_bytes)
                .filter(|bytes| *bytes <= MAX_SELECTED_NATIVE_BYTES)
                .ok_or(SecBulkError::QueryLimitExceeded)?;
            self.rows
                .try_reserve(1)
                .map_err(|_| SecBulkError::AllocationFailed)?;
            self.rows.push(row);
        }
        Ok(())
    }

    fn abort(&mut self) {
        self.rows.clear();
        self.nport_holding_ids.clear();
        self.accounted_native_bytes = 0;
        self.aborted = true;
    }
}

/// One typed canonical partition aligned to exact sealed native and row-map ranges.
#[derive(Debug)]
pub struct SecPreparedFundCanonicalPartition {
    partition_ordinal: u32,
    row_range: LogicalItemRange,
    records: Box<[FundEvidenceRecord]>,
    typed_input_digest: EvidenceDigest,
}

impl SecPreparedFundCanonicalPartition {
    /// Returns zero-based canonical partition ordinal.
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    /// Returns exact global row range shared with native and row-map partitions.
    pub const fn row_range(&self) -> LogicalItemRange {
        self.row_range
    }

    /// Returns bounded typed canonical records for data-layer Arrow/Parquet staging.
    pub fn records(&self) -> &[FundEvidenceRecord] {
        &self.records
    }

    /// Returns deterministic typed-record content identity for canonical staging verification.
    pub const fn typed_input_digest(&self) -> EvidenceDigest {
        self.typed_input_digest
    }
}

/// Noncloneable application/data-layer preparation; this is not publication authority.
#[derive(Debug)]
pub struct SecPreparedFundLogicalPublication {
    terminal: ProviderLogicalTerminalInput,
    scope: SecFundPublicationScope,
    objects: Box<[SealedLogicalObjectInput]>,
    partitions: Box<[SealedLogicalPartitionInput]>,
    canonical: Box<[SecPreparedFundCanonicalPartition]>,
    preparation_digest: EvidenceDigest,
}

impl SecPreparedFundLogicalPublication {
    /// Returns exact selected filing/fund scope.
    pub const fn scope(&self) -> &SecFundPublicationScope {
        &self.scope
    }

    /// Returns the terminal input that final common closure must consume unchanged.
    pub const fn terminal(&self) -> &ProviderLogicalTerminalInput {
        &self.terminal
    }

    /// Returns the exact sealed archive/readme object graph.
    pub fn objects(&self) -> &[SealedLogicalObjectInput] {
        &self.objects
    }

    /// Returns family-sorted aligned provider-native and row-map partitions.
    pub fn partitions(&self) -> &[SealedLogicalPartitionInput] {
        &self.partitions
    }

    /// Returns typed canonical batches awaiting the registered fund Arrow schema.
    pub fn canonical_partitions(&self) -> &[SecPreparedFundCanonicalPartition] {
        &self.canonical
    }

    /// Returns adapter evidence binding scope, terminal graph, and every prepared partition.
    pub const fn preparation_digest(&self) -> EvidenceDigest {
        self.preparation_digest
    }

    /// Consumes the only preparation into final application/data publication inputs.
    pub fn into_parts(
        self,
    ) -> (
        ProviderLogicalTerminalInput,
        SecFundPublicationScope,
        Box<[SealedLogicalObjectInput]>,
        Box<[SealedLogicalPartitionInput]>,
        Box<[SecPreparedFundCanonicalPartition]>,
        EvidenceDigest,
    ) {
        (
            self.terminal,
            self.scope,
            self.objects,
            self.partitions,
            self.canonical,
            self.preparation_digest,
        )
    }
}

#[derive(Debug)]
struct CanonicalCandidate {
    record: FundEvidenceRecord,
    row_indices: Vec<usize>,
    kind: CanonicalRecordKind,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalRecordKind {
    Report,
    ShareClass,
    PortfolioHolding,
}

#[derive(Serialize)]
struct NativeEnvelope<'a> {
    implementation_identifier: &'static str,
    implementation_tag: u8,
    scope: &'a SecFundPublicationScope,
    kind: CanonicalRecordKind,
    terminal_evidence: EvidenceDigest,
    native_generation: EvidenceDigest,
    rows: Vec<NativeEnvelopeRow<'a>>,
}

#[derive(Serialize)]
struct NativeEnvelopeRow<'a> {
    source_ordinal: u64,
    table: SecBulkTableKind,
    row_number: u64,
    row_evidence: EvidenceDigest,
    provider_row: &'a super::SecBulkNativeRow,
}

#[derive(Serialize)]
struct CanonicalRowMap<'a> {
    schema: &'static str,
    schema_version: u16,
    canonical_row_ordinal: u64,
    scope: &'a SecFundPublicationScope,
    kind: CanonicalRecordKind,
    terminal_evidence: EvidenceDigest,
    rows: Vec<CanonicalRowCoordinate>,
}

#[derive(Serialize)]
struct CanonicalRowCoordinate {
    logical_object_ordinal: u32,
    logical_component_ordinal: u32,
    logical_component_digest: EvidenceDigest,
    table: FundSourceTable,
    provider_row_number: u64,
    provider_row_evidence: EvidenceDigest,
    source_ordinal: u64,
}

impl SecBulkStagedLogicalPublication<SecFundPendingLogicalRows> {
    /// Maps one fully reverified selection and seals aligned native/row-map partitions.
    ///
    /// No canonical schema digest is accepted here. The registered data-schema owner consumes the
    /// returned typed batches, creates canonical expectations with its frozen Arrow fingerprint,
    /// and mints the final common binding in the application publication session.
    pub fn prepare_fund_logical_publication<A: SecFundIdentityAuthority>(
        self,
        authority: &mut A,
        ingested_at: Timestamp,
        admissions: SecFundPartitionAdmissions,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<SecPreparedFundLogicalPublication, SecBulkError> {
        let (selected, handoff) = self.into_parts();
        prepare_selected_fund(
            selected,
            handoff,
            authority,
            ingested_at,
            admissions,
            store,
            control,
        )
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "identity, clocks, physical admissions, and common store authority remain explicit"
)]
fn prepare_selected_fund<A: SecFundIdentityAuthority>(
    selected: SecFundPendingLogicalRows,
    handoff: SecBulkLogicalPublicationHandoff,
    authority: &mut A,
    ingested_at: Timestamp,
    admissions: SecFundPartitionAdmissions,
    store: &SealedResearchJournalStore,
    control: &dyn ResearchObjectControl,
) -> Result<SecPreparedFundLogicalPublication, SecBulkError> {
    if selected.aborted
        || selected.rows.is_empty()
        || selected.scope.family() != handoff.manifest().capture().selection().family()
        || handoff.manifest().capture().transport().body_received_at() > ingested_at
    {
        return Err(SecBulkError::PublicationNotReady);
    }
    let candidates = match selected.scope.family() {
        SecBulkFamily::Nport => map_nport(&selected, &handoff, authority, ingested_at)?,
        SecBulkFamily::Ncen => map_ncen(&selected, &handoff, authority, ingested_at)?,
    };
    if candidates.is_empty() {
        return Err(SecBulkError::PublicationNotReady);
    }
    let native_schema = digest_domain(SEC_FUND_NATIVE_SCHEMA_DOMAIN);
    let row_map_schema = digest_domain(SEC_FUND_ROW_MAP_SCHEMA_DOMAIN);
    let mut native = PendingLogicalPartitionSet::begin(
        LogicalPartitionFamily::ProviderNative,
        native_schema,
        admissions.native,
        0,
    )?;
    let mut row_map = PendingLogicalPartitionSet::begin(
        LogicalPartitionFamily::CanonicalRowMap,
        row_map_schema,
        admissions.row_map,
        0,
    )?;
    let mut canonical = Vec::new();
    let mut pending_records = Vec::new();
    let result = stage_candidates(
        &selected,
        &handoff,
        candidates,
        store,
        control,
        &mut native,
        &mut row_map,
        &mut canonical,
        &mut pending_records,
    );
    if let Err(error) = result {
        let _ = native.abort(store);
        let _ = row_map.abort(store);
        return Err(error);
    }
    finish_canonical_partition(&native, &mut pending_records, &mut canonical)?;
    let native = native.finish(store, control)?;
    let row_map = row_map.finish(store, control)?;
    validate_prepared_alignment(&canonical, &native, &row_map)?;
    let mut partitions = Vec::new();
    partitions
        .try_reserve_exact(native.partitions().len() + row_map.partitions().len())
        .map_err(|_| SecBulkError::AllocationFailed)?;
    partitions.extend(native.into_partitions());
    partitions.extend(row_map.into_partitions());

    let (_, report, objects, _, terminal_evidence) = handoff.into_shared_parts();
    let total_logical_object_bytes = objects.iter().try_fold(0_u64, |total, object| {
        total
            .checked_add(object.object().size_bytes())
            .ok_or(SecBulkError::QueryLimitExceeded)
    })?;
    let total_canonical_rows = canonical.iter().try_fold(0_u64, |total, partition| {
        total
            .checked_add(u64::from(partition.row_range.item_count().get()))
            .ok_or(SecBulkError::QueryLimitExceeded)
    })?;
    let source_revision_digest =
        sec_fund_source_revision(&selected.scope, report, terminal_evidence);
    let source_id = SourceId::try_from(SEC_EDGAR_SOURCE_ID)
        .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
    let terminal = ProviderLogicalTerminalInput {
        source_id,
        source_revision_digest,
        execution_attempt_digest: None,
        provider_terminal_evidence_digest: terminal_evidence,
        total_decoded_events: 0,
        total_canonical_rows,
        total_logical_object_bytes,
    };
    let preparation_digest = preparation_digest(
        &selected.scope,
        &terminal,
        &objects,
        &partitions,
        &canonical,
    );
    Ok(SecPreparedFundLogicalPublication {
        terminal,
        scope: selected.scope,
        objects,
        partitions: partitions.into_boxed_slice(),
        canonical: canonical.into_boxed_slice(),
        preparation_digest,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "aligned staging state remains explicit"
)]
fn stage_candidates(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    candidates: Vec<CanonicalCandidate>,
    store: &SealedResearchJournalStore,
    control: &dyn ResearchObjectControl,
    native: &mut PendingLogicalPartitionSet,
    row_map: &mut PendingLogicalPartitionSet,
    canonical: &mut Vec<SecPreparedFundCanonicalPartition>,
    pending_records: &mut Vec<FundEvidenceRecord>,
) -> Result<(), SecBulkError> {
    let native_generation = handoff.report().ordered_typed_rows_evidence();
    for (ordinal, candidate) in candidates.into_iter().enumerate() {
        let ordinal = u64::try_from(ordinal).map_err(|_| SecBulkError::QueryLimitExceeded)?;
        let native_bytes = native_envelope_bytes(
            selected,
            handoff,
            candidate.kind,
            native_generation,
            &candidate.row_indices,
        )?;
        let row_map_bytes = row_map_bytes(
            selected,
            handoff,
            candidate.kind,
            ordinal,
            &candidate.row_indices,
        )?;
        let native_len =
            u64::try_from(native_bytes.len()).map_err(|_| SecBulkError::QueryLimitExceeded)?;
        let row_map_len =
            u64::try_from(row_map_bytes.len()).map_err(|_| SecBulkError::QueryLimitExceeded)?;
        if !native.current_partition_accepts(native_len)?
            || !row_map.current_partition_accepts(row_map_len)?
        {
            finish_canonical_partition(native, pending_records, canonical)?;
            native.seal_current_partition(store, control)?;
            row_map.seal_current_partition(store, control)?;
        }
        let native_coordinate = native.stage_frame(
            store,
            control,
            ordinal,
            &native_bytes,
            sha256(&native_bytes),
        )?;
        let row_map_coordinate = row_map.stage_frame(
            store,
            control,
            ordinal,
            &row_map_bytes,
            sha256(&row_map_bytes),
        )?;
        validate_aligned_coordinates(native_coordinate, row_map_coordinate)?;
        pending_records
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        pending_records.push(candidate.record);
    }
    Ok(())
}

fn finish_canonical_partition(
    native: &PendingLogicalPartitionSet,
    pending_records: &mut Vec<FundEvidenceRecord>,
    canonical: &mut Vec<SecPreparedFundCanonicalPartition>,
) -> Result<(), SecBulkError> {
    let Some(row_range) = native.current_partition_range()? else {
        if pending_records.is_empty() {
            return Ok(());
        }
        return Err(SecBulkError::InvalidCanonicalMapping);
    };
    if usize::try_from(row_range.item_count().get()).ok() != Some(pending_records.len()) {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let partition_ordinal = native.current_partition_ordinal();
    let typed_input_digest = canonical_partition_digest(row_range, pending_records)?;
    let records = std::mem::take(pending_records).into_boxed_slice();
    canonical
        .try_reserve(1)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    canonical.push(SecPreparedFundCanonicalPartition {
        partition_ordinal,
        row_range,
        records,
        typed_input_digest,
    });
    Ok(())
}

fn validate_aligned_coordinates(
    native: StagedLogicalItemCoordinate,
    row_map: StagedLogicalItemCoordinate,
) -> Result<(), SecBulkError> {
    if native.family() != LogicalPartitionFamily::ProviderNative
        || row_map.family() != LogicalPartitionFamily::CanonicalRowMap
        || native.partition_ordinal() != row_map.partition_ordinal()
        || native.partition_item_ordinal() != row_map.partition_item_ordinal()
        || native.global_item_ordinal() != row_map.global_item_ordinal()
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    Ok(())
}

fn validate_prepared_alignment(
    canonical: &[SecPreparedFundCanonicalPartition],
    native: &market_squawk_sources::SealedLogicalPartitionSet,
    row_map: &market_squawk_sources::SealedLogicalPartitionSet,
) -> Result<(), SecBulkError> {
    if canonical.len() != native.partitions().len()
        || canonical.len() != row_map.partitions().len()
        || canonical
            .iter()
            .zip(native.partitions())
            .zip(row_map.partitions())
            .any(|((canonical, native), row_map)| {
                canonical.partition_ordinal != native.partition_ordinal()
                    || canonical.partition_ordinal != row_map.partition_ordinal()
                    || canonical.row_range != native.item_range()
                    || canonical.row_range != row_map.item_range()
            })
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    Ok(())
}

fn map_nport<A: SecFundIdentityAuthority>(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    authority: &mut A,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalCandidate>, SecBulkError> {
    let submission_index = unique_projected_index(selected, SecBulkTableKind::NportSubmission)?;
    let registrant_index = unique_projected_index(selected, SecBulkTableKind::NportRegistrant)?;
    let fund_index = unique_projected_index(selected, SecBulkTableKind::NportFundReportedInfo)?;
    let submission = nport_submission(&selected.rows[submission_index])?;
    let registrant = nport_registrant(&selected.rows[registrant_index])?;
    let fund = nport_fund(&selected.rows[fund_index])?;
    if submission.accession != *selected.scope.accession()
        || registrant.accession != submission.accession
        || fund.accession != submission.accession
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let fund_identity = authority.resolve_share_class(&fund.series_id, ingested_at)?;
    if fund_identity.provider_series_id() != &fund.series_id {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let common_indices = vec![submission_index, registrant_index, fund_index];
    let filing = nport_filing(
        handoff,
        submission,
        registrant,
        fund,
        fund_identity,
        ingested_at,
    )?;
    let common_lineage = source_lineage(selected, handoff, &common_indices)?;
    let report = FundReportEvidence::try_new(
        filing.clone(),
        FundReportAttributes::new(option_reported(submission.is_last_filing), not_applicable()),
        common_lineage.clone(),
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
    let share = FundShareClassEvidence::try_new(
        filing.clone(),
        FundShareClassAttributes::new(
            missing_source(),
            exact_decimal_value(fund.total_assets.as_ref()),
            exact_decimal_value(fund.total_liabilities.as_ref()),
            exact_decimal_value(fund.net_assets.as_ref()),
            not_applicable(),
            not_applicable(),
            not_applicable(),
            not_applicable(),
            FundEtfMechanics::new(
                not_applicable(),
                not_applicable(),
                not_applicable(),
                not_applicable(),
            ),
        ),
        Vec::new(),
        common_lineage,
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve(2)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    let mut holding_indices = BTreeMap::<SourceIdentifier, usize>::new();
    let mut identifiers = BTreeMap::<SourceIdentifier, Vec<usize>>::new();
    let mut supplements = BTreeMap::<SourceIdentifier, Vec<usize>>::new();
    for (index, row) in selected.rows.iter().enumerate() {
        match row.table() {
            SecBulkTableKind::NportFundReportedHolding => {
                let holding = nport_holding(row)?;
                if holding.accession != submission.accession
                    || holding_indices
                        .insert(holding.holding_id.clone(), index)
                        .is_some()
                {
                    return Err(SecBulkError::InvalidCanonicalMapping);
                }
            }
            SecBulkTableKind::NportIdentifiers => {
                let identifier = nport_identifier(row)?;
                identifiers
                    .entry(identifier.holding_id.clone())
                    .or_default()
                    .push(index);
            }
            table if is_canonical_holding_supplement(table) => {
                let holding_id = join_value(row, SecBulkJoinDomain::Holding)
                    .ok_or(SecBulkError::InvalidCanonicalMapping)?;
                supplements
                    .entry(SourceIdentifier::try_from(holding_id)?)
                    .or_default()
                    .push(index);
            }
            _ => {}
        }
    }
    if holding_indices.len() != selected.nport_holding_ids.len()
        || holding_indices
            .keys()
            .any(|holding_id| !selected.nport_holding_ids.contains(holding_id.as_str()))
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    for (holding_id, holding_index) in holding_indices {
        let holding = nport_holding(&selected.rows[holding_index])?;
        let identifier_indices = identifiers.remove(&holding_id).unwrap_or_default();
        let identifier_rows = identifier_indices
            .iter()
            .map(|index| nport_identifier(&selected.rows[*index]).cloned())
            .collect::<Result<Vec<_>, _>>()?;
        let held_security =
            authority.resolve_holding_security(holding, &identifier_rows, ingested_at)?;
        validate_holding_identity(&held_security, holding, &identifier_rows)?;
        let mut row_indices = common_indices.clone();
        row_indices.push(holding_index);
        row_indices.extend(identifier_indices);
        row_indices.extend(supplements.remove(&holding_id).unwrap_or_default());
        sort_row_indices(selected, &mut row_indices)?;
        let lineage = source_lineage(selected, handoff, &row_indices)?;
        let topology = handoff
            .nport_holding_supplements()
            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
        let mut completeness_rows = row_indices
            .iter()
            .map(|index| &selected.rows[*index])
            .filter(|row| {
                row.table() == SecBulkTableKind::NportFundReportedHolding
                    || is_canonical_holding_supplement(row.table())
            })
            .collect::<Vec<_>>();
        completeness_rows.sort_by_key(|row| row.lineage().source_ordinal());
        let completeness = topology.complete_holding(
            submission.accession.clone(),
            holding_id.clone(),
            &completeness_rows,
        )?;
        let canonical_supplements = canonical_supplements(&completeness, &lineage)?;
        let associations = holding_associations(holding, &identifier_rows);
        let attributes = holding_attributes(holding);
        let canonical = FundPortfolioHoldingEvidence::try_new(
            filing.clone(),
            holding_id,
            held_security,
            associations,
            attributes,
            canonical_supplements,
            lineage,
        )
        .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
        candidates
            .try_reserve(1)
            .map_err(|_| SecBulkError::AllocationFailed)?;
        candidates.push(CanonicalCandidate {
            record: FundEvidenceRecord::PortfolioHolding(Box::new(canonical)),
            row_indices,
            kind: CanonicalRecordKind::PortfolioHolding,
        });
    }
    if !identifiers.is_empty() || !supplements.is_empty() {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    candidates
        .try_reserve(2)
        .map_err(|_| SecBulkError::AllocationFailed)?;
    candidates.push(CanonicalCandidate {
        record: FundEvidenceRecord::Report(Box::new(report)),
        row_indices: common_indices.clone(),
        kind: CanonicalRecordKind::Report,
    });
    candidates.push(CanonicalCandidate {
        record: FundEvidenceRecord::ShareClass(Box::new(share)),
        row_indices: common_indices,
        kind: CanonicalRecordKind::ShareClass,
    });
    Ok(candidates)
}

fn map_ncen<A: SecFundIdentityAuthority>(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    authority: &mut A,
    ingested_at: Timestamp,
) -> Result<Vec<CanonicalCandidate>, SecBulkError> {
    let submission_index = unique_projected_index(selected, SecBulkTableKind::NcenSubmission)?;
    let registrant_index = unique_projected_index(selected, SecBulkTableKind::NcenRegistrant)?;
    let fund_index = unique_projected_index(selected, SecBulkTableKind::NcenFundReportedInfo)?;
    let submission = ncen_submission(&selected.rows[submission_index])?;
    let registrant = ncen_registrant(&selected.rows[registrant_index])?;
    let fund = ncen_fund(&selected.rows[fund_index])?;
    let series_id = fund
        .series_id
        .as_ref()
        .ok_or(SecBulkError::UnresolvedIdentity)?;
    if submission.accession != *selected.scope.accession()
        || registrant.accession != submission.accession
        || fund.accession != submission.accession
        || selected.scope.fund_id() != Some(&fund.fund_id)
    {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let fund_identity = authority.resolve_share_class(series_id, ingested_at)?;
    if fund_identity.provider_series_id() != series_id {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let filing = ncen_filing(
        handoff,
        submission,
        registrant,
        fund,
        fund_identity,
        ingested_at,
    )?;
    let common_indices = vec![submission_index, registrant_index, fund_index];
    let report = FundReportEvidence::try_new(
        filing.clone(),
        FundReportAttributes::new(
            not_applicable(),
            option_reported(submission.report_period_less_than_twelve_months),
        ),
        source_lineage(selected, handoff, &common_indices)?,
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;

    let etf_indices = projected_indices(selected, SecBulkTableKind::NcenEtf);
    if etf_indices.len() > 1 {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let etf = etf_indices
        .first()
        .map(|index| ncen_etf(&selected.rows[*index]))
        .transpose()?;
    let exchange_indices = projected_indices(selected, SecBulkTableKind::NcenSecurityExchange);
    if exchange_indices.len() > market_squawk_domain::MAX_FUND_EXCHANGE_ASSOCIATIONS {
        return Err(SecBulkError::QueryLimitExceeded);
    }
    let mut share_indices = common_indices.clone();
    share_indices.extend(etf_indices.iter().copied());
    share_indices.extend(exchange_indices.iter().copied());
    sort_row_indices(selected, &mut share_indices)?;
    let share_lineage = source_lineage(selected, handoff, &share_indices)?;
    let exchanges = exchange_indices
        .iter()
        .map(|index| {
            let row = &selected.rows[*index];
            let row_evidence = source_row_evidence(selected, handoff, *index)?;
            FundExchangeAssociation::try_new(
                source_identifier_field(row, "FUND_EXCHANGE")?,
                source_identifier_field(row, "FUND_TICKER_SYMBOL")?,
                row_evidence,
            )
            .map_err(|_| SecBulkError::InvalidCanonicalMapping)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let etf_mechanics = match etf {
        Some(etf) => FundEtfMechanics::new(
            option_reported(etf.collateral_required),
            exact_decimal_value(etf.shares_per_creation_unit.as_ref()),
            exact_decimal_value(etf.redeemed_shares_per_creation_unit.as_ref()),
            option_reported(etf.is_in_kind_etf),
        ),
        None => FundEtfMechanics::new(
            missing_source(),
            missing_source(),
            missing_source(),
            missing_source(),
        ),
    };
    let share = FundShareClassEvidence::try_new(
        filing,
        FundShareClassAttributes::new(
            missing_source(),
            not_applicable(),
            not_applicable(),
            not_applicable(),
            exact_decimal_value(fund.monthly_average_net_assets.as_ref()),
            exact_decimal_value(fund.daily_average_net_assets.as_ref()),
            option_reported(fund.is_etf),
            option_reported(fund.is_index),
            etf_mechanics,
        ),
        exchanges,
        share_lineage,
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
    Ok(vec![
        CanonicalCandidate {
            record: FundEvidenceRecord::Report(Box::new(report)),
            row_indices: common_indices,
            kind: CanonicalRecordKind::Report,
        },
        CanonicalCandidate {
            record: FundEvidenceRecord::ShareClass(Box::new(share)),
            row_indices: share_indices,
            kind: CanonicalRecordKind::ShareClass,
        },
    ])
}

fn nport_filing(
    handoff: &SecBulkLogicalPublicationHandoff,
    submission: &SecNportSubmissionRow,
    registrant: &SecNportRegistrantRow,
    fund: &SecNportFundRow,
    identity: FundShareClassIdentity,
    ingested_at: Timestamp,
) -> Result<FundFilingIdentity, SecBulkError> {
    let chronology = filing_chronology(
        handoff,
        submission.report_ending_period,
        submission.report_date,
        submission.filing_date,
        ingested_at,
    )?;
    filing_identity(
        handoff,
        FundSourceFamily::Nport,
        registrant.cik.clone(),
        submission.accession.clone(),
        submission.form.clone(),
        not_applicable(),
        identity,
        chronology,
    )
    .and_then(|filing| {
        if filing.fund().provider_series_id() != &fund.series_id {
            Err(SecBulkError::InvalidCanonicalMapping)
        } else {
            Ok(filing)
        }
    })
}

fn ncen_filing(
    handoff: &SecBulkLogicalPublicationHandoff,
    submission: &SecNcenSubmissionRow,
    registrant: &SecNcenRegistrantRow,
    fund: &SecNcenFundRow,
    identity: FundShareClassIdentity,
    ingested_at: Timestamp,
) -> Result<FundFilingIdentity, SecBulkError> {
    if registrant.cik != submission.cik {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    let chronology = filing_chronology(
        handoff,
        submission.report_ending_period,
        None,
        submission.filing_date,
        ingested_at,
    )?;
    filing_identity(
        handoff,
        FundSourceFamily::Ncen,
        submission.cik.clone(),
        submission.accession.clone(),
        submission.form.clone(),
        FundReportedValue::Reported(fund.fund_id.clone()),
        identity,
        chronology,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "filing evidence remains non-collapsible"
)]
fn filing_identity(
    handoff: &SecBulkLogicalPublicationHandoff,
    family: FundSourceFamily,
    cik: SourceIdentifier,
    accession: SourceIdentifier,
    form: SourceIdentifier,
    fund_id: FundReportedValue<SourceIdentifier>,
    identity: FundShareClassIdentity,
    chronology: FundFilingChronology,
) -> Result<FundFilingIdentity, SecBulkError> {
    let amendment = if form.as_str().ends_with("/A") {
        FundAmendmentState::Amendment
    } else {
        FundAmendmentState::Original
    };
    let predecessor = match amendment {
        FundAmendmentState::Original => FundRevisionLink::NotApplicable,
        FundAmendmentState::Amendment => FundRevisionLink::Unresolved,
    };
    let revision = FundRevisionEvidence::try_new(
        amendment,
        FundRevisionStatus::Unavailable,
        predecessor,
        FundRevisionLink::NotObserved,
        Vec::new(),
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)?;
    FundFilingIdentity::try_new(
        SchemaVersion::CURRENT,
        SourceId::try_from(SEC_EDGAR_SOURCE_ID)
            .map_err(|_| SecBulkError::InvalidCanonicalMapping)?,
        family,
        cik,
        accession,
        form,
        fund_id,
        identity,
        chronology,
        revision,
        release_coverage(handoff)?,
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)
}

fn filing_chronology(
    handoff: &SecBulkLogicalPublicationHandoff,
    report_period_end: Option<NaiveDate>,
    report_date: Option<NaiveDate>,
    filed_date: Option<NaiveDate>,
    ingested_at: Timestamp,
) -> Result<FundFilingChronology, SecBulkError> {
    let capture = handoff.manifest().capture();
    let received_at = capture.transport().body_received_at();
    let provider_published = capture.transport().last_modified_at();
    let availability = provider_published.map_or_else(
        || AvailabilityEvidence::local_first_observed(capture.first_observed_at()),
        |published| AvailabilityEvidence::evidenced(published, capture.locator().clone()),
    );
    FundFilingChronology::try_new(
        calendar_value(report_period_end)?,
        calendar_value(report_date)?,
        calendar_value(filed_date)?,
        missing_source(),
        option_reported(provider_published),
        availability,
        received_at,
        ingested_at,
    )
    .map_err(|_| SecBulkError::InvalidChronology)
}

fn release_coverage(
    handoff: &SecBulkLogicalPublicationHandoff,
) -> Result<FundReleaseCoverage, SecBulkError> {
    match handoff.manifest().capture().selection().coverage() {
        SecBulkCoverage::DerivedAsFiledIncludingAmendments => Ok(FundReleaseCoverage::Complete),
        SecBulkCoverage::AcceptedSchemaExcluded { schema } => {
            Ok(FundReleaseCoverage::AcceptedSchemaExclusion {
                accepted_schema: SourceIdentifier::try_from("sec-derived-quarterly-bulk")?,
                excluded_schema: schema.version().clone(),
            })
        }
    }
}

fn source_lineage(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    indices: &[usize],
) -> Result<FundSourceLineage, SecBulkError> {
    let family = match selected.scope.family() {
        SecBulkFamily::Nport => FundSourceFamily::Nport,
        SecBulkFamily::Ncen => FundSourceFamily::Ncen,
    };
    let rows = indices
        .iter()
        .map(|index| source_row_evidence(selected, handoff, *index))
        .collect::<Result<Vec<_>, _>>()?;
    FundSourceLineage::try_new(family, rows).map_err(|_| SecBulkError::InvalidCanonicalMapping)
}

fn source_row_evidence(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    index: usize,
) -> Result<FundSourceRowEvidence, SecBulkError> {
    let row = selected
        .rows
        .get(index)
        .ok_or(SecBulkError::InvalidCanonicalMapping)?;
    let table = fund_source_table(row.table()).ok_or(SecBulkError::InvalidCanonicalMapping)?;
    let (component_ordinal, component_digest) = table_component(handoff, row.table())?;
    FundSourceRowEvidence::try_new(
        table,
        component_ordinal,
        component_digest,
        NonZeroU64::new(row.row_number()).ok_or(SecBulkError::InvalidCanonicalMapping)?,
        row.row_evidence(),
        handoff.report().ordered_typed_rows_evidence(),
        handoff.manifest().evidence(),
        handoff.terminal_evidence(),
    )
    .map_err(|_| SecBulkError::InvalidCanonicalMapping)
}

fn table_component(
    handoff: &SecBulkLogicalPublicationHandoff,
    table: SecBulkTableKind,
) -> Result<(u32, EvidenceDigest), SecBulkError> {
    handoff
        .manifest()
        .tables()
        .iter()
        .enumerate()
        .find(|(_, receipt)| receipt.name().as_str() == table.member_name())
        .map(|(ordinal, receipt)| {
            Ok::<(u32, EvidenceDigest), SecBulkError>((
                u32::try_from(ordinal).map_err(|_| SecBulkError::InvalidCanonicalMapping)?,
                receipt.evidence(),
            ))
        })
        .transpose()?
        .ok_or(SecBulkError::InvalidCanonicalMapping)
}

fn canonical_supplements(
    completeness: &super::SecNportHoldingSupplementCompleteness,
    lineage: &FundSourceLineage,
) -> Result<Vec<FundHoldingSupplementEvidence>, SecBulkError> {
    completeness
        .tables()
        .iter()
        .map(|state| {
            let table = fund_source_table(state.table())
                .filter(|table| table.is_holding_supplement())
                .ok_or(SecBulkError::InvalidCanonicalMapping)?;
            let disposition = match state.state() {
                super::SecNportHoldingSupplementState::ReportedRows => {
                    FundSupplementDisposition::Reported
                }
                super::SecNportHoldingSupplementState::NoDerivedRowForHolding => {
                    FundSupplementDisposition::NoRowForHolding
                }
                super::SecNportHoldingSupplementState::TablePresentEmpty => {
                    FundSupplementDisposition::PresentEmpty
                }
                super::SecNportHoldingSupplementState::TableDeclaredAbsent => {
                    FundSupplementDisposition::DeclaredAbsent
                }
            };
            let range = if disposition == FundSupplementDisposition::Reported {
                let matching = lineage
                    .rows()
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| row.table() == table)
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                let start = *matching
                    .first()
                    .ok_or(SecBulkError::InvalidCanonicalMapping)?;
                if matching
                    .iter()
                    .enumerate()
                    .any(|(offset, index)| *index != start + offset)
                    || matching.len()
                        != usize::try_from(state.joined_rows())
                            .map_err(|_| SecBulkError::InvalidCanonicalMapping)?
                {
                    return Err(SecBulkError::InvalidCanonicalMapping);
                }
                Some(FundLineageRowRange::new(
                    u32::try_from(start).map_err(|_| SecBulkError::InvalidCanonicalMapping)?,
                    NonZeroU32::new(
                        u32::try_from(matching.len())
                            .map_err(|_| SecBulkError::InvalidCanonicalMapping)?,
                    )
                    .ok_or(SecBulkError::InvalidCanonicalMapping)?,
                ))
            } else {
                None
            };
            FundHoldingSupplementEvidence::try_new(
                table,
                disposition,
                state.joined_rows_evidence(),
                range,
            )
            .map_err(|_| SecBulkError::InvalidCanonicalMapping)
        })
        .collect()
}

fn holding_associations(
    holding: &SecNportHoldingRow,
    identifiers: &[SecNportIdentifierRow],
) -> FundHoldingAssociations {
    let isins = identifiers
        .iter()
        .filter_map(|row| row.isin.as_ref())
        .collect::<Vec<_>>();
    let tickers = identifiers
        .iter()
        .filter_map(|row| row.ticker.as_ref())
        .collect::<Vec<_>>();
    FundHoldingAssociations::new(
        source_text_value(holding.issuer_name.as_deref()),
        option_reported(holding.issuer_lei.clone()),
        source_text_value(holding.issuer_title.as_deref()),
        holding.cusip.as_ref().map_or_else(
            || missing_source(),
            |value| match value.as_str().try_into() {
                Ok(value) => FundReportedValue::Reported(value),
                Err(_) => FundReportedValue::Missing(FundMissingState::Invalid),
            },
        ),
        one_isin(&isins),
        one_identifier(&tickers),
    )
}

fn holding_attributes(holding: &SecNportHoldingRow) -> FundPortfolioHoldingAttributes {
    let quantity = holding.balance.as_ref().map_or_else(
        || missing_source(),
        |amount| {
            let amount = FundReportedDecimal::try_from_str(amount.as_str())
                .map_err(|_| FundMissingState::Invalid);
            let unit = holding.unit.as_ref().map_or(Ok(None), |unit| {
                holding_unit(unit, holding.currency.as_ref())
            });
            match (amount, unit) {
                (Ok(amount), Ok(Some(unit))) => {
                    FundReportedValue::Reported(FundHoldingQuantity::new(amount, unit))
                }
                (Err(state), _) | (_, Err(state)) => FundReportedValue::Missing(state),
                (_, Ok(None)) => missing_source(),
            }
        },
    );
    let value = match (&holding.value, &holding.currency) {
        (Some(amount), Some(currency)) => {
            match (
                FundReportedDecimal::try_from_str(amount.as_str()),
                Currency::try_from(currency.as_str()),
            ) {
                (Ok(amount), Ok(currency)) => {
                    FundReportedValue::Reported(FundCurrencyAmount::new(amount, currency))
                }
                _ => FundReportedValue::Missing(FundMissingState::Invalid),
            }
        }
        (None, None) => missing_source(),
        _ => FundReportedValue::Missing(FundMissingState::Invalid),
    };
    FundPortfolioHoldingAttributes::new(
        quantity,
        value,
        exact_decimal_value(holding.exchange_rate.as_ref()),
        exact_decimal_value(holding.percentage.as_ref()),
        option_reported(holding.payoff_profile.clone()),
        option_reported(holding.asset_category.clone()),
        source_text_value(holding.other_asset.as_deref()),
        option_reported(holding.issuer_type.clone()),
        source_text_value(holding.other_issuer.as_deref()),
        option_reported(holding.investment_country.clone()),
        option_reported(holding.restricted_security),
        option_reported(holding.fair_value_level.clone()),
        option_reported(holding.derivative_category.clone()),
    )
}

fn validate_holding_identity(
    identity: &FundHoldingSecurityIdentity,
    holding: &SecNportHoldingRow,
    identifiers: &[SecNportIdentifierRow],
) -> Result<(), SecBulkError> {
    let Some(identifier) = identity.authoritative_identifier() else {
        return Ok(());
    };
    let found = match identifier {
        market_squawk_domain::FundSecurityIdentifier::Cusip(cusip) => holding
            .cusip
            .as_ref()
            .is_some_and(|value| value.as_str() == cusip.as_str()),
        market_squawk_domain::FundSecurityIdentifier::Isin(isin) => identifiers.iter().any(|row| {
            row.isin
                .as_ref()
                .is_some_and(|value| value.as_str() == isin.as_str())
        }),
    };
    if found {
        Ok(())
    } else {
        Err(SecBulkError::InvalidCanonicalMapping)
    }
}

fn holding_unit(
    unit: &SourceIdentifier,
    currency: Option<&SourceIdentifier>,
) -> Result<Option<FundHoldingUnit>, FundMissingState> {
    let normalized = unit.as_str().to_ascii_lowercase();
    match normalized.as_str() {
        "shares" | "share" | "units" => Ok(Some(FundHoldingUnit::Shares)),
        "principal" | "par" => Ok(Some(FundHoldingUnit::Principal)),
        "contracts" | "contract" => Ok(Some(FundHoldingUnit::Contracts)),
        "currency" => currency.map_or(Err(FundMissingState::SourceAbsent), |currency| {
            Currency::try_from(currency.as_str())
                .map(FundHoldingUnit::Currency)
                .map(Some)
                .map_err(|_| FundMissingState::Invalid)
        }),
        _ => Ok(Some(FundHoldingUnit::Other(unit.clone()))),
    }
}

fn native_envelope_bytes(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    kind: CanonicalRecordKind,
    native_generation: EvidenceDigest,
    indices: &[usize],
) -> Result<Vec<u8>, SecBulkError> {
    let rows = indices
        .iter()
        .map(|index| {
            let row = selected
                .rows
                .get(*index)
                .ok_or(SecBulkError::InvalidCanonicalMapping)?;
            Ok(NativeEnvelopeRow {
                source_ordinal: row.lineage().source_ordinal(),
                table: row.table(),
                row_number: row.row_number(),
                row_evidence: row.row_evidence(),
                provider_row: row.provider_row(),
            })
        })
        .collect::<Result<Vec<_>, SecBulkError>>()?;
    serde_json::to_vec(&NativeEnvelope {
        implementation_identifier: native_lineage_coordinates(&selected.scope).0,
        implementation_tag: native_lineage_coordinates(&selected.scope).1,
        scope: &selected.scope,
        kind,
        terminal_evidence: handoff.terminal_evidence(),
        native_generation,
        rows,
    })
    .map_err(Into::into)
}

fn native_lineage_coordinates(scope: &SecFundPublicationScope) -> (&'static str, u8) {
    match scope {
        SecFundPublicationScope::Nport { .. } => (
            "market-squawk/sec-nport-bulk/provider-native-lineage/v1",
            12,
        ),
        SecFundPublicationScope::Ncen { .. } => {
            ("market-squawk/sec-ncen-bulk/provider-native-lineage/v1", 13)
        }
    }
}

fn row_map_bytes(
    selected: &SecFundPendingLogicalRows,
    handoff: &SecBulkLogicalPublicationHandoff,
    kind: CanonicalRecordKind,
    canonical_row_ordinal: u64,
    indices: &[usize],
) -> Result<Vec<u8>, SecBulkError> {
    let rows = indices
        .iter()
        .map(|index| {
            let row = selected
                .rows
                .get(*index)
                .ok_or(SecBulkError::InvalidCanonicalMapping)?;
            let (component_ordinal, component_digest) = table_component(handoff, row.table())?;
            Ok(CanonicalRowCoordinate {
                logical_object_ordinal: row.lineage().archive_object_ordinal(),
                logical_component_ordinal: component_ordinal,
                logical_component_digest: component_digest,
                table: fund_source_table(row.table())
                    .ok_or(SecBulkError::InvalidCanonicalMapping)?,
                provider_row_number: row.row_number(),
                provider_row_evidence: row.row_evidence(),
                source_ordinal: row.lineage().source_ordinal(),
            })
        })
        .collect::<Result<Vec<_>, SecBulkError>>()?;
    serde_json::to_vec(&CanonicalRowMap {
        schema: FUND_HOLDINGS_SCHEMA_NAME,
        schema_version: FUND_HOLDINGS_SCHEMA_VERSION,
        canonical_row_ordinal,
        scope: &selected.scope,
        kind,
        terminal_evidence: handoff.terminal_evidence(),
        rows,
    })
    .map_err(Into::into)
}

fn canonical_partition_digest(
    range: LogicalItemRange,
    records: &[FundEvidenceRecord],
) -> Result<EvidenceDigest, SecBulkError> {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_CANONICAL_PARTITION_DOMAIN);
    digest.update(range.first_ordinal().to_be_bytes());
    digest.update(range.item_count().get().to_be_bytes());
    for (offset, record) in records.iter().enumerate() {
        let ordinal = range
            .first_ordinal()
            .checked_add(u64::try_from(offset).map_err(|_| SecBulkError::QueryLimitExceeded)?)
            .ok_or(SecBulkError::QueryLimitExceeded)?;
        let bytes = serde_json::to_vec(record)?;
        digest.update(ordinal.to_be_bytes());
        digest.update(
            u64::try_from(bytes.len())
                .map_err(|_| SecBulkError::QueryLimitExceeded)?
                .to_be_bytes(),
        );
        digest.update(Sha256::digest(bytes));
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn sec_fund_source_revision(
    scope: &SecFundPublicationScope,
    report: super::SecBulkScanReport,
    terminal: EvidenceDigest,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_SOURCE_REVISION_DOMAIN);
    hash_scope(&mut digest, scope);
    digest.update(report.manifest_evidence().bytes());
    digest.update(report.source_rows().to_be_bytes());
    digest.update(report.emitted_typed_rows().to_be_bytes());
    digest.update(report.ordered_typed_rows_evidence().bytes());
    digest.update(terminal.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn preparation_digest(
    scope: &SecFundPublicationScope,
    terminal: &ProviderLogicalTerminalInput,
    objects: &[SealedLogicalObjectInput],
    partitions: &[SealedLogicalPartitionInput],
    canonical: &[SecPreparedFundCanonicalPartition],
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(SEC_FUND_PREPARATION_DOMAIN);
    hash_scope(&mut digest, scope);
    digest.update(terminal.source_revision_digest.bytes());
    digest.update(terminal.provider_terminal_evidence_digest.bytes());
    digest.update(terminal.total_canonical_rows.to_be_bytes());
    digest.update(terminal.total_logical_object_bytes.to_be_bytes());
    for object in objects {
        digest.update(object.ordinal().to_be_bytes());
        digest.update(object.semantic_identity().bytes());
        digest.update(object.object().content_digest().bytes());
        digest.update(object.object().size_bytes().to_be_bytes());
    }
    for partition in partitions {
        digest.update([match partition.family() {
            LogicalPartitionFamily::DecodedEvent => 1,
            LogicalPartitionFamily::ProviderNative => 2,
            LogicalPartitionFamily::CanonicalRowMap => 3,
            LogicalPartitionFamily::ResolverAssertion => 4,
            LogicalPartitionFamily::ResolverOutcome => 5,
            LogicalPartitionFamily::ResolverConflict => 6,
        }]);
        digest.update(partition.partition_ordinal().to_be_bytes());
        digest.update(partition.semantic_digest().bytes());
    }
    for partition in canonical {
        digest.update(partition.partition_ordinal.to_be_bytes());
        digest.update(partition.row_range.first_ordinal().to_be_bytes());
        digest.update(partition.row_range.item_count().get().to_be_bytes());
        digest.update(partition.typed_input_digest.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_scope(digest: &mut Sha256, scope: &SecFundPublicationScope) {
    match scope {
        SecFundPublicationScope::Nport { accession } => {
            digest.update([1]);
            hash_bytes(digest, accession.as_str().as_bytes());
        }
        SecFundPublicationScope::Ncen { accession, fund_id } => {
            digest.update([2]);
            hash_bytes(digest, accession.as_str().as_bytes());
            hash_bytes(digest, fund_id.as_str().as_bytes());
        }
    }
}

fn sort_row_indices(
    selected: &SecFundPendingLogicalRows,
    indices: &mut [usize],
) -> Result<(), SecBulkError> {
    indices.sort_by_key(|index| {
        let row = &selected.rows[*index];
        (
            fund_source_table(row.table()),
            row.row_number(),
            row.lineage().source_ordinal(),
        )
    });
    if indices.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(SecBulkError::InvalidCanonicalMapping);
    }
    Ok(())
}

fn projected_indices(selected: &SecFundPendingLogicalRows, table: SecBulkTableKind) -> Vec<usize> {
    selected
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.table() == table)
        .map(|(index, _)| index)
        .collect()
}

fn unique_projected_index(
    selected: &SecFundPendingLogicalRows,
    table: SecBulkTableKind,
) -> Result<usize, SecBulkError> {
    let indices = projected_indices(selected, table);
    if indices.len() == 1 {
        Ok(indices[0])
    } else {
        Err(SecBulkError::InvalidCanonicalMapping)
    }
}

fn nport_submission(row: &SecBulkLogicalRow) -> Result<&SecNportSubmissionRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NportSubmission(
            row,
        )) => Ok(row),
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn nport_registrant(row: &SecBulkLogicalRow) -> Result<&SecNportRegistrantRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NportRegistrant(
            row,
        )) => Ok(row),
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn nport_fund(row: &SecBulkLogicalRow) -> Result<&SecNportFundRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NportFund(row)) => {
            Ok(row)
        }
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn nport_holding(row: &SecBulkLogicalRow) -> Result<&SecNportHoldingRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NportHolding(row)) => {
            Ok(row)
        }
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn nport_identifier(row: &SecBulkLogicalRow) -> Result<&SecNportIdentifierRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NportIdentifier(
            row,
        )) => Ok(row),
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn ncen_submission(row: &SecBulkLogicalRow) -> Result<&SecNcenSubmissionRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NcenSubmission(row)) => {
            Ok(row)
        }
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn ncen_registrant(row: &SecBulkLogicalRow) -> Result<&SecNcenRegistrantRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NcenRegistrant(row)) => {
            Ok(row)
        }
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn ncen_fund(row: &SecBulkLogicalRow) -> Result<&SecNcenFundRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NcenFund(row)) => {
            Ok(row)
        }
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn ncen_etf(row: &SecBulkLogicalRow) -> Result<&SecNcenEtfRow, SecBulkError> {
    match row.projection_disposition() {
        SecBulkProjectionDisposition::Projected(SecBulkProviderProjection::NcenEtf(row)) => Ok(row),
        _ => Err(SecBulkError::InvalidCanonicalMapping),
    }
}

fn fund_source_table(table: SecBulkTableKind) -> Option<FundSourceTable> {
    Some(match table {
        SecBulkTableKind::NportSubmission => FundSourceTable::NportSubmission,
        SecBulkTableKind::NportRegistrant => FundSourceTable::NportRegistrant,
        SecBulkTableKind::NportFundReportedInfo => FundSourceTable::NportFund,
        SecBulkTableKind::NportFundReportedHolding => FundSourceTable::NportHolding,
        SecBulkTableKind::NportIdentifiers => FundSourceTable::NportIdentifiers,
        SecBulkTableKind::NportDebtSecurity => FundSourceTable::NportDebtSecurity,
        SecBulkTableKind::NportDebtSecurityReferenceInstrument => {
            FundSourceTable::NportDebtSecurityReferenceInstrument
        }
        SecBulkTableKind::NportConvertibleSecurityCurrency => {
            FundSourceTable::NportConvertibleSecurityCurrency
        }
        SecBulkTableKind::NportRepurchaseAgreement => FundSourceTable::NportRepurchaseAgreement,
        SecBulkTableKind::NportRepurchaseCounterparty => {
            FundSourceTable::NportRepurchaseCounterparty
        }
        SecBulkTableKind::NportRepurchaseCollateral => FundSourceTable::NportRepurchaseCollateral,
        SecBulkTableKind::NportDerivativeCounterparty => {
            FundSourceTable::NportDerivativeCounterparty
        }
        SecBulkTableKind::NportSwaptionOptionWarrantDerivative => {
            FundSourceTable::NportSwaptionOptionWarrantDerivative
        }
        SecBulkTableKind::NportDescriptionReferenceIndexBasket => {
            FundSourceTable::NportDescriptionReferenceIndexBasket
        }
        SecBulkTableKind::NportDescriptionReferenceIndexComponent => {
            FundSourceTable::NportDescriptionReferenceIndexComponent
        }
        SecBulkTableKind::NportDescriptionReferenceOther => {
            FundSourceTable::NportDescriptionReferenceOther
        }
        SecBulkTableKind::NportFutureForwardNonforeignCurrencyContract => {
            FundSourceTable::NportFutureForwardNonforeignCurrencyContract
        }
        SecBulkTableKind::NportForwardForeignCurrencyContractSwap => {
            FundSourceTable::NportForwardForeignCurrencyContractSwap
        }
        SecBulkTableKind::NportNonforeignExchangeSwap => {
            FundSourceTable::NportNonforeignExchangeSwap
        }
        SecBulkTableKind::NportFloatingRateResetTenor => {
            FundSourceTable::NportFloatingRateResetTenor
        }
        SecBulkTableKind::NportOtherDerivative => FundSourceTable::NportOtherDerivative,
        SecBulkTableKind::NportOtherDerivativeNotionalAmount => {
            FundSourceTable::NportOtherDerivativeNotionalAmount
        }
        SecBulkTableKind::NportSecuritiesLending => FundSourceTable::NportSecuritiesLending,
        SecBulkTableKind::NportExplanatoryNote => FundSourceTable::NportExplanatoryNote,
        SecBulkTableKind::NcenSubmission => FundSourceTable::NcenSubmission,
        SecBulkTableKind::NcenRegistrant => FundSourceTable::NcenRegistrant,
        SecBulkTableKind::NcenFundReportedInfo => FundSourceTable::NcenFund,
        SecBulkTableKind::NcenEtf => FundSourceTable::NcenEtf,
        SecBulkTableKind::NcenSecurityExchange => FundSourceTable::NcenSecurityExchange,
        _ => return None,
    })
}

fn source_identifier_field(
    row: &SecBulkLogicalRow,
    name: &str,
) -> Result<FundReportedValue<SourceIdentifier>, SecBulkError> {
    let field = row
        .fields()
        .iter()
        .find(|field| field.name().as_str() == name)
        .ok_or(SecBulkError::InvalidCanonicalMapping)?;
    Ok(match field.value() {
        SecBulkTypedValue::Missing => missing_source(),
        SecBulkTypedValue::Text(value) => match SourceIdentifier::try_from(value.as_str()) {
            Ok(value) => FundReportedValue::Reported(value),
            Err(_) => FundReportedValue::Missing(FundMissingState::Invalid),
        },
        SecBulkTypedValue::Date(_) | SecBulkTypedValue::Number(_) => {
            FundReportedValue::Missing(FundMissingState::Invalid)
        }
    })
}

fn is_canonical_holding_supplement(table: SecBulkTableKind) -> bool {
    fund_source_table(table).is_some_and(FundSourceTable::is_holding_supplement)
}

fn has_join(row: &SecBulkLogicalRow, domain: SecBulkJoinDomain, value: &str) -> bool {
    row.joins()
        .iter()
        .any(|join| join.domain() == domain && join.value() == value)
}

fn join_value(row: &SecBulkLogicalRow, domain: SecBulkJoinDomain) -> Option<&str> {
    row.joins()
        .iter()
        .find(|join| join.domain() == domain)
        .map(super::SecBulkJoinCoordinate::value)
}

fn validate_accession(accession: &SourceIdentifier) -> Result<(), SecBulkError> {
    let bytes = accession.as_str().as_bytes();
    if bytes.len() == 20
        && bytes.get(10) == Some(&b'-')
        && bytes.get(13) == Some(&b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| matches!(index, 10 | 13) || byte.is_ascii_digit())
    {
        Ok(())
    } else {
        Err(SecBulkError::InvalidCanonicalMapping)
    }
}

fn calendar_value(
    value: Option<NaiveDate>,
) -> Result<FundReportedValue<CalendarDate>, SecBulkError> {
    value.map_or_else(
        || Ok(missing_source()),
        |date| {
            Ok(FundReportedValue::Reported(
                CalendarDate::new(
                    u16::try_from(date.year()).map_err(|_| SecBulkError::InvalidChronology)?,
                    u8::try_from(date.month()).map_err(|_| SecBulkError::InvalidChronology)?,
                    u8::try_from(date.day()).map_err(|_| SecBulkError::InvalidChronology)?,
                )
                .map_err(|_| SecBulkError::InvalidChronology)?,
            ))
        },
    )
}

fn exact_decimal_value(value: Option<&SecExactNumber>) -> FundReportedValue<FundReportedDecimal> {
    value.map_or_else(
        missing_source,
        |number| match FundReportedDecimal::try_from_str(number.as_str()) {
            Ok(number) => FundReportedValue::Reported(number),
            Err(_) => FundReportedValue::Missing(FundMissingState::Invalid),
        },
    )
}

fn source_text_value(value: Option<&str>) -> FundReportedValue<FundSourceText> {
    value.map_or_else(
        missing_source,
        |value| match FundSourceText::try_from_string(value) {
            Ok(value) => FundReportedValue::Reported(value),
            Err(_) => FundReportedValue::Missing(FundMissingState::Invalid),
        },
    )
}

fn one_isin(values: &[&SourceIdentifier]) -> FundReportedValue<Isin> {
    let distinct = values
        .iter()
        .map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    match distinct.len() {
        0 => missing_source(),
        1 => match Isin::try_from(*distinct.first().unwrap_or(&"")) {
            Ok(value) => FundReportedValue::Reported(value),
            Err(_) => FundReportedValue::Missing(FundMissingState::Invalid),
        },
        _ => FundReportedValue::Conflict(
            market_squawk_domain::FundConflictState::CompetingSourceRows,
        ),
    }
}

fn one_identifier(values: &[&SourceIdentifier]) -> FundReportedValue<SourceIdentifier> {
    let distinct = values.iter().copied().collect::<BTreeSet<_>>();
    match distinct.len() {
        0 => missing_source(),
        1 => {
            FundReportedValue::Reported((*distinct.first().expect("one identifier exists")).clone())
        }
        _ => FundReportedValue::Conflict(
            market_squawk_domain::FundConflictState::CompetingSourceRows,
        ),
    }
}

fn option_reported<T>(value: Option<T>) -> FundReportedValue<T> {
    value.map_or_else(missing_source, FundReportedValue::Reported)
}

fn missing_source<T>() -> FundReportedValue<T> {
    FundReportedValue::Missing(FundMissingState::SourceAbsent)
}

fn not_applicable<T>() -> FundReportedValue<T> {
    FundReportedValue::Missing(FundMissingState::NotApplicable)
}

fn digest_domain(domain: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(domain).into())
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_bytes(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}
