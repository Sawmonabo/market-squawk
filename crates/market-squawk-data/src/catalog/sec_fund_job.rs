//! Exact durable SEC fund job-to-generation bindings in the sole analytical catalog.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, FundSourceFamily, InstrumentId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::SEC_EDGAR_SOURCE_ID;
use rusqlite::{Connection, OptionalExtension as _, params};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{Catalog, CatalogAuthority, CatalogError, trusted_catalog_now};
use crate::{
    AnalyticalManifestCatalog, ArrowConversionError, DatasetId, DatasetManifestRef,
    DatasetSchemaRegistry, FundPointInTimeRequest, FundPointInTimeRevisionMode,
    FundPointInTimeSelection, MAX_FUND_HOLDINGS_BATCH_RECORDS, ManifestCatalogError, PinnedDataset,
    Sha256Digest,
};

pub(crate) const SEC_FUND_DATASET_ID: &str = "sec.fund-holdings.v1";
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// Maximum equally-new durable SEC fund publications retained in one selector outcome.
pub const MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES: usize = 64;
/// Aggregate immutable-manifest metadata retained by one SEC fund selector result.
pub const MAX_SEC_FUND_POINT_IN_TIME_RETAINED_BYTES: usize = 16 * 1024 * 1024;

/// Filing family retained by one exact SEC fund job result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecFundJobFamily {
    /// Form N-PORT report and holding evidence.
    Nport,
    /// Form N-CEN annual report evidence.
    Ncen,
}

impl SecFundJobFamily {
    const fn as_catalog(self) -> &'static str {
        match self {
            Self::Nport => "nport",
            Self::Ncen => "ncen",
        }
    }

    fn from_catalog(value: &str) -> Result<Self, CatalogError> {
        match value {
            "nport" => Ok(Self::Nport),
            "ncen" => Ok(Self::Ncen),
            _ => Err(CatalogError::CorruptCatalog),
        }
    }

    pub(crate) const fn source_family(self) -> FundSourceFamily {
        match self {
            Self::Nport => FundSourceFamily::Nport,
            Self::Ncen => FundSourceFamily::Ncen,
        }
    }

    const fn from_source_family(value: FundSourceFamily) -> Self {
        match value {
            FundSourceFamily::Nport => Self::Nport,
            FundSourceFamily::Ncen => Self::Ncen,
        }
    }
}

/// Caller-safe SEC fund PIT request without a dataset, manifest, or provider-binding digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFundPointInTimeReadRequest {
    fund_instrument_id: InstrumentId,
    family: FundSourceFamily,
    revision_mode: FundPointInTimeRevisionMode,
    knowledge_cutoff: Timestamp,
    maximum_records: usize,
}

impl SecFundPointInTimeReadRequest {
    /// Requests one exact accession under a canonical fund identity and source family.
    pub fn try_as_filed(
        fund_instrument_id: InstrumentId,
        family: FundSourceFamily,
        accession: SourceIdentifier,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            fund_instrument_id,
            family,
            FundPointInTimeRevisionMode::AsFiled(accession),
            knowledge_cutoff,
            maximum_records,
        )
    }

    /// Requests every knowable accession without choosing a revision winner.
    pub fn try_all_known(
        fund_instrument_id: InstrumentId,
        family: FundSourceFamily,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            fund_instrument_id,
            family,
            FundPointInTimeRevisionMode::AllKnown,
            knowledge_cutoff,
            maximum_records,
        )
    }

    /// Requests a unique latest accession and preserves every revision-chain failure explicitly.
    pub fn try_latest_known(
        fund_instrument_id: InstrumentId,
        family: FundSourceFamily,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
    ) -> Result<Self, ArrowConversionError> {
        Self::try_new(
            fund_instrument_id,
            family,
            FundPointInTimeRevisionMode::LatestKnown,
            knowledge_cutoff,
            maximum_records,
        )
    }

    fn try_new(
        fund_instrument_id: InstrumentId,
        family: FundSourceFamily,
        revision_mode: FundPointInTimeRevisionMode,
        knowledge_cutoff: Timestamp,
        maximum_records: usize,
    ) -> Result<Self, ArrowConversionError> {
        if maximum_records == 0 || maximum_records > MAX_FUND_HOLDINGS_BATCH_RECORDS {
            return Err(ArrowConversionError::RetainedLimitExceeded);
        }
        Ok(Self {
            fund_instrument_id,
            family,
            revision_mode,
            knowledge_cutoff,
            maximum_records,
        })
    }

    pub const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }
    pub const fn family(&self) -> FundSourceFamily {
        self.family
    }
    pub const fn revision_mode(&self) -> &FundPointInTimeRevisionMode {
        &self.revision_mode
    }
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }
    pub const fn maximum_records(&self) -> usize {
        self.maximum_records
    }

    pub(crate) fn exact_request(
        &self,
        manifest: DatasetManifestRef,
    ) -> Result<FundPointInTimeRequest, ArrowConversionError> {
        let dataset = DatasetId::try_from(SEC_FUND_DATASET_ID)
            .map_err(|_| ArrowConversionError::InvalidSchemaMetadata)?;
        match &self.revision_mode {
            FundPointInTimeRevisionMode::AsFiled(accession) => {
                FundPointInTimeRequest::try_as_filed(
                    dataset,
                    self.fund_instrument_id,
                    Some(self.family),
                    accession.clone(),
                    self.knowledge_cutoff,
                    self.maximum_records,
                    Some(manifest),
                )
            }
            FundPointInTimeRevisionMode::AllKnown => FundPointInTimeRequest::try_all_known(
                dataset,
                self.fund_instrument_id,
                Some(self.family),
                self.knowledge_cutoff,
                self.maximum_records,
                Some(manifest),
            ),
            FundPointInTimeRevisionMode::LatestKnown => FundPointInTimeRequest::try_latest_known(
                dataset,
                self.fund_instrument_id,
                self.family,
                self.knowledge_cutoff,
                self.maximum_records,
                Some(manifest),
            ),
        }
    }
}

/// Exact common-job generation coordinate; no current/latest selector is representable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecFundJobCoordinate {
    job_id: Uuid,
    generation: u64,
    admitted_request_digest: EvidenceDigest,
}

impl SecFundJobCoordinate {
    /// Validates the complete durable job coordinate.
    pub fn try_new(
        job_id: Uuid,
        generation: u64,
        admitted_request_digest: EvidenceDigest,
    ) -> Result<Self, CatalogError> {
        if job_id.is_nil() || generation == 0 || !valid_sha256(admitted_request_digest) {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            job_id,
            generation,
            admitted_request_digest,
        })
    }

    /// Returns the common job identity.
    pub const fn job_id(self) -> Uuid {
        self.job_id
    }

    /// Returns the exact common job generation.
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the immutable admitted request identity.
    pub const fn admitted_request_digest(self) -> EvidenceDigest {
        self.admitted_request_digest
    }
}

/// One-shot SEC job intent claimed only at the final provider-logical commit boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFundJobCommit {
    coordinate: SecFundJobCoordinate,
    binding_digest: EvidenceDigest,
    preparation_digest: EvidenceDigest,
    family: SecFundJobFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    fund_id: Option<SourceIdentifier>,
    fund_instrument_id: InstrumentId,
    row_count: u64,
    logical_object_bytes: u64,
    logical_object_count: usize,
}

impl SecFundJobCommit {
    /// Closes the exact job, request, preparation, logical binding, identity, and count evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "the atomic catalog boundary keeps every independently verified coordinate explicit"
    )]
    pub fn try_new(
        coordinate: SecFundJobCoordinate,
        binding_digest: EvidenceDigest,
        preparation_digest: EvidenceDigest,
        family: SecFundJobFamily,
        year: u16,
        quarter: u8,
        accession: SourceIdentifier,
        fund_id: Option<SourceIdentifier>,
        fund_instrument_id: InstrumentId,
        row_count: u64,
        logical_object_bytes: u64,
        logical_object_count: usize,
    ) -> Result<Self, CatalogError> {
        if !valid_sha256(binding_digest)
            || !valid_sha256(preparation_digest)
            || !(1..=4).contains(&quarter)
            || !(1993..=9999).contains(&year)
            || row_count == 0
            || logical_object_bytes == 0
            || logical_object_count == 0
            || logical_object_count > 64
            || matches!(family, SecFundJobFamily::Nport) != fund_id.is_none()
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            coordinate,
            binding_digest,
            preparation_digest,
            family,
            year,
            quarter,
            accession,
            fund_id,
            fund_instrument_id,
            row_count,
            logical_object_bytes,
            logical_object_count,
        })
    }

    /// Returns the exact durable job coordinate.
    pub const fn coordinate(&self) -> SecFundJobCoordinate {
        self.coordinate
    }

    /// Returns the common logical-publication binding claimed by this job.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
}

/// Exact immutable SEC job publication recovered from catalog plus manifest authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFundJobDurablePublication {
    coordinate: SecFundJobCoordinate,
    family: SecFundJobFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    fund_id: Option<SourceIdentifier>,
    binding_digest: EvidenceDigest,
    preparation_digest: EvidenceDigest,
    fund_instrument_id: InstrumentId,
    pinned: PinnedDataset,
    row_count: u64,
    total_bytes: u64,
    object_count: usize,
    committed_at: Timestamp,
}

impl SecFundJobDurablePublication {
    pub const fn coordinate(&self) -> SecFundJobCoordinate {
        self.coordinate
    }

    pub const fn family(&self) -> SecFundJobFamily {
        self.family
    }

    pub const fn year(&self) -> u16 {
        self.year
    }

    pub const fn quarter(&self) -> u8 {
        self.quarter
    }

    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    pub const fn fund_id(&self) -> Option<&SourceIdentifier> {
        self.fund_id.as_ref()
    }

    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    pub const fn preparation_digest(&self) -> EvidenceDigest {
        self.preparation_digest
    }

    pub const fn fund_instrument_id(&self) -> InstrumentId {
        self.fund_instrument_id
    }

    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub const fn object_count(&self) -> usize {
        self.object_count
    }

    pub const fn committed_at(&self) -> Timestamp {
        self.committed_at
    }
}

/// Exact crash-recovery disposition for one common SEC fund job generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecFundJobRecovery {
    /// No analytical generation committed for this exact claimed coordinate.
    NotDataCommitted,
    /// Data committed but the atomic projection is absent or inconsistent; fail closed.
    DataCommittedWithoutProjection,
    /// Complete exact job, logical, manifest, identity, and count evidence.
    Published(SecFundJobDurablePublication),
}

/// Bounded newest-generation selection for one canonical fund identity and family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecFundJobPointInTimeSelection {
    /// No committed job generation exists at or before the knowledge cutoff.
    Missing,
    /// One exact immutable generation per accession closes the requested coordinate set.
    Exact(Box<[SecFundJobDurablePublication]>),
    /// Equally-new generations for an accession or selector truncation prevents exact closure.
    Ambiguous {
        candidates: Box<[SecFundJobDurablePublication]>,
        truncated: bool,
    },
    /// A committed admission lacks a complete exact generation projection.
    Conflict {
        coordinates: Box<[SecFundJobCoordinate]>,
        truncated: bool,
    },
}

/// Caller-safe SEC fund read result retaining every coordinate-selection state.
#[derive(Clone, Debug)]
pub enum SecFundPointInTimeReadOutcome {
    Missing,
    Ambiguous {
        candidates: Box<[SecFundJobDurablePublication]>,
        truncated: bool,
    },
    Conflict {
        coordinates: Box<[SecFundJobCoordinate]>,
        truncated: bool,
    },
    Exact {
        publication: SecFundJobDurablePublication,
        selection: FundPointInTimeSelection,
    },
    /// Multiple exact accession coordinates are knowable; V1 does not merge their object graphs.
    RevisionSet {
        publications: Box<[SecFundJobDurablePublication]>,
    },
}

impl SecFundPointInTimeReadOutcome {
    pub fn exact_publications(&self) -> &[SecFundJobDurablePublication] {
        match self {
            Self::Exact { publication, .. } => std::slice::from_ref(publication),
            Self::RevisionSet { publications } => publications,
            Self::Missing | Self::Ambiguous { .. } | Self::Conflict { .. } => &[],
        }
    }

    pub const fn exact_selection(&self) -> Option<&FundPointInTimeSelection> {
        match self {
            Self::Exact { selection, .. } => Some(selection),
            Self::Missing
            | Self::Ambiguous { .. }
            | Self::Conflict { .. }
            | Self::RevisionSet { .. } => None,
        }
    }

    pub const fn revision_chain_required(&self) -> bool {
        matches!(self, Self::RevisionSet { .. })
    }

    pub(crate) fn restart_matches(&self, replay: &Self) -> bool {
        match (self, replay) {
            (Self::Missing, Self::Missing) => true,
            (
                Self::Ambiguous {
                    candidates: left,
                    truncated: left_truncated,
                },
                Self::Ambiguous {
                    candidates: right,
                    truncated: right_truncated,
                },
            ) => left == right && left_truncated == right_truncated,
            (
                Self::Conflict {
                    coordinates: left,
                    truncated: left_truncated,
                },
                Self::Conflict {
                    coordinates: right,
                    truncated: right_truncated,
                },
            ) => left == right && left_truncated == right_truncated,
            (
                Self::Exact {
                    publication: left_publication,
                    selection: left_selection,
                },
                Self::Exact {
                    publication: right_publication,
                    selection: right_selection,
                },
            ) => {
                left_publication == right_publication
                    && left_selection.manifest() == right_selection.manifest()
                    && left_selection.selection_digest() == right_selection.selection_digest()
                    && left_selection.outcome() == right_selection.outcome()
            }
            (
                Self::RevisionSet {
                    publications: left_publications,
                },
                Self::RevisionSet {
                    publications: right_publications,
                },
            ) => left_publications == right_publications,
            _ => false,
        }
    }
}

/// Least-authority exact SEC job recovery over the sole catalog and manifest owners.
#[derive(Clone)]
pub struct SecFundJobCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    manifests: Arc<AnalyticalManifestCatalog>,
}

impl std::fmt::Debug for SecFundJobCatalogCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SecFundJobCatalogCapability")
            .field("authority", &"[SEALED SEC FUND JOB CATALOG AUTHORITY]")
            .field("manifests", &"[IMMUTABLE GENERATION CATALOG]")
            .finish()
    }
}

impl SecFundJobCatalogCapability {
    pub(crate) fn new(
        authority: Arc<Mutex<CatalogAuthority>>,
        manifests: Arc<AnalyticalManifestCatalog>,
    ) -> Self {
        Self {
            authority,
            manifests,
        }
    }

    /// Reopens only the supplied common-job coordinate and its exact immutable generation.
    pub fn recover_exact(
        &self,
        coordinate: SecFundJobCoordinate,
    ) -> Result<SecFundJobRecovery, SecFundJobCatalogError> {
        let record = self
            .authority
            .lock()
            .map_err(|_| SecFundJobCatalogError::AuthorityUnavailable)?
            .catalog()
            .sec_fund_job_recovery_record(coordinate)?;
        let SecFundJobCatalogRecord::Published(record) = record else {
            return Ok(match record {
                SecFundJobCatalogRecord::NotDataCommitted => SecFundJobRecovery::NotDataCommitted,
                SecFundJobCatalogRecord::DataCommittedWithoutProjection => {
                    SecFundJobRecovery::DataCommittedWithoutProjection
                }
                SecFundJobCatalogRecord::Published(_) => unreachable!(),
            });
        };
        let pinned = self.manifests.pinned(&record.manifest)?;
        if pinned.manifest() != &record.manifest
            || pinned.plan().row_count() != record.row_count
            || pinned.plan().total_bytes() != record.total_bytes
            || pinned.plan().objects().len() != record.object_count
        {
            return Ok(SecFundJobRecovery::DataCommittedWithoutProjection);
        }
        Ok(SecFundJobRecovery::Published(
            SecFundJobDurablePublication {
                coordinate: record.coordinate,
                family: record.family,
                year: record.year,
                quarter: record.quarter,
                accession: record.accession,
                fund_id: record.fund_id,
                binding_digest: record.binding_digest,
                preparation_digest: record.preparation_digest,
                fund_instrument_id: record.fund_instrument_id,
                pinned,
                row_count: record.row_count,
                total_bytes: record.total_bytes,
                object_count: record.object_count,
                committed_at: record.committed_at,
            },
        ))
    }

    /// Selects only the bounded set of equally-new committed generations at one PIT cutoff.
    pub fn select_point_in_time(
        &self,
        request: &SecFundPointInTimeReadRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SecFundJobPointInTimeSelection, SecFundJobCatalogError> {
        check_operation(deadline, cancellation)?;
        let coordinates = {
            let authority = self
                .authority
                .try_lock()
                .map_err(|_| SecFundJobCatalogError::AuthorityUnavailable)?;
            let connection = &authority.catalog().connection;
            install_progress_handler(connection, deadline, cancellation)?;
            let selected = newest_sec_fund_job_coordinates(
                connection,
                request.fund_instrument_id(),
                SecFundJobFamily::from_source_family(request.family()),
                match request.revision_mode() {
                    FundPointInTimeRevisionMode::AsFiled(accession) => Some(accession),
                    FundPointInTimeRevisionMode::AllKnown
                    | FundPointInTimeRevisionMode::LatestKnown => None,
                },
                request.knowledge_cutoff(),
            );
            clear_progress_handler(connection)?;
            match selected {
                Ok(selected) => selected,
                Err(error) => {
                    check_operation(deadline, cancellation)?;
                    return Err(error.into());
                }
            }
        };
        check_operation(deadline, cancellation)?;
        if coordinates.is_empty() {
            return Ok(SecFundJobPointInTimeSelection::Missing);
        }
        let truncated = coordinates.len() > MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES;
        let retained = coordinates
            .iter()
            .take(MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES)
            .cloned();
        let mut published = Vec::new();
        published
            .try_reserve_exact(coordinates.len().min(MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES))
            .map_err(|_| SecFundJobCatalogError::Allocation)?;
        let mut conflicts = Vec::new();
        conflicts
            .try_reserve_exact(coordinates.len().min(MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES))
            .map_err(|_| SecFundJobCatalogError::Allocation)?;
        let mut retained_manifest_bytes = 0_usize;
        for candidate in retained {
            check_operation(deadline, cancellation)?;
            let coordinate = candidate.coordinate;
            match self.recover_exact(coordinate)? {
                SecFundJobRecovery::Published(publication) => {
                    if publication.fund_instrument_id() != request.fund_instrument_id()
                        || publication.family()
                            != SecFundJobFamily::from_source_family(request.family())
                        || publication.accession() != &candidate.accession
                        || publication.committed_at() > request.knowledge_cutoff()
                    {
                        conflicts.push(coordinate);
                    } else {
                        retained_manifest_bytes = retained_manifest_bytes
                            .checked_add(publication.pinned().retained_bytes())
                            .ok_or(SecFundJobCatalogError::Allocation)?;
                        if retained_manifest_bytes > MAX_SEC_FUND_POINT_IN_TIME_RETAINED_BYTES {
                            return Ok(SecFundJobPointInTimeSelection::Ambiguous {
                                candidates: published.into_boxed_slice(),
                                truncated: true,
                            });
                        }
                        published.push(publication);
                    }
                }
                SecFundJobRecovery::NotDataCommitted
                | SecFundJobRecovery::DataCommittedWithoutProjection => {
                    conflicts.push(coordinate);
                }
            }
        }
        if !conflicts.is_empty() {
            return Ok(SecFundJobPointInTimeSelection::Conflict {
                coordinates: conflicts.into_boxed_slice(),
                truncated,
            });
        }
        published.sort_by(|left, right| {
            left.accession()
                .cmp(right.accession())
                .then_with(|| left.coordinate().job_id().cmp(&right.coordinate().job_id()))
                .then_with(|| {
                    left.coordinate()
                        .generation()
                        .cmp(&right.coordinate().generation())
                })
                .then_with(|| {
                    left.coordinate()
                        .admitted_request_digest()
                        .bytes()
                        .cmp(&right.coordinate().admitted_request_digest().bytes())
                })
        });
        let ambiguous_accession = published
            .windows(2)
            .any(|pair| pair[0].accession() == pair[1].accession());
        if truncated || ambiguous_accession {
            return Ok(SecFundJobPointInTimeSelection::Ambiguous {
                candidates: published.into_boxed_slice(),
                truncated,
            });
        }
        if published.is_empty() {
            return Err(SecFundJobCatalogError::Catalog(
                CatalogError::CorruptCatalog,
            ));
        }
        Ok(SecFundJobPointInTimeSelection::Exact(
            published.into_boxed_slice(),
        ))
    }
}

/// Exact SEC job catalog/recovery failure without exposing the general catalog writer.
#[derive(Debug, Error)]
pub enum SecFundJobCatalogError {
    #[error("SEC fund job catalog authority is unavailable")]
    AuthorityUnavailable,
    #[error("SEC fund job selection was cancelled")]
    Cancelled,
    #[error("SEC fund job selection deadline elapsed")]
    DeadlineExceeded,
    #[error("SEC fund job selection allocation failed")]
    Allocation,
    #[error("SEC fund job catalog record failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("SEC fund job exact manifest failed: {0}")]
    Manifest(#[from] ManifestCatalogError),
}

struct SecFundJobPublishedRecord {
    coordinate: SecFundJobCoordinate,
    family: SecFundJobFamily,
    year: u16,
    quarter: u8,
    accession: SourceIdentifier,
    fund_id: Option<SourceIdentifier>,
    binding_digest: EvidenceDigest,
    preparation_digest: EvidenceDigest,
    fund_instrument_id: InstrumentId,
    manifest: DatasetManifestRef,
    row_count: u64,
    total_bytes: u64,
    object_count: usize,
    committed_at: Timestamp,
}

enum SecFundJobCatalogRecord {
    NotDataCommitted,
    DataCommittedWithoutProjection,
    Published(SecFundJobPublishedRecord),
}

#[derive(Clone)]
struct SecFundJobCandidateCoordinate {
    accession: SourceIdentifier,
    coordinate: SecFundJobCoordinate,
}

fn newest_sec_fund_job_coordinates(
    connection: &Connection,
    fund_instrument_id: InstrumentId,
    family: SecFundJobFamily,
    exact_accession: Option<&SourceIdentifier>,
    knowledge_cutoff: Timestamp,
) -> Result<Vec<SecFundJobCandidateCoordinate>, CatalogError> {
    let retrieval_limit = i64::try_from(
        MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES
            .checked_add(1)
            .ok_or(CatalogError::InvalidRecord)?,
    )
    .map_err(|_| CatalogError::InvalidRecord)?;
    let mut statement = connection.prepare(
        "WITH newest AS (
             SELECT accession, MAX(resolved_at_ns) AS newest_at_ns
             FROM sec_fund_job_commit_admissions
             WHERE fund_instrument_id=?1 AND family=?2 AND state='committed'
               AND resolved_at_ns<=?3 AND (?4 IS NULL OR accession=?4)
             GROUP BY accession
         )
         SELECT admission.accession, admission.job_id, admission.job_generation,
                admission.admitted_request_digest
         FROM newest
         JOIN sec_fund_job_commit_admissions AS admission
           ON admission.accession=newest.accession
          AND admission.resolved_at_ns=newest.newest_at_ns
         WHERE admission.fund_instrument_id=?1 AND admission.family=?2
           AND admission.state='committed'
         ORDER BY admission.accession, admission.job_id, admission.job_generation,
                  admission.admitted_request_digest
         LIMIT ?5",
    )?;
    let mut rows = statement.query(params![
        fund_instrument_id.as_uuid().as_bytes().as_slice(),
        family.as_catalog(),
        knowledge_cutoff.unix_nanos(),
        exact_accession.map(SourceIdentifier::as_str),
        retrieval_limit,
    ])?;
    let mut coordinates = Vec::new();
    coordinates
        .try_reserve_exact(
            MAX_SEC_FUND_POINT_IN_TIME_CANDIDATES
                .checked_add(1)
                .ok_or(CatalogError::InvalidRecord)?,
        )
        .map_err(|_| CatalogError::Allocation)?;
    while let Some(row) = rows.next()? {
        let accession = SourceIdentifier::try_from(row.get::<_, String>(0)?)
            .map_err(|_| CatalogError::CorruptCatalog)?;
        let job_id =
            Uuid::parse_str(&row.get::<_, String>(1)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let generation =
            u64::try_from(row.get::<_, i64>(2)?).map_err(|_| CatalogError::CorruptCatalog)?;
        let request = parse_digest(&row.get::<_, Vec<u8>>(3)?)?;
        coordinates.push(SecFundJobCandidateCoordinate {
            accession,
            coordinate: SecFundJobCoordinate::try_new(job_id, generation, request)
                .map_err(|_| CatalogError::CorruptCatalog)?,
        });
    }
    Ok(coordinates)
}

impl Catalog {
    pub(crate) fn stage_sec_fund_job_commit(
        &self,
        commit: &SecFundJobCommit,
        ingest_run_id: Uuid,
    ) -> Result<(), CatalogError> {
        if ingest_run_id.is_nil() {
            return Err(CatalogError::InvalidRecord);
        }
        let transaction = self
            .connection
            .unchecked_transaction()
            .map_err(CatalogError::from)?;
        let now = trusted_catalog_now(&transaction)?;
        let coordinate = commit.coordinate;
        let binding = digest_bytes(commit.binding_digest);

        let prior: Option<(String, i64, Vec<u8>, String, String)> = transaction
            .query_row(
                "SELECT job_id, job_generation, admitted_request_digest, state, ingest_run_id
                 FROM sec_fund_job_commit_admissions
                 WHERE binding_digest=?1 AND state='pending'",
                [binding],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(CatalogError::from)?;
        if let Some((job_id, generation, digest, state, run_id)) = prior {
            let exact = job_id == coordinate.job_id.to_string()
                && u64::try_from(generation).ok() == Some(coordinate.generation)
                && digest.as_slice() == coordinate.admitted_request_digest.bytes()
                && state == "pending"
                && run_id == ingest_run_id.to_string();
            if !exact {
                let data_committed: bool = transaction
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1
                             FROM analytical_generation_provider_publication_bindings
                             WHERE publication_digest=?1 AND publication_kind='provider_logical'
                               AND run_id=?2
                         )",
                        params![binding, run_id],
                        |row| row.get(0),
                    )
                    .map_err(CatalogError::from)?;
                if data_committed {
                    return Err(CatalogError::CorruptCatalog);
                }
                transaction
                    .execute(
                        "UPDATE sec_fund_job_commit_admissions
                         SET state='rolled_back', resolved_at_ns=?1
                         WHERE binding_digest=?2 AND state='pending'",
                        params![now.unix_nanos(), binding],
                    )
                    .map_err(CatalogError::from)?;
            }
        }

        transaction
            .execute(
                "INSERT OR IGNORE INTO sec_fund_job_commit_admissions
                 (job_id, job_generation, admitted_request_digest, binding_digest,
                  ingest_run_id, preparation_digest, family, filing_year, filing_quarter,
                  accession, fund_id, fund_instrument_id, expected_row_count,
                  expected_logical_object_bytes, expected_logical_object_count, state,
                  admitted_at_ns, resolved_at_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                         ?15, 'pending', ?16, NULL)",
                params![
                    coordinate.job_id.to_string(),
                    to_i64(coordinate.generation)?,
                    digest_bytes(coordinate.admitted_request_digest),
                    binding,
                    ingest_run_id.to_string(),
                    digest_bytes(commit.preparation_digest),
                    commit.family.as_catalog(),
                    i64::from(commit.year),
                    i64::from(commit.quarter),
                    commit.accession.as_str(),
                    commit.fund_id.as_ref().map(SourceIdentifier::as_str),
                    commit.fund_instrument_id.as_uuid().as_bytes().as_slice(),
                    to_i64(commit.row_count)?,
                    to_i64(commit.logical_object_bytes)?,
                    to_i64(commit.logical_object_count)?,
                    now.unix_nanos(),
                ],
            )
            .map_err(CatalogError::from)?;
        let exact: bool = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sec_fund_job_commit_admissions
                     WHERE job_id=?1 AND job_generation=?2 AND admitted_request_digest=?3
                       AND binding_digest=?4 AND ingest_run_id=?5
                       AND preparation_digest=?6 AND family=?7
                       AND filing_year=?8 AND filing_quarter=?9 AND accession=?10
                       AND fund_id IS ?11 AND fund_instrument_id=?12
                       AND expected_row_count=?13 AND expected_logical_object_bytes=?14
                       AND expected_logical_object_count=?15 AND state='pending'
                 )",
                params![
                    coordinate.job_id.to_string(),
                    to_i64(coordinate.generation)?,
                    digest_bytes(coordinate.admitted_request_digest),
                    binding,
                    ingest_run_id.to_string(),
                    digest_bytes(commit.preparation_digest),
                    commit.family.as_catalog(),
                    i64::from(commit.year),
                    i64::from(commit.quarter),
                    commit.accession.as_str(),
                    commit.fund_id.as_ref().map(SourceIdentifier::as_str),
                    commit.fund_instrument_id.as_uuid().as_bytes().as_slice(),
                    to_i64(commit.row_count)?,
                    to_i64(commit.logical_object_bytes)?,
                    to_i64(commit.logical_object_count)?,
                ],
                |row| row.get(0),
            )
            .map_err(CatalogError::from)?;
        if !exact {
            return Err(CatalogError::InvalidRecord);
        }
        transaction.commit().map_err(CatalogError::from)
    }

    fn sec_fund_job_recovery_record(
        &self,
        coordinate: SecFundJobCoordinate,
    ) -> Result<SecFundJobCatalogRecord, CatalogError> {
        type Stored = (
            String,
            i64,
            i64,
            String,
            Option<String>,
            Vec<u8>,
            Vec<u8>,
            Vec<u8>,
            i64,
            String,
            i64,
            Vec<u8>,
            Vec<u8>,
            i64,
            i64,
            i64,
            i64,
        );
        let stored: Option<Stored> = self
            .connection
            .query_row(
                "SELECT admission.family, admission.filing_year, admission.filing_quarter,
                        admission.accession, admission.fund_id, publication.binding_digest,
                        publication.preparation_digest, publication.fund_instrument_id,
                        generation.manifest_version, generation.schema_name,
                        generation.schema_version, generation.schema_fingerprint,
                        generation.content_hash, publication.generation_row_count,
                        publication.generation_total_bytes, publication.generation_object_count,
                        publication.committed_at_ns
                 FROM sec_fund_job_publications AS publication
                 JOIN sec_fund_job_commit_admissions AS admission
                   USING (job_id, job_generation, admitted_request_digest)
                 JOIN analytical_generations AS generation
                   ON generation.generation_sequence=publication.generation_sequence
                  AND generation.dataset_id=publication.dataset_id
                  AND generation.manifest_version=publication.manifest_version
                 JOIN analytical_generation_provider_publication_bindings AS logical
                   ON logical.generation_sequence=publication.generation_sequence
                  AND logical.publication_digest=publication.binding_digest
                  AND logical.publication_kind='provider_logical'
                  AND logical.run_id=publication.ingest_run_id
                 JOIN provider_logical_publication_bindings AS binding
                   ON binding.binding_digest=publication.binding_digest
                  AND binding.source_id=?4
                 WHERE publication.job_id=?1 AND publication.job_generation=?2
                   AND publication.admitted_request_digest=?3
                   AND admission.state='committed'
                   AND admission.ingest_run_id=publication.ingest_run_id
                   AND admission.binding_digest=publication.binding_digest
                   AND admission.preparation_digest=publication.preparation_digest
                   AND admission.fund_instrument_id=publication.fund_instrument_id
                   AND admission.expected_row_count=publication.publication_row_count
                   AND admission.expected_logical_object_bytes=publication.logical_object_bytes
                   AND admission.expected_logical_object_count=publication.logical_object_count
                   AND generation.row_count=publication.generation_row_count
                   AND generation.total_bytes=publication.generation_total_bytes
                   AND publication.generation_object_count=(
                       SELECT COUNT(*) FROM analytical_generation_objects AS object
                       WHERE object.dataset_id=generation.dataset_id
                         AND object.manifest_version=generation.manifest_version
                   )
                   AND publication.committed_at_ns=generation.created_at_ns",
                params![
                    coordinate.job_id.to_string(),
                    to_i64(coordinate.generation)?,
                    digest_bytes(coordinate.admitted_request_digest),
                    SEC_EDGAR_SOURCE_ID,
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                    ))
                },
            )
            .optional()
            .map_err(CatalogError::from)?;
        if let Some(stored) = stored {
            let family = SecFundJobFamily::from_catalog(&stored.0)?;
            let year = u16::try_from(stored.1).map_err(|_| CatalogError::CorruptCatalog)?;
            let quarter = u8::try_from(stored.2).map_err(|_| CatalogError::CorruptCatalog)?;
            let accession =
                SourceIdentifier::try_from(stored.3).map_err(|_| CatalogError::CorruptCatalog)?;
            let fund_id = stored
                .4
                .map(SourceIdentifier::try_from)
                .transpose()
                .map_err(|_| CatalogError::CorruptCatalog)?;
            let binding_digest = parse_digest(&stored.5)?;
            let preparation_digest = parse_digest(&stored.6)?;
            let instrument_uuid =
                Uuid::from_slice(&stored.7).map_err(|_| CatalogError::CorruptCatalog)?;
            let fund_instrument_id = InstrumentId::try_from(instrument_uuid)
                .map_err(|_| CatalogError::CorruptCatalog)?;
            let expected_schema = DatasetSchemaRegistry::local()
                .canonical_fund_holdings()
                .map_err(|_| CatalogError::CorruptCatalog)?;
            if stored.9 != expected_schema.name()
                || u16::try_from(stored.10).ok() != Some(expected_schema.version().get())
                || stored.11.as_slice() != expected_schema.fingerprint()
            {
                return Err(CatalogError::CorruptCatalog);
            }
            let manifest = DatasetManifestRef::try_new_with_schema(
                DatasetId::try_from(SEC_FUND_DATASET_ID)
                    .map_err(|_| CatalogError::CorruptCatalog)?,
                u64::try_from(stored.8).map_err(|_| CatalogError::CorruptCatalog)?,
                expected_schema,
                Sha256Digest::new(parse_digest(&stored.12)?.bytes()),
            )
            .map_err(|_| CatalogError::CorruptCatalog)?;
            return Ok(SecFundJobCatalogRecord::Published(
                SecFundJobPublishedRecord {
                    coordinate,
                    family,
                    year,
                    quarter,
                    accession,
                    fund_id,
                    binding_digest,
                    preparation_digest,
                    fund_instrument_id,
                    manifest,
                    row_count: u64::try_from(stored.13)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                    total_bytes: u64::try_from(stored.14)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                    object_count: usize::try_from(stored.15)
                        .map_err(|_| CatalogError::CorruptCatalog)?,
                    committed_at: Timestamp::from_unix_nanos(stored.16),
                },
            ));
        }

        let admission: Option<(Vec<u8>, String)> = self
            .connection
            .query_row(
                "SELECT binding_digest, state
                 FROM sec_fund_job_commit_admissions
                 WHERE job_id=?1 AND job_generation=?2 AND admitted_request_digest=?3",
                params![
                    coordinate.job_id.to_string(),
                    to_i64(coordinate.generation)?,
                    digest_bytes(coordinate.admitted_request_digest),
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(CatalogError::from)?;
        let Some((binding, state)) = admission else {
            return Ok(SecFundJobCatalogRecord::NotDataCommitted);
        };
        let data_committed: bool = self
            .connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM analytical_generation_provider_publication_bindings
                     WHERE publication_digest=?1 AND publication_kind='provider_logical'
                       AND run_id=(
                           SELECT ingest_run_id FROM sec_fund_job_commit_admissions
                           WHERE job_id=?2 AND job_generation=?3
                             AND admitted_request_digest=?4
                       )
                 )",
                params![
                    binding,
                    coordinate.job_id.to_string(),
                    to_i64(coordinate.generation)?,
                    digest_bytes(coordinate.admitted_request_digest),
                ],
                |row| row.get(0),
            )
            .map_err(CatalogError::from)?;
        if data_committed || state == "committed" {
            Ok(SecFundJobCatalogRecord::DataCommittedWithoutProjection)
        } else if matches!(state.as_str(), "pending" | "rolled_back") {
            Ok(SecFundJobCatalogRecord::NotDataCommitted)
        } else {
            Err(CatalogError::CorruptCatalog)
        }
    }
}

fn valid_sha256(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes().iter().any(|byte| *byte != 0)
}

fn digest_bytes(digest: EvidenceDigest) -> [u8; 32] {
    digest.bytes()
}

fn parse_digest(bytes: &[u8]) -> Result<EvidenceDigest, CatalogError> {
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| CatalogError::CorruptCatalog)?;
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, bytes);
    if valid_sha256(digest) {
        Ok(digest)
    } else {
        Err(CatalogError::CorruptCatalog)
    }
}

fn install_progress_handler(
    connection: &Connection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecFundJobCatalogError> {
    let token = cancellation.clone();
    connection
        .progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )
        .map_err(CatalogError::from)?;
    Ok(())
}

fn clear_progress_handler(connection: &Connection) -> Result<(), SecFundJobCatalogError> {
    connection
        .progress_handler::<fn() -> bool>(0, None)
        .map_err(CatalogError::from)?;
    Ok(())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), SecFundJobCatalogError> {
    if cancellation.is_cancelled() {
        Err(SecFundJobCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SecFundJobCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn to_i64(value: impl TryInto<i64>) -> Result<i64, CatalogError> {
    value.try_into().map_err(|_| CatalogError::InvalidRecord)
}
