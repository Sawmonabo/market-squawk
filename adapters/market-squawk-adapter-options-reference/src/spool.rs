//! Disk-backed, bounded staging for complete option-reference publications.
//!
//! The spool is deliberately non-authoritative: it keeps millions of provider rows and mapping
//! uniqueness checks out of heap memory, then yields one sealed database for atomic publication by
//! [`crate::ReferenceArtifactStore`]. A failed or conflicted spool never advances a published
//! generation.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::NonZeroU32;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{ProviderInstrumentId, SourceIdentifier, Timestamp};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    CatalogConflict, CatalogConflictKind, CatalogCounts, CboeParseError, CboeSeriesReference,
    CboeSeriesStatus, CboeSymbolId, CboeVenue, OccDlpProductReference, OccExchangeCode,
    OccExchangeListingEvidence, OccParseError, OccPositionLimit, OccProductType,
    OptionContractIdentity, PageTerminalState, PublicationCatalog, PublicationCompleteness,
    PublicationRequest, ReferenceFetchControl, ReferencePageReceipt, ReferenceSurface,
    ReferenceTransportError,
};

const SPOOL_SCHEMA_VERSION: i64 = 6;
const SQLITE_PAGE_BYTES: u64 = 4_096;
const MIN_SPOOL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SPOOL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_CACHE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_QUERY_ROWS: u32 = 1_024;
const MAX_RETAINED_CONFLICTS: u32 = 10_000;
const CHECKPOINT_ROWS: u32 = 16_384;
const STAGED_DATABASE_PREFIX: &str = ".options-reference-spool-";
const SPOOL_LOCK_FILE: &str = ".options-reference-spool.lock";

/// Explicit disk, SQLite-cache, conflict, and typed-query limits for a publication spool.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReferenceSpoolLimits {
    max_database_bytes: u64,
    sqlite_cache_bytes: u64,
    max_conflicts: u32,
    max_query_rows: NonZeroU32,
}

impl ReferenceSpoolLimits {
    /// Constructs limits at or below the code-owned hard ceilings.
    ///
    /// # Errors
    ///
    /// Rejects a database too small for the selected large-file workload, an excessive cache,
    /// zero conflicts, or a query bound above the typed read ceiling.
    pub fn try_new(
        max_database_bytes: u64,
        sqlite_cache_bytes: u64,
        max_conflicts: u32,
        max_query_rows: u32,
    ) -> Result<Self, ReferenceSpoolError> {
        if !(MIN_SPOOL_BYTES..=MAX_SPOOL_BYTES).contains(&max_database_bytes)
            || max_database_bytes % SQLITE_PAGE_BYTES != 0
            || sqlite_cache_bytes == 0
            || sqlite_cache_bytes > MAX_CACHE_BYTES
            || sqlite_cache_bytes % 1_024 != 0
            || max_conflicts == 0
            || max_conflicts > MAX_RETAINED_CONFLICTS
            || max_query_rows == 0
            || max_query_rows > MAX_QUERY_ROWS
        {
            return Err(ReferenceSpoolError::InvalidLimits);
        }
        Ok(Self {
            max_database_bytes,
            sqlite_cache_bytes,
            max_conflicts,
            max_query_rows: NonZeroU32::new(max_query_rows)
                .ok_or(ReferenceSpoolError::InvalidLimits)?,
        })
    }

    /// Returns the exact database-file ceiling.
    pub const fn max_database_bytes(self) -> u64 {
        self.max_database_bytes
    }

    /// Returns the SQLite page-cache ceiling.
    pub const fn sqlite_cache_bytes(self) -> u64 {
        self.sqlite_cache_bytes
    }

    /// Returns the retained conflict ceiling.
    pub const fn max_conflicts(self) -> u32 {
        self.max_conflicts
    }

    /// Returns the typed query row ceiling.
    pub const fn max_query_rows(self) -> NonZeroU32 {
        self.max_query_rows
    }
}

/// Disk-backed staging generation with bounded transaction checkpoints per exact source object.
#[derive(Debug)]
pub struct ReferencePublicationSpool {
    request: PublicationRequest,
    control: ReferenceFetchControl,
    _spool_lock: OwnedSpoolLockGuard,
    limits: ReferenceSpoolLimits,
    database_file: File,
    connection: Option<Connection>,
    completed_surfaces: BTreeSet<ReferenceSurface>,
    aggregate: SpoolAggregate,
    poisoned: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct SpoolAggregate {
    pages: u32,
    bytes: u64,
    records: u64,
    conflicts: u32,
}

struct OwnedSpoolLockGuard {
    file: File,
}

impl std::fmt::Debug for OwnedSpoolLockGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnedSpoolLockGuard")
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedSpoolLockGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

impl ReferencePublicationSpool {
    /// Creates a fail-closed greenfield spool from an application-minted directory capability.
    ///
    /// # Errors
    ///
    /// The SQLite inode is created with no-follow/create-new semantics, opened through its file
    /// descriptor, then immediately unlinked. It therefore has no ambient production pathname or
    /// interrupted staging entry to follow after a crash. SQLite uses a memory journal with
    /// bounded row checkpoints; only a fully closed, integrity-checked descriptor can be published.
    ///
    /// Rejects an invalid capability, insufficient free disk, unsafe limits, or any inability to
    /// freeze the exact SQLite schema and page ceiling.
    pub(crate) fn create(
        request: PublicationRequest,
        control: ReferenceFetchControl,
        staging_directory: Dir,
        limits: ReferenceSpoolLimits,
    ) -> Result<Self, ReferenceSpoolError> {
        validate_capability_directory(&staging_directory)?;
        validate_spool_request(&request)?;
        ensure_publication_open(&control, &request)?;
        let spool_lock = acquire_spool_lock(&staging_directory)?;
        let required_reserve = limits
            .max_database_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(MIN_SPOOL_BYTES))
            .ok_or(ReferenceSpoolError::InvalidLimits)?;
        let available = available_capability_space(&staging_directory)?;
        if available < required_reserve {
            return Err(ReferenceSpoolError::InsufficientDisk {
                required: required_reserve,
                available,
            });
        }
        let (database_file, connection) = create_anonymous_sqlite_database(&staging_directory)?;
        configure_new_database(&connection, limits, &request)?;
        Ok(Self {
            request,
            control,
            _spool_lock: spool_lock,
            limits,
            database_file,
            connection: Some(connection),
            completed_surfaces: BTreeSet::new(),
            aggregate: SpoolAggregate::default(),
            poisoned: false,
        })
    }

    /// Opens one object/page batch. A bounded checkpoint may commit non-authoritative staging rows,
    /// but the spool is poisoned until `finish` commits the exact reconciled page receipt. Dropped
    /// or failed batches therefore make the whole anonymous spool permanently unsealable.
    ///
    /// # Errors
    ///
    /// Rejects unrequested or already-completed single-file surfaces.
    pub fn begin_page(
        &mut self,
        surface: ReferenceSurface,
    ) -> Result<ReferencePageBatch<'_>, ReferenceSpoolError> {
        ensure_publication_open(&self.control, &self.request)?;
        if self.poisoned
            || self.request.surfaces().binary_search(&surface).is_err()
            || self.completed_surfaces.contains(&surface)
        {
            return Err(ReferenceSpoolError::InvalidSurfaceState);
        }
        let connection = self
            .connection
            .as_mut()
            .ok_or(ReferenceSpoolError::AlreadySealed)?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(ReferenceSpoolError::sqlite)?;
        self.poisoned = true;
        Ok(ReferencePageBatch {
            connection,
            surface,
            request_started_at: self.request.requested_at(),
            request_deadline: self.request.deadline(),
            control: &self.control,
            publication_limits: self.request.limits(),
            max_conflicts: self
                .limits
                .max_conflicts
                .min(u32::try_from(self.request.limits().max_conflicts()).unwrap_or(u32::MAX)),
            rows: 0,
            conflicts: self.aggregate.conflicts,
            object_context: None,
            completed_surfaces: &mut self.completed_surfaces,
            aggregate: &mut self.aggregate,
            database_file: &self.database_file,
            max_database_bytes: self.limits.max_database_bytes,
            poisoned: &mut self.poisoned,
            finished: false,
        })
    }

    /// Seals a complete conflict-free staging database into a content-digested temporary
    /// generation ready for atomic publication.
    ///
    /// # Errors
    ///
    /// Rejects missing surfaces, retained conflicts, schema drift, failed integrity checks, or a
    /// database that exceeds the exact page/file ceiling.
    pub fn seal(mut self) -> Result<ReferenceSpoolSealOutcome, ReferenceSpoolError> {
        ensure_publication_open(&self.control, &self.request)?;
        if self.poisoned
            || self.completed_surfaces.len() != self.request.surfaces().len()
            || self
                .request
                .surfaces()
                .iter()
                .any(|surface| !self.completed_surfaces.contains(surface))
        {
            return Err(ReferenceSpoolError::IncompletePublication);
        }
        let connection = self
            .connection
            .take()
            .ok_or(ReferenceSpoolError::AlreadySealed)?;
        finalize_export_ordinals(&connection, &self.control, &self.request)?;
        validate_sealed_database(
            &connection,
            &self.database_file,
            self.limits,
            &self.request,
            &self.control,
        )?;
        let counts = read_counts(&connection)?;
        if counts.pages() != u64::from(self.aggregate.pages)
            || counts.bytes() != self.aggregate.bytes
            || counts.returned_records() != self.aggregate.records
        {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        let conflicts = read_conflicts(&connection, self.limits.max_conflicts)?;
        drop(connection);
        self.database_file
            .sync_all()
            .map_err(|_| ReferenceSpoolError::StagingIo)?;
        let (database_digest, database_bytes) = hash_regular_file(
            &self.database_file,
            self.limits.max_database_bytes,
            &self.control,
            &self.request,
        )?;
        ensure_publication_open(&self.control, &self.request)?;
        let catalog = PublicationCatalog::from_spool(
            self.request,
            PublicationCompleteness::Complete,
            counts,
            conflicts,
        );
        if catalog.conflicts().is_empty() {
            Ok(ReferenceSpoolSealOutcome::Complete(
                StagedReferenceGeneration {
                    database_file: self.database_file,
                    database_digest,
                    database_bytes,
                    limits: self.limits,
                    catalog,
                    control: self.control,
                    _spool_lock: self._spool_lock,
                },
            ))
        } else {
            Ok(ReferenceSpoolSealOutcome::Rejected(
                RejectedReferenceGeneration {
                    database_file: self.database_file,
                    database_digest,
                    database_bytes,
                    limits: self.limits,
                    catalog,
                    _spool_lock: self._spool_lock,
                },
            ))
        }
    }
}

#[cfg(unix)]
fn available_capability_space(directory: &Dir) -> Result<u64, ReferenceSpoolError> {
    let filesystem =
        rustix::fs::fstatvfs(directory).map_err(|_| ReferenceSpoolError::DiskProbeFailed)?;
    filesystem
        .f_frsize
        .checked_mul(filesystem.f_bavail)
        .ok_or(ReferenceSpoolError::DiskProbeFailed)
}

#[cfg(not(unix))]
fn available_capability_space(_directory: &Dir) -> Result<u64, ReferenceSpoolError> {
    Err(ReferenceSpoolError::CapabilityDatabaseUnavailable)
}

/// One exact source object/page batch with bounded SQLite journal checkpoints.
pub struct ReferencePageBatch<'a> {
    connection: &'a mut Connection,
    surface: ReferenceSurface,
    request_started_at: market_squawk_domain::Timestamp,
    request_deadline: market_squawk_domain::Timestamp,
    control: &'a ReferenceFetchControl,
    publication_limits: crate::PublicationLimits,
    max_conflicts: u32,
    rows: u32,
    conflicts: u32,
    object_context: Option<crate::ReferenceObjectContext>,
    completed_surfaces: &'a mut BTreeSet<ReferenceSurface>,
    aggregate: &'a mut SpoolAggregate,
    database_file: &'a File,
    max_database_bytes: u64,
    poisoned: &'a mut bool,
    finished: bool,
}

impl std::fmt::Debug for ReferencePageBatch<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReferencePageBatch")
            .field("surface", &self.surface)
            .field("rows", &self.rows)
            .finish()
    }
}

impl ReferencePageBatch<'_> {
    /// Streams one Cboe row into disk-backed global mapping and venue-presence indexes.
    ///
    /// # Errors
    ///
    /// Rejects cross-surface rows, row/count overflow, SQLite bounds, or conflict-limit overrun.
    pub fn record_cboe(&mut self, record: &CboeSeriesReference) -> Result<(), ReferenceSpoolError> {
        if record.object_context().surface() != &self.surface {
            return Err(ReferenceSpoolError::InvalidSurfaceState);
        }
        self.admit_object_context(record.object_context())?;
        let symbol = record.cboe_symbol_id().as_str();
        let osi = record.contract().osi().as_str();
        let underlying = record.underlying().as_str();
        let evidence = record.record_id().as_str();

        let by_symbol: Option<(String, String, String)> = self
            .connection
            .query_row(
                "SELECT osi, underlying, first_evidence FROM cboe_contracts WHERE cboe_symbol = ?1",
                [symbol],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(ReferenceSpoolError::sqlite)?;
        let by_osi: Option<(String, String)> = self
            .connection
            .query_row(
                "SELECT cboe_symbol, first_evidence FROM cboe_contracts WHERE osi = ?1",
                [osi],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(ReferenceSpoolError::sqlite)?;
        if let Some((existing_osi, existing_underlying, first_evidence)) = &by_symbol {
            if existing_osi != osi {
                self.record_conflict(
                    CatalogConflictKind::CboeSymbolMapsMultipleOsi,
                    &format!("cboe-symbol:{symbol}"),
                    first_evidence,
                    evidence,
                )?;
            }
            if existing_underlying != underlying {
                self.record_conflict(
                    CatalogConflictKind::CboeSymbolMapsMultipleUnderlying,
                    &format!("cboe-symbol-underlying:{symbol}"),
                    first_evidence,
                    evidence,
                )?;
            }
        }
        if let Some((existing_symbol, first_evidence)) = &by_osi {
            if existing_symbol != symbol {
                self.record_conflict(
                    CatalogConflictKind::CboeOsiMapsMultipleSymbols,
                    &format!("osi:{osi}"),
                    first_evidence,
                    evidence,
                )?;
            }
        }
        if by_symbol.is_none() && by_osi.is_none() {
            self.connection
                .execute(
                    "INSERT INTO cboe_contracts(cboe_symbol, osi, underlying, first_evidence) VALUES (?1, ?2, ?3, ?4)",
                    params![symbol, osi, underlying, evidence],
                )
                .map_err(ReferenceSpoolError::sqlite)?;
        }

        let prior_presence: Option<String> = self
            .connection
            .query_row(
                "SELECT evidence FROM cboe_presence WHERE venue = ?1 AND cboe_symbol = ?2",
                params![record.venue().stable_label(), symbol],
                |row| row.get(0),
            )
            .optional()
            .map_err(ReferenceSpoolError::sqlite)?;
        if let Some(first) = prior_presence {
            self.record_conflict(
                CatalogConflictKind::DuplicateProviderRecord,
                &format!("cboe-presence:{}:{symbol}", record.venue().stable_label()),
                &first,
                evidence,
            )?;
        } else {
            self.connection
                .execute(
                    "INSERT INTO cboe_presence(venue, cboe_symbol, matching_unit, status, object_id, row_number, evidence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    record.venue().stable_label(),
                    symbol,
                    i64::from(record.unit().get()),
                    record.status().stable_label(),
                    record.object_context().object_id().as_str(),
                    i64::from(record.provider_row_number()),
                    evidence,
                ],
                )
                .map_err(ReferenceSpoolError::sqlite)?;
        }
        self.rows = checked_next(self.rows)?;
        self.checkpoint_if_needed()?;
        Ok(())
    }

    /// Streams one OCC DLP product/root row into its exact product identity index.
    pub fn record_occ_product(
        &mut self,
        record: &OccDlpProductReference,
    ) -> Result<(), ReferenceSpoolError> {
        if record.object_context().surface() != &self.surface {
            return Err(ReferenceSpoolError::InvalidSurfaceState);
        }
        self.admit_object_context(record.object_context())?;
        let (position_state, position_value) = encode_position_limit(record.position_limit());
        let exchanges = encode_exchange_codes(record.trading_exchanges())?;
        let prior: Option<String> = self
            .connection
            .query_row(
                "SELECT evidence FROM occ_products WHERE options_symbol = ?1 AND product_type = ?2",
                params![
                    record.options_symbol().as_str(),
                    record.product_type().provider_code()
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(ReferenceSpoolError::sqlite)?;
        if let Some(first) = prior {
            self.record_conflict(
                CatalogConflictKind::DuplicateProviderRecord,
                &format!(
                    "occ-product:{}:{}",
                    record.options_symbol().as_str(),
                    record.product_type().provider_code()
                ),
                &first,
                record.record_id().as_str(),
            )?;
        } else {
            self.connection
                .execute(
                    "INSERT INTO occ_products(options_symbol, product_type, underlying_symbol, symbol_name, exchanges, exchange_state, position_state, position_value, object_id, row_number, evidence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    record.options_symbol().as_str(),
                    record.product_type().provider_code(),
                    record.underlying_symbol().as_str(),
                    record.symbol_name(),
                    exchanges,
                    record.exchange_listing_evidence().stable_label(),
                    position_state,
                    position_value,
                    record.object_context().object_id().as_str(),
                    i64::from(record.provider_row_number()),
                    record.record_id().as_str(),
                ],
                )
                .map_err(ReferenceSpoolError::sqlite)?;
        }
        self.rows = checked_next(self.rows)?;
        self.checkpoint_if_needed()?;
        Ok(())
    }

    /// Reconciles the strict parser count with the exact page receipt and closes the staged page.
    ///
    /// # Errors
    ///
    /// Rejects another surface, rejected rows, nonterminal single-file state, count mismatch,
    /// object conflict, or disk/page exhaustion. Dropping without success permanently poisons the
    /// anonymous staging database, including if earlier bounded checkpoints were committed.
    pub fn finish(mut self, receipt: &ReferencePageReceipt) -> Result<(), ReferenceSpoolError> {
        ensure_publication_open_for_deadline(self.control, self.request_deadline)?;
        if receipt.context().surface() != &self.surface
            || self.object_context.as_ref() != Some(receipt.context())
            || receipt.page_ordinal() != NonZeroU32::MIN
            || receipt.returned_records() != self.rows
            || receipt.rejected_records() != 0
            || !matches!(receipt.terminal_state(), PageTerminalState::Terminal)
            || receipt.context().clocks().received_at() < self.request_started_at
            || receipt.context().clocks().received_at() > self.request_deadline
            || !valid_source_cycle_evidence(receipt.context())
        {
            return Err(ReferenceSpoolError::PageReceiptMismatch);
        }
        let next_pages = self
            .aggregate
            .pages
            .checked_add(1)
            .ok_or(ReferenceSpoolError::CountOverflow)?;
        let next_bytes = self
            .aggregate
            .bytes
            .checked_add(receipt.context().payload_bytes())
            .ok_or(ReferenceSpoolError::CountOverflow)?;
        let next_records = self
            .aggregate
            .records
            .checked_add(u64::from(self.rows))
            .ok_or(ReferenceSpoolError::CountOverflow)?;
        if next_pages > self.publication_limits.max_pages()
            || next_bytes > self.publication_limits.max_total_bytes()
            || next_records > self.publication_limits.max_total_records()
            || usize::try_from(self.conflicts).unwrap_or(usize::MAX)
                > self.publication_limits.max_conflicts()
        {
            return Err(ReferenceSpoolError::PublicationLimitExceeded);
        }
        insert_page(self.connection, receipt)?;
        self.connection
            .execute_batch("COMMIT")
            .map_err(ReferenceSpoolError::sqlite)?;
        validate_database_file(self.database_file, self.max_database_bytes)?;
        self.aggregate.pages = next_pages;
        self.aggregate.bytes = next_bytes;
        self.aggregate.records = next_records;
        self.aggregate.conflicts = self.conflicts;
        self.completed_surfaces.insert(self.surface.clone());
        *self.poisoned = false;
        self.finished = true;
        Ok(())
    }

    fn checkpoint_if_needed(&mut self) -> Result<(), ReferenceSpoolError> {
        if self.rows == 0 || !self.rows.is_multiple_of(CHECKPOINT_ROWS) {
            return Ok(());
        }
        ensure_publication_open_for_deadline(self.control, self.request_deadline)?;
        self.connection
            .execute_batch("COMMIT; BEGIN IMMEDIATE")
            .map_err(ReferenceSpoolError::sqlite)?;
        validate_database_file(self.database_file, self.max_database_bytes).map(|_| ())
    }

    fn admit_object_context(
        &mut self,
        context: &crate::ReferenceObjectContext,
    ) -> Result<(), ReferenceSpoolError> {
        match &self.object_context {
            Some(expected) if expected == context => Ok(()),
            Some(_) => Err(ReferenceSpoolError::PageReceiptMismatch),
            None => {
                insert_object(self.connection, context)?;
                self.object_context = Some(context.clone());
                Ok(())
            }
        }
    }

    fn record_conflict(
        &mut self,
        kind: CatalogConflictKind,
        natural_key: &str,
        first_evidence: &str,
        second_evidence: &str,
    ) -> Result<(), ReferenceSpoolError> {
        if self.conflicts >= self.max_conflicts {
            return Err(ReferenceSpoolError::ConflictLimitExceeded);
        }
        self.connection
            .execute(
                "INSERT INTO conflicts(kind, natural_key, first_evidence, second_evidence) VALUES (?1, ?2, ?3, ?4)",
                params![conflict_label(kind), natural_key, first_evidence, second_evidence],
            )
            .map_err(ReferenceSpoolError::sqlite)?;
        self.conflicts = self
            .conflicts
            .checked_add(1)
            .ok_or(ReferenceSpoolError::CountOverflow)?;
        Ok(())
    }
}

impl Drop for ReferencePageBatch<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.connection.execute_batch("ROLLBACK");
            *self.poisoned = true;
        }
    }
}

/// Closed canonical identity state returned by provider-reference queries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalReferenceIdentityState {
    /// No explicit canonical resolver receipt was supplied; provider aliases remain unresolved.
    Unresolved,
}

/// One venue presence attached to an exact Cboe symbol/OSI mapping.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeVenuePresenceView {
    venue: CboeVenue,
    matching_unit: u16,
    status: CboeSeriesStatus,
    object_id: SourceIdentifier,
    provider_row_number: u32,
    evidence: SourceIdentifier,
}

impl CboeVenuePresenceView {
    pub(crate) fn try_from_spool(
        venue: String,
        matching_unit: i64,
        status: String,
        object_id: String,
        provider_row_number: i64,
        evidence: String,
    ) -> Result<Self, ReferenceSpoolError> {
        let matching_unit = u16::try_from(matching_unit)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ReferenceSpoolError::InvalidSealedDatabase)?;
        let provider_row_number = u32::try_from(provider_row_number)
            .ok()
            .filter(|value| *value >= 2)
            .ok_or(ReferenceSpoolError::InvalidSealedDatabase)?;
        Ok(Self {
            venue: CboeVenue::try_from_stable_label(&venue)?,
            matching_unit,
            status: CboeSeriesStatus::try_from_stable_label(&status)?,
            object_id: source_identifier(&object_id)?,
            provider_row_number,
            evidence: source_identifier(&evidence)?,
        })
    }

    /// Returns the exact Cboe venue.
    pub const fn venue(&self) -> CboeVenue {
        self.venue
    }

    /// Returns source-native matching-engine unit; this is never a contract multiplier.
    pub const fn matching_unit(&self) -> u16 {
        self.matching_unit
    }

    /// Returns venue-specific normal/closing-only state.
    pub const fn status(&self) -> CboeSeriesStatus {
        self.status
    }

    /// Returns the exact retained source object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the source row coordinate.
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    /// Returns the exact retained provider-row evidence identity.
    pub const fn evidence(&self) -> &SourceIdentifier {
        &self.evidence
    }
}

/// Exact provider contract mapping with independently retained underlying alias and venue evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeContractReferenceView {
    cboe_symbol_id: CboeSymbolId,
    contract: OptionContractIdentity,
    underlying: ProviderInstrumentId,
    canonical_identity: CanonicalReferenceIdentityState,
    venues: Vec<CboeVenuePresenceView>,
}

impl CboeContractReferenceView {
    pub(crate) fn try_from_spool(
        cboe_symbol_id: String,
        osi: String,
        underlying: String,
        canonical_identity: CanonicalReferenceIdentityState,
        venues: Vec<CboeVenuePresenceView>,
    ) -> Result<Self, ReferenceSpoolError> {
        if venues.is_empty()
            || venues
                .windows(2)
                .any(|pair| pair[0].venue() >= pair[1].venue())
        {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        Ok(Self {
            cboe_symbol_id: CboeSymbolId::try_from_provider(&cboe_symbol_id)?,
            contract: OptionContractIdentity::try_from_osi(&osi)
                .map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)?,
            underlying: ProviderInstrumentId::try_from(underlying.as_str())
                .map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)?,
            canonical_identity,
            venues,
        })
    }

    /// Returns the six-character Cboe Symbol ID.
    pub const fn cboe_symbol_id(&self) -> &CboeSymbolId {
        &self.cboe_symbol_id
    }

    /// Returns exact OSI terms without inferred century, multiplier, or canonical identity.
    pub const fn contract(&self) -> &OptionContractIdentity {
        &self.contract
    }

    /// Returns the independent provider underlying alias.
    pub const fn underlying(&self) -> &ProviderInstrumentId {
        &self.underlying
    }

    /// Returns explicit canonical identity state.
    pub const fn canonical_identity(&self) -> CanonicalReferenceIdentityState {
        self.canonical_identity
    }

    /// Returns every exact venue presence in deterministic venue order.
    pub fn venues(&self) -> &[CboeVenuePresenceView] {
        &self.venues
    }
}

/// Exact OCC product/root view. It deliberately does not claim an option contract series.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccProductReferenceView {
    options_symbol: ProviderInstrumentId,
    product_type: OccProductType,
    underlying_symbol: ProviderInstrumentId,
    symbol_name: String,
    trading_exchanges: Vec<OccExchangeCode>,
    exchange_listing_evidence: OccExchangeListingEvidence,
    position_limit: OccPositionLimit,
    canonical_identity: CanonicalReferenceIdentityState,
    object_id: SourceIdentifier,
    provider_row_number: u32,
    evidence: SourceIdentifier,
}

impl OccProductReferenceView {
    #[allow(
        clippy::too_many_arguments,
        reason = "the durable OCC row preserves every exact provider field and row lineage"
    )]
    pub(crate) fn try_from_spool(
        options_symbol: String,
        product_type: OccProductType,
        underlying_symbol: String,
        symbol_name: String,
        trading_exchanges: Vec<OccExchangeCode>,
        exchange_listing_evidence: OccExchangeListingEvidence,
        position_limit: OccPositionLimit,
        canonical_identity: CanonicalReferenceIdentityState,
        object_id: String,
        provider_row_number: i64,
        evidence: String,
    ) -> Result<Self, ReferenceSpoolError> {
        let provider_row_number = u32::try_from(provider_row_number)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ReferenceSpoolError::InvalidSealedDatabase)?;
        if symbol_name.is_empty()
            || symbol_name.len() > 512
            || (matches!(
                exchange_listing_evidence,
                OccExchangeListingEvidence::Reported
            ) != !trading_exchanges.is_empty())
        {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        Ok(Self {
            options_symbol: ProviderInstrumentId::try_from(options_symbol.as_str())
                .map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)?,
            product_type,
            underlying_symbol: ProviderInstrumentId::try_from(underlying_symbol.as_str())
                .map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)?,
            symbol_name,
            trading_exchanges,
            exchange_listing_evidence,
            position_limit,
            canonical_identity,
            object_id: source_identifier(&object_id)?,
            provider_row_number,
            evidence: source_identifier(&evidence)?,
        })
    }

    /// Returns the exact OCC product/root symbol.
    pub const fn options_symbol(&self) -> &ProviderInstrumentId {
        &self.options_symbol
    }

    /// Returns the OCC product type.
    pub const fn product_type(&self) -> OccProductType {
        self.product_type
    }

    /// Returns the independent provider underlying alias.
    pub const fn underlying_symbol(&self) -> &ProviderInstrumentId {
        &self.underlying_symbol
    }

    /// Returns source-preserved product name; it is never an identity resolver input.
    pub fn symbol_name(&self) -> &str {
        &self.symbol_name
    }

    /// Returns source exchange codes.
    pub fn trading_exchanges(&self) -> &[OccExchangeCode] {
        &self.trading_exchanges
    }

    /// Returns whether exchange codes were reported or the selected-directory blank sentinel was
    /// retained.
    pub const fn exchange_listing_evidence(&self) -> OccExchangeListingEvidence {
        self.exchange_listing_evidence
    }

    /// Returns qualified position-limit evidence, including documented-scope anomalies.
    pub const fn position_limit(&self) -> OccPositionLimit {
        self.position_limit
    }

    /// Returns explicit canonical identity state.
    pub const fn canonical_identity(&self) -> CanonicalReferenceIdentityState {
        self.canonical_identity
    }

    /// Returns the exact retained source object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the source row coordinate.
    pub const fn provider_row_number(&self) -> u32 {
        self.provider_row_number
    }

    /// Returns the exact retained provider-row evidence identity.
    pub const fn evidence(&self) -> &SourceIdentifier {
        &self.evidence
    }
}

/// Complete temporary generation after SQLite integrity, schema, size, and conflict validation.
pub struct StagedReferenceGeneration {
    database_file: File,
    database_digest: [u8; 32],
    database_bytes: u64,
    limits: ReferenceSpoolLimits,
    catalog: PublicationCatalog,
    control: ReferenceFetchControl,
    _spool_lock: OwnedSpoolLockGuard,
}

/// Sealing disposition: only `Complete` can enter the immutable generation namespace.
#[derive(Debug)]
pub enum ReferenceSpoolSealOutcome {
    /// Complete, conflict-free generation eligible for artifact publication.
    Complete(StagedReferenceGeneration),
    /// Complete source closure rejected for exact retained mapping conflicts.
    Rejected(RejectedReferenceGeneration),
}

/// Bounded rejected generation retained for doctor explanation and quarantine publication.
pub struct RejectedReferenceGeneration {
    database_file: File,
    database_digest: [u8; 32],
    database_bytes: u64,
    limits: ReferenceSpoolLimits,
    catalog: PublicationCatalog,
    _spool_lock: OwnedSpoolLockGuard,
}

impl std::fmt::Debug for RejectedReferenceGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RejectedReferenceGeneration")
            .field("database_digest", &self.database_digest)
            .field("database_bytes", &self.database_bytes)
            .field("conflicts", &self.catalog.conflicts().len())
            .finish_non_exhaustive()
    }
}

impl RejectedReferenceGeneration {
    /// Returns the complete but conflict-bearing catalog evidence.
    pub const fn catalog(&self) -> &PublicationCatalog {
        &self.catalog
    }

    /// Returns SHA-256 of the exact rejected SQLite evidence object.
    pub const fn database_digest(&self) -> [u8; 32] {
        self.database_digest
    }

    /// Returns exact rejected evidence bytes.
    pub const fn database_bytes(&self) -> u64 {
        self.database_bytes
    }

    /// Returns code-owned limits retained for safe diagnostic publication.
    pub const fn limits(&self) -> ReferenceSpoolLimits {
        self.limits
    }

    pub(crate) fn try_clone_database_file(&self) -> Result<File, ReferenceSpoolError> {
        self.database_file
            .try_clone()
            .map_err(|_| ReferenceSpoolError::StagingIo)
    }
}

impl std::fmt::Debug for StagedReferenceGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StagedReferenceGeneration")
            .field("database_digest", &self.database_digest)
            .field("database_bytes", &self.database_bytes)
            .field("catalog", &self.catalog)
            .finish_non_exhaustive()
    }
}

impl StagedReferenceGeneration {
    /// Returns the complete publication catalog.
    pub const fn catalog(&self) -> &PublicationCatalog {
        &self.catalog
    }

    /// Returns SHA-256 of the exact sealed SQLite generation.
    pub const fn database_digest(&self) -> [u8; 32] {
        self.database_digest
    }

    /// Returns exact sealed database bytes.
    pub const fn database_bytes(&self) -> u64 {
        self.database_bytes
    }

    /// Returns the limits embedded in the generation receipt.
    pub const fn limits(&self) -> ReferenceSpoolLimits {
        self.limits
    }

    pub(crate) fn ensure_publication_open(&self) -> Result<(), ReferenceSpoolError> {
        ensure_publication_open(&self.control, self.catalog.request())
    }

    pub(crate) fn try_clone_database_file(&self) -> Result<File, ReferenceSpoolError> {
        self.database_file
            .try_clone()
            .map_err(|_| ReferenceSpoolError::StagingIo)
    }
}

fn configure_new_database(
    connection: &Connection,
    limits: ReferenceSpoolLimits,
    request: &PublicationRequest,
) -> Result<(), ReferenceSpoolError> {
    let page_count = limits.max_database_bytes / SQLITE_PAGE_BYTES;
    let cache_kib = limits.sqlite_cache_bytes / 1_024;
    connection
        .execute_batch(&format!(
            "PRAGMA page_size={SQLITE_PAGE_BYTES};\
             PRAGMA journal_mode=MEMORY;\
             PRAGMA synchronous=FULL;\
             PRAGMA fullfsync=ON;\
             PRAGMA temp_store=MEMORY;\
             PRAGMA cache_size=-{cache_kib};\
             PRAGMA mmap_size=0;\
             PRAGMA foreign_keys=ON;\
             PRAGMA trusted_schema=OFF;\
             PRAGMA secure_delete=ON;\
             PRAGMA max_page_count={page_count};\
             PRAGMA user_version={SPOOL_SCHEMA_VERSION};\
             CREATE TABLE generation_metadata(\
               singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema_identity TEXT NOT NULL, schema_digest BLOB NOT NULL CHECK(length(schema_digest)=32),\
               request_id TEXT NOT NULL, requested_at INTEGER NOT NULL, deadline INTEGER NOT NULL CHECK(deadline>requested_at),\
               surfaces_json TEXT NOT NULL,\
               max_surfaces INTEGER NOT NULL, max_pages INTEGER NOT NULL, max_total_bytes INTEGER NOT NULL, max_total_records INTEGER NOT NULL,\
               max_conflicts INTEGER NOT NULL, max_database_bytes INTEGER NOT NULL, sqlite_cache_bytes INTEGER NOT NULL,\
               max_retained_conflicts INTEGER NOT NULL, max_query_rows INTEGER NOT NULL\
             ) STRICT;\
             CREATE TABLE objects(\
               surface TEXT NOT NULL, object_id TEXT PRIMARY KEY, provider TEXT NOT NULL CHECK(provider IN ('occ','cboe')),\
               configured_locator TEXT NOT NULL, final_locator TEXT NOT NULL, media_type TEXT NOT NULL, native_schema TEXT NOT NULL,\
               request_digest BLOB NOT NULL CHECK(length(request_digest)=32), receipt_digest BLOB NOT NULL CHECK(length(receipt_digest)=32),\
               http_status INTEGER NOT NULL CHECK(http_status=200), redirect_chain_json TEXT NOT NULL, observed_content_type TEXT NOT NULL,\
               observed_content_disposition TEXT, declared_content_length INTEGER CHECK(declared_content_length>0), cache_etag TEXT, cache_last_modified TEXT,\
               body_complete INTEGER NOT NULL CHECK(body_complete=1),\
               payload_digest BLOB NOT NULL CHECK(length(payload_digest)=32),\
               payload_bytes INTEGER NOT NULL CHECK(payload_bytes>0), received_at INTEGER NOT NULL, transport_elapsed_nanos INTEGER NOT NULL CHECK(transport_elapsed_nanos>0),\
               clocks_json TEXT NOT NULL, source_publication_date TEXT, source_filename TEXT, http_last_modified TEXT,\
               transport_json TEXT NOT NULL CHECK(length(transport_json)>0 AND length(transport_json)<=65536),\
               UNIQUE(surface,object_id)\
             ) STRICT;\
             CREATE TABLE pages(\
               surface TEXT NOT NULL, page_ordinal INTEGER NOT NULL CHECK(page_ordinal=1), object_id TEXT NOT NULL,\
               returned_records INTEGER NOT NULL CHECK(returned_records>=0), rejected_records INTEGER NOT NULL CHECK(rejected_records=0),\
               terminal_state TEXT NOT NULL CHECK(terminal_state='terminal'), PRIMARY KEY(surface,page_ordinal),\
               FOREIGN KEY(surface,object_id) REFERENCES objects(surface,object_id) DEFERRABLE INITIALLY DEFERRED\
             ) STRICT, WITHOUT ROWID;\
             CREATE TABLE cboe_contracts(\
               cboe_symbol TEXT PRIMARY KEY, osi TEXT NOT NULL UNIQUE, underlying TEXT NOT NULL, first_evidence TEXT NOT NULL\
             ) STRICT, WITHOUT ROWID;\
             CREATE TABLE cboe_presence(\
               venue TEXT NOT NULL, cboe_symbol TEXT NOT NULL, matching_unit INTEGER NOT NULL CHECK(matching_unit>0),\
               status TEXT NOT NULL CHECK(status IN ('normal','closing_only')), object_id TEXT NOT NULL REFERENCES objects(object_id) DEFERRABLE INITIALLY DEFERRED,\
               row_number INTEGER NOT NULL CHECK(row_number>1), evidence TEXT NOT NULL,\
               CHECK(venue IN ('c1','bzx','c2','edgx')), CHECK(matching_unit<=65535), PRIMARY KEY(venue,cboe_symbol)\
             ) STRICT, WITHOUT ROWID;\
             CREATE INDEX cboe_presence_symbol ON cboe_presence(cboe_symbol,venue);\
             CREATE TABLE occ_products(\
               options_symbol TEXT NOT NULL, product_type TEXT NOT NULL, underlying_symbol TEXT NOT NULL, symbol_name TEXT NOT NULL,\
               exchanges TEXT NOT NULL, exchange_state TEXT NOT NULL CHECK(exchange_state IN ('reported','not_reported_in_selected_directory')),\
               position_state TEXT NOT NULL CHECK(position_state IN ('equity_reported','non_equity_unavailable_zero','non_equity_provider_value_outside_documented_scope')),\
               position_value TEXT NOT NULL, object_id TEXT NOT NULL REFERENCES objects(object_id) DEFERRABLE INITIALLY DEFERRED,\
               row_number INTEGER NOT NULL CHECK(row_number>0), evidence TEXT NOT NULL, PRIMARY KEY(options_symbol,product_type),\
               CHECK((exchange_state='reported' AND length(exchanges)>0) OR (exchange_state='not_reported_in_selected_directory' AND exchanges='')),\
               CHECK((position_state='non_equity_unavailable_zero' AND position_value='0') OR (position_state!='non_equity_unavailable_zero' AND position_value!='0'))\
             ) STRICT, WITHOUT ROWID;\
             CREATE TABLE cboe_export(\
               export_ordinal INTEGER PRIMARY KEY CHECK(export_ordinal>0), cboe_symbol TEXT NOT NULL UNIQUE REFERENCES cboe_contracts(cboe_symbol) DEFERRABLE INITIALLY DEFERRED\
             ) STRICT, WITHOUT ROWID;\
             CREATE TABLE occ_export(\
               export_ordinal INTEGER PRIMARY KEY CHECK(export_ordinal>0), options_symbol TEXT NOT NULL, product_type TEXT NOT NULL,\
               UNIQUE(options_symbol,product_type), FOREIGN KEY(options_symbol,product_type) REFERENCES occ_products(options_symbol,product_type) DEFERRABLE INITIALLY DEFERRED\
             ) STRICT, WITHOUT ROWID;\
             CREATE TABLE conflicts(\
               conflict_id INTEGER PRIMARY KEY, kind TEXT NOT NULL, natural_key TEXT NOT NULL, first_evidence TEXT NOT NULL, second_evidence TEXT NOT NULL\
             ) STRICT;"
        ))
        .map_err(ReferenceSpoolError::sqlite)?;
    let schema_digest = sqlite_schema_digest(connection)?;
    let surfaces_json = serde_json::to_string(request.surfaces())
        .map_err(|_| ReferenceSpoolError::EncodingFailed)?;
    connection
        .execute(
            "INSERT INTO generation_metadata(singleton,schema_identity,schema_digest,request_id,requested_at,deadline,surfaces_json,max_surfaces,max_pages,max_total_bytes,max_total_records,max_conflicts,max_database_bytes,sqlite_cache_bytes,max_retained_conflicts,max_query_rows) VALUES (1,'market-squawk-options-reference-v6',?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                schema_digest.as_slice(),
                request.request_id().as_str(),
                request.requested_at().unix_nanos(),
                request.deadline().unix_nanos(),
                surfaces_json,
                i64::try_from(request.limits().max_surfaces()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::from(request.limits().max_pages()),
                i64::try_from(request.limits().max_total_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(request.limits().max_total_records()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(request.limits().max_conflicts()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(limits.max_database_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(limits.sqlite_cache_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::from(limits.max_conflicts()),
                i64::from(limits.max_query_rows().get()),
            ],
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    Ok(())
}

fn insert_object(
    connection: &Connection,
    context: &crate::ReferenceObjectContext,
) -> Result<(), ReferenceSpoolError> {
    let transport = context.transport_evidence();
    let surface = surface_key(context.surface())?;
    let digest = context.payload_digest().bytes();
    let publication_date = context
        .source_publication_date()
        .map(|date| date.to_string());
    let clocks_json =
        serde_json::to_string(context.clocks()).map_err(|_| ReferenceSpoolError::EncodingFailed)?;
    let transport_json =
        serde_json::to_string(transport).map_err(|_| ReferenceSpoolError::EncodingFailed)?;
    if transport_json.len() > 64 * 1024 {
        return Err(ReferenceSpoolError::EncodingFailed);
    }
    let provider = match context.provider() {
        crate::ReferenceProvider::Occ => "occ",
        crate::ReferenceProvider::Cboe => "cboe",
    };
    let redirect_chain_json = serde_json::to_string(transport.redirect_chain())
        .map_err(|_| ReferenceSpoolError::EncodingFailed)?;
    let declared_content_length = transport
        .declared_content_length()
        .map(i64::try_from)
        .transpose()
        .map_err(|_| ReferenceSpoolError::CountOverflow)?;
    connection
        .execute(
            "INSERT INTO objects(surface,object_id,provider,configured_locator,final_locator,media_type,native_schema,request_digest,receipt_digest,http_status,redirect_chain_json,observed_content_type,observed_content_disposition,declared_content_length,cache_etag,cache_last_modified,body_complete,payload_digest,payload_bytes,received_at,transport_elapsed_nanos,clocks_json,source_publication_date,source_filename,http_last_modified,transport_json) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26)",
            params![
                surface,
                context.object_id().as_str(),
                provider,
                context.configured_locator().as_str(),
                context.final_locator().as_str(),
                context.media_type().as_str(),
                context.native_schema().as_str(),
                transport.request_digest().bytes().as_slice(),
                transport.receipt_digest().bytes().as_slice(),
                i64::from(transport.status()),
                redirect_chain_json,
                transport.observed_content_type(),
                transport.observed_content_disposition(),
                declared_content_length,
                transport.etag(),
                transport.cache_last_modified(),
                i64::from(transport.body_complete()),
                digest.as_slice(),
                i64::try_from(context.payload_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                context.clocks().received_at().unix_nanos(),
                i64::try_from(context.clocks().transport_elapsed_nanos()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                clocks_json,
                publication_date,
                context.source_filename().map(SourceIdentifier::as_str),
                context.http_last_modified().map(|value| value.as_str()),
                transport_json,
            ],
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    Ok(())
}

fn finalize_export_ordinals(
    connection: &Connection,
    control: &ReferenceFetchControl,
    request: &PublicationRequest,
) -> Result<(), ReferenceSpoolError> {
    ensure_publication_open(control, request)?;
    let progress_control = control.clone();
    let progress_deadline = request.deadline();
    connection
        .progress_handler(
            10_000,
            Some(move || {
                ensure_publication_open_for_deadline(&progress_control, progress_deadline).is_err()
            }),
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    let result = connection
        .execute_batch(
            "BEGIN IMMEDIATE;\
             INSERT INTO cboe_export(export_ordinal,cboe_symbol) SELECT ROW_NUMBER() OVER (ORDER BY cboe_symbol COLLATE BINARY),cboe_symbol FROM cboe_contracts ORDER BY cboe_symbol COLLATE BINARY;\
             INSERT INTO occ_export(export_ordinal,options_symbol,product_type) SELECT ROW_NUMBER() OVER (ORDER BY options_symbol COLLATE BINARY,product_type COLLATE BINARY),options_symbol,product_type FROM occ_products ORDER BY options_symbol COLLATE BINARY,product_type COLLATE BINARY;\
             COMMIT;",
        )
        .map_err(ReferenceSpoolError::sqlite);
    if result.is_err() {
        let _ = connection.execute_batch("ROLLBACK");
    }
    connection
        .progress_handler::<fn() -> bool>(0, None)
        .map_err(ReferenceSpoolError::sqlite)?;
    result?;
    ensure_publication_open(control, request)?;
    let (cboe_contracts, cboe_export, occ_products, occ_export): (u64, u64, u64, u64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM cboe_contracts),(SELECT COUNT(*) FROM cboe_export),(SELECT COUNT(*) FROM occ_products),(SELECT COUNT(*) FROM occ_export)",
            [],
            |row| {
                Ok((
                    sqlite_u64(row, 0)?,
                    sqlite_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_u64(row, 3)?,
                ))
            },
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    if cboe_contracts == 0
        || occ_products == 0
        || cboe_contracts != cboe_export
        || occ_products != occ_export
    {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    validate_export_ordinals(connection, cboe_export, occ_export)?;
    Ok(())
}

fn validate_export_ordinals(
    connection: &Connection,
    cboe_count: u64,
    occ_count: u64,
) -> Result<(), ReferenceSpoolError> {
    let (cboe_min, cboe_max, cboe_misordered, occ_min, occ_max, occ_misordered): (
        Option<u64>,
        Option<u64>,
        u64,
        Option<u64>,
        Option<u64>,
        u64,
    ) = connection
        .query_row(
            "SELECT (SELECT MIN(export_ordinal) FROM cboe_export),(SELECT MAX(export_ordinal) FROM cboe_export),(SELECT COUNT(*) FROM (SELECT export_ordinal,ROW_NUMBER() OVER (ORDER BY cboe_symbol COLLATE BINARY) expected_ordinal FROM cboe_export) WHERE export_ordinal<>expected_ordinal),(SELECT MIN(export_ordinal) FROM occ_export),(SELECT MAX(export_ordinal) FROM occ_export),(SELECT COUNT(*) FROM (SELECT export_ordinal,ROW_NUMBER() OVER (ORDER BY options_symbol COLLATE BINARY,product_type COLLATE BINARY) expected_ordinal FROM occ_export) WHERE export_ordinal<>expected_ordinal)",
            [],
            |row| {
                Ok((
                    sqlite_optional_u64(row, 0)?,
                    sqlite_optional_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                    sqlite_optional_u64(row, 3)?,
                    sqlite_optional_u64(row, 4)?,
                    sqlite_u64(row, 5)?,
                ))
            },
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    if cboe_min != Some(1)
        || cboe_max != Some(cboe_count)
        || cboe_misordered != 0
        || occ_min != Some(1)
        || occ_max != Some(occ_count)
        || occ_misordered != 0
    {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    Ok(())
}

fn insert_page(
    connection: &Connection,
    receipt: &ReferencePageReceipt,
) -> Result<(), ReferenceSpoolError> {
    let context = receipt.context();
    let surface = surface_key(context.surface())?;
    connection
        .execute(
            "INSERT INTO pages(surface,page_ordinal,object_id,returned_records,rejected_records,terminal_state) VALUES (?1,?2,?3,?4,0,'terminal')",
            params![
                surface,
                i64::from(receipt.page_ordinal().get()),
                context.object_id().as_str(),
                i64::from(receipt.returned_records()),
            ],
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    Ok(())
}

fn validate_sealed_database(
    connection: &Connection,
    database_file: &File,
    limits: ReferenceSpoolLimits,
    request: &PublicationRequest,
    control: &ReferenceFetchControl,
) -> Result<(), ReferenceSpoolError> {
    ensure_publication_open(control, request)?;
    let progress_control = control.clone();
    let progress_deadline = request.deadline();
    connection
        .progress_handler(
            10_000,
            Some(move || {
                ensure_publication_open_for_deadline(&progress_control, progress_deadline).is_err()
            }),
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    let result = (|| {
        let user_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let page_size: u64 = connection
            .pragma_query_value(None, "page_size", |row| sqlite_u64(row, 0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let page_count: u64 = connection
            .pragma_query_value(None, "page_count", |row| sqlite_u64(row, 0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let integrity: String = connection
            .pragma_query_value(None, "integrity_check", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let foreign_key_failures: u32 = connection
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .map_err(ReferenceSpoolError::sqlite)?;
        let max_page_count: u64 = connection
            .pragma_query_value(None, "max_page_count", |row| sqlite_u64(row, 0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let mmap_size: u64 = connection
            .pragma_query_value(None, "mmap_size", |row| sqlite_u64(row, 0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let foreign_keys: u32 = connection
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let temp_store: u32 = connection
            .pragma_query_value(None, "temp_store", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let cache_size: i64 = connection
            .pragma_query_value(None, "cache_size", |row| row.get(0))
            .map_err(ReferenceSpoolError::sqlite)?;
        let expected_cache = -i64::try_from(limits.sqlite_cache_bytes / 1_024)
            .map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)?;
        let expected_max_pages = limits.max_database_bytes / SQLITE_PAGE_BYTES;
        if user_version != SPOOL_SCHEMA_VERSION
            || page_size != SQLITE_PAGE_BYTES
            || page_count.saturating_mul(page_size) > limits.max_database_bytes
            || max_page_count != expected_max_pages
            || mmap_size != 0
            || foreign_keys != 1
            || journal_mode != "memory"
            || temp_store != 2
            || cache_size != expected_cache
            || integrity != "ok"
            || foreign_key_failures != 0
        {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        validate_schema_and_metadata(connection, limits, request)?;
        let (page_total, object_total): (u64, u64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM pages),(SELECT COUNT(*) FROM objects)",
                [],
                |row| Ok((sqlite_u64(row, 0)?, sqlite_u64(row, 1)?)),
            )
            .map_err(ReferenceSpoolError::sqlite)?;
        if page_total != request.surfaces().len() as u64 || object_total != page_total {
            return Err(ReferenceSpoolError::IncompletePublication);
        }
        for surface in request.surfaces() {
            let count: u32 = connection
                .query_row(
                    "SELECT COUNT(*) FROM pages WHERE surface=?1",
                    [surface_key(surface)?],
                    |row| row.get(0),
                )
                .map_err(ReferenceSpoolError::sqlite)?;
            if count != 1 {
                return Err(ReferenceSpoolError::IncompletePublication);
            }
            let surface = surface_key(surface)?;
            let (returned, observed): (u64, u64) = connection
            .query_row(
                "SELECT p.returned_records, CASE WHEN ?1 LIKE 'cboe_all_series:%' THEN (SELECT COUNT(*) FROM cboe_presence c WHERE c.object_id=p.object_id) WHEN ?1 LIKE 'occ_dlp_%' THEN (SELECT COUNT(*) FROM occ_products o WHERE o.object_id=p.object_id) ELSE 0 END FROM pages p WHERE p.surface=?1",
                [surface],
                |row| Ok((sqlite_u64(row, 0)?, sqlite_u64(row, 1)?)),
            )
            .map_err(ReferenceSpoolError::sqlite)?;
            let conflicts: u64 = connection
                .query_row("SELECT COUNT(*) FROM conflicts", [], |row| {
                    sqlite_u64(row, 0)
                })
                .map_err(ReferenceSpoolError::sqlite)?;
            if conflicts == 0 && returned != observed {
                return Err(ReferenceSpoolError::InvalidSealedDatabase);
            }
        }
        let file_bytes = database_file
            .metadata()
            .map_err(|_| ReferenceSpoolError::StagingIo)?
            .len();
        if file_bytes != page_count.saturating_mul(page_size) {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        Ok(())
    })();
    connection
        .progress_handler::<fn() -> bool>(0, None)
        .map_err(ReferenceSpoolError::sqlite)?;
    ensure_publication_open(control, request)?;
    result
}

fn validate_schema_and_metadata(
    connection: &Connection,
    limits: ReferenceSpoolLimits,
    request: &PublicationRequest,
) -> Result<(), ReferenceSpoolError> {
    let stored_digest: Vec<u8> = connection
        .query_row(
            "SELECT schema_digest FROM generation_metadata WHERE singleton=1 AND schema_identity='market-squawk-options-reference-v6'",
            [],
            |row| row.get(0),
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    if stored_digest.as_slice() != sqlite_schema_digest(connection)? {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    let surfaces_json = serde_json::to_string(request.surfaces())
        .map_err(|_| ReferenceSpoolError::EncodingFailed)?;
    let matched: u32 = connection
        .query_row(
            "SELECT COUNT(*) FROM generation_metadata WHERE singleton=1 AND request_id=?1 AND requested_at=?2 AND deadline=?3 AND surfaces_json=?4 AND max_surfaces=?5 AND max_pages=?6 AND max_total_bytes=?7 AND max_total_records=?8 AND max_conflicts=?9 AND max_database_bytes=?10 AND sqlite_cache_bytes=?11 AND max_retained_conflicts=?12 AND max_query_rows=?13",
            params![
                request.request_id().as_str(),
                request.requested_at().unix_nanos(),
                request.deadline().unix_nanos(),
                surfaces_json,
                i64::try_from(request.limits().max_surfaces()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::from(request.limits().max_pages()),
                i64::try_from(request.limits().max_total_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(request.limits().max_total_records()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(request.limits().max_conflicts()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(limits.max_database_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::try_from(limits.sqlite_cache_bytes()).map_err(|_| ReferenceSpoolError::CountOverflow)?,
                i64::from(limits.max_conflicts()),
                i64::from(limits.max_query_rows().get()),
            ],
            |row| row.get(0),
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    if matched != 1 {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    Ok(())
}

fn read_counts(connection: &Connection) -> Result<CatalogCounts, ReferenceSpoolError> {
    let (pages, bytes, returned): (u64, u64, u64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(payload_bytes),0), COALESCE((SELECT SUM(returned_records) FROM pages),0) FROM objects",
            [],
            |row| {
                Ok((
                    sqlite_u64(row, 0)?,
                    sqlite_u64(row, 1)?,
                    sqlite_u64(row, 2)?,
                ))
            },
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    let cboe: u64 = connection
        .query_row("SELECT COUNT(*) FROM cboe_presence", [], |row| {
            sqlite_u64(row, 0)
        })
        .map_err(ReferenceSpoolError::sqlite)?;
    let occ: u64 = connection
        .query_row("SELECT COUNT(*) FROM occ_products", [], |row| {
            sqlite_u64(row, 0)
        })
        .map_err(ReferenceSpoolError::sqlite)?;
    Ok(CatalogCounts::from_spool(pages, bytes, returned, cboe, occ))
}

fn read_conflicts(
    connection: &Connection,
    maximum: u32,
) -> Result<Vec<CatalogConflict>, ReferenceSpoolError> {
    let mut statement = connection
        .prepare(
            "SELECT kind,natural_key,first_evidence,second_evidence FROM conflicts ORDER BY conflict_id LIMIT ?1",
        )
        .map_err(ReferenceSpoolError::sqlite)?;
    let rows = statement
        .query_map([i64::from(maximum)], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(ReferenceSpoolError::sqlite)?;
    let count: u32 = connection
        .query_row("SELECT COUNT(*) FROM conflicts", [], |row| row.get(0))
        .map_err(ReferenceSpoolError::sqlite)?;
    if count > maximum {
        return Err(ReferenceSpoolError::ConflictLimitExceeded);
    }
    let mut conflicts = Vec::new();
    conflicts
        .try_reserve(usize::try_from(count).unwrap_or(0))
        .map_err(|_| ReferenceSpoolError::CapacityUnavailable)?;
    for row in rows {
        let (kind, natural, first, second) = row.map_err(ReferenceSpoolError::sqlite)?;
        conflicts.push(CatalogConflict::new(
            parse_conflict_label(&kind)?,
            source_identifier(&natural)?,
            source_identifier(&first)?,
            source_identifier(&second)?,
        ));
    }
    Ok(conflicts)
}

fn encode_position_limit(value: OccPositionLimit) -> (&'static str, String) {
    match value {
        OccPositionLimit::EquityReported(value) => ("equity_reported", value.get().to_string()),
        OccPositionLimit::NonEquityUnavailableZero => {
            ("non_equity_unavailable_zero", "0".to_owned())
        }
        OccPositionLimit::NonEquityProviderValueOutsideDocumentedScope { raw_value } => (
            "non_equity_provider_value_outside_documented_scope",
            raw_value.get().to_string(),
        ),
    }
}

fn encode_exchange_codes(values: &[OccExchangeCode]) -> Result<String, ReferenceSpoolError> {
    let bytes: Vec<u8> = values.iter().map(|value| value.provider_byte()).collect();
    String::from_utf8(bytes).map_err(|_| ReferenceSpoolError::EncodingFailed)
}

fn surface_key(surface: &ReferenceSurface) -> Result<String, ReferenceSpoolError> {
    let value = match surface {
        ReferenceSurface::CboeAllSeries { venue } => {
            format!("cboe_all_series:{}", venue.stable_label())
        }
        ReferenceSurface::OccDlpSelectedText => "occ_dlp_selected_text".to_owned(),
        ReferenceSurface::OccDlpDailyText => "occ_dlp_daily_text".to_owned(),
        ReferenceSurface::OccDlpDailyXml => "occ_dlp_daily_xml".to_owned(),
        ReferenceSurface::OccMemoIndexCsv => "occ_memo_index_csv".to_owned(),
        ReferenceSurface::OccMemoIndexJson => "occ_memo_index_json".to_owned(),
        ReferenceSurface::OccMemoDocument { memo_number } => {
            format!("occ_memo_document:{memo_number}")
        }
        ReferenceSurface::OccMemoAttachment {
            memo_number,
            ordinal,
        } => format!("occ_memo_attachment:{memo_number}:{}", ordinal.get()),
    };
    if value.len() > 128 {
        Err(ReferenceSpoolError::EncodingFailed)
    } else {
        Ok(value)
    }
}

fn conflict_label(kind: CatalogConflictKind) -> &'static str {
    match kind {
        CatalogConflictKind::CboeSymbolMapsMultipleOsi => "cboe_symbol_maps_multiple_osi",
        CatalogConflictKind::CboeOsiMapsMultipleSymbols => "cboe_osi_maps_multiple_symbols",
        CatalogConflictKind::CboeSymbolMapsMultipleUnderlying => {
            "cboe_symbol_maps_multiple_underlying"
        }
        CatalogConflictKind::PageCoordinateDivergence => "page_coordinate_divergence",
        CatalogConflictKind::DuplicateProviderRecord => "duplicate_provider_record",
    }
}

fn parse_conflict_label(value: &str) -> Result<CatalogConflictKind, ReferenceSpoolError> {
    match value {
        "cboe_symbol_maps_multiple_osi" => Ok(CatalogConflictKind::CboeSymbolMapsMultipleOsi),
        "cboe_osi_maps_multiple_symbols" => Ok(CatalogConflictKind::CboeOsiMapsMultipleSymbols),
        "cboe_symbol_maps_multiple_underlying" => {
            Ok(CatalogConflictKind::CboeSymbolMapsMultipleUnderlying)
        }
        "page_coordinate_divergence" => Ok(CatalogConflictKind::PageCoordinateDivergence),
        "duplicate_provider_record" => Ok(CatalogConflictKind::DuplicateProviderRecord),
        _ => Err(ReferenceSpoolError::InvalidSealedDatabase),
    }
}

fn checked_next(value: u32) -> Result<u32, ReferenceSpoolError> {
    value
        .checked_add(1)
        .ok_or(ReferenceSpoolError::CountOverflow)
}

fn validate_spool_request(request: &PublicationRequest) -> Result<(), ReferenceSpoolError> {
    const CORE_SURFACES: usize = 5;
    if request.surfaces().len() != CORE_SURFACES {
        return Err(ReferenceSpoolError::InvalidPublicationCycle);
    }
    if request.surfaces().len()
        > usize::try_from(request.limits().max_pages()).unwrap_or(usize::MAX)
    {
        return Err(ReferenceSpoolError::PublicationLimitExceeded);
    }
    let mut cboe_venues = BTreeSet::new();
    let mut dlp_surfaces = 0_usize;
    for surface in request.surfaces() {
        match surface {
            ReferenceSurface::CboeAllSeries { venue } => {
                cboe_venues.insert(*venue);
            }
            ReferenceSurface::OccDlpSelectedText
            | ReferenceSurface::OccDlpDailyText
            | ReferenceSurface::OccDlpDailyXml => {
                dlp_surfaces = dlp_surfaces
                    .checked_add(1)
                    .ok_or(ReferenceSpoolError::CountOverflow)?;
            }
            ReferenceSurface::OccMemoIndexCsv
            | ReferenceSurface::OccMemoIndexJson
            | ReferenceSurface::OccMemoDocument { .. }
            | ReferenceSurface::OccMemoAttachment { .. } => {
                return Err(ReferenceSpoolError::UnsupportedPublicationSurface);
            }
        }
    }
    if dlp_surfaces != 1
        || cboe_venues
            != [
                CboeVenue::C1,
                CboeVenue::Bzx,
                CboeVenue::C2,
                CboeVenue::Edgx,
            ]
            .into_iter()
            .collect()
    {
        return Err(ReferenceSpoolError::InvalidPublicationCycle);
    }
    Ok(())
}

fn ensure_publication_open(
    control: &ReferenceFetchControl,
    request: &PublicationRequest,
) -> Result<(), ReferenceSpoolError> {
    ensure_publication_open_for_deadline(control, request.deadline())
}

fn ensure_publication_open_for_deadline(
    control: &ReferenceFetchControl,
    wall_deadline: Timestamp,
) -> Result<(), ReferenceSpoolError> {
    control.ensure_open().map_err(|error| match error {
        ReferenceTransportError::Cancelled => ReferenceSpoolError::PublicationCancelled,
        ReferenceTransportError::DeadlineExceeded => {
            ReferenceSpoolError::PublicationDeadlineExceeded
        }
        _ => ReferenceSpoolError::PublicationControlUnavailable,
    })?;
    let now = crate::transport::trusted_timestamp()
        .map_err(|_| ReferenceSpoolError::PublicationControlUnavailable)?;
    if now >= wall_deadline {
        Err(ReferenceSpoolError::PublicationDeadlineExceeded)
    } else {
        Ok(())
    }
}

fn valid_source_cycle_evidence(context: &crate::ReferenceObjectContext) -> bool {
    match context.surface() {
        ReferenceSurface::CboeAllSeries { .. }
        | ReferenceSurface::OccDlpDailyText
        | ReferenceSurface::OccDlpDailyXml => {
            context.source_filename().is_some()
                && context.source_publication_date().is_some()
                && context.http_last_modified().is_some()
        }
        ReferenceSurface::OccDlpSelectedText => {
            context.source_filename().is_some()
                && context.source_publication_date().is_none()
                && context.http_last_modified().is_some()
        }
        ReferenceSurface::OccMemoIndexCsv
        | ReferenceSurface::OccMemoIndexJson
        | ReferenceSurface::OccMemoDocument { .. }
        | ReferenceSurface::OccMemoAttachment { .. } => false,
    }
}

fn sqlite_schema_digest(connection: &Connection) -> Result<[u8; 32], ReferenceSpoolError> {
    use sha2::{Digest, Sha256};

    let mut statement = connection
        .prepare("SELECT type,name,sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY type,name")
        .map_err(ReferenceSpoolError::sqlite)?;
    let mut rows = statement.query([]).map_err(ReferenceSpoolError::sqlite)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk:options-reference-sqlite-schema:v4\0");
    let mut count = 0_u32;
    while let Some(row) = rows.next().map_err(ReferenceSpoolError::sqlite)? {
        count = count
            .checked_add(1)
            .ok_or(ReferenceSpoolError::InvalidSealedDatabase)?;
        if count > 10 {
            return Err(ReferenceSpoolError::InvalidSealedDatabase);
        }
        for value in [
            row.get::<_, String>(0)
                .map_err(ReferenceSpoolError::sqlite)?,
            row.get::<_, String>(1)
                .map_err(ReferenceSpoolError::sqlite)?,
            row.get::<_, String>(2)
                .map_err(ReferenceSpoolError::sqlite)?,
        ] {
            digest.update(
                u64::try_from(value.len())
                    .map_err(|_| ReferenceSpoolError::CountOverflow)?
                    .to_be_bytes(),
            );
            digest.update(value.as_bytes());
        }
    }
    if count != 10 {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    Ok(digest.finalize().into())
}

fn source_identifier(value: &str) -> Result<SourceIdentifier, ReferenceSpoolError> {
    SourceIdentifier::try_from(value).map_err(|_| ReferenceSpoolError::InvalidSealedDatabase)
}

fn validate_capability_directory(directory: &Dir) -> Result<(), ReferenceSpoolError> {
    let metadata = directory
        .metadata(".")
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    let direct = directory
        .symlink_metadata(".")
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    if !metadata.is_dir() || !direct.is_dir() {
        Err(ReferenceSpoolError::UnsafeStagingDirectory)
    } else {
        Ok(())
    }
}

fn acquire_spool_lock(directory: &Dir) -> Result<OwnedSpoolLockGuard, ReferenceSpoolError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let file = directory
        .open_with(SPOOL_LOCK_FILE, &options)
        .map_err(|_| ReferenceSpoolError::StagingIo)?
        .into_std();
    let metadata = file
        .metadata()
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    let direct = directory
        .symlink_metadata(SPOOL_LOCK_FILE)
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    if !metadata.is_file() || !direct.is_file() {
        return Err(ReferenceSpoolError::UnsafeStagingDirectory);
    }
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            ReferenceSpoolError::StagingBusy
        } else {
            ReferenceSpoolError::StagingIo
        }
    })?;
    Ok(OwnedSpoolLockGuard { file })
}

fn create_anonymous_sqlite_database(
    directory: &Dir,
) -> Result<(File, Connection), ReferenceSpoolError> {
    let name = format!("{STAGED_DATABASE_PREFIX}{}.sqlite", Uuid::new_v4());
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let file = directory
        .open_with(&name, &options)
        .map_err(|_| ReferenceSpoolError::StagingIo)?
        .into_std();
    let connection = match open_sqlite_staging_from_descriptor(&file) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = directory.remove_file(&name);
            let _ = sync_capability_directory(directory);
            return Err(error);
        }
    };
    if directory.remove_file(&name).is_err() || sync_capability_directory(directory).is_err() {
        let _ = directory.remove_file(&name);
        return Err(ReferenceSpoolError::StagingIo);
    }
    Ok((file, connection))
}

fn validate_database_file(file: &File, maximum: u64) -> Result<u64, ReferenceSpoolError> {
    let metadata = file
        .metadata()
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > maximum {
        Err(ReferenceSpoolError::DatabaseLimitExceeded)
    } else {
        Ok(metadata.len())
    }
}

fn hash_regular_file(
    source: &File,
    maximum: u64,
    control: &ReferenceFetchControl,
    request: &PublicationRequest,
) -> Result<([u8; 32], u64), ReferenceSpoolError> {
    use sha2::{Digest, Sha256};

    ensure_publication_open(control, request)?;
    let expected_bytes = validate_database_file(source, maximum)?;
    let mut file = source
        .try_clone()
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ReferenceSpoolError::StagingIo)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 128 * 1024];
    let mut read_total = 0_u64;
    loop {
        ensure_publication_open(control, request)?;
        let read = file
            .read(&mut buffer)
            .map_err(|_| ReferenceSpoolError::StagingIo)?;
        if read == 0 {
            break;
        }
        read_total = read_total
            .checked_add(u64::try_from(read).map_err(|_| ReferenceSpoolError::CountOverflow)?)
            .ok_or(ReferenceSpoolError::CountOverflow)?;
        if read_total > maximum {
            return Err(ReferenceSpoolError::DatabaseLimitExceeded);
        }
        digest.update(&buffer[..read]);
    }
    if read_total != expected_bytes {
        return Err(ReferenceSpoolError::InvalidSealedDatabase);
    }
    ensure_publication_open(control, request)?;
    Ok((digest.finalize().into(), read_total))
}

#[cfg(unix)]
fn open_sqlite_staging_from_descriptor(file: &File) -> Result<Connection, ReferenceSpoolError> {
    use std::os::fd::AsRawFd as _;

    let locator = format!("file:/dev/fd/{}?mode=rw", file.as_raw_fd());
    Connection::open_with_flags(
        locator,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(ReferenceSpoolError::sqlite)
}

#[cfg(not(unix))]
fn open_sqlite_staging_from_descriptor(_file: &File) -> Result<Connection, ReferenceSpoolError> {
    Err(ReferenceSpoolError::CapabilityDatabaseUnavailable)
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn sync_capability_directory(directory: &Dir) -> Result<(), ReferenceSpoolError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(".", &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|file| file.sync_all())
        .map_err(|_| ReferenceSpoolError::StagingIo)
}

#[cfg(not(unix))]
fn sync_capability_directory(directory: &Dir) -> Result<(), ReferenceSpoolError> {
    directory
        .try_clone()
        .and_then(|directory| directory.into_std_file().sync_all())
        .map_err(|_| ReferenceSpoolError::StagingIo)
}

fn sqlite_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let value = row.get::<_, i64>(index)?;
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn sqlite_optional_u64(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(u64::try_from)
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Integer,
                Box::new(error),
            )
        })
}

/// Disk-backed publication, schema, recovery, or query refusal.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ReferenceSpoolError {
    /// The caller cancelled the publication before its atomic activation boundary.
    #[error("option-reference publication was cancelled")]
    PublicationCancelled,
    /// The monotonic or trusted-wall publication deadline elapsed.
    #[error("option-reference publication deadline elapsed")]
    PublicationDeadlineExceeded,
    /// Trusted clock or publication-control evidence was unavailable.
    #[error("option-reference publication control is unavailable")]
    PublicationControlUnavailable,
    /// Another process owns the single admitted provider-local spool reservation.
    #[error("option-reference staging reservation is busy")]
    StagingBusy,
    /// Caller limits violated code-owned bounds.
    #[error("invalid option-reference spool limits")]
    InvalidLimits,
    /// Staging capability did not designate a direct directory.
    #[error("unsafe option-reference staging directory")]
    UnsafeStagingDirectory,
    /// Free-space measurement failed.
    #[error("option-reference staging disk probe failed")]
    DiskProbeFailed,
    /// Staging did not have the full admitted reserve.
    #[error("option-reference staging needs {required} bytes but only {available} are available")]
    InsufficientDisk {
        /// Exact required reserve.
        required: u64,
        /// Measured free bytes.
        available: u64,
    },
    /// Temporary file or directory I/O failed.
    #[error("option-reference staging I/O failed")]
    StagingIo,
    /// SQLite operation failed without exposing provider or local paths.
    #[error("option-reference spool database operation failed")]
    Sqlite,
    /// This platform cannot reopen the capability-owned anonymous SQLite inode by descriptor.
    #[error("capability-backed option-reference staging database is unavailable")]
    CapabilityDatabaseUnavailable,
    /// A page was unrequested, already committed, or contained another surface.
    #[error("invalid option-reference spool surface state")]
    InvalidSurfaceState,
    /// Row and page receipt semantics did not reconcile.
    #[error("option-reference page receipt mismatch")]
    PageReceiptMismatch,
    /// Required requested surfaces were absent.
    #[error("incomplete option-reference publication")]
    IncompletePublication,
    /// The request selected a paged/document surface this single-object spool cannot represent.
    #[error("unsupported option-reference publication surface")]
    UnsupportedPublicationSurface,
    /// More than one incompatible OCC DLP snapshot representation was selected for one cycle.
    #[error("invalid option-reference publication cycle")]
    InvalidPublicationCycle,
    /// Aggregate request pages, bytes, records, conflicts, or time window were exceeded.
    #[error("option-reference publication request limit exceeded")]
    PublicationLimitExceeded,
    /// Exact source mapping conflicts prevent publication.
    #[error("conflicted option-reference publication")]
    ConflictedPublication,
    /// Conflict evidence exceeded its explicit bound.
    #[error("option-reference conflict limit exceeded")]
    ConflictLimitExceeded,
    /// Database file/page bytes exceeded the admitted limit.
    #[error("option-reference database limit exceeded")]
    DatabaseLimitExceeded,
    /// The staging directory contained an unowned type/name or excessive sidecar inventory.
    #[error("unsafe option-reference staging inventory")]
    UnsafeStagingInventory,
    /// Sealed schema, integrity, foreign keys, or page accounting failed.
    #[error("invalid sealed option-reference database")]
    InvalidSealedDatabase,
    /// Row or byte accounting overflowed.
    #[error("option-reference spool count overflow")]
    CountOverflow,
    /// A bounded allocation was unavailable.
    #[error("option-reference spool capacity unavailable")]
    CapacityUnavailable,
    /// Stable provider data could not be encoded.
    #[error("option-reference spool encoding failed")]
    EncodingFailed,
    /// The spool was already sealed.
    #[error("option-reference spool is already sealed")]
    AlreadySealed,
    /// Cboe provider data could not be reconstructed.
    #[error("invalid Cboe row in sealed option-reference generation")]
    Cboe(#[from] CboeParseError),
    /// OCC provider data could not be reconstructed.
    #[error("invalid OCC row in sealed option-reference generation")]
    Occ(#[from] OccParseError),
}

impl ReferenceSpoolError {
    fn sqlite(_: rusqlite::Error) -> Self {
        Self::Sqlite
    }
}
