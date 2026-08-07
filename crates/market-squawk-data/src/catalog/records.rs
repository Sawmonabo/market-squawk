//! Durable source, rights, reference, ingest, artifact, manifest, and audit operations.

use std::collections::BTreeSet;
use std::mem::size_of;
use std::time::Instant;

use market_squawk_domain::{
    ContractRollMapping, CorporateActionObservation, InstrumentDefinition, InstrumentId,
    LifecycleTransition, LifecycleTransitionKind, SourceId, SymbolIdentityRecord, Timestamp,
};
use market_squawk_sources::SourceMetadata;
use rusqlite::{OptionalExtension as _, Transaction, params};
use serde::de::DeserializeOwned;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::storage::{
    AppendOutcome, ResultBudget, append_audit, persist_instrument_children, persist_symbol,
    query_records, require_instrument, sha256, trusted_catalog_now,
};
use super::types::*;
use crate::Sha256Digest;

impl Catalog {
    /// Registers exact, validated source metadata and its content digest.
    pub fn register_source(
        &self,
        source: &SourceMetadata,
        registered_at: Timestamp,
    ) -> Result<(), CatalogError> {
        let json = serde_json::to_string(source)?;
        let digest = sha256(json.as_bytes());
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        let existing_revision: Option<(String, i64)> = transaction
            .query_row(
                "SELECT metadata_json, registered_at_ns FROM source_revisions
                 WHERE source_id=?1 AND revision_digest=?2",
                params![source.source_id().as_str(), digest],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_json, existing_at)) = existing_revision {
            if existing_json == json && existing_at == registered_at.unix_nanos() {
                return Ok(());
            }
            let current_at: i64 = transaction.query_row(
                "SELECT current_registered_at_ns FROM sources WHERE source_id=?1",
                [source.source_id().as_str()],
                |row| row.get(0),
            )?;
            return Err(if registered_at.unix_nanos() < current_at {
                CatalogError::StaleSourceRevision
            } else {
                CatalogError::SourceRevisionConflict
            });
        }
        transaction.execute(
            "INSERT OR IGNORE INTO sources
             (source_id, current_revision_digest, current_registered_at_ns,
              first_registered_at_ns) VALUES (?1, ?2, ?3, ?3)",
            params![
                source.source_id().as_str(),
                digest,
                registered_at.unix_nanos()
            ],
        )?;
        transaction.execute(
            "INSERT INTO source_revisions
             (source_id, revision_digest, metadata_json, registered_at_ns)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                source.source_id().as_str(),
                digest,
                json,
                registered_at.unix_nanos()
            ],
        )?;
        transaction.execute(
            "UPDATE sources
             SET current_revision_digest=?1,
                 current_registered_at_ns=MAX(current_registered_at_ns, ?2)
             WHERE source_id=?3
               AND (current_revision_digest=?1 OR current_registered_at_ns < ?2)",
            params![
                digest,
                registered_at.unix_nanos(),
                source.source_id().as_str()
            ],
        )?;
        let (current_digest, current_at): (Vec<u8>, i64) = transaction.query_row(
            "SELECT current_revision_digest, current_registered_at_ns
             FROM sources WHERE source_id=?1",
            [source.source_id().as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if current_digest.as_slice() != digest {
            return Err(if registered_at.unix_nanos() < current_at {
                CatalogError::StaleSourceRevision
            } else {
                CatalogError::SourceRevisionConflict
            });
        }
        append_audit(
            &transaction,
            "source.registered",
            source.source_id().as_str(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Loads and revalidates registered source metadata.
    pub fn source(&self, source_id: &SourceId) -> Result<Option<SourceMetadata>, CatalogError> {
        let mut budget = ResultBudget::new(self.result_bytes);
        let row: Option<(String, Vec<u8>)> = self
            .connection
            .query_row(
                "SELECT revisions.metadata_json, revisions.revision_digest
                 FROM sources
                 JOIN source_revisions AS revisions
                   ON revisions.source_id=sources.source_id
                  AND revisions.revision_digest=sources.current_revision_digest
                 WHERE sources.source_id=?1",
                [source_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(value, digest)| deserialize_verified(&value, &digest, &mut budget))
            .transpose()
    }

    /// Returns newest-first immutable source revisions within the requested bound.
    pub fn source_history(
        &self,
        source_id: &SourceId,
        limit: CatalogLimit,
    ) -> Result<Vec<SourceMetadata>, CatalogError> {
        self.enforce_limit(limit)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let mut statement = self.connection.prepare(
            "SELECT metadata_json, revision_digest FROM source_revisions WHERE source_id=?1
             ORDER BY registered_at_ns DESC, revision_digest DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![source_id.as_str(), row_limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut history = Vec::new();
        history
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let (value, digest) = row?;
            history.push(deserialize_verified(&value, &digest, &mut budget)?);
        }
        Ok(history)
    }

    /// Persists a validated current instrument and its normalized identity evidence.
    pub fn put_instrument(
        &self,
        instrument: &InstrumentDefinition,
        observed_at: Timestamp,
    ) -> Result<(), CatalogError> {
        let json = serde_json::to_string(instrument)?;
        let digest = sha256(json.as_bytes());
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        put_instrument_revision(
            &transaction,
            instrument,
            observed_at,
            &json,
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Atomically publishes a bounded set of configured canonical instrument definitions.
    ///
    /// An unchanged current definition is a restart-safe no-op. Changed content must advance both
    /// the monotonic definition revision and durable observation time. The complete set commits
    /// together so conflicting provider mappings cannot expose a partial configured universe.
    pub fn synchronize_instruments(
        &self,
        instruments: &[InstrumentDefinition],
        observed_at: Timestamp,
        limit: CatalogLimit,
    ) -> Result<usize, CatalogError> {
        self.enforce_limit(limit)?;
        if instruments.len() > limit.get() {
            return Err(CatalogError::InvalidLimit);
        }
        let mut instrument_ids = BTreeSet::new();
        for instrument in instruments {
            if !instrument_ids.insert(instrument.instrument_id()) {
                return Err(CatalogError::InstrumentRevisionConflict);
            }
        }

        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        let mut published = 0_usize;
        for instrument in instruments {
            let json = serde_json::to_string(instrument)?;
            let digest = sha256(json.as_bytes());
            let current: Option<(String, Vec<u8>, i64)> = transaction
                .query_row(
                    "SELECT revisions.definition_json, revisions.revision_digest,
                            instruments.current_observed_at_ns
                     FROM instruments
                     JOIN instrument_revisions AS revisions
                       ON revisions.instrument_id=instruments.instrument_id
                      AND revisions.revision_digest=instruments.current_revision_digest
                     WHERE instruments.instrument_id=?1",
                    [instrument.instrument_id().to_string()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            if let Some((current_json, current_digest, current_observed_at)) = current {
                if current_digest.as_slice() == digest {
                    if current_json != json {
                        return Err(CatalogError::EvidenceConflict);
                    }
                    continue;
                }
                let current: InstrumentDefinition = serde_json::from_str(&current_json)?;
                if instrument.definition_revision() < current.definition_revision()
                    || observed_at.unix_nanos() < current_observed_at
                {
                    return Err(CatalogError::StaleInstrumentRevision);
                }
                if instrument.definition_revision() == current.definition_revision()
                    || observed_at.unix_nanos() == current_observed_at
                {
                    return Err(CatalogError::InstrumentRevisionConflict);
                }
            }
            put_instrument_revision(
                &transaction,
                instrument,
                observed_at,
                &json,
                digest,
                catalog_now,
            )?;
            published = published.checked_add(1).ok_or(CatalogError::Allocation)?;
        }
        transaction.commit()?;
        Ok(published)
    }

    /// Persists one explicit venue-symbol validity interval.
    pub fn put_symbol(&self, symbol: &SymbolIdentityRecord) -> Result<(), CatalogError> {
        let json = serde_json::to_string(symbol)?;
        let digest = sha256(json.as_bytes());
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        if persist_symbol(&transaction, symbol, &json)? == AppendOutcome::Replay {
            return Ok(());
        }
        append_audit(
            &transaction,
            "instrument.symbol-recorded",
            &symbol.instrument_id().to_string(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Persists a merger or delisting transition after validating referenced instruments.
    pub fn put_lifecycle(&self, transition: &LifecycleTransition) -> Result<(), CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        if let LifecycleTransitionKind::Merger { successor } = transition.kind() {
            require_instrument(&transaction, successor)?;
        }
        require_instrument(&transaction, transition.instrument_id())?;
        let json = serde_json::to_string(transition)?;
        let digest = sha256(json.as_bytes());
        let existing: Option<(String, Vec<u8>)> = transaction
            .query_row(
                "SELECT transition_json, transition_digest FROM lifecycle_transitions
                 WHERE instrument_id=?1 AND effective_at_ns=?2",
                params![
                    transition.instrument_id().to_string(),
                    transition.effective_at().unix_nanos()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_json, existing_digest)) = existing {
            return if existing_json == json && existing_digest.as_slice() == digest {
                Ok(())
            } else {
                Err(CatalogError::EvidenceConflict)
            };
        }
        transaction.execute(
            "INSERT INTO lifecycle_transitions
             (instrument_id, effective_at_ns, transition_json, transition_digest)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                transition.instrument_id().to_string(),
                transition.effective_at().unix_nanos(),
                json,
                digest
            ],
        )?;
        append_audit(
            &transaction,
            "instrument.lifecycle-recorded",
            &transition.instrument_id().to_string(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Persists a validated contract-roll mapping.
    pub fn put_contract_roll(&self, roll: &ContractRollMapping) -> Result<(), CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        require_instrument(&transaction, roll.from_instrument_id())?;
        require_instrument(&transaction, roll.to_instrument_id())?;
        let json = serde_json::to_string(roll)?;
        let digest = sha256(json.as_bytes());
        let existing: Option<(String, Vec<u8>)> = transaction
            .query_row(
                "SELECT roll_json, roll_digest FROM contract_rolls
                 WHERE from_instrument_id=?1 AND to_instrument_id=?2 AND effective_at_ns=?3",
                params![
                    roll.from_instrument_id().to_string(),
                    roll.to_instrument_id().to_string(),
                    roll.effective_at().unix_nanos()
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((existing_json, existing_digest)) = existing {
            return if existing_json == json && existing_digest.as_slice() == digest {
                Ok(())
            } else {
                Err(CatalogError::EvidenceConflict)
            };
        }
        transaction.execute(
            "INSERT INTO contract_rolls
             (from_instrument_id, to_instrument_id, effective_at_ns, roll_json, roll_digest)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                roll.from_instrument_id().to_string(),
                roll.to_instrument_id().to_string(),
                roll.effective_at().unix_nanos(),
                json,
                digest
            ],
        )?;
        append_audit(
            &transaction,
            "instrument.roll-recorded",
            &roll.from_instrument_id().to_string(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Persists one canonical, point-in-time corporate action.
    pub fn put_corporate_action(
        &self,
        action: &CorporateActionObservation,
    ) -> Result<(), CatalogError> {
        let context = action.context();
        let effective_at = context
            .time()
            .effective()
            .exact_timestamp()
            .ok_or(CatalogError::InvalidRecord)?;
        let instrument_id = context
            .provenance()
            .instrument_id()
            .ok_or(CatalogError::InvalidRecord)?;
        let json = serde_json::to_string(action)?;
        let digest = sha256(json.as_bytes());
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT action_json FROM corporate_actions
                 WHERE instrument_id=?1 AND source_id=?2 AND action_digest=?3",
                params![
                    instrument_id.to_string(),
                    context.provenance().source_id().as_str(),
                    digest
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(existing_json) = existing {
            return if existing_json == json {
                Ok(())
            } else {
                Err(CatalogError::EvidenceConflict)
            };
        }
        transaction.execute(
            "INSERT INTO corporate_actions
             (instrument_id, source_id, effective_at_ns, action_json, action_digest)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                instrument_id.to_string(),
                context.provenance().source_id().as_str(),
                effective_at.unix_nanos(),
                json,
                digest
            ],
        )?;
        append_audit(
            &transaction,
            "instrument.corporate-action-recorded",
            &instrument_id.to_string(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Returns a globally bounded reference-data bundle.
    pub fn reference_bundle(
        &self,
        instrument_id: InstrumentId,
        limit: CatalogLimit,
    ) -> Result<ReferenceBundle, CatalogError> {
        self.enforce_limit(limit)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let instrument = self.instrument(instrument_id, &mut budget)?;
        let mut remaining = limit
            .get()
            .checked_sub(usize::from(instrument.is_some()))
            .ok_or(CatalogError::CorruptCatalog)?;
        let symbols = query_records(
            &self.connection,
            "SELECT record_json, record_digest FROM symbol_history WHERE instrument_id=?1
             ORDER BY starts_at_ns, venue_id, venue_symbol LIMIT ?2",
            instrument_id,
            &mut remaining,
            &mut budget,
        )?;
        let lifecycle = query_records(
            &self.connection,
            "SELECT transition_json, transition_digest FROM lifecycle_transitions
             WHERE instrument_id=?1
             ORDER BY effective_at_ns, transition_digest LIMIT ?2",
            instrument_id,
            &mut remaining,
            &mut budget,
        )?;
        let contract_rolls = query_records(
            &self.connection,
            "SELECT roll_json, roll_digest FROM contract_rolls WHERE from_instrument_id=?1
             ORDER BY effective_at_ns, to_instrument_id LIMIT ?2",
            instrument_id,
            &mut remaining,
            &mut budget,
        )?;
        let corporate_actions = query_records(
            &self.connection,
            "SELECT action_json, action_digest FROM corporate_actions WHERE instrument_id=?1
             ORDER BY effective_at_ns, source_id, action_digest LIMIT ?2",
            instrument_id,
            &mut remaining,
            &mut budget,
        )?;
        Ok(ReferenceBundle {
            instrument,
            symbols,
            lifecycle,
            contract_rolls,
            corporate_actions,
        })
    }

    /// Returns newest-first immutable definitions for one stable instrument identity.
    pub fn instrument_history(
        &self,
        instrument_id: InstrumentId,
        limit: CatalogLimit,
    ) -> Result<Vec<InstrumentDefinition>, CatalogError> {
        self.enforce_limit(limit)?;
        let mut budget = ResultBudget::new(self.result_bytes);
        let row_limit = i64::try_from(limit.get()).map_err(|_| CatalogError::InvalidLimit)?;
        let mut statement = self.connection.prepare(
            "SELECT definition_json, revision_digest FROM instrument_revisions
             WHERE instrument_id=?1
             ORDER BY observed_at_ns DESC, revision_digest DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(params![instrument_id.to_string(), row_limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        let mut history = Vec::new();
        history
            .try_reserve_exact(budget.bounded_row_capacity(limit.get()))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            let (value, digest) = row?;
            history.push(deserialize_verified(&value, &digest, &mut budget)?);
        }
        Ok(history)
    }

    /// Pins complete, verified instrument-definition histories at one catalog knowledge bound.
    pub fn pin_instrument_definitions(
        &self,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        limit: CatalogLimit,
    ) -> Result<PinnedInstrumentDefinitions, CatalogError> {
        self.pin_instrument_definitions_checked(instrument_ids, as_of, limit, || Ok(()))
    }

    /// Pins complete definitions while observing one cancellation token and absolute deadline.
    pub fn pin_instrument_definitions_bounded(
        &self,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        limit: CatalogLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PinnedInstrumentDefinitions, CatalogError> {
        self.pin_instrument_definitions_checked(instrument_ids, as_of, limit, || {
            check_instrument_definition_read(deadline, cancellation)
        })
    }

    fn pin_instrument_definitions_checked(
        &self,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        limit: CatalogLimit,
        mut check_operation: impl FnMut() -> Result<(), CatalogError>,
    ) -> Result<PinnedInstrumentDefinitions, CatalogError> {
        check_operation()?;
        self.enforce_limit(limit)?;
        if instrument_ids.is_empty() {
            return Err(CatalogError::InvalidRecord);
        }
        if instrument_ids.len() > limit.get() {
            return Err(CatalogError::ResultRowLimitExceeded);
        }
        let requested = instrument_ids.iter().copied().collect::<BTreeSet<_>>();
        if requested.len() != instrument_ids.len() {
            return Err(CatalogError::InvalidRecord);
        }

        let mut budget = ResultBudget::new(self.result_bytes);
        let mut histories = Vec::new();
        histories
            .try_reserve_exact(requested.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut total_rows = 0_usize;
        let mut statement = self.connection.prepare(
            "SELECT definition_json, revision_digest, observed_at_ns
             FROM instrument_revisions
             WHERE instrument_id=?1 AND observed_at_ns<=?2
             ORDER BY observed_at_ns, revision_digest
             LIMIT ?3",
        )?;
        for instrument_id in requested {
            check_operation()?;
            let remaining = limit
                .get()
                .checked_sub(total_rows)
                .ok_or(CatalogError::ResultRowLimitExceeded)?;
            let probe_limit = remaining
                .checked_add(1)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or(CatalogError::InvalidLimit)?;
            let rows = statement.query_map(
                params![instrument_id.to_string(), as_of.unix_nanos(), probe_limit],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )?;
            let mut definitions = Vec::<PinnedInstrumentDefinition>::new();
            for row in rows {
                check_operation()?;
                if total_rows == limit.get() {
                    return Err(CatalogError::ResultRowLimitExceeded);
                }
                let (definition_json, stored_digest, observed_at_ns) = row?;
                budget.charge([definition_json.len(), stored_digest.len(), size_of::<i64>()])?;
                if stored_digest.len() != 32
                    || sha256(definition_json.as_bytes()).as_slice() != stored_digest
                {
                    return Err(CatalogError::CorruptCatalog);
                }
                let row_digest = <[u8; 32]>::try_from(stored_digest.as_slice())
                    .map_err(|_| CatalogError::CorruptCatalog)?;
                let definition: InstrumentDefinition = serde_json::from_str(&definition_json)?;
                let observed_at = Timestamp::from_unix_nanos(observed_at_ns);
                if definition.instrument_id() != instrument_id || observed_at > as_of {
                    return Err(CatalogError::CorruptCatalog);
                }
                if let Some(previous) = definitions.last_mut() {
                    if previous.effective_start >= observed_at
                        || previous.definition_revision >= definition.definition_revision()
                    {
                        return Err(CatalogError::InvalidRecord);
                    }
                    previous.effective_end = Some(observed_at);
                }
                definitions
                    .try_reserve(1)
                    .map_err(|_| CatalogError::Allocation)?;
                definitions.push(PinnedInstrumentDefinition {
                    row_digest: Sha256Digest::new(row_digest),
                    definition_revision: definition.definition_revision(),
                    execution_terms: definition.execution_terms(),
                    effective_start: observed_at,
                    effective_end: None,
                });
                total_rows = total_rows
                    .checked_add(1)
                    .ok_or(CatalogError::ResultRowLimitExceeded)?;
            }
            if definitions.is_empty() {
                return Err(CatalogError::InvalidRecord);
            }
            histories.push(PinnedInstrumentHistory {
                instrument_id,
                definitions: definitions.into_boxed_slice(),
            });
        }
        check_operation()?;
        let content_identity = pinned_definition_identity(
            b"market-squawk/instrument-definitions/content/v1",
            None,
            None,
            &histories,
        )?;
        let audit_identity = pinned_definition_identity(
            b"market-squawk/instrument-definitions/audit/v1",
            Some(as_of),
            Some(limit),
            &histories,
        )?;
        Ok(PinnedInstrumentDefinitions::new(
            as_of,
            histories,
            content_identity,
            audit_identity,
        ))
    }

    /// Stores a cursor without allowing an older update to overwrite newer progress.
    pub fn set_cursor(&self, cursor: &SourceCursor) -> Result<(), CatalogError> {
        let transaction = self.connection.unchecked_transaction()?;
        let catalog_now = trusted_catalog_now(&transaction)?;
        let existing: Option<(String, i64)> = transaction
            .query_row(
                "SELECT cursor_value, updated_at_ns FROM source_cursors
                 WHERE source_id=?1 AND cursor_name=?2",
                params![cursor.source_id.as_str(), cursor.name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if let Some((value, updated_at)) = existing {
            if cursor.updated_at.unix_nanos() < updated_at {
                return Err(CatalogError::StaleCursor);
            }
            if cursor.updated_at.unix_nanos() == updated_at {
                return if cursor.value == value {
                    Ok(())
                } else {
                    Err(CatalogError::CursorConflict)
                };
            }
        }
        transaction.execute(
            "INSERT INTO source_cursors(source_id, cursor_name, cursor_value, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(source_id, cursor_name) DO UPDATE SET
                 cursor_value=excluded.cursor_value,
                 updated_at_ns=excluded.updated_at_ns",
            params![
                cursor.source_id.as_str(),
                cursor.name,
                cursor.value,
                cursor.updated_at.unix_nanos()
            ],
        )?;
        append_audit(
            &transaction,
            "source.cursor-updated",
            cursor.source_id.as_str(),
            sha256(cursor.value.as_bytes()),
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Loads one exact source cursor.
    pub fn cursor(
        &self,
        source_id: &SourceId,
        name: &str,
    ) -> Result<Option<SourceCursor>, CatalogError> {
        let mut budget = ResultBudget::new(self.result_bytes);
        let stored = self
            .connection
            .query_row(
                "SELECT cursor_value, updated_at_ns FROM source_cursors
                 WHERE source_id=?1 AND cursor_name=?2",
                params![source_id.as_str(), name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        stored
            .map(|(value, updated_at)| {
                budget.charge([source_id.as_str().len(), name.len(), value.len()])?;
                SourceCursor::try_new(
                    source_id.clone(),
                    name,
                    value,
                    Timestamp::from_unix_nanos(updated_at),
                )
            })
            .transpose()
    }

    fn instrument(
        &self,
        instrument_id: InstrumentId,
        budget: &mut ResultBudget,
    ) -> Result<Option<InstrumentDefinition>, CatalogError> {
        let row: Option<(String, Vec<u8>)> = self
            .connection
            .query_row(
                "SELECT revisions.definition_json, revisions.revision_digest
                 FROM instruments
                 JOIN instrument_revisions AS revisions
                   ON revisions.instrument_id=instruments.instrument_id
                  AND revisions.revision_digest=instruments.current_revision_digest
                 WHERE instruments.instrument_id=?1",
                [instrument_id.to_string()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        row.map(|(value, digest)| deserialize_verified(&value, &digest, budget))
            .transpose()
    }
}

fn put_instrument_revision(
    transaction: &Transaction<'_>,
    instrument: &InstrumentDefinition,
    observed_at: Timestamp,
    json: &str,
    digest: [u8; 32],
    catalog_now: Timestamp,
) -> Result<(), CatalogError> {
    let existing_revision: Option<(String, i64)> = transaction
        .query_row(
            "SELECT definition_json, observed_at_ns FROM instrument_revisions
             WHERE instrument_id=?1 AND revision_digest=?2",
            params![instrument.instrument_id().to_string(), digest],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((existing_json, existing_at)) = existing_revision {
        if existing_json == json && existing_at == observed_at.unix_nanos() {
            return Ok(());
        }
        let current_at: i64 = transaction.query_row(
            "SELECT current_observed_at_ns FROM instruments WHERE instrument_id=?1",
            [instrument.instrument_id().to_string()],
            |row| row.get(0),
        )?;
        return Err(if observed_at.unix_nanos() < current_at {
            CatalogError::StaleInstrumentRevision
        } else {
            CatalogError::InstrumentRevisionConflict
        });
    }
    transaction.execute(
        "INSERT OR IGNORE INTO instruments
         (instrument_id, current_revision_digest, current_observed_at_ns,
          first_observed_at_ns) VALUES (?1, ?2, ?3, ?3)",
        params![
            instrument.instrument_id().to_string(),
            digest,
            observed_at.unix_nanos()
        ],
    )?;
    transaction.execute(
        "INSERT INTO instrument_revisions
         (instrument_id, revision_digest, definition_json, observed_at_ns)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            instrument.instrument_id().to_string(),
            digest,
            json,
            observed_at.unix_nanos()
        ],
    )?;
    transaction.execute(
        "UPDATE instruments
         SET current_revision_digest=?1,
             current_observed_at_ns=MAX(current_observed_at_ns, ?2)
         WHERE instrument_id=?3
           AND (current_revision_digest=?1 OR current_observed_at_ns < ?2)",
        params![
            digest,
            observed_at.unix_nanos(),
            instrument.instrument_id().to_string()
        ],
    )?;
    let (current_digest, current_at): (Vec<u8>, i64) = transaction.query_row(
        "SELECT current_revision_digest, current_observed_at_ns
         FROM instruments WHERE instrument_id=?1",
        [instrument.instrument_id().to_string()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if current_digest.as_slice() != digest {
        return Err(if observed_at.unix_nanos() < current_at {
            CatalogError::StaleInstrumentRevision
        } else {
            CatalogError::InstrumentRevisionConflict
        });
    }
    persist_instrument_children(transaction, instrument, observed_at)?;
    append_audit(
        transaction,
        "instrument.recorded",
        &instrument.instrument_id().to_string(),
        digest,
        catalog_now,
    )?;
    Ok(())
}

fn check_instrument_definition_read(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::InstrumentDefinitionReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::InstrumentDefinitionReadDeadlineExceeded)
    } else {
        Ok(())
    }
}

fn pinned_definition_identity(
    domain: &[u8],
    as_of: Option<Timestamp>,
    limit: Option<CatalogLimit>,
    histories: &[PinnedInstrumentHistory],
) -> Result<Sha256Digest, CatalogError> {
    let mut hash = Sha256::new();
    hash.update(domain);
    match as_of {
        Some(as_of) => {
            hash.update([1]);
            hash.update(as_of.unix_nanos().to_be_bytes());
        }
        None => hash.update([0]),
    }
    match limit {
        Some(limit) => {
            hash.update([1]);
            hash.update(
                u64::try_from(limit.get())
                    .map_err(|_| CatalogError::InvalidLimit)?
                    .to_be_bytes(),
            );
        }
        None => hash.update([0]),
    }
    hash.update(
        u64::try_from(histories.len())
            .map_err(|_| CatalogError::ResultRowLimitExceeded)?
            .to_be_bytes(),
    );
    for history in histories {
        hash.update(history.instrument_id.as_uuid().as_bytes());
        hash.update(
            u64::try_from(history.definitions.len())
                .map_err(|_| CatalogError::ResultRowLimitExceeded)?
                .to_be_bytes(),
        );
        for definition in &history.definitions {
            hash.update(definition.row_digest.bytes());
            hash.update(definition.definition_revision.get().to_be_bytes());
            hash.update(definition.effective_start.unix_nanos().to_be_bytes());
            match definition.effective_end {
                Some(effective_end) => {
                    hash.update([1]);
                    hash.update(effective_end.unix_nanos().to_be_bytes());
                }
                None => hash.update([0]),
            }
        }
    }
    Ok(Sha256Digest::new(hash.finalize().into()))
}

pub(super) fn deserialize_verified<T: DeserializeOwned>(
    value: &str,
    stored_digest: &[u8],
    budget: &mut ResultBudget,
) -> Result<T, CatalogError> {
    budget.charge([value.len(), stored_digest.len()])?;
    if stored_digest.len() != 32 || sha256(value.as_bytes()).as_slice() != stored_digest {
        return Err(CatalogError::CorruptCatalog);
    }
    Ok(serde_json::from_str(value)?)
}
