//! Durable source, rights, reference, ingest, artifact, manifest, and audit operations.

use market_squawk_domain::{
    ContractRollMapping, CorporateActionObservation, InstrumentDefinition, InstrumentId,
    LifecycleTransition, LifecycleTransitionKind, SourceId, SymbolIdentityRecord, Timestamp,
};
use market_squawk_sources::SourceMetadata;
use rusqlite::{OptionalExtension as _, params};
use serde::de::DeserializeOwned;

use super::storage::{
    AppendOutcome, ResultBudget, append_audit, persist_instrument_children, persist_symbol,
    query_records, require_instrument, sha256, trusted_catalog_now,
};
use super::types::*;

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
        persist_instrument_children(&transaction, instrument, observed_at)?;
        append_audit(
            &transaction,
            "instrument.recorded",
            &instrument.instrument_id().to_string(),
            digest,
            catalog_now,
        )?;
        transaction.commit()?;
        Ok(())
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

fn deserialize_verified<T: DeserializeOwned>(
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
