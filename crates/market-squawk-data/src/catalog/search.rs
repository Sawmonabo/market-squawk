//! Bounded canonical instrument discovery over the local reference master.

use std::time::Instant;

use market_squawk_domain::{InstrumentDefinition, SymbolIdentityRecord, Timestamp};
use rusqlite::params;
use tokio_util::sync::CancellationToken;

use super::records::deserialize_verified;
use super::storage::ResultBudget;
use super::types::{Catalog, CatalogError, CatalogLimit};

const MAXIMUM_SEARCH_QUERY_BYTES: usize = 256;
const MAXIMUM_MATCHING_SYMBOLS_PER_INSTRUMENT: usize = 64;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// One verified current instrument definition and the matching symbol-history evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSearchMatch {
    definition: InstrumentDefinition,
    definition_observed_at: Timestamp,
    matching_symbols: Box<[SymbolIdentityRecord]>,
    matching_symbols_truncated: bool,
}

impl InstrumentSearchMatch {
    /// Returns the verified current reference-master definition.
    pub const fn definition(&self) -> &InstrumentDefinition {
        &self.definition
    }

    /// Returns when the current definition was recorded in the catalog.
    pub const fn definition_observed_at(&self) -> Timestamp {
        self.definition_observed_at
    }

    /// Returns verified current or historical venue-symbol records matching the query.
    pub fn matching_symbols(&self) -> &[SymbolIdentityRecord] {
        &self.matching_symbols
    }

    /// Reports whether additional matching historical symbols were omitted by the hard bound.
    pub const fn matching_symbols_truncated(&self) -> bool {
        self.matching_symbols_truncated
    }
}

/// One bounded search page in deterministic relevance order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentSearchPage {
    matches: Box<[InstrumentSearchMatch]>,
    has_more: bool,
}

impl InstrumentSearchPage {
    /// Returns the verified matches retained by this page.
    pub fn matches(&self) -> &[InstrumentSearchMatch] {
        &self.matches
    }

    /// Reports whether at least one additional matching identity exists.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

impl Catalog {
    /// Searches stable IDs, current and historical venue symbols, admitted external identifiers,
    /// and accepted provider identities.
    ///
    /// Search never returns quarantined provider assertions, verified-unassigned identifiers, or
    /// identifiers whose retained rights policy is unknown or restricted. Every returned
    /// definition and historical symbol is digest-verified before leaving the catalog boundary.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidRecord`] for an empty, oversized, or control-character
    /// query, [`CatalogError::InvalidLimit`] for a limit outside the catalog's configured bound,
    /// and the typed instrument-definition cancellation or deadline errors when interrupted.
    pub fn search_instruments(
        &self,
        query: &str,
        limit: CatalogLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentSearchPage, CatalogError> {
        self.enforce_limit(limit)?;
        let query = query.trim();
        if query.is_empty()
            || query.len() > MAXIMUM_SEARCH_QUERY_BYTES
            || query.chars().any(char::is_control)
        {
            return Err(CatalogError::InvalidRecord);
        }
        check_search(deadline, cancellation)?;
        let normalized = query.to_ascii_lowercase();
        let retrieval_limit = limit
            .get()
            .checked_add(1)
            .ok_or(CatalogError::InvalidLimit)?;
        let retrieval_limit =
            i64::try_from(retrieval_limit).map_err(|_| CatalogError::InvalidLimit)?;
        let token = cancellation.clone();
        self.connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let operation = (|| {
            let mut statement = self.connection.prepare(SEARCH_CANDIDATES_SQL)?;
            let candidates = statement.query_map(params![normalized, retrieval_limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            let mut budget = ResultBudget::new(self.result_bytes);
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(limit.get().saturating_add(1))
                .map_err(|_| CatalogError::Allocation)?;
            for candidate in candidates {
                check_search(deadline, cancellation)?;
                let (_instrument_id, definition_json, definition_digest, observed_at_ns) =
                    candidate?;
                let definition: InstrumentDefinition =
                    deserialize_verified(&definition_json, &definition_digest, &mut budget)?;
                let (matching_symbols, matching_symbols_truncated) = self.matching_symbol_history(
                    definition.instrument_id(),
                    &normalized,
                    &mut budget,
                    deadline,
                    cancellation,
                )?;
                matches.push(InstrumentSearchMatch {
                    definition,
                    definition_observed_at: Timestamp::from_unix_nanos(observed_at_ns),
                    matching_symbols: matching_symbols.into_boxed_slice(),
                    matching_symbols_truncated,
                });
            }
            let has_more = matches.len() > limit.get();
            matches.truncate(limit.get());
            Ok(InstrumentSearchPage {
                matches: matches.into_boxed_slice(),
                has_more,
            })
        })();
        self.connection.progress_handler::<fn() -> bool>(0, None)?;
        operation.map_err(|error| classify_search_error(error, deadline, cancellation))
    }

    fn matching_symbol_history(
        &self,
        instrument_id: market_squawk_domain::InstrumentId,
        query: &str,
        budget: &mut ResultBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<SymbolIdentityRecord>, bool), CatalogError> {
        let retrieval_limit = i64::try_from(
            MAXIMUM_MATCHING_SYMBOLS_PER_INSTRUMENT
                .checked_add(1)
                .ok_or(CatalogError::InvalidLimit)?,
        )
        .map_err(|_| CatalogError::InvalidLimit)?;
        let mut statement = self.connection.prepare(
            "SELECT record_json, record_digest
             FROM symbol_history
             WHERE instrument_id=?1
               AND (instr(lower(venue_symbol), ?2)>0 OR instr(lower(venue_id), ?2)>0)
             ORDER BY
               CASE
                 WHEN lower(venue_symbol)=?2 OR lower(venue_id)=?2 THEN 0
                 WHEN instr(lower(venue_symbol), ?2)=1 OR instr(lower(venue_id), ?2)=1 THEN 1
                 ELSE 2
               END,
               starts_at_ns DESC, venue_id, venue_symbol
             LIMIT ?3",
        )?;
        let rows = statement.query_map(
            params![instrument_id.to_string(), query, retrieval_limit],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )?;
        let mut symbols = Vec::new();
        symbols
            .try_reserve_exact(MAXIMUM_MATCHING_SYMBOLS_PER_INSTRUMENT.saturating_add(1))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            check_search(deadline, cancellation)?;
            let (json, digest) = row?;
            symbols.push(deserialize_verified(&json, &digest, budget)?);
        }
        let truncated = symbols.len() > MAXIMUM_MATCHING_SYMBOLS_PER_INSTRUMENT;
        symbols.truncate(MAXIMUM_MATCHING_SYMBOLS_PER_INSTRUMENT);
        Ok((symbols, truncated))
    }
}

fn check_search(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::InstrumentDefinitionReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::InstrumentDefinitionReadDeadlineExceeded)
    } else {
        Ok(())
    }
}

fn classify_search_error(
    error: CatalogError,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> CatalogError {
    if cancellation.is_cancelled() {
        CatalogError::InstrumentDefinitionReadCancelled
    } else if Instant::now() >= deadline {
        CatalogError::InstrumentDefinitionReadDeadlineExceeded
    } else {
        error
    }
}

const SEARCH_CANDIDATES_SQL: &str = r#"
WITH current_definitions AS (
    SELECT
        instruments.instrument_id,
        instruments.current_observed_at_ns,
        revisions.definition_json,
        revisions.revision_digest
    FROM instruments
    JOIN instrument_revisions AS revisions
      ON revisions.instrument_id=instruments.instrument_id
     AND revisions.revision_digest=instruments.current_revision_digest
), candidate_ranks AS (
    SELECT instrument_id,
           CASE
             WHEN lower(instrument_id)=?1 THEN 0
             WHEN instr(lower(instrument_id), ?1)=1 THEN 1
             ELSE 2
           END AS rank
    FROM current_definitions
    WHERE instr(lower(instrument_id), ?1)>0

    UNION ALL

    SELECT current.instrument_id,
           CASE
             WHEN lower(json_extract(mapping.value, '$.venue_symbol'))=?1
               OR lower(json_extract(mapping.value, '$.venue_id'))=?1 THEN 0
             WHEN instr(lower(json_extract(mapping.value, '$.venue_symbol')), ?1)=1
               OR instr(lower(json_extract(mapping.value, '$.venue_id')), ?1)=1 THEN 1
             ELSE 2
           END AS rank
    FROM current_definitions AS current,
         json_each(current.definition_json, '$.venue_mappings') AS mapping
    WHERE instr(lower(json_extract(mapping.value, '$.venue_symbol')), ?1)>0
       OR instr(lower(json_extract(mapping.value, '$.venue_id')), ?1)>0

    UNION ALL

    SELECT current.instrument_id,
           CASE
             WHEN lower(CAST(json_extract(identifier.value, '$.identifier.value') AS TEXT))=?1
               OR lower(json_extract(identifier.value, '$.identifier.kind'))=?1 THEN 0
             WHEN instr(lower(CAST(json_extract(identifier.value, '$.identifier.value') AS TEXT)), ?1)=1
               OR instr(lower(json_extract(identifier.value, '$.identifier.kind')), ?1)=1 THEN 1
             ELSE 2
           END AS rank
    FROM current_definitions AS current,
         json_each(current.definition_json, '$.identifiers') AS identifier
    WHERE json_extract(identifier.value, '$.assignment_verification')<>'verified_unassigned'
      AND json_extract(identifier.value, '$.rights_policy.entitlement')<>'unknown_or_restricted'
      AND (
        instr(lower(CAST(json_extract(identifier.value, '$.identifier.value') AS TEXT)), ?1)>0
        OR instr(lower(json_extract(identifier.value, '$.identifier.kind')), ?1)>0
      )

    UNION ALL

    SELECT current.instrument_id,
           CASE
             WHEN lower(json_extract(provider.value, '$.provider_instrument_id'))=?1
               OR lower(json_extract(provider.value, '$.source_id'))=?1 THEN 0
             WHEN instr(lower(json_extract(provider.value, '$.provider_instrument_id')), ?1)=1
               OR instr(lower(json_extract(provider.value, '$.source_id')), ?1)=1 THEN 1
             ELSE 2
           END AS rank
    FROM current_definitions AS current,
         json_each(current.definition_json, '$.provider_identity_registry.accepted') AS provider
    WHERE instr(lower(json_extract(provider.value, '$.provider_instrument_id')), ?1)>0
       OR instr(lower(json_extract(provider.value, '$.source_id')), ?1)>0

    UNION ALL

    SELECT instrument_id,
           CASE
             WHEN lower(venue_symbol)=?1 OR lower(venue_id)=?1 THEN 0
             WHEN instr(lower(venue_symbol), ?1)=1 OR instr(lower(venue_id), ?1)=1 THEN 1
             ELSE 2
           END AS rank
    FROM symbol_history
    WHERE instr(lower(venue_symbol), ?1)>0 OR instr(lower(venue_id), ?1)>0
), ranked AS (
    SELECT instrument_id, MIN(rank) AS rank
    FROM candidate_ranks
    GROUP BY instrument_id
)
SELECT
    current.instrument_id,
    current.definition_json,
    current.revision_digest,
    current.current_observed_at_ns
FROM ranked
JOIN current_definitions AS current USING (instrument_id)
ORDER BY ranked.rank, current.instrument_id
LIMIT ?2
"#;
