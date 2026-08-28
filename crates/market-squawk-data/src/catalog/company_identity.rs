//! Atomic company-identity publication and bounded discovery.

use std::time::Instant;

use market_squawk_domain::{CompanyIdentityObservation, Timestamp};
use rusqlite::{OptionalExtension as _, Transaction, params};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::storage::{ResultBudget, append_audit, parse_digest, sha256};
use super::{Catalog, CatalogError, CatalogLimit, IngestReservation};

const MAXIMUM_SEARCH_QUERY_BYTES: usize = 256;
const MAXIMUM_MATCH_REASONS: usize = 8;
const SQLITE_PROGRESS_OPERATIONS: i32 = 1_000;

/// Provider field that matched a bounded company search.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompanyIdentityMatchKind {
    CurrentName,
    FormerName,
    ProviderCompanyId,
    Ticker,
    Exchange,
    EntityType,
    Sic,
    SicDescription,
}

impl CompanyIdentityMatchKind {
    const fn database_name(self) -> &'static str {
        match self {
            Self::CurrentName => "current_name",
            Self::FormerName => "former_name",
            Self::ProviderCompanyId => "provider_company_id",
            Self::Ticker => "ticker",
            Self::Exchange => "exchange",
            Self::EntityType => "entity_type",
            Self::Sic => "sic",
            Self::SicDescription => "sic_description",
        }
    }

    fn from_database(value: &str) -> Result<Self, CatalogError> {
        match value {
            "current_name" => Ok(Self::CurrentName),
            "former_name" => Ok(Self::FormerName),
            "provider_company_id" => Ok(Self::ProviderCompanyId),
            "ticker" => Ok(Self::Ticker),
            "exchange" => Ok(Self::Exchange),
            "entity_type" => Ok(Self::EntityType),
            "sic" => Ok(Self::Sic),
            "sic_description" => Ok(Self::SicDescription),
            _ => Err(CatalogError::CorruptCatalog),
        }
    }
}

/// One exact field-level reason a company identity matched the query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanyIdentityMatchReason {
    kind: CompanyIdentityMatchKind,
    value: String,
    association_ordinal: Option<usize>,
}

impl CompanyIdentityMatchReason {
    /// Returns the provider field that matched.
    pub const fn kind(&self) -> CompanyIdentityMatchKind {
        self.kind
    }
    /// Returns the exact provider value that matched.
    pub fn value(&self) -> &str {
        &self.value
    }
    /// Returns the paired ticker/exchange ordinal when applicable.
    pub const fn association_ordinal(&self) -> Option<usize> {
        self.association_ordinal
    }
}

/// One current, digest-verified company identity plus its bounded match evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanyIdentitySearchMatch {
    observation: CompanyIdentityObservation,
    completed_at: Timestamp,
    reasons: Box<[CompanyIdentityMatchReason]>,
    reasons_truncated: bool,
}

impl CompanyIdentitySearchMatch {
    /// Returns the current digest-verified observation.
    pub const fn observation(&self) -> &CompanyIdentityObservation {
        &self.observation
    }
    /// Returns when the owning research ingest completed.
    pub const fn completed_at(&self) -> Timestamp {
        self.completed_at
    }
    /// Returns bounded field-level match evidence.
    pub fn reasons(&self) -> &[CompanyIdentityMatchReason] {
        &self.reasons
    }
    /// Returns whether additional field-level reasons were omitted.
    pub const fn reasons_truncated(&self) -> bool {
        self.reasons_truncated
    }
}

/// One deterministic bounded company search page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanyIdentitySearchPage {
    matches: Box<[CompanyIdentitySearchMatch]>,
    has_more: bool,
}

impl CompanyIdentitySearchPage {
    /// Returns deterministic current company matches.
    pub fn matches(&self) -> &[CompanyIdentitySearchMatch] {
        &self.matches
    }
    /// Returns whether more company matches exist beyond the page.
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

impl Catalog {
    /// Searches current successful company-identity observations without granting instrument use.
    pub fn search_company_identities(
        &self,
        query: &str,
        limit: CatalogLimit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<CompanyIdentitySearchPage, CatalogError> {
        self.enforce_limit(limit)?;
        let query = query.trim();
        if query.is_empty()
            || query.len() > MAXIMUM_SEARCH_QUERY_BYTES
            || query.chars().any(char::is_control)
        {
            return Err(CatalogError::InvalidRecord);
        }
        check_search(deadline, cancellation)?;
        let normalized = query.to_lowercase();
        let retrieval_limit = i64::try_from(
            limit
                .get()
                .checked_add(1)
                .ok_or(CatalogError::InvalidLimit)?,
        )
        .map_err(|_| CatalogError::InvalidLimit)?;
        let token = cancellation.clone();
        self.connection.progress_handler(
            SQLITE_PROGRESS_OPERATIONS,
            Some(move || token.is_cancelled() || Instant::now() >= deadline),
        )?;
        let result = (|| {
            let mut statement = self.connection.prepare(CURRENT_COMPANY_SEARCH_SQL)?;
            let rows = statement.query_map(params![normalized, retrieval_limit], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            })?;
            let mut budget = ResultBudget::new(self.result_bytes);
            let mut matches = Vec::new();
            matches
                .try_reserve_exact(limit.get().saturating_add(1))
                .map_err(|_| CatalogError::Allocation)?;
            for row in rows {
                check_search(deadline, cancellation)?;
                let (
                    json,
                    digest,
                    completed_at,
                    source_id,
                    provider_company_id,
                    source_surface,
                    received_at,
                    available_at,
                    ingested_at,
                ) = row?;
                let observation: CompanyIdentityObservation =
                    super::records::deserialize_verified(&json, &digest, &mut budget)?;
                if observation.source_id().as_str() != source_id
                    || observation.provider_company_id().as_str() != provider_company_id
                    || observation.surface().database_name() != source_surface
                    || observation.received_at().unix_nanos() != received_at
                    || observation
                        .availability()
                        .conservative_available_at()
                        .map(Timestamp::unix_nanos)
                        != available_at
                    || observation.ingested_at().unix_nanos() != ingested_at
                {
                    return Err(CatalogError::CorruptCatalog);
                }
                let (reasons, reasons_truncated) = self.company_match_reasons(
                    &digest,
                    &normalized,
                    &observation,
                    &mut budget,
                    deadline,
                    cancellation,
                )?;
                if reasons.is_empty() {
                    return Err(CatalogError::CorruptCatalog);
                }
                matches.push(CompanyIdentitySearchMatch {
                    observation,
                    completed_at: Timestamp::from_unix_nanos(completed_at),
                    reasons: reasons.into_boxed_slice(),
                    reasons_truncated,
                });
            }
            let has_more = matches.len() > limit.get();
            matches.truncate(limit.get());
            Ok(CompanyIdentitySearchPage {
                matches: matches.into_boxed_slice(),
                has_more,
            })
        })();
        self.connection.progress_handler::<fn() -> bool>(0, None)?;
        result.map_err(|error| classify_search_error(error, deadline, cancellation))
    }

    fn company_match_reasons(
        &self,
        digest: &[u8],
        query: &str,
        observation: &CompanyIdentityObservation,
        budget: &mut ResultBudget,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(Vec<CompanyIdentityMatchReason>, bool), CatalogError> {
        let retrieval_limit = i64::try_from(MAXIMUM_MATCH_REASONS.saturating_add(1))
            .map_err(|_| CatalogError::InvalidLimit)?;
        let mut statement = self.connection.prepare(MATCHING_COMPANY_TERMS_SQL)?;
        let rows = statement.query_map(params![digest, query, retrieval_limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<i64>>(2)?,
            ))
        })?;
        let mut reasons = Vec::new();
        reasons
            .try_reserve_exact(MAXIMUM_MATCH_REASONS.saturating_add(1))
            .map_err(|_| CatalogError::Allocation)?;
        for row in rows {
            check_search(deadline, cancellation)?;
            let (kind, value, association_ordinal) = row?;
            budget.charge([kind.len(), value.len()])?;
            let kind = CompanyIdentityMatchKind::from_database(&kind)?;
            let association_ordinal = association_ordinal
                .map(usize::try_from)
                .transpose()
                .map_err(|_| CatalogError::CorruptCatalog)?;
            if !valid_match_reason(observation, kind, &value, association_ordinal) {
                return Err(CatalogError::CorruptCatalog);
            }
            reasons.push(CompanyIdentityMatchReason {
                kind,
                value,
                association_ordinal,
            });
        }
        let truncated = reasons.len() > MAXIMUM_MATCH_REASONS;
        reasons.truncate(MAXIMUM_MATCH_REASONS);
        Ok((reasons, truncated))
    }
}

fn valid_match_reason(
    observation: &CompanyIdentityObservation,
    kind: CompanyIdentityMatchKind,
    value: &str,
    association_ordinal: Option<usize>,
) -> bool {
    match kind {
        CompanyIdentityMatchKind::CurrentName => {
            association_ordinal.is_none() && observation.conformed_name() == value
        }
        CompanyIdentityMatchKind::FormerName => {
            association_ordinal.is_none()
                && observation
                    .former_names()
                    .iter()
                    .any(|former| former.name() == value)
        }
        CompanyIdentityMatchKind::ProviderCompanyId => {
            association_ordinal.is_none() && observation.provider_company_id().as_str() == value
        }
        CompanyIdentityMatchKind::Ticker => association_ordinal.is_some_and(|ordinal| {
            observation
                .associations()
                .get(ordinal)
                .is_some_and(|association| association.ticker() == value)
        }),
        CompanyIdentityMatchKind::Exchange => association_ordinal.is_some_and(|ordinal| {
            observation
                .associations()
                .get(ordinal)
                .is_some_and(|association| association.exchange() == value)
        }),
        CompanyIdentityMatchKind::EntityType => {
            association_ordinal.is_none() && observation.entity_type() == Some(value)
        }
        CompanyIdentityMatchKind::Sic => {
            association_ordinal.is_none() && observation.sic() == Some(value)
        }
        CompanyIdentityMatchKind::SicDescription => {
            association_ordinal.is_none() && observation.sic_description() == Some(value)
        }
    }
}

pub(super) fn persist_company_identity(
    transaction: &Transaction<'_>,
    reservation: &IngestReservation,
    observation: &CompanyIdentityObservation,
    catalog_now: Timestamp,
) -> Result<(), CatalogError> {
    let (source_id, payload_algorithm, payload_digest, state): (String, i64, Vec<u8>, String) =
        transaction.query_row(
            "SELECT source_id, payload_algorithm, payload_digest, state
             FROM ingest_runs WHERE run_id=?1",
            [reservation.run_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
    let retained_payload = parse_digest(payload_algorithm, &payload_digest)?;
    if source_id != observation.source_id().as_str()
        || retained_payload
            != observation
                .parent_ingest_payload_evidence()
                .content_digest()
        || !matches!(state.as_str(), "reserved" | "succeeded")
    {
        return Err(CatalogError::EvidenceConflict);
    }
    let manifest_id: String = transaction.query_row(
        "SELECT dataset_manifests.manifest_id
         FROM artifacts
         JOIN dataset_manifests USING (artifact_id)
         WHERE artifacts.run_id=?1",
        [reservation.run_id.to_string()],
        |row| row.get(0),
    )?;
    let json = serde_json::to_string(observation)?;
    let digest = sha256(json.as_bytes());
    let existing: Option<(String, Vec<u8>)> = transaction
        .query_row(
            "SELECT record_json, record_digest FROM company_identity_observations
             WHERE run_id=?1",
            [reservation.run_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((existing_json, existing_digest)) = existing {
        let existing_observation: CompanyIdentityObservation =
            serde_json::from_str(&existing_json).map_err(|_| CatalogError::CorruptCatalog)?;
        return if existing_digest.as_slice() == sha256(existing_json.as_bytes())
            && existing_observation.same_source_representation(observation)
        {
            Ok(())
        } else {
            Err(CatalogError::EvidenceConflict)
        };
    }
    transaction.execute(
        "INSERT INTO company_identity_observations
         (record_digest, run_id, manifest_id, source_id, source_surface, provider_company_id,
          record_json, received_at_ns, available_at_ns, ingested_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            digest,
            reservation.run_id.to_string(),
            manifest_id,
            observation.source_id().as_str(),
            observation.surface().database_name(),
            observation.provider_company_id().as_str(),
            json,
            observation.received_at().unix_nanos(),
            observation
                .availability()
                .conservative_available_at()
                .map(Timestamp::unix_nanos),
            observation.ingested_at().unix_nanos(),
        ],
    )?;
    for (ordinal, term) in company_search_terms(observation)?.into_iter().enumerate() {
        transaction.execute(
            "INSERT INTO company_identity_search_terms
             (record_digest, ordinal, term_kind, display_value, normalized_value,
              association_ordinal)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                digest,
                i64::try_from(ordinal).map_err(|_| CatalogError::InvalidRecord)?,
                term.kind.database_name(),
                term.display,
                term.normalized,
                term.association_ordinal
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| CatalogError::InvalidRecord)?,
            ],
        )?;
    }
    append_audit(
        transaction,
        "company-identity.recorded",
        observation.provider_company_id().as_str(),
        digest,
        catalog_now,
    )?;
    Ok(())
}

pub(super) fn validate_provider_company_identity_replay(
    transaction: &Transaction<'_>,
    reservation: &IngestReservation,
    observation: Option<&CompanyIdentityObservation>,
) -> Result<(), CatalogError> {
    let existing: Option<(String, Vec<u8>)> = transaction
        .query_row(
            "SELECT record_json, record_digest FROM company_identity_observations
             WHERE run_id=?1",
            [reservation.run_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    match (existing, observation) {
        (None, None) => Ok(()),
        (Some((json, digest)), Some(observation)) => {
            let retained: CompanyIdentityObservation =
                serde_json::from_str(&json).map_err(|_| CatalogError::CorruptCatalog)?;
            if digest.as_slice() != sha256(json.as_bytes()) {
                return Err(CatalogError::CorruptCatalog);
            }
            if retained == *observation {
                Ok(())
            } else {
                Err(CatalogError::EvidenceConflict)
            }
        }
        (None, Some(_)) | (Some(_), None) => Err(CatalogError::EvidenceConflict),
    }
}

struct CompanySearchTerm {
    kind: CompanyIdentityMatchKind,
    display: String,
    normalized: String,
    association_ordinal: Option<usize>,
}

fn company_search_terms(
    observation: &CompanyIdentityObservation,
) -> Result<Vec<CompanySearchTerm>, CatalogError> {
    let capacity = 5_usize
        .checked_add(observation.former_names().len())
        .and_then(|value| {
            observation
                .associations()
                .len()
                .checked_mul(2)
                .and_then(|associations| value.checked_add(associations))
        })
        .ok_or(CatalogError::Allocation)?;
    let mut terms = Vec::new();
    terms
        .try_reserve_exact(capacity)
        .map_err(|_| CatalogError::Allocation)?;
    push_term(
        &mut terms,
        CompanyIdentityMatchKind::ProviderCompanyId,
        observation.provider_company_id().as_str(),
        None,
    )?;
    push_term(
        &mut terms,
        CompanyIdentityMatchKind::CurrentName,
        observation.conformed_name(),
        None,
    )?;
    for former in observation.former_names() {
        push_term(
            &mut terms,
            CompanyIdentityMatchKind::FormerName,
            former.name(),
            None,
        )?;
    }
    for (ordinal, association) in observation.associations().iter().enumerate() {
        push_term(
            &mut terms,
            CompanyIdentityMatchKind::Ticker,
            association.ticker(),
            Some(ordinal),
        )?;
        push_term(
            &mut terms,
            CompanyIdentityMatchKind::Exchange,
            association.exchange(),
            Some(ordinal),
        )?;
    }
    for (kind, value) in [
        (
            CompanyIdentityMatchKind::EntityType,
            observation.entity_type(),
        ),
        (CompanyIdentityMatchKind::Sic, observation.sic()),
        (
            CompanyIdentityMatchKind::SicDescription,
            observation.sic_description(),
        ),
    ] {
        if let Some(value) = value {
            push_term(&mut terms, kind, value, None)?;
        }
    }
    Ok(terms)
}

fn push_term(
    terms: &mut Vec<CompanySearchTerm>,
    kind: CompanyIdentityMatchKind,
    value: &str,
    association_ordinal: Option<usize>,
) -> Result<(), CatalogError> {
    let normalized = value.to_lowercase();
    if normalized.is_empty() || normalized.len() > 2_048 {
        return Err(CatalogError::InvalidRecord);
    }
    terms.push(CompanySearchTerm {
        kind,
        display: value.to_owned(),
        normalized,
        association_ordinal,
    });
    Ok(())
}

fn check_search(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::CompanyIdentityReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::CompanyIdentityReadDeadlineExceeded)
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
        CatalogError::CompanyIdentityReadCancelled
    } else if Instant::now() >= deadline {
        CatalogError::CompanyIdentityReadDeadlineExceeded
    } else {
        error
    }
}

const CURRENT_COMPANY_SEARCH_SQL: &str = r#"
WITH ranked AS (
    SELECT observations.record_json,
           observations.record_digest,
           runs.completed_at_ns,
           observations.source_id,
           observations.provider_company_id,
           observations.source_surface,
           observations.received_at_ns,
           observations.available_at_ns,
           observations.ingested_at_ns,
           ROW_NUMBER() OVER (
               PARTITION BY observations.source_id,
                            observations.provider_company_id,
                            observations.source_surface
               ORDER BY runs.completed_at_ns DESC,
                        observations.ingested_at_ns DESC,
                        hex(observations.record_digest)
           ) AS current_rank
    FROM company_identity_observations AS observations
    JOIN ingest_runs AS runs ON runs.run_id=observations.run_id
    WHERE runs.state='succeeded'
)
SELECT ranked.record_json, ranked.record_digest, ranked.completed_at_ns
     , ranked.source_id, ranked.provider_company_id, ranked.source_surface
     , ranked.received_at_ns, ranked.available_at_ns, ranked.ingested_at_ns
FROM ranked
WHERE ranked.current_rank=1
  AND EXISTS (
      SELECT 1 FROM company_identity_search_terms AS terms
      WHERE terms.record_digest=ranked.record_digest
        AND instr(terms.normalized_value, ?1)>0
  )
ORDER BY (
    SELECT MIN(CASE
        WHEN terms.normalized_value=?1 THEN 0
        WHEN instr(terms.normalized_value, ?1)=1 THEN 1
        ELSE 2
    END)
    FROM company_identity_search_terms AS terms
    WHERE terms.record_digest=ranked.record_digest
      AND instr(terms.normalized_value, ?1)>0
), ranked.provider_company_id, ranked.source_surface
LIMIT ?2
"#;

const MATCHING_COMPANY_TERMS_SQL: &str = r#"
SELECT term_kind, display_value, association_ordinal
FROM company_identity_search_terms
WHERE record_digest=?1 AND instr(normalized_value, ?2)>0
ORDER BY CASE
    WHEN normalized_value=?2 THEN 0
    WHEN instr(normalized_value, ?2)=1 THEN 1
    ELSE 2
END, ordinal
LIMIT ?3
"#;
