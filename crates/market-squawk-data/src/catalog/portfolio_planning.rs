//! Immutable portfolio calculation references and separate saved markers on the sole catalog.

use std::sync::{Arc, Mutex};

use market_squawk_domain::{AccountId, Timestamp};
use rusqlite::{Connection, Row, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::CatalogAuthority;

const PAGE_ROWS: usize = 32;
const COMPLETION_COLUMNS: &str = "c.sequence,c.calculation_token,c.account_id,c.kind,c.snapshot_token,c.calculated_at_ns,c.portfolio_effective_at_ns,c.portfolio_available_at_ns,c.artifact_id,c.artifact_sha256,c.artifact_byte_length,c.artifact_media_type,c.record_sha256,c.chain_sha256";
const SAVE_COLUMNS: &str = "s.sequence,s.account_id,s.saved_at_ns,s.record_sha256,s.chain_sha256";

/// Closed successful calculation families; artifact interpretation remains application-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioPlanningKind {
    /// One scenario calculation.
    Scenario,
    /// One completed scenario batch.
    ScenarioBatch,
    /// A proposal-only rebalance.
    Rebalance,
    /// A completed comparison of the portfolio and a candidate position.
    PositionComparison,
}

impl PortfolioPlanningKind {
    fn name(self) -> &'static str {
        match self {
            Self::Scenario => "scenario",
            Self::ScenarioBatch => "scenario_batch",
            Self::Rebalance => "rebalance",
            Self::PositionComparison => "position_comparison",
        }
    }
}

/// One immutable append-only population prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioPlanningChainHead {
    /// Last committed sequence, or zero for the empty prefix.
    pub sequence: u64,
    /// Chained identity through that sequence.
    pub sha256: [u8; 32],
}

/// Retained completion and save fences for streaming backup and stable saved-result pages.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioPlanningHead {
    /// Includes completed calculations that have never been saved.
    pub completions: PortfolioPlanningChainHead,
    /// Independent append-only saved marker population.
    pub saves: PortfolioPlanningChainHead,
}

impl PortfolioPlanningHead {
    /// Identity of an empty portfolio planning inventory.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            completions: empty_head(false),
            saves: empty_head(true),
        }
    }
}

/// Small immutable coordinates of an application-verified completed artifact.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioPlanningCompletion {
    /// Exact opaque calculation identity minted by the application.
    pub calculation_token: Uuid,
    /// Account that supplied the original portfolio.
    pub account_id: AccountId,
    /// Successful operation family.
    pub kind: PortfolioPlanningKind,
    /// Original immutable portfolio snapshot token.
    pub snapshot_token: Uuid,
    /// Original calculation time.
    pub calculated_at: Timestamp,
    /// Original portfolio effective time.
    pub portfolio_effective_at: Timestamp,
    /// Original portfolio availability time.
    pub portfolio_available_at: Option<Timestamp>,
    /// Opaque controlled artifact identity; never a filesystem path.
    pub artifact_id: String,
    /// SHA-256 of the exact immutable artifact bytes.
    pub artifact_sha256: [u8; 32],
    /// Length of the exact immutable artifact bytes.
    pub artifact_byte_length: u64,
    /// Original controlled artifact media type.
    pub artifact_media_type: String,
}

impl PortfolioPlanningCompletion {
    fn validate(&self) -> Result<(), PortfolioPlanningError> {
        if self.calculation_token.is_nil()
            || self.snapshot_token.is_nil()
            || self.artifact_id.is_empty()
            || self.artifact_id.len() > 160
            || !self
                .artifact_id
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || !self
                .artifact_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            || self.artifact_media_type.is_empty()
            || self.artifact_media_type.len() > 128
            || !self.artifact_media_type.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'+' | b'-')
            })
            || self.artifact_byte_length == 0
            || self.artifact_byte_length > i64::MAX as u64
        {
            return Err(PortfolioPlanningError::InvalidRecord);
        }
        Ok(())
    }

    fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/portfolio-planning-completion/v1");
        hash.update(self.calculation_token.as_bytes());
        hash.update(self.account_id.as_uuid().as_bytes());
        text_digest(&mut hash, self.kind.name());
        hash.update(self.snapshot_token.as_bytes());
        for timestamp in [self.calculated_at, self.portfolio_effective_at] {
            hash.update(timestamp.unix_nanos().to_be_bytes());
        }
        match self.portfolio_available_at {
            None => hash.update([0]),
            Some(timestamp) => {
                hash.update([1]);
                hash.update(timestamp.unix_nanos().to_be_bytes());
            }
        }
        text_digest(&mut hash, &self.artifact_id);
        hash.update(self.artifact_sha256);
        hash.update(self.artifact_byte_length.to_be_bytes());
        text_digest(&mut hash, &self.artifact_media_type);
        hash.finalize().into()
    }
}

/// One completion and its immutable publication prefix.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioPlanningCompletionEntry {
    /// Original metadata, with payload demand-loaded by the application.
    pub completion: PortfolioPlanningCompletion,
    /// Completion prefix through this record.
    pub head: PortfolioPlanningChainHead,
}

/// A saved marker tied to the exact original completion.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PortfolioPlanningSavedEntry {
    /// Exact successful calculation; saving does not replace it.
    pub completion: PortfolioPlanningCompletionEntry,
    /// Original accepted Save time, retained on retries.
    pub saved_at: Timestamp,
    /// Saved-marker prefix through the original Save.
    pub head: PortfolioPlanningChainHead,
}

/// Cloneable narrow access to the composition-owned catalog; it never opens another writer.
#[derive(Clone)]
pub struct PortfolioPlanningCatalogCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl std::fmt::Debug for PortfolioPlanningCatalogCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PortfolioPlanningCatalogCapability([CATALOG AUTHORITY])")
    }
}

impl PortfolioPlanningCatalogCapability {
    pub(crate) const fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }

    /// Reads both inventory heads in one catalog snapshot without loading artifact history.
    pub fn head(&self) -> Result<PortfolioPlanningHead, PortfolioPlanningError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        let value = PortfolioPlanningHead {
            completions: read_head(&tx, false)?,
            saves: read_head(&tx, true)?,
        };
        tx.commit()?;
        Ok(value)
    }

    /// Publishes one original completed artifact reference, accepting only an exact replay.
    pub fn complete(
        &self,
        value: &PortfolioPlanningCompletion,
    ) -> Result<(PortfolioPlanningCompletionEntry, bool), PortfolioPlanningError> {
        value.validate()?;
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        if let Some(existing) = read_completion(&tx, None, value.calculation_token)? {
            if existing.completion != *value {
                return Err(PortfolioPlanningError::Conflict);
            }
            tx.commit()?;
            return Ok((existing, false));
        }
        let previous = read_head(&tx, false)?;
        let next = successor(previous, value.digest())?;
        tx.execute(
            "INSERT INTO portfolio_planning_completions(sequence,calculation_token,account_id,kind,snapshot_token,calculated_at_ns,portfolio_effective_at_ns,portfolio_available_at_ns,artifact_id,artifact_sha256,artifact_byte_length,artifact_media_type,record_sha256,chain_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![sql(next.sequence)?,value.calculation_token.to_string(),value.account_id.to_string(),value.kind.name(),value.snapshot_token.to_string(),value.calculated_at.unix_nanos(),value.portfolio_effective_at.unix_nanos(),value.portfolio_available_at.map(Timestamp::unix_nanos),value.artifact_id,value.artifact_sha256.as_slice(),sql(value.artifact_byte_length)?,value.artifact_media_type,value.digest().as_slice(),next.sha256.as_slice()],
        )?;
        tx.commit()?;
        Ok((
            PortfolioPlanningCompletionEntry {
                completion: value.clone(),
                head: next,
            },
            true,
        ))
    }

    /// Resolves one exact completion for its original account, including unsaved calculations.
    pub fn completion(
        &self,
        account_id: AccountId,
        calculation_token: Uuid,
    ) -> Result<Option<PortfolioPlanningCompletionEntry>, PortfolioPlanningError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        read_completion(
            &authority.catalog().connection,
            Some(account_id),
            calculation_token,
        )
    }

    /// Commits a separate saved marker or returns the original marker on every retry.
    pub fn save(
        &self,
        account_id: AccountId,
        calculation_token: Uuid,
        saved_at: Timestamp,
    ) -> Result<PortfolioPlanningSavedEntry, PortfolioPlanningError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        let tx = authority.catalog().connection.unchecked_transaction()?;
        if let Some(existing) = read_saved(&tx, account_id, calculation_token)? {
            tx.commit()?;
            return Ok(existing);
        }
        let completion = read_completion(&tx, Some(account_id), calculation_token)?
            .ok_or(PortfolioPlanningError::NotFound)?;
        let digest = save_digest(&completion.completion, saved_at);
        let next = successor(read_head(&tx, true)?, digest)?;
        tx.execute(
            "INSERT INTO portfolio_planning_saves(sequence,calculation_token,account_id,saved_at_ns,record_sha256,chain_sha256) VALUES(?1,?2,?3,?4,?5,?6)",
            params![sql(next.sequence)?,calculation_token.to_string(),account_id.to_string(),saved_at.unix_nanos(),digest.as_slice(),next.sha256.as_slice()],
        )?;
        tx.commit()?;
        Ok(PortfolioPlanningSavedEntry {
            completion,
            saved_at,
            head: next,
        })
    }

    /// Resolves one exact saved calculation for its original account.
    pub fn saved(
        &self,
        account_id: AccountId,
        calculation_token: Uuid,
    ) -> Result<Option<PortfolioPlanningSavedEntry>, PortfolioPlanningError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        read_saved(
            &authority.catalog().connection,
            account_id,
            calculation_token,
        )
    }

    /// Reads one saved-only account page from the exact retained prefix, in Save order.
    pub fn saved_page(
        &self,
        account_id: AccountId,
        fence: PortfolioPlanningHead,
        after: u64,
        limit: usize,
    ) -> Result<Vec<PortfolioPlanningSavedEntry>, PortfolioPlanningError> {
        self.saved_rows(fence, after, limit, Some(account_id))
    }

    /// Streams all saved markers for backup in fixed working pages.
    pub fn save_page(
        &self,
        fence: PortfolioPlanningHead,
        after: u64,
    ) -> Result<Vec<PortfolioPlanningSavedEntry>, PortfolioPlanningError> {
        self.saved_rows(fence, after, PAGE_ROWS, None)
    }

    fn saved_rows(
        &self,
        fence: PortfolioPlanningHead,
        after: u64,
        limit: usize,
        account_id: Option<AccountId>,
    ) -> Result<Vec<PortfolioPlanningSavedEntry>, PortfolioPlanningError> {
        if limit == 0 || limit > PAGE_ROWS || after > fence.saves.sequence {
            return Err(PortfolioPlanningError::InvalidRecord);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        let c = &authority.catalog().connection;
        verify_fence(c, fence)?;
        let account_scope = if account_id.is_some() {
            "s.account_id=?3"
        } else {
            "?3 IS NULL"
        };
        let mut statement = c.prepare(&format!(
            "SELECT {COMPLETION_COLUMNS},{SAVE_COLUMNS} FROM portfolio_planning_saves s JOIN portfolio_planning_completions c ON c.calculation_token=s.calculation_token AND c.account_id=s.account_id WHERE s.sequence>?1 AND s.sequence<=?2 AND {account_scope} ORDER BY s.sequence LIMIT ?4"
        ))?;
        let mut rows = statement.query(params![
            sql(after)?,
            sql(fence.saves.sequence)?,
            account_id.map(|value| value.to_string()),
            limit as i64
        ])?;
        let mut entries = Vec::with_capacity(limit);
        while let Some(row) = rows.next()? {
            let entry = decode_saved(c, row)?;
            if entry.completion.head.sequence > fence.completions.sequence {
                return Err(PortfolioPlanningError::Corrupt);
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Streams every completed calculation, including unsaved ones, without loading payloads.
    pub fn completion_page(
        &self,
        fence: PortfolioPlanningHead,
        after: u64,
    ) -> Result<Vec<PortfolioPlanningCompletionEntry>, PortfolioPlanningError> {
        if after > fence.completions.sequence {
            return Err(PortfolioPlanningError::InvalidRecord);
        }
        let authority = self
            .authority
            .lock()
            .map_err(|_| PortfolioPlanningError::Unavailable)?;
        let c = &authority.catalog().connection;
        verify_fence(c, fence)?;
        let mut statement = c.prepare(&format!(
            "SELECT {COMPLETION_COLUMNS} FROM portfolio_planning_completions c WHERE c.sequence>?1 AND c.sequence<=?2 ORDER BY c.sequence LIMIT ?3"
        ))?;
        let mut rows = statement.query(params![
            sql(after)?,
            sql(fence.completions.sequence)?,
            PAGE_ROWS as i64
        ])?;
        let mut entries = Vec::with_capacity(PAGE_ROWS);
        while let Some(row) = rows.next()? {
            entries.push(decode_completion(c, row)?);
        }
        Ok(entries)
    }

    /// Verifies both complete hash chains with bounded pages and no artifact materialization.
    pub fn verify(&self, fence: PortfolioPlanningHead) -> Result<(), PortfolioPlanningError> {
        let mut completion_head = empty_head(false);
        loop {
            let page = self.completion_page(fence, completion_head.sequence)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                if entry.head != successor(completion_head, entry.completion.digest())? {
                    return Err(PortfolioPlanningError::Corrupt);
                }
                completion_head = entry.head;
            }
        }
        let mut save_head = empty_head(true);
        loop {
            let page = self.save_page(fence, save_head.sequence)?;
            if page.is_empty() {
                break;
            }
            for entry in page {
                if entry.head
                    != successor(
                        save_head,
                        save_digest(&entry.completion.completion, entry.saved_at),
                    )?
                {
                    return Err(PortfolioPlanningError::Corrupt);
                }
                save_head = entry.head;
            }
        }
        if completion_head != fence.completions || save_head != fence.saves {
            return Err(PortfolioPlanningError::Corrupt);
        }
        Ok(())
    }
}

fn read_completion(
    c: &Connection,
    account: Option<AccountId>,
    token: Uuid,
) -> Result<Option<PortfolioPlanningCompletionEntry>, PortfolioPlanningError> {
    let mut statement = c.prepare(&format!(
        "SELECT {COMPLETION_COLUMNS} FROM portfolio_planning_completions c WHERE c.calculation_token=?1 AND (?2 IS NULL OR c.account_id=?2)"
    ))?;
    let mut rows = statement.query(params![
        token.to_string(),
        account.map(|value| value.to_string())
    ])?;
    rows.next()?
        .map(|row| decode_completion(c, row))
        .transpose()
}

fn read_saved(
    c: &Connection,
    account: AccountId,
    token: Uuid,
) -> Result<Option<PortfolioPlanningSavedEntry>, PortfolioPlanningError> {
    let mut statement = c.prepare(&format!(
        "SELECT {COMPLETION_COLUMNS},{SAVE_COLUMNS} FROM portfolio_planning_saves s JOIN portfolio_planning_completions c ON c.calculation_token=s.calculation_token AND c.account_id=s.account_id WHERE s.calculation_token=?1 AND s.account_id=?2"
    ))?;
    let mut rows = statement.query(params![token.to_string(), account.to_string()])?;
    rows.next()?.map(|row| decode_saved(c, row)).transpose()
}

fn decode_completion(
    c: &Connection,
    row: &Row<'_>,
) -> Result<PortfolioPlanningCompletionEntry, PortfolioPlanningError> {
    let kind = match bounded_text(row, 3, 32)?.as_str() {
        "scenario" => PortfolioPlanningKind::Scenario,
        "scenario_batch" => PortfolioPlanningKind::ScenarioBatch,
        "rebalance" => PortfolioPlanningKind::Rebalance,
        "position_comparison" => PortfolioPlanningKind::PositionComparison,
        _ => return Err(PortfolioPlanningError::Corrupt),
    };
    let completion = PortfolioPlanningCompletion {
        calculation_token: bounded_text(row, 1, 36)?
            .parse()
            .map_err(|_| PortfolioPlanningError::Corrupt)?,
        account_id: bounded_text(row, 2, 36)?
            .parse()
            .map_err(|_| PortfolioPlanningError::Corrupt)?,
        kind,
        snapshot_token: bounded_text(row, 4, 36)?
            .parse()
            .map_err(|_| PortfolioPlanningError::Corrupt)?,
        calculated_at: Timestamp::from_unix_nanos(row.get(5)?),
        portfolio_effective_at: Timestamp::from_unix_nanos(row.get(6)?),
        portfolio_available_at: row
            .get::<_, Option<i64>>(7)?
            .map(Timestamp::from_unix_nanos),
        artifact_id: bounded_text(row, 8, 160)?,
        artifact_sha256: digest_column(row, 9)?,
        artifact_byte_length: unsigned(row.get(10)?)?,
        artifact_media_type: bounded_text(row, 11, 128)?,
    };
    completion
        .validate()
        .map_err(|_| PortfolioPlanningError::Corrupt)?;
    let digest = completion.digest();
    if digest_column(row, 12)? != digest {
        return Err(PortfolioPlanningError::Corrupt);
    }
    let head = PortfolioPlanningChainHead {
        sequence: unsigned(row.get(0)?)?,
        sha256: digest_column(row, 13)?,
    };
    verify_entry(c, false, head, digest)?;
    Ok(PortfolioPlanningCompletionEntry { completion, head })
}

fn decode_saved(
    c: &Connection,
    row: &Row<'_>,
) -> Result<PortfolioPlanningSavedEntry, PortfolioPlanningError> {
    let completion = decode_completion(c, row)?;
    let account: AccountId = bounded_text(row, 15, 36)?
        .parse()
        .map_err(|_| PortfolioPlanningError::Corrupt)?;
    if account != completion.completion.account_id {
        return Err(PortfolioPlanningError::Corrupt);
    }
    let saved_at = Timestamp::from_unix_nanos(row.get(16)?);
    let digest = save_digest(&completion.completion, saved_at);
    if digest_column(row, 17)? != digest {
        return Err(PortfolioPlanningError::Corrupt);
    }
    let head = PortfolioPlanningChainHead {
        sequence: unsigned(row.get(14)?)?,
        sha256: digest_column(row, 18)?,
    };
    verify_entry(c, true, head, digest)?;
    Ok(PortfolioPlanningSavedEntry {
        completion,
        saved_at,
        head,
    })
}

fn bounded_text(
    row: &Row<'_>,
    column: usize,
    maximum: usize,
) -> Result<String, PortfolioPlanningError> {
    let bytes = row
        .get_ref(column)?
        .as_str()
        .map_err(|_| PortfolioPlanningError::Corrupt)?;
    if bytes.is_empty() || bytes.len() > maximum {
        return Err(PortfolioPlanningError::Corrupt);
    }
    Ok(bytes.to_owned())
}

fn digest_column(row: &Row<'_>, column: usize) -> Result<[u8; 32], PortfolioPlanningError> {
    row.get_ref(column)?
        .as_blob()
        .map_err(|_| PortfolioPlanningError::Corrupt)?
        .try_into()
        .map_err(|_| PortfolioPlanningError::Corrupt)
}

fn empty_head(saved: bool) -> PortfolioPlanningChainHead {
    let domain: &[u8] = if saved {
        b"market-squawk/portfolio-planning-saves/v1"
    } else {
        b"market-squawk/portfolio-planning-completions/v1"
    };
    PortfolioPlanningChainHead {
        sequence: 0,
        sha256: Sha256::digest(domain).into(),
    }
}

fn table(saved: bool) -> &'static str {
    if saved {
        "portfolio_planning_saves"
    } else {
        "portfolio_planning_completions"
    }
}

fn read_head(
    c: &Connection,
    saved: bool,
) -> Result<PortfolioPlanningChainHead, PortfolioPlanningError> {
    let mut statement = c.prepare(&format!(
        "SELECT sequence,chain_sha256 FROM {} ORDER BY sequence DESC LIMIT 1",
        table(saved)
    ))?;
    let mut rows = statement.query([])?;
    let Some(row) = rows.next()? else {
        return Ok(empty_head(saved));
    };
    let sequence = unsigned(row.get(0)?)?;
    if sequence == 0 {
        return Err(PortfolioPlanningError::Corrupt);
    }
    Ok(PortfolioPlanningChainHead {
        sequence,
        sha256: digest_column(row, 1)?,
    })
}

fn head_at(
    c: &Connection,
    saved: bool,
    sequence: u64,
) -> Result<PortfolioPlanningChainHead, PortfolioPlanningError> {
    if sequence == 0 {
        return Ok(empty_head(saved));
    }
    let mut statement = c.prepare(&format!(
        "SELECT chain_sha256 FROM {} WHERE sequence=?1",
        table(saved)
    ))?;
    let mut rows = statement.query([sql(sequence)?])?;
    let row = rows.next()?.ok_or(PortfolioPlanningError::Corrupt)?;
    Ok(PortfolioPlanningChainHead {
        sequence,
        sha256: digest_column(row, 0)?,
    })
}

fn verify_fence(
    c: &Connection,
    fence: PortfolioPlanningHead,
) -> Result<(), PortfolioPlanningError> {
    if head_at(c, false, fence.completions.sequence)? != fence.completions
        || head_at(c, true, fence.saves.sequence)? != fence.saves
    {
        return Err(PortfolioPlanningError::Corrupt);
    }
    Ok(())
}

fn verify_entry(
    c: &Connection,
    saved: bool,
    head: PortfolioPlanningChainHead,
    digest: [u8; 32],
) -> Result<(), PortfolioPlanningError> {
    let previous = head
        .sequence
        .checked_sub(1)
        .ok_or(PortfolioPlanningError::Corrupt)?;
    if successor(head_at(c, saved, previous)?, digest)? != head {
        return Err(PortfolioPlanningError::Corrupt);
    }
    Ok(())
}

fn successor(
    previous: PortfolioPlanningChainHead,
    digest: [u8; 32],
) -> Result<PortfolioPlanningChainHead, PortfolioPlanningError> {
    let sequence = previous
        .sequence
        .checked_add(1)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or(PortfolioPlanningError::Capacity)?;
    let mut hash = Sha256::new();
    hash.update(previous.sha256);
    hash.update(sequence.to_be_bytes());
    hash.update(digest);
    Ok(PortfolioPlanningChainHead {
        sequence,
        sha256: hash.finalize().into(),
    })
}

fn save_digest(completion: &PortfolioPlanningCompletion, saved_at: Timestamp) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/portfolio-planning-save/v1");
    hash.update(completion.digest());
    hash.update(saved_at.unix_nanos().to_be_bytes());
    hash.finalize().into()
}

fn text_digest(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

fn sql(value: u64) -> Result<i64, PortfolioPlanningError> {
    i64::try_from(value).map_err(|_| PortfolioPlanningError::Capacity)
}

fn unsigned(value: i64) -> Result<u64, PortfolioPlanningError> {
    u64::try_from(value).map_err(|_| PortfolioPlanningError::Corrupt)
}

/// Immutable calculation publication, saved marker, or catalog integrity failure.
#[derive(Debug, thiserror::Error)]
pub enum PortfolioPlanningError {
    /// Invalid metadata or page coordinates; bounds limit the working set, never history.
    #[error("portfolio planning record is invalid")]
    InvalidRecord,
    /// A token was reused for a different immutable calculation.
    #[error("portfolio planning calculation conflicts")]
    Conflict,
    /// No calculation exists for the supplied account and token.
    #[error("portfolio planning calculation was not found")]
    NotFound,
    /// Persisted metadata, hash chain, or retained prefix failed verification.
    #[error("portfolio planning inventory is corrupt")]
    Corrupt,
    /// A checked sequence or byte length cannot be represented.
    #[error("portfolio planning capacity exceeded")]
    Capacity,
    /// The composition-owned catalog authority is unavailable.
    #[error("portfolio planning inventory is unavailable")]
    Unavailable,
    /// SQLite operation failed without successful publication.
    #[error(transparent)]
    Storage(#[from] rusqlite::Error),
}
