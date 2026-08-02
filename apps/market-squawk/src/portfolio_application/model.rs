//! Immutable portfolio application read images and durable publication references.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use market_squawk_adapter_portfolio::{
    AccountObservation, HoldingObservation, PortfolioTransaction, ReconciliationDiscrepancy,
};
use market_squawk_domain::{AccountId, SourceId, Timestamp};
use market_squawk_portfolio::{
    PortfolioRevision, PortfolioRevisionToken, PortfolioService, PortfolioServiceLimitInput,
    PortfolioServiceLimits,
};
use serde::{Deserialize, Serialize};

use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError};

pub(super) const PUBLICATION_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicationEntry {
    pub(super) account_id: AccountId,
    pub(super) artifact_reference: String,
    pub(super) artifact_sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PublicationManifest {
    schema_version: u16,
    pub(super) entries: Vec<PublicationEntry>,
}

impl PublicationManifest {
    pub(super) const fn empty() -> Self {
        Self {
            schema_version: PUBLICATION_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, PortfolioApplicationServiceError> {
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        if manifest.schema_version != PUBLICATION_SCHEMA_VERSION {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(manifest)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
        serde_json::to_vec(self).map_err(|_| PortfolioApplicationServiceError::Publication)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct SourceKey {
    pub(super) source_id: SourceId,
    pub(super) metadata_revision: String,
}

#[derive(Clone, Debug)]
pub(super) struct PublishedRevision {
    pub(super) core: PortfolioRevision,
    pub(super) account: AccountObservation,
    pub(super) holdings: Vec<HoldingObservation>,
    pub(super) transactions: Vec<PortfolioTransaction>,
    pub(super) discrepancies: Vec<ReconciliationDiscrepancy>,
    pub(super) source_id: SourceId,
    pub(super) source_coverage: Vec<SourceId>,
    pub(super) effective_at: Timestamp,
    pub(super) available_at: Option<Timestamp>,
    pub(super) artifact_sha256: [u8; 32],
}

impl PublishedRevision {
    pub(super) fn token(&self) -> PortfolioRevisionToken {
        self.core.token()
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct AccountHistory {
    pub(super) revisions: Vec<PublishedRevision>,
}

#[derive(Clone, Debug)]
pub(super) struct PortfolioReadImage {
    pub(super) accounts: BTreeMap<AccountId, AccountHistory>,
    pub(super) revisions: PortfolioService,
}

impl PortfolioReadImage {
    pub(super) fn try_from_accounts(
        accounts: BTreeMap<AccountId, AccountHistory>,
        limits: PortfolioApplicationLimits,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if accounts.len() > limits.max_accounts {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        let mut current = Vec::new();
        let mut revoked = Vec::new();
        current
            .try_reserve_exact(accounts.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        let mut retained_bytes = std::mem::size_of::<Self>()
            .checked_add(
                accounts
                    .len()
                    .checked_mul(std::mem::size_of::<(AccountId, AccountHistory)>())
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
            )
            .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
        for history in accounts.values() {
            if history.revisions.len() > limits.max_history_per_account {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            retained_bytes = retained_bytes
                .checked_add(
                    history
                        .revisions
                        .len()
                        .checked_mul(std::mem::size_of::<PublishedRevision>())
                        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?,
                )
                .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
            for revision in &history.revisions {
                let external = serde_json::to_vec(&(
                    &revision.account,
                    &revision.holdings,
                    &revision.transactions,
                    &revision.discrepancies,
                    revision.source_id.as_str(),
                    &revision.source_coverage,
                    revision.effective_at.unix_nanos(),
                    revision.available_at.map(Timestamp::unix_nanos),
                    revision.artifact_sha256,
                ))
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
                retained_bytes = retained_bytes
                    .checked_add(revision.core.retained_bytes())
                    .and_then(|total| total.checked_add(external.len()))
                    .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
                if retained_bytes > limits.max_retained_bytes {
                    return Err(PortfolioApplicationServiceError::ResourceExhausted);
                }
            }
            let (head, prior) = history
                .revisions
                .split_last()
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            current.push(head.core.clone());
            revoked
                .try_reserve(prior.len())
                .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
            revoked.extend(prior.iter().map(PublishedRevision::token));
        }
        let service_limits = PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
            max_accounts: nonzero(limits.max_accounts)?,
            max_history_per_account: nonzero(limits.max_history_per_account)?,
            max_results: nonzero(limits.max_result_items)?,
            max_retained_bytes: nonzero(limits.max_retained_bytes)?,
        })
        .map_err(|_| PortfolioApplicationServiceError::InvalidLimits)?;
        let revisions = PortfolioService::try_new(current, revoked, service_limits)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        Ok(Self {
            accounts,
            revisions,
        })
    }
}

fn nonzero(value: usize) -> Result<NonZeroUsize, PortfolioApplicationServiceError> {
    NonZeroUsize::new(value).ok_or(PortfolioApplicationServiceError::InvalidLimits)
}
