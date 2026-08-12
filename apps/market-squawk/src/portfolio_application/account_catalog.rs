//! Complete, authority-free current portfolio-account catalog evidence.

use std::{
    fmt,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use market_squawk_domain::{
    AccountId, Currency, DigestAlgorithm, EvidenceDigest, SourceId, Timestamp,
};
use market_squawk_portfolio::PortfolioRevisionToken;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{PortfolioApplicationServiceError, Runtime, model::PortfolioReadImage};

const ACCOUNT_CATALOG_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/portfolio-current-account-catalog/v1\0";

/// One exact current account head retained in canonical account order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAccountHead {
    account_id: AccountId,
    revision: PortfolioRevisionToken,
    reporting_currency: Currency,
    effective_at: Timestamp,
    available_at: Option<Timestamp>,
    source_id: SourceId,
    source_coverage: Box<[SourceId]>,
    artifact_sha256: [u8; 32],
}

impl PortfolioAccountHead {
    /// Returns the stable portfolio account identity.
    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the exact current immutable portfolio revision.
    pub(crate) const fn revision(&self) -> &PortfolioRevisionToken {
        &self.revision
    }

    /// Returns the revision's reporting currency.
    pub(crate) const fn reporting_currency(&self) -> Currency {
        self.reporting_currency
    }

    /// Returns when the source facts represented by this head became effective.
    pub(crate) const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns when the head became knowable, when the producer retained that evidence.
    ///
    /// `None` is preserved as an explicit evidence gap. Personalized recommendation setup must
    /// not treat a missing availability time as point-in-time-admissible.
    pub(crate) const fn available_at(&self) -> Option<Timestamp> {
        self.available_at
    }

    /// Returns the source that published the current revision.
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the complete canonical source coverage retained by the current revision.
    pub(crate) fn source_coverage(&self) -> &[SourceId] {
        &self.source_coverage
    }

    /// Returns the exact admitted import-artifact identity.
    pub(crate) const fn artifact_sha256(&self) -> [u8; 32] {
        self.artifact_sha256
    }
}

/// Complete bounded current account-head image with a canonical semantic identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PortfolioAccountCatalogSnapshot {
    heads: Box<[PortfolioAccountHead]>,
    account_count: u64,
    digest: EvidenceDigest,
}

impl PortfolioAccountCatalogSnapshot {
    fn try_from_image(image: &PortfolioReadImage) -> Result<Self, PortfolioAccountCatalogError> {
        if image.revisions.current_revisions().len() != image.accounts.len() {
            return Err(PortfolioAccountCatalogError::CorruptPublication);
        }

        let mut heads = Vec::new();
        heads
            .try_reserve_exact(image.accounts.len())
            .map_err(|_| PortfolioAccountCatalogError::ResourceExhausted)?;
        for (account_id, history) in &image.accounts {
            let published = history
                .revisions
                .last()
                .ok_or(PortfolioAccountCatalogError::CorruptPublication)?;
            let revision = published.token();
            if published.account.account_id() != *account_id
                || published.core.account_id() != *account_id
                || published.account.currency() != published.core.base_currency()
                || published.account.as_of() > published.effective_at
                || published
                    .available_at
                    .is_some_and(|available_at| available_at < published.effective_at)
                || image
                    .revisions
                    .head(*account_id)
                    .map_err(|_| PortfolioAccountCatalogError::CorruptPublication)?
                    != revision
            {
                return Err(PortfolioAccountCatalogError::CorruptPublication);
            }

            let mut source_coverage = published.source_coverage.clone();
            source_coverage.sort_unstable();
            if source_coverage.is_empty()
                || source_coverage.windows(2).any(|pair| pair[0] == pair[1])
                || !source_coverage.contains(&published.source_id)
                || revision.bytes() == [0; 32]
                || published.artifact_sha256 == [0; 32]
            {
                return Err(PortfolioAccountCatalogError::CorruptPublication);
            }
            heads.push(PortfolioAccountHead {
                account_id: *account_id,
                revision,
                reporting_currency: published.account.currency(),
                effective_at: published.effective_at,
                available_at: published.available_at,
                source_id: published.source_id.clone(),
                source_coverage: source_coverage.into_boxed_slice(),
                artifact_sha256: published.artifact_sha256,
            });
        }
        let account_count = u64::try_from(heads.len())
            .map_err(|_| PortfolioAccountCatalogError::ResourceExhausted)?;
        let digest = catalog_digest(&heads)?;
        Ok(Self {
            heads: heads.into_boxed_slice(),
            account_count,
            digest,
        })
    }

    /// Returns every current account head in stable `AccountId` order.
    pub(crate) fn heads(&self) -> &[PortfolioAccountHead] {
        &self.heads
    }

    /// Returns the exact current head for one account without choosing a fallback.
    pub(crate) fn head(&self, account_id: AccountId) -> Option<&PortfolioAccountHead> {
        self.heads
            .binary_search_by_key(&account_id, PortfolioAccountHead::account_id)
            .ok()
            .and_then(|index| self.heads.get(index))
    }

    /// Returns the number of complete current account heads.
    pub(crate) const fn len(&self) -> usize {
        self.heads.len()
    }

    /// Returns whether no portfolio account is currently published.
    pub(crate) const fn is_empty(&self) -> bool {
        self.heads.is_empty()
    }

    /// Returns the canonical bounded account count retained in setup evidence.
    pub(crate) const fn account_count(&self) -> u64 {
        self.account_count
    }

    /// Returns the versioned SHA-256 identity of the complete ordered head set.
    pub(crate) const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Cloneable least-authority reader over the portfolio owner's immutable current image.
#[derive(Clone)]
pub(crate) struct PortfolioAccountCatalogReadCapability {
    pub(super) runtime: Arc<Runtime>,
}

impl PortfolioAccountCatalogReadCapability {
    /// Clones one complete bounded account-head snapshot without import or publication authority.
    pub(crate) fn snapshot_current(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PortfolioAccountCatalogSnapshot, PortfolioAccountCatalogError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, deadline, cancellation)?;
        let image = self.runtime.image.load_full();
        let snapshot = PortfolioAccountCatalogSnapshot::try_from_image(&image)?;
        ensure_live(&self.runtime, deadline, cancellation)?;
        Ok(snapshot)
    }

    /// Rechecks both every ordered head and the independently calculated catalog digest.
    ///
    /// A matching digest alone is never used as a substitute for exact typed-head equality.
    pub(crate) fn recheck(
        &self,
        expected: &PortfolioAccountCatalogSnapshot,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioAccountCatalogError> {
        let _guard = self.runtime.admit()?;
        ensure_live(&self.runtime, deadline, cancellation)?;
        let image = self.runtime.image.load_full();
        let current = PortfolioAccountCatalogSnapshot::try_from_image(&image)?;
        ensure_live(&self.runtime, deadline, cancellation)?;
        if current.heads != expected.heads
            || current.account_count != expected.account_count
            || current.digest != expected.digest
        {
            return Err(PortfolioAccountCatalogError::CatalogChanged);
        }
        Ok(())
    }
}

impl fmt::Debug for PortfolioAccountCatalogReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioAccountCatalogReadCapability")
            .field("authority", &"[IMMUTABLE COMPLETE PORTFOLIO HEADS]")
            .finish()
    }
}

/// Exact current-account snapshot or recheck failure.
#[derive(Debug, Error)]
pub(crate) enum PortfolioAccountCatalogError {
    /// The portfolio owner rejected lifecycle admission, cancellation, or the deadline.
    #[error("portfolio account catalog is unavailable: {0}")]
    Portfolio(#[from] PortfolioApplicationServiceError),
    /// The published account map and immutable revision service disagree.
    #[error("portfolio account catalog publication is corrupt")]
    CorruptPublication,
    /// A bounded current-head allocation could not be retained.
    #[error("portfolio account catalog capacity is exhausted")]
    ResourceExhausted,
    /// At least one exact ordered account head changed after the prior snapshot.
    #[error("portfolio account catalog changed")]
    CatalogChanged,
}

fn catalog_digest(
    heads: &[PortfolioAccountHead],
) -> Result<EvidenceDigest, PortfolioAccountCatalogError> {
    let mut digest = Sha256::new();
    digest.update(ACCOUNT_CATALOG_DIGEST_DOMAIN);
    digest.update(
        u64::try_from(heads.len())
            .map_err(|_| PortfolioAccountCatalogError::ResourceExhausted)?
            .to_be_bytes(),
    );
    for head in heads {
        digest.update(head.account_id.as_uuid().as_bytes());
        digest.update(head.revision.bytes());
        update_text(&mut digest, head.reporting_currency.as_str())?;
        digest.update(head.effective_at.unix_nanos().to_be_bytes());
        match head.available_at {
            Some(available_at) => {
                digest.update([1]);
                digest.update(available_at.unix_nanos().to_be_bytes());
            }
            None => digest.update([0]),
        }
        update_text(&mut digest, head.source_id.as_str())?;
        digest.update(
            u64::try_from(head.source_coverage.len())
                .map_err(|_| PortfolioAccountCatalogError::ResourceExhausted)?
                .to_be_bytes(),
        );
        for source_id in &head.source_coverage {
            update_text(&mut digest, source_id.as_str())?;
        }
        digest.update(head.artifact_sha256);
    }
    let bytes: [u8; 32] = digest.finalize().into();
    if bytes == [0; 32] {
        return Err(PortfolioAccountCatalogError::CorruptPublication);
    }
    Ok(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
}

fn update_text(digest: &mut Sha256, value: &str) -> Result<(), PortfolioAccountCatalogError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| PortfolioAccountCatalogError::ResourceExhausted)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}

fn ensure_live(
    runtime: &Runtime,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioAccountCatalogError> {
    if cancellation.is_cancelled() || runtime.cancellation.is_cancelled() {
        return Err(PortfolioApplicationServiceError::Cancelled.into());
    }
    if !runtime.accepting.load(Ordering::Acquire) {
        return Err(PortfolioApplicationServiceError::Cancelled.into());
    }
    if Instant::now() >= deadline {
        return Err(PortfolioApplicationServiceError::DeadlineExceeded.into());
    }
    Ok(())
}
