//! Owner-issued portfolio and transaction backup snapshots.

use std::fmt;
use std::sync::Arc;

use market_squawk_platform::LocalPaths;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    PortfolioApplicationLimits, PortfolioApplicationService, PortfolioApplicationServiceError,
    Runtime,
};
pub(crate) const PORTFOLIO_BACKUP_PRODUCER: &str = "market-squawk.portfolio-authority";
pub(crate) const PORTFOLIO_BACKUP_SCHEMA: &str = "market-squawk.portfolios-backup";
pub(crate) const TRANSACTION_BACKUP_SCHEMA: &str = "market-squawk.transactions-backup";
pub(super) const BACKUP_SCHEMA_VERSION: u16 = 1;
const MAXIMUM_COMPONENT_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// The two product components governed by one portfolio publication authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PortfolioBackupComponent {
    Portfolios,
    Transactions,
}

/// Cloneable least-authority entry point for portfolio backup and fresh-workspace restore.
#[derive(Clone)]
pub(crate) struct PortfolioBackupAuthority {
    pub(super) runtime: Arc<Runtime>,
}

impl PortfolioBackupAuthority {
    /// Freezes one consistent portfolio/transaction revision under the import mutation gate.
    pub(crate) fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<RetainedPortfolioBackupSnapshot, PortfolioApplicationServiceError> {
        if cancellation.is_cancelled() || self.runtime.cancellation.is_cancelled() {
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        let authority = self
            .runtime
            .authority
            .lock()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let snapshot = authority.backup_snapshot(&self.runtime.artifacts)?;
        if cancellation.is_cancelled() || self.runtime.cancellation.is_cancelled() {
            return Err(PortfolioApplicationServiceError::Cancelled);
        }
        Ok(snapshot)
    }

    /// Restores both paired components only into a freshly prepared workspace and reopens the
    /// resulting authority before returning it to composition.
    pub(crate) fn restore_fresh(
        paths: &LocalPaths,
        limits: PortfolioApplicationLimits,
        portfolios: &[u8],
        transactions: &[u8],
    ) -> Result<PortfolioApplicationService, PortfolioApplicationServiceError> {
        PortfolioApplicationService::restore_backup(paths, limits, portfolios, transactions)
    }
}

impl fmt::Debug for PortfolioBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PortfolioBackupAuthority([PAIRED OWNER CAPABILITY])")
    }
}

/// Immutable paired export retained before the common product cutoff is allocated.
pub(crate) struct RetainedPortfolioBackupSnapshot {
    authority_revision_sha256: [u8; 32],
    portfolios: Arc<[u8]>,
    transactions: Arc<[u8]>,
}

impl RetainedPortfolioBackupSnapshot {
    pub(super) fn try_new(
        authority_revision_sha256: [u8; 32],
        portfolios: Vec<u8>,
        transactions: Vec<u8>,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        if authority_revision_sha256 == [0; 32]
            || portfolios.is_empty()
            || transactions.is_empty()
            || u64::try_from(portfolios.len())
                .map_or(true, |length| length > MAXIMUM_COMPONENT_BYTES)
            || u64::try_from(transactions.len())
                .map_or(true, |length| length > MAXIMUM_COMPONENT_BYTES)
        {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        Ok(Self {
            authority_revision_sha256,
            portfolios: portfolios.into(),
            transactions: transactions.into(),
        })
    }

    /// Returns the shared content identity for the exact paired owner revision.
    pub(crate) const fn authority_revision_sha256(&self) -> [u8; 32] {
        self.authority_revision_sha256
    }

    /// Returns one immutable, versioned component payload.
    pub(crate) fn bytes(&self, component: PortfolioBackupComponent) -> &[u8] {
        match component {
            PortfolioBackupComponent::Portfolios => &self.portfolios,
            PortfolioBackupComponent::Transactions => &self.transactions,
        }
    }

    /// Revalidates the bytes emitted by an adapter against this retained owner revision.
    pub(crate) fn validate_emitted(
        &self,
        component: PortfolioBackupComponent,
        authority_revision_sha256: [u8; 32],
        byte_length: u64,
        sha256: [u8; 32],
    ) -> Result<(), PortfolioApplicationServiceError> {
        let bytes = self.bytes(component);
        if authority_revision_sha256 != self.authority_revision_sha256
            || usize::try_from(byte_length).ok() != Some(bytes.len())
            || <[u8; 32]>::from(Sha256::digest(bytes)) != sha256
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(())
    }
}

impl fmt::Debug for RetainedPortfolioBackupSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedPortfolioBackupSnapshot")
            .field("authority_revision_sha256", &"[SHA-256]")
            .field("portfolio_bytes", &self.portfolios.len())
            .field("transaction_bytes", &self.transactions.len())
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ImmutableBackupObject {
    pub(super) reference: String,
    pub(super) sha256: [u8; 32],
    #[serde(with = "base64_bytes")]
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct PortfolioBackupEnvelope {
    pub(super) schema_version: u16,
    pub(super) authority_revision_sha256: [u8; 32],
    #[serde(with = "base64_bytes")]
    pub(super) publication_manifest: Vec<u8>,
    pub(super) immutable_imports: Vec<ImmutableBackupObject>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct TransactionBackupEnvelope {
    pub(super) schema_version: u16,
    pub(super) authority_revision_sha256: [u8; 32],
    pub(super) publication_manifest_sha256: [u8; 32],
    pub(super) transaction_state_sha256: [u8; 32],
    pub(super) governance_objects: Vec<ImmutableBackupObject>,
}

pub(super) fn authority_revision(
    manifest: &[u8],
    transaction_state_sha256: [u8; 32],
    imports: &[ImmutableBackupObject],
    governance: &[ImmutableBackupObject],
) -> Result<[u8; 32], PortfolioApplicationServiceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-backup-authority/v1\0");
    digest.update(Sha256::digest(manifest));
    digest.update(transaction_state_sha256);
    for object in imports.iter().chain(governance) {
        digest.update(
            u64::try_from(object.reference.len())
                .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?
                .to_be_bytes(),
        );
        digest.update(object.reference.as_bytes());
        digest.update(object.sha256);
        digest.update(
            u64::try_from(object.bytes.len())
                .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?
                .to_be_bytes(),
        );
    }
    Ok(digest.finalize().into())
}

pub(super) fn decode_pair(
    portfolios: &[u8],
    transactions: &[u8],
    limits: PortfolioApplicationLimits,
) -> Result<(PortfolioBackupEnvelope, TransactionBackupEnvelope), PortfolioApplicationServiceError>
{
    if portfolios.is_empty()
        || transactions.is_empty()
        || u64::try_from(portfolios.len()).map_or(true, |value| value > MAXIMUM_COMPONENT_BYTES)
        || u64::try_from(transactions.len()).map_or(true, |value| value > MAXIMUM_COMPONENT_BYTES)
    {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let portfolio: PortfolioBackupEnvelope = serde_json::from_slice(portfolios)
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    let transaction: TransactionBackupEnvelope = serde_json::from_slice(transactions)
        .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
    if portfolio.schema_version != BACKUP_SCHEMA_VERSION
        || transaction.schema_version != BACKUP_SCHEMA_VERSION
        || portfolio.authority_revision_sha256 != transaction.authority_revision_sha256
        || portfolio.authority_revision_sha256 == [0; 32]
        || <[u8; 32]>::from(Sha256::digest(&portfolio.publication_manifest))
            != transaction.publication_manifest_sha256
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    if authority_revision(
        &portfolio.publication_manifest,
        transaction.transaction_state_sha256,
        &portfolio.immutable_imports,
        &transaction.governance_objects,
    )? != portfolio.authority_revision_sha256
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    validate_inventory(&portfolio.immutable_imports, limits)?;
    validate_inventory(&transaction.governance_objects, limits)?;
    Ok((portfolio, transaction))
}

mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize as _, Deserializer, Serializer};

    pub(super) fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

fn validate_inventory(
    objects: &[ImmutableBackupObject],
    limits: PortfolioApplicationLimits,
) -> Result<(), PortfolioApplicationServiceError> {
    let mut prior = None;
    for object in objects {
        if object.reference.is_empty()
            || object.bytes.is_empty()
            || object.bytes.len() > limits.max_artifact_bytes
            || <[u8; 32]>::from(Sha256::digest(&object.bytes)) != object.sha256
            || prior.is_some_and(|value: &str| value >= object.reference.as_str())
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        prior = Some(object.reference.as_str());
    }
    Ok(())
}
