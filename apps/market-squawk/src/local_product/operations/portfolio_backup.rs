//! Workspace-backup adapter for paired portfolio and transaction authority.

use std::{fmt, io::Write};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::backup::{
    ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
    ProductBackupSensitivity, ProductBackupSnapshot,
};
use crate::portfolio_application::{
    PORTFOLIO_BACKUP_PRODUCER, PORTFOLIO_BACKUP_SCHEMA, PortfolioApplicationServiceError,
    PortfolioBackupAuthority, PortfolioBackupComponent, RetainedPortfolioBackupSnapshot,
    TRANSACTION_BACKUP_SCHEMA,
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

/// Paired Portfolios and Transactions owner backed by one portfolio mutation authority.
pub(crate) struct PortfolioWorkspaceBackupAuthority {
    portfolio: PortfolioBackupAuthority,
    descriptors: [WorkspaceComponentDescriptor; 2],
}

impl PortfolioWorkspaceBackupAuthority {
    /// Binds both components to the exact same portfolio owner capability.
    pub(super) fn try_new(portfolio: PortfolioBackupAuthority) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(PORTFOLIO_BACKUP_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let portfolio_schema = ProductBackupComponentSchema::try_new(
            SourceIdentifier::try_from(PORTFOLIO_BACKUP_SCHEMA)
                .map_err(|_| ProductBackupError::InvalidComponent)?,
            SchemaVersion::CURRENT,
        )?;
        let transaction_schema = ProductBackupComponentSchema::try_new(
            SourceIdentifier::try_from(TRANSACTION_BACKUP_SCHEMA)
                .map_err(|_| ProductBackupError::InvalidComponent)?,
            SchemaVersion::CURRENT,
        )?;
        Ok(Self {
            portfolio,
            descriptors: [
                WorkspaceComponentDescriptor::try_new(
                    ProductBackupComponentKind::Portfolios,
                    producer.clone(),
                    portfolio_schema,
                    ProductBackupSensitivity::Protected,
                )?,
                WorkspaceComponentDescriptor::try_new(
                    ProductBackupComponentKind::Transactions,
                    producer,
                    transaction_schema,
                    ProductBackupSensitivity::Protected,
                )?,
            ],
        })
    }
}

impl fmt::Debug for PortfolioWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PortfolioWorkspaceBackupAuthority([PAIRED PORTFOLIO OWNER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for PortfolioWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        let retained = self
            .portfolio
            .retain(cancellation)
            .map_err(map_portfolio_backup_error)?;
        Ok(Box::new(RetainedPortfolioWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            snapshot: None,
            emitted: [false; 2],
            revalidated: [false; 2],
        }))
    }
}

struct RetainedPortfolioWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 2],
    retained: RetainedPortfolioBackupSnapshot,
    snapshot: Option<ProductBackupSnapshot>,
    emitted: [bool; 2],
    revalidated: [bool; 2],
}

impl fmt::Debug for RetainedPortfolioWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedPortfolioWorkspaceSnapshot([PAIRED IMMUTABLE EXPORT])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedPortfolioWorkspaceSnapshot {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        self.bind_snapshot(snapshot)?;
        let (component, index) = component(kind)?;
        if self.emitted[index] {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let bytes = self.retained.bytes(component);
        writer
            .write_all(bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        let byte_length =
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?;
        let sha256 = Sha256::digest(bytes).into();
        let authority_revision = self.retained.authority_revision_sha256();
        self.retained
            .validate_emitted(component, authority_revision, byte_length, sha256)
            .map_err(map_portfolio_backup_error)?;
        let receipt =
            WorkspaceComponentSnapshotReceipt::try_new(authority_revision, byte_length, sha256)?;
        self.emitted[index] = true;
        Ok(receipt)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        self.bind_snapshot(snapshot)?;
        let (component, index) = component(kind)?;
        if !self.emitted[index] || self.revalidated[index] {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let bytes = self.retained.bytes(component);
        let byte_length =
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?;
        let sha256 = Sha256::digest(bytes).into();
        let authority_revision = self.retained.authority_revision_sha256();
        let expected =
            WorkspaceComponentSnapshotReceipt::try_new(authority_revision, byte_length, sha256)?;
        if receipt != expected {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        self.retained
            .validate_emitted(component, authority_revision, byte_length, sha256)
            .map_err(map_portfolio_backup_error)?;
        self.revalidated[index] = true;
        Ok(())
    }
}

impl RetainedPortfolioWorkspaceSnapshot {
    fn bind_snapshot(&mut self, snapshot: ProductBackupSnapshot) -> Result<(), ProductBackupError> {
        match self.snapshot {
            Some(bound) if bound != snapshot => Err(ProductBackupError::SnapshotMismatch),
            Some(_) => Ok(()),
            None => {
                self.snapshot = Some(snapshot);
                Ok(())
            }
        }
    }
}

fn component(
    kind: ProductBackupComponentKind,
) -> Result<(PortfolioBackupComponent, usize), ProductBackupError> {
    match kind {
        ProductBackupComponentKind::Portfolios => Ok((PortfolioBackupComponent::Portfolios, 0)),
        ProductBackupComponentKind::Transactions => Ok((PortfolioBackupComponent::Transactions, 1)),
        _ => Err(ProductBackupError::InvalidComponent),
    }
}

fn map_portfolio_backup_error(error: PortfolioApplicationServiceError) -> ProductBackupError {
    match error {
        PortfolioApplicationServiceError::Cancelled => ProductBackupError::Cancelled,
        PortfolioApplicationServiceError::ResourceExhausted => ProductBackupError::InvalidComponent,
        PortfolioApplicationServiceError::CorruptPublication
        | PortfolioApplicationServiceError::Publication => ProductBackupError::ArtifactMismatch,
        PortfolioApplicationServiceError::InvalidLimits
        | PortfolioApplicationServiceError::Path
        | PortfolioApplicationServiceError::Authority
        | PortfolioApplicationServiceError::InvalidRequest
        | PortfolioApplicationServiceError::NotFound
        | PortfolioApplicationServiceError::DeadlineExceeded
        | PortfolioApplicationServiceError::Import
        | PortfolioApplicationServiceError::SnapshotUnavailable
        | PortfolioApplicationServiceError::RestoreTargetNotFresh
        | PortfolioApplicationServiceError::Analytics => ProductBackupError::SnapshotMismatch,
    }
}
