//! Workspace-backup adapter for the genuine Fair Value writer authority.

use std::{fmt, io::Write, sync::Arc};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::{
    backup::{
        ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
        ProductBackupSensitivity, ProductBackupSnapshot,
    },
    fair_value::{FairValueBackupAttestationLease, FairValueBackupError, FairValueDomainService},
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

const FAIR_VALUE_PRODUCER: &str = "market-squawk-fair-value";
const FAIR_VALUE_ATTESTATION_SCHEMA: &str = "market-squawk-fair-value-catalog-attestation";

/// The FairValueEvidence component owner over the sole in-process Fair Value writer.
pub(crate) struct FairValueWorkspaceBackupAuthority {
    fair_value: Arc<FairValueDomainService>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl FairValueWorkspaceBackupAuthority {
    /// Binds backup export to the same mutation authority used by Fair Value operations.
    pub(super) fn try_new(
        fair_value: Arc<FairValueDomainService>,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(FAIR_VALUE_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema_identity = SourceIdentifier::try_from(FAIR_VALUE_ATTESTATION_SCHEMA)
            .map_err(|_| ProductBackupError::InvalidComponentSchema)?;
        let schema =
            ProductBackupComponentSchema::try_new(schema_identity, SchemaVersion::CURRENT)?;
        Ok(Self {
            fair_value,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::FairValueEvidence,
                producer,
                schema,
                ProductBackupSensitivity::NonSecret,
            )?],
        })
    }
}

impl fmt::Debug for FairValueWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FairValueWorkspaceBackupAuthority([FAIR-VALUE WRITER ATTESTATION])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for FairValueWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        let retained = self
            .fair_value
            .retain_backup_attestation(cancellation)
            .await
            .map_err(map_backup_error)?;
        Ok(Box::new(RetainedFairValueWorkspaceSnapshot {
            descriptors: self.descriptors.clone(),
            retained,
            emitted: None,
        }))
    }
}

struct RetainedFairValueWorkspaceSnapshot {
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: FairValueBackupAttestationLease,
    emitted: Option<(ProductBackupSnapshot, WorkspaceComponentSnapshotReceipt)>,
}

impl fmt::Debug for RetainedFairValueWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RetainedFairValueWorkspaceSnapshot([RETAINED FAIR-VALUE WRITER])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedFairValueWorkspaceSnapshot {
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
        ensure_request(kind, cancellation)?;
        if self.emitted.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let bytes = self.retained.attestation().canonical_bytes();
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(
            digest,
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            digest,
        )?;
        writer
            .write_all(&bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        self.emitted = Some((snapshot, receipt));
        Ok(receipt)
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        snapshot: ProductBackupSnapshot,
        receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        ensure_request(kind, cancellation)?;
        if self.emitted != Some((snapshot, receipt)) {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        self.retained.revalidate().map_err(map_backup_error)
    }
}

fn ensure_request(
    kind: ProductBackupComponentKind,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        return Err(ProductBackupError::Cancelled);
    }
    if kind != ProductBackupComponentKind::FairValueEvidence {
        return Err(ProductBackupError::InvalidComponent);
    }
    Ok(())
}

fn map_backup_error(error: FairValueBackupError) -> ProductBackupError {
    match error {
        FairValueBackupError::Cancelled => ProductBackupError::Cancelled,
        FairValueBackupError::InvalidEncoding => ProductBackupError::InvalidComponent,
        FairValueBackupError::FairValue(_) | FairValueBackupError::CatalogMismatch => {
            ProductBackupError::SnapshotMismatch
        }
    }
}
