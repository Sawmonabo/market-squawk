//! Truthful workspace-backup selection for non-analytical source data.
//!
//! Research datasets and their source records are owned by the analytical backup. The default
//! v1 product-backup request selects no raw-capture segments, so this owner emits only a typed
//! selection manifest. It never opens or copies a live capture journal.

use std::{fmt, io::Write};

use async_trait::async_trait;
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::backup::{
    ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
    ProductBackupSensitivity, ProductBackupSnapshot,
};

use super::workspace_backup::{
    WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
    WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
};

const SOURCE_DATA_SCHEMA: &str = "market-squawk-source-data-selection-v1";
const MAXIMUM_SELECTION_BYTES: usize = 16 * 1024;
const AUTHORITY_REVISION_DOMAIN: &[u8] = b"market-squawk-source-data-selection-authority-v1\0\
policy=none\0analytical=analytical-backup\0raw-capture=excluded\0live-journal-copy=forbidden";

/// Code-owned SourceData component for the default selection of no raw-capture segments.
pub(crate) struct SourceDataWorkspaceBackupAuthority {
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl SourceDataWorkspaceBackupAuthority {
    /// Declares the truthful default SourceData selection schema.
    pub(super) fn try_new() -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(SOURCE_DATA_SCHEMA)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::SourceData,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for SourceDataWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SourceDataWorkspaceBackupAuthority([NO RAW CAPTURE SELECTED])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for SourceDataWorkspaceBackupAuthority {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn retain(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<Box<dyn WorkspaceComponentSnapshotLease>, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        Ok(Box::new(RetainedSourceDataSelection {
            descriptors: self.descriptors.clone(),
            authority_revision_sha256: Sha256::digest(AUTHORITY_REVISION_DOMAIN).into(),
            issued: None,
        }))
    }
}

struct RetainedSourceDataSelection {
    descriptors: [WorkspaceComponentDescriptor; 1],
    authority_revision_sha256: [u8; 32],
    issued: Option<(ProductBackupSnapshot, WorkspaceComponentSnapshotReceipt)>,
}

impl fmt::Debug for RetainedSourceDataSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedSourceDataSelection")
            .field("policy", &SourceDataSelectionPolicy::None)
            .field("issued", &self.issued.is_some())
            .finish()
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedSourceDataSelection {
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
        if kind != ProductBackupComponentKind::SourceData {
            return Err(ProductBackupError::InvalidComponent);
        }
        if self.issued.is_some() {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        let bytes = canonical_selection_bytes(snapshot)?;
        writer
            .write_all(&bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        let receipt = WorkspaceComponentSnapshotReceipt::try_new(
            self.authority_revision_sha256,
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            Sha256::digest(&bytes).into(),
        )?;
        self.issued = Some((snapshot, receipt));
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
        if kind != ProductBackupComponentKind::SourceData {
            return Err(ProductBackupError::InvalidComponent);
        }
        let bytes = canonical_selection_bytes(snapshot)?;
        let expected = WorkspaceComponentSnapshotReceipt::try_new(
            self.authority_revision_sha256,
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            Sha256::digest(bytes).into(),
        )?;
        if self.issued != Some((snapshot, receipt)) || receipt != expected {
            return Err(ProductBackupError::SnapshotMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceDataSelectionPolicy {
    None,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AnalyticalSourceDataDisposition {
    IncludedInAnalyticalBackup,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RawCaptureJournalDisposition {
    ExcludedNoSealedSegmentsSelected,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LiveJournalCopyPolicy {
    Forbidden,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SourceDataSelectionSnapshot {
    schema: String,
    schema_version: SchemaVersion,
    snapshot: ProductBackupSnapshot,
    selection_policy: SourceDataSelectionPolicy,
    analytical_source_data: AnalyticalSourceDataDisposition,
    raw_capture_journals: RawCaptureJournalDisposition,
    live_journal_copy_policy: LiveJournalCopyPolicy,
    included_raw_capture_segments: Vec<String>,
}

impl SourceDataSelectionSnapshot {
    fn for_snapshot(snapshot: ProductBackupSnapshot) -> Self {
        Self {
            schema: SOURCE_DATA_SCHEMA.to_owned(),
            schema_version: SchemaVersion::CURRENT,
            snapshot,
            selection_policy: SourceDataSelectionPolicy::None,
            analytical_source_data: AnalyticalSourceDataDisposition::IncludedInAnalyticalBackup,
            raw_capture_journals: RawCaptureJournalDisposition::ExcludedNoSealedSegmentsSelected,
            live_journal_copy_policy: LiveJournalCopyPolicy::Forbidden,
            included_raw_capture_segments: Vec::new(),
        }
    }
}

fn canonical_selection_bytes(
    snapshot: ProductBackupSnapshot,
) -> Result<Vec<u8>, ProductBackupError> {
    let bytes = serde_json::to_vec(&SourceDataSelectionSnapshot::for_snapshot(snapshot))
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    if bytes.is_empty() || bytes.len() > MAXIMUM_SELECTION_BYTES {
        return Err(ProductBackupError::InvalidComponent);
    }
    Ok(bytes)
}

/// Admits only the canonical default selection and never opens a raw live journal.
pub(super) fn validate_fresh_restore(
    snapshot: ProductBackupSnapshot,
    bytes: &[u8],
) -> Result<(), ProductBackupError> {
    if canonical_selection_bytes(snapshot)?.as_slice() == bytes {
        Ok(())
    } else {
        Err(ProductBackupError::InvalidComponent)
    }
}
