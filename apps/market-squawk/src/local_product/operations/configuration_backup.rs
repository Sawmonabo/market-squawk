//! Workspace-backup adapter for settings and recommendation-setup authorities.

use std::{fmt, io::Write, path::Path, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use market_squawk_domain::{SchemaVersion, SourceIdentifier};
use market_squawk_runtime::WorkspaceId;
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::{
    backup::{
        ProductBackupComponentKind, ProductBackupComponentSchema, ProductBackupError,
        ProductBackupSensitivity, ProductBackupSnapshot,
    },
    recommendation::{RecommendationSetupAuthority, RetainedRecommendationSetupBackup},
};

use super::{
    settings::{ProductionSettingsOperations, RetainedWorkspaceConfiguration},
    workspace_backup::{
        WorkspaceComponentDescriptor, WorkspaceComponentSnapshotAuthority,
        WorkspaceComponentSnapshotLease, WorkspaceComponentSnapshotReceipt,
    },
};

const CONFIGURATION_PRODUCER: &str = "market-squawk-workspace-configuration-v1";
const CONFIGURATION_FORMAT_VERSION: u16 = 2;
const CONFIGURATION_AUTHORITY_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/workspace-configuration-authorities/v2\0";

/// The one Configuration component owner: settings plus explicit recommendation setup.
pub(crate) struct ConfigurationWorkspaceBackupAuthority {
    settings: Arc<ProductionSettingsOperations>,
    recommendation_setup: Arc<RecommendationSetupAuthority>,
    descriptors: [WorkspaceComponentDescriptor; 1],
}

impl ConfigurationWorkspaceBackupAuthority {
    /// Binds the Configuration component to the same transaction owner that persists settings.
    pub(super) fn try_new(
        settings: Arc<ProductionSettingsOperations>,
        recommendation_setup: Arc<RecommendationSetupAuthority>,
    ) -> Result<Self, ProductBackupError> {
        let producer = SourceIdentifier::try_from(CONFIGURATION_PRODUCER)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        let schema =
            ProductBackupComponentSchema::try_new(producer.clone(), SchemaVersion::CURRENT)?;
        Ok(Self {
            settings,
            recommendation_setup,
            descriptors: [WorkspaceComponentDescriptor::try_new(
                ProductBackupComponentKind::Configuration,
                producer,
                schema,
                ProductBackupSensitivity::Protected,
            )?],
        })
    }
}

impl fmt::Debug for ConfigurationWorkspaceBackupAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "ConfigurationWorkspaceBackupAuthority([SETTINGS AND RECOMMENDATION SETUP OWNERS])",
        )
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotAuthority for ConfigurationWorkspaceBackupAuthority {
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
        let retained = self
            .settings
            .retain_workspace_configuration()
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        let recommendation_setup = self
            .recommendation_setup
            .retain_workspace_backup()
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        let component = RetainedConfigurationComponent::try_new(&retained, &recommendation_setup)?;
        Ok(Box::new(RetainedConfigurationWorkspaceSnapshot {
            settings: Arc::clone(&self.settings),
            recommendation_setup: Arc::clone(&self.recommendation_setup),
            descriptors: self.descriptors.clone(),
            retained,
            retained_recommendation_setup: recommendation_setup,
            canonical_bytes: component.canonical_bytes,
            authority_revision_sha256: component.authority_revision_sha256,
        }))
    }
}

struct RetainedConfigurationWorkspaceSnapshot {
    settings: Arc<ProductionSettingsOperations>,
    recommendation_setup: Arc<RecommendationSetupAuthority>,
    descriptors: [WorkspaceComponentDescriptor; 1],
    retained: RetainedWorkspaceConfiguration,
    retained_recommendation_setup: RetainedRecommendationSetupBackup,
    canonical_bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
}

impl fmt::Debug for RetainedConfigurationWorkspaceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .write_str("RetainedConfigurationWorkspaceSnapshot([CANONICAL CONFIGURATION EXPORT])")
    }
}

#[async_trait]
impl WorkspaceComponentSnapshotLease for RetainedConfigurationWorkspaceSnapshot {
    fn descriptors(&self) -> &[WorkspaceComponentDescriptor] {
        &self.descriptors
    }

    async fn write_snapshot(
        &mut self,
        kind: ProductBackupComponentKind,
        _snapshot: ProductBackupSnapshot,
        writer: &mut (dyn Write + Send),
        cancellation: &CancellationToken,
    ) -> Result<WorkspaceComponentSnapshotReceipt, ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::Configuration {
            return Err(ProductBackupError::InvalidComponent);
        }
        let bytes = &self.canonical_bytes;
        writer
            .write_all(bytes)
            .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
        WorkspaceComponentSnapshotReceipt::try_new(
            self.authority_revision_sha256,
            u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            Sha256::digest(bytes).into(),
        )
    }

    async fn revalidate(
        &mut self,
        kind: ProductBackupComponentKind,
        _snapshot: ProductBackupSnapshot,
        _receipt: WorkspaceComponentSnapshotReceipt,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        if cancellation.is_cancelled() {
            return Err(ProductBackupError::Cancelled);
        }
        if kind != ProductBackupComponentKind::Configuration {
            return Err(ProductBackupError::InvalidComponent);
        }
        self.settings
            .revalidate_workspace_configuration(&self.retained)
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
        self.recommendation_setup
            .revalidate_workspace_backup(&self.retained_recommendation_setup)
            .map_err(|_| ProductBackupError::SnapshotMismatch)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigurationComponentBackup {
    format_version: u16,
    settings_base64: String,
    recommendation_setup_base64: String,
    settings_bytes_sha256: [u8; 32],
    recommendation_setup_bytes_sha256: [u8; 32],
    settings_authority_sha256: [u8; 32],
    recommendation_setup_authority_sha256: [u8; 32],
    semantic_authority_sha256: [u8; 32],
}

struct RetainedConfigurationComponent {
    canonical_bytes: Vec<u8>,
    authority_revision_sha256: [u8; 32],
}

impl RetainedConfigurationComponent {
    fn try_new(
        settings: &RetainedWorkspaceConfiguration,
        recommendation_setup: &RetainedRecommendationSetupBackup,
    ) -> Result<Self, ProductBackupError> {
        let settings_bytes_sha256 = Sha256::digest(settings.canonical_bytes()).into();
        let recommendation_setup_bytes_sha256 =
            Sha256::digest(recommendation_setup.canonical_bytes()).into();
        let settings_authority_sha256 = settings.authority_revision_sha256();
        let recommendation_setup_authority_sha256 =
            recommendation_setup.authority_revision_sha256();
        let semantic_authority_sha256 = configuration_authority_digest(
            settings_authority_sha256,
            recommendation_setup_authority_sha256,
        );
        let backup = ConfigurationComponentBackup {
            format_version: CONFIGURATION_FORMAT_VERSION,
            settings_base64: STANDARD_NO_PAD.encode(settings.canonical_bytes()),
            recommendation_setup_base64: STANDARD_NO_PAD
                .encode(recommendation_setup.canonical_bytes()),
            settings_bytes_sha256,
            recommendation_setup_bytes_sha256,
            settings_authority_sha256,
            recommendation_setup_authority_sha256,
            semantic_authority_sha256,
        };
        let canonical_bytes =
            serde_json::to_vec(&backup).map_err(|_| ProductBackupError::InvalidComponent)?;
        if canonical_bytes.is_empty() {
            return Err(ProductBackupError::InvalidComponent);
        }
        Ok(Self {
            canonical_bytes,
            authority_revision_sha256: semantic_authority_sha256,
        })
    }
}

/// Restores the strict Configuration envelope through both fresh-target typed authorities.
pub(super) fn restore_configuration_component_absent(
    control_root: &Path,
    target_workspace: WorkspaceId,
    seed: crate::application::settings::SettingsSeed,
    lifecycle: super::settings::SettingsLifecycleAuthority,
    canonical_bytes: &[u8],
) -> Result<(), ServiceError> {
    let backup = serde_json::from_slice::<ConfigurationComponentBackup>(canonical_bytes)
        .map_err(|_| ServiceError::InvalidRequest)?;
    if backup.format_version != CONFIGURATION_FORMAT_VERSION
        || backup.settings_authority_sha256 == [0; 32]
        || backup.recommendation_setup_authority_sha256 == [0; 32]
        || backup.semantic_authority_sha256
            != configuration_authority_digest(
                backup.settings_authority_sha256,
                backup.recommendation_setup_authority_sha256,
            )
    {
        return Err(ServiceError::InvalidRequest);
    }
    let settings = decode_component(&backup.settings_base64)?;
    let recommendation_setup = decode_component(&backup.recommendation_setup_base64)?;
    if <[u8; 32]>::from(Sha256::digest(&settings)) != backup.settings_bytes_sha256
        || <[u8; 32]>::from(Sha256::digest(&recommendation_setup))
            != backup.recommendation_setup_bytes_sha256
    {
        return Err(ServiceError::InvalidRequest);
    }
    RecommendationSetupAuthority::validate_workspace_backup_for_rebind(
        target_workspace,
        &recommendation_setup,
    )
    .map_err(|_| ServiceError::InvalidRequest)?;
    RecommendationSetupAuthority::ensure_workspace_backup_target_absent(control_root)
        .map_err(|_| ServiceError::InvalidResult)?;
    let _settings = ProductionSettingsOperations::restore_workspace_configuration_absent(
        control_root,
        seed,
        lifecycle,
        &settings,
    )?;
    let restored = RecommendationSetupAuthority::restore_workspace_backup_rebound_absent(
        control_root,
        target_workspace,
        &recommendation_setup,
    )
    .map_err(|_| ServiceError::Unavailable)?;
    if restored.owner_workspace() != target_workspace {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn decode_component(encoded: &str) -> Result<Vec<u8>, ServiceError> {
    if encoded.is_empty() {
        return Err(ServiceError::InvalidRequest);
    }
    STANDARD_NO_PAD
        .decode(encoded)
        .map_err(|_| ServiceError::InvalidRequest)
}

fn configuration_authority_digest(
    settings_authority_sha256: [u8; 32],
    recommendation_setup_authority_sha256: [u8; 32],
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(CONFIGURATION_AUTHORITY_DIGEST_DOMAIN);
    digest.update(settings_authority_sha256);
    digest.update(recommendation_setup_authority_sha256);
    digest.finalize().into()
}
