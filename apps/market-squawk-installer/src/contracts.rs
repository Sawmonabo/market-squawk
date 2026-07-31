//! Public lifecycle requests, status, and receipts.

use std::path::{Path, PathBuf};

use serde::Serialize;
use url::Url;

use crate::lifecycle::InstallError;
use crate::manifest::{AdmittedRelease, ReleaseManifest};

/// A fully admitted local or downloaded initial release.
#[derive(Debug)]
pub struct InstallRequest {
    pub(crate) root: PathBuf,
    pub(crate) release: AdmittedRelease,
    pub(crate) bundle: PathBuf,
    pub(crate) channel_manifest_url: Option<Box<str>>,
}

impl InstallRequest {
    /// Creates an offline installation request from exact manifest bytes and a complete bundle.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError`] when the manifest cannot be admitted for this platform.
    pub fn from_local(root: PathBuf, manifest: &[u8], bundle: &Path) -> Result<Self, InstallError> {
        Ok(Self {
            root,
            release: ReleaseManifest::admit_current(manifest)?,
            bundle: bundle.to_path_buf(),
            channel_manifest_url: None,
        })
    }

    /// Binds a validated HTTPS update channel to this exact release.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError::ManifestUrl`] for a credentialed, non-HTTPS, hostless, or
    /// fragment-bearing URL.
    pub fn with_channel_manifest_url(mut self, url: &str) -> Result<Self, InstallError> {
        validate_manifest_url(url)?;
        self.channel_manifest_url = Some(url.into());
        Ok(self)
    }
}

/// A fully admitted newer local or downloaded release.
#[derive(Debug)]
pub struct UpdateRequest {
    pub(crate) root: PathBuf,
    pub(crate) release: AdmittedRelease,
    pub(crate) bundle: PathBuf,
    pub(crate) channel_manifest_url: Option<Box<str>>,
}

impl UpdateRequest {
    /// Creates an offline update request from exact manifest bytes and a complete bundle.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError`] when the manifest cannot be admitted for this platform.
    pub fn from_local(root: PathBuf, manifest: &[u8], bundle: &Path) -> Result<Self, InstallError> {
        Ok(Self {
            root,
            release: ReleaseManifest::admit_current(manifest)?,
            bundle: bundle.to_path_buf(),
            channel_manifest_url: None,
        })
    }

    /// Binds a validated HTTPS update channel to this exact release.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError::ManifestUrl`] when the URL is not an uncredentialed HTTPS URL.
    pub fn with_channel_manifest_url(mut self, url: &str) -> Result<Self, InstallError> {
        validate_manifest_url(url)?;
        self.channel_manifest_url = Some(url.into());
        Ok(self)
    }
}

/// Request to verify and reconstruct the active immutable version when necessary.
#[derive(Debug)]
pub struct RepairRequest {
    pub(crate) root: PathBuf,
    pub(crate) release: Option<AdmittedRelease>,
    pub(crate) bundle: Option<PathBuf>,
    pub(crate) channel_manifest_url: Option<Box<str>>,
}

impl RepairRequest {
    /// Creates a repair request that uses the exact retained release cache.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            release: None,
            bundle: None,
            channel_manifest_url: None,
        }
    }

    /// Creates a repair request with an admitted same-release recovery bundle.
    ///
    /// The lifecycle rejects recovery material that does not identify the active release exactly.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError`] when the manifest cannot be admitted for this platform.
    pub fn from_local(root: PathBuf, manifest: &[u8], bundle: &Path) -> Result<Self, InstallError> {
        Ok(Self {
            root,
            release: Some(ReleaseManifest::admit_current(manifest)?),
            bundle: Some(bundle.to_path_buf()),
            channel_manifest_url: None,
        })
    }

    /// Binds a validated HTTPS update channel to recovered state.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError::ManifestUrl`] when the URL is not an uncredentialed HTTPS URL.
    pub fn with_channel_manifest_url(mut self, url: &str) -> Result<Self, InstallError> {
        validate_manifest_url(url)?;
        self.channel_manifest_url = Some(url.into());
        Ok(self)
    }
}

/// Request to reactivate the retained previous known-good version.
#[derive(Debug)]
pub struct RollbackRequest {
    pub(crate) root: PathBuf,
}

impl RollbackRequest {
    /// Creates a rollback request for one controlled program root.
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

/// Explicit class of mutable user data eligible for separately confirmed deletion.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum MutableDataClass {
    /// Local configuration excluding credentials.
    Configuration,
    /// Locally stored credential material.
    Credentials,
    /// SQLite catalogs and control state.
    Catalogs,
    /// Portfolio source records and calculated state.
    Portfolios,
    /// Research datasets.
    Datasets,
    /// Model bundles and training products.
    Models,
    /// Local structured logs.
    Logs,
    /// Controlled large-output artifacts.
    Artifacts,
}

/// Data-preserving uninstall request with separately confirmed mutable-data deletions.
#[derive(Debug)]
pub struct UninstallRequest {
    pub(crate) root: PathBuf,
    pub(crate) deletions: Vec<(MutableDataClass, PathBuf)>,
}

impl UninstallRequest {
    /// Creates the default uninstall request, which removes programs only.
    pub fn preserving_data(root: PathBuf) -> Self {
        Self {
            root,
            deletions: Vec::new(),
        }
    }

    /// Adds one explicit mutable-data class deletion.
    ///
    /// Callers must obtain separate confirmation for each class before invoking this method.
    pub fn confirm_delete(mut self, class: MutableDataClass, root: PathBuf) -> Self {
        self.deletions.push((class, root));
        self
    }
}

/// Completed installation lifecycle receipt.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstallReceipt {
    pub(crate) version: Box<str>,
    pub(crate) previous_version: Option<Box<str>>,
    pub(crate) manifest_sha256: Box<str>,
    pub(crate) target: Box<str>,
    pub(crate) repaired: bool,
}

impl InstallReceipt {
    /// Returns the active release version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns whether the operation reconstructed a damaged active version.
    pub const fn repaired(&self) -> bool {
        self.repaired
    }
}

/// Current installed-program state.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstallStatus {
    pub(crate) installed: bool,
    pub(crate) active_version: Option<Box<str>>,
    pub(crate) previous_version: Option<Box<str>>,
    pub(crate) target: Option<Box<str>>,
    pub(crate) manifest_sha256: Option<Box<str>>,
    pub(crate) channel_manifest_url: Option<Box<str>>,
    pub(crate) healthy: bool,
}

impl InstallStatus {
    /// Returns whether an active selector exists.
    pub const fn is_installed(&self) -> bool {
        self.installed
    }

    /// Returns the active version when installed.
    pub fn active_version(&self) -> Option<&str> {
        self.active_version.as_deref()
    }

    /// Returns the retained previous version when present.
    pub fn previous_version(&self) -> Option<&str> {
        self.previous_version.as_deref()
    }

    /// Returns the retained HTTPS update channel when present.
    pub fn channel_manifest_url(&self) -> Option<&str> {
        self.channel_manifest_url.as_deref()
    }

    /// Returns whether every active component revalidated.
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }
}

/// Completed uninstall receipt.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UninstallReceipt {
    pub(crate) removed_program: bool,
    pub(crate) deleted_data_classes: Vec<MutableDataClass>,
}

impl UninstallReceipt {
    /// Returns whether program state was removed.
    pub const fn removed_program(&self) -> bool {
        self.removed_program
    }
}

fn validate_manifest_url(value: &str) -> Result<(), InstallError> {
    let url = Url::parse(value).map_err(|_| InstallError::ManifestUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(InstallError::ManifestUrl);
    }
    Ok(())
}
