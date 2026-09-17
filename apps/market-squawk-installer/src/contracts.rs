//! Public lifecycle requests, status, and receipts.

use std::path::{Path, PathBuf};

use serde::Serialize;
use url::Url;

use crate::lifecycle::InstallError;
use crate::manifest::{AdmittedRelease, ReleaseManifest};
use crate::update_metadata::PendingTrustedUpdate;

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
    pub(crate) trusted_update: Option<PendingTrustedUpdate>,
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
            trusted_update: None,
        })
    }

    /// Binds exact local release files to an already verified threshold-signed metadata chain.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError::TrustedUpdateIdentity`] unless both files match their exact signed
    /// targets and the release manifest's own archive identity.
    pub fn from_trusted_local(
        root: PathBuf,
        manifest: &[u8],
        bundle: &Path,
        pending: PendingTrustedUpdate,
        manifest_target_path: &str,
        archive_target_path: &str,
    ) -> Result<Self, InstallError> {
        if manifest_target_path == archive_target_path {
            return Err(InstallError::TrustedUpdateIdentity);
        }
        let release = ReleaseManifest::admit_current(manifest)?;
        let manifest_target = pending
            .target(manifest_target_path)
            .ok_or(InstallError::TrustedUpdateIdentity)?;
        let archive_target = pending
            .target(archive_target_path)
            .ok_or(InstallError::TrustedUpdateIdentity)?;
        let manifest_length =
            u64::try_from(manifest.len()).map_err(|_| InstallError::TrustedUpdateIdentity)?;
        if manifest_target.length() != manifest_length
            || lower_hex(manifest_target.sha256()) != release.manifest_sha256()
            || archive_target.length() != release.target_release().archive.size
            || lower_hex(archive_target.sha256())
                != release.target_release().archive.sha256.as_ref()
        {
            return Err(InstallError::TrustedUpdateIdentity);
        }
        Ok(Self {
            root,
            release,
            bundle: bundle.to_path_buf(),
            channel_manifest_url: None,
            trusted_update: Some(pending),
        })
    }

    /// Binds a validated HTTPS update channel to this exact release.
    ///
    /// # Errors
    ///
    /// Returns [`InstallError::ManifestUrl`] when the URL is not an uncredentialed HTTPS URL, or
    /// [`InstallError::TrustedUpdateRequired`] when the request has no signed update admission.
    pub fn with_channel_manifest_url(mut self, url: &str) -> Result<Self, InstallError> {
        validate_manifest_url(url)?;
        if self.trusted_update.is_none() {
            return Err(InstallError::TrustedUpdateRequired);
        }
        self.channel_manifest_url = Some(url.into());
        Ok(self)
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
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

/// Request to revalidate and reactivate the retained previous known-good version.
#[derive(Debug)]
pub struct RollbackRequest {
    pub(crate) root: PathBuf,
    pub(crate) release: Option<AdmittedRelease>,
    pub(crate) bundle: Option<PathBuf>,
    pub(crate) channel_manifest_url: Option<Box<str>>,
}

impl RollbackRequest {
    /// Creates a rollback request that uses the retained previous release cache.
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            release: None,
            bundle: None,
            channel_manifest_url: None,
        }
    }

    /// Creates a rollback request with an admitted exact copy of the retained previous release.
    ///
    /// The lifecycle uses this source only when it identifies the retained previous release
    /// exactly. A damaged retained cache or version tree is reconstructed before activation.
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

    /// Binds a validated HTTPS update channel to the recovered installation.
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

/// One revalidated installed-program view for startup composition.
///
/// Paths are present only when the active release, stable entrypoints, target, and requested
/// executable all pass one locked verification. Recovery readiness independently reports whether
/// the exact retained release cache can reconstruct that active release.
#[derive(Clone, Debug)]
pub struct ProgramInstallSnapshot {
    pub(crate) status: InstallStatus,
    pub(crate) active_release_root: Option<PathBuf>,
    pub(crate) program_path: Option<PathBuf>,
    pub(crate) recovery_ready: bool,
}

impl ProgramInstallSnapshot {
    /// Returns the status derived from the same locked verification.
    pub const fn status(&self) -> &InstallStatus {
        &self.status
    }

    /// Returns the verified active immutable release root.
    pub fn active_release_root(&self) -> Option<&Path> {
        self.active_release_root.as_deref()
    }

    /// Returns the verified requested program in the active release.
    pub fn program_path(&self) -> Option<&Path> {
        self.program_path.as_deref()
    }

    /// Returns whether the retained exact release cache passed complete verification.
    pub const fn recovery_ready(&self) -> bool {
        self.recovery_ready
    }

    pub(crate) fn absent() -> Self {
        Self {
            status: InstallStatus {
                installed: false,
                active_version: None,
                previous_version: None,
                target: None,
                manifest_sha256: None,
                channel_manifest_url: None,
                healthy: false,
            },
            active_release_root: None,
            program_path: None,
            recovery_ready: false,
        }
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
