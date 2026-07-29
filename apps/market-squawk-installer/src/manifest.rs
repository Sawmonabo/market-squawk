//! Closed release-manifest parsing and validation.

use std::collections::BTreeSet;

use chrono::DateTime;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use crate::platform::{PlatformError, SupportedTarget};

pub(crate) const MAXIMUM_ARCHIVE_BYTES: u64 = 2 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_ENTRY_BYTES: u64 = 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_EXPANDED_BYTES: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const MAXIMUM_MANIFEST_BYTES: usize = 1024 * 1024;
pub(crate) const MAXIMUM_ARCHIVE_ENTRIES: usize = 32_768;
pub(crate) const MANIFEST_SCHEMA_VERSION: u32 = 1;
const PRODUCT_IDENTITY: &str = "market-squawk";
const REPOSITORY_IDENTITY: &str = "Sawmonabo/market-squawk";
const MAXIMUM_COMPONENT_PATH_BYTES: usize = 1_024;
const MAXIMUM_PATH_COMPONENT_BYTES: usize = 255;

/// A complete multi-platform Market Squawk release manifest.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    schema_version: u32,
    product: Box<str>,
    version: Box<str>,
    tag: Box<str>,
    repository: Box<str>,
    commit_sha: Box<str>,
    tree_sha: Box<str>,
    generated_at: Box<str>,
    targets: Vec<TargetRelease>,
}

/// One current-platform release admitted from exact manifest bytes.
#[derive(Clone, Debug)]
pub struct AdmittedRelease {
    manifest: ReleaseManifest,
    manifest_bytes: Box<[u8]>,
    manifest_sha256: Box<str>,
    target_index: usize,
}

impl ReleaseManifest {
    /// Parses and validates a manifest, selecting the current supported target.
    ///
    /// # Errors
    ///
    /// Returns [`ManifestError`] when the document is oversized, malformed, contains an unknown
    /// field, violates the release identity, omits a required component, or has no exact
    /// current-platform entry.
    pub fn admit_current(bytes: &[u8]) -> Result<AdmittedRelease, ManifestError> {
        if bytes.is_empty() || bytes.len() > MAXIMUM_MANIFEST_BYTES {
            return Err(ManifestError::ManifestSize {
                bytes: bytes.len(),
                maximum: MAXIMUM_MANIFEST_BYTES,
            });
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(ManifestError::MalformedJson)?;
        let target = SupportedTarget::current()?;
        let target_index = manifest.validate(target)?;
        Ok(AdmittedRelease {
            manifest,
            manifest_bytes: bytes.into(),
            manifest_sha256: sha256_bytes(bytes).into(),
            target_index,
        })
    }

    fn validate(&self, selected_target: SupportedTarget) -> Result<usize, ManifestError> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION
            || self.product.as_ref() != PRODUCT_IDENTITY
            || self.repository.as_ref() != REPOSITORY_IDENTITY
        {
            return Err(ManifestError::ReleaseIdentity);
        }
        let version = Version::parse(&self.version).map_err(|_| ManifestError::ReleaseVersion)?;
        if self.version.as_ref() != version.to_string()
            || self.tag.as_ref() != format!("v{version}")
        {
            return Err(ManifestError::ReleaseVersion);
        }
        if !is_lower_object_id(&self.commit_sha) || !is_lower_object_id(&self.tree_sha) {
            return Err(ManifestError::RepositoryObjectIdentity);
        }
        DateTime::parse_from_rfc3339(&self.generated_at).map_err(|_| ManifestError::GeneratedAt)?;
        if self.targets.is_empty() {
            return Err(ManifestError::TargetSet);
        }

        let mut previous = None;
        let mut selected = None;
        for (index, target) in self.targets.iter().enumerate() {
            if previous.is_some_and(|prior| prior >= target.target) {
                return Err(ManifestError::TargetSet);
            }
            target.validate(&self.tag)?;
            if target.target == selected_target {
                selected = Some(index);
            }
            previous = Some(target.target);
        }
        selected.ok_or(ManifestError::TargetUnavailable {
            target: selected_target,
        })
    }
}

impl AdmittedRelease {
    /// Returns the release version.
    pub fn version(&self) -> &str {
        &self.manifest.version
    }

    /// Returns the release tag.
    pub fn tag(&self) -> &str {
        &self.manifest.tag
    }

    /// Returns the selected supported target.
    pub fn target(&self) -> SupportedTarget {
        self.target_release().target
    }

    /// Returns the SHA-256 identity of the exact admitted manifest bytes.
    pub fn manifest_sha256(&self) -> &str {
        &self.manifest_sha256
    }

    pub(crate) fn manifest_bytes(&self) -> &[u8] {
        &self.manifest_bytes
    }

    pub(crate) fn target_release(&self) -> &TargetRelease {
        &self.manifest.targets[self.target_index]
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetRelease {
    pub(crate) target: SupportedTarget,
    minimum_system: Box<str>,
    pub(crate) archive: ArtifactIdentity,
    pub(crate) components: Vec<ComponentIdentity>,
}

impl TargetRelease {
    fn validate(&self, tag: &str) -> Result<(), ManifestError> {
        if self.minimum_system.as_ref() != self.target.minimum_system() {
            return Err(ManifestError::MinimumSystem {
                target: self.target,
            });
        }
        self.archive.validate(tag)?;
        if self.components.is_empty() || self.components.len() > MAXIMUM_ARCHIVE_ENTRIES {
            return Err(ManifestError::ComponentSet);
        }

        let mut previous_path: Option<&str> = None;
        let mut portable_paths = BTreeSet::new();
        let mut expanded_bytes = 0_u64;
        for component in &self.components {
            component.validate()?;
            if previous_path.is_some_and(|previous| previous >= component.path.as_ref()) {
                return Err(ManifestError::ComponentSet);
            }
            if !portable_paths.insert(component.path.to_ascii_lowercase()) {
                return Err(ManifestError::ComponentSet);
            }
            expanded_bytes = expanded_bytes
                .checked_add(component.size)
                .ok_or(ManifestError::ExpandedSize)?;
            if expanded_bytes > MAXIMUM_EXPANDED_BYTES {
                return Err(ManifestError::ExpandedSize);
            }
            previous_path = Some(&component.path);
        }

        for role in ComponentRole::REQUIRED {
            let count = self
                .components
                .iter()
                .filter(|component| component.role == role)
                .count();
            if (role == ComponentRole::PythonEnvironment && count == 0)
                || (role != ComponentRole::PythonEnvironment && count != 1)
            {
                return Err(ManifestError::MissingRequiredRole { role });
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactIdentity {
    pub(crate) url: Box<str>,
    pub(crate) size: u64,
    pub(crate) sha256: Box<str>,
}

impl ArtifactIdentity {
    fn validate(&self, tag: &str) -> Result<(), ManifestError> {
        if self.size == 0 || self.size > MAXIMUM_ARCHIVE_BYTES || !is_lower_sha256(&self.sha256) {
            return Err(ManifestError::ArchiveIdentity);
        }
        let url = Url::parse(&self.url).map_err(|_| ManifestError::ArchiveUrl)?;
        let release_prefix = format!("/Sawmonabo/market-squawk/releases/download/{tag}/");
        let release_asset = url.path().strip_prefix(&release_prefix);
        if url.scheme() != "https"
            || url.host_str() != Some("github.com")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || release_asset.is_none_or(|asset| asset.is_empty() || asset.contains('/'))
        {
            return Err(ManifestError::ArchiveUrl);
        }
        Ok(())
    }
}

/// One exact regular file in a complete release archive.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ComponentIdentity {
    pub(crate) path: Box<str>,
    pub(crate) role: ComponentRole,
    pub(crate) size: u64,
    pub(crate) sha256: Box<str>,
    pub(crate) executable: bool,
}

impl ComponentIdentity {
    fn validate(&self) -> Result<(), ManifestError> {
        validate_portable_path(&self.path)?;
        if self.size == 0
            || self.size > MAXIMUM_ENTRY_BYTES
            || !is_lower_sha256(&self.sha256)
            || (self.role.requires_executable() && !self.executable)
            || (self.role == ComponentRole::TrainingDriver && self.executable)
        {
            return Err(ManifestError::ComponentIdentity {
                path: self.path.clone(),
            });
        }
        Ok(())
    }
}

/// Code-owned role of one complete-release component.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ComponentRole {
    Desktop,
    Cli,
    CaptureHelper,
    OnnxWorker,
    ModelValidator,
    TrainingDriver,
    Installer,
    Uv,
    PythonRuntime,
    PythonEnvironment,
    DesktopResource,
    License,
    Notice,
}

impl ComponentRole {
    const REQUIRED: [Self; 10] = [
        Self::Desktop,
        Self::Cli,
        Self::CaptureHelper,
        Self::OnnxWorker,
        Self::ModelValidator,
        Self::TrainingDriver,
        Self::Installer,
        Self::Uv,
        Self::PythonRuntime,
        Self::PythonEnvironment,
    ];

    const fn requires_executable(self) -> bool {
        matches!(
            self,
            Self::Desktop
                | Self::Cli
                | Self::CaptureHelper
                | Self::OnnxWorker
                | Self::ModelValidator
                | Self::Installer
                | Self::Uv
                | Self::PythonRuntime
        )
    }
}

/// Release-manifest admission failure.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// The manifest is empty or exceeds its fixed input bound.
    #[error("release manifest is {bytes} bytes; maximum is {maximum}")]
    ManifestSize {
        /// Observed byte length.
        bytes: usize,
        /// Maximum accepted byte length.
        maximum: usize,
    },
    /// JSON decoding failed or an unknown field was present.
    #[error("release manifest JSON is malformed or unsupported")]
    MalformedJson(#[source] serde_json::Error),
    /// Product, schema, or repository identity is not exact.
    #[error("release manifest has the wrong schema, product, or repository identity")]
    ReleaseIdentity,
    /// Version and tag do not form one canonical semantic version.
    #[error("release manifest version and tag are inconsistent")]
    ReleaseVersion,
    /// Commit or tree object identity is malformed.
    #[error("release manifest repository object identity is malformed")]
    RepositoryObjectIdentity,
    /// Generation time is not RFC 3339.
    #[error("release manifest generation time is malformed")]
    GeneratedAt,
    /// Targets are empty, duplicated, or unsorted.
    #[error("release manifest targets must be nonempty, sorted, and unique")]
    TargetSet,
    /// The current supported platform has no exact release entry.
    #[error("release manifest has no entry for {target:?}")]
    TargetUnavailable {
        /// Missing current target.
        target: SupportedTarget,
    },
    /// A target declares the wrong minimum operating system.
    #[error("release manifest has the wrong minimum system for {target:?}")]
    MinimumSystem {
        /// Target with the wrong floor.
        target: SupportedTarget,
    },
    /// Archive size or digest is invalid.
    #[error("release archive identity is malformed or outside its fixed size bound")]
    ArchiveIdentity,
    /// Archive URL is not an uncredentialed HTTPS URL.
    #[error("release archive URL must be an uncredentialed HTTPS URL without a fragment")]
    ArchiveUrl,
    /// Components are empty, excessive, duplicated, or unsorted.
    #[error("release components must be nonempty, bounded, sorted, and portable-unique")]
    ComponentSet,
    /// Expanded component sizes overflow or exceed the fixed bound.
    #[error("release components exceed the fixed expanded-size bound")]
    ExpandedSize,
    /// A component path, size, digest, role, or executable contract is invalid.
    #[error("release component identity is invalid: {path}")]
    ComponentIdentity {
        /// Rejected manifest-relative path.
        path: Box<str>,
    },
    /// A required complete-product role is absent or duplicated.
    #[error("release manifest does not contain exactly the required {role:?} role")]
    MissingRequiredRole {
        /// Missing or duplicated role.
        role: ComponentRole,
    },
    /// Current platform detection failed.
    #[error(transparent)]
    Platform(#[from] PlatformError),
}

fn validate_portable_path(path: &str) -> Result<(), ManifestError> {
    if path.is_empty()
        || path.len() > MAXIMUM_COMPONENT_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
    {
        return Err(component_path_error(path));
    }
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.len() > MAXIMUM_PATH_COMPONENT_BYTES
            || component.ends_with([' ', '.'])
            || component
                .chars()
                .any(|character| character.is_control() || "<>:\"|?*".contains(character))
            || reserved_windows_name(component)
        {
            return Err(component_path_error(path));
        }
    }
    Ok(())
}

fn reserved_windows_name(component: &str) -> bool {
    let stem = component
        .split_once('.')
        .map_or(component, |(stem, _)| stem)
        .to_ascii_uppercase();
    matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || stem
            .strip_prefix("COM")
            .or_else(|| stem.strip_prefix("LPT"))
            .is_some_and(|suffix| suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9'))
}

fn component_path_error(path: &str) -> ManifestError {
    ManifestError::ComponentIdentity { path: path.into() }
}

pub(crate) fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_lower_object_id(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
