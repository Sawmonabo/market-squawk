//! Admission of the complete native-package release before product composition.

use std::{
    collections::BTreeSet,
    fs,
    io::Read as _,
    path::{Path, PathBuf},
};

use market_squawk_installer::{
    InstallError, InstallRequest, MAXIMUM_MANIFEST_BYTES, ManifestError, PlatformError,
    ReleaseManifest, SupportedTarget, UpdateRequest, active_release_root, default_install_root,
    install, repair, status, update,
};
use semver::Version;
use tauri::Manager as _;
use thiserror::Error;

const MAXIMUM_CHECKSUM_BYTES: u64 = 64 * 1024;
const MAXIMUM_BOOTSTRAP_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_BUNDLE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Installed program state admitted for desktop composition.
#[derive(Debug)]
pub(crate) struct PreparedInstallation {
    pub(crate) root: PathBuf,
    pub(crate) active_release_root: Option<PathBuf>,
}

#[derive(Debug)]
struct PackagedRelease {
    manifest: Vec<u8>,
    bundle: PathBuf,
    channel_manifest_url: Box<str>,
    version: Version,
}

/// Installs, updates, or repairs the complete packaged release and returns its active root.
pub(crate) fn prepare(
    app: &tauri::AppHandle,
) -> Result<PreparedInstallation, InstallationStartupError> {
    let root = default_install_root()?;
    let packaged = packaged_release(app)?;
    let current = status(&root)?;

    if current.is_installed() {
        if let Some(packaged) = packaged {
            let active_version = current
                .active_version()
                .ok_or(InstallationStartupError::InvalidInstalledVersion)?;
            let active = Version::parse(active_version)
                .map_err(|_| InstallationStartupError::InvalidInstalledVersion)?;
            if should_activate_packaged_release(
                &packaged.version,
                &active,
                current.previous_version(),
            ) {
                update(
                    UpdateRequest::from_local(root.clone(), &packaged.manifest, &packaged.bundle)?
                        .with_channel_manifest_url(&packaged.channel_manifest_url)?,
                )?;
            } else if !current.is_healthy() && packaged.version == active {
                repair(
                    market_squawk_installer::RepairRequest::from_local(
                        root.clone(),
                        &packaged.manifest,
                        &packaged.bundle,
                    )?
                    .with_channel_manifest_url(&packaged.channel_manifest_url)?,
                )?;
            } else if !current.is_healthy() {
                repair(market_squawk_installer::RepairRequest::new(root.clone()))?;
            }
        } else if !current.is_healthy() {
            repair(market_squawk_installer::RepairRequest::new(root.clone()))?;
        }
        return Ok(PreparedInstallation {
            active_release_root: Some(active_release_root(&root)?),
            root,
        });
    }

    if let Some(packaged) = packaged {
        install(
            InstallRequest::from_local(root.clone(), &packaged.manifest, &packaged.bundle)?
                .with_channel_manifest_url(&packaged.channel_manifest_url)?,
        )?;
        return Ok(PreparedInstallation {
            active_release_root: Some(active_release_root(&root)?),
            root,
        });
    }

    if cfg!(debug_assertions) {
        return Ok(PreparedInstallation {
            root,
            active_release_root: None,
        });
    }
    Err(InstallationStartupError::PackagedReleaseUnavailable)
}

fn should_activate_packaged_release(
    packaged: &Version,
    active: &Version,
    previous: Option<&str>,
) -> bool {
    let packaged_version = packaged.to_string();
    packaged > active && previous != Some(packaged_version.as_str())
}

fn packaged_release(
    app: &tauri::AppHandle,
) -> Result<Option<PackagedRelease>, InstallationStartupError> {
    let directory = app.path().resource_dir()?.join("market-squawk-release");
    let metadata = match fs::symlink_metadata(&directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(InstallationStartupError::Io(source)),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(InstallationStartupError::InvalidPackagedRelease);
    }

    let target = SupportedTarget::current()?;
    let version = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|_| InstallationStartupError::InvalidPackagedRelease)?;
    let bundle_name = format!("market-squawk-{version}-{}.zip", target.as_str());
    let bootstrap_name = format!(
        "market-squawk-bootstrap-{}{}",
        target.as_str(),
        target.executable_suffix()
    );
    let expected = BTreeSet::from([
        "SHA256SUMS".to_owned(),
        bootstrap_name.clone(),
        bundle_name.clone(),
        "market-squawk-release.json".to_owned(),
    ]);
    let observed = read_file_names(&directory)?;
    if observed != expected {
        return Err(InstallationStartupError::InvalidPackagedRelease);
    }

    let manifest_path = directory.join("market-squawk-release.json");
    let manifest = read_bounded(&manifest_path, MAXIMUM_MANIFEST_BYTES as u64)?;
    let admitted = ReleaseManifest::admit_current(&manifest)?;
    if admitted.version() != version.to_string() {
        return Err(InstallationStartupError::InvalidPackagedRelease);
    }
    let bundle = bounded_regular_file(&directory.join(bundle_name), MAXIMUM_BUNDLE_BYTES)?;
    bounded_regular_file(&directory.join(bootstrap_name), MAXIMUM_BOOTSTRAP_BYTES)?;
    bounded_regular_file(&directory.join("SHA256SUMS"), MAXIMUM_CHECKSUM_BYTES)?;
    Ok(Some(PackagedRelease {
        manifest,
        bundle,
        channel_manifest_url: format!(
            "https://github.com/Sawmonabo/market-squawk/releases/latest/download/\
             market-squawk-release-{}.json",
            target.as_str()
        )
        .into(),
        version,
    }))
}

fn read_file_names(root: &Path) -> Result<BTreeSet<String>, InstallationStartupError> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| InstallationStartupError::InvalidPackagedRelease)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() || !names.insert(name) {
            return Err(InstallationStartupError::InvalidPackagedRelease);
        }
    }
    Ok(names)
}

fn bounded_regular_file(path: &Path, maximum: u64) -> Result<PathBuf, InstallationStartupError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(InstallationStartupError::InvalidPackagedRelease);
    }
    Ok(path.to_path_buf())
}

fn read_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>, InstallationStartupError> {
    let path = bounded_regular_file(path, maximum)?;
    let mut file = fs::File::open(path)?.take(maximum + 1);
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > maximum {
        return Err(InstallationStartupError::InvalidPackagedRelease);
    }
    Ok(bytes)
}

/// Complete packaged-release admission or activation failure.
#[derive(Debug, Error)]
pub(crate) enum InstallationStartupError {
    #[error("the native package does not contain a complete verified release")]
    PackagedReleaseUnavailable,
    #[error("the native package release resource is invalid")]
    InvalidPackagedRelease,
    #[error("the installed release version is invalid")]
    InvalidInstalledVersion,
    #[error("the native package resource path is unavailable")]
    ResourcePath(#[from] tauri::Error),
    #[error("the native package release could not be read")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Platform(#[from] PlatformError),
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Install(#[from] InstallError),
}

#[cfg(test)]
mod tests {
    use semver::Version;

    use super::should_activate_packaged_release;

    #[test]
    fn packaged_startup_preserves_an_intentional_rollback() {
        let older = Version::new(1, 0, 0);
        let packaged = Version::new(1, 1, 0);

        assert!(should_activate_packaged_release(&packaged, &older, None));
        assert!(!should_activate_packaged_release(
            &packaged,
            &older,
            Some("1.1.0")
        ));
        assert!(!should_activate_packaged_release(&older, &packaged, None));
    }
}
