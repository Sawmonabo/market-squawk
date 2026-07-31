//! Exclusive immutable-version store and crash-safe active selector.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use atomicwrites::{AllowOverwrite, AtomicFile, DisallowOverwrite, OverwriteBehavior};
use fs2::FileExt as _;
use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::archive::{
    ArchiveError, ComponentReceipt, seal_tree_root, sync_directory, verify_installed_tree,
};
use crate::manifest::{MAXIMUM_ARCHIVE_ENTRIES, MAXIMUM_MANIFEST_BYTES, is_lower_sha256};
use crate::platform::SupportedTarget;

const INSTALLATION_STATE_SCHEMA_VERSION: u32 = 1;
// The selector can retain the active and previous component inventories. Keep its admission bound
// aligned with two admitted release manifests plus fixed selector metadata.
const MAXIMUM_INSTALLATION_STATE_BYTES: usize = (2 * MAXIMUM_MANIFEST_BYTES) + (64 * 1024);
const STATE_FILE: &str = "installation.json";
const PENDING_ACTIVATION_FILE: &str = "activation.json";
const VERSIONS_DIRECTORY: &str = "versions";
const RELEASES_DIRECTORY: &str = "releases";
const STAGING_DIRECTORY: &str = "staging";
#[cfg(unix)]
const ENTRYPOINTS_DIRECTORY: &str = "bin";
const LOCK_FILE: &str = ".market-squawk-installer.lock";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstallationState {
    schema_version: u32,
    pub(crate) active: StoredVersion,
    pub(crate) previous: Option<StoredVersion>,
    pub(crate) channel_manifest_url: Option<Box<str>>,
    changed_at_unix_seconds: u64,
}

impl InstallationState {
    pub(crate) fn initial(
        active: StoredVersion,
        channel_manifest_url: Option<Box<str>>,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            schema_version: INSTALLATION_STATE_SCHEMA_VERSION,
            active,
            previous: None,
            channel_manifest_url,
            changed_at_unix_seconds: unix_time()?,
        })
    }

    pub(crate) fn activate(
        &mut self,
        active: StoredVersion,
        channel_manifest_url: Option<Box<str>>,
        retain_current_as_previous: bool,
    ) -> Result<(), StoreError> {
        let candidate_was_previous = self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.manifest_sha256 == active.manifest_sha256);
        if candidate_was_previous {
            self.previous = retain_current_as_previous.then(|| self.active.clone());
        } else if retain_current_as_previous {
            self.previous = Some(self.active.clone());
        }
        self.active = active;
        if channel_manifest_url.is_some() {
            self.channel_manifest_url = channel_manifest_url;
        }
        self.changed_at_unix_seconds = unix_time()?;
        self.validate()
    }

    pub(crate) fn swap_for_rollback(&mut self) -> Result<(), StoreError> {
        let previous = self
            .previous
            .take()
            .ok_or(StoreError::RollbackUnavailable)?;
        self.previous = Some(std::mem::replace(&mut self.active, previous));
        self.changed_at_unix_seconds = unix_time()?;
        self.validate()
    }

    fn validate(&self) -> Result<(), StoreError> {
        if self.schema_version != INSTALLATION_STATE_SCHEMA_VERSION {
            return Err(StoreError::CorruptState);
        }
        self.active.validate()?;
        if let Some(previous) = &self.previous {
            previous.validate()?;
            if previous.manifest_sha256 == self.active.manifest_sha256 {
                return Err(StoreError::CorruptState);
            }
        }
        if let Some(url) = &self.channel_manifest_url {
            let parsed = Url::parse(url).map_err(|_| StoreError::CorruptState)?;
            if parsed.scheme() != "https"
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.fragment().is_some()
            {
                return Err(StoreError::CorruptState);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredVersion {
    pub(crate) version: Box<str>,
    pub(crate) manifest_sha256: Box<str>,
    pub(crate) archive_sha256: Box<str>,
    pub(crate) target: SupportedTarget,
    pub(crate) directory: Box<str>,
    pub(crate) components: Vec<ComponentReceipt>,
    installed_at_unix_seconds: u64,
}

impl StoredVersion {
    pub(crate) fn new(
        version: &str,
        manifest_sha256: &str,
        archive_sha256: &str,
        target: SupportedTarget,
        components: Vec<ComponentReceipt>,
    ) -> Result<Self, StoreError> {
        let directory = version_directory_name(version, manifest_sha256)?;
        let installed = Self {
            version: version.into(),
            manifest_sha256: manifest_sha256.into(),
            archive_sha256: archive_sha256.into(),
            target,
            directory: directory.into(),
            components,
            installed_at_unix_seconds: unix_time()?,
        };
        installed.validate()?;
        Ok(installed)
    }

    fn validate(&self) -> Result<(), StoreError> {
        let parsed = Version::parse(&self.version).map_err(|_| StoreError::CorruptState)?;
        if self.version.as_ref() != parsed.to_string()
            || !is_lower_sha256(&self.manifest_sha256)
            || !is_lower_sha256(&self.archive_sha256)
            || self.directory.as_ref()
                != version_directory_name(&self.version, &self.manifest_sha256)?
            || self.components.is_empty()
            || self.components.len() > MAXIMUM_ARCHIVE_ENTRIES
            || self
                .components
                .windows(2)
                .any(|pair| pair[0].path >= pair[1].path)
        {
            return Err(StoreError::CorruptState);
        }
        Ok(())
    }
}

/// Exclusively locked program store.
#[derive(Debug)]
pub(crate) struct InstallStore {
    root: PathBuf,
    _lock: File,
}

impl InstallStore {
    pub(crate) fn open_or_create(root: &Path) -> Result<Self, StoreError> {
        let parent = root.parent().ok_or(StoreError::UnsafeRoot)?;
        fs::create_dir_all(parent)
            .map_err(|source| StoreError::io("create installation parent", source))?;
        ensure_safe_directory(parent)?;
        let lock = acquire_lock(parent)?;

        match fs::symlink_metadata(root) {
            Ok(metadata) if metadata.is_dir() && !is_directory_redirect(&metadata) => {}
            Ok(_) => return Err(StoreError::UnsafeRoot),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(root)
                    .map_err(|source| StoreError::io("create installation root", source))?;
            }
            Err(source) => {
                return Err(StoreError::io("inspect installation root", source));
            }
        }
        set_private_directory_permissions(root)?;

        let store = Self {
            root: root.to_path_buf(),
            _lock: lock,
        };
        store.prepare_reserved_directories()?;
        store.clear_staging()?;
        Ok(store)
    }

    pub(crate) fn open_existing(root: &Path) -> Result<Option<Self>, StoreError> {
        match fs::symlink_metadata(root) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Ok(metadata) if metadata.is_dir() && !is_directory_redirect(&metadata) => {}
            Ok(_) => return Err(StoreError::UnsafeRoot),
            Err(source) => {
                return Err(StoreError::io("inspect installation root", source));
            }
        }
        let parent = root.parent().ok_or(StoreError::UnsafeRoot)?;
        ensure_safe_directory(parent)?;
        let lock = acquire_lock(parent)?;
        set_private_directory_permissions(root)?;
        let store = Self {
            root: root.to_path_buf(),
            _lock: lock,
        };
        store.prepare_reserved_directories()?;
        store.clear_staging()?;
        Ok(Some(store))
    }

    pub(crate) fn load_state(&self) -> Result<Option<InstallationState>, StoreError> {
        self.load_state_file(STATE_FILE)
    }

    pub(crate) fn load_pending_activation(&self) -> Result<Option<InstallationState>, StoreError> {
        self.load_state_file(PENDING_ACTIVATION_FILE)
    }

    fn load_state_file(&self, file_name: &str) -> Result<Option<InstallationState>, StoreError> {
        let path = self.root.join(file_name);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(StoreError::io("inspect installation state", source)),
        };
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAXIMUM_INSTALLATION_STATE_BYTES as u64
        {
            return Err(StoreError::CorruptState);
        }
        let file =
            File::open(path).map_err(|source| StoreError::io("open installation state", source))?;
        let mut bytes = Vec::with_capacity(
            usize::try_from(metadata.len()).map_err(|_| StoreError::CorruptState)?,
        );
        file.take(MAXIMUM_INSTALLATION_STATE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| StoreError::io("read installation state", source))?;
        if bytes.len() > MAXIMUM_INSTALLATION_STATE_BYTES {
            return Err(StoreError::CorruptState);
        }
        let state: InstallationState =
            serde_json::from_slice(&bytes).map_err(|_| StoreError::CorruptState)?;
        state.validate()?;
        Ok(Some(state))
    }

    pub(crate) fn write_state(&self, state: &InstallationState) -> Result<(), StoreError> {
        self.write_state_file(STATE_FILE, state, AllowOverwrite)
    }

    pub(crate) fn write_pending_activation(
        &self,
        state: &InstallationState,
    ) -> Result<(), StoreError> {
        self.write_state_file(PENDING_ACTIVATION_FILE, state, DisallowOverwrite)
    }

    fn write_state_file(
        &self,
        file_name: &str,
        state: &InstallationState,
        behavior: OverwriteBehavior,
    ) -> Result<(), StoreError> {
        state.validate()?;
        let mut bytes = serde_json::to_vec_pretty(state).map_err(|_| StoreError::CorruptState)?;
        bytes.push(b'\n');
        if bytes.len() > MAXIMUM_INSTALLATION_STATE_BYTES {
            return Err(StoreError::CorruptState);
        }
        let atomic = AtomicFile::new(self.root.join(file_name), behavior);
        atomic
            .write(|file| file.write_all(&bytes))
            .map_err(|error| {
                let source: std::io::Error = error.into();
                StoreError::io("publish installation state", source)
            })?;
        sync_directory(&self.root)?;
        Ok(())
    }

    pub(crate) fn clear_pending_activation(&self) -> Result<(), StoreError> {
        let path = self.root.join(PENDING_ACTIVATION_FILE);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|source| StoreError::io("inspect pending activation", source))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(StoreError::CorruptState);
        }
        fs::remove_file(path)
            .map_err(|source| StoreError::io("clear pending activation", source))?;
        sync_directory(&self.root)?;
        Ok(())
    }

    pub(crate) fn create_stage(&self, purpose: &str) -> Result<PathBuf, StoreError> {
        let stage = self
            .root
            .join(STAGING_DIRECTORY)
            .join(format!("{purpose}-{}", Uuid::new_v4().as_simple()));
        fs::create_dir(&stage)
            .map_err(|source| StoreError::io("create installation stage", source))?;
        set_private_directory_permissions(&stage)?;
        Ok(stage)
    }

    pub(crate) fn version_path(&self, version: &StoredVersion) -> PathBuf {
        self.root.join(VERSIONS_DIRECTORY).join(&*version.directory)
    }

    pub(crate) fn release_path(&self, manifest_sha256: &str) -> PathBuf {
        self.root.join(RELEASES_DIRECTORY).join(manifest_sha256)
    }

    #[cfg(unix)]
    pub(crate) fn entrypoint_path(
        &self,
        program: crate::platform::ProgramName,
        target: SupportedTarget,
    ) -> Result<PathBuf, StoreError> {
        let file_name = program
            .relative_path(target)
            .file_name()
            .ok_or(StoreError::CorruptState)?
            .to_owned();
        Ok(self.root.join(ENTRYPOINTS_DIRECTORY).join(file_name))
    }

    #[cfg(unix)]
    pub(crate) fn replace_entrypoint_directory(&self, stage: &Path) -> Result<(), StoreError> {
        let destination = self.root.join(ENTRYPOINTS_DIRECTORY);
        let retired = self.root.join(STAGING_DIRECTORY).join(format!(
            "entrypoints-retired-{}",
            Uuid::new_v4().as_simple()
        ));
        set_private_directory_permissions(&destination)?;
        fs::rename(&destination, &retired)
            .map_err(|source| StoreError::io("retire stable program entrypoints", source))?;
        if let Err(source) = fs::rename(stage, &destination) {
            return match fs::rename(&retired, &destination) {
                Ok(()) => {
                    sync_directory(&self.root)?;
                    sync_directory(&self.root.join(STAGING_DIRECTORY))?;
                    Err(StoreError::io("publish stable program entrypoints", source))
                }
                Err(_) => Err(StoreError::ActivationIndeterminate),
            };
        }
        sync_directory(&self.root)?;
        sync_directory(&self.root.join(STAGING_DIRECTORY))?;
        // The caller may still be executing from the retired tree. A later store open clears it
        // after the lifecycle command has returned or the desktop has restarted.
        Ok(())
    }

    pub(crate) fn publish_new_version(
        &self,
        stage: &Path,
        version: &StoredVersion,
    ) -> Result<PathBuf, StoreError> {
        let final_path = self.version_path(version);
        match fs::symlink_metadata(&final_path) {
            Ok(_) => return Err(StoreError::ImmutableVersionExists),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(StoreError::io(
                    "inspect immutable version destination",
                    source,
                ));
            }
        }
        fs::rename(stage, &final_path)
            .map_err(|source| StoreError::io("publish immutable version", source))?;
        sync_directory(&self.root.join(VERSIONS_DIRECTORY))?;
        Ok(final_path)
    }

    pub(crate) fn replace_corrupt_version(
        &self,
        stage: &Path,
        version: &StoredVersion,
    ) -> Result<(), StoreError> {
        let final_path = self.version_path(version);
        let quarantine = self
            .root
            .join(STAGING_DIRECTORY)
            .join(format!("corrupt-{}", Uuid::new_v4().as_simple()));
        let metadata = fs::symlink_metadata(&final_path)
            .map_err(|source| StoreError::io("inspect corrupt version root", source))?;
        if metadata.is_dir() && !is_directory_redirect(&metadata) {
            set_private_directory_permissions(&final_path)?;
        }
        if let Err(source) = fs::rename(&final_path, &quarantine) {
            return match restore_version_root_permissions(&final_path) {
                Ok(()) => Err(StoreError::io("quarantine corrupt version", source)),
                Err(_) => Err(StoreError::RepairIndeterminate),
            };
        }
        if let Err(source) = fs::rename(stage, &final_path) {
            let restore = fs::rename(&quarantine, &final_path);
            return match restore {
                Ok(()) if restore_version_root_permissions(&final_path).is_ok() => {
                    Err(StoreError::io("replace corrupt version", source))
                }
                Err(_) => Err(StoreError::RepairIndeterminate),
                Ok(()) => Err(StoreError::RepairIndeterminate),
            };
        }
        sync_directory(&self.root.join(VERSIONS_DIRECTORY))?;
        seal_tree_root(&final_path)?;
        verify_installed_tree(&final_path, &version.components)?;
        let _cleanup = remove_quarantined_entry(&quarantine);
        Ok(())
    }

    pub(crate) fn publish_release_cache(
        &self,
        stage: &Path,
        manifest_sha256: &str,
    ) -> Result<PathBuf, StoreError> {
        let final_path = self.release_path(manifest_sha256);
        match fs::symlink_metadata(&final_path) {
            Ok(_) => return Err(StoreError::ImmutableReleaseExists),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(StoreError::io(
                    "inspect immutable release destination",
                    source,
                ));
            }
        }
        fs::rename(stage, &final_path)
            .map_err(|source| StoreError::io("publish immutable release cache", source))?;
        sync_directory(&self.root.join(RELEASES_DIRECTORY))?;
        Ok(final_path)
    }

    pub(crate) fn prune(&self, state: &InstallationState) -> Result<(), StoreError> {
        let mut retained_versions = BTreeSet::from([state.active.directory.as_ref()]);
        let mut retained_releases = BTreeSet::from([state.active.manifest_sha256.as_ref()]);
        if let Some(previous) = &state.previous {
            retained_versions.insert(previous.directory.as_ref());
            retained_releases.insert(previous.manifest_sha256.as_ref());
        }
        prune_directory(
            &self.root.join(VERSIONS_DIRECTORY),
            &retained_versions,
            "prune obsolete version",
        )?;
        prune_directory(
            &self.root.join(RELEASES_DIRECTORY),
            &retained_releases,
            "prune obsolete release cache",
        )?;
        Ok(())
    }

    pub(crate) fn quarantine_for_uninstall(&self) -> Result<PathBuf, StoreError> {
        let parent = self.root.parent().ok_or(StoreError::UnsafeRoot)?;
        let quarantine = parent.join(format!(
            ".market-squawk-program-removing-{}",
            Uuid::new_v4().as_simple()
        ));
        fs::rename(&self.root, &quarantine)
            .map_err(|source| StoreError::io("detach installation root", source))?;
        sync_directory(parent)?;
        Ok(quarantine)
    }

    fn prepare_reserved_directories(&self) -> Result<(), StoreError> {
        for name in [VERSIONS_DIRECTORY, RELEASES_DIRECTORY, STAGING_DIRECTORY] {
            self.prepare_reserved_directory(name)?;
        }
        #[cfg(unix)]
        self.prepare_reserved_directory(ENTRYPOINTS_DIRECTORY)?;
        Ok(())
    }

    fn prepare_reserved_directory(&self, name: &str) -> Result<(), StoreError> {
        let path = self.root.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_dir() && !is_directory_redirect(&metadata) => Ok(()),
            Ok(_) => Err(StoreError::UnsafeRoot),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&path)
                    .map_err(|source| StoreError::io("create reserved directory", source))?;
                set_private_directory_permissions(&path)
            }
            Err(source) => Err(StoreError::io("inspect reserved directory", source)),
        }
    }

    fn clear_staging(&self) -> Result<(), StoreError> {
        let staging = self.root.join(STAGING_DIRECTORY);
        for entry in fs::read_dir(&staging)
            .map_err(|source| StoreError::io("read staging directory", source))?
        {
            let entry = entry.map_err(|source| StoreError::io("read staging entry", source))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|source| StoreError::io("inspect staging entry", source))?;
            if entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("corrupt-") || name.starts_with("entrypoints-retired-")
            }) {
                let _cleanup = remove_quarantined_entry(&path);
            } else if !metadata.is_dir() || is_directory_redirect(&metadata) {
                return Err(StoreError::UnsafeRoot);
            } else {
                remove_tree(&path)?;
            }
        }
        Ok(())
    }
}

pub(crate) fn remove_tree(path: &Path) -> Result<(), StoreError> {
    make_tree_removable(path)?;
    fs::remove_dir_all(path).map_err(|source| StoreError::io("remove program tree", source))
}

fn remove_quarantined_tree(path: &Path) -> Result<(), StoreError> {
    make_quarantined_tree_removable(path)?;
    fs::remove_dir_all(path).map_err(|source| StoreError::io("remove corrupt quarantine", source))
}

fn remove_quarantined_entry(path: &Path) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| StoreError::io("inspect corrupt quarantine root", source))?;
    if metadata.is_dir() && !is_directory_redirect(&metadata) {
        return remove_quarantined_tree(path);
    }
    if metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
        make_file_writable(path)?;
    }
    fs::remove_file(path).map_err(|source| StoreError::io("remove corrupt quarantine root", source))
}

fn restore_version_root_permissions(path: &Path) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| StoreError::io("inspect restored version root", source))?;
    if metadata.is_dir() && !is_directory_redirect(&metadata) {
        seal_tree_root(path)?;
    }
    Ok(())
}

fn prune_directory(
    root: &Path,
    retained: &BTreeSet<&str>,
    operation: &'static str,
) -> Result<(), StoreError> {
    for entry in fs::read_dir(root).map_err(|source| StoreError::io(operation, source))? {
        let entry = entry.map_err(|source| StoreError::io(operation, source))?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|source| StoreError::io(operation, source))?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| StoreError::UnsafeRoot)?;
        if !metadata.is_dir() || is_directory_redirect(&metadata) {
            return Err(StoreError::UnsafeRoot);
        }
        if !retained.contains(name.as_str()) {
            remove_quarantined_tree(&entry.path())?;
        }
    }
    sync_directory(root)?;
    Ok(())
}

fn make_tree_removable(path: &Path) -> Result<(), StoreError> {
    let mut directories = vec![path.to_path_buf()];
    let mut cursor = 0;
    while cursor < directories.len() {
        set_private_directory_permissions(&directories[cursor])?;
        for entry in fs::read_dir(&directories[cursor])
            .map_err(|source| StoreError::io("read removable program tree", source))?
        {
            let entry =
                entry.map_err(|source| StoreError::io("read removable program entry", source))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|source| StoreError::io("inspect removable program entry", source))?;
            if is_directory_redirect(&metadata) {
                return Err(StoreError::UnsafeRoot);
            }
            if metadata.is_dir() {
                directories.push(entry.path());
            } else if metadata.file_type().is_file() {
                make_file_writable(&entry.path())?;
            } else {
                return Err(StoreError::UnsafeRoot);
            }
        }
        cursor += 1;
    }
    Ok(())
}

fn make_quarantined_tree_removable(path: &Path) -> Result<(), StoreError> {
    ensure_safe_directory(path)?;
    let mut directories = vec![path.to_path_buf()];
    let mut cursor = 0;
    while cursor < directories.len() {
        set_private_directory_permissions(&directories[cursor])?;
        for entry in fs::read_dir(&directories[cursor])
            .map_err(|source| StoreError::io("read corrupt quarantine", source))?
        {
            let entry =
                entry.map_err(|source| StoreError::io("read corrupt quarantine entry", source))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|source| StoreError::io("inspect corrupt quarantine entry", source))?;
            if metadata.is_dir() && !is_directory_redirect(&metadata) {
                directories.push(path);
            } else if metadata.file_type().is_file() && !metadata.file_type().is_symlink() {
                make_file_writable(&path)?;
            }
        }
        cursor += 1;
    }
    Ok(())
}

fn acquire_lock(parent: &Path) -> Result<File, StoreError> {
    let path = parent.join(LOCK_FILE);
    let metadata = fs::symlink_metadata(&path);
    if let Ok(metadata) = metadata
        && (!metadata.file_type().is_file() || metadata.file_type().is_symlink())
    {
        return Err(StoreError::UnsafeRoot);
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    configure_lock_options(&mut options);
    let lock = options
        .open(path)
        .map_err(|source| StoreError::io("open installation lock", source))?;
    lock.try_lock_exclusive()
        .map_err(|source| match source.kind() {
            std::io::ErrorKind::WouldBlock => StoreError::AlreadyLocked,
            _ => StoreError::io("lock installation root", source),
        })?;
    Ok(lock)
}

#[cfg(unix)]
fn configure_lock_options(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt as _;

    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_lock_options(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

fn ensure_safe_directory(path: &Path) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| StoreError::io("inspect controlled directory", source))?;
    if !metadata.is_dir() || is_directory_redirect(&metadata) {
        return Err(StoreError::UnsafeRoot);
    }
    Ok(())
}

fn set_private_directory_permissions(path: &Path) -> Result<(), StoreError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| StoreError::io("inspect controlled directory ownership", source))?;
    if !metadata.is_dir() || is_directory_redirect(&metadata) {
        return Err(StoreError::UnsafeRoot);
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if metadata.uid() != rustix::process::geteuid().as_raw() {
            return Err(StoreError::UnsafeRoot);
        }

        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| StoreError::io("secure controlled directory", source))?;
        let secured = fs::symlink_metadata(path)
            .map_err(|source| StoreError::io("verify controlled directory ownership", source))?;
        if !secured.is_dir()
            || is_directory_redirect(&secured)
            || secured.uid() != rustix::process::geteuid().as_raw()
            || secured.mode() & 0o077 != 0
        {
            return Err(StoreError::UnsafeRoot);
        }
    }
    #[cfg(windows)]
    {
        secure_private_windows_directory(path)?;
    }
    Ok(())
}

#[cfg(unix)]
fn is_directory_redirect(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn is_directory_redirect(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_type().is_symlink()
        || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(windows)]
fn secure_private_windows_directory(path: &Path) -> Result<(), StoreError> {
    use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};
    use windows_permissions::{
        LocalBox, SecurityDescriptor,
        constants::{AccessRights, AceFlags, AceType, SeObjectType, SecurityInformation},
        wrappers::{
            ConvertSecurityDescriptorToStringSecurityDescriptor, GetNamedSecurityInfo,
            SetNamedSecurityInfo,
        },
    };

    let current_user = SecurityIdentifier::get_current_user_sid()
        .map_err(|_| StoreError::UnsafeRoot)?
        .to_string();
    let existing = GetNamedSecurityInfo(
        path,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner,
    )
    .map_err(|source| StoreError::io("inspect controlled directory owner", source))?;
    if existing
        .owner()
        .is_none_or(|owner| owner.to_string() != current_user)
    {
        return Err(StoreError::UnsafeRoot);
    }

    let descriptor: LocalBox<SecurityDescriptor> = format!("D:P(A;OICI;FA;;;{current_user})")
        .parse()
        .map_err(|source| StoreError::io("build private directory ACL", source))?;
    let dacl = descriptor.dacl().ok_or(StoreError::UnsafeRoot)?;
    SetNamedSecurityInfo(
        path,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        Some(dacl),
        None,
    )
    .map_err(|source| StoreError::io("secure controlled directory", source))?;

    let secured = GetNamedSecurityInfo(
        path,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .map_err(|source| StoreError::io("verify controlled directory ACL", source))?;
    if secured
        .owner()
        .is_none_or(|owner| owner.to_string() != current_user)
    {
        return Err(StoreError::UnsafeRoot);
    }
    let secured_dacl = secured.dacl().ok_or(StoreError::UnsafeRoot)?;
    let ace = secured_dacl.get_ace(0).ok_or(StoreError::UnsafeRoot)?;
    let expected_flags = AceFlags::ContainerInherit | AceFlags::ObjectInherit;
    if secured_dacl.len() != 1
        || ace.ace_type() != AceType::ACCESS_ALLOWED_ACE_TYPE
        || ace.flags() != expected_flags
        || ace.mask() != AccessRights::FileAllAccess
        || ace
            .sid()
            .is_none_or(|allowed| allowed.to_string() != current_user)
    {
        return Err(StoreError::UnsafeRoot);
    }
    let dacl_sddl =
        ConvertSecurityDescriptorToStringSecurityDescriptor(&secured, SecurityInformation::Dacl)
            .map_err(|source| StoreError::io("verify private directory ACL protection", source))?;
    if !dacl_sddl.to_string_lossy().starts_with("D:P") {
        return Err(StoreError::UnsafeRoot);
    }
    Ok(())
}

fn make_file_writable(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|source| StoreError::io("make program file removable", source))?;
    }
    #[cfg(windows)]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|source| StoreError::io("inspect removable program file", source))?
            .permissions();
        permissions.set_readonly(false);
        fs::set_permissions(path, permissions)
            .map_err(|source| StoreError::io("make program file removable", source))?;
    }
    Ok(())
}

fn version_directory_name(version: &str, manifest_sha256: &str) -> Result<String, StoreError> {
    let parsed = Version::parse(version).map_err(|_| StoreError::CorruptState)?;
    if version != parsed.to_string() || !is_lower_sha256(manifest_sha256) {
        return Err(StoreError::CorruptState);
    }
    Ok(format!("{version}-{manifest_sha256}"))
}

fn unix_time() -> Result<u64, StoreError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| StoreError::Clock)
}

/// Immutable-store or active-selector failure.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("installation root or reserved entry is unsafe")]
    UnsafeRoot,
    #[error("another installer owns the installation lock")]
    AlreadyLocked,
    #[error("installation state is corrupt or unsupported")]
    CorruptState,
    #[error("the immutable version directory already exists")]
    ImmutableVersionExists,
    #[error("the immutable release cache already exists")]
    ImmutableReleaseExists,
    #[error("no retained previous version is available")]
    RollbackUnavailable,
    #[error("repair replacement became indeterminate and requires recovery")]
    RepairIndeterminate,
    #[error("activation became indeterminate and requires recovery")]
    ActivationIndeterminate,
    #[error("system time precedes the Unix epoch")]
    Clock,
    #[error(transparent)]
    Archive(#[from] ArchiveError),
    #[error("installer store operation failed during {operation}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl StoreError {
    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}
