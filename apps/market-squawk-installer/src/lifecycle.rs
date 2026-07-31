//! Verified installation, update, repair, rollback, status, and uninstall operations.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use semver::Version;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::archive::{
    ArchiveError, ComponentReceipt, extract_bundle, seal_tree_root, sha256_file, sync_directory,
    verify_bundle, verify_installed_tree,
};
#[cfg(unix)]
use crate::archive::{set_component_permissions, verify_component};
use crate::contracts::{
    InstallReceipt, InstallRequest, InstallStatus, RepairRequest, RollbackRequest,
    UninstallReceipt, UninstallRequest, UpdateRequest,
};
use crate::manifest::{
    AdmittedRelease, MAXIMUM_ARCHIVE_BYTES, MAXIMUM_MANIFEST_BYTES, ManifestError, ReleaseManifest,
};
use crate::platform::ProgramName;
use crate::store::{InstallStore, InstallationState, StoreError, StoredVersion, remove_tree};

const CACHED_MANIFEST_FILE: &str = "manifest.json";
const CACHED_BUNDLE_FILE: &str = "bundle.zip";
const COPY_BUFFER_BYTES: usize = 64 * 1024;
#[cfg(unix)]
const STABLE_PROGRAMS: [ProgramName; 5] = [
    ProgramName::Desktop,
    ProgramName::Cli,
    ProgramName::CaptureHelper,
    ProgramName::OnnxWorker,
    ProgramName::Installer,
];

/// Installs one complete release and creates the first active selector.
///
/// # Errors
///
/// Fails before selector publication if the store, manifest, archive, any component identity, or
/// immutable publication is invalid.
pub fn install(request: InstallRequest) -> Result<InstallReceipt, InstallError> {
    let store = InstallStore::open_or_create(&request.root)?;
    recover_pending_activation(&store)?;
    if store.load_state()?.is_some() {
        return Err(InstallError::AlreadyInstalled);
    }
    let active = prepare_candidate(&store, &request.release, &request.bundle)?;
    let state = InstallationState::initial(active, request.channel_manifest_url)?;
    commit_activation(&store, &state)?;
    receipt(&state, false)
}

/// Stages and atomically activates one strictly newer complete release.
///
/// # Errors
///
/// Returns [`InstallError::NotInstalled`] without an active selector and
/// [`InstallError::UpdateNotNewer`] unless the admitted semantic version is strictly newer.
pub fn update(request: UpdateRequest) -> Result<InstallReceipt, InstallError> {
    let store = InstallStore::open_existing(&request.root)?.ok_or(InstallError::NotInstalled)?;
    recover_pending_activation(&store)?;
    let mut state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
    let current =
        Version::parse(&state.active.version).map_err(|_| InstallError::CorruptInstallation)?;
    let candidate =
        Version::parse(request.release.version()).map_err(|_| InstallError::CorruptInstallation)?;
    if candidate <= current {
        return Err(InstallError::UpdateNotNewer);
    }
    let retain_current_as_previous =
        verify_installed_tree(&store.version_path(&state.active), &state.active.components).is_ok();
    let active = prepare_candidate(&store, &request.release, &request.bundle)?;
    state.activate(
        active,
        request.channel_manifest_url,
        retain_current_as_previous,
    )?;
    commit_activation(&store, &state)?;
    receipt(&state, false)
}

/// Re-verifies and, when necessary, reconstructs the active immutable version from its retained
/// exact release cache.
///
/// # Errors
///
/// Fails if no active installation exists, the retained cache no longer matches the selector, or
/// the reconstructed version cannot be published safely.
pub fn repair(request: RepairRequest) -> Result<InstallReceipt, InstallError> {
    let store = InstallStore::open_existing(&request.root)?.ok_or(InstallError::NotInstalled)?;
    recover_pending_activation(&store)?;
    let state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
    let active_path = store.version_path(&state.active);
    if verify_installed_tree(&active_path, &state.active.components).is_ok() {
        if verify_stable_programs(&store, &state).is_ok() {
            return receipt(&state, false);
        }
        publish_stable_programs(&store, &state)?;
        return receipt(&state, true);
    }

    let release = read_cached_release(&store, &state.active)?;
    let stage = store.create_stage("repair")?;
    let extracted = match extract_bundle(
        &store
            .release_path(&state.active.manifest_sha256)
            .join(CACHED_BUNDLE_FILE),
        release.target_release(),
        &stage,
    ) {
        Ok(extracted) => extracted,
        Err(error) => {
            let _ = remove_tree(&stage);
            return Err(error.into());
        }
    };
    if extracted != state.active.components {
        let _ = remove_tree(&stage);
        return Err(InstallError::CorruptInstallation);
    }
    match fs::symlink_metadata(&active_path) {
        Ok(_) => store.replace_corrupt_version(&stage, &state.active)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            store.publish_new_version(&stage, &state.active)?;
        }
        Err(source) => {
            return Err(InstallError::Io {
                operation: "inspect active version root",
                source,
            });
        }
    }
    seal_tree_root(&active_path)?;
    verify_installed_tree(&active_path, &state.active.components)?;
    publish_stable_programs(&store, &state)?;
    receipt(&state, true)
}

/// Reactivates the retained previous version after complete revalidation.
///
/// # Errors
///
/// Fails if no previous version exists or if any retained component no longer matches its receipt.
pub fn rollback(request: RollbackRequest) -> Result<InstallReceipt, InstallError> {
    let store = InstallStore::open_existing(&request.root)?.ok_or(InstallError::NotInstalled)?;
    recover_pending_activation(&store)?;
    let mut state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
    let previous = state
        .previous
        .as_ref()
        .ok_or(InstallError::RollbackUnavailable)?;
    verify_installed_tree(&store.version_path(previous), &previous.components)?;
    state.swap_for_rollback()?;
    commit_activation(&store, &state)?;
    receipt(&state, false)
}

/// Reads and revalidates current installation status.
///
/// # Errors
///
/// Fails when the root or selector is unsafe or corrupt. Component corruption is reported through
/// [`InstallStatus::is_healthy`] rather than converted into readiness.
pub fn status(root: &Path) -> Result<InstallStatus, InstallError> {
    let Some(store) = InstallStore::open_existing(root)? else {
        return Ok(absent_status());
    };
    recover_pending_activation(&store)?;
    let Some(state) = store.load_state()? else {
        return Ok(absent_status());
    };
    let healthy =
        verify_installed_tree(&store.version_path(&state.active), &state.active.components).is_ok()
            && verify_stable_programs(&store, &state).is_ok();
    Ok(InstallStatus {
        installed: true,
        active_version: Some(state.active.version.clone()),
        previous_version: state
            .previous
            .as_ref()
            .map(|version| version.version.clone()),
        target: Some(state.active.target.as_str().into()),
        manifest_sha256: Some(state.active.manifest_sha256.clone()),
        channel_manifest_url: state.channel_manifest_url.clone(),
        healthy,
    })
}

/// Returns the revalidated active immutable release root.
///
/// # Errors
///
/// Fails when no release is installed, the selector targets another platform, or any active
/// component differs from its retained receipt.
pub fn active_release_root(root: &Path) -> Result<PathBuf, InstallError> {
    let store = InstallStore::open_existing(root)?.ok_or(InstallError::NotInstalled)?;
    recover_pending_activation(&store)?;
    let state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
    if state.active.target != crate::platform::SupportedTarget::current()? {
        return Err(InstallError::CorruptInstallation);
    }
    let active = store.version_path(&state.active);
    verify_installed_tree(&active, &state.active.components)?;
    Ok(active)
}

/// Returns a revalidated stable installed entrypoint for one code-owned program.
///
/// On Unix, the returned path remains constant across update and rollback. Native Windows
/// packages own their operating-system application entrypoints, so this function returns the
/// active immutable program path there.
///
/// # Errors
///
/// Fails when the installation, selected release, or derived stable entrypoints are inconsistent.
pub fn stable_program_path(root: &Path, program: ProgramName) -> Result<PathBuf, InstallError> {
    #[cfg(unix)]
    {
        if !STABLE_PROGRAMS.contains(&program) {
            return active_program_path(root, program);
        }
        let store = InstallStore::open_existing(root)?.ok_or(InstallError::NotInstalled)?;
        recover_pending_activation(&store)?;
        let state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
        if state.active.target != crate::platform::SupportedTarget::current()? {
            return Err(InstallError::CorruptInstallation);
        }
        verify_installed_tree(&store.version_path(&state.active), &state.active.components)?;
        verify_stable_programs(&store, &state)?;
        Ok(store.entrypoint_path(program, state.active.target)?)
    }
    #[cfg(not(unix))]
    {
        active_program_path(root, program)
    }
}

/// Returns one revalidated executable from the selected immutable release.
///
/// Unlike [`stable_program_path`], this path remains bound to the exact selected release when a
/// later activation republishes the stable entrypoints.
///
/// # Errors
///
/// Fails when the installation, selected release, platform, program receipt, or installed tree is
/// missing or inconsistent.
pub fn active_program_path(
    root: &Path,
    program: crate::platform::ProgramName,
) -> Result<PathBuf, InstallError> {
    let store = InstallStore::open_existing(root)?.ok_or(InstallError::NotInstalled)?;
    recover_pending_activation(&store)?;
    let state = store.load_state()?.ok_or(InstallError::NotInstalled)?;
    if state.active.target != crate::platform::SupportedTarget::current()? {
        return Err(InstallError::CorruptInstallation);
    }
    let relative = program.relative_path(state.active.target);
    let portable = relative
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .ok_or(InstallError::CorruptInstallation)
        })
        .collect::<Result<Vec<_>, _>>()?
        .join("/");
    let receipt = state
        .active
        .components
        .iter()
        .find(|receipt| receipt.path.as_ref() == portable)
        .ok_or(InstallError::CorruptInstallation)?;
    if !receipt.executable {
        return Err(InstallError::CorruptInstallation);
    }
    let version_root = store.version_path(&state.active);
    verify_installed_tree(&version_root, &state.active.components)?;
    Ok(version_root.join(relative))
}

#[cfg(unix)]
fn publish_stable_programs(
    store: &InstallStore,
    state: &InstallationState,
) -> Result<(), InstallError> {
    let version_root = store.version_path(&state.active);
    verify_installed_tree(&version_root, &state.active.components)?;
    let stage = store.create_stage("entrypoints")?;
    let result = (|| {
        for program in STABLE_PROGRAMS {
            let receipt = program_receipt(state, program)?;
            let source = version_root.join(program.relative_path(state.active.target));
            let destination = stage.join(
                program
                    .relative_path(state.active.target)
                    .file_name()
                    .ok_or(InstallError::CorruptInstallation)?,
            );
            let copied = fs::copy(&source, &destination).map_err(|source| InstallError::Io {
                operation: "stage stable program entrypoint",
                source,
            })?;
            if copied != receipt.size {
                return Err(InstallError::CorruptInstallation);
            }
            set_component_permissions(&destination, true)?;
            File::open(&destination)
                .and_then(|file| file.sync_all())
                .map_err(|source| InstallError::Io {
                    operation: "synchronize stable program entrypoint",
                    source,
                })?;
            verify_component(&destination, receipt)?;
        }
        sync_directory(&stage)?;
        store.replace_entrypoint_directory(&stage)?;
        verify_stable_programs(store, state)
    })();
    if result.is_err() && stage.exists() {
        let _ = remove_tree(&stage);
    }
    result
}

fn commit_activation(store: &InstallStore, state: &InstallationState) -> Result<(), InstallError> {
    store.write_pending_activation(state)?;
    recover_pending_activation(store)
}

fn recover_pending_activation(store: &InstallStore) -> Result<(), InstallError> {
    let Some(state) = store.load_pending_activation()? else {
        return Ok(());
    };
    verify_installed_tree(&store.version_path(&state.active), &state.active.components)?;
    store.write_state(&state)?;
    publish_stable_programs(store, &state)?;
    store.clear_pending_activation()?;
    let _cleanup = store.prune(&state);
    Ok(())
}

#[cfg(not(unix))]
fn publish_stable_programs(
    _store: &InstallStore,
    _state: &InstallationState,
) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(unix)]
fn verify_stable_programs(
    store: &InstallStore,
    state: &InstallationState,
) -> Result<(), InstallError> {
    let mut observed = fs::read_dir(
        store
            .entrypoint_path(ProgramName::Cli, state.active.target)?
            .parent()
            .ok_or(InstallError::CorruptInstallation)?,
    )
    .map_err(|source| InstallError::Io {
        operation: "read stable program entrypoints",
        source,
    })?
    .map(|entry| {
        entry
            .map_err(|source| InstallError::Io {
                operation: "read stable program entrypoint",
                source,
            })?
            .file_name()
            .into_string()
            .map_err(|_| InstallError::CorruptInstallation)
    })
    .collect::<Result<Vec<_>, _>>()?;
    observed.sort();
    let mut expected = STABLE_PROGRAMS
        .iter()
        .map(|program| {
            program
                .relative_path(state.active.target)
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
                .ok_or(InstallError::CorruptInstallation)
        })
        .collect::<Result<Vec<_>, _>>()?;
    expected.sort();
    if observed != expected {
        return Err(InstallError::CorruptInstallation);
    }
    for program in STABLE_PROGRAMS {
        let receipt = program_receipt(state, program)?;
        let path = store.entrypoint_path(program, state.active.target)?;
        verify_component(&path, receipt)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_stable_programs(
    _store: &InstallStore,
    _state: &InstallationState,
) -> Result<(), InstallError> {
    Ok(())
}

#[cfg(unix)]
fn program_receipt(
    state: &InstallationState,
    program: ProgramName,
) -> Result<&ComponentReceipt, InstallError> {
    let relative = program.relative_path(state.active.target);
    let portable = relative.to_str().ok_or(InstallError::CorruptInstallation)?;
    state
        .active
        .components
        .iter()
        .find(|receipt| receipt.path.as_ref() == portable && receipt.executable)
        .ok_or(InstallError::CorruptInstallation)
}

/// Removes program state and only those mutable-data classes separately confirmed in the request.
///
/// # Errors
///
/// Fails closed for unsafe program or data roots. A default request never opens or deletes a
/// mutable-data path.
pub fn uninstall(request: UninstallRequest) -> Result<UninstallReceipt, InstallError> {
    preflight_deletions(&request.deletions, &request.root)?;
    let store = InstallStore::open_existing(&request.root)?;
    let removed_program = if let Some(store) = store.as_ref() {
        let detached = store.quarantine_for_uninstall()?;
        remove_tree(&detached)?;
        true
    } else {
        false
    };

    let mut deleted = Vec::with_capacity(request.deletions.len());
    for (class, path) in request.deletions {
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                remove_tree(&path)?;
            }
            Ok(_) => return Err(InstallError::UnsafeDataRoot),
            Err(source) => {
                return Err(InstallError::Io {
                    operation: "inspect mutable data root",
                    source,
                });
            }
        }
        deleted.push(class);
    }
    Ok(UninstallReceipt {
        removed_program,
        deleted_data_classes: deleted,
    })
}

fn preflight_deletions(
    deletions: &[(crate::contracts::MutableDataClass, PathBuf)],
    program_root: &Path,
) -> Result<(), InstallError> {
    let mut comparison_roots = Vec::with_capacity(deletions.len());
    for (_, path) in deletions {
        comparison_roots.push(validate_mutable_data_root(path, program_root)?);
        match fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(InstallError::UnsafeDataRoot),
            Err(source) => {
                return Err(InstallError::Io {
                    operation: "inspect mutable data root",
                    source,
                });
            }
        }
    }
    for (index, left) in comparison_roots.iter().enumerate() {
        for right in &comparison_roots[index + 1..] {
            if left == right || left.starts_with(right) || right.starts_with(left) {
                return Err(InstallError::UnsafeDataRoot);
            }
        }
    }
    Ok(())
}

fn prepare_candidate(
    store: &InstallStore,
    release: &AdmittedRelease,
    bundle: &Path,
) -> Result<StoredVersion, InstallError> {
    verify_bundle(bundle, &release.target_release().archive)?;
    let cached_bundle = persist_release_cache(store, release, bundle)?;
    let expected_receipts: Vec<ComponentReceipt> = release
        .target_release()
        .components
        .iter()
        .map(ComponentReceipt::from)
        .collect();
    let version = StoredVersion::new(
        release.version(),
        release.manifest_sha256(),
        &release.target_release().archive.sha256,
        release.target(),
        expected_receipts,
    )?;
    let final_path = store.version_path(&version);
    match fs::symlink_metadata(&final_path) {
        Ok(metadata) if metadata.is_dir() && !is_path_redirect(&metadata) => {
            seal_tree_root(&final_path)?;
            verify_installed_tree(&final_path, &version.components)?;
            return Ok(version);
        }
        Ok(_) => return Err(InstallError::CorruptInstallation),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(InstallError::Io {
                operation: "inspect retained version root",
                source,
            });
        }
    }

    let stage = store.create_stage("version")?;
    let extracted = match extract_bundle(&cached_bundle, release.target_release(), &stage) {
        Ok(extracted) => extracted,
        Err(error) => {
            let _ = remove_tree(&stage);
            return Err(error.into());
        }
    };
    if extracted != version.components {
        let _ = remove_tree(&stage);
        return Err(InstallError::CorruptInstallation);
    }
    store.publish_new_version(&stage, &version)?;
    seal_tree_root(&final_path)?;
    verify_installed_tree(&final_path, &version.components)?;
    Ok(version)
}

fn persist_release_cache(
    store: &InstallStore,
    release: &AdmittedRelease,
    source_bundle: &Path,
) -> Result<PathBuf, InstallError> {
    let final_directory = store.release_path(release.manifest_sha256());
    match fs::symlink_metadata(&final_directory) {
        Ok(metadata) if metadata.is_dir() && !is_path_redirect(&metadata) => {
            seal_cache_directory(&final_directory)?;
            verify_cached_release(&final_directory, release)?;
            return Ok(final_directory.join(CACHED_BUNDLE_FILE));
        }
        Ok(_) => return Err(InstallError::CorruptInstallation),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(InstallError::Io {
                operation: "inspect retained release root",
                source,
            });
        }
    }

    let stage = store.create_stage("release")?;
    let manifest_path = stage.join(CACHED_MANIFEST_FILE);
    let bundle_path = stage.join(CACHED_BUNDLE_FILE);
    if let Err(error) = write_new_file(&manifest_path, release.manifest_bytes()) {
        let _ = remove_tree(&stage);
        return Err(error);
    }
    if let Err(error) = copy_exact_bundle(
        source_bundle,
        &bundle_path,
        release.target_release().archive.size,
        &release.target_release().archive.sha256,
    ) {
        let _ = remove_tree(&stage);
        return Err(error);
    }
    seal_cache_file(&manifest_path)?;
    seal_cache_file(&bundle_path)?;
    sync_directory(&stage)?;
    store.publish_release_cache(&stage, release.manifest_sha256())?;
    seal_cache_directory(&final_directory)?;
    verify_cached_release(&final_directory, release)?;
    Ok(final_directory.join(CACHED_BUNDLE_FILE))
}

fn verify_cached_release(directory: &Path, release: &AdmittedRelease) -> Result<(), InstallError> {
    let metadata = fs::symlink_metadata(directory).map_err(|source| InstallError::Io {
        operation: "inspect retained release cache",
        source,
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(InstallError::CorruptInstallation);
    }
    let manifest = read_bounded_file(
        &directory.join(CACHED_MANIFEST_FILE),
        MAXIMUM_MANIFEST_BYTES,
    )?;
    if manifest.as_slice() != release.manifest_bytes()
        || sha256_file(
            &directory.join(CACHED_MANIFEST_FILE),
            MAXIMUM_MANIFEST_BYTES as u64,
        )? != release.manifest_sha256()
    {
        return Err(InstallError::CorruptInstallation);
    }
    verify_bundle(
        &directory.join(CACHED_BUNDLE_FILE),
        &release.target_release().archive,
    )?;
    Ok(())
}

fn read_cached_release(
    store: &InstallStore,
    version: &StoredVersion,
) -> Result<AdmittedRelease, InstallError> {
    let directory = store.release_path(&version.manifest_sha256);
    let bytes = read_bounded_file(
        &directory.join(CACHED_MANIFEST_FILE),
        MAXIMUM_MANIFEST_BYTES,
    )?;
    if sha256_file(
        &directory.join(CACHED_MANIFEST_FILE),
        MAXIMUM_MANIFEST_BYTES as u64,
    )? != version.manifest_sha256.as_ref()
    {
        return Err(InstallError::CorruptInstallation);
    }
    let release = ReleaseManifest::admit_current(&bytes)?;
    if release.version() != version.version.as_ref()
        || release.target() != version.target
        || release.target_release().archive.sha256.as_ref() != version.archive_sha256.as_ref()
        || release.manifest_sha256() != version.manifest_sha256.as_ref()
    {
        return Err(InstallError::CorruptInstallation);
    }
    verify_cached_release(&directory, &release)?;
    Ok(release)
}

fn copy_exact_bundle(
    source: &Path,
    destination: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), InstallError> {
    let metadata = fs::symlink_metadata(source).map_err(|source| InstallError::Io {
        operation: "inspect source release bundle",
        source,
    })?;
    if !metadata.file_type().is_file() || metadata.len() != expected_size {
        return Err(InstallError::Archive(ArchiveError::ArchiveIdentity));
    }
    let mut input = File::open(source).map_err(|source| InstallError::Io {
        operation: "open source release bundle",
        source,
    })?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|source| InstallError::Io {
            operation: "create retained release bundle",
            source,
        })?;
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let read = input.read(&mut buffer).map_err(|source| InstallError::Io {
            operation: "read source release bundle",
            source,
        })?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(u64::try_from(read).map_err(|_| InstallError::CorruptInstallation)?)
            .ok_or(InstallError::CorruptInstallation)?;
        if total > expected_size || total > MAXIMUM_ARCHIVE_BYTES {
            return Err(InstallError::Archive(ArchiveError::SizeLimit));
        }
        output
            .write_all(&buffer[..read])
            .map_err(|source| InstallError::Io {
                operation: "write retained release bundle",
                source,
            })?;
        digest.update(&buffer[..read]);
    }
    output.sync_all().map_err(|source| InstallError::Io {
        operation: "sync retained release bundle",
        source,
    })?;
    if total != expected_size || format!("{:x}", digest.finalize()) != expected_sha256 {
        return Err(InstallError::Archive(ArchiveError::ArchiveIdentity));
    }
    Ok(())
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), InstallError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| InstallError::Io {
            operation: "create retained release manifest",
            source,
        })?;
    file.write_all(bytes).map_err(|source| InstallError::Io {
        operation: "write retained release manifest",
        source,
    })?;
    file.sync_all().map_err(|source| InstallError::Io {
        operation: "sync retained release manifest",
        source,
    })
}

fn read_bounded_file(path: &Path, maximum: usize) -> Result<Vec<u8>, InstallError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| InstallError::Io {
        operation: "inspect bounded installer file",
        source,
    })?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > maximum as u64
    {
        return Err(InstallError::CorruptInstallation);
    }
    let file = File::open(path).map_err(|source| InstallError::Io {
        operation: "open bounded installer file",
        source,
    })?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(metadata.len()).map_err(|_| InstallError::CorruptInstallation)?,
    );
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| InstallError::Io {
            operation: "read bounded installer file",
            source,
        })?;
    if bytes.len() > maximum {
        return Err(InstallError::CorruptInstallation);
    }
    Ok(bytes)
}

fn seal_cache_file(path: &Path) -> Result<(), InstallError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(0o400)).map_err(|source| {
            InstallError::Io {
                operation: "seal retained release file",
                source,
            }
        })?;
    }
    #[cfg(windows)]
    {
        let mut permissions = fs::metadata(path)
            .map_err(|source| InstallError::Io {
                operation: "inspect retained release file",
                source,
            })?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(path, permissions).map_err(|source| InstallError::Io {
            operation: "seal retained release file",
            source,
        })?;
    }
    Ok(())
}

fn seal_cache_directory(path: &Path) -> Result<(), InstallError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(path, fs::Permissions::from_mode(0o500)).map_err(|source| {
            InstallError::Io {
                operation: "seal retained release directory",
                source,
            }
        })?;
    }
    #[cfg(windows)]
    {
        let _ = path;
    }
    Ok(())
}

fn receipt(state: &InstallationState, repaired: bool) -> Result<InstallReceipt, InstallError> {
    if state.active.target != crate::platform::SupportedTarget::current()? {
        return Err(InstallError::CorruptInstallation);
    }
    Ok(InstallReceipt {
        version: state.active.version.clone(),
        previous_version: state
            .previous
            .as_ref()
            .map(|version| version.version.clone()),
        manifest_sha256: state.active.manifest_sha256.clone(),
        target: state.active.target.as_str().into(),
        repaired,
    })
}

fn absent_status() -> InstallStatus {
    InstallStatus {
        installed: false,
        active_version: None,
        previous_version: None,
        target: None,
        manifest_sha256: None,
        channel_manifest_url: None,
        healthy: false,
    }
}

fn validate_mutable_data_root(path: &Path, program_root: &Path) -> Result<PathBuf, InstallError> {
    if !path.is_absolute()
        || path.parent().is_none()
        || path.components().count() < 3
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(InstallError::UnsafeDataRoot);
    }
    reject_redirecting_components(path)?;
    let data_comparison = canonical_comparison_path(path)?;
    let program_comparison = canonical_comparison_path(program_root)?;
    if data_comparison == program_comparison
        || data_comparison.starts_with(&program_comparison)
        || program_comparison.starts_with(&data_comparison)
    {
        return Err(InstallError::UnsafeDataRoot);
    }
    Ok(data_comparison)
}

fn canonical_comparison_path(path: &Path) -> Result<PathBuf, InstallError> {
    let mut cursor = path;
    let mut missing_components = Vec::new();
    loop {
        match fs::canonicalize(cursor) {
            Ok(mut canonical) => {
                for component in missing_components.iter().rev() {
                    canonical.push(component);
                }
                return Ok(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = cursor
                    .file_name()
                    .ok_or(InstallError::UnsafeDataRoot)?
                    .to_owned();
                missing_components.push(component);
                cursor = cursor.parent().ok_or(InstallError::UnsafeDataRoot)?;
            }
            Err(source) => {
                return Err(InstallError::Io {
                    operation: "resolve deletion root identity",
                    source,
                });
            }
        }
    }
}

fn reject_redirecting_components(path: &Path) -> Result<(), InstallError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(component, std::path::Component::Prefix(_)) || !current.has_root() {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if is_path_redirect(&metadata) => {
                return Err(InstallError::UnsafeDataRoot);
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(source) => {
                return Err(InstallError::Io {
                    operation: "inspect mutable data root ancestor",
                    source,
                });
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn is_path_redirect(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(windows)]
fn is_path_redirect(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

/// Complete installation lifecycle failure.
#[derive(Debug, Error)]
pub enum InstallError {
    /// A release manifest failed closed admission.
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    /// A release archive or installed version failed closed admission.
    #[error(transparent)]
    Archive(#[from] ArchiveError),
    /// The immutable program store or active selector failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// An active installation already exists.
    #[error("Market Squawk is already installed; use update or repair")]
    AlreadyInstalled,
    /// No active installation exists.
    #[error("Market Squawk is not installed")]
    NotInstalled,
    /// The update is not strictly newer than the active semantic version.
    #[error("the candidate release is not newer than the active release")]
    UpdateNotNewer,
    /// No retained previous version is available.
    #[error("no previous known-good version is available")]
    RollbackUnavailable,
    /// The manifest channel URL is not an uncredentialed HTTPS URL.
    #[error("manifest URL must be an uncredentialed HTTPS URL without a fragment")]
    ManifestUrl,
    /// Installed state or retained release evidence is inconsistent.
    #[error("installed program state is corrupt or inconsistent")]
    CorruptInstallation,
    /// An explicitly selected mutable-data root is unsafe.
    #[error("refusing to delete an unsafe or program-overlapping mutable-data root")]
    UnsafeDataRoot,
    /// A lifecycle filesystem operation failed.
    #[error("installer filesystem operation failed during {operation}")]
    Io {
        /// Bounded operation identifier.
        operation: &'static str,
        /// Underlying operating-system failure.
        #[source]
        source: std::io::Error,
    },
    /// Current release platform detection failed.
    #[error(transparent)]
    Platform(#[from] crate::platform::PlatformError),
}
