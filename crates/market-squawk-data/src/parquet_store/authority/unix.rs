//! Unix exact-file publication and private control-file policy.

#[cfg(test)]
use std::fs::File;

#[cfg(test)]
use cap_std::fs::Dir;
use cap_std::fs::OpenOptions;
#[cfg(test)]
use market_squawk_platform::ArtifactRoot;

#[cfg(test)]
use super::{
    OpenedRootRecord, ParquetStoreError, RootBindingCheckpointInternal, open_committed_record,
    open_root_control_file, read_root_record, root_control_exists, validate_root_control_file,
    verify_root_record,
};
#[cfg(test)]
use crate::parquet_store::sync_directory;

#[allow(
    clippy::too_many_arguments,
    reason = "recovery keeps the exact record identity and checkpoint boundary explicit"
)]
#[cfg(test)]
pub(super) fn recover_committed_root_record(
    directory: &Dir,
    _root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    _catalog_committed: bool,
    _durable_checkpoint: RootBindingCheckpointInternal,
    _checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    reconcile_committed_record(
        directory,
        final_name,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "publication keeps the exact record identity and checkpoint boundary explicit"
)]
#[cfg(test)]
pub(super) fn publish_prepared_root_record(
    directory: &Dir,
    _root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    pending: OpenedRootRecord,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    directory.hard_link(pending_name, directory, final_name)?;
    sync_directory(directory, ".")?;
    validate_root_control_file(directory, pending_name, &pending.file, 2)?;
    let mut committed = open_root_control_file(directory, final_name, 2)?
        .ok_or(ParquetStoreError::RootCatalogMismatch)?;
    if !same_opened_file(&pending.file, &committed)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    let committed_record =
        read_root_record(&mut committed, magic)?.ok_or(ParquetStoreError::RootCatalogMismatch)?;
    validate_root_control_file(directory, final_name, &committed, 2)?;
    if committed_record != pending.record {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    verify_root_record(&committed_record, catalog_binding, expected_payload)?;
    checkpoint(durable_checkpoint)?;

    drop(pending.file);
    drop(committed);
    directory.remove_file(pending_name)?;
    sync_directory(directory, ".")?;
    open_committed_record(
        directory,
        final_name,
        magic,
        catalog_binding,
        expected_payload,
        1,
    )
}

#[cfg(test)]
fn reconcile_committed_record(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    if !root_control_exists(directory, pending_name)? {
        return open_committed_record(
            directory,
            final_name,
            magic,
            catalog_binding,
            expected_payload,
            1,
        );
    }
    let pending = open_committed_record(
        directory,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
        2,
    )?;
    let committed = open_committed_record(
        directory,
        final_name,
        magic,
        catalog_binding,
        expected_payload,
        2,
    )?;
    if pending.record != committed.record || !same_opened_file(&pending.file, &committed.file)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    drop(pending.file);
    drop(committed.file);
    directory.remove_file(pending_name)?;
    sync_directory(directory, ".")?;
    open_committed_record(
        directory,
        final_name,
        magic,
        catalog_binding,
        expected_payload,
        1,
    )
}

#[cfg(test)]
fn same_opened_file(first: &File, second: &File) -> Result<bool, ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let first = first.metadata()?;
    let second = second.metadata()?;
    Ok((first.dev(), first.ino()) == (second.dev(), second.ino()))
}

pub(super) fn configure_private_root_control(options: &mut OpenOptions) {
    use cap_fs_ext::OpenOptionsSyncExt as _;
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600).nonblock(true);
}

pub(super) fn private_root_control_metadata(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::PermissionsExt as _;

    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use std::os::unix::net::UnixListener;
    use std::process::Command;

    use cap_std::ambient_authority;
    use rustix::fs::OFlags;

    use super::super::{
        ParquetStoreError, ROOT_AUTHORITY_LOCK, open_or_create_root_authority_lock,
    };

    #[test]
    fn root_authority_lock_open_is_nonblocking_and_rejects_unsafe_named_types()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let directory = cap_std::fs::Dir::open_ambient_dir(temporary.path(), ambient_authority())?;

        let lock = open_or_create_root_authority_lock(&directory)?;
        assert!(rustix::fs::fcntl_getfl(&lock)?.contains(OFlags::NONBLOCK));
        drop(lock);
        directory.remove_file(ROOT_AUTHORITY_LOCK)?;

        let socket_path = temporary.path().join(ROOT_AUTHORITY_LOCK);
        let listener = UnixListener::bind(&socket_path)?;
        fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            open_or_create_root_authority_lock(&directory),
            Err(ParquetStoreError::RootCatalogMismatch)
        ));
        drop(listener);
        directory.remove_file(ROOT_AUTHORITY_LOCK)?;

        let fifo_path = temporary.path().join(ROOT_AUTHORITY_LOCK);
        if !Command::new("mkfifo").arg(&fifo_path).status()?.success() {
            return Err("mkfifo failed".into());
        }
        fs::set_permissions(&fifo_path, fs::Permissions::from_mode(0o600))?;
        assert!(matches!(
            open_or_create_root_authority_lock(&directory),
            Err(ParquetStoreError::RootCatalogMismatch)
        ));
        Ok(())
    }
}
