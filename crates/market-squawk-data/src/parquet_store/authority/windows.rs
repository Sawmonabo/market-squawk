//! Windows exact-target no-replace publication and private control-file policy.

use std::path::PathBuf;

use cap_std::fs::{Dir, OpenOptions};
use market_squawk_platform::ArtifactRoot;

use super::{
    OpenedRootRecord, ParquetStoreError, ROOT_CATALOG_BINDING, ROOT_CATALOG_BINDING_PENDING,
    ROOT_IDENTITY_MARKER, ROOT_IDENTITY_PENDING, RootBindingCheckpointInternal, RootRecord,
    open_committed_record, root_control_exists, validate_root_control_file,
};

#[allow(
    clippy::too_many_arguments,
    reason = "recovery keeps the exact record identity and checkpoint boundary explicit"
)]
pub(super) fn recover_committed_root_record(
    directory: &Dir,
    root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    catalog_committed: bool,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    let committed = reconcile_committed_record(
        directory,
        final_name,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
    )?;
    if catalog_committed {
        return Ok(committed);
    }

    let expected_record = committed.record;
    drop(committed.file);
    let prepared = move_windows_root_record(
        directory,
        root,
        final_name,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
        &expected_record,
    )?;
    publish_prepared_root_record(
        directory,
        root,
        final_name,
        pending_name,
        magic,
        catalog_binding,
        expected_payload,
        prepared,
        durable_checkpoint,
        checkpoint,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "publication keeps the exact record identity and checkpoint boundary explicit"
)]
pub(super) fn publish_prepared_root_record(
    directory: &Dir,
    root: &ArtifactRoot,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    pending: OpenedRootRecord,
    durable_checkpoint: RootBindingCheckpointInternal,
    checkpoint: &mut impl FnMut(RootBindingCheckpointInternal) -> Result<(), ParquetStoreError>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    validate_root_control_file(directory, pending_name, &pending.file, 1)?;
    let expected_record = pending.record;
    drop(pending.file);
    let committed = move_windows_root_record(
        directory,
        root,
        pending_name,
        final_name,
        magic,
        catalog_binding,
        expected_payload,
        &expected_record,
    )?;
    checkpoint(durable_checkpoint)?;
    Ok(committed)
}

fn reconcile_committed_record(
    directory: &Dir,
    final_name: &str,
    pending_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    if root_control_exists(directory, pending_name)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    open_committed_record(
        directory,
        final_name,
        magic,
        catalog_binding,
        expected_payload,
        1,
    )
}

enum WindowsPublicationState {
    Source(OpenedRootRecord),
    Destination(OpenedRootRecord),
}

#[allow(
    clippy::too_many_arguments,
    reason = "the safe ambient wrapper is bounded by both exact record names and identities"
)]
fn move_windows_root_record(
    directory: &Dir,
    root: &ArtifactRoot,
    source_name: &str,
    destination_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    expected_record: &RootRecord,
) -> Result<OpenedRootRecord, ParquetStoreError> {
    let source = open_committed_record(
        directory,
        source_name,
        magic,
        catalog_binding,
        expected_payload,
        1,
    )?;
    if source.record != *expected_record || root_control_exists(directory, destination_name)? {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    drop(source.file);

    let source_path = windows_root_control_path(root, source_name)?;
    let destination_path = windows_root_control_path(root, destination_name)?;
    validate_windows_root_endpoint(directory, root)?;
    // The pinned wrapper requests MoveFileExW WRITE_THROUGH without replacement or cross-volume
    // copying. The retained root remains the authority; ambient paths only bridge this one safe
    // Windows durability primitive and are revalidated immediately around it.
    let publication = atomicwrites::move_atomic(&source_path, &destination_path);
    let endpoint_validation = validate_windows_root_endpoint(directory, root);
    let state = validate_windows_publication_state(
        directory,
        source_name,
        destination_name,
        magic,
        catalog_binding,
        expected_payload,
        expected_record,
    );
    endpoint_validation?;
    let state = state?;
    match (publication, state) {
        (Ok(()), WindowsPublicationState::Destination(committed)) => Ok(committed),
        (Ok(()), WindowsPublicationState::Source(_)) => Err(ParquetStoreError::RootCatalogMismatch),
        (Err(error), WindowsPublicationState::Source(_))
        | (Err(error), WindowsPublicationState::Destination(_)) => Err(error.into()),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "ambiguous recovery validates one exact source or destination record"
)]
fn validate_windows_publication_state(
    directory: &Dir,
    source_name: &str,
    destination_name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
    expected_record: &RootRecord,
) -> Result<WindowsPublicationState, ParquetStoreError> {
    let source = open_optional_committed_record(
        directory,
        source_name,
        magic,
        catalog_binding,
        expected_payload,
    )?;
    let destination = open_optional_committed_record(
        directory,
        destination_name,
        magic,
        catalog_binding,
        expected_payload,
    )?;
    match (source, destination) {
        (Some(source), None) if source.record == *expected_record => {
            Ok(WindowsPublicationState::Source(source))
        }
        (None, Some(destination)) if destination.record == *expected_record => {
            Ok(WindowsPublicationState::Destination(destination))
        }
        _ => Err(ParquetStoreError::RootCatalogMismatch),
    }
}

fn open_optional_committed_record(
    directory: &Dir,
    name: &str,
    magic: &[u8; 8],
    catalog_binding: [u8; 32],
    expected_payload: Option<[u8; 32]>,
) -> Result<Option<OpenedRootRecord>, ParquetStoreError> {
    if !root_control_exists(directory, name)? {
        return Ok(None);
    }
    open_committed_record(directory, name, magic, catalog_binding, expected_payload, 1).map(Some)
}

fn windows_root_control_path(
    root: &ArtifactRoot,
    name: &str,
) -> Result<PathBuf, ParquetStoreError> {
    if ![
        ROOT_IDENTITY_MARKER,
        ROOT_IDENTITY_PENDING,
        ROOT_CATALOG_BINDING,
        ROOT_CATALOG_BINDING_PENDING,
    ]
    .contains(&name)
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(root.root().join(name))
}

fn validate_windows_root_endpoint(
    directory: &Dir,
    root: &ArtifactRoot,
) -> Result<(), ParquetStoreError> {
    use cap_fs_ext::MetadataExt as _;

    let retained = directory.dir_metadata()?;
    let displayed = root
        .try_clone_directory()
        .map_err(crate::parquet_store::map_artifact_root_clone_error)?;
    let displayed = displayed.dir_metadata()?;
    if !retained.is_dir()
        || !displayed.is_dir()
        || (retained.dev(), retained.ino()) != (displayed.dev(), displayed.ino())
    {
        return Err(ParquetStoreError::RootCatalogMismatch);
    }
    Ok(())
}

pub(super) fn configure_private_root_control(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    options
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

pub(super) fn private_root_control_metadata(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT == 0
}
