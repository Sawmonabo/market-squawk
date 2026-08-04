//! Filesystem local-socket boundary with bilateral effective-UID authentication.

use std::{
    fs,
    os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    time::Duration,
};

use interprocess::local_socket::{
    GenericFilePath, ListenerOptions, Stream as SyncLocalSocketStream,
    prelude::*,
    tokio::{Listener as LocalSocketListener, Stream as LocalSocketStream, prelude::*},
};
#[cfg(target_os = "linux")]
use interprocess::os::unix::local_socket::ListenerOptionsExt as _;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;
use tokio_util::sync::CancellationToken;

use crate::service::InstalledServiceError;

const SOCKET_DIGEST_BYTES: usize = 24;

pub(super) type Stream = LocalSocketStream;
pub(super) type BlockingStream = SyncLocalSocketStream;

pub(super) struct Listener {
    inner: LocalSocketListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Listener {
    pub(super) fn bind(
        root: &Path,
        endpoint_key: &[u8; 32],
    ) -> Result<Self, InstalledServiceError> {
        prepare_private_root(root)?;
        let runtime_root = runtime_root();
        prepare_private_root(&runtime_root)?;
        let path = socket_path(root, &runtime_root, endpoint_key);
        remove_stale_socket(&path)?;
        let name = path.as_os_str().to_fs_name::<GenericFilePath>()?;
        let options = ListenerOptions::new()
            .name(name)
            .reclaim_name(false)
            .try_overwrite(false);
        #[cfg(target_os = "linux")]
        let options = options.mode(0o600);
        let inner = options.create_tokio()?;
        #[cfg(target_os = "macos")]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != effective_uid()
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(InstalledServiceError::AdmissionUnavailable);
        }
        Ok(Self {
            inner,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(super) async fn accept(
        &self,
        _cancellation: CancellationToken,
    ) -> Result<Stream, InstalledServiceError> {
        let stream = self.inner.accept().await?;
        require_async_peer(&stream)?;
        Ok(stream)
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
        }) {
            let _removed = fs::remove_file(&self.path);
        }
    }
}

pub(super) fn connect_blocking(
    root: &Path,
    endpoint_key: &[u8; 32],
    timeout: Duration,
) -> Result<BlockingStream, InstalledServiceError> {
    validate_private_root(root)?;
    let runtime_root = runtime_root();
    validate_private_root(&runtime_root)?;
    let path = socket_path(root, &runtime_root, endpoint_key);
    validate_socket(&path)?;
    let name = path.to_fs_name::<GenericFilePath>()?;
    let stream = SyncLocalSocketStream::connect(name)?;
    stream.set_recv_timeout(Some(timeout))?;
    stream.set_send_timeout(Some(timeout))?;
    require_sync_peer(&stream)?;
    Ok(stream)
}

pub(super) async fn authenticate_preface(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    let mut preface = [0_u8; super::PREFACE.len()];
    stream.read_exact(&mut preface).await?;
    if preface == *super::PREFACE {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionProtocol)
    }
}

fn prepare_private_root(root: &Path) -> Result<(), InstalledServiceError> {
    match fs::create_dir(root) {
        Ok(()) => fs::set_permissions(root, fs::Permissions::from_mode(0o700))?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    validate_private_root(root)
}

fn validate_private_root(root: &Path) -> Result<(), InstalledServiceError> {
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err(InstalledServiceError::AdmissionUnavailable);
    }
    Ok(())
}

fn runtime_root() -> PathBuf {
    Path::new("/tmp").join(format!("market-squawk-{}", effective_uid()))
}

fn socket_path(root: &Path, runtime_root: &Path, endpoint_key: &[u8; 32]) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-ready-admission-socket-v1\0");
    hasher.update(root.as_os_str().as_encoded_bytes());
    hasher.update(endpoint_key);
    let digest = hasher.finalize();
    let mut name = String::with_capacity(2 + SOCKET_DIGEST_BYTES * 2);
    name.push_str("a-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &digest[..SOCKET_DIGEST_BYTES] {
        name.push(char::from(HEX[usize::from(*byte >> 4)]));
        name.push(char::from(HEX[usize::from(*byte & 0x0f)]));
    }
    runtime_root.join(name)
}

fn remove_stale_socket(path: &Path) -> Result<(), InstalledServiceError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.uid() == effective_uid()
                && metadata.permissions().mode() & 0o777 == 0o600 =>
        {
            fs::remove_file(path)?;
            Ok(())
        }
        Ok(_) => Err(InstalledServiceError::AdmissionUnavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_socket(path: &Path) -> Result<(), InstalledServiceError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_socket()
        && metadata.uid() == effective_uid()
        && metadata.permissions().mode() & 0o777 == 0o600
    {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionUnavailable)
    }
}

fn require_async_peer(stream: &LocalSocketStream) -> Result<(), InstalledServiceError> {
    if stream.peer_creds()?.euid() == Some(effective_uid()) {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionRejected)
    }
}

fn require_sync_peer(stream: &SyncLocalSocketStream) -> Result<(), InstalledServiceError> {
    if stream.peer_creds()?.euid() == Some(effective_uid()) {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionRejected)
    }
}

fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
