//! Filesystem local-socket boundary with bilateral effective-UID authentication.

use std::{
    fs,
    os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use interprocess::local_socket::{
    GenericFilePath, ListenerOptions,
    tokio::{Listener as LocalSocketListener, Stream as LocalSocketStream, prelude::*},
};
#[cfg(target_os = "linux")]
use interprocess::os::unix::local_socket::ListenerOptionsExt as _;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

use crate::service::InstalledServiceError;

const SOCKET_DIGEST_BYTES: usize = 24;

pub(super) type Stream = LocalSocketStream;

pub(super) struct Listener {
    inner: LocalSocketListener,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl Listener {
    pub(super) fn bind(root: &Path) -> Result<Self, InstalledServiceError> {
        prepare_private_root(root)?;
        let runtime_root = runtime_root();
        prepare_private_root(&runtime_root)?;
        let path = socket_path(root, &runtime_root);
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
            return Err(InstalledServiceError::BootstrapUnavailable);
        }
        Ok(Self {
            inner,
            path,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    pub(super) async fn accept(&self) -> Result<Stream, InstalledServiceError> {
        let stream = self.inner.accept().await?;
        require_peer(&stream)?;
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

pub(super) async fn connect(root: &Path) -> Result<Stream, InstalledServiceError> {
    validate_private_root(root)?;
    let runtime_root = runtime_root();
    validate_private_root(&runtime_root)?;
    let path = socket_path(root, &runtime_root);
    validate_socket(&path)?;
    let name = path.to_fs_name::<GenericFilePath>()?;
    let stream = LocalSocketStream::connect(name).await?;
    require_peer(&stream)?;
    Ok(stream)
}

pub(super) async fn authenticate_preface(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    let mut preface = [0_u8; super::PREFACE.len()];
    stream.read_exact(&mut preface).await?;
    if preface == *super::PREFACE {
        Ok(())
    } else {
        Err(InstalledServiceError::BootstrapProtocol)
    }
}

pub(super) async fn finish_request(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    // interprocess 2.4.3 makes shutdown on its local-socket enum a no-op. Dispatch directly to
    // Tokio's UnixStream so the service can prove the exact request frame ended at write EOF.
    let LocalSocketStream::UdSocket(inner) = stream;
    inner
        .inner_mut()
        .shutdown()
        .await
        .map_err(|_error| InstalledServiceError::BootstrapUnavailable)
}

pub(super) async fn complete_response_read(
    _stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    Ok(())
}

pub(super) async fn complete_response_write(
    _stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    Ok(())
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
        return Err(InstalledServiceError::BootstrapUnavailable);
    }
    Ok(())
}

fn runtime_root() -> PathBuf {
    Path::new("/tmp").join(format!("market-squawk-{}", effective_uid()))
}

fn socket_path(bootstrap_root: &Path, runtime_root: &Path) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-bootstrap-socket-v1\0");
    hasher.update(bootstrap_root.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    let mut name = String::with_capacity(2 + SOCKET_DIGEST_BYTES * 2);
    name.push_str("b-");
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
        Ok(_) => Err(InstalledServiceError::BootstrapUnavailable),
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
        Err(InstalledServiceError::BootstrapUnavailable)
    }
}

fn require_peer(stream: &LocalSocketStream) -> Result<(), InstalledServiceError> {
    if stream.peer_creds()?.euid() == Some(effective_uid()) {
        Ok(())
    } else {
        Err(InstalledServiceError::BootstrapRejected)
    }
}

fn effective_uid() -> u32 {
    rustix::process::geteuid().as_raw()
}
