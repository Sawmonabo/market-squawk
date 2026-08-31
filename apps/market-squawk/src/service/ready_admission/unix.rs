//! Filesystem local-socket boundary with bilateral effective-UID authentication.

use std::{
    fs,
    io::{self, Read, Write},
    os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use interprocess::ConnectWaitMode;
use interprocess::local_socket::{
    ConnectOptions, GenericFilePath, ListenerOptions, Stream as SyncLocalSocketStream,
    prelude::*,
    tokio::{Listener as LocalSocketListener, Stream as LocalSocketStream, prelude::*},
};
#[cfg(target_os = "linux")]
use interprocess::os::unix::local_socket::ListenerOptionsExt as _;
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    io::Errno,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio_util::sync::CancellationToken;
use zeroize::{Zeroize as _, Zeroizing};

use crate::service::InstalledServiceError;

const SOCKET_DIGEST_BYTES: usize = 24;

pub(super) type Stream = LocalSocketStream;
pub(super) struct BlockingStream {
    inner: SyncLocalSocketStream,
    deadline: Instant,
}

impl Read for BlockingStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            wait_until_ready(&self.inner, PollFlags::IN, self.deadline)?;
            match self.inner.read(buffer) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                result => return result,
            }
        }
    }
}

impl Write for BlockingStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            wait_until_ready(&self.inner, PollFlags::OUT, self.deadline)?;
            match self.inner.write(buffer) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                    ) => {}
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

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
        cleanup_stale_sockets(&runtime_root)?;
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
        cancellation: CancellationToken,
    ) -> Result<Stream, InstalledServiceError> {
        loop {
            let stream = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(InstalledServiceError::AdmissionUnavailable);
                }
                accepted = self.inner.accept() => accepted?,
            };
            match require_async_peer(&stream) {
                Ok(()) => return Ok(stream),
                Err(InstalledServiceError::AdmissionRejected) => drop(stream),
                Err(error) => return Err(error),
            }
        }
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
    deadline: Instant,
) -> Result<BlockingStream, InstalledServiceError> {
    validate_private_root(root)?;
    let runtime_root = runtime_root();
    validate_private_root(&runtime_root)?;
    let path = socket_path(root, &runtime_root, endpoint_key);
    validate_socket(&path)?;
    let name = path.to_fs_name::<GenericFilePath>()?;
    let stream = ConnectOptions::new()
        .name(name)
        .wait_mode(ConnectWaitMode::Timeout(super::remaining(deadline)?))
        .nonblocking_stream(true)
        .connect_sync()
        .map_err(super::map_admission_io)?;
    require_sync_peer(&stream)?;
    Ok(BlockingStream {
        inner: stream,
        deadline,
    })
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

pub(super) fn finish_request(stream: &mut BlockingStream) -> Result<(), InstalledServiceError> {
    let SyncLocalSocketStream::UdSocket(inner) = &stream.inner;
    inner.inner().shutdown(std::net::Shutdown::Write)?;
    Ok(())
}

pub(super) async fn require_request_end(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    let mut trailing = [0_u8; 1];
    if stream.read(&mut trailing).await? == 0 {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionProtocol)
    }
}

pub(super) async fn write_response(
    stream: &mut Stream,
    mut response: Zeroizing<Vec<u8>>,
    deadline: Instant,
) -> Result<(), InstalledServiceError> {
    let length = u32::try_from(response.len())
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
        .to_be_bytes();
    tokio::time::timeout(super::remaining(deadline)?, async {
        stream.write_all(&length).await?;
        stream.write_all(&response).await?;
        // interprocess 2.4.3 makes shutdown on its local-socket enum a no-op. Dispatch directly
        // to Tokio's UnixStream so the client can prove the exact response frame ended at EOF.
        let LocalSocketStream::UdSocket(inner) = stream;
        inner.inner_mut().shutdown().await
    })
    .await
    .map_err(|_elapsed| InstalledServiceError::AdmissionDeadline)??;
    response.zeroize();
    Ok(())
}

fn prepare_private_root(root: &Path) -> Result<(), InstalledServiceError> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(root) {
        Ok(()) => fs::set_permissions(root, fs::Permissions::from_mode(0o700))?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    validate_private_root(root)
}

fn cleanup_stale_sockets(runtime_root: &Path) -> Result<(), InstalledServiceError> {
    const MAXIMUM_CANDIDATES: usize = 256;
    const PROBE_TIMEOUT: Duration = Duration::from_millis(20);

    let mut candidates = 0_usize;
    for entry in fs::read_dir(runtime_root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !is_admission_socket_name(name) {
            continue;
        }
        candidates = candidates
            .checked_add(1)
            .ok_or(InstalledServiceError::AdmissionUnavailable)?;
        if candidates > MAXIMUM_CANDIDATES {
            return Err(InstalledServiceError::AdmissionUnavailable);
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket()
            || metadata.uid() != effective_uid()
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            continue;
        }
        let name = path.as_os_str().to_fs_name::<GenericFilePath>()?;
        let connect = ConnectOptions::new()
            .name(name)
            .wait_mode(ConnectWaitMode::Timeout(PROBE_TIMEOUT))
            .connect_sync();
        match connect {
            Ok(stream) => drop(stream),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                let current = fs::symlink_metadata(&path)?;
                if current.file_type().is_socket()
                    && current.uid() == effective_uid()
                    && current.dev() == metadata.dev()
                    && current.ino() == metadata.ino()
                {
                    fs::remove_file(path)?;
                }
            }
            Err(_) => {}
        }
    }
    Ok(())
}

fn is_admission_socket_name(name: &str) -> bool {
    name.len() == 2 + SOCKET_DIGEST_BYTES * 2
        && name.starts_with("a-")
        && name[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn remaining_io(deadline: Instant) -> std::io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(admission_timeout)
}

fn wait_until_ready(
    stream: &SyncLocalSocketStream,
    interest: PollFlags,
    deadline: Instant,
) -> io::Result<()> {
    loop {
        let timeout = Timespec::try_from(remaining_io(deadline)?).map_err(|_error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "ready admission transaction deadline exceeds the platform poll range",
            )
        })?;
        let SyncLocalSocketStream::UdSocket(inner) = stream;
        let mut descriptors = [PollFd::new(inner.inner(), interest)];
        match poll(&mut descriptors, Some(&timeout)) {
            Ok(0) => return Err(admission_timeout()),
            Ok(_) => {
                let ready = descriptors[0].revents();
                if ready.contains(PollFlags::NVAL) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "ready admission socket descriptor is invalid",
                    ));
                }
                if ready.intersects(interest | PollFlags::HUP | PollFlags::ERR) {
                    return Ok(());
                }
            }
            Err(Errno::INTR) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn admission_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "ready admission transaction deadline elapsed",
    )
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
