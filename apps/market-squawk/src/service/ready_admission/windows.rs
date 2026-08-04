//! Local-only named-pipe boundary with exact logon-SID admission and impersonation.

use std::{
    fs,
    io::{ErrorKind, Read, Write},
    os::windows::io::OwnedHandle,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use interprocess::{
    local_socket::{
        GenericNamespaced, Listener as SyncListener, ListenerNonblockingMode, ListenerOptions,
        Stream as SyncStream, prelude::*, tokio::Stream as TokioStream,
    },
    os::windows::{
        local_socket::ListenerOptionsExt as _, named_pipe::local_socket::tokio as tokio_pipe,
        security_descriptor::SecurityDescriptor,
    },
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use widestring::U16CString;
use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};

use crate::service::InstalledServiceError;

pub(super) type Stream = TokioStream;

pub(super) struct BlockingStream {
    inner: SyncStream,
    deadline: Instant,
}

impl Read for BlockingStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.read(buffer) {
                Ok(0) => {
                    if Instant::now() >= self.deadline {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "ready admission read deadline elapsed",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= self.deadline {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "ready admission read deadline elapsed",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }
}

impl Write for BlockingStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.write(buffer) {
                Ok(0) => {
                    if Instant::now() >= self.deadline {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "ready admission write deadline elapsed",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= self.deadline {
                        return Err(std::io::Error::new(
                            ErrorKind::TimedOut,
                            "ready admission write deadline elapsed",
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(super) struct Listener {
    inner: Arc<SyncListener>,
    logon_sid: Arc<SecurityIdentifier>,
}

impl Listener {
    pub(super) fn bind(
        root: &Path,
        endpoint_key: &[u8; 32],
    ) -> Result<Self, InstalledServiceError> {
        fs::create_dir_all(root)?;
        let logon_sid = SecurityIdentifier::get_current_logon_sid()
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?
            .ok_or(InstalledServiceError::AdmissionUnavailable)?;
        let sddl = U16CString::from_str(format!("D:P(A;;GA;;;{logon_sid})"))
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
        let descriptor = SecurityDescriptor::deserialize(&sddl)
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
        let name_text = pipe_name(root, endpoint_key);
        let name = name_text.to_ns_name::<GenericNamespaced>()?;
        let inner = ListenerOptions::new()
            .name(name)
            .reclaim_name(false)
            .try_overwrite(false)
            .nonblocking(ListenerNonblockingMode::Accept)
            .security_descriptor(descriptor)
            .create_sync()?;
        Ok(Self {
            inner: Arc::new(inner),
            logon_sid: Arc::new(logon_sid),
        })
    }

    pub(super) async fn accept(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Stream, InstalledServiceError> {
        let listener = Arc::clone(&self.inner);
        let logon_sid = Arc::clone(&self.logon_sid);
        tokio::task::spawn_blocking(move || {
            accept_authenticated(&listener, &logon_sid, &cancellation)
        })
        .await
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?
    }
}

pub(super) fn connect_blocking(
    root: &Path,
    endpoint_key: &[u8; 32],
    timeout: Duration,
) -> Result<BlockingStream, InstalledServiceError> {
    let name_text = pipe_name(root, endpoint_key);
    let name = name_text.to_ns_name::<GenericNamespaced>()?;
    let stream = SyncStream::connect(name)?;
    stream.set_nonblocking(true)?;
    Ok(BlockingStream {
        inner: stream,
        deadline: Instant::now()
            .checked_add(timeout)
            .ok_or(InstalledServiceError::AdmissionDeadline)?,
    })
}

pub(super) async fn authenticate_preface(
    _stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    Ok(())
}

fn accept_authenticated(
    listener: &SyncListener,
    logon_sid: &SecurityIdentifier,
    cancellation: &CancellationToken,
) -> Result<Stream, InstalledServiceError> {
    let mut stream = loop {
        if cancellation.is_cancelled() {
            return Err(InstalledServiceError::AdmissionUnavailable);
        }
        match listener.accept() {
            Ok(stream) => break stream,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    };
    stream.set_nonblocking(true)?;
    let SyncStream::NamedPipe(ref mut pipe) = stream;
    let mut preface = [0_u8; super::PREFACE.len()];
    let mut offset = 0;
    let deadline = Instant::now()
        .checked_add(super::CONNECTION_TIMEOUT)
        .ok_or(InstalledServiceError::AdmissionDeadline)?;
    while offset < preface.len() {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(InstalledServiceError::AdmissionDeadline);
        }
        match pipe.read(&mut preface[offset..]) {
            Ok(0) => std::thread::sleep(Duration::from_millis(1)),
            Ok(bytes) => offset += bytes,
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => return Err(error.into()),
        }
    }
    if preface != *super::PREFACE {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    {
        let _impersonation = pipe.inner().impersonate_client()?;
        if !SecurityIdentifier::is_current_user_member_of(logon_sid.as_sid())
            .map_err(|_error| InstalledServiceError::AdmissionRejected)?
        {
            return Err(InstalledServiceError::AdmissionRejected);
        }
    }
    let SyncStream::NamedPipe(pipe) = stream;
    let handle = OwnedHandle::from(pipe);
    let pipe = tokio_pipe::Stream::try_from(handle)
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
    Ok(TokioStream::NamedPipe(pipe))
}

fn pipe_name(root: &Path, endpoint_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk-ready-admission-pipe-v1\0");
    hasher.update(root.as_os_str().as_encoded_bytes());
    hasher.update(endpoint_key);
    let digest = hasher.finalize();
    let mut name = String::from("market-squawk-admission-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &digest[..16] {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    name
}
