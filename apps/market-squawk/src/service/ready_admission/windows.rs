//! Local-only named-pipe boundary with exact logon-SID DACL admission.

use std::{
    fs,
    io::{ErrorKind, Read, Write},
    os::windows::io::OwnedHandle,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use interprocess::{
    ConnectWaitMode, TryClone as _,
    os::windows::{
        named_pipe::{
            DuplexPipeStream as SyncPipeStream, PipeListener, PipeListenerOptions,
            pipe_mode::Bytes, tokio::DuplexPipeStream as TokioPipeStream,
        },
        security_descriptor::SecurityDescriptor,
    },
};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt as _, ReadBuf};
use tokio_util::sync::CancellationToken;
use widestring::U16CString;
use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};
use zeroize::{Zeroize as _, Zeroizing};

use crate::service::InstalledServiceError;

type SyncPipe = SyncPipeStream<Bytes>;
type TokioPipe = TokioPipeStream<Bytes>;
type AdmissionPipeListener = PipeListener<Bytes, Bytes>;

pub(super) struct Stream {
    inner: TokioPipe,
    response: Option<SyncPipe>,
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(context, buffer)
    }
}

pub(super) struct BlockingStream {
    inner: SyncPipe,
    deadline: Instant,
}

impl Read for BlockingStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner.read(buffer) {
                Ok(0) => return Ok(0),
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
    inner: Arc<AdmissionPipeListener>,
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
        let buffer_bytes = u32::try_from(super::MAXIMUM_FRAME_BYTES + 4)
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
        let inner = PipeListenerOptions::new()
            .path(pipe_path(root, endpoint_key))
            .nonblocking(true)
            .input_buffer_size_hint(buffer_bytes)
            .output_buffer_size_hint(buffer_bytes)
            .security_descriptor(Some(descriptor))
            .create_duplex::<Bytes>()?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub(super) async fn accept(
        &self,
        cancellation: CancellationToken,
    ) -> Result<Stream, InstalledServiceError> {
        let listener = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || accept_authenticated(&listener, &cancellation))
            .await
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?
    }
}

pub(super) fn connect_blocking(
    root: &Path,
    endpoint_key: &[u8; 32],
    deadline: Instant,
) -> Result<BlockingStream, InstalledServiceError> {
    let stream = SyncPipe::connect_by_path_with_wait_mode(
        pipe_path(root, endpoint_key),
        ConnectWaitMode::Timeout(super::remaining(deadline)?),
    )
    .map_err(super::map_admission_io)?;
    stream.set_nonblocking(true)?;
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

pub(super) fn finish_request(_stream: &mut BlockingStream) -> Result<(), InstalledServiceError> {
    // Windows byte-mode named pipes do not expose a safe half-close. REQUEST_COMMIT is the fixed
    // transaction terminator; the server never admits a second request on the connection.
    Ok(())
}

pub(super) async fn require_request_end(_stream: &mut Stream) -> Result<(), InstalledServiceError> {
    Ok(())
}

pub(super) async fn write_response(
    stream: &mut Stream,
    mut response: Zeroizing<Vec<u8>>,
    deadline: Instant,
) -> Result<(), InstalledServiceError> {
    let length = u32::try_from(response.len())
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
        .to_be_bytes();
    let mut wire = Zeroizing::new(Vec::with_capacity(length.len() + response.len()));
    wire.extend_from_slice(&length);
    wire.extend_from_slice(&response);
    response.zeroize();
    let mut pipe = stream
        .response
        .take()
        .ok_or(InstalledServiceError::AdmissionUnavailable)?;
    // DuplicateHandle aliases the same pipe instance state, so do not assume the response clone
    // has an independent wait mode. PIPE_NOWAIT is selected only after the complete request has
    // been consumed; no Tokio read is attempted after this transition.
    pipe.set_nonblocking(true)?;
    tokio::task::spawn_blocking(move || write_all_before(&mut pipe, &wire, deadline))
        .await
        .map_err(|_join| InstalledServiceError::AdmissionUnavailable)??;
    Ok(())
}

fn write_all_before(
    pipe: &mut SyncPipe,
    wire: &[u8],
    deadline: Instant,
) -> Result<(), InstalledServiceError> {
    let mut written = 0_usize;
    while written < wire.len() {
        match pipe.write(&wire[written..]) {
            Ok(0) => {
                if Instant::now() >= deadline {
                    return Err(InstalledServiceError::AdmissionDeadline);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(count) => {
                written = written
                    .checked_add(count)
                    .ok_or(InstalledServiceError::AdmissionProtocol)?;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(InstalledServiceError::AdmissionDeadline);
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn accept_authenticated(
    listener: &AdmissionPipeListener,
    cancellation: &CancellationToken,
) -> Result<Stream, InstalledServiceError> {
    let stream = loop {
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
    // The listener itself remains nonblocking so cancellation can be observed, but the accepted
    // pipe handed to Tokio must use PIPE_WAIT. Tokio supplies overlapped readiness; PIPE_NOWAIT
    // would turn a transient empty read into a false EOF.
    stream.set_nonblocking(false)?;
    let response = stream.try_clone()?;
    let handle: OwnedHandle = stream
        .try_into()
        .map_err(|_stream| InstalledServiceError::AdmissionUnavailable)?;
    let inner = TokioPipe::try_from(handle)
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
    Ok(Stream {
        inner,
        response: Some(response),
    })
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

fn pipe_path(root: &Path, endpoint_key: &[u8; 32]) -> String {
    format!(r"\\.\pipe\{}", pipe_name(root, endpoint_key))
}
