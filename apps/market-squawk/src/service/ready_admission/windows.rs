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
    ConnectWaitMode,
    os::windows::{
        named_pipe::{
            DuplexPipeStream, PipeListener, PipeListenerOptions, PipeMode,
            pipe_mode::{Bytes, Messages},
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

type SyncBytePipe = DuplexPipeStream<Bytes>;
type SyncMessagePipe = DuplexPipeStream<Messages>;
type AdmissionPipeListener = PipeListener<Messages, Messages>;

const MAXIMUM_REQUEST_MESSAGE_BYTES: usize =
    super::PREFACE.len() + 4 + super::MAXIMUM_FRAME_BYTES + super::REQUEST_COMMIT.len();
const MAXIMUM_RESPONSE_MESSAGE_BYTES: usize = 4 + super::MAXIMUM_FRAME_BYTES;
const RESPONSE_ACK: &[u8; 8] = b"MSQARACK";

pub(super) struct Stream {
    pending: Option<SyncMessagePipe>,
    inner: Option<SyncBytePipe>,
    request: Zeroizing<Vec<u8>>,
    request_offset: usize,
    deadline: Instant,
    io_cancellation: CancellationToken,
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let available = self.request.len().saturating_sub(self.request_offset);
        let count = available.min(buffer.remaining());
        if count != 0 {
            let end = self.request_offset + count;
            buffer.put_slice(&self.request[self.request_offset..end]);
            self.request_offset = end;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.io_cancellation.cancel();
        if let Some(pipe) = self.pending.as_ref() {
            pipe.assume_flushed();
        }
        if let Some(pipe) = self.inner.as_ref() {
            pipe.assume_flushed();
        }
    }
}

pub(super) struct BlockingStream {
    pending: Option<SyncMessagePipe>,
    response: Option<SyncBytePipe>,
    request: Zeroizing<Vec<u8>>,
    response_remaining: Option<usize>,
    deadline: Instant,
}

impl Read for BlockingStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        let remaining = self.response_remaining.ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotConnected,
                "ready admission request has not been committed",
            )
        })?;
        if remaining == 0 {
            // The complete response message, rather than pipe closure, is the response boundary.
            return Ok(0);
        }
        let pipe = self.response.as_mut().ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::NotConnected,
                "ready admission response pipe is unavailable",
            )
        })?;
        let limit = remaining.min(buffer.len());
        loop {
            match pipe.read(&mut buffer[..limit]) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "ready admission response ended inside its message",
                    ));
                }
                Ok(count) => {
                    self.response_remaining = Some(remaining - count);
                    return Ok(count);
                }
                Err(error)
                    if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
                {
                    wait_before_retry(self.deadline)?;
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Write for BlockingStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.pending.is_none() {
            return Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "ready admission request was already committed",
            ));
        }
        let new_len = self
            .request
            .len()
            .checked_add(buffer.len())
            .filter(|length| *length <= MAXIMUM_REQUEST_MESSAGE_BYTES)
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::InvalidData,
                    "ready admission request message exceeds its bound",
                )
            })?;
        self.request
            .try_reserve(new_len - self.request.len())
            .map_err(|_error| std::io::Error::other("ready admission request allocation failed"))?;
        self.request.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for BlockingStream {
    fn drop(&mut self) {
        if let Some(pipe) = self.pending.as_ref() {
            pipe.assume_flushed();
        }
        if let Some(pipe) = self.response.as_ref() {
            pipe.assume_flushed();
        }
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
        let buffer_bytes = u32::try_from(MAXIMUM_REQUEST_MESSAGE_BYTES)
            .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
        let inner = PipeListenerOptions::new()
            .path(pipe_path(root, endpoint_key))
            .mode(PipeMode::Messages)
            .nonblocking(true)
            .input_buffer_size_hint(buffer_bytes)
            .output_buffer_size_hint(buffer_bytes)
            .security_descriptor(Some(descriptor))
            .create_duplex::<Messages>()?;
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
    let stream = SyncMessagePipe::connect_by_path_with_wait_mode(
        pipe_path(root, endpoint_key),
        ConnectWaitMode::Timeout(super::remaining(deadline)?),
    )
    .map_err(super::map_admission_io)?;
    stream.set_nonblocking(true)?;
    Ok(BlockingStream {
        pending: Some(stream),
        response: None,
        request: Zeroizing::new(Vec::with_capacity(128)),
        response_remaining: None,
        deadline,
    })
}

pub(super) async fn authenticate_preface(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    let pipe = stream
        .pending
        .take()
        .ok_or(InstalledServiceError::AdmissionUnavailable)?;
    let deadline = stream.deadline;
    let io_cancellation = stream.io_cancellation.clone();
    let (pipe, request) = tokio::task::spawn_blocking(move || {
        receive_message(
            pipe,
            MAXIMUM_REQUEST_MESSAGE_BYTES,
            deadline,
            &io_cancellation,
        )
    })
    .await
    .map_err(|_error| InstalledServiceError::AdmissionUnavailable)??;
    stream.inner = Some(pipe);
    stream.request = request;
    let mut preface = [0_u8; super::PREFACE.len()];
    stream.read_exact(&mut preface).await?;
    if preface == *super::PREFACE {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionProtocol)
    }
}

pub(super) fn finish_request(stream: &mut BlockingStream) -> Result<(), InstalledServiceError> {
    let pipe = stream
        .pending
        .take()
        .ok_or(InstalledServiceError::AdmissionUnavailable)?;
    let mut request = std::mem::take(&mut stream.request);
    let sent = send_message_before(&pipe, &request, stream.deadline, None);
    request.zeroize();
    if let Err(error) = sent {
        pipe.assume_flushed();
        return Err(error);
    }
    let response_length = match wait_for_message(&pipe, stream.deadline, None) {
        Ok(length) if length <= MAXIMUM_RESPONSE_MESSAGE_BYTES => length,
        Ok(_) => {
            pipe.assume_flushed();
            return Err(InstalledServiceError::AdmissionProtocol);
        }
        Err(error) => {
            pipe.assume_flushed();
            return Err(error);
        }
    };
    // A response can exist only after the server consumed the request message, so clearing the
    // dependency's conservative conversion-dirty state cannot discard an outstanding client send.
    let pipe = into_byte_pipe(pipe, false)?;
    stream.response = Some(pipe);
    stream.response_remaining = Some(response_length);
    Ok(())
}

pub(super) async fn require_request_end(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    if stream.request_offset != stream.request.len() {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    stream.request.zeroize();
    stream.request.clear();
    stream.request_offset = 0;
    Ok(())
}

pub(super) fn finish_response(stream: &mut BlockingStream) -> Result<(), InstalledServiceError> {
    if stream.response_remaining != Some(0) {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    let pipe = stream
        .response
        .take()
        .ok_or(InstalledServiceError::AdmissionUnavailable)?;
    // The response is fully consumed before the acknowledgement send begins.
    let pipe = into_message_pipe(pipe)?;
    if let Err(error) = send_message_before(&pipe, RESPONSE_ACK, stream.deadline, None) {
        pipe.assume_flushed();
        return Err(error);
    }
    // Preserve the acknowledgement's dirty state until server closure proves that the peer has
    // consumed it. Drop clears it on every deadline/error path, so no linger work can escape.
    let mut pipe = into_byte_pipe(pipe, true)?;
    let closed = wait_for_close(&mut pipe, stream.deadline);
    pipe.assume_flushed();
    closed
}

pub(super) async fn write_response(
    stream: &mut Stream,
    mut response: Zeroizing<Vec<u8>>,
    deadline: Instant,
) -> Result<(), InstalledServiceError> {
    let deadline = deadline.min(stream.deadline);
    let length = u32::try_from(response.len())
        .map_err(|_error| InstalledServiceError::AdmissionProtocol)?
        .to_be_bytes();
    let mut wire = Zeroizing::new(Vec::with_capacity(length.len() + response.len()));
    wire.extend_from_slice(&length);
    wire.extend_from_slice(&response);
    response.zeroize();
    let pipe = stream
        .inner
        .take()
        .ok_or(InstalledServiceError::AdmissionUnavailable)?;
    let io_cancellation = stream.io_cancellation.clone();
    tokio::task::spawn_blocking(move || exchange_response(pipe, &wire, deadline, &io_cancellation))
        .await
        .map_err(|_join| InstalledServiceError::AdmissionUnavailable)??;
    Ok(())
}

fn exchange_response(
    pipe: SyncBytePipe,
    wire: &[u8],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), InstalledServiceError> {
    // No server send is outstanding when the request is complete.
    let pipe = into_message_pipe(pipe)?;
    if let Err(error) = send_message_before(&pipe, wire, deadline, Some(cancellation)) {
        pipe.assume_flushed();
        return Err(error);
    }
    let acknowledgement_length = match wait_for_message(&pipe, deadline, Some(cancellation)) {
        Ok(length) if length == RESPONSE_ACK.len() => length,
        Ok(_) => {
            pipe.assume_flushed();
            return Err(InstalledServiceError::AdmissionProtocol);
        }
        Err(error) => {
            pipe.assume_flushed();
            return Err(error);
        }
    };
    // The client emits the acknowledgement only after consuming the complete response message.
    let mut pipe = into_byte_pipe(pipe, false)?;
    let mut acknowledgement = [0_u8; RESPONSE_ACK.len()];
    let result = read_exact_before(
        &mut pipe,
        &mut acknowledgement[..acknowledgement_length],
        deadline,
        Some(cancellation),
    );
    pipe.assume_flushed();
    result?;
    if acknowledgement == *RESPONSE_ACK {
        Ok(())
    } else {
        Err(InstalledServiceError::AdmissionProtocol)
    }
}

fn receive_message(
    pipe: SyncMessagePipe,
    maximum: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(SyncBytePipe, Zeroizing<Vec<u8>>), InstalledServiceError> {
    let length = match wait_for_message(&pipe, deadline, Some(cancellation)) {
        Ok(length) if length <= maximum => length,
        Ok(_) => {
            pipe.assume_flushed();
            return Err(InstalledServiceError::AdmissionProtocol);
        }
        Err(error) => {
            pipe.assume_flushed();
            return Err(error);
        }
    };
    let mut pipe = into_byte_pipe(pipe, false)?;
    let mut message = Zeroizing::new(vec![0_u8; length]);
    if let Err(error) = read_exact_before(&mut pipe, &mut message, deadline, Some(cancellation)) {
        pipe.assume_flushed();
        return Err(error);
    }
    Ok((pipe, message))
}

fn send_message_before(
    pipe: &SyncMessagePipe,
    message: &[u8],
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<(), InstalledServiceError> {
    if message.is_empty() {
        return Err(InstalledServiceError::AdmissionProtocol);
    }
    loop {
        require_io_open(deadline, cancellation)?;
        match pipe.send(message) {
            Ok(count) if count == message.len() => return Ok(()),
            Ok(_) => return Err(InstalledServiceError::AdmissionProtocol),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline).map_err(super::map_admission_io)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn wait_for_message(
    pipe: &SyncMessagePipe,
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<usize, InstalledServiceError> {
    loop {
        require_io_open(deadline, cancellation)?;
        match pipe.peek_msg_len() {
            Ok(0) => wait_before_retry(deadline).map_err(super::map_admission_io)?,
            Ok(length) => return Ok(length),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline).map_err(super::map_admission_io)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn read_exact_before(
    pipe: &mut SyncBytePipe,
    mut buffer: &mut [u8],
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<(), InstalledServiceError> {
    while !buffer.is_empty() {
        require_io_open(deadline, cancellation)?;
        match pipe.read(buffer) {
            Ok(0) => return Err(InstalledServiceError::AdmissionProtocol),
            Ok(count) => buffer = &mut buffer[count..],
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline).map_err(super::map_admission_io)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn wait_for_close(pipe: &mut SyncBytePipe, deadline: Instant) -> Result<(), InstalledServiceError> {
    let mut trailing = [0_u8; 1];
    loop {
        require_io_open(deadline, None)?;
        match pipe.read(&mut trailing) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(InstalledServiceError::AdmissionProtocol),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline).map_err(super::map_admission_io)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn require_io_open(
    deadline: Instant,
    cancellation: Option<&CancellationToken>,
) -> Result<(), InstalledServiceError> {
    if cancellation.is_some_and(|cancellation| cancellation.is_cancelled()) {
        return Err(InstalledServiceError::AdmissionUnavailable);
    }
    if Instant::now() >= deadline {
        return Err(InstalledServiceError::AdmissionDeadline);
    }
    Ok(())
}

fn into_byte_pipe(
    pipe: SyncMessagePipe,
    outstanding_send: bool,
) -> Result<SyncBytePipe, InstalledServiceError> {
    let handle: OwnedHandle = pipe.try_into().map_err(|pipe: SyncMessagePipe| {
        pipe.assume_flushed();
        InstalledServiceError::AdmissionUnavailable
    })?;
    let pipe = SyncBytePipe::try_from(handle)
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
    if let Err(error) = pipe.set_nonblocking(true) {
        pipe.assume_flushed();
        return Err(error.into());
    }
    if !outstanding_send {
        pipe.assume_flushed();
    }
    Ok(pipe)
}

fn into_message_pipe(pipe: SyncBytePipe) -> Result<SyncMessagePipe, InstalledServiceError> {
    let handle: OwnedHandle = pipe.try_into().map_err(|pipe: SyncBytePipe| {
        pipe.assume_flushed();
        InstalledServiceError::AdmissionUnavailable
    })?;
    let pipe = SyncMessagePipe::try_from(handle)
        .map_err(|_error| InstalledServiceError::AdmissionUnavailable)?;
    if let Err(error) = pipe.set_nonblocking(true) {
        pipe.assume_flushed();
        return Err(error.into());
    }
    pipe.assume_flushed();
    Ok(pipe)
}

fn wait_before_retry(deadline: Instant) -> std::io::Result<()> {
    if Instant::now() >= deadline {
        return Err(std::io::Error::new(
            ErrorKind::TimedOut,
            "ready admission I/O deadline elapsed",
        ));
    }
    std::thread::sleep(Duration::from_millis(1));
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
    stream.set_nonblocking(true)?;
    let deadline = Instant::now()
        .checked_add(super::CONNECTION_TIMEOUT)
        .ok_or(InstalledServiceError::AdmissionDeadline)?;
    Ok(Stream {
        pending: Some(stream),
        inner: None,
        request: Zeroizing::new(Vec::new()),
        request_offset: 0,
        deadline,
        io_cancellation: CancellationToken::new(),
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
