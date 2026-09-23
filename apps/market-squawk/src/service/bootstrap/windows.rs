//! Local-only message-pipe boundary with exact logon-SID admission.

use interprocess::{
    ConnectWaitMode,
    os::windows::named_pipe::{
        DuplexPipeStream, PipeListener, PipeListenerOptions, PipeMode, pipe_mode::Messages,
    },
    os::windows::security_descriptor::SecurityDescriptor,
};
use recvmsg::{MsgBuf, RecvResult, sync::RecvMsg as _};
use sha2::{Digest as _, Sha256};
use std::{
    fs,
    io::ErrorKind,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};
use widestring::U16CString;
use win_security_identifier::{GetCurrentSid as _, SecurityIdentifier};
use zeroize::{Zeroize as _, Zeroizing};

use crate::service::InstalledServiceError;

type SyncMessagePipe = DuplexPipeStream<Messages>;
type BootstrapPipeListener = PipeListener<Messages, Messages>;

const MAXIMUM_REQUEST_MESSAGE_BYTES: usize =
    super::PREFACE.len() + 4 + super::MAXIMUM_FRAME_BYTES + super::REQUEST_COMMIT.len();
const MAXIMUM_RESPONSE_MESSAGE_BYTES: usize = 4 + super::MAXIMUM_FRAME_BYTES;
const RESPONSE_ACK: &[u8; 8] = b"MSQBRACK";

pub(super) struct Stream {
    pipe: Option<SyncMessagePipe>,
    read: Zeroizing<Vec<u8>>,
    read_offset: usize,
    write: Zeroizing<Vec<u8>>,
    write_limit: usize,
    deadline: Instant,
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let available = self.read.len().saturating_sub(self.read_offset);
        let count = available.min(buffer.remaining());
        if count != 0 {
            let end = self.read_offset + count;
            buffer.put_slice(&self.read[self.read_offset..end]);
            self.read_offset = end;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if buffer.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let Some(new_len) = self
            .write
            .len()
            .checked_add(buffer.len())
            .filter(|length| *length <= self.write_limit)
        else {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::InvalidData,
                "bootstrap message exceeds its bound",
            )));
        };
        let additional = new_len - self.write.len();
        if self.write.try_reserve(additional).is_err() {
            return Poll::Ready(Err(std::io::Error::other(
                "bootstrap message allocation failed",
            )));
        }
        self.write.extend_from_slice(buffer);
        Poll::Ready(Ok(buffer.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let Some(pipe) = self.pipe.as_ref() {
            pipe.assume_flushed();
        }
    }
}

pub(super) struct Listener {
    inner: Arc<BootstrapPipeListener>,
    cancellation: Arc<AtomicBool>,
}

impl Listener {
    pub(super) fn bind(root: &Path) -> Result<Self, InstalledServiceError> {
        fs::create_dir_all(root)?;
        let logon_sid = SecurityIdentifier::get_current_logon_sid()
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
            .ok_or(InstalledServiceError::BootstrapUnavailable)?;
        let sddl = U16CString::from_str(format!("D:P(A;;GA;;;{logon_sid})"))
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let descriptor = SecurityDescriptor::deserialize(&sddl)
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let buffer_bytes = u32::try_from(MAXIMUM_REQUEST_MESSAGE_BYTES)
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?;
        let inner = PipeListenerOptions::new()
            .path(pipe_path(root))
            .mode(PipeMode::Messages)
            .nonblocking(true)
            .input_buffer_size_hint(buffer_bytes)
            .output_buffer_size_hint(buffer_bytes)
            .security_descriptor(Some(descriptor))
            .create_duplex::<Messages>()?;
        Ok(Self {
            inner: Arc::new(inner),
            cancellation: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(super) async fn accept(&self) -> Result<Stream, InstalledServiceError> {
        let listener = Arc::clone(&self.inner);
        let cancellation = Arc::clone(&self.cancellation);
        tokio::task::spawn_blocking(move || accept_request(&listener, &cancellation))
            .await
            .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.cancellation.store(true, Ordering::Release);
    }
}

pub(super) async fn connect(root: &Path) -> Result<Stream, InstalledServiceError> {
    let path = pipe_path(root);
    tokio::task::spawn_blocking(move || {
        let deadline = transaction_deadline()?;
        let pipe = SyncMessagePipe::connect_by_path_with_wait_mode(
            path,
            ConnectWaitMode::Timeout(remaining(deadline)?),
        )?;
        pipe.set_nonblocking(false)?;
        Ok(Stream {
            pipe: Some(pipe),
            read: Zeroizing::new(Vec::new()),
            read_offset: 0,
            write: Zeroizing::new(Vec::with_capacity(128)),
            write_limit: MAXIMUM_REQUEST_MESSAGE_BYTES,
            deadline,
        })
    })
    .await
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
}

pub(super) async fn authenticate_preface(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    let pipe = stream
        .pipe
        .take()
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    let deadline = stream.deadline;
    let (pipe, request) = tokio::task::spawn_blocking(move || {
        let request = receive_message(&pipe, MAXIMUM_REQUEST_MESSAGE_BYTES, deadline);
        match request {
            Ok(request) => Ok((pipe, request)),
            Err(error) => {
                pipe.assume_flushed();
                Err(error)
            }
        }
    })
    .await
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)??;
    stream.pipe = Some(pipe);
    stream.read = request;
    stream.read_offset = 0;

    // The complete request message is admitted by the exact logon-SID DACL. Consume and verify
    // the fixed preface before frame decoding.
    let mut preface = [0_u8; super::PREFACE.len()];
    stream.read_exact(&mut preface).await?;
    if preface == *super::PREFACE {
        Ok(())
    } else {
        Err(InstalledServiceError::BootstrapProtocol)
    }
}

pub(super) async fn finish_request(stream: &mut Stream) -> Result<(), InstalledServiceError> {
    if !stream.read.is_empty() || stream.read_offset != 0 {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let pipe = stream
        .pipe
        .take()
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    let deadline = stream.deadline;
    let mut request = std::mem::take(&mut stream.write);
    let (pipe, response) = tokio::task::spawn_blocking(move || {
        let sent = send_message_before(&pipe, &request, deadline);
        request.zeroize();
        if let Err(error) = sent {
            pipe.assume_flushed();
            return Err(error);
        }
        let response = receive_message(&pipe, MAXIMUM_RESPONSE_MESSAGE_BYTES, deadline)?;
        // Receiving the response proves the server consumed the complete request message.
        pipe.assume_flushed();
        Ok((pipe, response))
    })
    .await
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)??;
    stream.pipe = Some(pipe);
    stream.read = response;
    stream.read_offset = 0;
    Ok(())
}

pub(super) async fn complete_response_read(
    stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    if stream.read_offset != stream.read.len() || !stream.write.is_empty() {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let pipe = stream
        .pipe
        .take()
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    let deadline = stream.deadline;
    tokio::task::spawn_blocking(move || {
        if let Err(error) = send_message_before(&pipe, RESPONSE_ACK, deadline) {
            pipe.assume_flushed();
            return Err(error);
        }
        let closed = wait_for_close(&pipe, deadline);
        pipe.assume_flushed();
        closed
    })
    .await
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
}

pub(super) async fn complete_response_write(
    stream: &mut Stream,
) -> Result<(), InstalledServiceError> {
    if stream.read_offset != stream.read.len() || stream.write.is_empty() {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let pipe = stream
        .pipe
        .take()
        .ok_or(InstalledServiceError::BootstrapUnavailable)?;
    let deadline = stream.deadline;
    let mut response = std::mem::take(&mut stream.write);
    tokio::task::spawn_blocking(move || {
        let sent = send_message_before(&pipe, &response, deadline);
        response.zeroize();
        if let Err(error) = sent {
            pipe.assume_flushed();
            return Err(error);
        }
        let acknowledgement = receive_message(&pipe, RESPONSE_ACK.len(), deadline);
        pipe.assume_flushed();
        if acknowledgement?.as_slice() == RESPONSE_ACK {
            Ok(())
        } else {
            Err(InstalledServiceError::BootstrapProtocol)
        }
    })
    .await
    .map_err(|_error| InstalledServiceError::BootstrapUnavailable)?
}

fn accept_request(
    listener: &BootstrapPipeListener,
    cancellation: &AtomicBool,
) -> Result<Stream, InstalledServiceError> {
    let pipe = loop {
        if cancellation.load(Ordering::Acquire) {
            return Err(InstalledServiceError::BootstrapUnavailable);
        }
        match listener.accept() {
            Ok(pipe) => break pipe,
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    };
    pipe.set_nonblocking(false)?;
    let deadline = transaction_deadline()?;
    Ok(Stream {
        pipe: Some(pipe),
        read: Zeroizing::new(Vec::new()),
        read_offset: 0,
        write: Zeroizing::new(Vec::with_capacity(64)),
        write_limit: MAXIMUM_RESPONSE_MESSAGE_BYTES,
        deadline,
    })
}

fn receive_message(
    pipe: &SyncMessagePipe,
    maximum: usize,
    deadline: Instant,
) -> Result<Zeroizing<Vec<u8>>, InstalledServiceError> {
    let length = wait_for_message(pipe, deadline)?;
    if length == 0 || length > maximum {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    let mut message = Zeroizing::new(vec![0_u8; length]);
    let received_length = {
        let mut buffer = MsgBuf::from(message.as_mut_slice());
        let mut receiver = pipe;
        match receiver.recv_msg(&mut buffer, None) {
            Ok(RecvResult::Fit) => Ok(buffer.len_filled()),
            Ok(RecvResult::Spilled | RecvResult::EndOfStream | RecvResult::QuotaExceeded(_)) => {
                Err(InstalledServiceError::BootstrapProtocol)
            }
            Err(error) => Err(error.into()),
        }
    }?;
    if received_length == length {
        Ok(message)
    } else {
        Err(InstalledServiceError::BootstrapProtocol)
    }
}

fn send_message_before(
    pipe: &SyncMessagePipe,
    message: &[u8],
    deadline: Instant,
) -> Result<(), InstalledServiceError> {
    if message.is_empty() {
        return Err(InstalledServiceError::BootstrapProtocol);
    }
    loop {
        remaining(deadline)?;
        match pipe.send(message) {
            Ok(count) if count == message.len() => return Ok(()),
            Ok(_) => return Err(InstalledServiceError::BootstrapProtocol),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn wait_for_message(
    pipe: &SyncMessagePipe,
    deadline: Instant,
) -> Result<usize, InstalledServiceError> {
    loop {
        remaining(deadline)?;
        match pipe.peek_msg_len() {
            Ok(0) => wait_before_retry(deadline)?,
            Ok(length) => return Ok(length),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn wait_for_close(pipe: &SyncMessagePipe, deadline: Instant) -> Result<(), InstalledServiceError> {
    loop {
        remaining(deadline)?;
        match pipe.peek_msg_len() {
            Ok(0) => wait_before_retry(deadline)?,
            Ok(_) => return Err(InstalledServiceError::BootstrapProtocol),
            Err(error) if error.kind() == ErrorKind::BrokenPipe => return Ok(()),
            Err(error)
                if matches!(error.kind(), ErrorKind::Interrupted | ErrorKind::WouldBlock) =>
            {
                wait_before_retry(deadline)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn transaction_deadline() -> Result<Instant, InstalledServiceError> {
    Instant::now()
        .checked_add(super::CONNECTION_TIMEOUT)
        .ok_or(InstalledServiceError::BootstrapDeadline)
}

fn remaining(deadline: Instant) -> Result<Duration, InstalledServiceError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(InstalledServiceError::BootstrapDeadline)
}

fn wait_before_retry(deadline: Instant) -> Result<(), InstalledServiceError> {
    remaining(deadline)?;
    std::thread::sleep(Duration::from_millis(1));
    Ok(())
}

fn pipe_name(root: &Path) -> String {
    let digest = Sha256::digest(root.as_os_str().as_encoded_bytes());
    let mut name = String::from("market-squawk-bootstrap-");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in &digest[..16] {
        name.push(char::from(HEX[usize::from(byte >> 4)]));
        name.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    name
}

fn pipe_path(root: &Path) -> String {
    format!(r"\\.\pipe\{}", pipe_name(root))
}
