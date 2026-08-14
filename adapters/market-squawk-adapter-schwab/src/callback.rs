//! Code-owned, one-shot HTTPS loopback OAuth callback receiver.
//!
//! This module owns the fixed listener, bounded HTTP request grammar, callback route, and shutdown
//! lifecycle. TLS private-key/certificate custody remains an injected capability so key material
//! never enters adapter configuration or logs. There is deliberately no plaintext acceptor.

use std::fmt;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

use crate::{CallbackOutcome, OAuthCallback, RequestAdmission, SchwabAdapterError};

const CALLBACK_ADDRESS: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8182));
const AUTHORIZED_BODY: &[u8] = b"Authorization received. Return to Market Squawk.";
const DENIED_BODY: &[u8] = b"Authorization was not completed. Return to Market Squawk.";

/// Finite local listener and request bounds. None is a provider capacity claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OAuthLoopbackBounds {
    accept_timeout: Duration,
    tls_handshake_timeout: Duration,
    io_timeout: Duration,
    max_request_bytes: NonZeroUsize,
    max_header_count: NonZeroUsize,
}

impl OAuthLoopbackBounds {
    /// Constructs explicit callback resource controls.
    pub fn try_new(
        accept_timeout: Duration,
        tls_handshake_timeout: Duration,
        io_timeout: Duration,
        max_request_bytes: NonZeroUsize,
        max_header_count: NonZeroUsize,
    ) -> Result<Self, OAuthLoopbackError> {
        if accept_timeout.is_zero()
            || tls_handshake_timeout.is_zero()
            || io_timeout.is_zero()
            || max_request_bytes.get() < 128
        {
            return Err(OAuthLoopbackError::InvalidConfiguration);
        }
        Ok(Self {
            accept_timeout,
            tls_handshake_timeout,
            io_timeout,
            max_request_bytes,
            max_header_count,
        })
    }

    pub const fn max_request_bytes(self) -> usize {
        self.max_request_bytes.get()
    }
}

/// TLS-authenticated byte stream returned by the code-selected TLS authority.
pub trait OAuthLoopbackTlsStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T> OAuthLoopbackTlsStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// Owned future returned by the installation TLS capability.
pub type OAuthLoopbackTlsAcceptFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Box<dyn OAuthLoopbackTlsStream>, OAuthLoopbackTlsAcceptError>>
            + Send
            + 'a,
    >,
>;

/// Capability that performs a server-side TLS handshake for the code-owned callback listener.
///
/// A production implementation must use the installation's protected callback identity. The
/// adapter exposes no plaintext implementation and never accepts an operator-selected endpoint.
pub trait OAuthLoopbackTlsAcceptor: fmt::Debug + Send + Sync {
    fn accept(&self, stream: TcpStream) -> OAuthLoopbackTlsAcceptFuture<'_>;
}

/// Redacted failure from the installation-owned TLS boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Schwab OAuth callback TLS handshake failed")]
pub struct OAuthLoopbackTlsAcceptError;

/// One fixed-address callback receiver. It accepts at most one TLS connection.
pub struct OAuthLoopbackReceiver {
    listener: TcpListener,
    tls: Arc<dyn OAuthLoopbackTlsAcceptor>,
    bounds: OAuthLoopbackBounds,
}

impl fmt::Debug for OAuthLoopbackReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthLoopbackReceiver")
            .field("address", &CALLBACK_ADDRESS)
            .field("tls", &"[INSTALLATION CALLBACK IDENTITY]")
            .field("bounds", &self.bounds)
            .finish()
    }
}

impl OAuthLoopbackReceiver {
    /// Acquires the exact code-owned IPv4 loopback callback endpoint.
    pub async fn bind(
        tls: Arc<dyn OAuthLoopbackTlsAcceptor>,
        bounds: OAuthLoopbackBounds,
    ) -> Result<Self, OAuthLoopbackError> {
        let listener = TcpListener::bind(CALLBACK_ADDRESS)
            .await
            .map_err(|_| OAuthLoopbackError::AddressUnavailable)?;
        if listener
            .local_addr()
            .map_err(|_| OAuthLoopbackError::AddressUnavailable)?
            != CALLBACK_ADDRESS
        {
            return Err(OAuthLoopbackError::TrustBoundary);
        }
        Ok(Self {
            listener,
            tls,
            bounds,
        })
    }

    /// Receives, validates, acknowledges, and consumes exactly one callback connection.
    pub async fn receive(
        self,
        expected_state: &str,
        cancellation: CancellationToken,
    ) -> Result<CallbackOutcome, OAuthLoopbackError> {
        let (socket, peer) = cancellable_timeout(
            self.bounds.accept_timeout,
            &cancellation,
            self.listener.accept(),
        )
        .await?
        .map_err(|_| OAuthLoopbackError::Transport)?;
        if !peer.ip().is_loopback() {
            return Err(OAuthLoopbackError::TrustBoundary);
        }
        socket
            .set_nodelay(true)
            .map_err(|_| OAuthLoopbackError::Transport)?;
        let mut stream = cancellable_timeout(
            self.bounds.tls_handshake_timeout,
            &cancellation,
            self.tls.accept(socket),
        )
        .await?
        .map_err(|_| OAuthLoopbackError::Tls)?;
        let request = read_request(&mut *stream, self.bounds, &cancellation).await?;
        let redirected_url = callback_url(&request, self.bounds)?;
        let admission = RequestAdmission::new(self.bounds.max_request_bytes, NonZeroUsize::MIN);
        let outcome = OAuthCallback::parse(&redirected_url, expected_state, admission)?;
        let body = match &outcome {
            CallbackOutcome::Authorized(_) => AUTHORIZED_BODY,
            CallbackOutcome::Denied { .. } => DENIED_BODY,
        };
        write_response(&mut *stream, body, self.bounds.io_timeout, &cancellation).await?;
        Ok(outcome)
    }
}

async fn read_request(
    stream: &mut dyn OAuthLoopbackTlsStream,
    bounds: OAuthLoopbackBounds,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, OAuthLoopbackError> {
    let mut request = Vec::new();
    request
        .try_reserve_exact(bounds.max_request_bytes())
        .map_err(|_| OAuthLoopbackError::BoundsExceeded)?;
    let mut chunk = [0_u8; 1024];
    loop {
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(request);
        }
        if request.len() == bounds.max_request_bytes() {
            return Err(OAuthLoopbackError::BoundsExceeded);
        }
        let remaining = bounds.max_request_bytes() - request.len();
        let read_limit = remaining.min(chunk.len());
        let read = cancellable_timeout(
            bounds.io_timeout,
            cancellation,
            stream.read(&mut chunk[..read_limit]),
        )
        .await?
        .map_err(|_| OAuthLoopbackError::Transport)?;
        if read == 0 {
            return Err(OAuthLoopbackError::Protocol);
        }
        request.extend_from_slice(&chunk[..read]);
    }
}

fn callback_url(request: &[u8], bounds: OAuthLoopbackBounds) -> Result<String, OAuthLoopbackError> {
    let text = std::str::from_utf8(request).map_err(|_| OAuthLoopbackError::Protocol)?;
    let end = text.find("\r\n\r\n").ok_or(OAuthLoopbackError::Protocol)?;
    if end.checked_add(4) != Some(text.len()) {
        return Err(OAuthLoopbackError::Protocol);
    }
    let mut lines = text[..end].split("\r\n");
    let request_line = lines.next().ok_or(OAuthLoopbackError::Protocol)?;
    let mut parts = request_line.split(' ');
    let method = parts.next().ok_or(OAuthLoopbackError::Protocol)?;
    let target = parts.next().ok_or(OAuthLoopbackError::Protocol)?;
    let version = parts.next().ok_or(OAuthLoopbackError::Protocol)?;
    if parts.next().is_some()
        || method != "GET"
        || version != "HTTP/1.1"
        || !target.starts_with("/?")
        || target.contains('#')
        || target.len() > bounds.max_request_bytes()
    {
        return Err(OAuthLoopbackError::Protocol);
    }
    let mut host = None;
    let mut header_count = 0usize;
    for line in lines {
        header_count = header_count
            .checked_add(1)
            .ok_or(OAuthLoopbackError::BoundsExceeded)?;
        if header_count > bounds.max_header_count.get() {
            return Err(OAuthLoopbackError::BoundsExceeded);
        }
        let (name, value) = line.split_once(':').ok_or(OAuthLoopbackError::Protocol)?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || value.contains(['\r', '\n'])
        {
            return Err(OAuthLoopbackError::Protocol);
        }
        let value = value.trim();
        if name.eq_ignore_ascii_case("host") && host.replace(value).is_some() {
            return Err(OAuthLoopbackError::Protocol);
        }
        if (name.eq_ignore_ascii_case("content-length") && value != "0")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("authorization")
            || name.eq_ignore_ascii_case("proxy-authorization")
        {
            return Err(OAuthLoopbackError::Protocol);
        }
    }
    if host != Some("127.0.0.1:8182") {
        return Err(OAuthLoopbackError::TrustBoundary);
    }
    let capacity = "https://127.0.0.1:8182"
        .len()
        .checked_add(target.len())
        .ok_or(OAuthLoopbackError::BoundsExceeded)?;
    let mut url = String::new();
    url.try_reserve_exact(capacity)
        .map_err(|_| OAuthLoopbackError::BoundsExceeded)?;
    url.push_str("https://127.0.0.1:8182");
    url.push_str(target);
    Ok(url)
}

async fn write_response(
    stream: &mut dyn OAuthLoopbackTlsStream,
    body: &[u8],
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), OAuthLoopbackError> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nContent-Security-Policy: default-src 'none'\r\nReferrer-Policy: no-referrer\r\nX-Content-Type-Options: nosniff\r\nConnection: close\r\n\r\n",
        body.len()
    );
    cancellable_timeout(timeout, cancellation, async {
        stream.write_all(response.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.flush().await
    })
    .await?
    .map_err(|_| OAuthLoopbackError::Transport)
}

async fn cancellable_timeout<T>(
    timeout: Duration,
    cancellation: &CancellationToken,
    operation: impl Future<Output = T>,
) -> Result<T, OAuthLoopbackError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(OAuthLoopbackError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| OAuthLoopbackError::Deadline)
        }
    }
}

/// Secret-free callback receiver failure.
#[derive(Debug, Error)]
pub enum OAuthLoopbackError {
    #[error("Schwab OAuth callback receiver configuration is invalid")]
    InvalidConfiguration,
    #[error("Schwab OAuth callback endpoint is unavailable")]
    AddressUnavailable,
    #[error("Schwab OAuth callback trust boundary failed")]
    TrustBoundary,
    #[error("Schwab OAuth callback TLS handshake failed")]
    Tls,
    #[error("Schwab OAuth callback transport failed")]
    Transport,
    #[error("Schwab OAuth callback request is invalid")]
    Protocol,
    #[error("Schwab OAuth callback request exceeded local bounds")]
    BoundsExceeded,
    #[error("Schwab OAuth callback operation timed out")]
    Deadline,
    #[error("Schwab OAuth callback operation was cancelled")]
    Cancelled,
    #[error(transparent)]
    Adapter(#[from] SchwabAdapterError),
}
