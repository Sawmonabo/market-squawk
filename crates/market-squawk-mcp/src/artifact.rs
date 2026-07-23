//! Opaque artifact publication contract without filesystem authority.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAXIMUM_ARTIFACT_ID_BYTES: usize = 160;
const MAXIMUM_MEDIA_TYPE_BYTES: usize = 128;

/// Complete content-addressed publication handed to a capability-confined repository.
///
/// The repository implementation is responsible for staging, fsync, atomic rename, and catalog
/// registration under the controlled artifact root. MCP never receives a path or directory handle.
#[derive(Clone)]
pub struct ArtifactPublication {
    content: Arc<[u8]>,
    sha256_hex: Arc<str>,
    media_type: Arc<str>,
}

impl ArtifactPublication {
    pub(crate) fn try_json(content: Vec<u8>) -> Result<Self, ArtifactError> {
        if content.is_empty() {
            return Err(ArtifactError::InvalidPublication);
        }
        Ok(Self {
            sha256_hex: Arc::from(format!("{:x}", Sha256::digest(&content))),
            content: content.into(),
            media_type: Arc::from("application/json"),
        })
    }

    /// Complete immutable content. Implementations must never log it.
    #[must_use]
    pub fn content(&self) -> &[u8] {
        &self.content
    }

    /// Lowercase SHA-256 digest of the complete content.
    #[must_use]
    pub fn sha256_hex(&self) -> &str {
        &self.sha256_hex
    }

    /// Exact content length.
    #[must_use]
    pub fn byte_count(&self) -> usize {
        self.content.len()
    }

    /// Registered media type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }
}

impl fmt::Debug for ArtifactPublication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactPublication")
            .field("content", &"[ARTIFACT CONTENT REDACTED]")
            .field("sha256_hex", &self.sha256_hex)
            .field("media_type", &self.media_type)
            .field("byte_count", &self.content.len())
            .finish()
    }
}

/// Request lifecycle authority for one artifact publication.
#[derive(Clone)]
pub struct ArtifactPublicationContext {
    cancellation: CancellationToken,
    deadline: Instant,
}

impl ArtifactPublicationContext {
    pub(crate) const fn new(cancellation: CancellationToken, deadline: Instant) -> Self {
        Self {
            cancellation,
            deadline,
        }
    }

    /// Request cancellation propagated from the transport-neutral service call.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Absolute monotonic request deadline.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Rejects work after request cancellation or deadline expiry.
    ///
    /// # Errors
    ///
    /// Returns the exact terminal lifecycle class when publication authority has ended.
    pub fn ensure_live(&self) -> Result<(), ArtifactError> {
        if self.cancellation.is_cancelled() {
            return Err(ArtifactError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(ArtifactError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl fmt::Debug for ArtifactPublicationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactPublicationContext")
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("deadline", &self.deadline)
            .finish()
    }
}

/// Path-free reference returned to protocol clients.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactReference {
    id: Arc<str>,
    sha256: Arc<str>,
    byte_count: usize,
    media_type: Arc<str>,
}

impl ArtifactReference {
    /// Creates a reference whose identifier cannot be interpreted as a path or URI.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::InvalidReference`] when the identifier, digest, byte count, or
    /// media type violates the opaque-reference grammar.
    pub fn try_new(
        id: impl Into<Arc<str>>,
        sha256: impl Into<Arc<str>>,
        byte_count: usize,
        media_type: impl Into<Arc<str>>,
    ) -> Result<Self, ArtifactError> {
        let id = id.into();
        let sha256 = sha256.into();
        let media_type = media_type.into();
        let valid_id = !id.is_empty()
            && id.len() <= MAXIMUM_ARTIFACT_ID_BYTES
            && id
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
        let valid_digest = sha256.len() == 64
            && sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'));
        let valid_media = !media_type.is_empty()
            && media_type.len() <= MAXIMUM_MEDIA_TYPE_BYTES
            && media_type.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'+' | b'-')
            });
        if !valid_id || !valid_digest || byte_count == 0 || !valid_media {
            return Err(ArtifactError::InvalidReference);
        }
        Ok(Self {
            id,
            sha256,
            byte_count,
            media_type,
        })
    }

    pub(crate) fn matches(&self, publication: &ArtifactPublication) -> bool {
        self.sha256.as_ref() == publication.sha256_hex()
            && self.byte_count == publication.byte_count()
            && self.media_type.as_ref() == publication.media_type()
    }

    /// Returns the path-free repository identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the lowercase SHA-256 digest of the complete immutable content.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns the exact complete content length.
    #[must_use]
    pub const fn byte_count(&self) -> usize {
        self.byte_count
    }

    /// Returns the registered media type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }
}

/// Path-free, caller-bounded request for one exact opaque artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReadRequest {
    reference: ArtifactReference,
    maximum_bytes: NonZeroUsize,
}

impl ArtifactReadRequest {
    /// Binds a complete opaque reference to an explicit caller-selected byte ceiling.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::ReadLimitExceeded`] when the reference declares more complete
    /// content than the caller is willing to retain.
    pub fn try_new(
        reference: ArtifactReference,
        maximum_bytes: NonZeroUsize,
    ) -> Result<Self, ArtifactError> {
        if reference.byte_count() > maximum_bytes.get() {
            return Err(ArtifactError::ReadLimitExceeded);
        }
        Ok(Self {
            reference,
            maximum_bytes,
        })
    }

    /// Returns the exact path-free identity and content metadata supplied by the publisher.
    #[must_use]
    pub const fn reference(&self) -> &ArtifactReference {
        &self.reference
    }

    /// Returns the caller-selected complete-content byte ceiling.
    #[must_use]
    pub const fn maximum_bytes(&self) -> NonZeroUsize {
        self.maximum_bytes
    }

    /// Consumes the request and returns its exact opaque reference.
    #[must_use]
    pub fn into_reference(self) -> ArtifactReference {
        self.reference
    }
}

/// Request lifecycle authority for one immutable artifact read.
#[derive(Clone)]
pub struct ArtifactReadContext {
    cancellation: CancellationToken,
    deadline: Instant,
}

impl ArtifactReadContext {
    /// Creates lifecycle authority from the shared application request.
    #[must_use]
    pub const fn new(cancellation: CancellationToken, deadline: Instant) -> Self {
        Self {
            cancellation,
            deadline,
        }
    }

    /// Request cancellation propagated from the transport-neutral service call.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Absolute monotonic request deadline.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Rejects work after request cancellation or deadline expiry.
    pub fn ensure_live(&self) -> Result<(), ArtifactError> {
        if self.cancellation.is_cancelled() {
            return Err(ArtifactError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(ArtifactError::DeadlineExceeded);
        }
        Ok(())
    }
}

impl fmt::Debug for ArtifactReadContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactReadContext")
            .field("cancellation", &"[CANCELLATION TOKEN]")
            .field("deadline", &self.deadline)
            .finish()
    }
}

/// Complete immutable artifact content verified against its opaque reference.
#[derive(Clone)]
pub struct ArtifactRead {
    reference: ArtifactReference,
    content: Arc<[u8]>,
}

impl ArtifactRead {
    /// Validates complete bytes against the reference's digest and length.
    ///
    /// Repository implementations use this as the final return boundary after capability-confined
    /// reads. Content is never included in debug output.
    pub fn try_new(reference: ArtifactReference, content: Vec<u8>) -> Result<Self, ArtifactError> {
        if content.len() != reference.byte_count()
            || format!("{:x}", Sha256::digest(&content)) != reference.sha256()
        {
            return Err(ArtifactError::Unavailable);
        }
        Ok(Self {
            reference,
            content: content.into(),
        })
    }

    /// Returns the exact verified path-free identity and content metadata.
    #[must_use]
    pub const fn reference(&self) -> &ArtifactReference {
        &self.reference
    }

    /// Returns complete immutable content. Callers must never log it.
    #[must_use]
    pub fn content(&self) -> &[u8] {
        &self.content
    }
}

impl fmt::Debug for ArtifactRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactRead")
            .field("reference", &self.reference)
            .field("content", &"[ARTIFACT CONTENT REDACTED]")
            .finish()
    }
}

/// Capability-confined immutable artifact repository.
#[async_trait]
pub trait ArtifactRepository: Send + Sync + 'static {
    /// Atomically publishes and registers a complete content-addressed artifact.
    ///
    /// The implementation must stage, fsync, atomically rename, and durably register the digest
    /// before returning. The returned identifier is opaque and must contain no path. Implementations
    /// must observe the supplied cancellation and deadline before irreversible work and at safe
    /// publication boundaries. Once the context cancellation token is cancelled, the returned
    /// future must be immediately safe to drop; the bounded session-shutdown timeout is only
    /// best-effort operational grace and is not a prerequisite for Drop safety. Externally visible
    /// state must still be either absent or a complete atomically published object.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError`] when durable publication or registration cannot complete.
    async fn publish(
        &self,
        publication: ArtifactPublication,
        context: ArtifactPublicationContext,
    ) -> Result<ArtifactReference, ArtifactError>;

    /// Reads complete immutable content through a path-free, caller-bounded reference.
    ///
    /// Implementations must verify the identifier, digest, exact byte count, and media type before
    /// returning. They must observe cancellation and deadline authority while reading and must
    /// never expose a filesystem path, directory handle, or partially verified content.
    async fn read(
        &self,
        request: ArtifactReadRequest,
        context: ArtifactReadContext,
    ) -> Result<ArtifactRead, ArtifactError>;
}

/// Artifact contract or repository failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ArtifactError {
    /// Complete publication content was invalid.
    #[error("artifact publication is invalid")]
    InvalidPublication,
    /// Repository returned a path-like or inconsistent reference.
    #[error("artifact reference is invalid")]
    InvalidReference,
    /// The caller-selected complete-content bound is below the reference's declared size.
    #[error("artifact read byte limit was exceeded")]
    ReadLimitExceeded,
    /// No immutable artifact matches the complete opaque reference.
    #[error("artifact was not found")]
    NotFound,
    /// Durable capability-confined repository is unavailable.
    #[error("artifact repository is unavailable")]
    Unavailable,
    /// Request cancellation ended publication authority.
    #[error("artifact publication was cancelled")]
    Cancelled,
    /// Request deadline ended publication authority.
    #[error("artifact publication deadline exceeded")]
    DeadlineExceeded,
}
