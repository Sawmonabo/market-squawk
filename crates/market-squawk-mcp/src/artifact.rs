//! Opaque artifact publication contract without filesystem authority.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;

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
}

/// Capability-confined immutable artifact repository.
#[async_trait]
pub trait ArtifactRepository: Send + Sync + 'static {
    /// Atomically publishes and registers a complete content-addressed artifact.
    ///
    /// The implementation must stage, fsync, atomically rename, and durably register the digest
    /// before returning. The returned identifier is opaque and must contain no path.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError`] when durable publication or registration cannot complete.
    async fn publish(
        &self,
        publication: ArtifactPublication,
    ) -> Result<ArtifactReference, ArtifactError>;
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
    /// Durable capability-confined repository is unavailable.
    #[error("artifact repository is unavailable")]
    Unavailable,
}
