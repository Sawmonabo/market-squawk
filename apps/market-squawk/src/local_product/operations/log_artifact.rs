//! Operations-owned diagnostic exports over the shared controlled artifact authority.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRepository,
    NDJSON_ARTIFACT_MEDIA_TYPE,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    application::logs::{
        DiagnosticArtifactAdmission, DiagnosticArtifactPublisher, DiagnosticArtifactReceipt,
        StructuredLogError,
    },
    artifact_repository::ControlledArtifactRepository,
};

/// Bounded diagnostic-artifact adapter over the application's sole artifact repository.
pub(crate) struct ControlledDiagnosticArtifactPublisher {
    repository: Arc<ControlledArtifactRepository>,
    maximum_bytes: NonZeroUsize,
    maximum_records: NonZeroUsize,
}

impl ControlledDiagnosticArtifactPublisher {
    /// Retains the shared artifact authority with stricter Operations-owned export bounds.
    pub(crate) fn new(
        repository: Arc<ControlledArtifactRepository>,
        maximum_bytes: NonZeroUsize,
        maximum_records: NonZeroUsize,
    ) -> Self {
        Self {
            repository,
            maximum_bytes,
            maximum_records,
        }
    }

    fn admit(&self, admission: &DiagnosticArtifactAdmission) -> Result<(), StructuredLogError> {
        if admission.bytes.len() > self.maximum_bytes.get()
            || admission.record_count > self.maximum_records.get()
        {
            return Err(StructuredLogError::ExportTooLarge);
        }
        if admission.media_type != NDJSON_ARTIFACT_MEDIA_TYPE
            || admission.bytes.is_empty()
            || admission.record_count == 0
            || count_records(&admission.bytes)? != admission.record_count
            || <[u8; 32]>::from(Sha256::digest(&admission.bytes)) != admission.sha256
        {
            return Err(StructuredLogError::InvalidArtifactReceipt);
        }
        Ok(())
    }
}

impl fmt::Debug for ControlledDiagnosticArtifactPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlledDiagnosticArtifactPublisher")
            .field("repository", &"[RETAINED ARTIFACT AUTHORITY]")
            .field("maximum_bytes", &self.maximum_bytes)
            .field("maximum_records", &self.maximum_records)
            .finish()
    }
}

#[async_trait]
impl DiagnosticArtifactPublisher for ControlledDiagnosticArtifactPublisher {
    async fn publish(
        &self,
        admission: DiagnosticArtifactAdmission,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<DiagnosticArtifactReceipt, StructuredLogError> {
        self.admit(&admission)?;
        let expected_byte_count = admission.bytes.len();
        let expected_sha256 = admission.sha256;
        let publication =
            ArtifactPublication::try_ndjson(admission.bytes).map_err(map_artifact_error)?;
        let reference = ArtifactRepository::publish(
            self.repository.as_ref(),
            publication,
            ArtifactPublicationContext::new(cancellation, deadline),
        )
        .await
        .map_err(map_artifact_error)?;

        let expected_sha256_hex = encode_sha256(expected_sha256);
        if reference.byte_count() != expected_byte_count
            || reference.sha256() != expected_sha256_hex.as_str()
            || reference.media_type() != NDJSON_ARTIFACT_MEDIA_TYPE
        {
            return Err(StructuredLogError::InvalidArtifactReceipt);
        }
        let byte_length =
            u64::try_from(expected_byte_count).map_err(|_| StructuredLogError::Allocation)?;
        DiagnosticArtifactReceipt::try_new(reference.id(), byte_length, expected_sha256)
    }
}

fn count_records(bytes: &[u8]) -> Result<usize, StructuredLogError> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .try_fold(0_usize, |count, _line| {
            count.checked_add(1).ok_or(StructuredLogError::Allocation)
        })
}

fn encode_sha256(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

const fn map_artifact_error(error: ArtifactError) -> StructuredLogError {
    match error {
        ArtifactError::InvalidPublication | ArtifactError::InvalidReference => {
            StructuredLogError::InvalidArtifactReceipt
        }
        ArtifactError::ReadLimitExceeded => StructuredLogError::ExportTooLarge,
        ArtifactError::NotFound
        | ArtifactError::Unavailable
        | ArtifactError::Cancelled
        | ArtifactError::DeadlineExceeded => StructuredLogError::Unavailable,
    }
}
