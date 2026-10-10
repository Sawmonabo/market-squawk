//! Original study instruction bytes retained by the existing controlled artifact authority.

use std::{num::NonZeroUsize, time::Instant};

use market_squawk_data::Sha256Digest;
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRead,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ServiceError,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{
    ProductionGovernedBacktestRepository,
    lifecycle::{LinkedOperation, ensure_operation_live},
};

pub(super) const MAXIMUM_MATERIALIZATION_BYTES: usize = 64 * 1024 * 1024;

/// Inert complete reference. Only verified artifact bytes plus original input rejoin mint evidence.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MaterializationReference {
    artifact_id: String,
    sha256: String,
    byte_count: usize,
    materialized_digest: [u8; 32],
}

impl MaterializationReference {
    pub(super) fn artifact(&self) -> Result<ArtifactReference, ServiceError> {
        if self.byte_count > MAXIMUM_MATERIALIZATION_BYTES || self.materialized_digest == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        ArtifactReference::try_new(
            self.artifact_id.as_str(),
            self.sha256.as_str(),
            self.byte_count,
            "application/json",
        )
        .map_err(|_| ServiceError::InvalidResult)
    }

    pub(super) const fn digest(&self) -> Sha256Digest {
        Sha256Digest::new(self.materialized_digest)
    }
}

impl ProductionGovernedBacktestRepository {
    pub(super) async fn publish_materialization(
        &self,
        bytes: Vec<u8>,
        digest: Sha256Digest,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<MaterializationReference, ServiceError> {
        ensure_operation_live(cancellation, &self.lifecycle, deadline)?;
        if bytes.len() > MAXIMUM_MATERIALIZATION_BYTES {
            return Err(ServiceError::ResourceExhausted);
        }
        if digest.bytes() == [0; 32] {
            return Err(ServiceError::InvalidResult);
        }
        let publication = ArtifactPublication::try_json(bytes).map_err(map_error)?;
        let sha256 = publication.sha256_hex().to_owned();
        let byte_count = publication.byte_count();
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let published = self
            .artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(operation.token().clone(), deadline),
            )
            .await;
        ensure_operation_live(cancellation, &self.lifecycle, deadline)?;
        let artifact = published.map_err(map_error)?;
        if artifact.sha256() != sha256
            || artifact.byte_count() != byte_count
            || artifact.media_type() != "application/json"
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(MaterializationReference {
            artifact_id: artifact.id().to_owned(),
            sha256,
            byte_count,
            materialized_digest: digest.bytes(),
        })
    }

    pub(super) async fn read_materialization(
        &self,
        reference: &MaterializationReference,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ArtifactRead, ServiceError> {
        ensure_operation_live(cancellation, &self.lifecycle, deadline)?;
        let request = ArtifactReadRequest::try_new(
            reference.artifact()?,
            NonZeroUsize::new(MAXIMUM_MATERIALIZATION_BYTES).ok_or(ServiceError::Internal)?,
        )
        .map_err(map_error)?;
        let operation = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let read = self
            .artifacts
            .read(
                request,
                ArtifactReadContext::new(operation.token().clone(), deadline),
            )
            .await;
        ensure_operation_live(cancellation, &self.lifecycle, deadline)?;
        let read = read.map_err(map_error)?;
        if read.reference() != &reference.artifact()? {
            return Err(ServiceError::InvalidResult);
        }
        Ok(read)
    }
}

fn map_error(error: ArtifactError) -> ServiceError {
    match error {
        ArtifactError::Cancelled => ServiceError::Cancelled,
        ArtifactError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ArtifactError::ReadLimitExceeded => ServiceError::ResourceExhausted,
        ArtifactError::NotFound => ServiceError::NotFound,
        ArtifactError::InvalidPublication | ArtifactError::InvalidReference => {
            ServiceError::InvalidResult
        }
        ArtifactError::Unavailable => ServiceError::Unavailable,
    }
}
