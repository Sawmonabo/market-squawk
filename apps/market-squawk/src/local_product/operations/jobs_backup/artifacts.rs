//! Exact controlled forecast inputs and completed recommendation evidence share the jobs component.

use std::{
    io::{Read, Write},
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use crate::application::analysis::{
    GovernedBacktestInputAuthorityLimits, GovernedBacktestRepositoryLimits,
    ProductionGovernedBacktestInputAuthority, ProductionGovernedBacktestRepository,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_jobs::{
    JobRepositoryConfig, JobsAndReceiptsBackupBinding, JobsAndReceiptsBackupExport,
    SqliteJobRepository,
};
use market_squawk_platform::{JobDatabaseLocation, LocalPaths};
use market_squawk_services::{
    ArtifactAuthority, ArtifactError, ArtifactPublication, ArtifactPublicationContext,
    ArtifactReadContext, ArtifactReadRequest, ArtifactReference, ArtifactRepository,
    ArtifactResolveRequest,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::super::workspace_backup::WorkspaceComponentSnapshotReceipt;
use crate::application::{
    backup::{ProductBackupError, ProductBackupSnapshot},
    model::forecast_preparation::MAXIMUM_FORECAST_JOB_INPUT_BYTES,
};

const MAGIC: &[u8; 16] = b"MSQJOBARTIFACT1\0";
const MAXIMUM_STUDY_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_COMPONENT_BYTES: u64 = 1024 * 1024 * 1024;
pub(super) const MAXIMUM_INPUT_ARTIFACTS: usize = 4_096;
const ARTIFACT_DEADLINE: Duration = Duration::from_secs(60);
const CHUNK_BYTES: usize = 64 * 1024;

pub(super) async fn write_component(
    export: &JobsAndReceiptsBackupExport,
    artifacts: &dyn ArtifactAuthority,
    authority_indices: [&[u8]; 2],
    index_revisions: [[u8; 32]; 2],
    terminal_artifacts: &[ArtifactReference],
    writer: &mut (dyn Write + Send),
    cancellation: &CancellationToken,
) -> Result<(WorkspaceComponentSnapshotReceipt, Vec<ArtifactReference>), ProductBackupError> {
    let (binding, inputs) = inventory(export.as_bytes(), terminal_artifacts)?;
    if binding != export.receipt().binding() {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    let mut references = Vec::new();
    references
        .try_reserve_exact(inputs.len())
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    for input in &inputs {
        let reference = artifacts
            .resolve(
                ArtifactResolveRequest::try_new(
                    input.identity().as_str(),
                    artifact_limit(input.maximum_bytes)?,
                )
                .map_err(map_artifact)?,
                read_context(cancellation)?,
            )
            .await
            .map_err(map_artifact)?;
        validate_reference(input, &reference)?;
        references.push(reference);
    }
    let mut output = ComponentWriter {
        writer,
        digest: Sha256::new(),
        length: 0,
    };
    output.append(MAGIC, cancellation)?;
    output.append(&export.receipt().byte_length().to_be_bytes(), cancellation)?;
    output.append(
        &u32::try_from(references.len())
            .map_err(|_| ProductBackupError::InvalidComponent)?
            .to_be_bytes(),
        cancellation,
    )?;
    output.append(export.as_bytes(), cancellation)?;
    for bytes in authority_indices {
        if bytes.is_empty()
            || bytes.len()
                > market_squawk_platform::LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ProductBackupError::InvalidComponent);
        }
        output.append(
            &u64::try_from(bytes.len())
                .map_err(|_| ProductBackupError::InvalidComponent)?
                .to_be_bytes(),
            cancellation,
        )?;
        output.append(bytes, cancellation)?;
    }

    for reference in &references {
        let read = artifacts
            .read(
                ArtifactReadRequest::try_new(
                    reference.clone(),
                    artifact_limit(reference.byte_count())?,
                )
                .map_err(map_artifact)?,
                read_context(cancellation)?,
            )
            .await
            .map_err(map_artifact)?;
        let id = reference.id().as_bytes();
        output.append(
            &u16::try_from(id.len())
                .map_err(|_| ProductBackupError::InvalidComponent)?
                .to_be_bytes(),
            cancellation,
        )?;
        output.append(id, cancellation)?;
        output.append(
            &u64::try_from(read.content().len())
                .map_err(|_| ProductBackupError::InvalidComponent)?
                .to_be_bytes(),
            cancellation,
        )?;
        output.append(read.content(), cancellation)?;
    }
    let (length, sha256) = output.finish();
    let mut authority = Sha256::new();
    authority.update(b"market-squawk/jobs-controlled-input-snapshot/v1\0");
    authority.update(export.receipt().authority_revision_sha256());
    for revision in index_revisions {
        authority.update(revision);
    }
    authority.update(sha256);
    let receipt =
        WorkspaceComponentSnapshotReceipt::try_new(authority.finalize().into(), length, sha256)?;
    Ok((receipt, references))
}

pub(super) async fn revalidate_artifacts(
    artifacts: &dyn ArtifactAuthority,
    references: &[ArtifactReference],
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    for reference in references {
        artifacts
            .read(
                ArtifactReadRequest::try_new(
                    reference.clone(),
                    artifact_limit(reference.byte_count())?,
                )
                .map_err(map_artifact)?,
                read_context(cancellation)?,
            )
            .await
            .map_err(map_artifact)?;
    }
    Ok(())
}

/// Restores exact controlled inputs before making their validated job ledger runnable.
#[allow(
    clippy::too_many_arguments,
    reason = "fresh database, artifact, snapshot, and allocation authority remain explicit"
)]
pub(super) async fn restore_component(
    reader: &mut (dyn Read + Send),
    location: JobDatabaseLocation,
    paths: &LocalPaths,
    input_limits: GovernedBacktestInputAuthorityLimits,
    terminal_limits: GovernedBacktestRepositoryLimits,
    config: JobRepositoryConfig,
    artifacts: &dyn ArtifactRepository,
    snapshot: ProductBackupSnapshot,
    maximum_buffered_bytes: NonZeroUsize,
    cancellation: &CancellationToken,
) -> Result<(), ProductBackupError> {
    let mut input = ComponentReader {
        reader,
        observed: 0,
    };
    let mut magic = [0; 16];
    input.exact(&mut magic, cancellation)?;
    if &magic != MAGIC {
        return Err(ProductBackupError::InvalidComponentSchema);
    }
    let mut length = [0; 8];
    input.exact(&mut length, cancellation)?;
    let owner_length = usize::try_from(u64::from_be_bytes(length))
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    if owner_length == 0 || owner_length > maximum_buffered_bytes.get() {
        return Err(ProductBackupError::InvalidComponent);
    }
    let mut count = [0; 4];
    input.exact(&mut count, cancellation)?;
    let count = usize::try_from(u32::from_be_bytes(count))
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    if count > MAXIMUM_INPUT_ARTIFACTS {
        return Err(ProductBackupError::InvalidComponent);
    }
    let owner = input.bytes(owner_length, cancellation)?;
    // Exact two existing owner indices; all bytes count toward the original component ceiling.
    let mut buffered = owner.len();
    let mut indices = Vec::with_capacity(2);
    for bound in [
        input_limits.maximum_backup_index_bytes(),
        terminal_limits.maximum_backup_index_bytes(),
    ] {
        let mut length = [0; 8];
        input.exact(&mut length, cancellation)?;
        let length = usize::try_from(u64::from_be_bytes(length))
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        buffered = buffered
            .checked_add(length)
            .ok_or(ProductBackupError::InvalidComponent)?;
        if length == 0 || length > bound || buffered > maximum_buffered_bytes.get() {
            return Err(ProductBackupError::InvalidComponent);
        }
        indices.push(input.bytes(length, cancellation)?);
    }
    ProductionGovernedBacktestInputAuthority::validate_backup_index(&indices[0], input_limits)
        .map_err(super::map_index_error)?;
    let terminal_artifacts =
        ProductionGovernedBacktestRepository::validated_backup_index_artifacts(
            &indices[1],
            terminal_limits,
            MAXIMUM_INPUT_ARTIFACTS,
        )
        .map_err(super::map_index_error)?;
    let (binding, expected) = inventory(&owner, &terminal_artifacts)?;
    if expected.len() != count || binding != super::binding(snapshot)? {
        return Err(ProductBackupError::SnapshotMismatch);
    }
    for expected in expected {
        let mut length = [0; 2];
        input.exact(&mut length, cancellation)?;
        let id_length = usize::from(u16::from_be_bytes(length));
        if id_length == 0 || id_length > 160 {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let id = input.bytes(id_length, cancellation)?;
        if id.as_slice() != expected.identity().as_str().as_bytes() {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let mut length = [0; 8];
        input.exact(&mut length, cancellation)?;
        let byte_length = usize::try_from(u64::from_be_bytes(length))
            .map_err(|_| ProductBackupError::ArtifactMismatch)?;
        if byte_length == 0
            || byte_length > expected.maximum_bytes
            || buffered
                .checked_add(byte_length)
                .is_none_or(|total| total > maximum_buffered_bytes.get())
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let bytes = input.bytes(byte_length, cancellation)?;
        if <[u8; 32]>::from(Sha256::digest(&bytes)) != expected.digest().bytes() {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let expected_reference = ArtifactReference::try_new(
            expected.identity().as_str(),
            hex(expected.digest().bytes()),
            byte_length,
            "application/json",
        )
        .map_err(map_artifact)?;
        validate_reference(&expected, &expected_reference)?;
        let publication = ArtifactPublication::try_json(bytes).map_err(map_artifact)?;
        if !expected_reference.matches(&publication) {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let actual = artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(cancellation.clone(), deadline()?),
            )
            .await
            .map_err(map_artifact)?;
        if actual != expected_reference {
            return Err(ProductBackupError::ArtifactMismatch);
        }
    }
    input.finish(cancellation)?;
    ensure_live(cancellation)?;
    // Original index stores are restored only after complete artifact verification and EOF.
    let restore_deadline = deadline()?;
    ProductionGovernedBacktestInputAuthority::restore_backup_index_fresh(
        paths,
        input_limits,
        &indices[0],
        cancellation,
        restore_deadline,
    )
    .map_err(super::map_index_error)?;
    ProductionGovernedBacktestRepository::restore_backup_index_fresh(
        paths,
        terminal_limits,
        &indices[1],
        cancellation,
        restore_deadline,
    )
    .map_err(super::map_index_error)?;
    drop(indices);
    ensure_live(cancellation)?;
    SqliteJobRepository::restore_fresh(location, config, &owner)
        .await
        .map_err(|_| ProductBackupError::RestoreComponents)?;
    ensure_live(cancellation)
}

struct ExpectedArtifact {
    identity: SourceIdentifier,
    digest: EvidenceDigest,
    maximum_bytes: usize,
    exact: Option<ArtifactReference>,
}
impl ExpectedArtifact {
    fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }
    const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

fn inventory(
    encoded: &[u8],
    terminal_artifacts: &[ArtifactReference],
) -> Result<(JobsAndReceiptsBackupBinding, Vec<ExpectedArtifact>), ProductBackupError> {
    let kind = SourceIdentifier::try_from("model.forecast-generation.v1")
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    let authority = SourceIdentifier::try_from("model.forecast-input.v1")
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    let (binding, inputs) =
        SqliteJobRepository::validated_backup_inputs(encoded, &kind, &authority)
            .map_err(|_| ProductBackupError::SnapshotMismatch)?;
    if inputs.len() > MAXIMUM_INPUT_ARTIFACTS
        || inputs
            .iter()
            .any(|input| input.digest().algorithm() != DigestAlgorithm::Sha256)
    {
        return Err(ProductBackupError::InvalidComponent);
    }
    let kind = SourceIdentifier::try_from("analysis.backtest.v1")
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    let authority = SourceIdentifier::try_from("analysis.governed-backtest-terminal.v1")
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    let (result_binding, results) = SqliteJobRepository::validated_backup_result_artifacts(
        encoded,
        &kind,
        &authority,
        NonZeroUsize::new(MAXIMUM_INPUT_ARTIFACTS).ok_or(ProductBackupError::InvalidComponent)?,
    )
    .map_err(|_| ProductBackupError::SnapshotMismatch)?;
    if result_binding != binding
        || inputs.len().saturating_add(results.len()) > MAXIMUM_INPUT_ARTIFACTS
    {
        return Err(ProductBackupError::InvalidComponent);
    }
    let mut expected = Vec::new();
    expected
        .try_reserve_exact(inputs.len() + results.len())
        .map_err(|_| ProductBackupError::InvalidComponent)?;
    expected.extend(inputs.into_iter().map(|input| ExpectedArtifact {
        identity: input.identity().clone(),
        digest: input.digest(),
        maximum_bytes: MAXIMUM_FORECAST_JOB_INPUT_BYTES,
        exact: None,
    }));
    for result in results {
        if result.media_type() != "application/json"
            || result.byte_count() > MAXIMUM_STUDY_ARTIFACT_BYTES
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        let digest = decode_digest(result.sha256())?;
        expected.push(ExpectedArtifact {
            identity: SourceIdentifier::try_from(result.id())
                .map_err(|_| ProductBackupError::ArtifactMismatch)?,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            maximum_bytes: MAXIMUM_STUDY_ARTIFACT_BYTES,
            exact: Some(result),
        });
    }
    expected.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
    if expected
        .windows(2)
        .any(|pair| pair[0].identity == pair[1].identity)
    {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    // Terminal-store custody is independent of a job's completion state.
    for reference in terminal_artifacts {
        if reference.media_type() != "application/json"
            || reference.byte_count() > MAXIMUM_STUDY_ARTIFACT_BYTES
        {
            return Err(ProductBackupError::ArtifactMismatch);
        }
        match expected.binary_search_by(|entry| entry.identity.as_str().cmp(reference.id())) {
            Ok(index) => validate_reference(&expected[index], reference)?,
            Err(index) => {
                if expected.len() >= MAXIMUM_INPUT_ARTIFACTS {
                    return Err(ProductBackupError::InvalidComponent);
                }
                expected
                    .try_reserve_exact(1)
                    .map_err(|_| ProductBackupError::InvalidComponent)?;
                expected.insert(
                    index,
                    ExpectedArtifact {
                        identity: SourceIdentifier::try_from(reference.id())
                            .map_err(|_| ProductBackupError::ArtifactMismatch)?,
                        digest: EvidenceDigest::new(
                            DigestAlgorithm::Sha256,
                            decode_digest(reference.sha256())?,
                        ),
                        maximum_bytes: MAXIMUM_STUDY_ARTIFACT_BYTES,
                        exact: Some(reference.clone()),
                    },
                );
            }
        }
    }
    // The two original owner index frames consume the same finite component item budget.
    if expected
        .len()
        .checked_add(2)
        .is_none_or(|count| count > MAXIMUM_INPUT_ARTIFACTS)
    {
        return Err(ProductBackupError::InvalidComponent);
    }
    Ok((binding, expected))
}
fn validate_reference(
    input: &ExpectedArtifact,
    reference: &ArtifactReference,
) -> Result<(), ProductBackupError> {
    if reference.id() != input.identity().as_str()
        || reference.sha256() != hex(input.digest().bytes())
        || reference.byte_count() > input.maximum_bytes
        || reference.media_type() != "application/json"
        || input
            .exact
            .as_ref()
            .is_some_and(|expected| expected != reference)
    {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    Ok(())
}
fn artifact_limit(maximum: usize) -> Result<NonZeroUsize, ProductBackupError> {
    if maximum > MAXIMUM_STUDY_ARTIFACT_BYTES.max(MAXIMUM_FORECAST_JOB_INPUT_BYTES) {
        return Err(ProductBackupError::InvalidComponent);
    }
    NonZeroUsize::new(maximum).ok_or(ProductBackupError::InvalidComponent)
}
fn deadline() -> Result<Instant, ProductBackupError> {
    Instant::now()
        .checked_add(ARTIFACT_DEADLINE)
        .ok_or(ProductBackupError::ArtifactUnavailable)
}
fn read_context(
    cancellation: &CancellationToken,
) -> Result<ArtifactReadContext, ProductBackupError> {
    ensure_live(cancellation)?;
    Ok(ArtifactReadContext::new(cancellation.clone(), deadline()?))
}
fn ensure_live(cancellation: &CancellationToken) -> Result<(), ProductBackupError> {
    if cancellation.is_cancelled() {
        Err(ProductBackupError::Cancelled)
    } else {
        Ok(())
    }
}
fn map_artifact(error: ArtifactError) -> ProductBackupError {
    if error == ArtifactError::Cancelled {
        ProductBackupError::Cancelled
    } else {
        ProductBackupError::ArtifactUnavailable
    }
}
fn decode_digest(value: &str) -> Result<[u8; 32], ProductBackupError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProductBackupError::ArtifactMismatch);
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ProductBackupError::ArtifactMismatch)?;
    }
    Ok(digest)
}
fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct ComponentWriter<'a> {
    writer: &'a mut (dyn Write + Send),
    digest: Sha256,
    length: u64,
}
impl ComponentWriter<'_> {
    fn append(
        &mut self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.length = self
            .length
            .checked_add(
                u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            )
            .filter(|length| *length <= MAXIMUM_COMPONENT_BYTES)
            .ok_or(ProductBackupError::InvalidComponent)?;
        for chunk in bytes.chunks(CHUNK_BYTES) {
            ensure_live(cancellation)?;
            self.writer
                .write_all(chunk)
                .map_err(|_| ProductBackupError::ArtifactUnavailable)?;
            self.digest.update(chunk);
        }
        Ok(())
    }
    fn finish(self) -> (u64, [u8; 32]) {
        (self.length, self.digest.finalize().into())
    }
}
struct ComponentReader<'a> {
    reader: &'a mut (dyn Read + Send),
    observed: u64,
}
impl ComponentReader<'_> {
    fn exact(
        &mut self,
        bytes: &mut [u8],
        cancellation: &CancellationToken,
    ) -> Result<(), ProductBackupError> {
        self.observed = self
            .observed
            .checked_add(
                u64::try_from(bytes.len()).map_err(|_| ProductBackupError::InvalidComponent)?,
            )
            .filter(|length| *length <= MAXIMUM_COMPONENT_BYTES)
            .ok_or(ProductBackupError::InvalidComponent)?;
        for chunk in bytes.chunks_mut(CHUNK_BYTES) {
            ensure_live(cancellation)?;
            self.reader
                .read_exact(chunk)
                .map_err(|_| ProductBackupError::ArtifactMismatch)?;
        }
        Ok(())
    }
    fn bytes(
        &mut self,
        size: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, ProductBackupError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|_| ProductBackupError::InvalidComponent)?;
        bytes.resize(size, 0);
        self.exact(&mut bytes, cancellation)?;
        Ok(bytes)
    }
    fn finish(&mut self, cancellation: &CancellationToken) -> Result<(), ProductBackupError> {
        ensure_live(cancellation)?;
        let mut extra = [0];
        match self.reader.read(&mut extra) {
            Ok(0) => Ok(()),
            _ => Err(ProductBackupError::ArtifactMismatch),
        }
    }
}
