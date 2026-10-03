//! Catalog registration of complete immutable SEC derived indexes; no source rights are stored.
use super::read_snapshot::CatalogReadSnapshot;
use super::storage::{append_audit, digest_columns, parse_digest, trusted_catalog_now};
use super::{ArtifactRecord, CatalogAuthority, CatalogError, CatalogResultLimits};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::CatalogLocation;
use rusqlite::{OptionalExtension as _, params};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct SecPreparedIndexRegistry {
    authority: Arc<Mutex<CatalogAuthority>>,
    location: CatalogLocation,
    binding: [u8; 32],
    limits: CatalogResultLimits,
}
impl SecPreparedIndexRegistry {
    pub(crate) fn new(
        authority: Arc<Mutex<CatalogAuthority>>,
        location: CatalogLocation,
        binding: [u8; 32],
        limits: CatalogResultLimits,
    ) -> Self {
        Self {
            authority,
            location,
            binding,
            limits,
        }
    }
    pub(crate) fn get(
        &self,
        key: EvidenceDigest,
        source: Uuid,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ArtifactRecord>, CatalogError> {
        let snapshot = CatalogReadSnapshot::open(
            &self.location,
            self.binding,
            self.limits,
            deadline,
            cancellation,
        )?;
        snapshot.read(|snapshot| load(snapshot.connection(), key, source))
    }
    pub(crate) fn register(
        &self,
        key: EvidenceDigest,
        source: Uuid,
        artifact: &ArtifactRecord,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ArtifactRecord, CatalogError> {
        check(deadline, cancellation)?;
        if key.algorithm() != DigestAlgorithm::Sha256
            || key.bytes() == [0; 32]
            || artifact.content_digest().algorithm() != DigestAlgorithm::Sha256
            || artifact.size_bytes() == 0
            || !artifact.relative_reference().starts_with("sec-prepared/")
        {
            return Err(CatalogError::InvalidRecord);
        }
        // Preparation owns no catalog lock while authenticating, indexing, hashing or syncing.
        let authority = super::authority::lock_catalog_writer(&self.authority, deadline, || {
            check(deadline, cancellation)
        })?;
        let transaction = authority.catalog().connection.unchecked_transaction()?;
        let now = trusted_catalog_now(&transaction)?;
        if let Some(existing) = load(&transaction, key, source)? {
            return Ok(existing);
        }
        if artifact.created_at() > now {
            return Err(CatalogError::PublicationTimeConflict);
        }
        let (algorithm, digest) = digest_columns(artifact.content_digest());
        transaction.execute("INSERT INTO sec_prepared_indexes(generation_digest,source_artifact_id,artifact_id,relative_reference,content_algorithm,content_digest,size_bytes,created_at_ns) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![key.bytes().as_slice(),source.to_string(),artifact.artifact_id().to_string(),artifact.relative_reference(),algorithm,digest,i64::try_from(artifact.size_bytes()).map_err(|_|CatalogError::InvalidRecord)?,artifact.created_at().unix_nanos()])?;
        append_audit(
            &transaction,
            "sec-prepared.published",
            &artifact.artifact_id().to_string(),
            key.bytes(),
            now,
        )?;
        check(deadline, cancellation)?;
        transaction.commit()?;
        Ok(artifact.clone())
    }
}
fn load(
    connection: &rusqlite::Connection,
    key: EvidenceDigest,
    source: Uuid,
) -> Result<Option<ArtifactRecord>, CatalogError> {
    let value:Option<(String,String,String,i64,Vec<u8>,i64,i64)>=connection.query_row("SELECT source_artifact_id,artifact_id,relative_reference,content_algorithm,content_digest,size_bytes,created_at_ns FROM sec_prepared_indexes WHERE generation_digest=?1",[key.bytes().as_slice()],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?))).optional()?;
    value
        .map(|(parent, id, path, algorithm, digest, bytes, created)| {
            if parent != source.to_string() {
                return Err(CatalogError::EvidenceConflict);
            }
            ArtifactRecord::try_from_stored(
                Uuid::parse_str(&id).map_err(|_| CatalogError::CorruptCatalog)?,
                path,
                parse_digest(algorithm, &digest)?,
                u64::try_from(bytes).map_err(|_| CatalogError::CorruptCatalog)?,
                Timestamp::from_unix_nanos(created),
            )
        })
        .transpose()
}
fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), CatalogError> {
    if cancellation.is_cancelled() {
        Err(CatalogError::MarketRecoveryReadCancelled)
    } else if Instant::now() >= deadline {
        Err(CatalogError::MarketRecoveryReadDeadlineExceeded)
    } else {
        Ok(())
    }
}
