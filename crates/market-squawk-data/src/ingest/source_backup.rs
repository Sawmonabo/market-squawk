//! Exact full-catalog sealed raw closure for the existing workspace SourceData component.
//!
//! The analytical component restores the complete catalog. Raw claims are enumerated in one
//! independent WAL read snapshot; explicit backup streams every required body without holding
//! the ingest writer or retaining the full inventory in memory.
use super::*;
use sha2::{Digest as _, Sha256};
use std::io::{Read, Write};

const MAGIC: &[u8; 16] = b"MSQSOURCECLOSE1\0";

/// Value receipt of the exact canonical catalog claim inventory, without retaining raw payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedSourceBackupInventory {
    claims: u64,
    bytes: u64,
    digest: [u8; 32],
}
impl SealedSourceBackupInventory {
    pub const fn claims(self) -> u64 {
        self.claims
    }
    pub const fn raw_bytes(self) -> u64 {
        self.bytes
    }
    pub const fn digest(self) -> [u8; 32] {
        self.digest
    }
}
impl AnalyticalDataService {
    /// Captures a small fingerprint before analytical snapshot creation. Revalidate after export;
    /// do not retain the analytical mutation gate across the analytical backup's own gate.
    pub fn source_backup_inventory(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SealedSourceBackupInventory, IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| scan(snapshot, deadline, cancellation, |_| Ok(())))
            .map_err(map_source_backup_error)
    }
    /// Synchronous bounded owner operation; the caller retains its supervised backup worker.
    pub fn write_source_backup(
        &self,
        expected: SealedSourceBackupInventory,
        store: &market_squawk_platform::SealedResearchJournalStore,
        writer: &mut dyn Write,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), IngestError> {
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                if scan(snapshot, deadline, cancellation, |_| Ok(()))? != expected {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                let control = MarketEventReadControl {
                    deadline,
                    cancellation,
                };
                for bytes in [
                    MAGIC.as_slice(),
                    &expected.claims.to_be_bytes(),
                    &expected.bytes.to_be_bytes(),
                    &expected.digest,
                ] {
                    check_market_event_read(deadline, cancellation)?;
                    writer
                        .write_all(bytes)
                        .map_err(|_| IngestError::ProviderCaptureRequired)?;
                }
                let actual = scan(snapshot, deadline, cancellation, |claim| {
                    store
                        .write_backup_claim(claim, writer, &control)
                        .map_err(map_provider_recovery_store_error)
                })?;
                if actual != expected {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                check_market_event_read(deadline, cancellation)
            })
            .map_err(map_source_backup_error)
    }
    /// Restores only claims physically present in this already-restored exact analytical catalog.
    /// The stream contains no path claims; canonical catalog order defines every body and length.
    pub fn restore_source_backup(
        &self,
        store: &market_squawk_platform::SealedResearchJournalStore,
        reader: &mut dyn Read,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SealedSourceBackupInventory, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let snapshot = self
            .manifests
            .read_snapshot(self.catalog_read_limits, deadline, cancellation)
            .map_err(map_market_recovery_catalog_error)?;
        snapshot
            .read(|snapshot| {
                let expected = scan(snapshot, deadline, cancellation, |_| Ok(()))?;
                let mut magic = [0; 16];
                let mut claims = [0; 8];
                let mut bytes = [0; 8];
                let mut digest = [0; 32];
                for buffer in [
                    magic.as_mut_slice(),
                    claims.as_mut_slice(),
                    bytes.as_mut_slice(),
                    digest.as_mut_slice(),
                ] {
                    check_market_event_read(deadline, cancellation)?;
                    reader
                        .read_exact(buffer)
                        .map_err(|_| IngestError::ProviderCaptureRequired)?;
                }
                if &magic != MAGIC
                    || u64::from_be_bytes(claims) != expected.claims
                    || u64::from_be_bytes(bytes) != expected.bytes
                    || digest != expected.digest
                {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                let control = MarketEventReadControl {
                    deadline,
                    cancellation,
                };
                let actual = scan(snapshot, deadline, cancellation, |claim| {
                    store
                        .restore_backup_claim(claim, reader, &control)
                        .map_err(map_provider_recovery_store_error)
                })?;
                if actual != expected {
                    return Err(IngestError::ProviderCaptureRequired);
                }
                check_market_event_read(deadline, cancellation)?;
                Ok(actual)
            })
            .map_err(map_source_backup_error)
    }
}
fn scan(
    snapshot: &crate::catalog::CatalogReadSnapshot,
    deadline: Instant,
    cancellation: &CancellationToken,
    mut visit: impl FnMut(&SealedResearchRawClaim) -> Result<(), IngestError>,
) -> Result<SealedSourceBackupInventory, IngestError> {
    let mut after = None;
    let mut claims = 0u64;
    let mut bytes = 0u64;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/source-backup-catalog-closure/v1\0");
    loop {
        check_market_event_read(deadline, cancellation)?;
        let page = snapshot.authoritative_provider_raw_claim_page(after)?;
        if page.is_empty() {
            break;
        }
        for (key, claim) in page {
            check_market_event_read(deadline, cancellation)?;
            if key.algorithm() != DigestAlgorithm::Sha256
                || after.is_some_and(|prior: EvidenceDigest| key.bytes() <= prior.bytes())
            {
                return Err(IngestError::ProviderCaptureRequired);
            }
            claims = claims
                .checked_add(1)
                .ok_or(IngestError::ProviderCaptureRequired)?;
            let (kind, size, content, physical) = match &claim {
                SealedResearchRawClaim::JournalSegment(c) => (
                    0u8,
                    c.size_bytes(),
                    c.content_digest(),
                    c.physical_receipt_digest(),
                ),
                SealedResearchRawClaim::LogicalObject(c) => (
                    1u8,
                    c.size_bytes(),
                    c.content_digest(),
                    c.physical_receipt_digest(),
                ),
            };
            bytes = bytes
                .checked_add(size)
                .ok_or(IngestError::ProviderCaptureRequired)?;
            digest.update(key.bytes());
            digest.update([kind]);
            digest.update(size.to_be_bytes());
            digest.update(content.bytes());
            digest.update(physical.bytes());
            visit(&claim)?;
            after = Some(key);
        }
    }
    digest.update(claims.to_be_bytes());
    digest.update(bytes.to_be_bytes());
    Ok(SealedSourceBackupInventory {
        claims,
        bytes,
        digest: digest.finalize().into(),
    })
}

fn map_source_backup_error(error: IngestError) -> IngestError {
    match error {
        IngestError::Catalog(error) => map_market_recovery_catalog_error(error),
        other => other,
    }
}
