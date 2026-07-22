//! Canonical operation and audit-chain hashing.

use super::*;

pub(super) fn operation_digest(
    kind: FairValueOperationKind,
    actor: &str,
    business_at: Timestamp,
    records: &[FairValueCatalogRecord],
    links: &[FairValueCatalogLink],
) -> Result<[u8; 32], CatalogError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/fair-value-catalog-operation/v1");
    hash.update(kind.tag().to_be_bytes());
    hash_bytes(&mut hash, actor.as_bytes())?;
    hash.update(business_at.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(records.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    for record in records {
        hash.update(record.kind.tag().to_be_bytes());
        hash.update(record.id);
        hash.update(record.payload_sha256);
    }
    hash.update(
        u64::try_from(links.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    for link in links {
        hash.update(link.source_kind.tag().to_be_bytes());
        hash.update(link.source_id);
        hash.update(link.relation.tag().to_be_bytes());
        hash.update(link.target_kind.tag().to_be_bytes());
        hash.update(link.target_id);
    }
    Ok(hash.finalize().into())
}

pub(super) fn audit_digest(
    sequence: u64,
    previous: Option<[u8; 32]>,
    operation_id: [u8; 32],
    kind: FairValueOperationKind,
    actor: &str,
    business_at: Timestamp,
    appended_at: Timestamp,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/fair-value-catalog-audit/v1");
    hash.update(sequence.to_be_bytes());
    match previous {
        Some(value) => {
            hash.update([1]);
            hash.update(value);
        }
        None => hash.update([0]),
    }
    hash.update(operation_id);
    hash.update(kind.tag().to_be_bytes());
    hash.update((actor.len() as u64).to_be_bytes());
    hash.update(actor.as_bytes());
    hash.update(business_at.unix_nanos().to_be_bytes());
    hash.update(appended_at.unix_nanos().to_be_bytes());
    hash.finalize().into()
}

pub(super) fn hash_bytes(hash: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

pub(super) fn fixed_digest(value: Vec<u8>) -> Result<[u8; 32], CatalogError> {
    value.try_into().map_err(|_| CatalogError::CorruptCatalog)
}

pub(super) fn count_exceeds(value: i64, limit: usize) -> bool {
    value < 0 || usize::try_from(value).map_or(true, |value| value > limit)
}
