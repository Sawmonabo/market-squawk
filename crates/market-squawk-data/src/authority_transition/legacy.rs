//! Canonical evidence for explicit migration of exact version-1 root controls.

use sha2::{Digest as _, Sha256};

use super::{
    AuthorityEvidenceDigest, CatalogEndpointIdentity, LegacyAuthorityRequirement,
    RootEndpointIdentity,
};
use crate::parquet_store::VerifiedLegacyRootAuthority;

pub(super) fn migration_evidence_digest(
    requirement: LegacyAuthorityRequirement,
    catalog_identity: CatalogEndpointIdentity,
    catalog_binding: [u8; 32],
    root_endpoint: RootEndpointIdentity,
    legacy_root: &VerifiedLegacyRootAuthority,
) -> Result<AuthorityEvidenceDigest, super::AuthorityTransitionError> {
    let marker = legacy_root.marker_record();
    let binding = legacy_root.binding_record();
    let marker_digest =
        legacy_control_digest(b"market-squawk/legacy-root-marker-record/v1", marker)?;
    let binding_digest =
        legacy_control_digest(b"market-squawk/legacy-root-binding-record/v1", binding)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/legacy-authority-migration-evidence/v2");
    digest.update(requirement.head().sequence().get().to_be_bytes());
    digest.update(requirement.head().event_digest().bytes());
    digest.update(requirement.evidence_digest().bytes());
    digest.update(requirement.catalog_identity().bytes());
    digest.update(catalog_identity.bytes());
    digest.update(catalog_binding);
    digest.update(root_endpoint.bytes());
    digest.update(legacy_root.stable_root().bytes());
    append_exact_record(&mut digest, marker, marker_digest)?;
    append_exact_record(&mut digest, binding, binding_digest)?;
    AuthorityEvidenceDigest::try_new(digest.finalize().into())
        .ok_or(super::AuthorityTransitionError::InvalidIdentity)
}

fn legacy_control_digest(
    domain: &[u8],
    record: &[u8],
) -> Result<[u8; 32], super::AuthorityTransitionError> {
    let length = u64::try_from(record.len())
        .map_err(|_| super::AuthorityTransitionError::InvalidIdentity)?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(length.to_be_bytes());
    digest.update(record);
    Ok(digest.finalize().into())
}

fn append_exact_record(
    digest: &mut Sha256,
    record: &[u8],
    record_digest: [u8; 32],
) -> Result<(), super::AuthorityTransitionError> {
    let length = u64::try_from(record.len())
        .map_err(|_| super::AuthorityTransitionError::InvalidIdentity)?;
    digest.update(length.to_be_bytes());
    digest.update(record_digest);
    digest.update(record);
    Ok(())
}
