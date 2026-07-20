//! Canonical relationship-bearing catalog evidence identity.

use sha2::{Digest as _, Sha256};

use super::{CatalogContentEvidenceDigest, CatalogEvidenceSnapshot, EvidenceError};
use crate::GenerationKind;

pub(super) fn evidence_digest(
    snapshot: &CatalogEvidenceSnapshot,
) -> Result<CatalogContentEvidenceDigest, EvidenceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/analytical-catalog-evidence/v1");
    digest.update(snapshot.request().cutoff().unix_nanos().to_be_bytes());

    let mut artifacts: Vec<_> = snapshot.artifacts().iter().collect();
    artifacts.sort_unstable_by_key(|artifact| artifact.artifact_id());
    section_count(&mut digest, b"artifacts", artifacts.len())?;
    for artifact in artifacts {
        digest.update(artifact.artifact_id().as_bytes());
        digest.update(artifact.run_id().as_bytes());
        text(&mut digest, artifact.relative_reference())?;
        digest.update(artifact.content_hash().bytes());
        digest.update(artifact.size_bytes().to_be_bytes());
    }

    let mut manifests: Vec<_> = snapshot.manifests().iter().collect();
    manifests.sort_unstable_by_key(|manifest| manifest.manifest_id());
    section_count(&mut digest, b"manifests", manifests.len())?;
    for manifest in manifests {
        digest.update(manifest.manifest_id().as_bytes());
        text(&mut digest, manifest.dataset_id().as_str())?;
        digest.update(manifest.schema_version().to_be_bytes());
        digest.update(manifest.artifact_id().as_bytes());
        digest.update(manifest.content_hash().bytes());
    }

    let mut generations: Vec<_> = snapshot.generations().iter().collect();
    generations.sort_unstable_by(|left, right| {
        left.dataset_id()
            .as_str()
            .cmp(right.dataset_id().as_str())
            .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
    });
    section_count(&mut digest, b"generations", generations.len())?;
    for generation in generations {
        text(&mut digest, generation.dataset_id().as_str())?;
        digest.update(generation.manifest_version().to_be_bytes());
        digest.update(generation.content_hash().bytes());
        digest.update(generation.lineage_hash().bytes());
        digest.update(generation.row_count().to_be_bytes());
        digest.update(generation.total_bytes().to_be_bytes());
        digest.update(generation.schema_version().to_be_bytes());
        digest.update(generation.anchor_manifest_id().as_bytes());
        match generation.parent_version() {
            Some(parent) => {
                digest.update([1]);
                digest.update(parent.to_be_bytes());
            }
            None => digest.update([0]),
        }
        digest.update([match generation.kind() {
            GenerationKind::Ingest => 1,
            GenerationKind::Compaction => 2,
        }]);
        section_count(&mut digest, b"objects", generation.objects().len())?;
        for (ordinal, object) in generation.objects().iter().enumerate() {
            digest.update(
                u64::try_from(ordinal)
                    .map_err(|_| EvidenceError::ResourceLimitExceeded)?
                    .to_be_bytes(),
            );
            digest.update(object.artifact_id().as_bytes());
            digest.update(object.content_hash().bytes());
            digest.update(object.row_count().to_be_bytes());
            digest.update(object.size_bytes().to_be_bytes());
            digest.update(object.lineage_hash().bytes());
        }
    }

    let mut query_artifacts: Vec<_> = snapshot.query_artifacts().iter().collect();
    query_artifacts.sort_unstable_by_key(|query| query.reservation_id());
    section_count(&mut digest, b"live-query-artifacts", query_artifacts.len())?;
    for query in query_artifacts {
        digest.update(query.reservation_id().as_bytes());
        text(&mut digest, query.owner().as_str())?;
        digest.update(query.request_hash().bytes());
        digest.update(query.artifact_id().as_bytes());
        text(&mut digest, query.relative_reference())?;
        digest.update(query.content_hash().bytes());
        digest.update(query.size_bytes().to_be_bytes());
        digest.update(query.expires_at().unix_nanos().to_be_bytes());
    }
    CatalogContentEvidenceDigest::try_new(digest.finalize().into())
        .ok_or(EvidenceError::InvalidCatalogEvidence)
}

fn section_count(digest: &mut Sha256, domain: &[u8], count: usize) -> Result<(), EvidenceError> {
    text(
        digest,
        std::str::from_utf8(domain).map_err(|_| EvidenceError::InvalidCatalogEvidence)?,
    )?;
    digest.update(
        u64::try_from(count)
            .map_err(|_| EvidenceError::ResourceLimitExceeded)?
            .to_be_bytes(),
    );
    Ok(())
}

fn text(digest: &mut Sha256, value: &str) -> Result<(), EvidenceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| EvidenceError::ResourceLimitExceeded)?
            .to_be_bytes(),
    );
    digest.update(value.as_bytes());
    Ok(())
}
