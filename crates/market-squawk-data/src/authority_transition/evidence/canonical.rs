//! Streaming encoder of the unchanged relationship-bearing domain-v4 catalog identity.
use super::catalog::*;
use super::{CatalogContentEvidenceDigest, EvidenceError};
use crate::GenerationKind;
use crate::manifest::GenerationParentRelation;
use market_squawk_domain::Timestamp;
use sha2::{Digest as _, Sha256};

pub(crate) struct EvidenceDigest {
    digest: Sha256,
}
impl EvidenceDigest {
    pub(crate) fn new(cutoff: Timestamp) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/analytical-catalog-evidence/v4");
        digest.update(cutoff.unix_nanos().to_be_bytes());
        Self { digest }
    }
    pub(crate) fn section(&mut self, domain: &str, count: u64) -> Result<(), EvidenceError> {
        text(&mut self.digest, domain)?;
        self.digest.update(count.to_be_bytes());
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<CatalogContentEvidenceDigest, EvidenceError> {
        CatalogContentEvidenceDigest::try_new(self.digest.finalize().into())
            .ok_or(EvidenceError::InvalidCatalogEvidence)
    }
    pub(crate) fn artifact(&mut self, artifact: &ArtifactEvidenceRow) -> Result<(), EvidenceError> {
        self.digest.update(artifact.artifact_id().as_bytes());
        self.digest.update(artifact.run_id().as_bytes());
        self.digest
            .update(artifact.publication_ordinal().to_be_bytes());
        text(&mut self.digest, artifact.relative_reference())?;
        self.digest.update(artifact.content_hash().bytes());
        self.digest.update(artifact.size_bytes().to_be_bytes());
        Ok(())
    }

    pub(crate) fn manifest(&mut self, manifest: &ManifestEvidenceRow) -> Result<(), EvidenceError> {
        self.digest.update(manifest.manifest_id().as_bytes());
        text(&mut self.digest, manifest.dataset_id().as_str())?;
        self.digest.update(manifest.schema_version().to_be_bytes());
        self.digest.update(manifest.artifact_id().as_bytes());
        self.digest.update(manifest.content_hash().bytes());
        Ok(())
    }

    pub(crate) fn query_artifact(
        &mut self,
        query: &QueryArtifactEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.digest.update(query.reservation_id().as_bytes());
        text(&mut self.digest, query.owner().as_str())?;
        self.digest.update(query.request_hash().bytes());
        self.digest.update(query.artifact_id().as_bytes());
        text(&mut self.digest, query.relative_reference())?;
        self.digest.update(query.content_hash().bytes());
        self.digest.update(query.size_bytes().to_be_bytes());
        self.digest
            .update(query.expires_at().unix_nanos().to_be_bytes());
        Ok(())
    }

    pub(crate) fn archive(
        &mut self,
        archive: &MarketEventArchiveEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.digest.update(archive.content_hash().bytes());
        text(&mut self.digest, archive.relative_reference())?;
        text(&mut self.digest, archive.schema().name())?;
        self.digest
            .update(archive.schema().version().get().to_be_bytes());
        self.digest.update(archive.schema().fingerprint());
        self.digest.update(archive.size_bytes().to_be_bytes());
        self.digest.update(archive.row_count().to_be_bytes());
        self.digest
            .update(archive.created_at().unix_nanos().to_be_bytes());
        self.digest
            .update(archive.published_at().unix_nanos().to_be_bytes());
        Ok(())
    }

    pub(crate) fn provider_relation(
        &mut self,
        row: &ProviderCatalogRelationEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.digest.update([row.relation().canonical_tag()]);
        text(&mut self.digest, row.relation().database_name())?;
        bytes(&mut self.digest, row.primary_key())?;
        self.digest.update(row.row_content_digest().bytes());
        Ok(())
    }

    pub(crate) fn generation_header(
        &mut self,
        generation: &GenerationEvidenceHeader,
    ) -> Result<(), EvidenceError> {
        self.digest
            .update(generation.generation_sequence().to_be_bytes());
        text(&mut self.digest, generation.dataset_id().as_str())?;
        self.digest
            .update(generation.manifest_version().to_be_bytes());
        self.digest.update(generation.content_hash().bytes());
        self.digest.update(generation.lineage_hash().bytes());
        self.digest.update(generation.row_count().to_be_bytes());
        self.digest.update(generation.total_bytes().to_be_bytes());
        text(&mut self.digest, generation.schema().name())?;
        self.digest
            .update(generation.schema().version().get().to_be_bytes());
        self.digest.update(generation.schema().fingerprint());
        self.digest
            .update(generation.anchor_manifest_id().as_bytes());
        match generation.build_spec_digest() {
            Some(build_spec) => {
                self.digest.update([1]);
                self.digest.update(build_spec.digest().bytes());
            }
            None => self.digest.update([0]),
        }
        self.digest.update([match generation.kind() {
            GenerationKind::Ingest => 1,
            GenerationKind::Compaction => 2,
            GenerationKind::Derived => 3,
        }]);
        Ok(())
    }

    pub(crate) fn parent(
        &mut self,
        ordinal: u64,
        edge: &GenerationParentEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.digest.update(ordinal.to_be_bytes());
        self.digest.update(edge.generation_sequence().to_be_bytes());
        self.digest.update([match edge.parent().relation() {
            GenerationParentRelation::AppendPredecessor => 1,
            GenerationParentRelation::CompactionPredecessor => 2,
            GenerationParentRelation::DerivedInput => 3,
        }]);
        let parent = edge.parent().manifest();
        text(&mut self.digest, parent.dataset_id().as_str())?;
        self.digest.update(parent.manifest_version().to_be_bytes());
        text(&mut self.digest, parent.schema().name())?;
        self.digest
            .update(parent.schema().version().get().to_be_bytes());
        self.digest.update(parent.schema().fingerprint());
        self.digest.update(parent.content_hash().bytes());
        Ok(())
    }

    pub(crate) fn object(
        &mut self,
        ordinal: u64,
        object: &GenerationObjectEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.digest.update(ordinal.to_be_bytes());
        self.digest.update(object.artifact_id().as_bytes());
        self.digest.update(object.content_hash().bytes());
        self.digest.update(object.row_count().to_be_bytes());
        self.digest.update(object.size_bytes().to_be_bytes());
        self.digest.update(object.lineage_hash().bytes());
        Ok(())
    }
}
fn text(digest: &mut Sha256, value: &str) -> Result<(), EvidenceError> {
    bytes(digest, value.as_bytes())
}

fn bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), EvidenceError> {
    digest.update(
        u64::try_from(value.len())
            .map_err(|_| EvidenceError::ResourceLimitExceeded)?
            .to_be_bytes(),
    );
    digest.update(value);
    Ok(())
}
