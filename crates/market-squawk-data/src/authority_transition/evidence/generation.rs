//! Constant-space replay of the existing ManifestPlan hash and lineage encodings.
use super::{EvidenceError, GenerationEvidenceHeader, GenerationObjectEvidenceRow};
use crate::Sha256Digest;
use sha2::{Digest as _, Sha256};

pub(crate) struct GenerationPlanEvidence {
    plan: Sha256,
    lineage: Sha256,
    first_lineage: Option<Sha256Digest>,
    objects: u64,
    rows: u64,
    bytes: u64,
}
impl GenerationPlanEvidence {
    pub(crate) fn new(header: &GenerationEvidenceHeader) -> Result<Self, EvidenceError> {
        let mut plan = Sha256::new();
        plan.update(b"market-squawk/manifest-plan/v1");
        plan.update(
            u64::try_from(header.dataset_id.as_str().len())
                .map_err(|_| EvidenceError::ResourceLimitExceeded)?
                .to_be_bytes(),
        );
        plan.update(header.dataset_id.as_str().as_bytes());
        plan.update(header.row_count.to_be_bytes());
        plan.update(header.total_bytes.to_be_bytes());
        plan.update(header.lineage_hash.bytes());
        let mut lineage = Sha256::new();
        lineage.update(b"market-squawk/analytical-lineage/v1");
        Ok(Self {
            plan,
            lineage,
            first_lineage: None,
            objects: 0,
            rows: 0,
            bytes: 0,
        })
    }
    pub(crate) fn object(
        &mut self,
        object: &GenerationObjectEvidenceRow,
    ) -> Result<(), EvidenceError> {
        self.objects = self
            .objects
            .checked_add(1)
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        self.rows = self
            .rows
            .checked_add(object.row_count())
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(object.size_bytes())
            .ok_or(EvidenceError::ResourceLimitExceeded)?;
        self.first_lineage.get_or_insert(object.lineage_hash());
        self.lineage.update(object.lineage_hash().bytes());
        self.plan.update(object.content_hash().bytes());
        self.plan.update(object.row_count().to_be_bytes());
        self.plan.update(object.size_bytes().to_be_bytes());
        self.plan.update(object.lineage_hash().bytes());
        Ok(())
    }
    pub(crate) fn finish(
        self,
        header: &GenerationEvidenceHeader,
        expected_objects: u64,
    ) -> Result<(), EvidenceError> {
        let lineage = if self.objects == 1 {
            self.first_lineage
                .ok_or(EvidenceError::GenerationSemanticMismatch)?
        } else {
            Sha256Digest::new(self.lineage.finalize().into())
        };
        if self.objects == 0
            || self.objects != expected_objects
            || self.rows != header.row_count
            || self.bytes != header.total_bytes
            || lineage != header.lineage_hash
            || Sha256Digest::new(self.plan.finalize().into()) != header.content_hash
        {
            Err(EvidenceError::GenerationSemanticMismatch)
        } else {
            Ok(())
        }
    }
}
