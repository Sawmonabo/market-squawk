//! Private augmentation from original economic-date read capabilities, never decoded flags.

use super::*;
use crate::corporate_actions::current_ordinary::{
    CurrentOrdinaryActionFamily, CurrentOrdinaryActionSourceRead,
};

impl CorporateActionSourceCoverage {
    pub(in crate::corporate_actions) fn current_projection_records(
        &self,
    ) -> &[CorporateActionRecord] {
        &self.projection_records
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::corporate_actions) fn try_complete_current(
        mut self,
        reads: &[CurrentOrdinaryActionSourceRead],
        calendar: &RetainedCorporateActionCalendar,
        projection_records: Box<[CorporateActionRecord]>,
        gaps: Vec<OrdinaryActionCoverageGap>,
        reconciled: Vec<ReconciledOrdinaryAction>,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Self, CorporateActionError> {
        let mut audit = AuditBuffer {
            bytes: Vec::new(),
            limit: limits.max_retained_bytes().get(),
            deadline,
            cancellation,
        };
        audit
            .write_all(&self.source_audit)
            .map_err(|_| audit.failure())?;
        let mut roots = self.roots.into_vec();
        for read in reads {
            audit.json(&(
                match read.family() {
                    CurrentOrdinaryActionFamily::Distributions => 0_u8,
                    CurrentOrdinaryActionFamily::Splits => 1,
                },
                read.binding_digest(),
                read.captured_at(),
                read.knowledge_cutoff(),
                read.interval(),
                read.instrument().revision_digest(),
                read.instrument().revision_sequence(),
                read.query_identity(),
            ))?;
            audit
                .write_all(&(read.source_audit().len() as u64).to_be_bytes())
                .map_err(|_| audit.failure())?;
            audit
                .write_all(read.source_audit())
                .map_err(|_| audit.failure())?;
            if !roots.contains(read.manifest()) {
                roots.push(read.manifest().clone());
            }
        }
        for record in &projection_records {
            audit.record(record)?;
        }
        for overlap in &reconciled {
            audit.json(&(
                overlap.instrument,
                overlap.date,
                match overlap.field {
                    OrdinaryActionField::Shares => 0_u8,
                    OrdinaryActionField::Cash => 1,
                },
                overlap.applied_alpaca_record,
                overlap.corroborating_tiingo_record,
            ))?;
        }
        for root in &roots {
            audit.json(&(
                root.dataset_id().as_str(),
                root.manifest_version(),
                root.schema().name(),
                root.schema_version().get(),
                root.schema().fingerprint(),
                root.content_hash().bytes(),
            ))?;
        }
        let first_open = calendar
            .native_session_bounds(self.interval)
            .ok_or(CorporateActionError::InvalidApplication)?
            .0;
        self.application_starts = self
            .instruments
            .iter()
            .map(|id| (*id, first_open))
            .collect();
        self.projection_records = projection_records;
        self.roots = roots.into_boxed_slice();
        self.source_audit = audit.bytes.into_boxed_slice();
        self.ordinary_present = true;
        self.current_us_equity_dates = true;
        let mut all_gaps = self.gaps.into_vec();
        all_gaps.extend(gaps);
        self.gaps = all_gaps.into_boxed_slice();
        let mut all_reconciled = self.reconciled.into_vec();
        all_reconciled.extend(reconciled);
        self.reconciled = all_reconciled.into_boxed_slice();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/source-admitted-current-economic-actions/v1\0");
        hash.update(&self.source_audit);
        self.digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        self.retained_bytes = self.checked_retained_bytes()?;
        super::super::retained::require_retained_limit(
            self.retained_bytes,
            limits.max_retained_bytes().get(),
        )?;
        Ok(self)
    }
}
