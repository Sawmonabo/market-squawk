//! Bounded immutable fair-value read capabilities.

mod selection;

pub(super) use selection::approval_status_at;
pub use selection::{
    FairValueSelectionDisposition, FairValueSelectionError, FairValueSelectionOrderEntry,
    FairValueSelectionReceipt, FairValueSelectionReceiptHash, FairValueSelectionRequest,
    SelectedFairValueEvidence,
};

use super::*;

impl FairValueService {
    /// Returns the validated service limits used for recovery, retention, and bounded reads.
    pub const fn limits(&self) -> FairValueLimits {
        self.limits
    }

    /// Returns the number of immutable measurements retained by this service image.
    pub fn measurement_count(&self) -> usize {
        self.measurements.len()
    }

    /// Returns one bounded oldest-to-newest page of the retained audit hash chain.
    ///
    /// A cursor names both sequence and event identity, preventing a stale or unrelated chain
    /// position from silently skipping records. The returned continuation always identifies the
    /// last event in this page and is present only while another retained event exists.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::QueryLimitExceeded`] for an invalid limit,
    /// [`FairValueError::InvalidAuditCursor`] for an unknown chain position, or
    /// [`FairValueError::CorruptPersistence`] when retained sequence/link continuity is broken.
    pub fn audit_page(
        &self,
        after: Option<FairValueAuditCursor>,
        limit: usize,
    ) -> Result<FairValueAuditPage, FairValueError> {
        self.validate_query_limit(limit)?;
        let start = match after {
            None => 0,
            Some(cursor) => {
                let index = usize::try_from(
                    cursor
                        .sequence()
                        .checked_sub(1)
                        .ok_or(FairValueError::InvalidAuditCursor)?,
                )
                .map_err(|_| FairValueError::InvalidAuditCursor)?;
                let event = self
                    .audit
                    .get(index)
                    .ok_or(FairValueError::InvalidAuditCursor)?;
                if event.sequence() != cursor.sequence() || event.id() != cursor.event_id() {
                    return Err(FairValueError::InvalidAuditCursor);
                }
                index.checked_add(1).ok_or(FairValueError::Arithmetic)?
            }
        };
        let available_from_cursor = self
            .audit
            .len()
            .checked_sub(start)
            .ok_or(FairValueError::CorruptPersistence)?;
        let end = start
            .checked_add(limit)
            .ok_or(FairValueError::Arithmetic)?
            .min(self.audit.len());
        let events = self.audit[start..end].to_vec();
        let expected_previous = after.map(FairValueAuditCursor::event_id);
        let expected_first_sequence = u64::try_from(start)
            .map_err(|_| FairValueError::Arithmetic)?
            .checked_add(1)
            .ok_or(FairValueError::Arithmetic)?;
        if events.first().is_some_and(|event| {
            event.previous_event_id() != expected_previous
                || event.sequence() != expected_first_sequence
        }) {
            return Err(FairValueError::CorruptPersistence);
        }
        for pair in events.windows(2) {
            let expected_sequence = pair[0]
                .sequence()
                .checked_add(1)
                .ok_or(FairValueError::Arithmetic)?;
            if pair[1].previous_event_id() != Some(pair[0].id())
                || pair[1].sequence() != expected_sequence
            {
                return Err(FairValueError::CorruptPersistence);
            }
        }
        let next_cursor = if end < self.audit.len() {
            events
                .last()
                .map(|event| FairValueAuditCursor::after(event))
                .transpose()?
        } else {
            None
        };
        Ok(FairValueAuditPage {
            events,
            total_count: self.audit.len(),
            available_from_cursor,
            next_cursor,
        })
    }

    /// Returns measurements in deterministic content-identity order under an explicit bound.
    pub fn measurements(
        &self,
        limit: usize,
    ) -> Result<Vec<Arc<ValuationMeasurement>>, FairValueError> {
        self.validate_query_limit(limit)?;
        Ok(self
            .measurements
            .values()
            .take(limit)
            .map(Arc::clone)
            .collect())
    }

    /// Returns the number of immutable decisions linked to one retained measurement.
    pub fn decision_count_for_measurement(
        &self,
        measurement_id: MeasurementId,
    ) -> Result<usize, FairValueError> {
        self.require_measurement(measurement_id)?;
        Ok(self
            .decisions
            .values()
            .filter(|decision| decision.measurement_id() == measurement_id)
            .count())
    }

    /// Returns decisions for one measurement in deterministic identity order.
    pub fn decisions_for_measurement(
        &self,
        measurement_id: MeasurementId,
        limit: usize,
    ) -> Result<Vec<Arc<ClassificationDecision>>, FairValueError> {
        self.validate_query_limit(limit)?;
        self.require_measurement(measurement_id)?;
        Ok(self
            .decisions
            .values()
            .filter(|decision| decision.measurement_id() == measurement_id)
            .take(limit)
            .map(Arc::clone)
            .collect())
    }

    /// Returns the unique rules decision for one measurement and exact ruleset, when retained.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::CorruptPersistence`] if durable state contains more than one
    /// rules decision for the same measurement and ruleset identity.
    pub fn rules_decision_for_measurement(
        &self,
        measurement_id: MeasurementId,
        ruleset_hash: crate::RulesetHash,
    ) -> Result<Option<Arc<ClassificationDecision>>, FairValueError> {
        self.require_measurement(measurement_id)?;
        let mut matching = self.decisions.values().filter(|decision| {
            decision.measurement_id() == measurement_id
                && decision.ruleset_hash() == ruleset_hash
                && decision.basis() == DecisionBasis::Rules
        });
        let first = matching.next().map(Arc::clone);
        if matching.next().is_some() {
            return Err(FairValueError::CorruptPersistence);
        }
        Ok(first)
    }

    /// Returns the number of immutable approvals linked to one retained measurement.
    pub fn approval_count_for_measurement(
        &self,
        measurement_id: MeasurementId,
    ) -> Result<usize, FairValueError> {
        self.require_measurement(measurement_id)?;
        Ok(self
            .approvals
            .values()
            .filter(|approval| approval.measurement_id() == measurement_id)
            .count())
    }

    /// Returns approvals for one measurement in deterministic identity order.
    pub fn approvals_for_measurement(
        &self,
        measurement_id: MeasurementId,
        limit: usize,
    ) -> Result<Vec<Arc<ValuationApproval>>, FairValueError> {
        self.validate_query_limit(limit)?;
        self.require_measurement(measurement_id)?;
        Ok(self
            .approvals
            .values()
            .filter(|approval| approval.measurement_id() == measurement_id)
            .take(limit)
            .map(Arc::clone)
            .collect())
    }

    /// Selects the latest exact-instrument, currency-compatible, approved fair-value evidence.
    ///
    /// Business times and catalog-trusted append times are both evaluated through
    /// `request.as_of()`. Ordering is measurement time descending, preparation time descending,
    /// stable measurement and decision identities, then approval time descending and stable
    /// approval identity. Co-leading measurements or multiple active decisions for the leading
    /// measurement return an explicit conflict rather than arbitrary accounting authority.
    ///
    /// # Errors
    ///
    /// Returns a typed limit, temporary-capacity, or corrupt-persistence failure.
    pub fn select_latest_fair_value(
        &self,
        request: FairValueSelectionRequest,
    ) -> Result<FairValueSelectionReceipt, FairValueSelectionError> {
        self.validate_query_limit(request.max_eligible())?;
        selection::select_latest_from_retained(
            &self.measurements,
            &self.decisions,
            &self.overrides,
            &self.approvals,
            &self.revocations,
            &self.audit,
            request,
        )
    }

    fn require_measurement(
        &self,
        measurement_id: MeasurementId,
    ) -> Result<&Arc<ValuationMeasurement>, FairValueError> {
        self.measurements
            .get(&measurement_id)
            .ok_or(FairValueError::MeasurementNotFound)
    }
}
