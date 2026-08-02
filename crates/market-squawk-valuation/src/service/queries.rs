//! Bounded immutable fair-value read capabilities.

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

    fn require_measurement(
        &self,
        measurement_id: MeasurementId,
    ) -> Result<&Arc<ValuationMeasurement>, FairValueError> {
        self.measurements
            .get(&measurement_id)
            .ok_or(FairValueError::MeasurementNotFound)
    }
}
