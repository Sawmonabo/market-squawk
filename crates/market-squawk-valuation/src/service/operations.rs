//! Durable workflow operations and bounded public queries.

use super::memory::{incremental_index_bytes, recovered_retained_bytes};
use super::recovery::recover_audit;
use super::*;

impl<'catalog> FairValueService<'catalog> {
    /// Opens and semantically reconstructs complete fair-value state from the local catalog.
    ///
    /// # Errors
    ///
    /// Fails closed on bounded recovery, canonical decode, identity recomputation, audit-chain,
    /// relationship, configured-family, or retained-memory violations.
    pub fn open(
        catalog: &'catalog CatalogAuthority,
        limits: FairValueLimits,
    ) -> Result<Self, FairValueError> {
        let snapshot = catalog
            .fair_value_snapshot(limits.catalog_limits)
            .map_err(|_| FairValueError::Persistence)?;
        let recovered = persistence::recover(&snapshot)?;
        let record_ids = snapshot
            .records()
            .iter()
            .map(|record| (record.kind(), record.id()))
            .collect::<BTreeSet<_>>();
        let operation_ids = snapshot
            .audit()
            .iter()
            .map(FairValueCatalogAuditEvent::operation_id)
            .collect::<BTreeSet<_>>();
        let usage = CatalogUsage {
            records: snapshot.records().len(),
            operations: snapshot.audit().len(),
            memberships: snapshot.membership_count(),
            links: snapshot.link_count(),
        };
        let position = snapshot.position();
        let audit = recover_audit(&snapshot, &recovered)?;
        let retained_bytes =
            recovered_retained_bytes(&recovered, &audit, record_ids.len(), operation_ids.len())?;
        let service = Self {
            catalog,
            limits,
            measurements: recovered.measurements,
            decisions: recovered.decisions,
            overrides: recovered.overrides,
            approvals: recovered.approvals,
            revocations: recovered.revocations,
            market_access: recovered.market_access,
            audit,
            record_ids,
            operation_ids,
            position,
            usage,
            retained_bytes,
        };
        service.validate_recovered_limits()?;
        Ok(service)
    }

    /// Classifies and durably retains one immutable measurement and rules decision.
    pub fn classify(
        &mut self,
        measurement: ValuationMeasurement,
        ruleset: ClassificationRuleset,
    ) -> Result<Arc<ClassificationDecision>, FairValueError> {
        if measurement.inputs().len() > self.limits.max_inputs_per_measurement {
            return Err(FairValueError::LimitExceeded {
                resource: "measurement inputs",
                observed: measurement.inputs().len(),
                limit: self.limits.max_inputs_per_measurement,
            });
        }
        for input in measurement.inputs() {
            if let Some(access) = input.market_access_assessment()
                && self.market_access.get(&access.id()).map(AsRef::as_ref) != Some(access)
            {
                return Err(FairValueError::InvalidMarketAccessAssessment);
            }
        }
        let decision = Arc::new(ruleset.classify(&measurement)?);
        if let Some(existing) = self.decisions.get(&decision.id()) {
            return Ok(Arc::clone(existing));
        }
        let new_measurement = !self.measurements.contains_key(&measurement.id());
        self.ensure_family_capacity("decisions", self.decisions.len(), 1)?;
        if new_measurement {
            self.ensure_count(
                "measurements",
                self.measurements.len(),
                1,
                self.limits.max_measurements,
            )?;
        }
        let operation = persistence::classify_operation(&measurement, &decision, &ruleset)?;
        let draft = AuditDraft::try_new(
            AuditEventKind::Classified {
                measurement_id: measurement.id(),
                decision_id: decision.id(),
            },
            measurement.prepared_by().clone(),
            measurement.prepared_at(),
        )?;
        let domain_bytes = checked_add(
            decision.retained_bytes(),
            if new_measurement {
                measurement.retained_bytes()
            } else {
                0
            },
        )?;
        self.persist(
            operation,
            draft,
            domain_bytes,
            1 + usize::from(new_measurement),
        )?;
        if new_measurement {
            self.measurements
                .insert(measurement.id(), Arc::new(measurement));
        }
        self.decisions.insert(decision.id(), Arc::clone(&decision));
        Ok(decision)
    }

    /// Creates and durably retains a governed non-Level-1 override and replacement decision.
    #[allow(clippy::too_many_arguments)]
    pub fn propose_override(
        &mut self,
        base_decision_id: DecisionId,
        requested_hierarchy: FairValueHierarchy,
        justification: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<OverrideProposal, FairValueError> {
        let base = self
            .decisions
            .get(&base_decision_id)
            .cloned()
            .ok_or(FairValueError::DecisionNotFound)?;
        let measurement = self
            .measurements
            .get(&base.measurement_id())
            .ok_or(FairValueError::MeasurementNotFound)?;
        if prepared_at < measurement.prepared_at() {
            return Err(FairValueError::InvalidTime);
        }
        let valuation_override = Arc::new(ValuationOverride::try_new(
            &base,
            requested_hierarchy,
            justification,
            prepared_by,
            prepared_at,
            expires_at,
        )?);
        let decision = Arc::new(ClassificationDecision::overridden(
            &base,
            valuation_override.id(),
            requested_hierarchy,
        )?);
        if let (Some(existing_override), Some(existing_decision)) = (
            self.overrides.get(&valuation_override.id()),
            self.decisions.get(&decision.id()),
        ) {
            return Ok(OverrideProposal::new(
                Arc::clone(existing_override),
                Arc::clone(existing_decision),
            ));
        }
        self.ensure_family_capacity("overrides", self.overrides.len(), 1)?;
        self.ensure_family_capacity("decisions", self.decisions.len(), 1)?;
        let operation = persistence::override_operation(&valuation_override, &decision)?;
        let draft = AuditDraft::try_new(
            AuditEventKind::OverrideProposed {
                override_id: valuation_override.id(),
                decision_id: decision.id(),
            },
            valuation_override.prepared_by().clone(),
            valuation_override.prepared_at(),
        )?;
        let domain_bytes = checked_add(
            valuation_override.retained_bytes(),
            decision.retained_bytes(),
        )?;
        self.persist(operation, draft, domain_bytes, 2)?;
        self.overrides
            .insert(valuation_override.id(), Arc::clone(&valuation_override));
        self.decisions.insert(decision.id(), Arc::clone(&decision));
        Ok(OverrideProposal::new(valuation_override, decision))
    }

    /// Independently approves one exact rules or override decision.
    pub fn approve(
        &mut self,
        decision_id: DecisionId,
        approved_by: ActorId,
        approved_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Arc<ValuationApproval>, FairValueError> {
        let decision = self
            .decisions
            .get(&decision_id)
            .cloned()
            .ok_or(FairValueError::DecisionNotFound)?;
        let measurement = self
            .measurements
            .get(&decision.measurement_id())
            .ok_or(FairValueError::MeasurementNotFound)?;
        if approved_at < measurement.prepared_at() {
            return Err(FairValueError::InvalidApprovalWindow);
        }
        if measurement.prepared_by() == &approved_by {
            return Err(FairValueError::SeparationOfDuties);
        }
        let override_id = match decision.basis() {
            DecisionBasis::Rules => None,
            DecisionBasis::Override { override_id, .. } => {
                let valuation_override = self
                    .overrides
                    .get(&override_id)
                    .ok_or(FairValueError::InvalidOverride)?;
                if valuation_override.prepared_by() == &approved_by {
                    return Err(FairValueError::SeparationOfDuties);
                }
                if approved_at < valuation_override.prepared_at()
                    || expires_at > valuation_override.expires_at()
                {
                    return Err(FairValueError::InvalidApprovalWindow);
                }
                Some(override_id)
            }
        };
        let approval = Arc::new(ValuationApproval::try_new(
            &decision,
            override_id,
            approved_by,
            approved_at,
            expires_at,
        )?);
        if let Some(existing) = self.approvals.get(&approval.id()) {
            return Ok(Arc::clone(existing));
        }
        self.ensure_family_capacity("approvals", self.approvals.len(), 1)?;
        let operation = persistence::approval_operation(&approval)?;
        let draft = AuditDraft::try_new(
            AuditEventKind::Approved {
                approval_id: approval.id(),
                decision_id,
            },
            approval.approved_by().clone(),
            approval.approved_at(),
        )?;
        self.persist(operation, draft, approval.retained_bytes(), 1)?;
        self.approvals.insert(approval.id(), Arc::clone(&approval));
        Ok(approval)
    }

    /// Appends an immutable revocation without mutating the approval.
    pub fn revoke_approval(
        &mut self,
        approval_id: ValuationApprovalId,
        revoked_by: ActorId,
        revoked_at: Timestamp,
        reason: &str,
    ) -> Result<Arc<ApprovalRevocation>, FairValueError> {
        let approval = self
            .approvals
            .get(&approval_id)
            .cloned()
            .ok_or(FairValueError::ApprovalNotFound)?;
        let revocation = Arc::new(ApprovalRevocation::try_new(
            &approval, revoked_by, revoked_at, reason,
        )?);
        if let Some(existing) = self.revocations.get(&approval_id) {
            return if existing.id() == revocation.id() {
                Ok(Arc::clone(existing))
            } else {
                Err(FairValueError::AlreadyRevoked)
            };
        }
        self.ensure_family_capacity("revocations", self.revocations.len(), 1)?;
        let operation = persistence::revocation_operation(&revocation)?;
        let draft = AuditDraft::try_new(
            AuditEventKind::Revoked {
                revocation_id: revocation.id(),
                approval_id,
            },
            revocation.revoked_by().clone(),
            revocation.revoked_at(),
        )?;
        self.persist(operation, draft, revocation.retained_bytes(), 1)?;
        self.revocations
            .insert(approval_id, Arc::clone(&revocation));
        Ok(revocation)
    }

    /// Creates and durably records a dual-approved reporting-entity market-access conclusion.
    #[allow(clippy::too_many_arguments)]
    pub fn approve_market_access(
        &mut self,
        account_id: AccountId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        conclusion: MarketAccess,
        effective_from: Timestamp,
        effective_until: Timestamp,
        rationale: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        approved_by: ActorId,
        approved_at: Timestamp,
    ) -> Result<Arc<ApprovedMarketAccess>, FairValueError> {
        let access = Arc::new(ApprovedMarketAccess::try_new(
            account_id,
            venue_id,
            instrument_id,
            conclusion,
            effective_from,
            effective_until,
            rationale,
            prepared_by,
            prepared_at,
            approved_by,
            approved_at,
        )?);
        if let Some(existing) = self.market_access.get(&access.id()) {
            return Ok(Arc::clone(existing));
        }
        self.ensure_family_capacity("market access", self.market_access.len(), 1)?;
        let operation = persistence::market_access_operation(&access)?;
        let draft = AuditDraft::try_new(
            AuditEventKind::MarketAccessApproved {
                assessment_id: access.id(),
            },
            access.approved_by().clone(),
            access.approved_at(),
        )?;
        self.persist(operation, draft, access.retained_bytes(), 1)?;
        self.market_access.insert(access.id(), Arc::clone(&access));
        Ok(access)
    }

    /// Evaluates immutable approval and revocation times at one query instant.
    pub fn approval_status(
        &self,
        approval_id: ValuationApprovalId,
        at: Timestamp,
    ) -> Result<ApprovalStatus, FairValueError> {
        let approval = self
            .approvals
            .get(&approval_id)
            .ok_or(FairValueError::ApprovalNotFound)?;
        if at < approval.approved_at() {
            return Ok(ApprovalStatus::NotYetEffective);
        }
        if self
            .revocations
            .get(&approval_id)
            .is_some_and(|revocation| revocation.revoked_at() <= at)
        {
            return Ok(ApprovalStatus::Revoked);
        }
        if at > approval.expires_at() {
            Ok(ApprovalStatus::Expired)
        } else {
            Ok(ApprovalStatus::Active)
        }
    }

    /// Returns one immutable measurement by content identity.
    pub fn measurement(&self, id: MeasurementId) -> Option<Arc<ValuationMeasurement>> {
        self.measurements.get(&id).map(Arc::clone)
    }

    /// Returns one immutable classification decision by content identity.
    pub fn decision(&self, id: DecisionId) -> Option<Arc<ClassificationDecision>> {
        self.decisions.get(&id).map(Arc::clone)
    }

    /// Returns one immutable override by content identity.
    pub fn valuation_override(&self, id: OverrideId) -> Option<Arc<ValuationOverride>> {
        self.overrides.get(&id).map(Arc::clone)
    }

    /// Returns one immutable approval by content identity.
    pub fn approval(&self, id: ValuationApprovalId) -> Option<Arc<ValuationApproval>> {
        self.approvals.get(&id).map(Arc::clone)
    }

    /// Returns an immutable revocation for an approval when one exists.
    pub fn revocation(&self, approval_id: ValuationApprovalId) -> Option<Arc<ApprovalRevocation>> {
        self.revocations.get(&approval_id).map(Arc::clone)
    }

    /// Returns one independently approved market-access assessment.
    pub fn market_access(&self, id: MarketAccessAssessmentId) -> Option<Arc<ApprovedMarketAccess>> {
        self.market_access.get(&id).map(Arc::clone)
    }

    /// Returns decisions for one instrument in deterministic ID order under an explicit bound.
    pub fn decisions_for_instrument(
        &self,
        instrument_id: InstrumentId,
        limit: usize,
    ) -> Result<Vec<Arc<ClassificationDecision>>, FairValueError> {
        self.validate_query_limit(limit)?;
        Ok(self
            .decisions
            .values()
            .filter(|decision| {
                self.measurements
                    .get(&decision.measurement_id())
                    .is_some_and(|measurement| measurement.instrument_id() == instrument_id)
            })
            .take(limit)
            .map(Arc::clone)
            .collect())
    }

    /// Returns oldest-to-newest catalog audit events under an explicit bound.
    pub fn audit_events(
        &self,
        limit: usize,
    ) -> Result<Vec<Arc<FairValueAuditEvent>>, FairValueError> {
        self.validate_query_limit(limit)?;
        Ok(self.audit.iter().take(limit).map(Arc::clone).collect())
    }

    /// Returns estimated bytes retained by immutable service state.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn persist(
        &mut self,
        operation: FairValueCatalogOperation,
        draft: AuditDraft,
        domain_bytes: usize,
        domain_index_entries: usize,
    ) -> Result<(), FairValueError> {
        if self.operation_ids.contains(&operation.id()) {
            return Err(FairValueError::CorruptPersistence);
        }
        let projected = self.projected_usage(&operation)?;
        let new_record_ids = projected
            .records
            .checked_sub(self.usage.records)
            .ok_or(FairValueError::Arithmetic)?;
        let index_bytes = incremental_index_bytes(domain_index_entries, new_record_ids)?;
        let added = checked_add(
            checked_add(domain_bytes, draft.retained_bytes)?,
            index_bytes,
        )?;
        self.ensure_retained_bytes(added)?;
        let previous_event_id = self.audit.last().map(|event| event.id());
        let expected_sequence = u64::try_from(self.usage.operations)
            .map_err(|_| FairValueError::Arithmetic)?
            .checked_add(1)
            .ok_or(FairValueError::Arithmetic)?;
        let commit = self
            .catalog
            .append_fair_value_operation(&operation, self.limits.catalog_limits, self.position)
            .map_err(|_| FairValueError::Persistence)?;
        if commit.disposition() != FairValueCommitDisposition::Inserted
            || commit.audit_sequence() != expected_sequence
            || commit.record_count() != projected.records
            || commit.operation_count() != projected.operations
            || commit.membership_count() != projected.memberships
            || commit.link_count() != projected.links
        {
            return Err(FairValueError::CorruptPersistence);
        }
        let event = draft.finish(commit, previous_event_id);
        for identity in operation.record_identities() {
            self.record_ids.insert(identity);
        }
        self.operation_ids.insert(operation.id());
        self.position = commit.position();
        self.usage = projected;
        self.audit.push(Arc::new(event));
        self.retained_bytes = checked_add(self.retained_bytes, added)?;
        Ok(())
    }

    fn projected_usage(
        &self,
        operation: &FairValueCatalogOperation,
    ) -> Result<CatalogUsage, FairValueError> {
        let new_records = operation
            .record_identities()
            .filter(|identity| !self.record_ids.contains(identity))
            .count();
        let projected = CatalogUsage {
            records: checked_add(self.usage.records, new_records)?,
            operations: checked_add(self.usage.operations, 1)?,
            memberships: checked_add(self.usage.memberships, operation.record_count())?,
            links: checked_add(self.usage.links, operation.link_count())?,
        };
        let limits = self.limits.catalog_limits;
        if projected.records > limits.max_records()
            || projected.operations > limits.max_operations()
            || projected.memberships > limits.max_memberships()
            || projected.links > limits.max_links()
        {
            return Err(FairValueError::LimitExceeded {
                resource: "recoverable catalog footprint",
                observed: projected
                    .records
                    .max(projected.operations)
                    .max(projected.memberships)
                    .max(projected.links),
                limit: limits
                    .max_records()
                    .max(limits.max_operations())
                    .max(limits.max_memberships())
                    .max(limits.max_links()),
            });
        }
        Ok(projected)
    }

    fn validate_recovered_limits(&self) -> Result<(), FairValueError> {
        let families = [
            (
                "measurements",
                self.measurements.len(),
                self.limits.max_measurements,
            ),
            (
                "decisions",
                self.decisions.len(),
                self.limits.max_records_per_family,
            ),
            (
                "overrides",
                self.overrides.len(),
                self.limits.max_records_per_family,
            ),
            (
                "approvals",
                self.approvals.len(),
                self.limits.max_records_per_family,
            ),
            (
                "revocations",
                self.revocations.len(),
                self.limits.max_records_per_family,
            ),
            (
                "market access",
                self.market_access.len(),
                self.limits.max_records_per_family,
            ),
        ];
        if let Some((resource, observed, limit)) = families
            .into_iter()
            .find(|(_, observed, limit)| observed > limit)
        {
            return Err(FairValueError::LimitExceeded {
                resource,
                observed,
                limit,
            });
        }
        if self
            .measurements
            .values()
            .any(|measurement| measurement.inputs().len() > self.limits.max_inputs_per_measurement)
        {
            return Err(FairValueError::LimitExceeded {
                resource: "measurement inputs",
                observed: self
                    .measurements
                    .values()
                    .map(|measurement| measurement.inputs().len())
                    .max()
                    .unwrap_or_default(),
                limit: self.limits.max_inputs_per_measurement,
            });
        }
        if self.retained_bytes > self.limits.max_retained_bytes {
            return Err(FairValueError::RetainedBytesExceeded {
                observed: self.retained_bytes,
                limit: self.limits.max_retained_bytes,
            });
        }
        Ok(())
    }

    fn validate_query_limit(&self, requested: usize) -> Result<(), FairValueError> {
        if requested == 0 || requested > self.limits.max_query_results {
            Err(FairValueError::QueryLimitExceeded {
                requested,
                limit: self.limits.max_query_results,
            })
        } else {
            Ok(())
        }
    }

    fn ensure_family_capacity(
        &self,
        resource: &'static str,
        current: usize,
        added: usize,
    ) -> Result<(), FairValueError> {
        self.ensure_count(resource, current, added, self.limits.max_records_per_family)
    }

    fn ensure_count(
        &self,
        resource: &'static str,
        current: usize,
        added: usize,
        limit: usize,
    ) -> Result<(), FairValueError> {
        let observed = checked_add(current, added)?;
        if observed > limit {
            Err(FairValueError::LimitExceeded {
                resource,
                observed,
                limit,
            })
        } else {
            Ok(())
        }
    }

    fn ensure_retained_bytes(&self, added: usize) -> Result<(), FairValueError> {
        let observed = checked_add(self.retained_bytes, added)?;
        if observed > self.limits.max_retained_bytes {
            Err(FairValueError::RetainedBytesExceeded {
                observed,
                limit: self.limits.max_retained_bytes,
            })
        } else {
            Ok(())
        }
    }
}
