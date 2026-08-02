//! Evidence-resolving fair-value application service.

use std::{collections::HashSet, fmt, str::FromStr, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_data::DatasetId;
use market_squawk_domain::{
    AccountId, Currency, FairValueHierarchy, InstrumentId, Money, Timestamp, VenueId,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_valuation::{
    ActorId, ApprovalStatus, ApprovedMarketAccess, AuditEventId, AuditEventKind,
    ClassificationRuleset, DecisionBasis, DecisionId, EvidenceOrigin, FairValueAuditCursor,
    FairValueAuditEvent, FairValueError, FairValueEvidenceHash, FairValueService,
    InputSignificance, MarketAccess, MarketAccessAssessmentId, MarketPriceSelection, MeasurementId,
    OverrideProposal, RulesetHash, ValuationAmount, ValuationApprovalId, ValuationInput,
    ValuationMeasurement, ValuationMeasurementSpec, ValuationMethod,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use tokio_util::sync::CancellationToken;

use super::{
    ApplicationDomainService,
    domain_support::{DomainLifecycle, admitted_result_limits, ensure_request_live},
};

mod resolver;
mod serialization;

pub use resolver::{
    AnalyticsFairValueInputPublisher, FairValueInputAuthorityError,
    FairValueInputAuthorityLimitInput, FairValueInputAuthorityLimits, FairValueReceiptReference,
    FairValueReceiptRegistration, LiveFairValueInputPublisher, PortfolioFairValueInputPublisher,
    ProductionFairValueInputAuthority, ProductionFairValueInputResolver,
    ResearchFairValueInputPublisher,
};
use serialization::{
    approval_value, classification_value, evidence_value, explanation_reason_value,
    market_access_assessment_value, measurement_value, predicate_result_value, timestamp_value,
};

const LIST_MEASUREMENTS: &str = "FairValue.ListMeasurements";
const GET_CLASSIFICATION: &str = "FairValue.GetClassification";
const EXPLAIN: &str = "FairValue.Explain";
const GET_EVIDENCE: &str = "FairValue.GetEvidence";
const GET_APPROVAL_STATUS: &str = "FairValue.GetApprovalStatus";
const MEASURE: &str = "FairValue.Measure";
const CLASSIFY: &str = "FairValue.Classify";
const APPROVE: &str = "FairValue.Approve";
const APPROVE_MARKET_ACCESS: &str = "FairValue.ApproveMarketAccess";
const GET_MARKET_ACCESS: &str = "FairValue.GetMarketAccess";
const PROPOSE_OVERRIDE: &str = "FairValue.ProposeOverride";
const REVOKE_APPROVAL: &str = "FairValue.RevokeApproval";
const LIST_AUDIT_EVENTS: &str = "FairValue.ListAuditEvents";

/// Producer family named by one opaque, application-resolved receipt selector.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FairValueProducerKind {
    /// Post-commit qualified live observation bundle.
    Live,
    /// Manifest-pinned canonical research monetary cell.
    Research,
    /// Manifest-pinned registered analytical monetary feature.
    Analytics,
    /// Immutable portfolio revision and selected real position.
    Portfolio,
}

/// Closed caller selection for one genuine producer-owned fair-value input.
///
/// Financial values, currency, quality, timestamps, provenance, physical paths, SQL, and
/// hierarchy classifications are deliberately absent. Live requests select only an exact venue
/// and trade or quote side; the genuine post-action observation remains producer-owned. Research
/// and analytics select one bounded canonical row from a catalog-resolved immutable generation.
/// Portfolio selects the measurement account's current immutable revision.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FairValueProducerSelection {
    /// Latest retained post-action observation for one exact venue and price family.
    Live {
        /// Exact venue whose directly verified observation is required.
        venue_id: VenueId,
        /// Trade, bid, or ask selected from the genuine retained observation.
        selection: MarketPriceSelection,
        /// Whether the resulting input is significant to the measurement.
        significance: InputSignificance,
    },
    /// Canonical research monetary row from the current generation pinned at admission.
    Research {
        /// Stable local dataset identity.
        dataset_id: DatasetId,
        /// Zero-based canonical row offset.
        row: usize,
        /// Whether the resulting input is significant to the measurement.
        significance: InputSignificance,
    },
    /// Registered analytical monetary-feature row from the current generation pinned at admission.
    Analytics {
        /// Stable local feature-dataset identity.
        dataset_id: DatasetId,
        /// Zero-based canonical monetary-feature row offset.
        row: usize,
        /// Whether the resulting input is significant to the measurement.
        significance: InputSignificance,
    },
    /// Current immutable revision for the measurement account and subject instrument.
    Portfolio {
        /// Whether the resulting input is significant to the measurement.
        significance: InputSignificance,
    },
}

impl FairValueProducerSelection {
    /// Returns the producer family selected by this closed request.
    pub const fn producer(&self) -> FairValueProducerKind {
        match self {
            Self::Live { .. } => FairValueProducerKind::Live,
            Self::Research { .. } => FairValueProducerKind::Research,
            Self::Analytics { .. } => FairValueProducerKind::Analytics,
            Self::Portfolio { .. } => FairValueProducerKind::Portfolio,
        }
    }

    /// Returns whether the selected input is significant to the measurement.
    pub const fn significance(&self) -> InputSignificance {
        match self {
            Self::Live { significance, .. }
            | Self::Research { significance, .. }
            | Self::Analytics { significance, .. }
            | Self::Portfolio { significance } => *significance,
        }
    }
}

/// Complete bounded authority context for one in-process producer selection.
#[derive(Clone)]
pub struct FairValueProducerSelectionRequest {
    selection: FairValueProducerSelection,
    account_id: AccountId,
    instrument_id: InstrumentId,
    measurement_at: Timestamp,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl FairValueProducerSelectionRequest {
    /// Returns the closed producer selector.
    pub const fn selection(&self) -> &FairValueProducerSelection {
        &self.selection
    }

    /// Returns the reporting account from the enclosing measurement.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the subject instrument from the enclosing measurement.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact fair-value measurement cutoff.
    pub const fn measurement_at(&self) -> Timestamp {
        self.measurement_at
    }

    /// Returns cancellation owned by the admitted measurement request.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the admitted absolute monotonic deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for FairValueProducerSelectionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueProducerSelectionRequest")
            .field("selection", &self.selection)
            .field("account_id", &self.account_id)
            .field("instrument_id", &self.instrument_id)
            .field("measurement_at", &self.measurement_at)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Genuine producer selection or receipt-publication failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FairValueProducerSelectionError {
    /// The selected dataset, row, producer schema, or immutable value is invalid.
    #[error("fair-value producer selection is invalid")]
    InvalidSelection,
    /// The selected dataset generation or portfolio revision does not exist.
    #[error("fair-value selected producer was not found")]
    NotFound,
    /// The selected producer does not belong to the measurement account or instrument.
    #[error("fair-value selected producer is not authorized")]
    Unauthorized,
    /// A producer-owned count, memory, query, or retained-byte ceiling was reached.
    #[error("fair-value producer selection resource limit was exceeded")]
    ResourceExhausted,
    /// Request cancellation won the selection race.
    #[error("fair-value producer selection was cancelled")]
    Cancelled,
    /// The admitted request deadline elapsed.
    #[error("fair-value producer selection deadline elapsed")]
    DeadlineExceeded,
    /// Required producer authority is not currently available.
    #[error("fair-value producer selection authority is unavailable")]
    Unavailable,
    /// Producer selection failed without caller-safe details.
    #[error("fair-value producer selection failed")]
    Internal,
}

/// Least-authority bridge from closed selections to genuine process-local producer receipts.
///
/// Implementations may consume only non-forgeable live leases or immutable analytical and
/// portfolio producer capabilities, then publish through separated fair-value receipt handles.
/// They must not accept or reconstruct financial values from transport data. Publication is
/// bounded and idempotent; cancellation that races after a successful authority commit may leave
/// only the genuine process-local receipt, never a partially persisted measurement.
#[async_trait]
pub trait FairValueProducerSelectionAuthority: Send + Sync + 'static {
    /// Pins, reads, validates, and publishes one genuine producer receipt.
    async fn publish(
        &self,
        request: FairValueProducerSelectionRequest,
    ) -> Result<FairValueReceiptRegistration, FairValueProducerSelectionError>;
}

/// Least-authority request for one producer-derived valuation input.
///
/// The opaque receipt selector carries no source value, price, quality, hierarchy, or provenance.
/// The injected resolver must exchange it for a non-forgeable producer receipt and use the
/// valuation crate's producer-specific constructors.
#[derive(Clone)]
pub struct FairValueInputResolutionRequest {
    producer: FairValueProducerKind,
    receipt_id: Box<str>,
    significance: InputSignificance,
    account_id: AccountId,
    instrument_id: InstrumentId,
    measurement_at: Timestamp,
    ruleset: ClassificationRuleset,
    market_access_assessment: Option<Arc<ApprovedMarketAccess>>,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl FairValueInputResolutionRequest {
    /// Returns the exact producer family the resolver must use.
    pub const fn producer(&self) -> FairValueProducerKind {
        self.producer
    }

    /// Returns the bounded opaque producer-receipt selector.
    pub fn receipt_id(&self) -> &str {
        &self.receipt_id
    }

    /// Returns whether the resolved input is significant to the measurement.
    pub const fn significance(&self) -> InputSignificance {
        self.significance
    }

    /// Returns the reporting account for access and portfolio authority checks.
    pub const fn account_id(&self) -> AccountId {
        self.account_id
    }

    /// Returns the measured subject instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact fair-value measurement instant.
    pub const fn measurement_at(&self) -> Timestamp {
        self.measurement_at
    }

    /// Returns the current code-owned classification ruleset.
    pub const fn ruleset(&self) -> &ClassificationRuleset {
        &self.ruleset
    }

    /// Returns the durable assessment selected inside the fair-value authority boundary.
    pub fn market_access_assessment(&self) -> Option<&ApprovedMarketAccess> {
        self.market_access_assessment.as_deref()
    }

    /// Returns cancellation owned by the admitted request.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the admitted absolute monotonic deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for FairValueInputResolutionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueInputResolutionRequest")
            .field("producer", &self.producer)
            .field("receipt_id", &"[OPAQUE PRODUCER RECEIPT]")
            .field("significance", &self.significance)
            .field("account_id", &self.account_id)
            .field("instrument_id", &self.instrument_id)
            .field("measurement_at", &self.measurement_at)
            .field("ruleset", &self.ruleset)
            .field(
                "market_access_assessment",
                &self
                    .market_access_assessment
                    .as_ref()
                    .map(|value| value.id()),
            )
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Fixed, caller-safe producer-receipt resolution failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FairValueInputResolutionError {
    /// The opaque selector is invalid for the named producer family.
    #[error("fair-value producer receipt selector is invalid")]
    InvalidReference,
    /// No retained producer receipt matches the selector.
    #[error("fair-value producer receipt was not found")]
    NotFound,
    /// Required source or account authority is absent.
    #[error("fair-value producer receipt is not authorized")]
    Unauthorized,
    /// A producer-owned count or memory ceiling was reached.
    #[error("fair-value producer receipt limit was exceeded")]
    ResourceExhausted,
    /// Request cancellation won the resolution race.
    #[error("fair-value producer receipt resolution was cancelled")]
    Cancelled,
    /// The admitted request deadline elapsed.
    #[error("fair-value producer receipt resolution deadline elapsed")]
    DeadlineExceeded,
    /// The producer authority is not currently available.
    #[error("fair-value producer receipt authority is unavailable")]
    Unavailable,
    /// Producer receipt resolution failed without caller-safe details.
    #[error("fair-value producer receipt resolution failed")]
    Internal,
}

/// Read-only authority that exchanges opaque selectors for genuine producer-derived inputs.
///
/// Implementations receive no catalog write authority and must be cancellation-safe: dropping the
/// returned future must not publish or mutate producer state. A receipt selector is only a key in
/// a bounded injected producer registry; it must never be treated as an ambient filesystem path,
/// URL, SQL fragment, or instruction to perform an unrelated network request. An admitted key is
/// immutable: every exact retry must resolve the same producer receipt or fail closed.
#[async_trait]
pub trait FairValueInputResolver: Send + Sync + 'static {
    /// Resolves one selector through the named producer's existing read capability.
    async fn resolve(
        &self,
        request: FairValueInputResolutionRequest,
    ) -> Result<ValuationInput, FairValueInputResolutionError>;
}

/// Application-owned fair-value surface over one durable catalog writer and receipt resolver.
pub struct FairValueDomainService {
    state: Mutex<FairValueService>,
    resolver: Arc<dyn FairValueInputResolver>,
    selection_authority: Arc<dyn FairValueProducerSelectionAuthority>,
    ruleset: ClassificationRuleset,
    maximum_inputs: usize,
    maximum_query_results: usize,
    lifecycle: Arc<DomainLifecycle>,
}

/// Immutable fair-value evidence retained by a governed approval or override preview.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedFairValueDecisionEvidence {
    measurement_id: MeasurementId,
    decision_id: DecisionId,
    evidence_hash: FairValueEvidenceHash,
    ruleset_hash: RulesetHash,
    hierarchy: FairValueHierarchy,
    basis: DecisionBasis,
}

impl GovernedFairValueDecisionEvidence {
    pub(crate) const fn measurement_id(&self) -> MeasurementId {
        self.measurement_id
    }

    pub(crate) const fn decision_id(&self) -> DecisionId {
        self.decision_id
    }

    pub(crate) const fn evidence_hash(&self) -> FairValueEvidenceHash {
        self.evidence_hash
    }

    pub(crate) const fn ruleset_hash(&self) -> RulesetHash {
        self.ruleset_hash
    }

    pub(crate) const fn hierarchy(&self) -> FairValueHierarchy {
        self.hierarchy
    }

    pub(crate) const fn basis(&self) -> DecisionBasis {
        self.basis
    }
}

/// Immutable active approval plus its exact measurement/decision evidence chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedFairValueApprovalEvidence {
    approval_id: ValuationApprovalId,
    decision: GovernedFairValueDecisionEvidence,
    expires_at: Timestamp,
}

impl GovernedFairValueApprovalEvidence {
    pub(crate) const fn approval_id(&self) -> ValuationApprovalId {
        self.approval_id
    }

    pub(crate) const fn decision(&self) -> &GovernedFairValueDecisionEvidence {
        &self.decision
    }

    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Exact typed market coordinates and current retained assessment identity, when one exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GovernedFairValueMarketAccessEvidence {
    account_id: AccountId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    effective_from: Timestamp,
    current_assessment_id: Option<MarketAccessAssessmentId>,
}

impl GovernedFairValueMarketAccessEvidence {
    pub(crate) const fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    pub(crate) const fn current_assessment_id(&self) -> Option<MarketAccessAssessmentId> {
        self.current_assessment_id
    }
}

impl FairValueDomainService {
    /// Binds one durable service to code-owned rules, genuine selection, and receipt resolution.
    ///
    /// # Errors
    ///
    /// Returns a ruleset error for an invalid configured quote-age ceiling.
    pub fn try_new(
        service: FairValueService,
        resolver: Arc<dyn FairValueInputResolver>,
        selection_authority: Arc<dyn FairValueProducerSelectionAuthority>,
        maximum_quote_age_nanos: u64,
    ) -> Result<Self, FairValueError> {
        let limits = service.limits();
        Ok(Self {
            state: Mutex::new(service),
            resolver,
            selection_authority,
            ruleset: ClassificationRuleset::current(maximum_quote_age_nanos)?,
            maximum_inputs: limits.max_inputs_per_measurement(),
            maximum_query_results: limits.max_query_results(),
            lifecycle: DomainLifecycle::new(),
        })
    }

    /// Resolves the immutable evidence chain used by one governed approval or override preview.
    pub(crate) async fn governed_decision_evidence(
        &self,
        measurement_id: MeasurementId,
        decision_id: DecisionId,
    ) -> Result<GovernedFairValueDecisionEvidence, FairValueError> {
        let state = self.state.lock().await;
        resolve_governed_decision(&state, measurement_id, decision_id)
    }

    /// Resolves an active approval and its complete immutable measurement/decision chain.
    pub(crate) async fn governed_approval_evidence(
        &self,
        approval_id: ValuationApprovalId,
        at: Timestamp,
    ) -> Result<GovernedFairValueApprovalEvidence, FairValueError> {
        let state = self.state.lock().await;
        resolve_governed_approval(&state, approval_id, at)
    }

    /// Resolves the exact typed market and the assessment current at the proposed effective start.
    pub(crate) async fn governed_market_access_evidence(
        &self,
        account_id: AccountId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        effective_from: Timestamp,
    ) -> Result<GovernedFairValueMarketAccessEvidence, FairValueError> {
        let state = self.state.lock().await;
        resolve_governed_market_access(&state, account_id, venue_id, instrument_id, effective_from)
    }

    /// Revalidates the preview evidence and durably approves through the existing authority.
    pub(crate) async fn commit_governed_approval(
        &self,
        evidence: GovernedFairValueDecisionEvidence,
        approved_by: ActorId,
        approved_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<ValuationApprovalId, FairValueError> {
        let mut state = self.state.lock().await;
        if resolve_governed_decision(&state, evidence.measurement_id, evidence.decision_id)?
            != evidence
        {
            return Err(FairValueError::InvalidMeasurement);
        }
        state
            .approve(evidence.decision_id, approved_by, approved_at, expires_at)
            .map(|approval| approval.id())
    }

    /// Revalidates the preview evidence and durably records one non-promoting override.
    pub(crate) async fn commit_governed_override(
        &self,
        evidence: GovernedFairValueDecisionEvidence,
        requested_hierarchy: FairValueHierarchy,
        justification: &str,
        prepared_by: ActorId,
        prepared_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<market_squawk_valuation::OverrideId, FairValueError> {
        let mut state = self.state.lock().await;
        let current =
            resolve_governed_decision(&state, evidence.measurement_id, evidence.decision_id)?;
        validate_governed_override(&current, requested_hierarchy)?;
        if current != evidence {
            return Err(FairValueError::InvalidOverride);
        }
        state
            .propose_override(
                evidence.decision_id,
                requested_hierarchy,
                justification,
                prepared_by,
                prepared_at,
                expires_at,
            )
            .map(|proposal| proposal.valuation_override().id())
    }

    /// Revalidates active status and durably revokes through the existing authority.
    pub(crate) async fn commit_governed_revocation(
        &self,
        evidence: GovernedFairValueApprovalEvidence,
        revoked_by: ActorId,
        revoked_at: Timestamp,
        reason: &str,
    ) -> Result<market_squawk_valuation::ApprovalRevocationId, FairValueError> {
        let mut state = self.state.lock().await;
        if resolve_governed_approval(&state, evidence.approval_id, revoked_at)? != evidence {
            return Err(FairValueError::InvalidRevocationTime);
        }
        state
            .revoke_approval(evidence.approval_id, revoked_by, revoked_at, reason)
            .map(|revocation| revocation.id())
    }

    /// Revalidates the exact market head and durably appends a dual-principal assessment.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_governed_market_access(
        &self,
        evidence: GovernedFairValueMarketAccessEvidence,
        conclusion: MarketAccess,
        effective_until: Timestamp,
        rationale: &str,
        prepared_by: ActorId,
        approved_by: ActorId,
        committed_at: Timestamp,
    ) -> Result<MarketAccessAssessmentId, FairValueError> {
        let mut state = self.state.lock().await;
        if resolve_governed_market_access(
            &state,
            evidence.account_id,
            evidence.venue_id.clone(),
            evidence.instrument_id,
            evidence.effective_from,
        )? != evidence
        {
            return Err(FairValueError::InvalidMarketAccessAssessment);
        }
        state
            .approve_market_access(
                evidence.account_id,
                evidence.venue_id,
                evidence.instrument_id,
                conclusion,
                evidence.effective_from,
                effective_until,
                rationale,
                prepared_by,
                committed_at,
                approved_by,
                committed_at,
            )
            .map(|assessment| assessment.id())
    }

    async fn list_measurements(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let limit = limits
            .maximum_result_items()
            .min(self.maximum_query_results);
        let state = self.lock_state(context).await?;
        let available = state.measurement_count();
        let measurements = state.measurements(limit).map_err(map_fair_value_error)?;
        drop(state);
        let values = measurements
            .iter()
            .map(|measurement| measurement_value(measurement))
            .collect::<Vec<_>>();
        bounded_result(
            json!({"measurements": values}),
            values.len(),
            available,
            limits,
        )
    }

    async fn classification(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let state = self.lock_state(context).await?;
        let measurement = state
            .measurement(measurement_id)
            .ok_or(ServiceError::NotFound)?;
        let decision = state
            .rules_decision_for_measurement(measurement_id, self.ruleset.hash())
            .map_err(map_fair_value_error)?
            .ok_or(ServiceError::NotFound)?;
        drop(state);
        one_result(
            json!({
                "measurement": measurement_value(&measurement),
                "classification": classification_value(&decision)
            }),
            request,
            context,
        )
    }

    async fn explain(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let state = self.lock_state(context).await?;
        let decision = state
            .rules_decision_for_measurement(measurement_id, self.ruleset.hash())
            .map_err(map_fair_value_error)?
            .ok_or(ServiceError::NotFound)?;
        drop(state);

        let available = decision
            .truth_table()
            .len()
            .checked_add(decision.reasons().len())
            .ok_or(ServiceError::InvalidResult)?;
        let limit = limits
            .maximum_result_items()
            .min(self.maximum_query_results);
        let truth_table = decision
            .truth_table()
            .iter()
            .take(limit)
            .copied()
            .map(predicate_result_value)
            .collect::<Vec<_>>();
        let remaining = limit.saturating_sub(truth_table.len());
        let reasons = decision
            .reasons()
            .iter()
            .take(remaining)
            .copied()
            .map(explanation_reason_value)
            .collect::<Vec<_>>();
        let returned = truth_table
            .len()
            .checked_add(reasons.len())
            .ok_or(ServiceError::InvalidResult)?;
        bounded_result(
            json!({
                "classification": classification_value(&decision),
                "truthTable": truth_table,
                "reasons": reasons
            }),
            returned,
            available,
            limits,
        )
    }

    async fn evidence(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let state = self.lock_state(context).await?;
        let measurement = state
            .measurement(measurement_id)
            .ok_or(ServiceError::NotFound)?;
        drop(state);
        let available = measurement.inputs().len();
        let evidence = measurement
            .inputs()
            .iter()
            .take(
                limits
                    .maximum_result_items()
                    .min(self.maximum_query_results),
            )
            .map(evidence_value)
            .collect::<Vec<_>>();
        bounded_result(
            json!({
                "measurementId": measurement.id().to_string(),
                "evidenceHash": measurement.evidence_hash().to_string(),
                "inputs": evidence
            }),
            evidence.len(),
            available,
            limits,
        )
    }

    async fn approval_status(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let at = admitted_timestamp(request.arguments(), "at")?;
        let limit = limits
            .maximum_result_items()
            .min(self.maximum_query_results);
        let state = self.lock_state(context).await?;
        let available = state
            .approval_count_for_measurement(measurement_id)
            .map_err(map_fair_value_error)?;
        let approvals = state
            .approvals_for_measurement(measurement_id, limit)
            .map_err(map_fair_value_error)?;
        let values = approvals
            .iter()
            .map(|approval| {
                let status = state
                    .approval_status(approval.id(), at)
                    .map_err(map_fair_value_error)?;
                Ok(approval_value(
                    approval,
                    status,
                    state.revocation(approval.id()).as_deref(),
                ))
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        drop(state);
        bounded_result(
            json!({
                "measurementId": measurement_id.to_string(),
                "at": timestamp_value(at),
                "approvals": values
            }),
            values.len(),
            available,
            limits,
        )
    }

    async fn measure(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let parsed = ParsedMeasurement::from_request(request, self.maximum_inputs)?;
        let ParsedMeasurement {
            account_id,
            instrument_id,
            amount,
            measurement_at,
            prepared_at,
            prepared_by,
            method,
            receipts,
            selections,
        } = parsed;
        let input_count = receipts
            .len()
            .checked_add(selections.len())
            .ok_or(ServiceError::ResourceExhausted)?;
        let mut inputs = Vec::new();
        inputs
            .try_reserve_exact(input_count)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for receipt in receipts {
            let resolution = FairValueInputResolutionRequest {
                producer: receipt.producer,
                receipt_id: receipt.receipt_id,
                significance: receipt.significance,
                account_id,
                instrument_id,
                measurement_at,
                ruleset: self.ruleset.clone(),
                market_access_assessment: None,
                cancellation: context.cancellation().clone(),
                deadline: context.deadline(),
            };
            let input = self.resolve_input(resolution, context).await?;
            validate_resolved_input(
                &input,
                receipt.producer,
                receipt.significance,
                account_id,
                instrument_id,
            )?;
            inputs.push(input);
        }
        for selection in selections {
            let producer = selection.producer();
            let significance = selection.significance();
            let market_access_assessment = match &selection {
                FairValueProducerSelection::Live { venue_id, .. } => Some(
                    self.current_accessible_market(
                        account_id,
                        venue_id,
                        instrument_id,
                        measurement_at,
                        context,
                    )
                    .await?,
                ),
                FairValueProducerSelection::Research { .. }
                | FairValueProducerSelection::Analytics { .. }
                | FairValueProducerSelection::Portfolio { .. } => None,
            };
            let registration = self
                .publish_selection(
                    FairValueProducerSelectionRequest {
                        selection,
                        account_id,
                        instrument_id,
                        measurement_at,
                        cancellation: context.cancellation().clone(),
                        deadline: context.deadline(),
                    },
                    context,
                )
                .await?;
            let input = self
                .resolve_input(
                    FairValueInputResolutionRequest {
                        producer,
                        receipt_id: registration.reference().as_str().into(),
                        significance,
                        account_id,
                        instrument_id,
                        measurement_at,
                        ruleset: self.ruleset.clone(),
                        market_access_assessment,
                        cancellation: context.cancellation().clone(),
                        deadline: context.deadline(),
                    },
                    context,
                )
                .await?;
            validate_resolved_input(&input, producer, significance, account_id, instrument_id)?;
            inputs.push(input);
        }
        let measurement = ValuationMeasurement::try_new(ValuationMeasurementSpec {
            account_id,
            instrument_id,
            amount,
            measurement_at,
            prepared_at,
            prepared_by,
            method,
            inputs,
        })
        .map_err(map_fair_value_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        let mut state = self.lock_state(context).await?;
        let measurement_replay = state.measurement(measurement.id()).is_some();
        let classification_replay = if measurement_replay {
            state
                .rules_decision_for_measurement(measurement.id(), self.ruleset.hash())
                .map_err(map_fair_value_error)?
                .is_some()
        } else {
            false
        };
        let decision = state
            .classify(measurement, self.ruleset.clone())
            .map_err(map_fair_value_error)?;
        let retained = state
            .measurement(decision.measurement_id())
            .ok_or(ServiceError::Internal)?;
        drop(state);
        one_result(
            json!({
                "measurement": measurement_value(&retained),
                "classification": classification_value(&decision),
                "measurementReplay": measurement_replay,
                "classificationReplay": classification_replay
            }),
            request,
            context,
        )
    }

    async fn classify(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let mut state = self.lock_state(context).await?;
        let existing = state
            .rules_decision_for_measurement(measurement_id, self.ruleset.hash())
            .map_err(map_fair_value_error)?;
        let (decision, replay) = match existing {
            Some(decision) => (decision, true),
            None => {
                let measurement = state
                    .measurement(measurement_id)
                    .ok_or(ServiceError::NotFound)?;
                let decision = state
                    .classify((*measurement).clone(), self.ruleset.clone())
                    .map_err(map_fair_value_error)?;
                (decision, false)
            }
        };
        drop(state);
        one_result(
            json!({
                "classification": classification_value(&decision),
                "classificationReplay": replay
            }),
            request,
            context,
        )
    }

    async fn approve(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let decision_id = admitted_decision_id(request.arguments())?;
        let approved_by = ActorId::try_from(required_string(request.arguments(), "approvedBy")?)
            .map_err(map_fair_value_error)?;
        let approved_at = admitted_timestamp(request.arguments(), "approvedAt")?;
        let expires_at = admitted_timestamp(request.arguments(), "expiresAt")?;
        let mut state = self.lock_state(context).await?;
        let decision = state.decision(decision_id).ok_or(ServiceError::NotFound)?;
        if decision.measurement_id() != measurement_id {
            return Err(ServiceError::InvalidRequest);
        }
        let approval = state
            .approve(decision_id, approved_by, approved_at, expires_at)
            .map_err(map_fair_value_error)?;
        let status = state
            .approval_status(approval.id(), approved_at)
            .map_err(map_fair_value_error)?;
        drop(state);
        one_result(
            json!({"approval": approval_value(&approval, status, None)}),
            request,
            context,
        )
    }

    async fn propose_override(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let measurement_id = admitted_measurement_id(request.arguments())?;
        let decision_id = admitted_decision_id(request.arguments())?;
        let requested_hierarchy = match required_string(request.arguments(), "requestedHierarchy")?
        {
            "level_2" => FairValueHierarchy::Level2,
            "level_3" => FairValueHierarchy::Level3,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let justification = required_string(request.arguments(), "justification")?;
        let prepared_by = ActorId::try_from(required_string(request.arguments(), "preparedBy")?)
            .map_err(map_fair_value_error)?;
        let prepared_at = admitted_timestamp(request.arguments(), "preparedAt")?;
        let expires_at = admitted_timestamp(request.arguments(), "expiresAt")?;
        let mut state = self.lock_state(context).await?;
        let decision = state.decision(decision_id).ok_or(ServiceError::NotFound)?;
        if decision.measurement_id() != measurement_id {
            return Err(ServiceError::InvalidRequest);
        }
        let proposal = state
            .propose_override(
                decision_id,
                requested_hierarchy,
                justification,
                prepared_by,
                prepared_at,
                expires_at,
            )
            .map_err(map_fair_value_error)?;
        drop(state);
        one_result(override_proposal_value(&proposal), request, context)
    }

    async fn revoke_approval(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let approval_id =
            ValuationApprovalId::from_str(required_string(request.arguments(), "approvalId")?)
                .map_err(|_| ServiceError::InvalidRequest)?;
        let revoked_by = ActorId::try_from(required_string(request.arguments(), "revokedBy")?)
            .map_err(map_fair_value_error)?;
        let revoked_at = admitted_timestamp(request.arguments(), "revokedAt")?;
        let reason = required_string(request.arguments(), "reason")?;
        let mut state = self.lock_state(context).await?;
        let revocation = state
            .revoke_approval(approval_id, revoked_by, revoked_at, reason)
            .map_err(map_fair_value_error)?;
        let approval = state.approval(approval_id).ok_or(ServiceError::Internal)?;
        let status = state
            .approval_status(approval_id, revoked_at)
            .map_err(map_fair_value_error)?;
        if status != ApprovalStatus::Revoked {
            return Err(ServiceError::Internal);
        }
        drop(state);
        one_result(
            json!({
                "approval": approval_value(&approval, status, Some(&revocation)),
            }),
            request,
            context,
        )
    }

    async fn list_audit_events(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let requested_limit = request
            .arguments()
            .get("limit")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        if requested_limit == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        let limit = requested_limit
            .min(limits.maximum_result_items())
            .min(self.maximum_query_results);
        let after = admitted_audit_cursor(request.arguments())?;
        let state = self.lock_state(context).await?;
        let page = state
            .audit_page(after, limit)
            .map_err(map_fair_value_error)?;
        drop(state);
        let values = page
            .events()
            .iter()
            .map(|event| audit_event_value(event))
            .collect::<Vec<_>>();
        let next_cursor = page.next_cursor().map(|cursor| {
            json!({
                "sequence": cursor.sequence(),
                "eventId": cursor.event_id().to_string(),
            })
        });
        let returned = values.len();
        bounded_result(
            json!({
                "events": values,
                "totalEventCount": page.total_count(),
                "nextCursor": next_cursor,
            }),
            returned,
            page.available_from_cursor(),
            limits,
        )
    }

    async fn approve_market_access(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let account_id = AccountId::from_str(required_string(request.arguments(), "accountId")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let venue_id = VenueId::try_from(required_string(request.arguments(), "venueId")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let instrument_id =
            InstrumentId::from_str(required_string(request.arguments(), "instrumentId")?)
                .map_err(|_| ServiceError::InvalidRequest)?;
        let conclusion = match required_string(request.arguments(), "conclusion")? {
            "accessible" => MarketAccess::Accessible,
            "inaccessible" => MarketAccess::Inaccessible,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let effective_from = admitted_timestamp(request.arguments(), "effectiveFrom")?;
        let effective_until = admitted_timestamp(request.arguments(), "effectiveUntil")?;
        let rationale = required_string(request.arguments(), "rationale")?;
        let prepared_by = ActorId::try_from(required_string(request.arguments(), "preparedBy")?)
            .map_err(map_fair_value_error)?;
        let prepared_at = admitted_timestamp(request.arguments(), "preparedAt")?;
        let approved_by = ActorId::try_from(required_string(request.arguments(), "approvedBy")?)
            .map_err(map_fair_value_error)?;
        let approved_at = admitted_timestamp(request.arguments(), "approvedAt")?;
        let mut state = self.lock_state(context).await?;
        let assessment = state
            .approve_market_access(
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
            )
            .map_err(map_fair_value_error)?;
        drop(state);
        one_result(
            json!({"marketAccess": market_access_assessment_value(&assessment)}),
            request,
            context,
        )
    }

    async fn market_access(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let assessment_id = MarketAccessAssessmentId::from_str(required_string(
            request.arguments(),
            "assessmentId",
        )?)
        .map_err(|_| ServiceError::InvalidRequest)?;
        let state = self.lock_state(context).await?;
        let assessment = state
            .market_access(assessment_id)
            .ok_or(ServiceError::NotFound)?;
        drop(state);
        one_result(
            json!({"marketAccess": market_access_assessment_value(&assessment)}),
            request,
            context,
        )
    }

    async fn current_accessible_market(
        &self,
        account_id: AccountId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        measurement_at: Timestamp,
        context: &RequestContext,
    ) -> Result<Arc<ApprovedMarketAccess>, ServiceError> {
        let state = self.lock_state(context).await?;
        let assessment = state
            .current_market_access(account_id, venue_id, instrument_id, measurement_at)
            .map_err(map_fair_value_error)?
            .ok_or(ServiceError::Unauthorized)?;
        if assessment.conclusion() != MarketAccess::Accessible {
            return Err(ServiceError::Unauthorized);
        }
        drop(state);
        Ok(assessment)
    }

    async fn resolve_input(
        &self,
        request: FairValueInputResolutionRequest,
        context: &RequestContext,
    ) -> Result<ValuationInput, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let resolved = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = self.lifecycle.shutdown_token().cancelled() => {
                return Err(ServiceError::Unavailable);
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            resolved = self.resolver.resolve(request) => resolved,
        }
        .map_err(map_resolution_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        Ok(resolved)
    }

    async fn publish_selection(
        &self,
        request: FairValueProducerSelectionRequest,
        context: &RequestContext,
    ) -> Result<FairValueReceiptRegistration, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let registration = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = self.lifecycle.shutdown_token().cancelled() => {
                return Err(ServiceError::Unavailable);
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            registration = self.selection_authority.publish(request) => registration,
        }
        .map_err(map_selection_error)?;
        ensure_request_live(context, &self.lifecycle)?;
        Ok(registration)
    }

    async fn lock_state(
        &self,
        context: &RequestContext,
    ) -> Result<MutexGuard<'_, FairValueService>, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => Err(ServiceError::Cancelled),
            _ = self.lifecycle.shutdown_token().cancelled() => Err(ServiceError::Unavailable),
            _ = tokio::time::sleep_until(deadline) => Err(ServiceError::DeadlineExceeded),
            state = self.state.lock() => Ok(state),
        }
    }
}

impl fmt::Debug for FairValueDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueDomainService")
            .field("resolver", &"[LEAST-AUTHORITY PRODUCER RESOLVER]")
            .field(
                "selection_authority",
                &"[GENUINE PRODUCER SELECTION AUTHORITY]",
            )
            .field("ruleset_version", &self.ruleset.version())
            .field("ruleset_hash", &self.ruleset.hash())
            .field("maximum_inputs", &self.maximum_inputs)
            .field("maximum_query_results", &self.maximum_query_results)
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

#[async_trait]
impl ApplicationDomainService for FairValueDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::FairValue
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if request.contract().domain() != ServiceDomain::FairValue {
            return Err(ServiceError::InvalidRequest);
        }
        let _call = DomainLifecycle::enter(&self.lifecycle, &context)?;
        let result = match request.name() {
            LIST_MEASUREMENTS => self.list_measurements(&request, &context).await,
            GET_CLASSIFICATION => self.classification(&request, &context).await,
            EXPLAIN => self.explain(&request, &context).await,
            GET_EVIDENCE => self.evidence(&request, &context).await,
            GET_APPROVAL_STATUS => self.approval_status(&request, &context).await,
            MEASURE => self.measure(&request, &context).await,
            CLASSIFY => self.classify(&request, &context).await,
            APPROVE => self.approve(&request, &context).await,
            PROPOSE_OVERRIDE => self.propose_override(&request, &context).await,
            REVOKE_APPROVAL => self.revoke_approval(&request, &context).await,
            LIST_AUDIT_EVENTS => self.list_audit_events(&request, &context).await,
            APPROVE_MARKET_ACCESS => self.approve_market_access(&request, &context).await,
            GET_MARKET_ACCESS => self.market_access(&request, &context).await,
            _ => Err(ServiceError::NotFound),
        }?;
        ensure_request_live(&context, &self.lifecycle)?;
        Ok(result)
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.lifecycle.finish_shutdown(deadline).await
    }
}

impl Drop for FairValueDomainService {
    fn drop(&mut self) {
        self.lifecycle.begin_shutdown();
    }
}

struct ParsedMeasurement {
    account_id: AccountId,
    instrument_id: InstrumentId,
    amount: ValuationAmount,
    measurement_at: Timestamp,
    prepared_at: Timestamp,
    prepared_by: ActorId,
    method: ValuationMethod,
    receipts: Vec<ParsedReceipt>,
    selections: Vec<FairValueProducerSelection>,
}

impl ParsedMeasurement {
    fn from_request(
        request: &TypedToolRequest,
        maximum_inputs: usize,
    ) -> Result<Self, ServiceError> {
        let measurement = request
            .arguments()
            .get("measurement")
            .and_then(Value::as_object)
            .ok_or(ServiceError::InvalidRequest)?;
        let account_id = AccountId::from_str(required_string(measurement, "accountId")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let instrument_id = InstrumentId::from_str(required_string(measurement, "instrumentId")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let decimal = Decimal::from_str(required_string(measurement, "amount")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let currency = Currency::try_from(required_string(measurement, "currency")?)
            .map_err(|_| ServiceError::InvalidRequest)?;
        let scale = measurement
            .get("scale")
            .and_then(Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let amount = ValuationAmount::try_new(Money::new(decimal, currency), scale)
            .map_err(map_fair_value_error)?;
        let measurement_at = admitted_timestamp(measurement, "measurementAt")?;
        let prepared_at = admitted_timestamp(measurement, "preparedAt")?;
        let prepared_by = ActorId::try_from(required_string(measurement, "preparedBy")?)
            .map_err(map_fair_value_error)?;
        let method = match required_string(measurement, "method")? {
            "quoted_market_price" => ValuationMethod::QuotedMarketPrice,
            "market_approach" => ValuationMethod::MarketApproach,
            "income_approach" => ValuationMethod::IncomeApproach,
            "cost_approach" => ValuationMethod::CostApproach,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let receipt_values = optional_array(measurement, "producerReceipts")?;
        let selection_values = optional_array(measurement, "producerSelections")?;
        let input_count = receipt_values
            .len()
            .checked_add(selection_values.len())
            .ok_or(ServiceError::ResourceExhausted)?;
        if input_count == 0 || input_count > maximum_inputs {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut receipts = Vec::new();
        receipts
            .try_reserve_exact(receipt_values.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for value in receipt_values {
            receipts.push(ParsedReceipt::try_from(value)?);
        }
        let mut unique = HashSet::new();
        unique
            .try_reserve(receipts.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        if receipts
            .iter()
            .any(|receipt| !unique.insert((receipt.producer, receipt.receipt_id.as_ref())))
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut selections = Vec::new();
        selections
            .try_reserve_exact(selection_values.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for value in selection_values {
            selections.push(FairValueProducerSelection::try_from(value)?);
        }
        let mut unique_selections = HashSet::new();
        unique_selections
            .try_reserve(selections.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        if selections
            .iter()
            .any(|selection| !unique_selections.insert(selection_coordinate(selection)))
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            account_id,
            instrument_id,
            amount,
            measurement_at,
            prepared_at,
            prepared_by,
            method,
            receipts,
            selections,
        })
    }
}

#[derive(Eq, Hash, PartialEq)]
enum SelectionCoordinate<'selection> {
    Live(&'selection VenueId, MarketPriceSelection),
    Dataset(FairValueProducerKind, &'selection DatasetId, usize),
    Portfolio,
}

fn selection_coordinate(selection: &FairValueProducerSelection) -> SelectionCoordinate<'_> {
    match selection {
        FairValueProducerSelection::Live {
            venue_id,
            selection,
            ..
        } => SelectionCoordinate::Live(venue_id, *selection),
        FairValueProducerSelection::Research {
            dataset_id, row, ..
        }
        | FairValueProducerSelection::Analytics {
            dataset_id, row, ..
        } => SelectionCoordinate::Dataset(selection.producer(), dataset_id, *row),
        FairValueProducerSelection::Portfolio { .. } => SelectionCoordinate::Portfolio,
    }
}

impl TryFrom<&Value> for FairValueProducerSelection {
    type Error = ServiceError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let value = value.as_object().ok_or(ServiceError::InvalidRequest)?;
        let significance = admitted_significance(value)?;
        match required_string(value, "producer")? {
            "live" => {
                if value.len() != 4
                    || value.keys().any(|key| {
                        !matches!(
                            key.as_str(),
                            "producer" | "venueId" | "selection" | "significance"
                        )
                    })
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let venue_id = VenueId::try_from(required_string(value, "venueId")?)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let selection = match required_string(value, "selection")? {
                    "trade" => MarketPriceSelection::Trade,
                    "bid" => MarketPriceSelection::Bid,
                    "ask" => MarketPriceSelection::Ask,
                    _ => return Err(ServiceError::InvalidRequest),
                };
                Ok(Self::Live {
                    venue_id,
                    selection,
                    significance,
                })
            }
            "research" | "analytics" => {
                if value.len() != 4
                    || value.keys().any(|key| {
                        !matches!(
                            key.as_str(),
                            "producer" | "datasetId" | "row" | "significance"
                        )
                    })
                {
                    return Err(ServiceError::InvalidRequest);
                }
                let dataset_id = DatasetId::try_from(required_string(value, "datasetId")?)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                let row = value
                    .get("row")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or(ServiceError::InvalidRequest)?;
                if required_string(value, "producer")? == "research" {
                    Ok(Self::Research {
                        dataset_id,
                        row,
                        significance,
                    })
                } else {
                    Ok(Self::Analytics {
                        dataset_id,
                        row,
                        significance,
                    })
                }
            }
            "portfolio" => {
                if value.len() != 2
                    || value
                        .keys()
                        .any(|key| !matches!(key.as_str(), "producer" | "significance"))
                {
                    return Err(ServiceError::InvalidRequest);
                }
                Ok(Self::Portfolio { significance })
            }
            _ => Err(ServiceError::InvalidRequest),
        }
    }
}

struct ParsedReceipt {
    producer: FairValueProducerKind,
    receipt_id: Box<str>,
    significance: InputSignificance,
}

impl TryFrom<&Value> for ParsedReceipt {
    type Error = ServiceError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let value = value.as_object().ok_or(ServiceError::InvalidRequest)?;
        let producer = match required_string(value, "producer")? {
            "research" => FairValueProducerKind::Research,
            "analytics" => FairValueProducerKind::Analytics,
            "portfolio" => FairValueProducerKind::Portfolio,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let receipt_id = required_string(value, "receiptId")?.into();
        let significance = admitted_significance(value)?;
        Ok(Self {
            producer,
            receipt_id,
            significance,
        })
    }
}

fn optional_array<'value>(
    arguments: &'value Map<String, Value>,
    field: &str,
) -> Result<&'value [Value], ServiceError> {
    arguments.get(field).map_or(Ok(&[][..]), |value| {
        value
            .as_array()
            .filter(|values| !values.is_empty())
            .map(Vec::as_slice)
            .ok_or(ServiceError::InvalidRequest)
    })
}

fn admitted_significance(
    arguments: &Map<String, Value>,
) -> Result<InputSignificance, ServiceError> {
    match required_string(arguments, "significance")? {
        "significant" => Ok(InputSignificance::Significant),
        "not_significant" => Ok(InputSignificance::NotSignificant),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn admitted_measurement_id(arguments: &Map<String, Value>) -> Result<MeasurementId, ServiceError> {
    MeasurementId::from_str(required_string(arguments, "measurementId")?)
        .map_err(|_| ServiceError::InvalidRequest)
}

fn admitted_decision_id(arguments: &Map<String, Value>) -> Result<DecisionId, ServiceError> {
    DecisionId::from_str(required_string(arguments, "decisionId")?)
        .map_err(|_| ServiceError::InvalidRequest)
}

fn admitted_audit_cursor(
    arguments: &Map<String, Value>,
) -> Result<Option<FairValueAuditCursor>, ServiceError> {
    let Some(value) = arguments.get("after") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let cursor = value.as_object().ok_or(ServiceError::InvalidRequest)?;
    let sequence = cursor
        .get("sequence")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::InvalidRequest)?;
    let event_id = AuditEventId::from_str(required_string(cursor, "eventId")?)
        .map_err(|_| ServiceError::InvalidRequest)?;
    FairValueAuditCursor::try_new(sequence, event_id)
        .map(Some)
        .map_err(map_fair_value_error)
}

fn override_proposal_value(proposal: &OverrideProposal) -> Value {
    let valuation_override = proposal.valuation_override();
    json!({
        "override": {
            "overrideId": valuation_override.id().to_string(),
            "baseDecisionId": valuation_override.base_decision_id().to_string(),
            "requestedHierarchy": fair_value_hierarchy_name(
                valuation_override.requested_hierarchy()
            ),
            "justification": valuation_override.justification(),
            "preparedBy": valuation_override.prepared_by().as_str(),
            "preparedAt": timestamp_value(valuation_override.prepared_at()),
            "expiresAt": timestamp_value(valuation_override.expires_at()),
        },
        "classification": classification_value(proposal.decision()),
    })
}

fn audit_event_value(event: &FairValueAuditEvent) -> Value {
    let subject = match event.kind() {
        AuditEventKind::Classified {
            measurement_id,
            decision_id,
        } => json!({
            "kind": "classified",
            "measurementId": measurement_id.to_string(),
            "decisionId": decision_id.to_string(),
        }),
        AuditEventKind::OverrideProposed {
            override_id,
            decision_id,
        } => json!({
            "kind": "override_proposed",
            "overrideId": override_id.to_string(),
            "decisionId": decision_id.to_string(),
        }),
        AuditEventKind::Approved {
            approval_id,
            decision_id,
        } => json!({
            "kind": "approved",
            "approvalId": approval_id.to_string(),
            "decisionId": decision_id.to_string(),
        }),
        AuditEventKind::Revoked {
            revocation_id,
            approval_id,
        } => json!({
            "kind": "revoked",
            "revocationId": revocation_id.to_string(),
            "approvalId": approval_id.to_string(),
        }),
        AuditEventKind::MarketAccessApproved { assessment_id } => json!({
            "kind": "market_access_approved",
            "assessmentId": assessment_id.to_string(),
        }),
    };
    json!({
        "auditEventId": event.id().to_string(),
        "sequence": event.sequence(),
        "previousEventId": event.previous_event_id().map(|value| value.to_string()),
        "subject": subject,
        "actor": event.actor().as_str(),
        "businessAt": timestamp_value(event.business_at()),
        "occurredAt": timestamp_value(event.occurred_at()),
    })
}

const fn fair_value_hierarchy_name(hierarchy: FairValueHierarchy) -> &'static str {
    match hierarchy {
        FairValueHierarchy::Level1 => "level_1",
        FairValueHierarchy::Level2 => "level_2",
        FairValueHierarchy::Level3 => "level_3",
        FairValueHierarchy::Unclassified => "unclassified",
    }
}

fn admitted_timestamp(
    arguments: &Map<String, Value>,
    field: &str,
) -> Result<Timestamp, ServiceError> {
    DateTime::parse_from_rfc3339(required_string(arguments, field)?)
        .map_err(|_| ServiceError::InvalidRequest)?
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)
}

fn required_string<'value>(
    arguments: &'value Map<String, Value>,
    field: &str,
) -> Result<&'value str, ServiceError> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

fn one_result(
    content: Value,
    request: &TypedToolRequest,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    TypedToolResult::try_new(
        content,
        1,
        ToolResultMetadata::complete_not_applicable(),
        admitted_result_limits(request, context)?,
    )
    .map_err(Into::into)
}

fn bounded_result(
    content: Value,
    returned: usize,
    available: usize,
    limits: market_squawk_services::ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let metadata = if returned < available {
        ToolResultMetadata::try_truncated_not_applicable(available)?
    } else {
        ToolResultMetadata::complete_not_applicable()
    };
    TypedToolResult::try_new(content, returned.max(1), metadata, limits).map_err(Into::into)
}

fn validate_resolved_input(
    input: &ValuationInput,
    producer: FairValueProducerKind,
    significance: InputSignificance,
    account_id: AccountId,
    instrument_id: InstrumentId,
) -> Result<(), ServiceError> {
    if input.subject_instrument_id() != instrument_id
        || input.significance() != significance
        || !origin_matches(producer, input.evidence().origin(), account_id)
    {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok(())
    }
}

fn resolve_governed_decision(
    state: &FairValueService,
    measurement_id: MeasurementId,
    decision_id: DecisionId,
) -> Result<GovernedFairValueDecisionEvidence, FairValueError> {
    let measurement = state
        .measurement(measurement_id)
        .ok_or(FairValueError::MeasurementNotFound)?;
    let decision = state
        .decision(decision_id)
        .ok_or(FairValueError::DecisionNotFound)?;
    if decision.measurement_id() != measurement_id
        || decision.evidence_hash() != measurement.evidence_hash()
    {
        return Err(FairValueError::InvalidMeasurement);
    }
    if decision.hierarchy() == FairValueHierarchy::Level1
        && (decision.basis() != DecisionBasis::Rules
            || measurement
                .inputs()
                .iter()
                .filter(|input| input.significance() == InputSignificance::Significant)
                .any(|input| {
                    input.market_access() != MarketAccess::Accessible
                        || input.market_access_assessment().is_none()
                }))
    {
        return Err(FairValueError::InvalidMeasurement);
    }
    Ok(GovernedFairValueDecisionEvidence {
        measurement_id,
        decision_id,
        evidence_hash: measurement.evidence_hash(),
        ruleset_hash: decision.ruleset_hash(),
        hierarchy: decision.hierarchy(),
        basis: decision.basis(),
    })
}

fn resolve_governed_approval(
    state: &FairValueService,
    approval_id: ValuationApprovalId,
    at: Timestamp,
) -> Result<GovernedFairValueApprovalEvidence, FairValueError> {
    let approval = state
        .approval(approval_id)
        .ok_or(FairValueError::ApprovalNotFound)?;
    if state.approval_status(approval_id, at)? != ApprovalStatus::Active {
        return Err(FairValueError::InvalidRevocationTime);
    }
    let decision =
        resolve_governed_decision(state, approval.measurement_id(), approval.decision_id())?;
    Ok(GovernedFairValueApprovalEvidence {
        approval_id,
        decision,
        expires_at: approval.expires_at(),
    })
}

fn resolve_governed_market_access(
    state: &FairValueService,
    account_id: AccountId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    effective_from: Timestamp,
) -> Result<GovernedFairValueMarketAccessEvidence, FairValueError> {
    let current_assessment_id = state
        .current_market_access(account_id, &venue_id, instrument_id, effective_from)?
        .map(|assessment| assessment.id());
    Ok(GovernedFairValueMarketAccessEvidence {
        account_id,
        venue_id,
        instrument_id,
        effective_from,
        current_assessment_id,
    })
}

/// Rejects every Level 1/Unclassified request and every hierarchy promotion before preview.
pub(crate) fn validate_governed_override(
    evidence: &GovernedFairValueDecisionEvidence,
    requested: FairValueHierarchy,
) -> Result<(), FairValueError> {
    let current_rank = match evidence.hierarchy {
        FairValueHierarchy::Level1 => 1_u8,
        FairValueHierarchy::Level2 => 2,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Unclassified => return Err(FairValueError::InvalidOverride),
    };
    let requested_rank = match requested {
        FairValueHierarchy::Level2 => 2_u8,
        FairValueHierarchy::Level3 => 3,
        FairValueHierarchy::Level1 | FairValueHierarchy::Unclassified => {
            return Err(FairValueError::InvalidOverride);
        }
    };
    if evidence.basis != DecisionBasis::Rules || requested_rank <= current_rank {
        return Err(FairValueError::InvalidOverride);
    }
    Ok(())
}

fn origin_matches(
    expected: FairValueProducerKind,
    origin: &EvidenceOrigin,
    account_id: AccountId,
) -> bool {
    match (expected, origin) {
        (FairValueProducerKind::Live, EvidenceOrigin::Market { .. })
        | (FairValueProducerKind::Research, EvidenceOrigin::Research { .. })
        | (FairValueProducerKind::Analytics, EvidenceOrigin::Analytics { .. }) => true,
        (
            FairValueProducerKind::Portfolio,
            EvidenceOrigin::Portfolio {
                account_id: evidence_account,
                ..
            },
        ) => *evidence_account == account_id,
        _ => false,
    }
}

fn map_selection_error(error: FairValueProducerSelectionError) -> ServiceError {
    match error {
        FairValueProducerSelectionError::InvalidSelection => ServiceError::InvalidRequest,
        FairValueProducerSelectionError::NotFound => ServiceError::NotFound,
        FairValueProducerSelectionError::Unauthorized => ServiceError::Unauthorized,
        FairValueProducerSelectionError::ResourceExhausted => ServiceError::ResourceExhausted,
        FairValueProducerSelectionError::Cancelled => ServiceError::Cancelled,
        FairValueProducerSelectionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        FairValueProducerSelectionError::Unavailable => ServiceError::Unavailable,
        FairValueProducerSelectionError::Internal => ServiceError::Internal,
    }
}

fn map_resolution_error(error: FairValueInputResolutionError) -> ServiceError {
    match error {
        FairValueInputResolutionError::InvalidReference => ServiceError::InvalidRequest,
        FairValueInputResolutionError::NotFound => ServiceError::NotFound,
        FairValueInputResolutionError::Unauthorized => ServiceError::Unauthorized,
        FairValueInputResolutionError::ResourceExhausted => ServiceError::ResourceExhausted,
        FairValueInputResolutionError::Cancelled => ServiceError::Cancelled,
        FairValueInputResolutionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        FairValueInputResolutionError::Unavailable => ServiceError::Unavailable,
        FairValueInputResolutionError::Internal => ServiceError::Internal,
    }
}

fn map_fair_value_error(error: FairValueError) -> ServiceError {
    match error {
        FairValueError::MeasurementNotFound
        | FairValueError::DecisionNotFound
        | FairValueError::ApprovalNotFound => ServiceError::NotFound,
        FairValueError::LimitExceeded { .. }
        | FairValueError::RetainedBytesExceeded { .. }
        | FairValueError::QueryLimitExceeded { .. } => ServiceError::ResourceExhausted,
        FairValueError::Persistence => ServiceError::Unavailable,
        FairValueError::CorruptPersistence | FairValueError::Arithmetic => ServiceError::Internal,
        FairValueError::SeparationOfDuties => ServiceError::Unauthorized,
        FairValueError::InvalidActorId
        | FairValueError::InvalidText
        | FairValueError::InvalidAmount
        | FairValueError::InvalidTime
        | FairValueError::InvalidEvidenceDigest
        | FairValueError::InvalidInstrumentRelationship
        | FairValueError::InvalidProducerEvidence
        | FairValueError::MissingProducerInstrument
        | FairValueError::InvalidInputAssessment
        | FairValueError::InvalidMarketAccessAssessment
        | FairValueError::InvalidMeasurement
        | FairValueError::InvalidRuleset
        | FairValueError::DuplicateInput
        | FairValueError::InvalidOverride
        | FairValueError::InvalidApprovalWindow
        | FairValueError::AlreadyRevoked
        | FairValueError::InvalidRevocationTime
        | FairValueError::InvalidAuditCursor => ServiceError::InvalidRequest,
    }
}
