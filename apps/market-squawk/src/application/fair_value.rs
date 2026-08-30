//! Evidence-resolving fair-value application service.

use std::{collections::HashSet, fmt, num::NonZeroUsize, str::FromStr, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::DateTime;
use market_squawk_data::{DatasetId, FairValueCatalogCapability};
use market_squawk_domain::{
    AccountId, Currency, FairValueHierarchy, InstrumentId, Money, Timestamp, VenueId,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_valuation::{
    ActorId, ApprovalRevocation, ApprovalStatus, ApprovedMarketAccess, AuditEventId,
    AuditEventKind, ClassificationDecision, ClassificationRuleset, DecisionBasis, DecisionId,
    EvidenceOrigin, FairValueAuditCursor, FairValueAuditEvent, FairValueError,
    FairValueEvidenceHash, FairValueLimits, FairValueSelectionError, FairValueSelectionReceipt,
    FairValueSelectionRequest, FairValueService, InputId, InputSignificance, MarketAccess,
    MarketAccessAssessmentId, MarketPriceSelection, MeasurementId, OverrideProposal, RulesetHash,
    ValuationAmount, ValuationAmountBasis, ValuationApprovalId, ValuationInput,
    ValuationMeasurement, ValuationMeasurementSpec, ValuationMethod,
};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

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
    access_name, activity_name, adjustment_name, amount_value, approval_status_name,
    approval_value, classification_value, evidence_value, explanation_reason_value, hierarchy_name,
    market_access_assessment_value, measurement_value, method_name, observability_name,
    predicate_name, predicate_result_value, product_basis_value, quality_name, reason_name,
    relation_name, significance_name, timestamp_value,
};

const LIST_MEASUREMENTS: &str = "FairValue.ListMeasurements";
const GET_WORKSPACE: &str = "FairValue.GetWorkspace";
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
const BACKUP_ATTESTATION_MAGIC: [u8; 8] = *b"MSQFVA01";
const BACKUP_ATTESTATION_FORMAT_VERSION: u16 = 1;
const BACKUP_ATTESTATION_BYTES: usize = 115;
const RECOMMENDATION_MAXIMUM_ELIGIBLE_SELECTIONS: usize = 256;
const MAXIMUM_WORKFLOW_TOKENS: usize = 16_384;

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
    state: Arc<Mutex<FairValueService>>,
    workflow_tokens: Arc<Mutex<FairValueWorkflowTokens>>,
    resolver: Arc<dyn FairValueInputResolver>,
    selection_authority: Arc<dyn FairValueProducerSelectionAuthority>,
    ruleset: ClassificationRuleset,
    maximum_inputs: usize,
    maximum_query_results: usize,
    recommendation_maximum_eligible: NonZeroUsize,
    lifecycle: Arc<DomainLifecycle>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClassificationCoordinate {
    measurement_id: MeasurementId,
    decision_id: DecisionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MarketInputCoordinate {
    input_id: InputId,
    account_id: AccountId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProductTokenBinding<T> {
    token: Uuid,
    value: T,
}

struct ProductTokenStore<T> {
    bindings: Vec<ProductTokenBinding<T>>,
}

impl<T> Default for ProductTokenStore<T> {
    fn default() -> Self {
        Self {
            bindings: Vec::new(),
        }
    }
}

impl<T: Clone + Eq> ProductTokenStore<T> {
    fn token_for(&mut self, value: T) -> Result<Uuid, ServiceError> {
        if let Some(existing) = self.bindings.iter().find(|binding| binding.value == value) {
            return Ok(existing.token);
        }
        if self.bindings.len() >= MAXIMUM_WORKFLOW_TOKENS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.bindings
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        let token = Uuid::new_v4();
        self.bindings.push(ProductTokenBinding { token, value });
        Ok(token)
    }

    fn resolve(&self, token: Uuid) -> Result<T, ServiceError> {
        self.bindings
            .iter()
            .find(|binding| binding.token == token)
            .map(|binding| binding.value.clone())
            .ok_or(ServiceError::NotFound)
    }
}

#[derive(Default)]
struct FairValueWorkflowTokens {
    measurements: ProductTokenStore<MeasurementId>,
    classifications: ProductTokenStore<ClassificationCoordinate>,
    inputs: ProductTokenStore<InputId>,
    approvals: ProductTokenStore<ValuationApprovalId>,
    market_inputs: ProductTokenStore<MarketInputCoordinate>,
}

/// Cloneable read-only fair-value authority for personalized investment analysis.
///
/// The capability selects only retained, governed evidence for one exact account, instrument,
/// currency, and point-in-time cutoff. It carries no catalog mutation, producer-resolution, or
/// approval authority and accepts no caller-supplied money or governance identity.
#[derive(Clone)]
pub(crate) struct FairValueRecommendationReadCapability {
    state: Arc<Mutex<FairValueService>>,
    maximum_eligible: NonZeroUsize,
    lifecycle: Arc<DomainLifecycle>,
}

/// Typed read-only fair-value evidence for in-process dossier assembly.
///
/// This capability deliberately returns retained domain records rather than serialized product
/// JSON. Dossier assembly can therefore bind exact evidence identities without reconstructing
/// native authority from a transport representation.
#[derive(Clone)]
pub(crate) struct FairValueDossierReadCapability {
    state: Arc<Mutex<FairValueService>>,
    workflow_tokens: Arc<Mutex<FairValueWorkflowTokens>>,
    ruleset_hash: RulesetHash,
    maximum_scan: usize,
    lifecycle: Arc<DomainLifecycle>,
}

/// One dossier-eligible measurement and its exact current rules classification.
#[derive(Clone, Debug)]
pub(crate) struct FairValueDossierRecord {
    selector_token: Uuid,
    measurement: Arc<ValuationMeasurement>,
    decision: Arc<ClassificationDecision>,
}

impl FairValueDossierRecord {
    pub(crate) const fn selector_token(&self) -> Uuid {
        self.selector_token
    }

    pub(crate) fn measurement(&self) -> &ValuationMeasurement {
        &self.measurement
    }

    pub(crate) fn decision(&self) -> &ClassificationDecision {
        &self.decision
    }
}

impl FairValueDossierReadCapability {
    pub(crate) async fn records(
        &self,
        instrument_id: InstrumentId,
        selected_at: Timestamp,
        maximum: usize,
        context: &RequestContext,
    ) -> Result<Vec<FairValueDossierRecord>, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        if maximum == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        let scan_limit = self.maximum_scan.min(maximum.max(1));
        let state = lock_fair_value_state(&self.state, &self.lifecycle, context).await?;
        let measurements = state
            .measurements(scan_limit)
            .map_err(map_fair_value_error)?;
        let mut selected = Vec::new();
        selected
            .try_reserve(measurements.len().min(maximum))
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for measurement in measurements {
            if measurement.instrument_id() != instrument_id
                || measurement.measurement_at() > selected_at
                || measurement.prepared_at() > selected_at
            {
                continue;
            }
            let Some(decision) = state
                .rules_decision_for_measurement(measurement.id(), self.ruleset_hash)
                .map_err(map_fair_value_error)?
            else {
                continue;
            };
            selected.push((measurement, decision));
        }
        drop(state);
        selected.sort_unstable_by(
            |(left_measurement, left_decision), (right_measurement, right_decision)| {
                right_measurement
                    .measurement_at()
                    .cmp(&left_measurement.measurement_at())
                    .then_with(|| {
                        right_measurement
                            .prepared_at()
                            .cmp(&left_measurement.prepared_at())
                    })
                    .then_with(|| left_decision.id().cmp(&right_decision.id()))
            },
        );
        selected.truncate(maximum);

        let mut tokens = self.workflow_tokens.lock().await;
        let mut records = Vec::new();
        records
            .try_reserve_exact(selected.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for (measurement, decision) in selected {
            let selector_token = tokens.classifications.token_for(ClassificationCoordinate {
                measurement_id: measurement.id(),
                decision_id: decision.id(),
            })?;
            records.push(FairValueDossierRecord {
                selector_token,
                measurement,
                decision,
            });
        }
        drop(tokens);
        ensure_request_live(context, &self.lifecycle)?;
        Ok(records)
    }
}

impl FairValueRecommendationReadCapability {
    /// Selects the latest governed fair-value evidence without collapsing selector dispositions.
    pub(crate) async fn select_latest(
        &self,
        account_id: AccountId,
        instrument_id: InstrumentId,
        currency: Currency,
        as_of: Timestamp,
        context: &RequestContext,
    ) -> Result<FairValueSelectionReceipt, ServiceError> {
        let _call = DomainLifecycle::enter(&self.lifecycle, context)?;
        let request = FairValueSelectionRequest::new(
            instrument_id,
            currency,
            ValuationAmountBasis::PerInstrumentUnit,
            Some(account_id),
            as_of,
            self.maximum_eligible,
        );
        let state = lock_fair_value_state(&self.state, &self.lifecycle, context).await?;
        let selected = state.select_latest_fair_value(request);
        drop(state);
        ensure_request_live(context, &self.lifecycle)?;
        let receipt = selected.map_err(map_recommendation_selection_error)?;
        if receipt.request() != request {
            return Err(ServiceError::Internal);
        }
        Ok(receipt)
    }
}

impl fmt::Debug for FairValueRecommendationReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FairValueRecommendationReadCapability")
            .field("maximum_eligible", &self.maximum_eligible)
            .field("lifecycle", &self.lifecycle)
            .finish_non_exhaustive()
    }
}

/// Versioned owner-issued proof that service state and analytical catalog share one exact head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FairValueBackupAttestation {
    catalog_digest: [u8; 32],
    records: u64,
    operations: u64,
    memberships: u64,
    links: u64,
    last_audit_sequence: u64,
    last_audit_id: Option<[u8; 32]>,
}

impl FairValueBackupAttestation {
    fn try_from_service(service: &FairValueService) -> Result<Self, FairValueBackupError> {
        let (position, catalog_digest) = service.backup_attestation()?;
        Ok(Self {
            catalog_digest,
            records: u64::try_from(position.record_count())
                .map_err(|_| FairValueBackupError::InvalidEncoding)?,
            operations: u64::try_from(position.operation_count())
                .map_err(|_| FairValueBackupError::InvalidEncoding)?,
            memberships: u64::try_from(position.membership_count())
                .map_err(|_| FairValueBackupError::InvalidEncoding)?,
            links: u64::try_from(position.link_count())
                .map_err(|_| FairValueBackupError::InvalidEncoding)?,
            last_audit_sequence: position.last_audit_sequence(),
            last_audit_id: position.last_audit_id(),
        })
    }

    /// Returns the fixed v1 canonical artifact consumed by product backup and restore.
    pub(crate) fn canonical_bytes(self) -> [u8; BACKUP_ATTESTATION_BYTES] {
        let mut output = [0_u8; BACKUP_ATTESTATION_BYTES];
        output[..8].copy_from_slice(&BACKUP_ATTESTATION_MAGIC);
        output[8..10].copy_from_slice(&BACKUP_ATTESTATION_FORMAT_VERSION.to_be_bytes());
        output[10..42].copy_from_slice(&self.catalog_digest);
        output[42..50].copy_from_slice(&self.records.to_be_bytes());
        output[50..58].copy_from_slice(&self.operations.to_be_bytes());
        output[58..66].copy_from_slice(&self.memberships.to_be_bytes());
        output[66..74].copy_from_slice(&self.links.to_be_bytes());
        output[74..82].copy_from_slice(&self.last_audit_sequence.to_be_bytes());
        if let Some(last_audit_id) = self.last_audit_id {
            output[82] = 1;
            output[83..115].copy_from_slice(&last_audit_id);
        }
        output
    }

    /// Decodes only the fixed canonical v1 attestation artifact.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, FairValueBackupError> {
        if bytes.len() != BACKUP_ATTESTATION_BYTES {
            return Err(FairValueBackupError::InvalidEncoding);
        }
        let mut decoder = FairValueBackupAttestationDecoder::new(bytes);
        if decoder.fixed::<8>()? != BACKUP_ATTESTATION_MAGIC
            || u16::from_be_bytes(decoder.fixed::<2>()?) != BACKUP_ATTESTATION_FORMAT_VERSION
        {
            return Err(FairValueBackupError::InvalidEncoding);
        }
        let catalog_digest = decoder.fixed::<32>()?;
        let records = u64::from_be_bytes(decoder.fixed::<8>()?);
        let operations = u64::from_be_bytes(decoder.fixed::<8>()?);
        let memberships = u64::from_be_bytes(decoder.fixed::<8>()?);
        let links = u64::from_be_bytes(decoder.fixed::<8>()?);
        let last_audit_sequence = u64::from_be_bytes(decoder.fixed::<8>()?);
        let has_last_audit_id = decoder.fixed::<1>()?[0];
        let last_audit_bytes = decoder.fixed::<32>()?;
        if !decoder.finished() {
            return Err(FairValueBackupError::InvalidEncoding);
        }
        let last_audit_id = match has_last_audit_id {
            0 if last_audit_bytes == [0; 32] => None,
            1 if last_audit_bytes != [0; 32] => Some(last_audit_bytes),
            _ => return Err(FairValueBackupError::InvalidEncoding),
        };
        if (last_audit_sequence == 0) != last_audit_id.is_none()
            || operations != last_audit_sequence
        {
            return Err(FairValueBackupError::InvalidEncoding);
        }
        Ok(Self {
            catalog_digest,
            records,
            operations,
            memberships,
            links,
            last_audit_sequence,
            last_audit_id,
        })
    }

    /// Reopens a fresh restored catalog and recomputes its complete logical identity.
    pub(crate) fn validate_restored_catalog(
        self,
        catalog: FairValueCatalogCapability,
        limits: FairValueLimits,
    ) -> Result<FairValueService, FairValueBackupError> {
        let restored = FairValueService::open(catalog, limits)?;
        if Self::try_from_service(&restored)? != self {
            return Err(FairValueBackupError::CatalogMismatch);
        }
        Ok(restored)
    }
}

/// Non-cloneable mutation fence retained across Fair Value backup materialization.
pub(crate) struct FairValueBackupAttestationLease {
    state: OwnedMutexGuard<FairValueService>,
    attestation: FairValueBackupAttestation,
}

impl FairValueBackupAttestationLease {
    /// Returns the exact owner-issued attestation captured before the common cutoff.
    pub(crate) const fn attestation(&self) -> FairValueBackupAttestation {
        self.attestation
    }

    /// Proves that the retained owner and catalog still match the emitted attestation.
    pub(crate) fn revalidate(&self) -> Result<(), FairValueBackupError> {
        if FairValueBackupAttestation::try_from_service(&self.state)? != self.attestation {
            return Err(FairValueBackupError::CatalogMismatch);
        }
        Ok(())
    }
}

impl fmt::Debug for FairValueBackupAttestationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FairValueBackupAttestationLease([RETAINED FAIR-VALUE WRITER])")
    }
}

/// Caller-safe backup attestation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum FairValueBackupError {
    /// The Fair Value writer could not recompute a complete catalog position.
    #[error("fair-value backup catalog verification failed")]
    FairValue(#[from] FairValueError),
    /// Cancellation won before the mutation fence was acquired.
    #[error("fair-value backup attestation was cancelled")]
    Cancelled,
    /// The fixed versioned attestation artifact is malformed.
    #[error("fair-value backup attestation encoding is invalid")]
    InvalidEncoding,
    /// The restored or retained catalog differs from the owner-issued attestation.
    #[error("fair-value backup attestation does not match the catalog")]
    CatalogMismatch,
}

struct FairValueBackupAttestationDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FairValueBackupAttestationDecoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn fixed<const LENGTH: usize>(&mut self) -> Result<[u8; LENGTH], FairValueBackupError> {
        let end = self
            .offset
            .checked_add(LENGTH)
            .ok_or(FairValueBackupError::InvalidEncoding)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(FairValueBackupError::InvalidEncoding)?
            .try_into()
            .map_err(|_| FairValueBackupError::InvalidEncoding)?;
        self.offset = end;
        Ok(value)
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
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
        let maximum_query_results = limits.max_query_results();
        let recommendation_maximum_eligible = NonZeroUsize::new(
            maximum_query_results.min(RECOMMENDATION_MAXIMUM_ELIGIBLE_SELECTIONS),
        )
        .ok_or(FairValueError::QueryLimitExceeded {
            requested: 0,
            limit: maximum_query_results,
        })?;
        Ok(Self {
            state: Arc::new(Mutex::new(service)),
            workflow_tokens: Arc::new(Mutex::new(FairValueWorkflowTokens::default())),
            resolver,
            selection_authority,
            ruleset: ClassificationRuleset::current(maximum_quote_age_nanos)?,
            maximum_inputs: limits.max_inputs_per_measurement(),
            maximum_query_results,
            recommendation_maximum_eligible,
            lifecycle: DomainLifecycle::new(),
        })
    }

    /// Issues a cloneable read-only capability for personalized investment analysis.
    pub(crate) fn recommendation_read_capability(&self) -> FairValueRecommendationReadCapability {
        FairValueRecommendationReadCapability {
            state: Arc::clone(&self.state),
            maximum_eligible: self.recommendation_maximum_eligible,
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }

    /// Issues a typed read-only capability for exact in-process dossier preparation.
    pub(crate) fn dossier_read_capability(&self) -> FairValueDossierReadCapability {
        FairValueDossierReadCapability {
            state: Arc::clone(&self.state),
            workflow_tokens: Arc::clone(&self.workflow_tokens),
            ruleset_hash: self.ruleset.hash(),
            maximum_scan: self.maximum_query_results,
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }

    /// Retains the sole Fair Value writer and captures its exact analytical-catalog attestation.
    pub(crate) async fn retain_backup_attestation(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<FairValueBackupAttestationLease, FairValueBackupError> {
        let state = Arc::clone(&self.state);
        let guard = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(FairValueBackupError::Cancelled),
            guard = state.lock_owned() => guard,
        };
        if cancellation.is_cancelled() {
            return Err(FairValueBackupError::Cancelled);
        }
        let attestation = FairValueBackupAttestation::try_from_service(&guard)?;
        Ok(FairValueBackupAttestationLease {
            state: guard,
            attestation,
        })
    }

    /// Resolves product workflow tokens to the immutable evidence used by governance.
    pub(crate) async fn governed_decision_evidence_for_tokens(
        &self,
        measurement_token: Uuid,
        classification_token: Uuid,
    ) -> Result<GovernedFairValueDecisionEvidence, FairValueError> {
        let tokens = self.workflow_tokens.lock().await;
        let measurement_id = tokens
            .measurements
            .resolve(measurement_token)
            .map_err(|_error| FairValueError::InvalidMeasurement)?;
        let coordinate = tokens
            .classifications
            .resolve(classification_token)
            .map_err(|_error| FairValueError::InvalidMeasurement)?;
        drop(tokens);
        if coordinate.measurement_id != measurement_id {
            return Err(FairValueError::InvalidMeasurement);
        }
        let state = self.state.lock().await;
        resolve_governed_decision(&state, measurement_id, coordinate.decision_id)
    }

    /// Resolves one product approval token to active immutable approval evidence.
    pub(crate) async fn governed_approval_evidence_for_token(
        &self,
        approval_token: Uuid,
        at: Timestamp,
    ) -> Result<GovernedFairValueApprovalEvidence, FairValueError> {
        let approval_id = self
            .workflow_tokens
            .lock()
            .await
            .approvals
            .resolve(approval_token)
            .map_err(|_error| FairValueError::ApprovalNotFound)?;
        let state = self.state.lock().await;
        resolve_governed_approval(&state, approval_id, at)
    }

    /// Resolves one market-input token to exact typed market evidence.
    pub(crate) async fn governed_market_access_evidence_for_token(
        &self,
        market_input_token: Uuid,
        effective_from: Timestamp,
    ) -> Result<GovernedFairValueMarketAccessEvidence, FairValueError> {
        let coordinate = self
            .workflow_tokens
            .lock()
            .await
            .market_inputs
            .resolve(market_input_token)
            .map_err(|_error| FairValueError::InvalidMarketAccessAssessment)?;
        let state = self.state.lock().await;
        resolve_governed_market_access(
            &state,
            coordinate.account_id,
            coordinate.venue_id,
            coordinate.instrument_id,
            effective_from,
        )
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

    async fn workspace(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let limits = admitted_result_limits(request, context)?;
        let at = admitted_timestamp(request.arguments(), "at")?;
        let requested_measurement = request
            .arguments()
            .get("measurementToken")
            .map(|value| {
                value
                    .as_str()
                    .ok_or(ServiceError::InvalidRequest)
                    .and_then(|value| {
                        Uuid::parse_str(value).map_err(|_error| ServiceError::InvalidRequest)
                    })
            })
            .transpose()?;
        let requested_measurement_id = match requested_measurement {
            Some(token) => Some(
                self.workflow_tokens
                    .lock()
                    .await
                    .measurements
                    .resolve(token)?,
            ),
            None => None,
        };
        let limit = limits
            .maximum_result_items()
            .min(self.maximum_query_results);
        let state = self.lock_state(context).await?;
        let available = state.measurement_count();
        let mut measurements = state.measurements(limit).map_err(map_fair_value_error)?;
        measurements.sort_unstable_by(|left, right| {
            right
                .measurement_at()
                .cmp(&left.measurement_at())
                .then_with(|| right.prepared_at().cmp(&left.prepared_at()))
                .then_with(|| left.id().cmp(&right.id()))
        });
        let mut summaries = Vec::new();
        summaries
            .try_reserve_exact(measurements.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for measurement in &measurements {
            let decision = state
                .rules_decision_for_measurement(measurement.id(), self.ruleset.hash())
                .map_err(map_fair_value_error)?;
            summaries.push((Arc::clone(measurement), decision));
        }
        let selected_measurement = match requested_measurement_id {
            Some(id) => state.measurement(id).ok_or(ServiceError::NotFound)?,
            None => match measurements.first() {
                Some(measurement) => Arc::clone(measurement),
                None => {
                    drop(state);
                    return bounded_result(
                        json!({"measurements": [], "selectedMeasurement": null}),
                        0,
                        available,
                        limits,
                    );
                }
            },
        };
        let selected_decision = state
            .rules_decision_for_measurement(selected_measurement.id(), self.ruleset.hash())
            .map_err(map_fair_value_error)?;
        let approvals = state
            .approvals_for_measurement(selected_measurement.id(), limit)
            .map_err(map_fair_value_error)?
            .into_iter()
            .map(|approval| {
                let status = state
                    .approval_status(approval.id(), at)
                    .map_err(map_fair_value_error)?;
                let revocation = state.revocation(approval.id());
                Ok((approval, status, revocation))
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        drop(state);

        let mut tokens = self.workflow_tokens.lock().await;
        let summary_values = summaries
            .iter()
            .map(|(measurement, decision)| {
                product_measurement_summary(&mut tokens, measurement, decision.as_deref())
            })
            .collect::<Result<Vec<_>, ServiceError>>()?;
        let selected_value = product_measurement_detail(
            &mut tokens,
            &selected_measurement,
            selected_decision.as_deref(),
            &approvals,
        )?;
        drop(tokens);
        bounded_result(
            json!({
                "measurements": summary_values,
                "selectedMeasurement": selected_value,
            }),
            summary_values.len(),
            available,
            limits,
        )
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
        let mut tokens = self.workflow_tokens.lock().await;
        let product_measurement =
            product_measurement_detail(&mut tokens, &retained, Some(&decision), &[])?;
        drop(tokens);
        one_result(
            json!({
                "measurement": product_measurement,
                "created": !measurement_replay,
                "classified": !classification_replay,
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
        lock_fair_value_state(&self.state, &self.lifecycle, context).await
    }
}

fn product_measurement_summary(
    tokens: &mut FairValueWorkflowTokens,
    measurement: &ValuationMeasurement,
    decision: Option<&ClassificationDecision>,
) -> Result<Value, ServiceError> {
    let measurement_token = tokens.measurements.token_for(measurement.id())?;
    let classification = decision
        .map(|decision| product_classification_value(tokens, measurement.id(), decision))
        .transpose()?;
    Ok(json!({
        "measurementToken": measurement_token,
        "accountId": measurement.account_id(),
        "instrumentId": measurement.instrument_id(),
        "amount": amount_value(measurement.amount()),
        "measurementAt": timestamp_value(measurement.measurement_at()),
        "preparedAt": timestamp_value(measurement.prepared_at()),
        "preparedBy": measurement.prepared_by().as_str(),
        "method": method_name(measurement.method()),
        "inputCount": measurement.inputs().len(),
        "classification": classification,
    }))
}

fn product_measurement_detail(
    tokens: &mut FairValueWorkflowTokens,
    measurement: &ValuationMeasurement,
    decision: Option<&ClassificationDecision>,
    approvals: &[(
        Arc<market_squawk_valuation::ValuationApproval>,
        ApprovalStatus,
        Option<Arc<ApprovalRevocation>>,
    )],
) -> Result<Value, ServiceError> {
    let mut value = product_measurement_summary(tokens, measurement, decision)?;
    let object = value.as_object_mut().ok_or(ServiceError::Internal)?;
    let inputs = measurement
        .inputs()
        .iter()
        .map(|input| product_input_value(tokens, measurement, input))
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let explanation = decision
        .map(|decision| product_explanation_value(tokens, decision))
        .transpose()?;
    let approvals = approvals
        .iter()
        .map(|(approval, status, revocation)| {
            product_approval_value(tokens, approval, *status, revocation.as_deref())
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    object.insert("inputs".to_owned(), Value::Array(inputs));
    object.insert("explanation".to_owned(), explanation.unwrap_or(Value::Null));
    object.insert("approvals".to_owned(), Value::Array(approvals));
    Ok(value)
}

fn product_classification_value(
    tokens: &mut FairValueWorkflowTokens,
    measurement_id: MeasurementId,
    decision: &ClassificationDecision,
) -> Result<Value, ServiceError> {
    if decision.measurement_id() != measurement_id {
        return Err(ServiceError::InvalidResult);
    }
    let classification_token = tokens.classifications.token_for(ClassificationCoordinate {
        measurement_id,
        decision_id: decision.id(),
    })?;
    Ok(json!({
        "classificationToken": classification_token,
        "hierarchy": hierarchy_name(decision.hierarchy()),
        "basis": product_basis_value(decision.basis()),
        "checkCount": decision.truth_table().len(),
        "reasonCount": decision.reasons().len(),
    }))
}

fn product_explanation_value(
    tokens: &mut FairValueWorkflowTokens,
    decision: &ClassificationDecision,
) -> Result<Value, ServiceError> {
    let checks = decision
        .truth_table()
        .iter()
        .map(|result| {
            Ok(json!({
                "inputToken": tokens.inputs.token_for(result.input_id())?,
                "check": predicate_name(result.predicate()),
                "passed": result.passed(),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    let reasons = decision
        .reasons()
        .iter()
        .map(|reason| {
            let input_token = reason
                .input_id()
                .map(|input_id| tokens.inputs.token_for(input_id))
                .transpose()?;
            Ok(json!({
                "inputToken": input_token,
                "reason": reason_name(reason.code()),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    Ok(json!({"checks": checks, "reasons": reasons}))
}

fn product_input_value(
    tokens: &mut FairValueWorkflowTokens,
    measurement: &ValuationMeasurement,
    input: &ValuationInput,
) -> Result<Value, ServiceError> {
    let input_token = tokens.inputs.token_for(input.id())?;
    let market_input_token = match input.evidence().origin() {
        EvidenceOrigin::Market { venue_id, .. } => {
            Some(tokens.market_inputs.token_for(MarketInputCoordinate {
                input_id: input.id(),
                account_id: measurement.account_id(),
                venue_id: venue_id.clone(),
                instrument_id: input.reference_instrument_id(),
            })?)
        }
        EvidenceOrigin::Research { .. }
        | EvidenceOrigin::Analytics { .. }
        | EvidenceOrigin::Portfolio { .. } => None,
    };
    let evidence = input.evidence();
    let (evidence_kind, evidence_label) = match evidence.origin() {
        EvidenceOrigin::Market { .. } => ("market_observation", "Current market observation"),
        EvidenceOrigin::Research { .. } => ("published_research", "Published research"),
        EvidenceOrigin::Analytics { .. } => ("analysis", "Analytical estimate"),
        EvidenceOrigin::Portfolio { .. } => ("portfolio", "Portfolio position"),
    };
    let market_access = input.market_access_assessment().map(|assessment| {
        json!({
            "conclusion": access_name(assessment.conclusion()),
            "effectiveFrom": timestamp_value(assessment.effective_from()),
            "effectiveUntil": timestamp_value(assessment.effective_until()),
            "rationale": assessment.rationale(),
            "preparedBy": assessment.prepared_by().as_str(),
            "preparedAt": timestamp_value(assessment.prepared_at()),
            "approvedBy": assessment.approved_by().as_str(),
            "approvedAt": timestamp_value(assessment.approved_at()),
        })
    });
    let use_assessment = input.use_assessment().map(|assessment| {
        json!({
            "relationship": relation_name(assessment.relationship()),
            "observability": observability_name(assessment.observability()),
            "adjustment": adjustment_name(assessment.adjustment()),
            "rationale": assessment.rationale(),
            "assessedBy": assessment.assessed_by().as_str(),
            "assessedAt": timestamp_value(assessment.assessed_at()),
        })
    });
    Ok(json!({
        "inputToken": input_token,
        "marketInputToken": market_input_token,
        "referenceInstrumentId": input.reference_instrument_id(),
        "relationship": relation_name(input.relationship()),
        "amount": amount_value(input.amount()),
        "significance": significance_name(input.significance()),
        "observability": observability_name(input.observability()),
        "adjustment": adjustment_name(input.adjustment()),
        "marketActivity": activity_name(input.market_activity()),
        "marketAccess": access_name(input.market_access()),
        "marketAccessAssessment": market_access,
        "dataQuality": quality_name(input.data_quality()),
        "useAssessment": use_assessment,
        "evidence": {
            "kind": evidence_kind,
            "label": evidence_label,
            "observedAt": evidence.source_timestamp().map(timestamp_value),
            "effectiveAt": evidence.effective_at().map(timestamp_value),
            "publishedAt": evidence.published_at().map(timestamp_value),
            "availableAt": evidence.available_at().map(timestamp_value),
            "receivedAt": evidence.received_at().map(timestamp_value),
            "validUntil": evidence.qualification_valid_until().map(timestamp_value),
            "recordedAt": timestamp_value(evidence.ingested_at()),
            "verification": match evidence.verification() {
                market_squawk_valuation::EvidenceVerification::Verified => "verified",
                market_squawk_valuation::EvidenceVerification::Unverified => "unverified",
            },
        },
    }))
}

fn product_approval_value(
    tokens: &mut FairValueWorkflowTokens,
    approval: &market_squawk_valuation::ValuationApproval,
    status: ApprovalStatus,
    revocation: Option<&ApprovalRevocation>,
) -> Result<Value, ServiceError> {
    let approval_token = tokens.approvals.token_for(approval.id())?;
    Ok(json!({
        "approvalToken": approval_token,
        "approvedBy": approval.approved_by().as_str(),
        "approvedAt": timestamp_value(approval.approved_at()),
        "expiresAt": timestamp_value(approval.expires_at()),
        "status": approval_status_name(status),
        "revocation": revocation.map(|revocation| json!({
            "revokedBy": revocation.revoked_by().as_str(),
            "revokedAt": timestamp_value(revocation.revoked_at()),
            "reason": revocation.reason(),
        })),
    }))
}

async fn lock_fair_value_state<'state>(
    state: &'state Mutex<FairValueService>,
    lifecycle: &DomainLifecycle,
    context: &RequestContext,
) -> Result<MutexGuard<'state, FairValueService>, ServiceError> {
    ensure_request_live(context, lifecycle)?;
    let deadline = tokio::time::Instant::from_std(context.deadline());
    tokio::select! {
        biased;
        _ = context.cancellation().cancelled() => Err(ServiceError::Cancelled),
        _ = lifecycle.shutdown_token().cancelled() => Err(ServiceError::Unavailable),
        _ = tokio::time::sleep_until(deadline) => Err(ServiceError::DeadlineExceeded),
        state = state.lock() => Ok(state),
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
            .field(
                "recommendation_maximum_eligible",
                &self.recommendation_maximum_eligible,
            )
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
            GET_WORKSPACE => self.workspace(&request, &context).await,
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
        let basis = match required_string(measurement, "amountBasis")? {
            "per_instrument_unit" => ValuationAmountBasis::PerInstrumentUnit,
            "reporting_entity_total" => ValuationAmountBasis::ReportingEntityTotal,
            "position_total" => ValuationAmountBasis::PositionTotal,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let amount = ValuationAmount::try_new(Money::new(decimal, currency), scale, basis)
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
    TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
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

fn map_recommendation_selection_error(error: FairValueSelectionError) -> ServiceError {
    match error {
        FairValueSelectionError::FairValue(error) => map_fair_value_error(error),
        FairValueSelectionError::TemporaryCapacityUnavailable { .. } => {
            ServiceError::ResourceExhausted
        }
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
