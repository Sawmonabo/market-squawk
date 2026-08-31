//! Application-owned investment-target preparation and one-use admission receipts.

use std::{
    fmt,
    num::NonZeroU32,
    time::{Duration, Instant},
};

use market_squawk_decisions::{
    DecisionActorId, DecisionAuthority, DecisionContentDigest, DecisionDossier, DecisionText,
    DossierId, DossierSection, GovernedTargetSet, InvestmentTargetSet, InvestmentTargetSetId,
    ReferenceMark, TargetAssumption, TargetDecisionContext, TargetEvidence, TargetGovernanceInput,
    TargetMethod, TargetPriceCases, TargetPriceRange,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, Money, RevisionNumber, Timestamp,
};
use market_squawk_portfolio::PriceEvidence;
use market_squawk_runtime::{ServiceGeneration, WorkspaceId};
use market_squawk_services::RequestOrigin;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use super::{DecisionApplicationError, DecisionState, persist_outcome};

const TARGET_RULESET_VERSION: u32 = 1;
const MAXIMUM_REFERENCE_MARKS: usize = 4_096;
const MAXIMUM_PREPARED_TARGETS: usize = 256;
const ID_ALLOCATION_ATTEMPTS: usize = 16;
const RECEIPT_LIFETIME_NANOS: i64 = 300_000_000_000;
const RECEIPT_LIFETIME: Duration = Duration::from_secs(300);
const MAXIMUM_REFERENCE_MARK_AGE_NANOS: i64 = 604_800_000_000_000;
const DAY_NANOS: i64 = 86_400_000_000_000;

/// A target preparation request was not safely admissible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetPreparationError {
    /// The request selected malformed, contradictory, or policy-forbidden target semantics.
    InvalidRequest,
    /// The selected dossier, target, reference mark, or evidence option was not retained.
    NotFound,
    /// The target head or another preparation precondition changed before consumption.
    Conflict,
    /// The one-use receipt elapsed before it was consumed.
    Expired,
    /// The receipt did not belong to this origin, workspace, and service generation.
    FenceMismatch,
    /// A closed preparation registry bound was reached.
    Capacity,
    /// The durable decision authority failed while serving the operation.
    Application(DecisionApplicationError),
}

impl fmt::Display for TargetPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest => formatter.write_str("target preparation request is invalid"),
            Self::NotFound => formatter.write_str("target preparation evidence was not found"),
            Self::Conflict => formatter.write_str("target preparation precondition changed"),
            Self::Expired => formatter.write_str("target preparation receipt expired"),
            Self::FenceMismatch => formatter.write_str("target preparation receipt fence mismatch"),
            Self::Capacity => formatter.write_str("target preparation capacity is exhausted"),
            Self::Application(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for TargetPreparationError {}

impl From<DecisionApplicationError> for TargetPreparationError {
    fn from(error: DecisionApplicationError) -> Self {
        Self::Application(error)
    }
}

/// Exact installed authority boundary to which a preparation receipt is confined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetPreparationFence {
    origin: RequestOrigin,
    workspace_id: WorkspaceId,
    service_generation: ServiceGeneration,
}

impl TargetPreparationFence {
    /// Constructs a fence only when the installed request belongs to the active workspace.
    pub fn try_new(
        origin: RequestOrigin,
        workspace_id: WorkspaceId,
        service_generation: ServiceGeneration,
    ) -> Result<Self, TargetPreparationError> {
        if origin.workspace_id() != workspace_id.as_uuid() {
            return Err(TargetPreparationError::FenceMismatch);
        }
        Ok(Self {
            origin,
            workspace_id,
            service_generation,
        })
    }

    /// Installed transport origin bound into the receipt.
    pub const fn origin(self) -> RequestOrigin {
        self.origin
    }

    /// Active workspace bound into the receipt.
    pub const fn workspace_id(self) -> WorkspaceId {
        self.workspace_id
    }

    /// Running service generation bound into the receipt.
    pub const fn service_generation(self) -> ServiceGeneration {
        self.service_generation
    }
}

/// Opaque selector for an application-admitted reference mark.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TargetReferenceMarkSelector(Uuid);

impl TargetReferenceMarkSelector {
    /// Parses the opaque selector returned by the application.
    pub fn parse(value: &str) -> Result<Self, TargetPreparationError> {
        let value =
            Uuid::parse_str(value).map_err(|_error| TargetPreparationError::InvalidRequest)?;
        if value.is_nil() {
            return Err(TargetPreparationError::InvalidRequest);
        }
        Ok(Self(value))
    }
}

impl fmt::Display for TargetReferenceMarkSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl fmt::Debug for TargetReferenceMarkSelector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TargetReferenceMarkSelector([OPAQUE])")
    }
}

/// Opaque, expiring, one-use target admission receipt.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct TargetPreparationReceipt(Uuid);

impl TargetPreparationReceipt {
    /// Parses the opaque receipt returned by the application.
    pub fn parse(value: &str) -> Result<Self, TargetPreparationError> {
        let value =
            Uuid::parse_str(value).map_err(|_error| TargetPreparationError::InvalidRequest)?;
        if value.is_nil() {
            return Err(TargetPreparationError::InvalidRequest);
        }
        Ok(Self(value))
    }
}

impl fmt::Display for TargetPreparationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl fmt::Debug for TargetPreparationReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TargetPreparationReceipt([OPAQUE])")
    }
}

/// Research decision posture validated against the admitted reference mark and price ladder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetIntent {
    /// The reference mark is at or below the entry zone and base value is higher.
    Buy,
    /// The reference mark is at or above the trim zone and base value is lower.
    Sell,
    /// The reference mark remains strictly between entry and trim zones.
    Hold,
}

/// Code-owned temporal policy choices; callers never submit business timestamps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetHorizon {
    /// Ninety-day analytical horizon with monthly review.
    Quarter,
    /// One-year analytical horizon with quarterly review.
    Year,
    /// Three-year analytical horizon with semiannual review.
    ThreeYears,
}

impl TargetHorizon {
    fn offsets(self) -> (i64, i64) {
        match self {
            Self::Quarter => (90 * DAY_NANOS, 30 * DAY_NANOS),
            Self::Year => (365 * DAY_NANOS, 90 * DAY_NANOS),
            Self::ThreeYears => (1_095 * DAY_NANOS, 180 * DAY_NANOS),
        }
    }
}

/// Create a new series or derive the next revision of one retained target series.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetPreparationOperation {
    /// Allocate a code-owned target series identity and revision one.
    Create,
    /// Resolve the current head and derive its exact successor revision.
    Reevaluate { target_id: InvestmentTargetSetId },
}

/// Closed mutation endpoint that must match the operation retained behind a receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetPreparationCommitKind {
    /// Commit a new target series.
    Create,
    /// Commit the next revision of an existing target series.
    Reevaluate,
}

/// Price judgments accepted by preparation; identities, evidence, and timestamps are absent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetPriceDraft {
    pub downside: Money,
    pub add: Money,
    pub entry_lower: Money,
    pub entry_upper: Money,
    pub base: Money,
    pub trim_lower: Money,
    pub trim_upper: Money,
    pub exit_lower: Money,
    pub exit_upper: Money,
    pub upside: Money,
}

/// Closed authoritative source for one human-readable target assumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TargetAssumptionEvidenceSelection {
    /// The complete immutable dossier evidence set.
    Dossier,
    /// One exact reference in the dossier's bounded presentation order.
    DossierReference { index: usize },
    /// The selected forecast reference.
    Forecast,
    /// The dossier's typed fair-value classification decision.
    FairValue,
    /// The dossier's immutable portfolio revision.
    Portfolio,
    /// The selected producer-admitted reference mark.
    ReferenceMark,
}

/// Human-authored narrative plus a closed selection of application-owned evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetAssumptionDraft {
    pub text: Box<str>,
    pub evidence: TargetAssumptionEvidenceSelection,
}

/// Selection from one enumerated dossier/evidence inventory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetEvidenceSelection {
    pub reference_mark: TargetReferenceMarkSelector,
    pub forecast_reference: Option<usize>,
    pub use_fair_value: bool,
    pub use_portfolio: bool,
}

/// Presentation-authored target judgment with no authority-bearing identities or timestamps.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetPreparationDraft {
    pub operation: TargetPreparationOperation,
    pub dossier_id: DossierId,
    pub intent: TargetIntent,
    pub horizon: TargetHorizon,
    pub prices: TargetPriceDraft,
    pub method: TargetMethod,
    pub assumptions: Vec<TargetAssumptionDraft>,
    pub thesis: Box<str>,
    pub risks: Vec<Box<str>>,
    pub invalidation_conditions: Vec<Box<str>>,
    pub evidence: TargetEvidenceSelection,
}

/// One selectable forecast reference already committed inside the durable dossier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TargetForecastEvidenceOption {
    pub index: usize,
}

/// One selectable producer-owned reference mark, rendered without its evidence digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetReferenceMarkOption {
    pub selector: TargetReferenceMarkSelector,
    pub price: Money,
    pub observed_at: Timestamp,
    pub quality: DataQuality,
    pub source: Box<str>,
}

/// Human-safe evidence inventory for one exact retained dossier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetEvidenceInventory {
    pub dossier_id: DossierId,
    pub instrument_id: InstrumentId,
    pub assembled_at: Timestamp,
    pub forecast: Vec<TargetForecastEvidenceOption>,
    pub fair_value_available: bool,
    pub portfolio_available: bool,
    pub reference_marks: Vec<TargetReferenceMarkOption>,
}

/// Human preview paired with a non-inspectable authority receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTargetPreview {
    pub receipt: TargetPreparationReceipt,
    pub target_id: InvestmentTargetSetId,
    pub revision: RevisionNumber,
    pub dossier_id: DossierId,
    pub instrument_id: InstrumentId,
    pub intent: TargetIntent,
    pub reference_mark: Money,
    pub reference_mark_observed_at: Timestamp,
    pub reference_mark_quality: DataQuality,
    pub reference_mark_source: Box<str>,
    pub prices: TargetPriceDraft,
    pub method: TargetMethod,
    pub assumptions: Vec<TargetAssumptionDraft>,
    pub thesis: Box<str>,
    pub risks: Vec<Box<str>>,
    pub invalidation_conditions: Vec<Box<str>>,
    pub created_at: Timestamp,
    pub horizon_at: Timestamp,
    pub expires_at: Timestamp,
    pub review_due_at: Timestamp,
    pub receipt_expires_at: Timestamp,
    pub author: DecisionActorId,
    pub ruleset_version: NonZeroU32,
    pub forecast_selected: bool,
    pub fair_value_selected: bool,
    pub portfolio_selected: bool,
}

#[derive(Clone, Debug)]
struct AdmittedReferenceMark {
    selector: TargetReferenceMarkSelector,
    instrument_id: InstrumentId,
    mark: ReferenceMark,
    quality: DataQuality,
    source: Box<str>,
}

#[derive(Clone, Debug)]
enum PreparedOperation {
    Create,
    Reevaluate { expected: RevisionNumber },
}

impl PreparedOperation {
    const fn commit_kind(&self) -> TargetPreparationCommitKind {
        match self {
            Self::Create => TargetPreparationCommitKind::Create,
            Self::Reevaluate { .. } => TargetPreparationCommitKind::Reevaluate,
        }
    }
}

#[derive(Clone, Debug)]
struct PreparedTarget {
    receipt: TargetPreparationReceipt,
    fence: TargetPreparationFence,
    receipt_expires_at: Timestamp,
    receipt_deadline: Instant,
    binding_digest: [u8; 32],
    dossier_identity: DecisionContentDigest,
    reference_selector: TargetReferenceMarkSelector,
    reference_identity: DecisionContentDigest,
    operation: PreparedOperation,
    intent: TargetIntent,
    target: GovernedTargetSet,
}

#[derive(Debug, Default)]
pub(super) struct TargetPreparationAuthority {
    reference_marks: Vec<AdmittedReferenceMark>,
    prepared: Vec<PreparedTarget>,
}

impl TargetPreparationAuthority {
    pub(super) fn admit_reference_mark(
        &mut self,
        evidence: &PriceEvidence,
        quality: DataQuality,
        admitted_at: Timestamp,
    ) -> Result<TargetReferenceMarkSelector, TargetPreparationError> {
        if evidence.as_of() > admitted_at || !admitted_mark_quality(quality) {
            return Err(TargetPreparationError::InvalidRequest);
        }
        let identity = reference_mark_identity(evidence, quality)?;
        if let Some(existing) = self
            .reference_marks
            .iter()
            .find(|entry| entry.mark.content_identity() == identity)
        {
            return Ok(existing.selector);
        }
        if self.reference_marks.len() >= MAXIMUM_REFERENCE_MARKS {
            return Err(TargetPreparationError::Capacity);
        }
        let selector = (0..ID_ALLOCATION_ATTEMPTS)
            .map(|_attempt| TargetReferenceMarkSelector(Uuid::new_v4()))
            .find(|candidate| {
                self.reference_marks
                    .iter()
                    .all(|entry| entry.selector != *candidate)
            })
            .ok_or(TargetPreparationError::Capacity)?;
        let mark = ReferenceMark::try_new(evidence.price(), evidence.as_of(), identity)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        self.reference_marks.push(AdmittedReferenceMark {
            selector,
            instrument_id: evidence.instrument_id(),
            mark,
            quality,
            source: evidence.source().as_str().into(),
        });
        Ok(selector)
    }

    pub(super) fn inventory(
        &self,
        dossier: &DecisionDossier,
        now: Timestamp,
    ) -> Result<TargetEvidenceInventory, TargetPreparationError> {
        let instrument_id = dossier.dossier().instrument_id();
        if dossier.dossier().assembled_at() > now {
            return Err(TargetPreparationError::InvalidRequest);
        }
        let forecast = dossier
            .references()
            .iter()
            .enumerate()
            .filter(|(_index, reference)| reference.section() == DossierSection::Forecast)
            .map(|(index, _reference)| TargetForecastEvidenceOption { index })
            .collect();
        let reference_marks = self
            .reference_marks
            .iter()
            .filter(|entry| {
                entry.instrument_id == instrument_id && reference_mark_current(entry.mark, now)
            })
            .map(|entry| TargetReferenceMarkOption {
                selector: entry.selector,
                price: entry.mark.price(),
                observed_at: entry.mark.observed_at(),
                quality: entry.quality,
                source: entry.source.clone(),
            })
            .collect();
        Ok(TargetEvidenceInventory {
            dossier_id: dossier.dossier().id().clone(),
            instrument_id,
            assembled_at: dossier.dossier().assembled_at(),
            forecast,
            fair_value_available: dossier.dossier().evidence().fair_value_decision().is_some(),
            portfolio_available: dossier.dossier().evidence().portfolio_revision().is_some(),
            reference_marks,
        })
    }

    pub(super) fn prepare(
        &mut self,
        state: &DecisionStateView<'_>,
        fence: TargetPreparationFence,
        draft: TargetPreparationDraft,
        now: Timestamp,
    ) -> Result<PreparedTargetPreview, TargetPreparationError> {
        let monotonic_now = Instant::now();
        self.prepared.retain(|entry| {
            entry.receipt_expires_at > now && entry.receipt_deadline > monotonic_now
        });
        if self.prepared.len() >= MAXIMUM_PREPARED_TARGETS {
            return Err(TargetPreparationError::Capacity);
        }
        let dossier = state
            .dossier(&draft.dossier_id)
            .ok_or(TargetPreparationError::NotFound)?;
        let dossier_core = dossier.dossier();
        if dossier_core.assembled_at() > now {
            return Err(TargetPreparationError::InvalidRequest);
        }
        let reference = self
            .reference_marks
            .iter()
            .find(|entry| entry.selector == draft.evidence.reference_mark)
            .ok_or(TargetPreparationError::NotFound)?;
        if reference.instrument_id != dossier_core.instrument_id()
            || !reference_mark_current(reference.mark, now)
        {
            return Err(TargetPreparationError::InvalidRequest);
        }
        let selected_forecast = select_forecast(dossier, draft.evidence.forecast_reference)?;
        let selected_fair_value = if draft.evidence.use_fair_value {
            Some(
                dossier_core
                    .evidence()
                    .fair_value_decision()
                    .ok_or(TargetPreparationError::NotFound)?,
            )
        } else {
            None
        };
        let selected_portfolio = if draft.evidence.use_portfolio {
            Some(
                dossier_core
                    .evidence()
                    .portfolio_revision()
                    .cloned()
                    .ok_or(TargetPreparationError::NotFound)?,
            )
        } else {
            None
        };
        validate_method(draft.method, selected_forecast, selected_fair_value)?;
        let (cases, entry_range, trim_range, exit_range) =
            validate_prices(draft.prices, reference.mark.price(), draft.intent)?;
        let assumptions = draft
            .assumptions
            .iter()
            .map(|assumption| {
                let text = DecisionText::try_new(&assumption.text)
                    .map_err(|_error| TargetPreparationError::InvalidRequest)?;
                let identity = assumption_identity(
                    assumption.evidence,
                    dossier,
                    selected_forecast,
                    selected_fair_value,
                    selected_portfolio.as_ref(),
                    reference.mark,
                )?;
                Ok(TargetAssumption::new(text, identity))
            })
            .collect::<Result<Vec<_>, TargetPreparationError>>()?;
        let thesis = DecisionText::try_new(&draft.thesis)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let risks = decision_texts(&draft.risks)?;
        let invalidation_conditions = decision_texts(&draft.invalidation_conditions)?;
        let (target_id, revision, prepared_operation, supersedes) =
            state.resolve_operation(&draft.operation, dossier_core.instrument_id(), now)?;
        if matches!(prepared_operation, PreparedOperation::Create)
            && self
                .prepared
                .iter()
                .any(|entry| entry.target.target().id() == &target_id)
        {
            return Err(TargetPreparationError::Capacity);
        }
        let (horizon_offset, review_offset) = draft.horizon.offsets();
        let horizon_at = now
            .checked_add_nanos(horizon_offset)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let expires_at = horizon_at
            .checked_add_nanos(30 * DAY_NANOS)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let review_due_at = now
            .checked_add_nanos(review_offset)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let author =
            DecisionActorId::try_new(format!("client.{}", fence.origin().client_id().simple()))
                .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let ruleset_version = NonZeroU32::new(TARGET_RULESET_VERSION)
            .ok_or(TargetPreparationError::InvalidRequest)?;
        let evidence = TargetEvidence::new(selected_forecast, selected_fair_value);
        let context = TargetDecisionContext::new(draft.dossier_id.clone(), selected_portfolio);
        let provisional = TargetDigestInput {
            target_id: &target_id,
            revision,
            dossier_id: &draft.dossier_id,
            instrument_id: dossier_core.instrument_id(),
            reference_mark: reference.mark,
            cases,
            add_case: draft.prices.add,
            entry_range,
            trim_range,
            exit_range,
            method: draft.method,
            assumptions: &assumptions,
            context: &context,
            created_at: now,
            effective_at: now,
            horizon_at,
            expires_at,
            review_due_at,
            supersedes,
            thesis: &thesis,
            risks: &risks,
            invalidation_conditions: &invalidation_conditions,
            evidence,
            mark_quality: reference.quality,
            author: &author,
            ruleset_version,
            intent: draft.intent,
        };
        let content_identity = target_content_identity(&provisional)?;
        let target = InvestmentTargetSet::try_new(
            target_id.clone(),
            revision,
            draft.dossier_id,
            dossier_core.instrument_id(),
            reference.mark,
            cases,
            entry_range,
            trim_range,
            exit_range,
            now,
            horizon_at,
            expires_at,
            content_identity,
        )
        .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let target = GovernedTargetSet::try_new(TargetGovernanceInput {
            target,
            add_case: draft.prices.add,
            method: draft.method,
            assumptions,
            decision_context: context,
            effective_at: now,
            review_due_at,
            supersedes,
            thesis,
            risks,
            invalidation_conditions,
            evidence,
            mark_quality: reference.quality,
            author: author.clone(),
            ruleset_version,
        })
        .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let receipt = (0..ID_ALLOCATION_ATTEMPTS)
            .map(|_attempt| TargetPreparationReceipt(Uuid::new_v4()))
            .find(|candidate| {
                self.prepared
                    .iter()
                    .all(|entry| entry.receipt != *candidate)
            })
            .ok_or(TargetPreparationError::Capacity)?;
        let receipt_expires_at = now
            .checked_add_nanos(RECEIPT_LIFETIME_NANOS)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?;
        let receipt_deadline = monotonic_now
            .checked_add(RECEIPT_LIFETIME)
            .ok_or(TargetPreparationError::Capacity)?;
        let binding_digest = receipt_binding_digest(
            receipt,
            fence,
            receipt_expires_at,
            target.target().content_identity(),
        );
        let preview_reference_mark = reference.mark.price();
        let preview_reference_mark_observed_at = reference.mark.observed_at();
        let preview_reference_mark_quality = reference.quality;
        let preview_reference_mark_source = reference.source.clone();
        self.prepared.push(PreparedTarget {
            receipt,
            fence,
            receipt_expires_at,
            receipt_deadline,
            binding_digest,
            dossier_identity: dossier_core.evidence().content_identity(),
            reference_selector: reference.selector,
            reference_identity: reference.mark.content_identity(),
            operation: prepared_operation,
            intent: draft.intent,
            target: target.clone(),
        });
        Ok(PreparedTargetPreview {
            receipt,
            target_id,
            revision,
            dossier_id: target.target().dossier_id().clone(),
            instrument_id: dossier_core.instrument_id(),
            intent: draft.intent,
            reference_mark: preview_reference_mark,
            reference_mark_observed_at: preview_reference_mark_observed_at,
            reference_mark_quality: preview_reference_mark_quality,
            reference_mark_source: preview_reference_mark_source,
            prices: draft.prices,
            method: draft.method,
            assumptions: draft.assumptions,
            thesis: draft.thesis,
            risks: draft.risks,
            invalidation_conditions: draft.invalidation_conditions,
            created_at: now,
            horizon_at,
            expires_at,
            review_due_at,
            receipt_expires_at,
            author,
            ruleset_version,
            forecast_selected: selected_forecast.is_some(),
            fair_value_selected: selected_fair_value.is_some(),
            portfolio_selected: draft.evidence.use_portfolio,
        })
    }

    fn take(
        &mut self,
        receipt: TargetPreparationReceipt,
        fence: TargetPreparationFence,
        expected: TargetPreparationCommitKind,
        now: Timestamp,
    ) -> Result<PreparedTarget, TargetPreparationError> {
        let index = self
            .prepared
            .iter()
            .position(|entry| entry.receipt == receipt)
            .ok_or(TargetPreparationError::NotFound)?;
        if self.prepared[index].fence != fence {
            return Err(TargetPreparationError::FenceMismatch);
        }
        if self.prepared[index].operation.commit_kind() != expected {
            return Err(TargetPreparationError::InvalidRequest);
        }
        let entry = self.prepared.remove(index);
        if entry.receipt_expires_at <= now || Instant::now() >= entry.receipt_deadline {
            return Err(TargetPreparationError::Expired);
        }
        let expected = receipt_binding_digest(
            entry.receipt,
            entry.fence,
            entry.receipt_expires_at,
            entry.target.target().content_identity(),
        );
        if !bool::from(entry.binding_digest.ct_eq(&expected)) {
            return Err(TargetPreparationError::Conflict);
        }
        Ok(entry)
    }

    fn reference_mark(
        &self,
        selector: TargetReferenceMarkSelector,
    ) -> Option<&AdmittedReferenceMark> {
        self.reference_marks
            .iter()
            .find(|entry| entry.selector == selector)
    }
}

pub(super) struct DecisionStateView<'a> {
    pub(super) authority: &'a DecisionAuthority,
}

type ResolvedTargetPreparationOperation = (
    InvestmentTargetSetId,
    RevisionNumber,
    PreparedOperation,
    Option<(RevisionNumber, Timestamp)>,
);

impl DecisionStateView<'_> {
    fn dossier(&self, id: &DossierId) -> Option<&DecisionDossier> {
        self.authority.get_dossier(id).ok()
    }

    fn resolve_operation(
        &self,
        operation: &TargetPreparationOperation,
        instrument_id: InstrumentId,
        now: Timestamp,
    ) -> Result<ResolvedTargetPreparationOperation, TargetPreparationError> {
        match operation {
            TargetPreparationOperation::Create => {
                let target_id = (0..ID_ALLOCATION_ATTEMPTS)
                    .map(|_attempt| {
                        InvestmentTargetSetId::try_new(format!(
                            "target.{}",
                            Uuid::new_v4().simple()
                        ))
                        .map_err(|_error| TargetPreparationError::InvalidRequest)
                    })
                    .find_map(|candidate| match candidate {
                        Ok(candidate)
                            if self.authority.list_targets(&candidate).next().is_none() =>
                        {
                            Some(Ok(candidate))
                        }
                        Ok(_) => None,
                        Err(error) => Some(Err(error)),
                    })
                    .transpose()?
                    .ok_or(TargetPreparationError::Capacity)?;
                Ok((
                    target_id,
                    RevisionNumber::new(1)
                        .map_err(|_error| TargetPreparationError::InvalidRequest)?,
                    PreparedOperation::Create,
                    None,
                ))
            }
            TargetPreparationOperation::Reevaluate { target_id } => {
                let prior = self
                    .authority
                    .list_targets(target_id)
                    .last()
                    .ok_or(TargetPreparationError::NotFound)?;
                if prior.target().instrument_id() != instrument_id {
                    return Err(TargetPreparationError::InvalidRequest);
                }
                let expected = prior.target().revision();
                let revision = expected
                    .get()
                    .checked_add(1)
                    .ok_or(TargetPreparationError::Capacity)
                    .and_then(|value| {
                        RevisionNumber::new(value)
                            .map_err(|_error| TargetPreparationError::Capacity)
                    })?;
                Ok((
                    target_id.clone(),
                    revision,
                    PreparedOperation::Reevaluate { expected },
                    Some((expected, now)),
                ))
            }
        }
    }
}

pub(super) fn consume_prepared(
    state: &mut DecisionState,
    receipt: TargetPreparationReceipt,
    fence: TargetPreparationFence,
    expected: TargetPreparationCommitKind,
    now: Timestamp,
) -> Result<market_squawk_decisions::AppendOutcome, TargetPreparationError> {
    let prepared = state.preparation.take(receipt, fence, expected, now)?;
    let dossier = state
        .authority
        .get_dossier(prepared.target.target().dossier_id())
        .map_err(|_error| TargetPreparationError::Conflict)?;
    if dossier.dossier().evidence().content_identity() != prepared.dossier_identity {
        return Err(TargetPreparationError::Conflict);
    }
    let reference = state
        .preparation
        .reference_mark(prepared.reference_selector)
        .ok_or(TargetPreparationError::Conflict)?;
    if reference.mark.content_identity() != prepared.reference_identity
        || reference.instrument_id != prepared.target.target().instrument_id()
        || !reference_mark_current(reference.mark, now)
    {
        return Err(TargetPreparationError::Conflict);
    }
    revalidate_prepared_semantics(&prepared.target, prepared.intent)?;
    let recomputed = target_content_identity(&TargetDigestInput::from_governed(
        &prepared.target,
        prepared.intent,
    ))?;
    if recomputed != prepared.target.target().content_identity() {
        return Err(TargetPreparationError::Conflict);
    }
    let encoded = super::codec::target(&prepared.target)?;
    let outcome = match prepared.operation {
        PreparedOperation::Create => {
            if state
                .authority
                .list_targets(prepared.target.target().id())
                .next()
                .is_some()
            {
                return Err(TargetPreparationError::Conflict);
            }
            state
                .authority
                .create_target(prepared.target)
                .map_err(DecisionApplicationError::from)?
        }
        PreparedOperation::Reevaluate { expected } => {
            let head = state
                .authority
                .list_targets(prepared.target.target().id())
                .last()
                .map(|target| target.target().revision());
            if head != Some(expected) {
                return Err(TargetPreparationError::Conflict);
            }
            state
                .authority
                .reevaluate_target(expected, prepared.target)
                .map_err(DecisionApplicationError::from)?
        }
    };
    persist_outcome(state, &encoded, outcome).map_err(Into::into)
}

fn revalidate_prepared_semantics(
    target: &GovernedTargetSet,
    intent: TargetIntent,
) -> Result<(), TargetPreparationError> {
    let core = target.target();
    let cases = core.cases();
    let entry = core.entry_range();
    let trim = core.trim_range();
    let exit = core.exit_range();
    validate_prices(
        TargetPriceDraft {
            downside: cases.downside(),
            add: target.add_case(),
            entry_lower: entry.lower(),
            entry_upper: entry.upper(),
            base: cases.base(),
            trim_lower: trim.lower(),
            trim_upper: trim.upper(),
            exit_lower: exit.lower(),
            exit_upper: exit.upper(),
            upside: cases.upside(),
        },
        core.reference_mark().price(),
        intent,
    )?;
    validate_method(
        target.method(),
        target.evidence().forecast(),
        target.evidence().fair_value(),
    )
}

fn select_forecast(
    dossier: &DecisionDossier,
    selected: Option<usize>,
) -> Result<Option<DecisionContentDigest>, TargetPreparationError> {
    selected
        .map(|index| {
            dossier
                .references()
                .get(index)
                .filter(|reference| reference.section() == DossierSection::Forecast)
                .map(|reference| reference.content_identity())
                .ok_or(TargetPreparationError::NotFound)
        })
        .transpose()
}

fn validate_method(
    method: TargetMethod,
    forecast: Option<DecisionContentDigest>,
    fair_value: Option<market_squawk_valuation::DecisionId>,
) -> Result<(), TargetPreparationError> {
    let valid = match method {
        TargetMethod::ComparableEvidence => forecast.is_some() || fair_value.is_some(),
        TargetMethod::DiscountedCashFlow
        | TargetMethod::ResidualIncome
        | TargetMethod::ForecastDistribution => forecast.is_some(),
        TargetMethod::FairValueMeasurement => fair_value.is_some(),
    };
    if valid {
        Ok(())
    } else {
        Err(TargetPreparationError::InvalidRequest)
    }
}

fn validate_prices(
    prices: TargetPriceDraft,
    reference: Money,
    intent: TargetIntent,
) -> Result<
    (
        TargetPriceCases,
        TargetPriceRange,
        TargetPriceRange,
        TargetPriceRange,
    ),
    TargetPreparationError,
> {
    let values = [
        prices.downside,
        prices.add,
        prices.entry_lower,
        prices.entry_upper,
        prices.base,
        prices.trim_lower,
        prices.trim_upper,
        prices.exit_lower,
        prices.exit_upper,
        prices.upside,
    ];
    if values
        .iter()
        .any(|value| value.currency() != reference.currency() || value.amount().is_sign_negative())
        || values
            .windows(2)
            .any(|pair| pair[0].amount() > pair[1].amount())
    {
        return Err(TargetPreparationError::InvalidRequest);
    }
    let intent_valid = match intent {
        TargetIntent::Buy => {
            reference.amount() <= prices.entry_upper.amount()
                && prices.base.amount() > reference.amount()
        }
        TargetIntent::Sell => {
            reference.amount() >= prices.trim_lower.amount()
                && prices.base.amount() < reference.amount()
        }
        TargetIntent::Hold => {
            reference.amount() > prices.entry_upper.amount()
                && reference.amount() < prices.trim_lower.amount()
        }
    };
    if !intent_valid {
        return Err(TargetPreparationError::InvalidRequest);
    }
    Ok((
        TargetPriceCases::try_new(prices.downside, prices.base, prices.upside)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?,
        TargetPriceRange::try_new(prices.entry_lower, prices.entry_upper)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?,
        TargetPriceRange::try_new(prices.trim_lower, prices.trim_upper)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?,
        TargetPriceRange::try_new(prices.exit_lower, prices.exit_upper)
            .map_err(|_error| TargetPreparationError::InvalidRequest)?,
    ))
}

fn assumption_identity(
    selection: TargetAssumptionEvidenceSelection,
    dossier: &DecisionDossier,
    forecast: Option<DecisionContentDigest>,
    fair_value: Option<market_squawk_valuation::DecisionId>,
    portfolio: Option<&market_squawk_portfolio::PortfolioRevisionToken>,
    reference: ReferenceMark,
) -> Result<DecisionContentDigest, TargetPreparationError> {
    match selection {
        TargetAssumptionEvidenceSelection::Dossier => {
            Ok(dossier.dossier().evidence().content_identity())
        }
        TargetAssumptionEvidenceSelection::DossierReference { index } => dossier
            .references()
            .get(index)
            .map(|reference| reference.content_identity())
            .ok_or(TargetPreparationError::NotFound),
        TargetAssumptionEvidenceSelection::Forecast => {
            forecast.ok_or(TargetPreparationError::NotFound)
        }
        TargetAssumptionEvidenceSelection::FairValue => fair_value
            .map(|value| derived_identity(b"market-squawk/target/fair-value/v1", &value.bytes()))
            .transpose()?
            .ok_or(TargetPreparationError::NotFound),
        TargetAssumptionEvidenceSelection::Portfolio => portfolio
            .map(|value| derived_identity(b"market-squawk/target/portfolio/v1", &value.bytes()))
            .transpose()?
            .ok_or(TargetPreparationError::NotFound),
        TargetAssumptionEvidenceSelection::ReferenceMark => Ok(reference.content_identity()),
    }
}

fn decision_texts(values: &[Box<str>]) -> Result<Vec<DecisionText>, TargetPreparationError> {
    values
        .iter()
        .map(|value| {
            DecisionText::try_new(value).map_err(|_error| TargetPreparationError::InvalidRequest)
        })
        .collect()
}

fn admitted_mark_quality(quality: DataQuality) -> bool {
    !matches!(
        quality,
        DataQuality::Modeled
            | DataQuality::Estimated
            | DataQuality::Stale
            | DataQuality::Quarantined
    )
}

fn reference_mark_current(mark: ReferenceMark, now: Timestamp) -> bool {
    if mark.observed_at() > now {
        return false;
    }
    now.unix_nanos()
        .checked_sub(mark.observed_at().unix_nanos())
        .is_some_and(|age| age <= MAXIMUM_REFERENCE_MARK_AGE_NANOS)
}

fn reference_mark_identity(
    evidence: &PriceEvidence,
    quality: DataQuality,
) -> Result<DecisionContentDigest, TargetPreparationError> {
    let mut hash = CanonicalHash::new(b"market-squawk/target-reference-mark/v1");
    hash.fixed(evidence.instrument_id().as_uuid().as_bytes());
    hash.money(evidence.price());
    hash.i64(evidence.as_of().unix_nanos());
    hash.bytes(evidence.source().as_str().as_bytes());
    hash.u8(quality_tag(quality));
    digest_identity(hash.finish())
}

fn derived_identity(
    domain: &[u8],
    bytes: &[u8],
) -> Result<DecisionContentDigest, TargetPreparationError> {
    let mut hash = CanonicalHash::new(domain);
    hash.fixed(bytes);
    digest_identity(hash.finish())
}

fn digest_identity(bytes: [u8; 32]) -> Result<DecisionContentDigest, TargetPreparationError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_error| TargetPreparationError::InvalidRequest)
}

struct TargetDigestInput<'a> {
    target_id: &'a InvestmentTargetSetId,
    revision: RevisionNumber,
    dossier_id: &'a DossierId,
    instrument_id: InstrumentId,
    reference_mark: ReferenceMark,
    cases: TargetPriceCases,
    add_case: Money,
    entry_range: TargetPriceRange,
    trim_range: TargetPriceRange,
    exit_range: TargetPriceRange,
    method: TargetMethod,
    assumptions: &'a [TargetAssumption],
    context: &'a TargetDecisionContext,
    created_at: Timestamp,
    effective_at: Timestamp,
    horizon_at: Timestamp,
    expires_at: Timestamp,
    review_due_at: Timestamp,
    supersedes: Option<(RevisionNumber, Timestamp)>,
    thesis: &'a DecisionText,
    risks: &'a [DecisionText],
    invalidation_conditions: &'a [DecisionText],
    evidence: TargetEvidence,
    mark_quality: DataQuality,
    author: &'a DecisionActorId,
    ruleset_version: NonZeroU32,
    intent: TargetIntent,
}

impl<'a> TargetDigestInput<'a> {
    fn from_governed(target: &'a GovernedTargetSet, intent: TargetIntent) -> Self {
        let core = target.target();
        Self {
            target_id: core.id(),
            revision: core.revision(),
            dossier_id: core.dossier_id(),
            instrument_id: core.instrument_id(),
            reference_mark: core.reference_mark(),
            cases: core.cases(),
            add_case: target.add_case(),
            entry_range: core.entry_range(),
            trim_range: core.trim_range(),
            exit_range: core.exit_range(),
            method: target.method(),
            assumptions: target.assumptions(),
            context: target.decision_context(),
            created_at: core.created_at(),
            effective_at: target.effective_at(),
            horizon_at: core.horizon_at(),
            expires_at: core.expires_at(),
            review_due_at: target.review_due_at(),
            supersedes: target.supersedes(),
            thesis: target.thesis(),
            risks: target.risks(),
            invalidation_conditions: target.invalidation_conditions(),
            evidence: target.evidence(),
            mark_quality: target.mark_quality(),
            author: target.author(),
            ruleset_version: target.ruleset_version(),
            intent,
        }
    }
}

fn target_content_identity(
    input: &TargetDigestInput<'_>,
) -> Result<DecisionContentDigest, TargetPreparationError> {
    let mut hash = CanonicalHash::new(b"market-squawk/investment-target/v1");
    hash.bytes(input.target_id.as_str().as_bytes());
    hash.u32(input.revision.get());
    hash.bytes(input.dossier_id.as_str().as_bytes());
    hash.fixed(input.instrument_id.as_uuid().as_bytes());
    hash.money(input.reference_mark.price());
    hash.i64(input.reference_mark.observed_at().unix_nanos());
    hash.decision_digest(input.reference_mark.content_identity());
    hash.money(input.cases.downside());
    hash.money(input.add_case);
    hash.money(input.entry_range.lower());
    hash.money(input.entry_range.upper());
    hash.money(input.cases.base());
    hash.money(input.trim_range.lower());
    hash.money(input.trim_range.upper());
    hash.money(input.exit_range.lower());
    hash.money(input.exit_range.upper());
    hash.money(input.cases.upside());
    hash.u8(method_tag(input.method));
    hash.u8(intent_tag(input.intent));
    hash.u64(
        u64::try_from(input.assumptions.len())
            .map_err(|_error| TargetPreparationError::Capacity)?,
    );
    for assumption in input.assumptions {
        hash.bytes(assumption.text().as_str().as_bytes());
        hash.decision_digest(assumption.evidence_identity());
    }
    hash.bytes(input.context.dossier_id().as_str().as_bytes());
    match input.context.portfolio_revision() {
        Some(revision) => {
            hash.u8(1);
            hash.fixed(&revision.bytes());
        }
        None => hash.u8(0),
    }
    for time in [
        input.created_at,
        input.effective_at,
        input.horizon_at,
        input.expires_at,
        input.review_due_at,
    ] {
        hash.i64(time.unix_nanos());
    }
    match input.supersedes {
        Some((revision, at)) => {
            hash.u8(1);
            hash.u32(revision.get());
            hash.i64(at.unix_nanos());
        }
        None => hash.u8(0),
    }
    hash.bytes(input.thesis.as_str().as_bytes());
    hash.texts(input.risks)?;
    hash.texts(input.invalidation_conditions)?;
    match input.evidence.forecast() {
        Some(forecast) => {
            hash.u8(1);
            hash.decision_digest(forecast);
        }
        None => hash.u8(0),
    }
    match input.evidence.fair_value() {
        Some(fair_value) => {
            hash.u8(1);
            hash.fixed(&fair_value.bytes());
        }
        None => hash.u8(0),
    }
    hash.u8(quality_tag(input.mark_quality));
    hash.bytes(input.author.as_str().as_bytes());
    hash.u32(input.ruleset_version.get());
    digest_identity(hash.finish())
}

fn receipt_binding_digest(
    receipt: TargetPreparationReceipt,
    fence: TargetPreparationFence,
    expires_at: Timestamp,
    target_identity: DecisionContentDigest,
) -> [u8; 32] {
    let mut hash = CanonicalHash::new(b"market-squawk/target-preparation-receipt/v1");
    hash.fixed(receipt.0.as_bytes());
    hash.fixed(fence.origin().workspace_id().as_bytes());
    hash.fixed(fence.origin().client_id().as_bytes());
    hash.fixed(fence.workspace_id().as_uuid().as_bytes());
    hash.u64(fence.service_generation().get());
    hash.i64(expires_at.unix_nanos());
    hash.decision_digest(target_identity);
    hash.finish()
}

fn method_tag(method: TargetMethod) -> u8 {
    match method {
        TargetMethod::ComparableEvidence => 1,
        TargetMethod::DiscountedCashFlow => 2,
        TargetMethod::ResidualIncome => 3,
        TargetMethod::ForecastDistribution => 4,
        TargetMethod::FairValueMeasurement => 5,
    }
}

fn intent_tag(intent: TargetIntent) -> u8 {
    match intent {
        TargetIntent::Buy => 1,
        TargetIntent::Sell => 2,
        TargetIntent::Hold => 3,
    }
}

fn quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

struct CanonicalHash(Sha256);

impl CanonicalHash {
    fn new(domain: &[u8]) -> Self {
        let mut hash = Sha256::new();
        hash.update((domain.len() as u64).to_be_bytes());
        hash.update(domain);
        Self(hash)
    }

    fn u8(&mut self, value: u8) {
        self.0.update([value]);
    }

    fn u32(&mut self, value: u32) {
        self.0.update(value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.update(value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.update(value.to_be_bytes());
    }

    fn fixed(&mut self, value: &[u8]) {
        self.0.update(value);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.update(value);
    }

    fn money(&mut self, value: Money) {
        let amount: Decimal = value.amount().normalize();
        self.0.update(amount.mantissa().to_be_bytes());
        self.u32(amount.scale());
        self.fixed(value.currency().as_str().as_bytes());
    }

    fn decision_digest(&mut self, value: DecisionContentDigest) {
        let digest = value.evidence_digest();
        self.u8(match digest.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        });
        self.fixed(&digest.bytes());
    }

    fn texts(&mut self, values: &[DecisionText]) -> Result<(), TargetPreparationError> {
        self.u64(u64::try_from(values.len()).map_err(|_error| TargetPreparationError::Capacity)?);
        for value in values {
            self.bytes(value.as_str().as_bytes());
        }
        Ok(())
    }

    fn finish(self) -> [u8; 32] {
        self.0.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::Currency;

    use super::*;

    #[test]
    fn preparation_receipt_is_fenced_and_consumed_once() -> Result<(), Box<dyn std::error::Error>> {
        let workspace_uuid = Uuid::new_v4();
        let workspace_id = WorkspaceId::try_from_uuid(workspace_uuid)?;
        let generation = ServiceGeneration::try_new(7)?;
        let fence = TargetPreparationFence::try_new(
            RequestOrigin::try_new(workspace_uuid, Uuid::new_v4())?,
            workspace_id,
            generation,
        )?;
        let wrong_fence = TargetPreparationFence::try_new(
            RequestOrigin::try_new(workspace_uuid, Uuid::new_v4())?,
            workspace_id,
            generation,
        )?;
        let target = governed_target()?;
        let receipt = TargetPreparationReceipt(Uuid::new_v4());
        let receipt_expires_at = Timestamp::from_unix_nanos(100);
        let binding_digest = receipt_binding_digest(
            receipt,
            fence,
            receipt_expires_at,
            target.target().content_identity(),
        );
        let mut authority = TargetPreparationAuthority::default();
        authority.prepared.push(PreparedTarget {
            receipt,
            fence,
            receipt_expires_at,
            receipt_deadline: Instant::now()
                .checked_add(RECEIPT_LIFETIME)
                .ok_or(TargetPreparationError::Capacity)?,
            binding_digest,
            dossier_identity: digest(2)?,
            reference_selector: TargetReferenceMarkSelector(Uuid::new_v4()),
            reference_identity: digest(3)?,
            operation: PreparedOperation::Create,
            intent: TargetIntent::Buy,
            target,
        });

        assert!(matches!(
            authority.take(
                receipt,
                wrong_fence,
                TargetPreparationCommitKind::Create,
                Timestamp::from_unix_nanos(1),
            ),
            Err(TargetPreparationError::FenceMismatch)
        ));
        assert!(matches!(
            authority.take(
                receipt,
                fence,
                TargetPreparationCommitKind::Reevaluate,
                Timestamp::from_unix_nanos(1),
            ),
            Err(TargetPreparationError::InvalidRequest)
        ));
        let consumed = authority.take(
            receipt,
            fence,
            TargetPreparationCommitKind::Create,
            Timestamp::from_unix_nanos(1),
        )?;
        assert_eq!(consumed.receipt, receipt);
        assert!(matches!(
            authority.take(
                receipt,
                fence,
                TargetPreparationCommitKind::Create,
                Timestamp::from_unix_nanos(1),
            ),
            Err(TargetPreparationError::NotFound)
        ));
        Ok(())
    }

    fn governed_target() -> Result<GovernedTargetSet, Box<dyn std::error::Error>> {
        let currency = Currency::try_from("USD")?;
        let money = |amount| Money::new(Decimal::new(amount, 0), currency);
        let dossier_id = DossierId::try_new("dossier.receipt-test")?;
        let reference =
            ReferenceMark::try_new(money(100), Timestamp::from_unix_nanos(1), digest(4)?)?;
        let core = InvestmentTargetSet::try_new(
            InvestmentTargetSetId::try_new("target.receipt-test")?,
            RevisionNumber::new(1)?,
            dossier_id.clone(),
            InstrumentId::try_from(Uuid::new_v4())?,
            reference,
            TargetPriceCases::try_new(money(80), money(120), money(160))?,
            TargetPriceRange::try_new(money(90), money(100))?,
            TargetPriceRange::try_new(money(130), money(140))?,
            TargetPriceRange::try_new(money(150), money(160))?,
            Timestamp::from_unix_nanos(2),
            Timestamp::from_unix_nanos(20),
            Timestamp::from_unix_nanos(30),
            digest(5)?,
        )?;
        let forecast = digest(6)?;
        Ok(GovernedTargetSet::try_new(TargetGovernanceInput {
            target: core,
            add_case: money(85),
            method: TargetMethod::ForecastDistribution,
            assumptions: vec![TargetAssumption::new(
                DecisionText::try_new("bounded assumption")?,
                forecast,
            )],
            decision_context: TargetDecisionContext::new(dossier_id, None),
            effective_at: Timestamp::from_unix_nanos(2),
            review_due_at: Timestamp::from_unix_nanos(10),
            supersedes: None,
            thesis: DecisionText::try_new("bounded thesis")?,
            risks: vec![DecisionText::try_new("bounded risk")?],
            invalidation_conditions: vec![DecisionText::try_new("bounded invalidation")?],
            evidence: TargetEvidence::new(Some(forecast), None),
            mark_quality: DataQuality::DirectVerified,
            author: DecisionActorId::try_new("client.receipt-test")?,
            ruleset_version: NonZeroU32::new(1).ok_or(TargetPreparationError::InvalidRequest)?,
        })?)
    }

    fn digest(value: u8) -> Result<DecisionContentDigest, TargetPreparationError> {
        digest_identity([value; 32])
    }
}
