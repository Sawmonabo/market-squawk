use std::num::NonZeroU32;
use std::str::FromStr as _;

use market_squawk_decisions::{
    DecisionActorId, DecisionContentDigest, DossierId, GovernedTargetSet, InvalidationKind,
    InvestmentTargetSet, InvestmentTargetSetId, ReferenceMark, TargetAssumption,
    TargetDecisionContext, TargetEvidence, TargetGovernanceInput, TargetInvalidation,
    TargetInvalidationId, TargetMethod, TargetPriceCases, TargetPriceRange, TargetReview,
    TargetReviewDisposition, TargetReviewId,
};
use market_squawk_domain::{DataQuality, EvidenceDigest, InstrumentId, Money, Timestamp};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::DecisionId;
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::common::{content_digest, decision_text, revision, revision_key};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum TargetMethodWire {
    ComparableEvidence,
    DiscountedCashFlow,
    ResidualIncome,
    ForecastDistribution,
    FairValueMeasurement,
}

impl From<TargetMethod> for TargetMethodWire {
    fn from(value: TargetMethod) -> Self {
        match value {
            TargetMethod::ComparableEvidence => Self::ComparableEvidence,
            TargetMethod::DiscountedCashFlow => Self::DiscountedCashFlow,
            TargetMethod::ResidualIncome => Self::ResidualIncome,
            TargetMethod::ForecastDistribution => Self::ForecastDistribution,
            TargetMethod::FairValueMeasurement => Self::FairValueMeasurement,
        }
    }
}

impl From<TargetMethodWire> for TargetMethod {
    fn from(value: TargetMethodWire) -> Self {
        match value {
            TargetMethodWire::ComparableEvidence => Self::ComparableEvidence,
            TargetMethodWire::DiscountedCashFlow => Self::DiscountedCashFlow,
            TargetMethodWire::ResidualIncome => Self::ResidualIncome,
            TargetMethodWire::ForecastDistribution => Self::ForecastDistribution,
            TargetMethodWire::FairValueMeasurement => Self::FairValueMeasurement,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AssumptionWire {
    text: String,
    evidence_identity: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetWire {
    id: String,
    revision: u32,
    dossier_id: String,
    instrument_id: InstrumentId,
    reference_price: Money,
    reference_observed_at: Timestamp,
    reference_identity: EvidenceDigest,
    downside: Money,
    base: Money,
    upside: Money,
    entry_lower: Money,
    entry_upper: Money,
    trim_lower: Money,
    trim_upper: Money,
    exit_lower: Money,
    exit_upper: Money,
    created_at: Timestamp,
    horizon_at: Timestamp,
    expires_at: Timestamp,
    target_identity: EvidenceDigest,
    add_case: Money,
    method: TargetMethodWire,
    assumptions: Vec<AssumptionWire>,
    portfolio_revision: Option<[u8; 32]>,
    effective_at: Timestamp,
    review_due_at: Timestamp,
    supersedes_revision: Option<u32>,
    supersedes_at: Option<Timestamp>,
    thesis: String,
    risks: Vec<String>,
    invalidation_conditions: Vec<String>,
    forecast: Option<EvidenceDigest>,
    fair_value: Option<String>,
    mark_quality: DataQuality,
    author: String,
    ruleset_version: u32,
}

impl From<&GovernedTargetSet> for TargetWire {
    fn from(value: &GovernedTargetSet) -> Self {
        let target = value.target();
        let supersedes = value.supersedes();
        Self {
            id: target.id().as_str().to_owned(),
            revision: target.revision().get(),
            dossier_id: target.dossier_id().as_str().to_owned(),
            instrument_id: target.instrument_id(),
            reference_price: target.reference_mark().price(),
            reference_observed_at: target.reference_mark().observed_at(),
            reference_identity: target.reference_mark().content_identity().evidence_digest(),
            downside: target.cases().downside(),
            base: target.cases().base(),
            upside: target.cases().upside(),
            entry_lower: target.entry_range().lower(),
            entry_upper: target.entry_range().upper(),
            trim_lower: target.trim_range().lower(),
            trim_upper: target.trim_range().upper(),
            exit_lower: target.exit_range().lower(),
            exit_upper: target.exit_range().upper(),
            created_at: target.created_at(),
            horizon_at: target.horizon_at(),
            expires_at: target.expires_at(),
            target_identity: target.content_identity().evidence_digest(),
            add_case: value.add_case(),
            method: value.method().into(),
            assumptions: value
                .assumptions()
                .iter()
                .map(|assumption| AssumptionWire {
                    text: assumption.text().as_str().to_owned(),
                    evidence_identity: assumption.evidence_identity().evidence_digest(),
                })
                .collect(),
            portfolio_revision: value
                .decision_context()
                .portfolio_revision()
                .map(PortfolioRevisionToken::bytes),
            effective_at: value.effective_at(),
            review_due_at: value.review_due_at(),
            supersedes_revision: supersedes.map(|entry| entry.0.get()),
            supersedes_at: supersedes.map(|entry| entry.1),
            thesis: value.thesis().as_str().to_owned(),
            risks: value
                .risks()
                .iter()
                .map(|text| text.as_str().to_owned())
                .collect(),
            invalidation_conditions: value
                .invalidation_conditions()
                .iter()
                .map(|text| text.as_str().to_owned())
                .collect(),
            forecast: value
                .evidence()
                .forecast()
                .map(DecisionContentDigest::evidence_digest),
            fair_value: value.evidence().fair_value().map(|id| id.to_string()),
            mark_quality: value.mark_quality(),
            author: value.author().as_str().to_owned(),
            ruleset_version: value.ruleset_version().get(),
        }
    }
}

impl TargetWire {
    pub(super) fn key(&self) -> Result<String, DecisionApplicationError> {
        Ok(revision_key(&self.id, revision(self.revision)?))
    }

    pub(super) fn decode(self) -> Result<GovernedTargetSet, DecisionApplicationError> {
        let target = InvestmentTargetSet::try_new(
            InvestmentTargetSetId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            revision(self.revision)?,
            DossierId::try_new(&self.dossier_id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.instrument_id,
            ReferenceMark::try_new(
                self.reference_price,
                self.reference_observed_at,
                content_digest(self.reference_identity)?,
            )
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            TargetPriceCases::try_new(self.downside, self.base, self.upside)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            TargetPriceRange::try_new(self.entry_lower, self.entry_upper)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            TargetPriceRange::try_new(self.trim_lower, self.trim_upper)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            TargetPriceRange::try_new(self.exit_lower, self.exit_upper)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.created_at,
            self.horizon_at,
            self.expires_at,
            content_digest(self.target_identity)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let assumptions = self
            .assumptions
            .into_iter()
            .map(|assumption| {
                Ok(TargetAssumption::new(
                    decision_text(&assumption.text)?,
                    content_digest(assumption.evidence_identity)?,
                ))
            })
            .collect::<Result<Vec<_>, DecisionApplicationError>>()?;
        let supersedes = match (self.supersedes_revision, self.supersedes_at) {
            (None, None) => None,
            (Some(revision_value), Some(at)) => Some((revision(revision_value)?, at)),
            _ => return Err(DecisionApplicationError::InvalidPersistentState),
        };
        let evidence = TargetEvidence::new(
            self.forecast.map(content_digest).transpose()?,
            self.fair_value
                .as_deref()
                .map(DecisionId::from_str)
                .transpose()
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
        );
        GovernedTargetSet::try_new(TargetGovernanceInput {
            target,
            add_case: self.add_case,
            method: self.method.into(),
            assumptions,
            decision_context: TargetDecisionContext::new(
                DossierId::try_new(&self.dossier_id)
                    .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
                self.portfolio_revision
                    .map(PortfolioRevisionToken::from_bytes),
            ),
            effective_at: self.effective_at,
            review_due_at: self.review_due_at,
            supersedes,
            thesis: decision_text(&self.thesis)?,
            risks: self
                .risks
                .iter()
                .map(|text| decision_text(text))
                .collect::<Result<Vec<_>, _>>()?,
            invalidation_conditions: self
                .invalidation_conditions
                .iter()
                .map(|text| decision_text(text))
                .collect::<Result<Vec<_>, _>>()?,
            evidence,
            mark_quality: self.mark_quality,
            author: DecisionActorId::try_new(&self.author)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            ruleset_version: NonZeroU32::new(self.ruleset_version)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
        })
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReviewDispositionWire {
    Activate,
    Reject,
    NeedsChanges,
}

impl From<TargetReviewDisposition> for ReviewDispositionWire {
    fn from(value: TargetReviewDisposition) -> Self {
        match value {
            TargetReviewDisposition::Activate => Self::Activate,
            TargetReviewDisposition::Reject => Self::Reject,
            TargetReviewDisposition::NeedsChanges => Self::NeedsChanges,
        }
    }
}

impl From<ReviewDispositionWire> for TargetReviewDisposition {
    fn from(value: ReviewDispositionWire) -> Self {
        match value {
            ReviewDispositionWire::Activate => Self::Activate,
            ReviewDispositionWire::Reject => Self::Reject,
            ReviewDispositionWire::NeedsChanges => Self::NeedsChanges,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReviewWire {
    id: String,
    target_id: String,
    target_revision: u32,
    reviewer: String,
    reviewed_at: Timestamp,
    disposition: ReviewDispositionWire,
    content_identity: EvidenceDigest,
}

impl From<&TargetReview> for ReviewWire {
    fn from(value: &TargetReview) -> Self {
        Self {
            id: value.id().as_str().to_owned(),
            target_id: value.target_id().as_str().to_owned(),
            target_revision: value.target_revision().get(),
            reviewer: value.reviewer().as_str().to_owned(),
            reviewed_at: value.reviewed_at(),
            disposition: value.disposition().into(),
            content_identity: value.content_identity().evidence_digest(),
        }
    }
}

impl ReviewWire {
    pub(super) fn key(&self) -> &str {
        &self.id
    }

    pub(super) fn target_coordinate(
        &self,
    ) -> Result<
        (InvestmentTargetSetId, market_squawk_domain::RevisionNumber),
        DecisionApplicationError,
    > {
        Ok((
            InvestmentTargetSetId::try_new(&self.target_id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            revision(self.target_revision)?,
        ))
    }

    pub(super) fn decode(
        self,
        target: &InvestmentTargetSet,
    ) -> Result<TargetReview, DecisionApplicationError> {
        if target.id().as_str() != self.target_id || target.revision().get() != self.target_revision
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        TargetReview::try_new(
            TargetReviewId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            target,
            DecisionActorId::try_new(&self.reviewer)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.reviewed_at,
            self.disposition.into(),
            content_digest(self.content_identity)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum InvalidationKindWire {
    CorporateAction,
    Model,
    Data,
    ReferenceMark,
    Assumption,
}

impl From<InvalidationKind> for InvalidationKindWire {
    fn from(value: InvalidationKind) -> Self {
        match value {
            InvalidationKind::CorporateAction => Self::CorporateAction,
            InvalidationKind::Model => Self::Model,
            InvalidationKind::Data => Self::Data,
            InvalidationKind::ReferenceMark => Self::ReferenceMark,
            InvalidationKind::Assumption => Self::Assumption,
        }
    }
}

impl From<InvalidationKindWire> for InvalidationKind {
    fn from(value: InvalidationKindWire) -> Self {
        match value {
            InvalidationKindWire::CorporateAction => Self::CorporateAction,
            InvalidationKindWire::Model => Self::Model,
            InvalidationKindWire::Data => Self::Data,
            InvalidationKindWire::ReferenceMark => Self::ReferenceMark,
            InvalidationKindWire::Assumption => Self::Assumption,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InvalidationWire {
    id: String,
    target_id: String,
    target_revision: u32,
    kind: InvalidationKindWire,
    actor: String,
    observed_at: Timestamp,
    content_identity: EvidenceDigest,
}

impl From<&TargetInvalidation> for InvalidationWire {
    fn from(value: &TargetInvalidation) -> Self {
        Self {
            id: value.id().as_str().to_owned(),
            target_id: value.target_id().as_str().to_owned(),
            target_revision: value.target_revision().get(),
            kind: value.kind().into(),
            actor: value.actor().as_str().to_owned(),
            observed_at: value.observed_at(),
            content_identity: value.content_identity().evidence_digest(),
        }
    }
}

impl InvalidationWire {
    pub(super) fn key(&self) -> &str {
        &self.id
    }

    pub(super) fn target_coordinate(
        &self,
    ) -> Result<
        (InvestmentTargetSetId, market_squawk_domain::RevisionNumber),
        DecisionApplicationError,
    > {
        Ok((
            InvestmentTargetSetId::try_new(&self.target_id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            revision(self.target_revision)?,
        ))
    }

    pub(super) fn decode(
        self,
        target: &InvestmentTargetSet,
    ) -> Result<TargetInvalidation, DecisionApplicationError> {
        if target.id().as_str() != self.target_id || target.revision().get() != self.target_revision
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        TargetInvalidation::try_new(
            TargetInvalidationId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            target,
            self.kind.into(),
            DecisionActorId::try_new(self.actor)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.observed_at,
            content_digest(self.content_identity)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}
