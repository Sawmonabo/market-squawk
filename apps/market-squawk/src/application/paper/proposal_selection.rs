//! Original decision provenance selected for an explicitly confirmed manual paper draft.
//! Selection and research prices grant no market, sizing, risk, or execution authority.

use market_squawk_decisions::{
    GeneratedInvestmentProposal, InvestmentAnalysisId, InvestmentAnalysisPublicationId,
    InvestmentProposalDecision, InvestmentProposalId, InvestmentTargetSetId,
    PreparedPublishedInvestmentAnalysis, RecommendationAction, RecommendationDerivationDigest,
    TargetState,
};
use market_squawk_domain::{
    Currency, DigestAlgorithm, InstrumentId, Money, OrderSide, RevisionNumber, Timestamp,
};
use market_squawk_execution::OrderTargetReference;
use market_squawk_services::{RequestOrigin, ServiceError};
use uuid::Uuid;

use super::{PaperController, TargetLadderSelector, map_decision_error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum PaperTargetSelection {
    Governed {
        id: InvestmentTargetSetId,
        revision: RevisionNumber,
    },
    Generated {
        action_token: Uuid,
        analysis_id: InvestmentAnalysisId,
        proposal_id: InvestmentProposalId,
        publication_id: InvestmentAnalysisPublicationId,
        derivation: RecommendationDerivationDigest,
    },
}

pub(super) enum ResolvedPaperTarget {
    Governed(TargetState),
    Generated {
        action_token: Uuid,
        original: Box<PreparedPublishedInvestmentAnalysis>,
    },
}

impl PaperTargetSelection {
    pub(super) fn reopen(
        &self,
        controller: &PaperController,
        origin: RequestOrigin,
        now: Timestamp,
    ) -> Result<ResolvedPaperTarget, ServiceError> {
        let resolved = match self {
            Self::Governed { id, revision } => {
                ResolvedPaperTarget::Governed(controller.current_active_target(id, *revision, now)?)
            }
            Self::Generated { action_token, .. } => {
                ResolvedPaperTarget::read_generated(controller, *action_token, origin, now)?
            }
        };
        if resolved.selection()? != *self {
            return Err(ServiceError::Unavailable);
        }
        Ok(resolved)
    }

    pub(super) fn token(&self) -> Result<Box<str>, ServiceError> {
        let encode = |parts: &[&[u8]]| {
            super::super::opaque_product_text_token(
                "paper_target_",
                b"market-squawk/product-paper-target-selection/v1\0",
                parts,
                512,
            )
            .map_err(|_| ServiceError::ResourceExhausted)
        };
        match self {
            Self::Governed {
                id,
                revision: value,
            } => {
                let revision = value.get().to_string();
                encode(&[b"governed", id.as_str().as_bytes(), revision.as_bytes()])
            }
            Self::Generated {
                analysis_id,
                proposal_id,
                publication_id,
                derivation,
                ..
            } => {
                let generated = [
                    analysis_id.bytes(),
                    proposal_id.bytes(),
                    publication_id.bytes(),
                    derivation.bytes(),
                ];
                encode(&[
                    b"generated",
                    &generated[0],
                    &generated[1],
                    &generated[2],
                    &generated[3],
                ])
            }
        }
    }
}

impl ResolvedPaperTarget {
    pub(super) fn read_generated(
        controller: &PaperController,
        action_token: Uuid,
        origin: RequestOrigin,
        now: Timestamp,
    ) -> Result<Self, ServiceError> {
        let analysis_id = controller
            .decisions
            .resolve_investment_analysis_product_token(action_token)
            .map_err(map_decision_error)?;
        let original = controller
            .decisions
            .get_prepared_published_investment_analysis(analysis_id)
            .map_err(map_decision_error)?;
        let read = controller
            .decisions
            .read_investment_analysis(analysis_id)
            .map_err(map_decision_error)?;
        let publication = original.publication();
        let InvestmentProposalDecision::Generated(proposal) = original.decision() else {
            return Err(ServiceError::Unavailable);
        };
        if proposal.action() == RecommendationAction::Hold
            || &read.decision != original.decision()
            || read.current.as_ref().map(|current| current.publication()) != Some(publication)
            || proposal.analysis_id() != analysis_id
            || publication.analysis_id() != analysis_id
            || publication.proposal_id() != Some(proposal.proposal_id())
            || publication.derivation_digest() != Some(proposal.derivation_digest())
            || proposal.evidence().as_of() > proposal.evidence().admitted_at()
            || proposal.evidence().admitted_at() > publication.published_at()
            || publication.published_at() > now
            || now >= proposal.expires_at()
            || now >= proposal.horizon_at()
        {
            return Err(ServiceError::Unavailable);
        }
        let provenance = original
            .request_provenance()
            .ok_or(ServiceError::Unavailable)?;
        if provenance.workspace_id() != *origin.workspace_id().as_bytes() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(Self::Generated {
            action_token,
            original: Box::new(original),
        })
    }

    pub(super) fn proposal(&self) -> Result<Option<&GeneratedInvestmentProposal>, ServiceError> {
        match self {
            Self::Governed(_) => Ok(None),
            Self::Generated { original, .. } => match original.decision() {
                InvestmentProposalDecision::Generated(proposal) => Ok(Some(proposal)),
                _ => Err(ServiceError::Unavailable),
            },
        }
    }

    pub(super) fn selection(&self) -> Result<PaperTargetSelection, ServiceError> {
        match self {
            Self::Governed(state) => Ok(PaperTargetSelection::Governed {
                id: state.target().target().id().clone(),
                revision: state.target().target().revision(),
            }),
            Self::Generated {
                action_token,
                original,
            } => {
                let proposal = self.proposal()?.ok_or(ServiceError::Unavailable)?;
                Ok(PaperTargetSelection::Generated {
                    action_token: *action_token,
                    analysis_id: proposal.analysis_id(),
                    proposal_id: proposal.proposal_id(),
                    publication_id: original.publication().publication_id(),
                    derivation: proposal.derivation_digest(),
                })
            }
        }
    }

    pub(super) fn instrument_id(&self) -> InstrumentId {
        match self {
            Self::Governed(state) => state.target().target().instrument_id(),
            Self::Generated { original, .. } => original.decision().evidence().instrument_id(),
        }
    }

    pub(super) fn currency(&self) -> Currency {
        match self {
            Self::Governed(state) => state.target().target().reference_mark().price().currency(),
            Self::Generated { original, .. } => original.decision().evidence().currency(),
        }
    }

    pub(super) fn expires_at(&self) -> Timestamp {
        match self {
            Self::Governed(state) => state.target().target().expires_at(),
            Self::Generated { original, .. } => original
                .decision()
                .expires_at()
                .min(original.decision().horizon_at()),
        }
    }

    pub(super) fn permits_side(&self, side: OrderSide) -> Result<bool, ServiceError> {
        Ok(match self.proposal()? {
            None => true,
            Some(proposal) => match proposal.action() {
                RecommendationAction::Buy | RecommendationAction::Add => side == OrderSide::Buy,
                RecommendationAction::Trim | RecommendationAction::Sell => side == OrderSide::Sell,
                RecommendationAction::Hold => false,
            },
        })
    }

    pub(super) fn price(&self, level: TargetLadderSelector) -> Result<Money, ServiceError> {
        if let Self::Governed(state) = self {
            return Ok(level.price(state));
        }
        let ladder = self
            .proposal()?
            .ok_or(ServiceError::Unavailable)?
            .price_ladder();
        Ok(match level {
            TargetLadderSelector::Downside => ladder.cases().downside(),
            TargetLadderSelector::Add => ladder.add_case(),
            TargetLadderSelector::EntryLower => ladder.entry_range().lower(),
            TargetLadderSelector::EntryUpper => ladder.entry_range().upper(),
            TargetLadderSelector::Base => ladder.cases().base(),
            TargetLadderSelector::TrimLower => ladder.trim_range().lower(),
            TargetLadderSelector::TrimUpper => ladder.trim_range().upper(),
            TargetLadderSelector::ExitLower => ladder.exit_range().lower(),
            TargetLadderSelector::ExitUpper => ladder.exit_range().upper(),
            TargetLadderSelector::Upside => ladder.cases().upside(),
        })
    }

    pub(super) fn reference(&self) -> Result<OrderTargetReference, ServiceError> {
        match self {
            Self::Governed(state) => {
                let core = state.target().target();
                let digest = core.content_identity().evidence_digest();
                if digest.algorithm() != DigestAlgorithm::Sha256 {
                    return Err(ServiceError::Unavailable);
                }
                OrderTargetReference::try_new(
                    core.id().as_str(),
                    std::num::NonZeroU64::new(u64::from(core.revision().get()))
                        .ok_or(ServiceError::Unavailable)?,
                    digest.bytes(),
                )
            }
            Self::Generated { .. } => {
                let proposal = self.proposal()?.ok_or(ServiceError::Unavailable)?;
                let mut id = String::new();
                id.try_reserve_exact(73)
                    .map_err(|_| ServiceError::ResourceExhausted)?;
                id.push_str("proposal.");
                use std::fmt::Write as _;
                for byte in proposal.proposal_id().bytes() {
                    write!(id, "{byte:02x}").map_err(|_| ServiceError::ResourceExhausted)?;
                }
                OrderTargetReference::try_new(
                    id,
                    std::num::NonZeroU64::MIN,
                    proposal.derivation_digest().bytes(),
                )
            }
        }
        .map_err(|_| ServiceError::Unavailable)
    }
}
