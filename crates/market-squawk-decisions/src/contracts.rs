//! Immutable values that bind evidence across the decision-research boundary.

use std::num::NonZeroU32;

use market_squawk_analytics::{FeatureKey, FeatureSemanticDigest, StatisticalF64};
use market_squawk_domain::{InstrumentId, RevisionNumber, Timestamp};
use market_squawk_modeling::BundleId;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::DecisionId;

use crate::identity::{
    CandidateId, DecisionContentDigest, DecisionContractError, DossierId, ScreenId, ScreenRunId,
};

/// Maximum distinct feature semantics bound to one screen run.
pub const MAX_SCREEN_FEATURE_BINDINGS: usize = 128;

/// Exact immutable revision of a saved screen.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenRevision {
    id: ScreenId,
    revision: RevisionNumber,
}

impl ScreenRevision {
    /// Constructs a screen revision from already validated identities.
    #[must_use]
    pub const fn new(id: ScreenId, revision: RevisionNumber) -> Self {
        Self { id, revision }
    }

    /// Returns the stable saved-screen identity.
    #[must_use]
    pub const fn id(&self) -> &ScreenId {
        &self.id
    }

    /// Returns the one-based immutable revision.
    #[must_use]
    pub const fn revision(&self) -> RevisionNumber {
        self.revision
    }
}

/// One exact code-owned feature semantic consumed by a screen run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenFeatureBinding {
    key: FeatureKey,
    semantic_digest: FeatureSemanticDigest,
}

impl ScreenFeatureBinding {
    /// Binds a canonical feature key to its full semantic digest.
    #[must_use]
    pub const fn new(key: FeatureKey, semantic_digest: FeatureSemanticDigest) -> Self {
        Self {
            key,
            semantic_digest,
        }
    }

    /// Returns the exact feature name and version.
    #[must_use]
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns the digest of all execution-relevant feature semantics.
    #[must_use]
    pub const fn semantic_digest(&self) -> FeatureSemanticDigest {
        self.semantic_digest
    }
}

/// Point-in-time identity of one bounded screen execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenRun {
    id: ScreenRunId,
    screen: ScreenRevision,
    as_of: Timestamp,
    dataset_identity: DecisionContentDigest,
    universe_identity: DecisionContentDigest,
    feature_bindings: Box<[ScreenFeatureBinding]>,
}

impl ScreenRun {
    /// Constructs one screen execution identity from exact point-in-time inputs.
    ///
    /// The content identities are representation commitments only. The Task 11 application
    /// authority admits the corresponding dataset and universe receipts before construction.
    ///
    /// # Errors
    ///
    /// Rejects an empty, oversized, or duplicate feature-binding set.
    pub fn try_new(
        id: ScreenRunId,
        screen: ScreenRevision,
        as_of: Timestamp,
        dataset_identity: DecisionContentDigest,
        universe_identity: DecisionContentDigest,
        mut feature_bindings: Vec<ScreenFeatureBinding>,
    ) -> Result<Self, DecisionContractError> {
        if feature_bindings.is_empty() || feature_bindings.len() > MAX_SCREEN_FEATURE_BINDINGS {
            return Err(DecisionContractError::InvalidScreenFeatureCount);
        }
        feature_bindings.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if feature_bindings
            .windows(2)
            .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(DecisionContractError::DuplicateScreenFeature);
        }
        Ok(Self {
            id,
            screen,
            as_of,
            dataset_identity,
            universe_identity,
            feature_bindings: feature_bindings.into_boxed_slice(),
        })
    }

    /// Returns the immutable screen-run identity.
    #[must_use]
    pub const fn id(&self) -> &ScreenRunId {
        &self.id
    }

    /// Returns the exact saved-screen revision.
    #[must_use]
    pub const fn screen(&self) -> &ScreenRevision {
        &self.screen
    }

    /// Returns the point-in-time evaluation cutoff.
    #[must_use]
    pub const fn as_of(&self) -> Timestamp {
        self.as_of
    }

    /// Returns the exact point-in-time dataset content identity.
    #[must_use]
    pub const fn dataset_identity(&self) -> DecisionContentDigest {
        self.dataset_identity
    }

    /// Returns the exact historical-universe content identity.
    #[must_use]
    pub const fn universe_identity(&self) -> DecisionContentDigest {
        self.universe_identity
    }

    /// Returns the sorted unique feature semantics without allocation.
    #[must_use]
    pub fn feature_bindings(&self) -> &[ScreenFeatureBinding] {
        &self.feature_bindings
    }
}

/// Immutable ranked candidate selected by one exact screen run.
#[derive(Clone, Debug, PartialEq)]
pub struct CandidateRecord {
    id: CandidateId,
    screen_run_id: ScreenRunId,
    screen: ScreenRevision,
    instrument_id: InstrumentId,
    rank: NonZeroU32,
    score: StatisticalF64,
    selected_at: Timestamp,
}

impl CandidateRecord {
    /// Constructs a candidate that cannot predate its point-in-time screen run.
    ///
    /// # Errors
    ///
    /// Returns [`DecisionContractError::InvalidTimeOrder`] when selection predates the run cutoff.
    pub fn try_new(
        id: CandidateId,
        screen_run: &ScreenRun,
        instrument_id: InstrumentId,
        rank: NonZeroU32,
        score: StatisticalF64,
        selected_at: Timestamp,
    ) -> Result<Self, DecisionContractError> {
        if selected_at < screen_run.as_of {
            return Err(DecisionContractError::InvalidTimeOrder);
        }
        Ok(Self {
            id,
            screen_run_id: screen_run.id.clone(),
            screen: screen_run.screen.clone(),
            instrument_id,
            rank,
            score,
            selected_at,
        })
    }

    /// Returns the candidate identity.
    #[must_use]
    pub const fn id(&self) -> &CandidateId {
        &self.id
    }

    /// Returns the exact screen-run identity that selected the candidate.
    #[must_use]
    pub const fn screen_run_id(&self) -> &ScreenRunId {
        &self.screen_run_id
    }

    /// Returns the saved-screen revision used for selection.
    #[must_use]
    pub const fn screen(&self) -> &ScreenRevision {
        &self.screen
    }

    /// Returns the selected instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the one-based screen rank.
    #[must_use]
    pub const fn rank(&self) -> NonZeroU32 {
        self.rank
    }

    /// Returns the finite research score; it is not an executable price.
    #[must_use]
    pub const fn score(&self) -> StatisticalF64 {
        self.score
    }

    /// Returns when this candidate record was selected.
    #[must_use]
    pub const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }
}

/// Existing authoritative identities assembled into a dossier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DossierEvidence {
    model_bundle: Option<BundleId>,
    portfolio_revision: Option<PortfolioRevisionToken>,
    fair_value_decision: Option<DecisionId>,
    content_identity: DecisionContentDigest,
}

impl DossierEvidence {
    /// Constructs a dossier evidence set without manufacturing upstream identities.
    #[must_use]
    pub const fn new(
        model_bundle: Option<BundleId>,
        portfolio_revision: Option<PortfolioRevisionToken>,
        fair_value_decision: Option<DecisionId>,
        content_identity: DecisionContentDigest,
    ) -> Self {
        Self {
            model_bundle,
            portfolio_revision,
            fair_value_decision,
            content_identity,
        }
    }

    /// Returns the admitted model bundle, when used.
    #[must_use]
    pub const fn model_bundle(&self) -> Option<&BundleId> {
        self.model_bundle.as_ref()
    }

    /// Returns the immutable portfolio precondition, when used.
    #[must_use]
    pub const fn portfolio_revision(&self) -> Option<&PortfolioRevisionToken> {
        self.portfolio_revision.as_ref()
    }

    /// Returns the fair-value classification decision, when used.
    #[must_use]
    pub const fn fair_value_decision(&self) -> Option<DecisionId> {
        self.fair_value_decision
    }

    /// Returns the commitment to the complete assembled dossier content.
    #[must_use]
    pub const fn content_identity(&self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Immutable dossier of references for one candidate and instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Dossier {
    id: DossierId,
    candidate_id: CandidateId,
    instrument_id: InstrumentId,
    assembled_at: Timestamp,
    evidence: DossierEvidence,
}

impl Dossier {
    /// Constructs a dossier that cannot predate candidate selection.
    ///
    /// # Errors
    ///
    /// Returns [`DecisionContractError::InvalidTimeOrder`] for retrospective time reversal.
    pub fn try_new(
        id: DossierId,
        candidate: &CandidateRecord,
        assembled_at: Timestamp,
        evidence: DossierEvidence,
    ) -> Result<Self, DecisionContractError> {
        if assembled_at < candidate.selected_at {
            return Err(DecisionContractError::InvalidTimeOrder);
        }
        Ok(Self {
            id,
            candidate_id: candidate.id.clone(),
            instrument_id: candidate.instrument_id,
            assembled_at,
            evidence,
        })
    }

    /// Returns the dossier identity.
    #[must_use]
    pub const fn id(&self) -> &DossierId {
        &self.id
    }

    /// Returns the candidate identity this dossier explains.
    #[must_use]
    pub const fn candidate_id(&self) -> &CandidateId {
        &self.candidate_id
    }

    /// Returns the dossier instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the assembly time.
    #[must_use]
    pub const fn assembled_at(&self) -> Timestamp {
        self.assembled_at
    }

    /// Returns the exact upstream evidence references.
    #[must_use]
    pub const fn evidence(&self) -> &DossierEvidence {
        &self.evidence
    }
}
