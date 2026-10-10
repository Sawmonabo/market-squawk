//! Exact physical and logical event source-use authority for automatic valuation.

use super::{
    AutomaticValuationConflict, AutomaticValuationError, AutomaticValuationUnavailable,
    MAX_METHOD_INPUTS, automatic_event_input, automatic_input_manifests, valid_sha256,
};
use crate::{CanonicalHasher, ValuationInput};
use market_squawk_data::{
    AuthorizedMarketEventUse, AuthorizedResearchUse, MarketEventCommitRef, MarketEventUseInput,
    MarketEventUseRequest, ResearchUse, ResearchUseDecisionDigest, ResearchUseGraphDigest,
    ResearchUseLimits,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};

/// Retained exact event admission. Reconstructing this audit evidence issues no authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValuationEventRightsAdmission {
    pub(super) commit: MarketEventCommitRef,
    pub(super) inputs: Box<[MarketEventUseInput]>,
    pub(super) rights_input_digest: EvidenceDigest,
    pub(super) decision_digest: ResearchUseDecisionDigest,
    pub(super) evaluated_at: Timestamp,
    pub(super) expires_at: Timestamp,
}

impl ValuationEventRightsAdmission {
    /// Returns the original logical horizon.
    pub const fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }
    /// Returns exact source row bindings requiring fresh authorization after restart.
    pub fn inputs(&self) -> &[MarketEventUseInput] {
        &self.inputs
    }
    /// Returns the exact event input commitment, separate from any physical graph.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.rights_input_digest
    }
    /// Returns the original source grant decision commitment.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.decision_digest
    }
    /// Returns the original authorization clock.
    pub const fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }
    /// Returns the original exclusive event-use authority expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(crate) fn try_recover(
        commit: MarketEventCommitRef,
        inputs: Vec<MarketEventUseInput>,
        rights_input_digest: EvidenceDigest,
        decision_digest: ResearchUseDecisionDigest,
        evaluated_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, AutomaticValuationError> {
        if inputs.is_empty()
            || inputs.len() > MAX_METHOD_INPUTS
            || expires_at <= evaluated_at
            || !valid_sha256(rights_input_digest)
            || decision_digest.bytes() == [0; 32]
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        // Only deterministic reconstruction occurs here. No grant is minted by recovery.
        let limits = ResearchUseLimits::try_new(
            market_squawk_data::MAX_RESEARCH_USE_ROOTS,
            market_squawk_data::MAX_RESEARCH_USE_GRAPH_NODES,
            market_squawk_data::MAX_RESEARCH_USE_EDGES,
            MAX_METHOD_INPUTS,
            market_squawk_data::MAX_RESEARCH_USE_RETAINED_BYTES,
            std::time::Duration::from_secs(
                market_squawk_data::MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
            ),
            std::time::Duration::from_secs(
                market_squawk_data::MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS,
            ),
        )
        .map_err(|_| AutomaticValuationError::InvalidContract)?;
        let request = MarketEventUseRequest::try_from_retained(
            commit.clone(),
            inputs,
            ResearchUse::LocalAnalysis,
            limits,
        )
        .map_err(|_| AutomaticValuationError::InvalidContract)?;
        if request.rights_input_digest() != rights_input_digest {
            return Err(AutomaticValuationError::Conflict(
                AutomaticValuationConflict::Evidence,
            ));
        }
        Ok(Self {
            commit,
            inputs: request.inputs().to_vec().into_boxed_slice(),
            rights_input_digest,
            decision_digest,
            evaluated_at,
            expires_at,
        })
    }

    fn from_authorization(
        value: &AuthorizedMarketEventUse,
    ) -> Result<Self, AutomaticValuationError> {
        Self::try_recover(
            value.commit().clone(),
            value.inputs().to_vec(),
            value.rights_input_digest(),
            value.decision_digest(),
            value.evaluated_at(),
            value.expires_at(),
        )
    }
}

/// Single-use physical authority and exact logical event authorities retained through calculation.
#[derive(Debug)]
pub struct ValuationRightsReceipt {
    pub(super) authorization: AuthorizedResearchUse,
    pub(super) event_authorizations: Vec<AuthorizedMarketEventUse>,
    pub(super) event_admissions: Box<[ValuationEventRightsAdmission]>,
    pub(super) rights_input_digest: EvidenceDigest,
    pub(super) expires_at: Timestamp,
}

impl ValuationRightsReceipt {
    /// Retains each genuine source authority without casting logical events to physical manifests.
    pub fn try_from_authorization(
        authorization: AuthorizedResearchUse,
        mut events: Vec<AuthorizedMarketEventUse>,
    ) -> Result<Self, AutomaticValuationError> {
        if authorization.research_use() != ResearchUse::LocalAnalysis
            || events.len() > MAX_METHOD_INPUTS
            || events
                .iter()
                .any(|event| event.research_use() != ResearchUse::LocalAnalysis)
        {
            return Err(AutomaticValuationError::Unavailable(
                AutomaticValuationUnavailable::Rights,
            ));
        }
        let input_count = events
            .iter()
            .try_fold(0usize, |count, event| {
                count.checked_add(event.inputs().len())
            })
            .ok_or(AutomaticValuationError::InvalidContract)?;
        if input_count > MAX_METHOD_INPUTS {
            return Err(AutomaticValuationError::InvalidContract);
        }
        events.sort_unstable_by_key(|event| event.rights_input_digest().bytes());
        if events
            .windows(2)
            .any(|pair| pair[0].rights_input_digest() == pair[1].rights_input_digest())
        {
            return Err(AutomaticValuationError::InvalidContract);
        }
        let event_admissions = events
            .iter()
            .map(ValuationEventRightsAdmission::from_authorization)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice();
        let expires_at = events
            .iter()
            .fold(authorization.expires_at(), |expiry, event| {
                expiry.min(event.expires_at())
            });
        let rights_input_digest =
            combined_rights_input_digest(authorization.graph().digest(), &event_admissions);
        Ok(Self {
            authorization,
            event_authorizations: events,
            event_admissions,
            rights_input_digest,
            expires_at,
        })
    }
    /// Returns the physical research decision identity; event decisions are retained separately.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.authorization.decision_digest()
    }
    /// Returns the physical source graph only.
    pub const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.authorization.graph().digest()
    }
    /// Returns the combined exact physical and logical input identity.
    pub const fn rights_input_digest(&self) -> EvidenceDigest {
        self.rights_input_digest
    }
    /// Returns the earliest exclusive expiry across all admitted authorities.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    pub(super) fn admits(&self, input: &ValuationInput) -> bool {
        if let Some((commit, publication, row, canonical_digest)) = automatic_event_input(input) {
            return self.event_authorizations.iter().any(|authority| {
                authority.admits_event(commit, publication, row, canonical_digest)
            });
        }
        let manifests = automatic_input_manifests(input);
        !manifests.is_empty()
            && manifests.iter().all(|manifest| {
                self.authorization
                    .graph()
                    .nodes()
                    .iter()
                    .any(|node| node.manifest() == manifest)
            })
    }
}

pub(super) fn combined_rights_input_digest(
    graph: ResearchUseGraphDigest,
    events: &[ValuationEventRightsAdmission],
) -> EvidenceDigest {
    let mut hash = CanonicalHasher::new(b"market-squawk/valuation-rights-inputs/v1");
    hash.fixed(graph.bytes());
    hash.u64(events.len() as u64);
    for event in events {
        hash.fixed(event.rights_input_digest.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish())
}

pub(super) fn hash_event_admission(
    hash: &mut CanonicalHasher,
    value: &ValuationEventRightsAdmission,
) {
    crate::evidence::hash_market_event_commit(hash, value.commit());
    hash.fixed(value.rights_input_digest().bytes());
    hash.fixed(value.decision_digest().bytes());
    hash.i64(value.evaluated_at().unix_nanos());
    hash.i64(value.expires_at().unix_nanos());
    hash.u64(value.inputs().len() as u64);
    for input in value.inputs() {
        hash.fixed(input.publication_digest().bytes());
        hash.u8(match input.publication_kind() {
            market_squawk_data::ProviderMarketEventPublicationKind::ResponseMarketEvent => 1,
            market_squawk_data::ProviderMarketEventPublicationKind::EventMicrobatch => 2,
            market_squawk_data::ProviderMarketEventPublicationKind::CompositeResponseEvent => 3,
        });
        hash.u32(input.row_ordinal());
        hash.fixed(input.coordinate_digest().bytes());
        hash.fixed(input.canonical_event_digest().bytes());
        hash.bytes(input.source_id().as_str().as_bytes());
        hash.i64(input.origin_committed_at().unix_nanos());
    }
}
