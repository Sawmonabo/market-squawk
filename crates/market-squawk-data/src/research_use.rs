//! Bounded research-use authority contracts and canonical identities.

mod canonical;
mod catalog;
mod decision;
mod derived;
mod graph;
mod identity;
mod market_event;
mod model;
pub use market_event::{AuthorizedMarketEventUse, MarketEventUseInput, MarketEventUseRequest};
pub(crate) use market_event::{
    authorize_current_market_event_use_in_snapshot, authorize_market_event_use_in_snapshot,
    recheck_market_event_use_in_snapshot,
};
mod permit;
mod persistence;
pub(crate) use persistence::{
    RetainedSourceUseGrant, SourceGrantSelection, select_source_use_grant, source_use_frontier,
};
mod publication;
mod traversal;

pub use self::catalog::{
    AuthorizedResearchUse, DerivedOutputObjectInput, PublishedDerivedGeneration,
    RegisteredResearchUseGrant, ResearchUseCatalogError, ResearchUseGrantInput, ResearchUseRequest,
    ResearchUseRevocationInput, ResearchUseRevocationReason, ResearchUseRevocationReceipt,
    RetainedResearchUsePolicy,
};
pub use self::decision::{
    ResearchUseAuthorityEvidence, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
    ResearchUseDenialReason,
};
pub use self::graph::{
    ResearchUseGeneration, ResearchUseGraph, ResearchUseGraphEdge, ResearchUseSourceInput,
};
pub use self::model::{
    MAX_RESEARCH_USE_EDGES, MAX_RESEARCH_USE_GRAPH_NODES, MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS,
    MAX_RESEARCH_USE_RETAINED_BYTES, MAX_RESEARCH_USE_ROOTS, MAX_RESEARCH_USE_SOURCES,
    MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS, ResearchUse, ResearchUseDecisionDigest,
    ResearchUseError, ResearchUseGraphDigest, ResearchUseLimits, ResearchUseSet,
};
pub use self::permit::ResearchUsePermit;
pub use self::publication::{
    DerivedPublicationDigest, DerivedPublicationInput, DerivedPublicationObject,
    DerivedRetentionOperation, MAX_DERIVED_PUBLICATION_OBJECTS,
};

#[cfg(test)]
use self::permit::issue_permit;

#[cfg(test)]
mod tests;
