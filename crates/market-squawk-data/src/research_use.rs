//! Bounded research-use authority contracts and canonical identities.

mod canonical;
mod catalog;
mod decision;
mod derived;
mod graph;
mod identity;
mod model;
mod permit;
mod persistence;
mod publication;
mod traversal;

pub use self::catalog::{
    AuthorizedResearchUse, DerivedOutputObjectInput, PublishedDerivedGeneration,
    RegisteredResearchUseGrant, ResearchUseCatalogError, ResearchUseGrantInput, ResearchUseRequest,
    ResearchUseRevocationInput, ResearchUseRevocationReason, ResearchUseRevocationReceipt,
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
