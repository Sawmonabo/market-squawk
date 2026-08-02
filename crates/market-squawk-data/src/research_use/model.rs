//! Closed research-use vocabulary, hard bounds, typed digests, and shared failures.

use std::fmt;
use std::time::Duration;

use thiserror::Error;

use crate::SourceOperation;

/// Maximum exact root generations authorized by one request.
pub const MAX_RESEARCH_USE_ROOTS: usize = 256;
/// Maximum transitive generations visited by one request.
pub const MAX_RESEARCH_USE_GRAPH_NODES: usize = 100_000;
/// Maximum transitive parent edges visited by one request.
pub const MAX_RESEARCH_USE_EDGES: usize = 400_000;
/// Maximum direct source contributions visited by one request.
pub const MAX_RESEARCH_USE_SOURCES: usize = 100_000;
/// Maximum traversal-owned memory retained by one request.
pub const MAX_RESEARCH_USE_RETAINED_BYTES: usize = 64 * 1024 * 1024;
/// Maximum wall-clock traversal deadline accepted from a caller.
pub const MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS: u64 = 30;
/// Maximum lifetime of a single-use process-local research permit.
pub const MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS: u64 = 5 * 60;

/// Closed downstream use independently authorized by retained evidence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ResearchUse {
    /// Render bounded observations to the authorized local user.
    Display,
    /// Compute local research, financial analytics, backtests, or model inference.
    LocalAnalysis,
    /// Use observations as inputs to model fitting or parameter estimation.
    Train,
}

impl ResearchUse {
    pub(super) const fn tag(self) -> u8 {
        match self {
            Self::Display => 1,
            Self::LocalAnalysis => 2,
            Self::Train => 3,
        }
    }

    pub(crate) const fn database_name(self) -> &'static str {
        match self {
            Self::Display => "display",
            Self::LocalAnalysis => "local_analysis",
            Self::Train => "train",
        }
    }

    const fn bit(self) -> u8 {
        match self {
            Self::Display => 1 << 0,
            Self::LocalAnalysis => 1 << 1,
            Self::Train => 1 << 2,
        }
    }
}

/// Nonempty, duplicate-free set of closed downstream uses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchUseSet(u8);

impl ResearchUseSet {
    /// Constructs a bounded use set and rejects duplicate authority claims.
    pub fn try_new(mut uses: Vec<ResearchUse>) -> Result<Self, ResearchUseError> {
        if uses.is_empty() || uses.len() > 3 {
            return Err(ResearchUseError::InvalidUseSet);
        }
        uses.sort_unstable();
        if uses.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ResearchUseError::DuplicateUse);
        }
        Ok(Self(
            uses.into_iter().fold(0, |mask, value| mask | value.bit()),
        ))
    }

    /// Returns whether this set contains the exact requested use.
    pub const fn contains(self, requested: ResearchUse) -> bool {
        self.0 & requested.bit() != 0
    }

    /// Returns the number of distinct uses.
    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns whether the set is empty.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub(crate) const fn mask(self) -> u8 {
        self.0
    }

    /// Returns the source-rights operations that must already cover this downstream-use set.
    ///
    /// Local analysis consumes durable analytical generations and therefore requires persistence
    /// authority. Display and training preserve their exact operation-specific authority.
    pub(crate) const fn required_source_operation_mask(self) -> u8 {
        let mut required = 0;
        if self.contains(ResearchUse::Display) {
            required |= SourceOperation::Display.mask();
        }
        if self.contains(ResearchUse::LocalAnalysis) {
            required |= SourceOperation::Persist.mask();
        }
        if self.contains(ResearchUse::Train) {
            required |= SourceOperation::Train.mask();
        }
        required
    }
}

/// Caller-selected operation bounds within fixed process ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchUseLimits {
    max_roots: usize,
    max_nodes: usize,
    max_edges: usize,
    max_sources: usize,
    max_retained_bytes: usize,
    traversal_deadline: Duration,
    permit_lifetime: Duration,
}

impl ResearchUseLimits {
    /// Constructs nonzero caller bounds that cannot widen the fixed process ceilings.
    #[allow(
        clippy::too_many_arguments,
        reason = "the seven independent limits are audit fields"
    )]
    pub fn try_new(
        max_roots: usize,
        max_nodes: usize,
        max_edges: usize,
        max_sources: usize,
        max_retained_bytes: usize,
        traversal_deadline: Duration,
        permit_lifetime: Duration,
    ) -> Result<Self, ResearchUseError> {
        if max_roots == 0
            || max_roots > MAX_RESEARCH_USE_ROOTS
            || max_nodes == 0
            || max_nodes > MAX_RESEARCH_USE_GRAPH_NODES
            || max_edges == 0
            || max_edges > MAX_RESEARCH_USE_EDGES
            || max_sources == 0
            || max_sources > MAX_RESEARCH_USE_SOURCES
            || max_retained_bytes == 0
            || max_retained_bytes > MAX_RESEARCH_USE_RETAINED_BYTES
            || traversal_deadline.is_zero()
            || traversal_deadline > Duration::from_secs(MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS)
            || permit_lifetime.is_zero()
            || permit_lifetime > Duration::from_secs(MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS)
        {
            return Err(ResearchUseError::InvalidLimits);
        }
        Ok(Self {
            max_roots,
            max_nodes,
            max_edges,
            max_sources,
            max_retained_bytes,
            traversal_deadline,
            permit_lifetime,
        })
    }

    /// Returns the maximum exact roots.
    pub const fn max_roots(self) -> usize {
        self.max_roots
    }

    /// Returns the maximum transitive nodes.
    pub const fn max_nodes(self) -> usize {
        self.max_nodes
    }

    /// Returns the maximum transitive edges.
    pub const fn max_edges(self) -> usize {
        self.max_edges
    }

    /// Returns the maximum direct source contributions.
    pub const fn max_sources(self) -> usize {
        self.max_sources
    }

    /// Returns the maximum operation-owned retained bytes.
    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }

    /// Returns the caller's bounded traversal deadline.
    pub const fn traversal_deadline(self) -> Duration {
        self.traversal_deadline
    }

    /// Returns the caller's bounded permit lifetime.
    pub const fn permit_lifetime(self) -> Duration {
        self.permit_lifetime
    }
}

macro_rules! canonical_digest {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 32]);

        impl $name {
            /// Reconstructs a non-reserved retained identity.
            pub fn try_from_bytes(bytes: [u8; 32]) -> Result<Self, ResearchUseError> {
                if bytes == [0; 32] {
                    Err(ResearchUseError::MalformedDigest)
                } else {
                    Ok(Self(bytes))
                }
            }

            pub(super) const fn from_canonical(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            /// Returns the exact SHA-256 bytes.
            pub const fn bytes(self) -> [u8; 32] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "([SHA-256])"))
            }
        }
    };
}

canonical_digest!(
    ResearchUseGraphDigest,
    "Exact SHA-256 identity of one bounded canonical transitive research graph."
);
canonical_digest!(
    ResearchUseDecisionDigest,
    "Exact SHA-256 identity of one bounded canonical research-use decision."
);

/// Construction or canonicalization failure for research-use authority contracts.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResearchUseError {
    /// A use set was empty or contained an unsupported bit.
    #[error("research-use set is invalid")]
    InvalidUseSet,
    /// A use set repeated an authority claim.
    #[error("research-use set repeats a use")]
    DuplicateUse,
    /// One or more caller limits were zero or exceeded a process ceiling.
    #[error("research-use limits are invalid")]
    InvalidLimits,
    /// A retained digest used the all-zero reserved value.
    #[error("research-use digest is malformed")]
    MalformedDigest,
    /// A generation sequence, kind, or build identity was inconsistent.
    #[error("research-use generation is invalid")]
    InvalidGeneration,
    /// A source mapping used a reserved identity.
    #[error("research-use source input is invalid")]
    InvalidSourceInput,
    /// Graph structure, reachability, or relationship semantics were invalid.
    #[error("research-use graph is invalid")]
    InvalidGraph,
    /// A canonical graph repeated a node, edge, root, or source mapping.
    #[error("research-use graph repeats a member")]
    DuplicateGraphMember,
    /// A graph reused one immutable manifest coordinate with conflicting identity.
    #[error("research-use graph contains conflicting manifest identities")]
    ConflictingGraphMember,
    /// Selected source or research-grant evidence was incomplete.
    #[error("research-use authority evidence is invalid")]
    InvalidAuthorityEvidence,
    /// Selected authority evidence repeated one direct source generation.
    #[error("research-use decision repeats source authority")]
    DuplicateAuthorityEvidence,
    /// Policy, outcome, expiry, or authority evidence was inconsistent.
    #[error("research-use decision is invalid")]
    InvalidDecision,
    /// A derived publication was empty or inconsistent with its immutable plan.
    #[error("derived publication is invalid")]
    InvalidPublication,
    /// A derived publication repeated a reservation, run, artifact, or object.
    #[error("derived publication repeats an output member")]
    DuplicatePublicationMember,
    /// A bounded vector allocation failed.
    #[error("research-use bounded allocation failed")]
    AllocationFailed,
    /// A platform-size value could not be represented by canonical encoding.
    #[error("research-use canonical encoding overflow")]
    CanonicalEncodingOverflow,
    /// A process-local permit used a reserved catalog-session identity.
    #[error("research-use permit session is invalid")]
    InvalidPermitSession,
}
