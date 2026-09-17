//! Authoritative-reference-only dossier assembly.

use crate::{DecisionContentDigest, DecisionContractError, Dossier};

/// Maximum authoritative references assembled into one dossier.
pub const MAX_DOSSIER_REFERENCES: usize = 64;

/// Closed source section for one upstream evidence reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DossierSection {
    /// Point-in-time dataset or source lineage.
    Data,
    /// Corporate-action evidence.
    CorporateActions,
    /// Fundamental evidence.
    Fundamentals,
    /// Model or forecast evidence.
    Forecast,
    /// Portfolio-impact evidence.
    PortfolioImpact,
    /// Fair-value evidence.
    FairValue,
    /// Analyst-authored decision evidence.
    DecisionContext,
}

/// Opaque controlled reference to authoritative upstream bytes or a typed upstream record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DossierReference {
    section: DossierSection,
    content_identity: DecisionContentDigest,
}

impl DossierReference {
    /// Constructs a path-free, value-free evidence reference.
    #[must_use]
    pub const fn new(section: DossierSection, content_identity: DecisionContentDigest) -> Self {
        Self {
            section,
            content_identity,
        }
    }

    /// Evidence section.
    #[must_use]
    pub const fn section(self) -> DossierSection {
        self.section
    }

    /// Exact authoritative content identity.
    #[must_use]
    pub const fn content_identity(self) -> DecisionContentDigest {
        self.content_identity
    }
}

/// Immutable dossier core plus bounded authoritative references.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionDossier {
    dossier: Dossier,
    references: Box<[DossierReference]>,
}

impl DecisionDossier {
    /// Assembles references without accepting source values, paths, queries, or executables.
    pub fn try_new(
        dossier: Dossier,
        references: Vec<DossierReference>,
    ) -> Result<Self, DecisionContractError> {
        if references.is_empty()
            || references.len() > MAX_DOSSIER_REFERENCES
            || references
                .iter()
                .enumerate()
                .any(|(index, reference)| references[index + 1..].contains(reference))
        {
            return Err(DecisionContractError::InvalidBound);
        }
        Ok(Self {
            dossier,
            references: references.into_boxed_slice(),
        })
    }

    /// Immutable Task 7 dossier identity and upstream model/portfolio/fair-value references.
    #[must_use]
    pub const fn dossier(&self) -> &Dossier {
        &self.dossier
    }

    /// Additional controlled evidence references in presentation order.
    #[must_use]
    pub fn references(&self) -> &[DossierReference] {
        &self.references
    }
}
