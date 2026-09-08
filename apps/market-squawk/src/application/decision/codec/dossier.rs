use std::str::FromStr as _;

use market_squawk_decisions::{
    CandidateRecord, DecisionDossier, Dossier, DossierEvidence, DossierId, DossierReference,
    DossierSection,
};
use market_squawk_domain::{EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_modeling::BundleId;
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_valuation::DecisionId;
use serde::{Deserialize, Serialize};

use super::super::DecisionApplicationError;
use super::common::content_digest;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DossierSectionWire {
    Data,
    CorporateActions,
    Fundamentals,
    Forecast,
    PortfolioImpact,
    FairValue,
    DecisionContext,
}

impl From<DossierSection> for DossierSectionWire {
    fn from(value: DossierSection) -> Self {
        match value {
            DossierSection::Data => Self::Data,
            DossierSection::CorporateActions => Self::CorporateActions,
            DossierSection::Fundamentals => Self::Fundamentals,
            DossierSection::Forecast => Self::Forecast,
            DossierSection::PortfolioImpact => Self::PortfolioImpact,
            DossierSection::FairValue => Self::FairValue,
            DossierSection::DecisionContext => Self::DecisionContext,
        }
    }
}

impl From<DossierSectionWire> for DossierSection {
    fn from(value: DossierSectionWire) -> Self {
        match value {
            DossierSectionWire::Data => Self::Data,
            DossierSectionWire::CorporateActions => Self::CorporateActions,
            DossierSectionWire::Fundamentals => Self::Fundamentals,
            DossierSectionWire::Forecast => Self::Forecast,
            DossierSectionWire::PortfolioImpact => Self::PortfolioImpact,
            DossierSectionWire::FairValue => Self::FairValue,
            DossierSectionWire::DecisionContext => Self::DecisionContext,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DossierReferenceWire {
    section: DossierSectionWire,
    content_identity: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DossierWire {
    id: String,
    candidate_id: String,
    instrument_id: InstrumentId,
    assembled_at: Timestamp,
    model_bundle: Option<String>,
    portfolio_revision: Option<[u8; 32]>,
    fair_value_decision: Option<String>,
    evidence_identity: EvidenceDigest,
    references: Vec<DossierReferenceWire>,
}

impl From<&DecisionDossier> for DossierWire {
    fn from(value: &DecisionDossier) -> Self {
        let dossier = value.dossier();
        Self {
            id: dossier.id().as_str().to_owned(),
            candidate_id: dossier.candidate_id().as_str().to_owned(),
            instrument_id: dossier.instrument_id(),
            assembled_at: dossier.assembled_at(),
            model_bundle: dossier
                .evidence()
                .model_bundle()
                .map(|bundle| bundle.as_str().to_owned()),
            portfolio_revision: dossier
                .evidence()
                .portfolio_revision()
                .map(PortfolioRevisionToken::bytes),
            fair_value_decision: dossier
                .evidence()
                .fair_value_decision()
                .map(|decision| decision.to_string()),
            evidence_identity: dossier.evidence().content_identity().evidence_digest(),
            references: value
                .references()
                .iter()
                .map(|reference| DossierReferenceWire {
                    section: reference.section().into(),
                    content_identity: reference.content_identity().evidence_digest(),
                })
                .collect(),
        }
    }
}

impl DossierWire {
    pub(super) fn key(&self) -> &str {
        &self.id
    }

    pub(super) fn candidate_key(&self) -> &str {
        &self.candidate_id
    }

    pub(super) fn decode(
        self,
        candidate: &CandidateRecord,
    ) -> Result<DecisionDossier, DecisionApplicationError> {
        if candidate.id().as_str() != self.candidate_id
            || candidate.instrument_id() != self.instrument_id
        {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        let evidence = DossierEvidence::new(
            self.model_bundle
                .as_deref()
                .map(BundleId::try_new)
                .transpose()
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.portfolio_revision
                .map(PortfolioRevisionToken::from_bytes),
            self.fair_value_decision
                .as_deref()
                .map(DecisionId::from_str)
                .transpose()
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            content_digest(self.evidence_identity)?,
        );
        let dossier = Dossier::try_new(
            DossierId::try_new(&self.id)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            candidate,
            self.assembled_at,
            evidence,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        let references = self
            .references
            .into_iter()
            .map(|reference| {
                Ok(DossierReference::new(
                    reference.section.into(),
                    content_digest(reference.content_identity)?,
                ))
            })
            .collect::<Result<Vec<_>, DecisionApplicationError>>()?;
        DecisionDossier::try_new(dossier, references)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}
