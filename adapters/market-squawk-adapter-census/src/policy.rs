use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_sources::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, ProviderBudgetPolicy,
    ProviderBudgetWindow, ProviderRateDeclaration,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use crate::{
    CENSUS_APPLICATION_REQUESTS_PER_DAY, CENSUS_APPLICATION_REQUESTS_PER_SECOND,
    update_digest_component,
};

const CENSUS_SECOND_NANOS: u64 = 1_000_000_000;
const CENSUS_DAY_NANOS: u64 = 86_400_000_000_000;
const CENSUS_MAXIMUM_BACKOFF_NANOS: u64 = 60_000_000_000;

/// Exact notice Census requires every API-backed service to display prominently.
pub const CENSUS_NON_ENDORSEMENT_NOTICE: &str =
    "This product uses the Census Bureau Data API but is not endorsed or certified by the Census Bureau.";

/// Closed provider-local Census operation vocabulary.
///
/// This matrix describes the sole supported product posture. It is not proof that root granted a
/// use: the application must independently rejoin its opaque, current rights lease at activation,
/// publication, analytical use, and presentation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CensusIntendedUse {
    /// Retrieve official responses for the owner's private research.
    PrivateRetrieval,
    /// Display data inside the owner's private research console.
    PrivateDisplay,
    /// Persist exact raw, native, or canonical private-research evidence.
    PrivatePersistence,
    /// Transform retained data into private features, labels, or research artifacts.
    PrivateTransformation,
    /// Use point-in-time-safe data in private backtests.
    PrivateBacktesting,
    /// Use retained data in private forecasts.
    PrivateForecasting,
    /// Train private models from retained data.
    PrivateModelTraining,
    /// Operate private models that consume retained or transformed data.
    PrivateModelOperation,
    /// Export provider data outside the owner's private-use boundary.
    Export,
    /// Sell Census data or a data product that redistributes it.
    Sale,
    /// Redistribute Census data outside the owner's private-use boundary.
    Redistribution,
    /// Re-identify a respondent or entity represented by aggregate provider evidence.
    Reidentification,
    /// Link provider evidence to identifying data for re-identification.
    IdentifyingLinkage,
}

/// Immutable provider-local private-research operation matrix.
///
/// This zero-configuration value deliberately contains no owner attestation, mutable commerce
/// flag, or root rights-decision digest. Only the root-owned opaque lease proves owner authority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusPrivateResearchPolicy {}

impl CensusPrivateResearchPolicy {
    /// Returns the sole supported Census provider-local policy.
    pub const fn personal_research() -> Self {
        Self {}
    }

    /// Applies the fixed provider-local matrix.
    pub const fn authorize(self, intended: CensusIntendedUse) -> Result<(), CensusPolicyError> {
        match intended {
            CensusIntendedUse::PrivateRetrieval
            | CensusIntendedUse::PrivateDisplay
            | CensusIntendedUse::PrivatePersistence
            | CensusIntendedUse::PrivateTransformation
            | CensusIntendedUse::PrivateBacktesting
            | CensusIntendedUse::PrivateForecasting
            | CensusIntendedUse::PrivateModelTraining
            | CensusIntendedUse::PrivateModelOperation => Ok(()),
            CensusIntendedUse::Export
            | CensusIntendedUse::Sale
            | CensusIntendedUse::Redistribution
            | CensusIntendedUse::Reidentification
            | CensusIntendedUse::IdentifyingLinkage => Err(CensusPolicyError::ProhibitedUse),
        }
    }

    /// Returns the deterministic non-authoritative identity of the complete fixed matrix.
    pub fn policy_digest(self) -> Result<EvidenceDigest, CensusPolicyError> {
        let operations = [
            CensusIntendedUse::PrivateRetrieval,
            CensusIntendedUse::PrivateDisplay,
            CensusIntendedUse::PrivatePersistence,
            CensusIntendedUse::PrivateTransformation,
            CensusIntendedUse::PrivateBacktesting,
            CensusIntendedUse::PrivateForecasting,
            CensusIntendedUse::PrivateModelTraining,
            CensusIntendedUse::PrivateModelOperation,
            CensusIntendedUse::Export,
            CensusIntendedUse::Sale,
            CensusIntendedUse::Redistribution,
            CensusIntendedUse::Reidentification,
            CensusIntendedUse::IdentifyingLinkage,
        ];
        let matrix = operations.map(|operation| (operation, self.authorize(operation).is_ok()));
        let wire = serde_json::to_vec(&matrix).map_err(|_| CensusPolicyError::InvalidPolicy)?;
        let presentation = census_presentation_obligation()?;
        let mut digest = Sha256::new();
        update_digest_component(
            &mut digest,
            b"market-squawk/census-private-research-policy/v1",
        );
        update_digest_component(&mut digest, &wire);
        update_digest_component(&mut digest, &presentation.obligation_digest().bytes());
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }
}

/// Closed terms obligations root presentation and analytical-use admission must enforce.
///
/// This is a provider-local requirement, not a presentation receipt. Desktop, MCP, exports, and
/// analytical jobs must rejoin root's current presentation/rights authority before consuming it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusPresentationObligation {
    notice_id: SourceIdentifier,
    notice_text: String,
    prominent_display_required: bool,
    reidentification_prohibited: bool,
    identifying_linkage_prohibited: bool,
    obligation_digest: EvidenceDigest,
}

impl CensusPresentationObligation {
    /// Returns the stable notice identity for Desktop/MCP presentation policy.
    pub const fn notice_id(&self) -> &SourceIdentifier {
        &self.notice_id
    }

    /// Returns the exact required non-endorsement notice.
    pub fn notice_text(&self) -> &str {
        &self.notice_text
    }

    /// Returns whether the notice must be displayed prominently.
    pub const fn prominent_display_required(&self) -> bool {
        self.prominent_display_required
    }

    /// Returns whether respondent/entity re-identification is prohibited.
    pub const fn reidentification_prohibited(&self) -> bool {
        self.reidentification_prohibited
    }

    /// Returns whether identifying linkage to other data is prohibited.
    pub const fn identifying_linkage_prohibited(&self) -> bool {
        self.identifying_linkage_prohibited
    }

    /// Returns the complete display and purpose-restriction identity.
    pub const fn obligation_digest(&self) -> EvidenceDigest {
        self.obligation_digest
    }

    /// Recomputes the closed obligation rather than trusting a transported copy.
    pub fn validate(&self) -> Result<(), CensusPolicyError> {
        if self != &census_presentation_obligation()? {
            return Err(CensusPolicyError::InvalidPolicy);
        }
        Ok(())
    }
}

/// Adapter-local declaration of root authorities that must be rejoined before production use.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CensusActivationRequirements {
    shared_provider_rate_authority: bool,
    sealed_raw_before_publish: bool,
    shared_publication_authority: bool,
    root_rights_decision_rejoin: bool,
    root_presentation_rejoin: bool,
    root_credential_generation_rejoin: bool,
    root_dataset_schema_rejoin: bool,
}

impl CensusActivationRequirements {
    /// Returns the fixed production activation requirements.
    pub const fn production() -> Self {
        Self {
            shared_provider_rate_authority: true,
            sealed_raw_before_publish: true,
            shared_publication_authority: true,
            root_rights_decision_rejoin: true,
            root_presentation_rejoin: true,
            root_credential_generation_rejoin: true,
            root_dataset_schema_rejoin: true,
        }
    }

    /// Census uses the sole shared durable rate authority.
    pub const fn shared_provider_rate_authority_required(self) -> bool {
        self.shared_provider_rate_authority
    }

    /// Exact metadata and data bytes must be physically sealed before publication.
    pub const fn sealed_raw_before_publish(self) -> bool {
        self.sealed_raw_before_publish
    }

    /// Only root data authority may mint manifests, generations, revisions, and PIT reads.
    pub const fn shared_publication_authority_required(self) -> bool {
        self.shared_publication_authority
    }

    /// Root must rejoin its nonforgeable current rights decision at every use seam.
    pub const fn root_rights_decision_rejoin_required(self) -> bool {
        self.root_rights_decision_rejoin
    }

    /// Root must enforce the exact presentation obligation for every user-facing read.
    pub const fn root_presentation_rejoin_required(self) -> bool {
        self.root_presentation_rejoin
    }

    /// Root must keep its protected credential generation current for the source instance.
    pub const fn root_credential_generation_rejoin_required(self) -> bool {
        self.root_credential_generation_rejoin
    }

    /// Root must bind records to its actual `DatasetSchemaRef`; adapter digests cannot substitute.
    pub const fn root_dataset_schema_rejoin_required(self) -> bool {
        self.root_dataset_schema_rejoin
    }

    /// Returns the deterministic identity of the complete rejoin requirement set.
    pub fn requirements_digest(self) -> Result<EvidenceDigest, CensusPolicyError> {
        let wire = serde_json::to_vec(&self).map_err(|_| CensusPolicyError::InvalidPolicy)?;
        let mut digest = Sha256::new();
        update_digest_component(
            &mut digest,
            b"market-squawk/census-activation-requirements/v1",
        );
        update_digest_component(&mut digest, &wire);
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ))
    }
}

/// Returns the exact code-owned Census display and no-reidentification obligation.
pub fn census_presentation_obligation(
) -> Result<CensusPresentationObligation, CensusPolicyError> {
    let notice_id = SourceIdentifier::try_from("census-api-non-endorsement-notice-v1")
        .map_err(|_| CensusPolicyError::InvalidPolicy)?;
    let mut digest = Sha256::new();
    update_digest_component(
        &mut digest,
        b"market-squawk/census-presentation-obligation/v1",
    );
    update_digest_component(&mut digest, notice_id.as_str().as_bytes());
    update_digest_component(&mut digest, CENSUS_NON_ENDORSEMENT_NOTICE.as_bytes());
    update_digest_component(&mut digest, b"prominent-display=required");
    update_digest_component(&mut digest, b"reidentification=denied");
    update_digest_component(&mut digest, b"identifying-linkage=denied");
    Ok(CensusPresentationObligation {
        notice_id,
        notice_text: CENSUS_NON_ENDORSEMENT_NOTICE.to_owned(),
        prominent_display_required: true,
        reidentification_prohibited: true,
        identifying_linkage_prohibited: true,
        obligation_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest.finalize().into(),
        ),
    })
}

/// Builds the exact product-wide Census request allocation for one stable credential subject.
///
/// Both windows are conservative Market Squawk policy because Census does not publish a current
/// numeric keyed-request limit. App composition must register this declaration with the durable
/// `ProviderRateAuthority`, and use the same stable root authorization subject for the Census
/// source, doctor, and jobs. This adapter never creates a private quota pool.
pub fn census_provider_rate_declaration(
    authorization_subject: &SourceIdentifier,
) -> Result<ProviderRateDeclaration, CensusPolicyError> {
    let provider = SourceIdentifier::try_from("us-census")
        .map_err(|_| CensusPolicyError::InvalidRateDeclaration)?;
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(CENSUS_APPLICATION_REQUESTS_PER_SECOND)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_SECOND_NANOS)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CensusPolicyError::InvalidRateDeclaration)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(CENSUS_APPLICATION_REQUESTS_PER_DAY)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_DAY_NANOS)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CensusPolicyError::InvalidRateDeclaration)?,
    ];
    let policy = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(provider, authorization_subject.clone()),
        &windows,
        NonZeroU16::new(1).ok_or(CensusPolicyError::InvalidRateDeclaration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(CENSUS_SECOND_NANOS)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_MAXIMUM_BACKOFF_NANOS)
                .ok_or(CensusPolicyError::InvalidRateDeclaration)?,
            0,
        )
        .map_err(|_| CensusPolicyError::InvalidRateDeclaration)?,
    )
    .map_err(|_| CensusPolicyError::InvalidRateDeclaration)?;
    ProviderRateDeclaration::try_for_authorization_subject(policy, authorization_subject)
        .map_err(|_| CensusPolicyError::InvalidRateDeclaration)
}

/// Census provider-local fixed-policy or rate-policy failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CensusPolicyError {
    /// The code-owned fixed policy or obligation could not be represented or revalidated.
    #[error("invalid Census private-research policy")]
    InvalidPolicy,
    /// The requested operation is outside the fixed private-use/no-commerce boundary.
    #[error("Census use is prohibited by the fixed private-research policy")]
    ProhibitedUse,
    /// The conservative shared provider-rate declaration could not be represented.
    #[error("invalid Census provider-rate declaration")]
    InvalidRateDeclaration,
}
