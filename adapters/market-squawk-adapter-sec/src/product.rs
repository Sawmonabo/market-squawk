//! Closed SEC filings and Company Facts product coordinates.
//!
//! These coordinates select evidence-bearing regulatory research objects. They do not establish a
//! tradable security identity, current market state, valuation, forecast, or investment signal.

use market_squawk_domain::SourceIdentifier;

use crate::{SecClientError, SecObjectLocator};

/// Dataset prefix for one registrant's complete submissions history.
pub const SEC_SUBMISSIONS_DATASET_PREFIX: &str = "sec.submissions.cik.";

/// Dataset prefix for one registrant's Company Facts response.
pub const SEC_COMPANY_FACTS_DATASET_PREFIX: &str = "sec.company-facts.cik.";

/// Provider-native SEC research family selected by a product request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecResearchDatasetKind {
    /// Current submissions plus every provider-declared companion required for completeness.
    Submissions,
    /// One current Company Facts response.
    CompanyFacts,
}

/// Exact dataset, initial provider locator, and source-object identity for one ten-digit CIK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchDataset {
    kind: SecResearchDatasetKind,
    cik: String,
    dataset: SourceIdentifier,
    initial_provider_locator: SourceIdentifier,
    source_object_id: SourceIdentifier,
}

impl SecResearchDataset {
    /// Selects complete submissions for one exact zero-padded CIK.
    pub fn submissions(cik: &str) -> Result<Self, SecClientError> {
        Self::try_new(SecResearchDatasetKind::Submissions, cik)
    }

    /// Selects Company Facts for one exact zero-padded CIK.
    pub fn company_facts(cik: &str) -> Result<Self, SecClientError> {
        Self::try_new(SecResearchDatasetKind::CompanyFacts, cik)
    }

    /// Parses only the two product-admitted SEC dataset families.
    pub fn try_from_identifier(dataset: &SourceIdentifier) -> Result<Self, SecClientError> {
        if let Some(cik) = dataset
            .as_str()
            .strip_prefix(SEC_SUBMISSIONS_DATASET_PREFIX)
        {
            return Self::submissions(cik);
        }
        if let Some(cik) = dataset
            .as_str()
            .strip_prefix(SEC_COMPANY_FACTS_DATASET_PREFIX)
        {
            return Self::company_facts(cik);
        }
        Err(SecClientError::InvalidLocator)
    }

    fn try_new(kind: SecResearchDatasetKind, cik: &str) -> Result<Self, SecClientError> {
        validate_product_cik(cik)?;
        let (dataset_prefix, initial_provider_locator, source_object_id) = match kind {
            SecResearchDatasetKind::Submissions => {
                let locator = SecObjectLocator::submissions(cik)?;
                (
                    SEC_SUBMISSIONS_DATASET_PREFIX,
                    SourceIdentifier::try_from(locator.url())?,
                    SourceIdentifier::try_from(format!("sec.submissions.composite.CIK{cik}"))?,
                )
            }
            SecResearchDatasetKind::CompanyFacts => {
                let locator = SecObjectLocator::company_facts(cik)?;
                let locator = SourceIdentifier::try_from(locator.url())?;
                (SEC_COMPANY_FACTS_DATASET_PREFIX, locator.clone(), locator)
            }
        };
        Ok(Self {
            kind,
            cik: cik.to_owned(),
            dataset: SourceIdentifier::try_from(format!("{dataset_prefix}{cik}"))?,
            initial_provider_locator,
            source_object_id,
        })
    }

    /// Returns the selected provider-native research family.
    pub const fn kind(&self) -> SecResearchDatasetKind {
        self.kind
    }

    /// Returns the exact zero-padded ten-digit registrant CIK.
    pub fn cik(&self) -> &str {
        &self.cik
    }

    /// Returns the dataset identifier consumed by bounded discovery and product queries.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the first exact provider locator in the acquisition graph.
    ///
    /// Submissions may name additional companion locators in the first response. Those companions
    /// must be followed to terminal completeness and are not predicted here.
    pub const fn initial_provider_locator(&self) -> &SourceIdentifier {
        &self.initial_provider_locator
    }

    /// Returns the exact source-object identity produced after complete acquisition.
    pub const fn source_object_id(&self) -> &SourceIdentifier {
        &self.source_object_id
    }
}

/// Paired filings and Company Facts coordinates for one registrant selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchSelection {
    submissions: SecResearchDataset,
    company_facts: SecResearchDataset,
}

impl SecResearchSelection {
    /// Builds both supported SEC research families for one exact ten-digit CIK.
    pub fn try_new(cik: &str) -> Result<Self, SecClientError> {
        Ok(Self {
            submissions: SecResearchDataset::submissions(cik)?,
            company_facts: SecResearchDataset::company_facts(cik)?,
        })
    }

    /// Returns complete-filings acquisition coordinates.
    pub const fn submissions(&self) -> &SecResearchDataset {
        &self.submissions
    }

    /// Returns Company Facts acquisition coordinates.
    pub const fn company_facts(&self) -> &SecResearchDataset {
        &self.company_facts
    }
}

fn validate_product_cik(cik: &str) -> Result<(), SecClientError> {
    if cik.len() != 10
        || !cik.bytes().all(|byte| byte.is_ascii_digit())
        || cik.bytes().all(|byte| byte == b'0')
    {
        Err(SecClientError::InvalidLocator)
    } else {
        Ok(())
    }
}
