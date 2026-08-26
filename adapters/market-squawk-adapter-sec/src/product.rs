//! Closed SEC filings, Company Facts, and filing-XBRL product coordinates.
//!
//! These coordinates select evidence-bearing regulatory research objects. They do not establish a
//! tradable security identity, current market state, valuation, forecast, or investment signal.

use std::fmt::Write as _;

use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier,
    Timestamp,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::xbrl::SecValidatedXbrlTaxonomySet;
use crate::{
    RawEvidenceStore, RetrievedSecBytes, RetrievedSubmissions, SecClientError, SecFiling,
    SecObjectLocator, SecParserLimits, SubmissionsDocument,
};

/// Dataset prefix for one registrant's complete submissions history.
pub const SEC_SUBMISSIONS_DATASET_PREFIX: &str = "sec.submissions.cik.";

/// Dataset prefix for one registrant's Company Facts response.
pub const SEC_COMPANY_FACTS_DATASET_PREFIX: &str = "sec.company-facts.cik.";

/// Short canonical commitment prefix for one filing-XBRL dataset selection.
pub(crate) const SEC_FILING_XBRL_DATASET_PREFIX: &str = "sec.filing-xbrl.dataset.sha256.";

const SEC_ACCEPTANCE_EVIDENCE_PREFIX: &str = "sec-submissions-acceptance.sha256.";
const SEC_FILING_XBRL_ANALYTICAL_PREFIX: &str = "sec.filing-xbrl.sha256.";

/// Provider-native SEC research family selected by a product request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SecResearchDatasetKind {
    /// Current submissions plus every provider-declared companion required for completeness.
    Submissions,
    /// One current Company Facts response.
    CompanyFacts,
    /// One exact filing document under an admitted taxonomy-set capability.
    FilingXbrl,
}

/// Authoritative acceptance timestamp bound to the exact submissions payload that supplied it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFilingAcceptanceEvidence {
    accepted_at: Timestamp,
    evidence: SourceIdentifier,
}

impl SecFilingAcceptanceEvidence {
    fn from_captured_row(
        accepted_at: Timestamp,
        row_commitment: EvidenceDigest,
    ) -> Result<Self, SecClientError> {
        if row_commitment.algorithm() != DigestAlgorithm::Sha256
            || row_commitment.bytes().iter().all(|byte| *byte == 0)
        {
            return Err(SecClientError::InvalidLocator);
        }
        Ok(Self {
            accepted_at,
            evidence: SourceIdentifier::try_from(format!(
                "{SEC_ACCEPTANCE_EVIDENCE_PREFIX}{}",
                encode_digest(row_commitment.bytes())?
            ))?,
        })
    }

    /// Returns the exact SEC acceptance instant.
    pub const fn accepted_at(&self) -> Timestamp {
        self.accepted_at
    }

    /// Returns the exact submissions-payload evidence identity supporting the timestamp.
    pub const fn evidence(&self) -> &SourceIdentifier {
        &self.evidence
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SecFilingSubmissionsBinding {
    locator: SourceIdentifier,
    evidence: EvidenceDigest,
    observation_digest: EvidenceDigest,
    size_bytes: u64,
    first_observed_at: Timestamp,
    retrieval_revision: u64,
    row_commitment: EvidenceDigest,
}

/// Exact filing coordinates retained beside one filing-XBRL dataset selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFilingXbrlCoordinates {
    cik: String,
    accession: SourceIdentifier,
    document: SourceIdentifier,
    filing_form: SourceIdentifier,
    filed_on: CalendarDate,
    report_date: Option<CalendarDate>,
    filing_size_bytes: Option<u64>,
    is_inline_xbrl: bool,
    acceptance: Option<SecFilingAcceptanceEvidence>,
    submissions: SecFilingSubmissionsBinding,
}

impl SecFilingXbrlCoordinates {
    pub(crate) fn from_captured_current_submissions(
        retrieved: &RetrievedSubmissions,
        accession: &str,
        raw_store: &RawEvidenceStore,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        parser_limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecClientError> {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let current = retrieved.current_component();
        let locator = SecObjectLocator::submissions(retrieved.document().cik().as_str())?;
        let receipt = current
            .capture_receipt()
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        if current.locator() != Some(locator.url())
            || receipt.source_id() != source_id
            || receipt.metadata_revision() != metadata_revision
        {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        current
            .capture_material()?
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let size_bytes =
            u64::try_from(current.bytes().len()).map_err(|_| SecClientError::ResponseTooLarge)?;
        let reopened = raw_store.read_verified_bounded_cancellable(
            &current.evidence(),
            size_bytes,
            cancellation,
        )?;
        if reopened.as_slice() != current.bytes().as_ref() {
            return Err(SecClientError::RawEvidenceMismatch);
        }
        let current_document =
            SubmissionsDocument::parse_with_cancellation(&reopened, parser_limits, cancellation)?;
        if current_document.cik() != retrieved.document().cik() {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        let filing = current_document
            .filing(accession)
            .ok_or(SecClientError::InvalidCompositeRepresentation)?;
        if retrieved.document().filing(accession) != Some(filing)
            || (!filing.is_xbrl() && !filing.is_inline_xbrl())
        {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        let document = filing
            .primary_document()
            .ok_or(SecClientError::InvalidCompositeRepresentation)?;
        let _validated_locator = SecObjectLocator::filing_document(
            current_document.cik().as_str(),
            filing.accession().as_str(),
            document.as_str(),
        )?;
        let retrieval_revision = current
            .retrieval_revision()
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        let row_commitment = filing_row_commitment(
            current_document.cik(),
            filing,
            document,
            current.evidence(),
            receipt.observation_digest(),
            retrieval_revision,
        );
        let acceptance = filing
            .accepted_at()
            .map(|accepted_at| {
                if accepted_at > current.received_at() {
                    return Err(SecClientError::InvalidCompositeRepresentation);
                }
                SecFilingAcceptanceEvidence::from_captured_row(accepted_at, row_commitment)
            })
            .transpose()?;
        Ok(Self {
            cik: current_document.cik().as_str().to_owned(),
            accession: filing.accession().clone(),
            document: document.clone(),
            filing_form: filing.form().clone(),
            filed_on: filing.filed_on(),
            report_date: filing.report_date(),
            filing_size_bytes: filing.size_bytes(),
            is_inline_xbrl: filing.is_inline_xbrl(),
            acceptance,
            submissions: SecFilingSubmissionsBinding {
                locator: SourceIdentifier::try_from(locator.url())?,
                evidence: current.evidence(),
                observation_digest: receipt.observation_digest(),
                size_bytes,
                first_observed_at: current.received_at(),
                retrieval_revision,
                row_commitment,
            },
        })
    }

    pub(crate) fn revalidate_current_submissions(
        &self,
        current: &RetrievedSecBytes,
        raw_store: &RawEvidenceStore,
        source_id: &SourceId,
        metadata_revision: &MetadataRevision,
        parser_limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<(), SecClientError> {
        let retrieved = RetrievedSubmissions::new(
            SubmissionsDocument::parse_with_cancellation(
                current.bytes(),
                parser_limits,
                cancellation,
            )?,
            current.clone(),
            Vec::new(),
        );
        let rebuilt = Self::from_captured_current_submissions(
            &retrieved,
            self.accession.as_str(),
            raw_store,
            source_id,
            metadata_revision,
            parser_limits,
            cancellation,
        )?;
        if &rebuilt != self {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        Ok(())
    }

    /// Returns the exact zero-padded ten-digit registrant CIK.
    pub fn cik(&self) -> &str {
        &self.cik
    }

    /// Returns the exact filing accession.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }

    /// Returns the exact filing document name.
    pub const fn document(&self) -> &SourceIdentifier {
        &self.document
    }

    /// Returns the exact filing form, including an amendment suffix when present.
    pub const fn filing_form(&self) -> &SourceIdentifier {
        &self.filing_form
    }

    /// Returns the source filing date without inventing a knowledge time.
    pub const fn filed_on(&self) -> CalendarDate {
        self.filed_on
    }

    /// Returns the source-reported report period when present.
    pub const fn report_date(&self) -> Option<CalendarDate> {
        self.report_date
    }

    /// Returns the provider-declared filing size when present.
    pub const fn filing_size_bytes(&self) -> Option<u64> {
        self.filing_size_bytes
    }

    /// Returns whether SEC marked the selected primary document as Inline XBRL.
    pub const fn is_inline_xbrl(&self) -> bool {
        self.is_inline_xbrl
    }

    /// Returns authoritative acceptance evidence when submissions supplied it.
    pub const fn acceptance(&self) -> Option<&SecFilingAcceptanceEvidence> {
        self.acceptance.as_ref()
    }

    pub(crate) fn checked_dynamic_retained_bytes(&self) -> Option<usize> {
        self.cik
            .capacity()
            .checked_add(self.accession.retained_bytes())?
            .checked_add(self.document.retained_bytes())?
            .checked_add(self.filing_form.retained_bytes())?
            .checked_add(
                self.acceptance
                    .as_ref()
                    .map_or(0, |acceptance| acceptance.evidence.retained_bytes()),
            )?
            .checked_add(self.submissions.locator.retained_bytes())
    }
}

/// Exact dataset, initial provider locator, and source-object identity for one ten-digit CIK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecResearchDataset {
    kind: SecResearchDatasetKind,
    cik: String,
    dataset: SourceIdentifier,
    initial_provider_locator: SourceIdentifier,
    source_object_id: SourceIdentifier,
    filing_xbrl: Option<SecFilingXbrlCoordinates>,
    xbrl_taxonomy: Option<SecValidatedXbrlTaxonomySet>,
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

    /// Selects one exact filing document under a registry-minted taxonomy set.
    pub fn filing_xbrl(
        filing: SecFilingXbrlCoordinates,
        taxonomy: SecValidatedXbrlTaxonomySet,
    ) -> Result<Self, SecClientError> {
        let locator = SecObjectLocator::filing_document(
            filing.cik(),
            filing.accession().as_str(),
            filing.document().as_str(),
        )?;
        let dataset = filing_xbrl_identifier(&filing, &taxonomy)?;
        let locator = SourceIdentifier::try_from(locator.url())?;
        Ok(Self {
            kind: SecResearchDatasetKind::FilingXbrl,
            cik: filing.cik().to_owned(),
            dataset,
            initial_provider_locator: locator.clone(),
            source_object_id: locator,
            filing_xbrl: Some(filing),
            xbrl_taxonomy: Some(taxonomy),
        })
    }

    /// Parses only identifier-self-contained SEC dataset families.
    ///
    /// Filing XBRL requires an opaque captured submissions/taxonomy admission and therefore fails
    /// closed when only a serialized identifier is available.
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
            SecResearchDatasetKind::FilingXbrl => return Err(SecClientError::InvalidLocator),
        };
        Ok(Self {
            kind,
            cik: cik.to_owned(),
            dataset: SourceIdentifier::try_from(format!("{dataset_prefix}{cik}"))?,
            initial_provider_locator,
            source_object_id,
            filing_xbrl: None,
            xbrl_taxonomy: None,
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

    /// Returns exact filing metadata only for a filing-XBRL selection.
    pub const fn filing_xbrl_coordinates(&self) -> Option<&SecFilingXbrlCoordinates> {
        self.filing_xbrl.as_ref()
    }

    /// Returns the registry-minted taxonomy set only for a filing-XBRL selection.
    pub const fn xbrl_taxonomy(&self) -> Option<&SecValidatedXbrlTaxonomySet> {
        self.xbrl_taxonomy.as_ref()
    }

    pub(crate) fn into_filing_xbrl_parts(
        self,
    ) -> Result<
        (
            SourceIdentifier,
            SecFilingXbrlCoordinates,
            SecValidatedXbrlTaxonomySet,
        ),
        SecClientError,
    > {
        if self.kind != SecResearchDatasetKind::FilingXbrl {
            return Err(SecClientError::InvalidCompositeRepresentation);
        }
        Ok((
            self.dataset,
            self.filing_xbrl
                .ok_or(SecClientError::InvalidCompositeRepresentation)?,
            self.xbrl_taxonomy
                .ok_or(SecClientError::InvalidCompositeRepresentation)?,
        ))
    }

    /// Returns a storage-safe analytical identity for the exact filing-XBRL coordinate set.
    pub fn analytical_dataset_identifier(&self) -> Result<SourceIdentifier, SecClientError> {
        if self.kind != SecResearchDatasetKind::FilingXbrl {
            return Ok(self.dataset.clone());
        }
        let digest: [u8; 32] = Sha256::digest(self.dataset.as_str().as_bytes()).into();
        SourceIdentifier::try_from(format!(
            "{SEC_FILING_XBRL_ANALYTICAL_PREFIX}{}",
            encode_digest(digest)?
        ))
        .map_err(Into::into)
    }
}

fn filing_xbrl_identifier(
    filing: &SecFilingXbrlCoordinates,
    taxonomy: &SecValidatedXbrlTaxonomySet,
) -> Result<SourceIdentifier, SecClientError> {
    let mut digest = Sha256::new();
    hash_product_field(&mut digest, b"market-squawk/sec-filing-xbrl-dataset/v2");
    hash_product_field(&mut digest, filing.cik.as_bytes());
    hash_product_field(&mut digest, filing.accession.as_str().as_bytes());
    hash_product_field(&mut digest, filing.document.as_str().as_bytes());
    hash_product_field(&mut digest, filing.filing_form.as_str().as_bytes());
    hash_calendar_date(&mut digest, filing.filed_on);
    match filing.report_date {
        Some(report_date) => {
            digest.update([1]);
            hash_calendar_date(&mut digest, report_date);
        }
        None => digest.update([0]),
    }
    match filing.filing_size_bytes {
        Some(size_bytes) => {
            digest.update([1]);
            digest.update(size_bytes.to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest.update([u8::from(filing.is_inline_xbrl)]);
    match &filing.acceptance {
        Some(acceptance) => {
            digest.update([1]);
            digest.update(acceptance.accepted_at.unix_nanos().to_be_bytes());
            hash_product_field(&mut digest, acceptance.evidence.as_str().as_bytes());
        }
        None => digest.update([0]),
    }
    hash_product_field(&mut digest, filing.submissions.locator.as_str().as_bytes());
    digest.update(filing.submissions.evidence.bytes());
    digest.update(filing.submissions.observation_digest.bytes());
    digest.update(filing.submissions.size_bytes.to_be_bytes());
    digest.update(
        filing
            .submissions
            .first_observed_at
            .unix_nanos()
            .to_be_bytes(),
    );
    digest.update(filing.submissions.retrieval_revision.to_be_bytes());
    digest.update(filing.submissions.row_commitment.bytes());
    hash_product_field(&mut digest, taxonomy.version().as_str().as_bytes());
    digest.update(taxonomy.artifact_set().bytes());
    digest.update(taxonomy.fingerprint().bytes());
    SourceIdentifier::try_from(format!(
        "{SEC_FILING_XBRL_DATASET_PREFIX}{}",
        encode_digest(digest.finalize().into())?
    ))
    .map_err(Into::into)
}

fn filing_row_commitment(
    cik: &SourceIdentifier,
    filing: &SecFiling,
    document: &SourceIdentifier,
    submissions_evidence: EvidenceDigest,
    submissions_observation_digest: EvidenceDigest,
    retrieval_revision: u64,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    hash_product_field(&mut digest, b"market-squawk/sec-filing-row/v1");
    hash_product_field(&mut digest, cik.as_str().as_bytes());
    hash_product_field(&mut digest, filing.accession().as_str().as_bytes());
    hash_product_field(&mut digest, filing.form().as_str().as_bytes());
    hash_product_field(&mut digest, document.as_str().as_bytes());
    hash_product_field(&mut digest, filing.filed_on().to_string().as_bytes());
    match filing.report_date() {
        Some(report_date) => {
            digest.update([1]);
            hash_product_field(&mut digest, report_date.to_string().as_bytes());
        }
        None => digest.update([0]),
    }
    match filing.accepted_at() {
        Some(accepted_at) => {
            digest.update([1]);
            digest.update(accepted_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    match filing.size_bytes() {
        Some(size_bytes) => {
            digest.update([1]);
            digest.update(size_bytes.to_be_bytes());
        }
        None => digest.update([0]),
    }
    digest.update([u8::from(filing.is_xbrl())]);
    digest.update([u8::from(filing.is_inline_xbrl())]);
    digest.update(submissions_evidence.bytes());
    digest.update(submissions_observation_digest.bytes());
    digest.update(retrieval_revision.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn hash_product_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(
        u64::try_from(value.len())
            .map_or(u64::MAX, |length| length)
            .to_be_bytes(),
    );
    digest.update(value);
}

fn hash_calendar_date(digest: &mut Sha256, value: CalendarDate) {
    digest.update(value.year().to_be_bytes());
    digest.update([value.month(), value.day()]);
}

fn encode_digest(bytes: [u8; 32]) -> Result<String, SecClientError> {
    let mut encoded = String::new();
    encoded
        .try_reserve_exact(64)
        .map_err(|_| SecClientError::AllocationFailed)?;
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").map_err(|_| SecClientError::InvalidLocator)?;
    }
    Ok(encoded)
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
