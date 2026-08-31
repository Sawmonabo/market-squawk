//! SEC submissions and historical companion reconciliation.

use std::sync::Arc;

use serde::{Serialize, Serializer};

use super::*;

const MAX_COMPANY_NAME_BYTES: usize = 512;
const MAX_ENTITY_TYPE_BYTES: usize = 64;
const MAX_SIC_BYTES: usize = 16;
const MAX_SIC_DESCRIPTION_BYTES: usize = 512;
const MAX_TICKER_BYTES: usize = 64;
const MAX_EXCHANGE_BYTES: usize = 128;
const MAX_FORMER_NAMES: usize = 64;
const MAX_TICKER_EXCHANGE_PAIRS: usize = 64;

/// One former EDGAR-conformed company name and its source-reported validity interval.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecFormerName {
    name: String,
    effective_from: Timestamp,
    effective_to: Timestamp,
}

impl SecFormerName {
    /// Returns the former EDGAR-conformed company name exactly as supplied.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the beginning of the source-reported name interval.
    pub const fn effective_from(&self) -> Timestamp {
        self.effective_from
    }

    /// Returns the end of the source-reported name interval.
    pub const fn effective_to(&self) -> Timestamp {
        self.effective_to
    }
}

/// One exact ticker and exchange association from the SEC submissions document.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecTickerExchangePair {
    ticker: String,
    exchange: String,
}

/// One provider-declared historical submissions object and its exact promised coverage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecSubmissionsCompanion {
    name: SourceIdentifier,
    filing_count: u64,
    filing_from: CalendarDate,
    filing_to: CalendarDate,
}

impl SecSubmissionsCompanion {
    /// Returns the exact provider object name.
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the exact provider-declared row count.
    pub const fn filing_count(&self) -> u64 {
        self.filing_count
    }

    /// Returns the first provider-declared filing date.
    pub const fn filing_from(&self) -> CalendarDate {
        self.filing_from
    }

    /// Returns the last provider-declared filing date.
    pub const fn filing_to(&self) -> CalendarDate {
        self.filing_to
    }
}

impl SecTickerExchangePair {
    /// Returns the source ticker without normalization or venue inference.
    pub fn ticker(&self) -> &str {
        &self.ticker
    }

    /// Returns the source exchange paired with this ticker at the same array position.
    pub fn exchange(&self) -> &str {
        &self.exchange
    }
}

/// Bounded company metadata retained from the official SEC submissions shape.
///
/// This record is source evidence. It does not establish canonical instrument identity or venue
/// coverage by itself.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecSubmissionCompanyMetadata {
    conformed_name: String,
    former_names: Vec<SecFormerName>,
    entity_type: Option<String>,
    sic: Option<String>,
    sic_description: Option<String>,
    ticker_exchange_pairs: Vec<SecTickerExchangePair>,
}

impl SecSubmissionCompanyMetadata {
    /// Returns the current EDGAR-conformed company name exactly as supplied.
    pub fn conformed_name(&self) -> &str {
        &self.conformed_name
    }

    /// Returns source-reported former names in provider order.
    pub fn former_names(&self) -> &[SecFormerName] {
        &self.former_names
    }

    /// Returns the source entity type when SEC supplied a nonempty value.
    pub fn entity_type(&self) -> Option<&str> {
        self.entity_type.as_deref()
    }

    /// Returns the source SIC code when SEC supplied a nonempty value.
    pub fn sic(&self) -> Option<&str> {
        self.sic.as_deref()
    }

    /// Returns the source SIC description when SEC supplied a nonempty value.
    pub fn sic_description(&self) -> Option<&str> {
        self.sic_description.as_deref()
    }

    /// Returns exact positional ticker and exchange associations.
    pub fn ticker_exchange_pairs(&self) -> &[SecTickerExchangePair] {
        &self.ticker_exchange_pairs
    }
}

/// One SEC filing reconstructed from the submissions columnar representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecFiling {
    inner: Arc<SecFilingInner>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
struct SecFilingInner {
    accession: SourceIdentifier,
    form: SourceIdentifier,
    filed_on: CalendarDate,
    report_date: Option<CalendarDate>,
    accepted_at: Option<Timestamp>,
    primary_document: Option<SourceIdentifier>,
    size_bytes: Option<u64>,
    is_xbrl: bool,
    is_inline_xbrl: bool,
}

impl SecFiling {
    /// Returns the stable EDGAR accession number.
    pub fn accession(&self) -> &SourceIdentifier {
        &self.inner.accession
    }
    /// Returns the source form code, including amendment suffixes.
    pub fn form(&self) -> &SourceIdentifier {
        &self.inner.form
    }
    /// Returns whether the form code denotes an amendment.
    pub fn is_amendment(&self) -> bool {
        self.inner.form.as_str().ends_with("/A")
    }
    /// Returns exact SEC acceptance time when the provider supplied it.
    pub fn accepted_at(&self) -> Option<Timestamp> {
        self.inner.accepted_at
    }
    /// Returns the filing date without inventing time-of-day availability.
    pub fn filed_on(&self) -> CalendarDate {
        self.inner.filed_on
    }
    /// Returns the source-reported report period.
    pub fn report_date(&self) -> Option<CalendarDate> {
        self.inner.report_date
    }
    /// Returns the provider-declared primary filing document when present.
    pub fn primary_document(&self) -> Option<&SourceIdentifier> {
        self.inner.primary_document.as_ref()
    }
    /// Returns the provider-declared filing size in bytes when present.
    pub fn size_bytes(&self) -> Option<u64> {
        self.inner.size_bytes
    }
    /// Returns whether SEC marks the filing as containing XBRL.
    pub fn is_xbrl(&self) -> bool {
        self.inner.is_xbrl
    }
    /// Returns whether SEC marks the filing as containing Inline XBRL.
    pub fn is_inline_xbrl(&self) -> bool {
        self.inner.is_inline_xbrl
    }
}

impl Serialize for SecFiling {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.inner.serialize(serializer)
    }
}

/// Reconciled SEC submissions document with recent and historical accessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionsDocument {
    inner: Arc<SubmissionsDocumentInner>,
}

#[derive(Debug, Eq, PartialEq)]
struct SubmissionsDocumentInner {
    cik: SourceIdentifier,
    company_metadata: Arc<SecSubmissionCompanyMetadata>,
    filings: Vec<SecFiling>,
    companions: Arc<Vec<SecSubmissionsCompanion>>,
}

impl SubmissionsDocument {
    /// Parses a bounded `submissions/CIK##########.json` document.
    pub fn parse(bytes: &[u8], limits: SecParserLimits) -> Result<Self, SecParserError> {
        Self::parse_with_cancellation(bytes, limits, &CancellationToken::new())
    }

    /// Parses current submissions with cooperative node and record cancellation.
    pub fn parse_with_cancellation(
        bytes: &[u8],
        limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<Self, SecParserError> {
        Self::parse_with_allocation_authority(
            bytes,
            limits,
            cancellation,
            RetainedJsonBudget::new(limits),
        )
    }

    pub(crate) fn parse_with_allocation_authority(
        bytes: &[u8],
        limits: SecParserLimits,
        cancellation: &CancellationToken,
        retained: RetainedJsonBudget,
    ) -> Result<Self, SecParserError> {
        let root = parse_bounded_json_with_allocation_authority(
            bytes,
            limits,
            cancellation,
            retained.clone(),
        )?;
        let object = as_object(&root, "submissions root")?;
        let cik = parse_cik_with_allocation_authority(required(object, "cik")?, &retained)?;
        let company_metadata = parse_company_metadata(object, limits, cancellation, &retained)?;
        let filings_object = as_object(required(object, "filings")?, "filings")?;
        let filings = parse_filing_columns(
            as_object(required(filings_object, "recent")?, "recent filings")?,
            limits,
            cancellation,
            &retained,
        )?;
        for filing in &filings {
            validate_accession_owner(filing.accession(), &cik)?;
        }
        let companions =
            parse_companions(filings_object.get("files"), limits, cancellation, &retained)?;
        retained.admit_bytes(
            std::mem::size_of::<SecSubmissionCompanyMetadata>()
                .checked_add(std::mem::size_of::<Vec<SecSubmissionsCompanion>>())
                .and_then(|bytes| {
                    bytes.checked_add(std::mem::size_of::<SubmissionsDocumentInner>())
                })
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
        Ok(Self::from_inner(SubmissionsDocumentInner {
            cik,
            company_metadata: Arc::new(company_metadata),
            filings,
            companions: Arc::new(companions),
        }))
    }

    /// Parses a bounded historical companion filing-columns document.
    pub fn parse_archive(
        bytes: &[u8],
        limits: SecParserLimits,
    ) -> Result<SubmissionsArchive, SecParserError> {
        Self::parse_archive_with_cancellation(bytes, limits, &CancellationToken::new())
    }

    /// Parses one historical companion with cooperative node and record cancellation.
    pub fn parse_archive_with_cancellation(
        bytes: &[u8],
        limits: SecParserLimits,
        cancellation: &CancellationToken,
    ) -> Result<SubmissionsArchive, SecParserError> {
        Self::parse_archive_with_allocation_authority(
            bytes,
            limits,
            cancellation,
            RetainedJsonBudget::new(limits),
        )
    }

    pub(crate) fn parse_archive_with_allocation_authority(
        bytes: &[u8],
        limits: SecParserLimits,
        cancellation: &CancellationToken,
        retained: RetainedJsonBudget,
    ) -> Result<SubmissionsArchive, SecParserError> {
        let root = parse_bounded_json_with_allocation_authority(
            bytes,
            limits,
            cancellation,
            retained.clone(),
        )?;
        Ok(SubmissionsArchive {
            filings: parse_filing_columns(
                as_object(&root, "archive root")?,
                limits,
                cancellation,
                &retained,
            )?,
        })
    }
    /// Returns the exact zero-padded ten-digit CIK.
    pub fn cik(&self) -> &SourceIdentifier {
        &self.inner.cik
    }
    /// Returns bounded source company metadata without promoting it to canonical identity.
    pub fn company_metadata(&self) -> &SecSubmissionCompanyMetadata {
        &self.inner.company_metadata
    }
    /// Returns accessions ordered deterministically by filing date and accession.
    pub fn filings(&self) -> &[SecFiling] {
        &self.inner.filings
    }
    /// Looks up a filing by accession.
    pub fn filing(&self, accession: &str) -> Option<&SecFiling> {
        self.inner
            .filings
            .iter()
            .find(|filing| filing.accession().as_str() == accession)
    }
    /// Returns provider-declared historical objects with their promised count/date coverage.
    pub fn companions(&self) -> &[SecSubmissionsCompanion] {
        &self.inner.companions
    }

    fn from_inner(inner: SubmissionsDocumentInner) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

/// Parsed historical submissions companion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionsArchive {
    filings: Vec<SecFiling>,
}

impl SubmissionsArchive {
    /// Returns the exact filings parsed from this one historical companion object.
    pub fn filings(&self) -> &[SecFiling] {
        &self.filings
    }
}

/// Deduplicates accessions across recent and companion files without collapsing amendments.
pub fn reconcile_submissions(
    recent: &SubmissionsDocument,
    archives: &[SubmissionsArchive],
    limits: SecParserLimits,
) -> Result<SubmissionsDocument, SecParserError> {
    reconcile_submissions_with_cancellation(recent, archives, limits, &CancellationToken::new())
}

/// Reconciles current and historical submissions with cooperative record cancellation.
pub fn reconcile_submissions_with_cancellation(
    recent: &SubmissionsDocument,
    archives: &[SubmissionsArchive],
    limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<SubmissionsDocument, SecParserError> {
    let retained = RetainedJsonBudget::new(limits);
    admit_document_allocations(&retained, recent)?;
    for archive in archives {
        admit_archive_allocations(&retained, archive)?;
    }
    reconcile_submissions_with_allocation_authority(
        recent,
        archives,
        limits,
        cancellation,
        retained,
    )
}

pub(crate) fn reconcile_submissions_with_allocation_authority(
    recent: &SubmissionsDocument,
    archives: &[SubmissionsArchive],
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: RetainedJsonBudget,
) -> Result<SubmissionsDocument, SecParserError> {
    if recent.inner.companions.len() != archives.len() {
        return Err(SecParserError::InvalidCompanionCoverage);
    }
    for (declaration, archive) in recent.inner.companions.iter().zip(archives) {
        validate_companion_coverage(declaration, archive, &recent.inner.cik)?;
    }
    let total_filings = recent
        .inner
        .filings
        .len()
        .checked_add(
            archives
                .iter()
                .try_fold(0usize, |total, archive| {
                    total.checked_add(archive.filings.len())
                })
                .ok_or(SecParserError::RecordLimitExceeded)?,
        )
        .ok_or(SecParserError::RecordLimitExceeded)?;
    if total_filings > limits.max_records {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut ordered = Vec::new();
    try_reserve_exact_bounded(&mut ordered, total_filings, &retained)?;
    for filing in recent
        .inner
        .filings
        .iter()
        .chain(archives.iter().flat_map(|archive| archive.filings.iter()))
    {
        check_parser_cancelled(cancellation)?;
        ordered.push(filing);
    }
    ordered.sort_unstable_by(|left, right| left.accession().cmp(right.accession()));
    let mut unique_count = 0usize;
    let mut previous: Option<&SecFiling> = None;
    for filing in &ordered {
        match previous {
            Some(existing) if existing.accession() == filing.accession() && existing != *filing => {
                return Err(SecParserError::ConflictingAccession);
            }
            Some(existing) if existing.accession() == filing.accession() => {}
            _ => {
                unique_count = unique_count
                    .checked_add(1)
                    .ok_or(SecParserError::RecordLimitExceeded)?;
                previous = Some(filing);
            }
        }
    }
    let mut filings = Vec::new();
    try_reserve_exact_bounded(&mut filings, unique_count, &retained)?;
    previous = None;
    for filing in ordered {
        if previous.is_none_or(|existing| existing.accession() != filing.accession()) {
            filings.push(filing.clone());
            previous = Some(filing);
        }
    }
    filings.sort_by(|left, right| {
        left.filed_on()
            .cmp(&right.filed_on())
            .then_with(|| left.accession().cmp(right.accession()))
    });
    retained.admit_bytes(recent.inner.cik.retained_bytes())?;
    admit_company_metadata(&retained, &recent.inner.company_metadata)?;
    admit_vec_allocation(&retained, &recent.inner.companions)?;
    for companion in recent.inner.companions.as_slice() {
        retained.admit_bytes(companion.name.retained_bytes())?;
    }
    let cik = recent.inner.cik.clone();
    let company_metadata = Arc::clone(&recent.inner.company_metadata);
    let companions = Arc::clone(&recent.inner.companions);
    retained.admit_bytes(std::mem::size_of::<SubmissionsDocumentInner>())?;
    Ok(SubmissionsDocument::from_inner(SubmissionsDocumentInner {
        cik,
        company_metadata,
        filings,
        companions,
    }))
}

fn parse_company_metadata(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &RetainedJsonBudget,
) -> Result<SecSubmissionCompanyMetadata, SecParserError> {
    let conformed_name =
        validated_metadata_text(required_string(object, "name")?, MAX_COMPANY_NAME_BYTES)?;
    retained.admit_bytes(conformed_name.capacity())?;
    let former_names =
        parse_former_names(object.get("formerNames"), limits, cancellation, retained)?;
    let entity_type = optional_metadata_text(object, "entityType", MAX_ENTITY_TYPE_BYTES)?;
    let sic = optional_metadata_text(object, "sic", MAX_SIC_BYTES)?;
    let sic_description =
        optional_metadata_text(object, "sicDescription", MAX_SIC_DESCRIPTION_BYTES)?;
    for value in [&entity_type, &sic, &sic_description].into_iter().flatten() {
        retained.admit_bytes(value.capacity())?;
    }
    let ticker_exchange_pairs =
        parse_ticker_exchange_pairs(object, limits, cancellation, retained)?;
    Ok(SecSubmissionCompanyMetadata {
        conformed_name,
        former_names,
        entity_type,
        sic,
        sic_description,
        ticker_exchange_pairs,
    })
}

fn parse_former_names(
    value: Option<&Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &RetainedJsonBudget,
) -> Result<Vec<SecFormerName>, SecParserError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let entries = as_array(value, "former names")?;
    if entries.len() > limits.records() || entries.len() > MAX_FORMER_NAMES {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut former_names = Vec::new();
    try_reserve_exact_bounded(&mut former_names, entries.len(), retained)?;
    for value in entries {
        check_parser_cancelled(cancellation)?;
        let object = as_object(value, "former name")?;
        let name =
            validated_metadata_text(required_string(object, "name")?, MAX_COMPANY_NAME_BYTES)?;
        let effective_from = parse_rfc3339_timestamp(required_string(object, "from")?)?;
        let effective_to = parse_rfc3339_timestamp(required_string(object, "to")?)?;
        if effective_from > effective_to {
            return Err(SecParserError::InvalidPeriod);
        }
        retained.admit_bytes(name.capacity())?;
        former_names.push(SecFormerName {
            name,
            effective_from,
            effective_to,
        });
    }
    validate_former_name_associations(&former_names, retained)?;
    Ok(former_names)
}

fn parse_ticker_exchange_pairs(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &RetainedJsonBudget,
) -> Result<Vec<SecTickerExchangePair>, SecParserError> {
    let tickers = required_array(object, "tickers")?;
    let exchanges = required_array(object, "exchanges")?;
    if tickers.len() != exchanges.len() {
        return Err(SecParserError::MetadataAssociationLengthMismatch);
    }
    if tickers.len() > limits.records() || tickers.len() > MAX_TICKER_EXCHANGE_PAIRS {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut pairs = Vec::new();
    try_reserve_exact_bounded(&mut pairs, tickers.len(), retained)?;
    for index in 0..tickers.len() {
        check_parser_cancelled(cancellation)?;
        let ticker = validated_metadata_text(array_string(tickers, index)?, MAX_TICKER_BYTES)?;
        let exchange =
            validated_metadata_text(array_string(exchanges, index)?, MAX_EXCHANGE_BYTES)?;
        retained.admit_bytes(
            ticker
                .capacity()
                .checked_add(exchange.capacity())
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
        pairs.push(SecTickerExchangePair { ticker, exchange });
    }
    validate_ticker_associations(&pairs, retained)?;
    Ok(pairs)
}

fn optional_metadata_text(
    object: &Map<String, Value>,
    key: &str,
    max_bytes: usize,
) -> Result<Option<String>, SecParserError> {
    optional_string(object, key)?
        .filter(|value| !value.is_empty())
        .map(|value| validated_metadata_text(value, max_bytes))
        .transpose()
}

fn parse_filing_columns(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &RetainedJsonBudget,
) -> Result<Vec<SecFiling>, SecParserError> {
    let accessions = required_array(object, "accessionNumber")?;
    if accessions.len() > limits.max_records {
        return Err(SecParserError::RecordLimitExceeded);
    }
    for required_column in ["filingDate", "reportDate", "acceptanceDateTime", "form"] {
        if required_array(object, required_column)?.len() != accessions.len() {
            return Err(SecParserError::ColumnLengthMismatch);
        }
    }
    for optional_column in ["primaryDocument", "size", "isXBRL", "isInlineXBRL"] {
        if object
            .get(optional_column)
            .map(|value| as_array(value, optional_column))
            .transpose()?
            .is_some_and(|column| column.len() != accessions.len())
        {
            return Err(SecParserError::ColumnLengthMismatch);
        }
    }
    let mut filings = Vec::new();
    try_reserve_exact_bounded(&mut filings, accessions.len(), retained)?;
    for index in 0..accessions.len() {
        check_parser_cancelled(cancellation)?;
        let accession = SourceIdentifier::try_from(array_string(accessions, index)?)?;
        validate_accession(accession.as_str())?;
        retained.admit_bytes(std::mem::size_of::<SecFilingInner>())?;
        let filing = SecFiling {
            inner: Arc::new(SecFilingInner {
                accession,
                form: SourceIdentifier::try_from(column_string(object, "form", index)?)?,
                filed_on: parse_date(column_string(object, "filingDate", index)?)?,
                report_date: nonempty_column_string(object, "reportDate", index)?
                    .map(parse_date)
                    .transpose()?,
                accepted_at: nonempty_column_string(object, "acceptanceDateTime", index)?
                    .map(parse_acceptance_timestamp)
                    .transpose()?,
                primary_document: optional_nonempty_column_string(
                    object,
                    "primaryDocument",
                    index,
                )?
                .map(SourceIdentifier::try_from)
                .transpose()?,
                size_bytes: optional_column_u64(object, "size", index)?,
                is_xbrl: optional_column_boolish(object, "isXBRL", index)?.unwrap_or(false),
                is_inline_xbrl: optional_column_boolish(object, "isInlineXBRL", index)?
                    .unwrap_or(false),
            }),
        };
        retained.admit_bytes(filing_dynamic_bytes(&filing)?)?;
        filings.push(filing);
    }
    validate_unique_filing_accessions(&filings, retained)?;
    Ok(filings)
}

fn parse_companions(
    value: Option<&Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &RetainedJsonBudget,
) -> Result<Vec<SecSubmissionsCompanion>, SecParserError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let entries = as_array(value, "companion files")?;
    if entries.len() > limits.max_records {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut companions = Vec::new();
    try_reserve_exact_bounded(&mut companions, entries.len(), retained)?;
    for entry in entries {
        check_parser_cancelled(cancellation)?;
        let object = as_object(entry, "companion file")?;
        let name = required_string(object, "name")?;
        if name.contains('/') || name.contains('\\') || !name.ends_with(".json") {
            return Err(SecParserError::InvalidCompanionName);
        }
        let name = SourceIdentifier::try_from(name)?;
        let filing_count = required(object, "filingCount")?
            .as_u64()
            .filter(|count| *count > 0)
            .ok_or(SecParserError::InvalidCompanionCoverage)?;
        if filing_count > u64::try_from(limits.records()).unwrap_or(u64::MAX) {
            return Err(SecParserError::RecordLimitExceeded);
        }
        let filing_from = parse_date(required_string(object, "filingFrom")?)?;
        let filing_to = parse_date(required_string(object, "filingTo")?)?;
        if filing_from > filing_to {
            return Err(SecParserError::InvalidCompanionCoverage);
        }
        retained.admit_bytes(name.retained_bytes())?;
        companions.push(SecSubmissionsCompanion {
            name,
            filing_count,
            filing_from,
            filing_to,
        });
    }
    validate_unique_companion_names(&companions, retained)?;
    Ok(companions)
}

fn sorted_indices_by<T, F>(
    values: &[T],
    retained: &RetainedJsonBudget,
    mut compare: F,
) -> Result<Vec<usize>, SecParserError>
where
    F: FnMut(&T, &T) -> std::cmp::Ordering,
{
    let mut indices = Vec::new();
    try_reserve_exact_bounded(&mut indices, values.len(), retained)?;
    indices.extend(0..values.len());
    indices.sort_unstable_by(|left, right| compare(&values[*left], &values[*right]));
    Ok(indices)
}

fn validate_former_name_associations(
    values: &[SecFormerName],
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let indices = sorted_indices_by(values, retained, |left, right| left.name.cmp(&right.name))?;
    for pair in indices.windows(2) {
        let left = &values[pair[0]];
        let right = &values[pair[1]];
        if left.name == right.name {
            return if left.effective_from == right.effective_from
                && left.effective_to == right.effective_to
            {
                Err(SecParserError::DuplicateMetadataAssociation)
            } else {
                Err(SecParserError::ConflictingMetadataAssociation)
            };
        }
    }
    Ok(())
}

fn validate_ticker_associations(
    values: &[SecTickerExchangePair],
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let indices = sorted_indices_by(values, retained, |left, right| {
        left.ticker.cmp(&right.ticker)
    })?;
    for pair in indices.windows(2) {
        let left = &values[pair[0]];
        let right = &values[pair[1]];
        if left.ticker == right.ticker {
            return if left.exchange == right.exchange {
                Err(SecParserError::DuplicateMetadataAssociation)
            } else {
                Err(SecParserError::ConflictingMetadataAssociation)
            };
        }
    }
    Ok(())
}

fn validate_unique_filing_accessions(
    values: &[SecFiling],
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let indices = sorted_indices_by(values, retained, |left, right| {
        left.accession().cmp(right.accession())
    })?;
    if indices
        .windows(2)
        .any(|pair| values[pair[0]].accession() == values[pair[1]].accession())
    {
        Err(SecParserError::ConflictingAccession)
    } else {
        Ok(())
    }
}

fn validate_unique_companion_names(
    values: &[SecSubmissionsCompanion],
    retained: &RetainedJsonBudget,
) -> Result<(), SecParserError> {
    let indices = sorted_indices_by(values, retained, |left, right| left.name.cmp(&right.name))?;
    if indices
        .windows(2)
        .any(|pair| values[pair[0]].name == values[pair[1]].name)
    {
        Err(SecParserError::InvalidCompanionCoverage)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_companion_coverage(
    declaration: &SecSubmissionsCompanion,
    archive: &SubmissionsArchive,
    cik: &SourceIdentifier,
) -> Result<(), SecParserError> {
    if u64::try_from(archive.filings.len()).ok() != Some(declaration.filing_count) {
        return Err(SecParserError::InvalidCompanionCoverage);
    }
    for filing in &archive.filings {
        validate_accession_owner(filing.accession(), cik)?;
        if filing.filed_on() < declaration.filing_from || filing.filed_on() > declaration.filing_to
        {
            return Err(SecParserError::InvalidCompanionCoverage);
        }
    }
    Ok(())
}

fn admit_company_metadata(
    retained: &RetainedJsonBudget,
    metadata: &SecSubmissionCompanyMetadata,
) -> Result<(), SecParserError> {
    retained.admit_bytes(metadata.conformed_name.capacity())?;
    admit_vec_allocation(retained, &metadata.former_names)?;
    for former in &metadata.former_names {
        retained.admit_bytes(former.name.capacity())?;
    }
    for value in [
        &metadata.entity_type,
        &metadata.sic,
        &metadata.sic_description,
    ]
    .into_iter()
    .flatten()
    {
        retained.admit_bytes(value.capacity())?;
    }
    admit_vec_allocation(retained, &metadata.ticker_exchange_pairs)?;
    for pair in &metadata.ticker_exchange_pairs {
        retained.admit_bytes(
            pair.ticker
                .capacity()
                .checked_add(pair.exchange.capacity())
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
    }
    Ok(())
}

fn admit_vec_allocation<T>(
    retained: &RetainedJsonBudget,
    values: &Vec<T>,
) -> Result<(), SecParserError> {
    retained.admit_bytes(
        values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
    )
}

fn admit_archive_allocations(
    retained: &RetainedJsonBudget,
    archive: &SubmissionsArchive,
) -> Result<(), SecParserError> {
    admit_vec_allocation(retained, &archive.filings)?;
    for filing in &archive.filings {
        retained.admit_bytes(filing_dynamic_bytes(filing)?)?;
    }
    Ok(())
}

pub(crate) fn admit_document_allocations(
    retained: &RetainedJsonBudget,
    document: &SubmissionsDocument,
) -> Result<(), SecParserError> {
    retained.admit_bytes(document.inner.cik.retained_bytes())?;
    admit_company_metadata(retained, &document.inner.company_metadata)?;
    admit_vec_allocation(retained, &document.inner.filings)?;
    for filing in &document.inner.filings {
        retained.admit_bytes(std::mem::size_of::<SecFilingInner>())?;
        retained.admit_bytes(filing_dynamic_bytes(filing)?)?;
    }
    admit_vec_allocation(retained, &document.inner.companions)?;
    for companion in document.inner.companions.as_slice() {
        retained.admit_bytes(companion.name.retained_bytes())?;
    }
    Ok(())
}

fn filing_dynamic_bytes(filing: &SecFiling) -> Result<usize, SecParserError> {
    [
        Some(filing.accession()),
        Some(filing.form()),
        filing.primary_document(),
    ]
    .into_iter()
    .flatten()
    .try_fold(0usize, |total, value| {
        total
            .checked_add(value.retained_bytes())
            .ok_or(SecParserError::RetainedOutputLimitExceeded)
    })
}
