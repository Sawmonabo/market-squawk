//! SEC submissions and historical companion reconciliation.

use std::collections::{BTreeMap, btree_map::Entry};

use serde::Serialize;

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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SecFiling {
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
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    /// Returns the source form code, including amendment suffixes.
    pub const fn form(&self) -> &SourceIdentifier {
        &self.form
    }
    /// Returns whether the form code denotes an amendment.
    pub fn is_amendment(&self) -> bool {
        self.form.as_str().ends_with("/A")
    }
    /// Returns exact SEC acceptance time when the provider supplied it.
    pub const fn accepted_at(&self) -> Option<Timestamp> {
        self.accepted_at
    }
    /// Returns the filing date without inventing time-of-day availability.
    pub const fn filed_on(&self) -> CalendarDate {
        self.filed_on
    }
    /// Returns the source-reported report period.
    pub const fn report_date(&self) -> Option<CalendarDate> {
        self.report_date
    }
    /// Returns the provider-declared primary filing document when present.
    pub const fn primary_document(&self) -> Option<&SourceIdentifier> {
        self.primary_document.as_ref()
    }
    /// Returns the provider-declared filing size in bytes when present.
    pub const fn size_bytes(&self) -> Option<u64> {
        self.size_bytes
    }
    /// Returns whether SEC marks the filing as containing XBRL.
    pub const fn is_xbrl(&self) -> bool {
        self.is_xbrl
    }
    /// Returns whether SEC marks the filing as containing Inline XBRL.
    pub const fn is_inline_xbrl(&self) -> bool {
        self.is_inline_xbrl
    }
}

/// Reconciled SEC submissions document with recent and historical accessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionsDocument {
    cik: SourceIdentifier,
    company_metadata: SecSubmissionCompanyMetadata,
    filings: Vec<SecFiling>,
    companions: Vec<SecSubmissionsCompanion>,
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
        let (root, mut retained) =
            parse_bounded_json_with_retained_budget(bytes, limits, cancellation)?;
        let object = as_object(&root, "submissions root")?;
        let cik = parse_cik(required(object, "cik")?)?;
        retained.admit::<SubmissionsDocument>(cik.retained_bytes())?;
        let company_metadata = parse_company_metadata(object, limits, cancellation, &mut retained)?;
        let filings_object = as_object(required(object, "filings")?, "filings")?;
        let filings = parse_filing_columns(
            as_object(required(filings_object, "recent")?, "recent filings")?,
            limits,
            cancellation,
            &mut retained,
        )?;
        let companions = parse_companions(
            filings_object.get("files"),
            limits,
            cancellation,
            &mut retained,
        )?;
        Ok(Self {
            cik,
            company_metadata,
            filings,
            companions,
        })
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
        let (root, mut retained) =
            parse_bounded_json_with_retained_budget(bytes, limits, cancellation)?;
        retained.admit::<SubmissionsArchive>(0)?;
        Ok(SubmissionsArchive {
            filings: parse_filing_columns(
                as_object(&root, "archive root")?,
                limits,
                cancellation,
                &mut retained,
            )?,
        })
    }
    /// Returns the exact zero-padded ten-digit CIK.
    pub const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }
    /// Returns bounded source company metadata without promoting it to canonical identity.
    pub const fn company_metadata(&self) -> &SecSubmissionCompanyMetadata {
        &self.company_metadata
    }
    /// Returns accessions ordered deterministically by filing date and accession.
    pub fn filings(&self) -> &[SecFiling] {
        &self.filings
    }
    /// Looks up a filing by accession.
    pub fn filing(&self, accession: &str) -> Option<&SecFiling> {
        self.filings
            .iter()
            .find(|filing| filing.accession.as_str() == accession)
    }
    /// Returns provider-declared historical objects with their promised count/date coverage.
    pub fn companions(&self) -> &[SecSubmissionsCompanion] {
        &self.companions
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
    if recent.companions.len() != archives.len() {
        return Err(SecParserError::InvalidCompanionCoverage);
    }
    for (declaration, archive) in recent.companions.iter().zip(archives) {
        validate_companion_coverage(declaration, archive)?;
    }
    let mut retained = RetainedJsonBudget::new(limits);
    retained.admit::<SubmissionsDocument>(recent.cik.retained_bytes())?;
    admit_company_metadata(&mut retained, &recent.company_metadata)?;
    for companion in &recent.companions {
        retained.admit::<SecSubmissionsCompanion>(companion.name.retained_bytes())?;
    }
    for filing in &recent.filings {
        retained.admit::<SecFiling>(filing_dynamic_bytes(filing)?)?;
    }
    for archive in archives {
        retained.admit::<SubmissionsArchive>(0)?;
        for filing in &archive.filings {
            retained.admit::<SecFiling>(filing_dynamic_bytes(filing)?)?;
        }
    }
    let mut filings = BTreeMap::new();
    for filing in recent
        .filings
        .iter()
        .chain(archives.iter().flat_map(|archive| archive.filings.iter()))
    {
        check_parser_cancelled(cancellation)?;
        retained.admit_btree_entry::<String, SecFiling>(
            filing
                .accession
                .retained_bytes()
                .checked_add(filing_dynamic_bytes(filing)?)
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
        match filings.entry(filing.accession.as_str().to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(filing.clone());
            }
            Entry::Occupied(entry) if entry.get() == filing => {}
            Entry::Occupied(_) => return Err(SecParserError::ConflictingAccession),
        }
        if filings.len() > limits.max_records {
            return Err(SecParserError::RecordLimitExceeded);
        }
    }
    retained.admit::<SubmissionsDocument>(recent.cik.retained_bytes())?;
    admit_company_metadata(&mut retained, &recent.company_metadata)?;
    for companion in &recent.companions {
        retained.admit::<SecSubmissionsCompanion>(companion.name.retained_bytes())?;
    }
    for _ in filings.values() {
        retained.admit::<SecFiling>(0)?;
    }
    let mut filings: Vec<_> = filings.into_values().collect();
    filings.sort_by(|left, right| {
        left.filed_on
            .cmp(&right.filed_on)
            .then_with(|| left.accession.cmp(&right.accession))
    });
    Ok(SubmissionsDocument {
        cik: recent.cik.clone(),
        company_metadata: recent.company_metadata.clone(),
        filings,
        companions: recent.companions.clone(),
    })
}

fn parse_company_metadata(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &mut RetainedJsonBudget,
) -> Result<SecSubmissionCompanyMetadata, SecParserError> {
    let conformed_name =
        validated_metadata_text(required_string(object, "name")?, MAX_COMPANY_NAME_BYTES)?;
    retained.admit::<SecSubmissionCompanyMetadata>(conformed_name.capacity())?;
    let former_names =
        parse_former_names(object.get("formerNames"), limits, cancellation, retained)?;
    let entity_type = optional_metadata_text(object, "entityType", MAX_ENTITY_TYPE_BYTES)?;
    let sic = optional_metadata_text(object, "sic", MAX_SIC_BYTES)?;
    let sic_description =
        optional_metadata_text(object, "sicDescription", MAX_SIC_DESCRIPTION_BYTES)?;
    for value in [&entity_type, &sic, &sic_description].into_iter().flatten() {
        retained.admit::<String>(value.capacity())?;
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
    retained: &mut RetainedJsonBudget,
) -> Result<Vec<SecFormerName>, SecParserError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let entries = as_array(value, "former names")?;
    if entries.len() > limits.records() || entries.len() > MAX_FORMER_NAMES {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut former_names = Vec::new();
    former_names
        .try_reserve(entries.len())
        .map_err(|_| SecParserError::AllocationFailed)?;
    let mut seen = BTreeMap::new();
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
        let interval = (effective_from, effective_to);
        retained.admit_btree_entry::<String, (Timestamp, Timestamp)>(name.capacity())?;
        match seen.entry(name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(interval);
            }
            Entry::Occupied(entry) if *entry.get() == interval => {
                return Err(SecParserError::DuplicateMetadataAssociation);
            }
            Entry::Occupied(_) => {
                return Err(SecParserError::ConflictingMetadataAssociation);
            }
        }
        retained.admit::<SecFormerName>(name.capacity())?;
        former_names.push(SecFormerName {
            name,
            effective_from,
            effective_to,
        });
    }
    Ok(former_names)
}

fn parse_ticker_exchange_pairs(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &mut RetainedJsonBudget,
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
    pairs
        .try_reserve(tickers.len())
        .map_err(|_| SecParserError::AllocationFailed)?;
    let mut seen = BTreeMap::new();
    for index in 0..tickers.len() {
        check_parser_cancelled(cancellation)?;
        let ticker = validated_metadata_text(array_string(tickers, index)?, MAX_TICKER_BYTES)?;
        let exchange =
            validated_metadata_text(array_string(exchanges, index)?, MAX_EXCHANGE_BYTES)?;
        retained.admit_btree_entry::<String, String>(
            ticker
                .capacity()
                .checked_add(exchange.capacity())
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
        match seen.entry(ticker.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(exchange.clone());
            }
            Entry::Occupied(entry) if entry.get() == &exchange => {
                return Err(SecParserError::DuplicateMetadataAssociation);
            }
            Entry::Occupied(_) => {
                return Err(SecParserError::ConflictingMetadataAssociation);
            }
        }
        retained.admit::<SecTickerExchangePair>(
            ticker
                .capacity()
                .checked_add(exchange.capacity())
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
        pairs.push(SecTickerExchangePair { ticker, exchange });
    }
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
    retained: &mut RetainedJsonBudget,
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
    filings
        .try_reserve(accessions.len())
        .map_err(|_| SecParserError::AllocationFailed)?;
    let mut page_accessions = BTreeMap::new();
    for index in 0..accessions.len() {
        check_parser_cancelled(cancellation)?;
        let accession = SourceIdentifier::try_from(array_string(accessions, index)?)?;
        validate_accession(accession.as_str())?;
        let filing = SecFiling {
            accession,
            form: SourceIdentifier::try_from(column_string(object, "form", index)?)?,
            filed_on: parse_date(column_string(object, "filingDate", index)?)?,
            report_date: nonempty_column_string(object, "reportDate", index)?
                .map(parse_date)
                .transpose()?,
            accepted_at: nonempty_column_string(object, "acceptanceDateTime", index)?
                .map(parse_acceptance_timestamp)
                .transpose()?,
            primary_document: optional_nonempty_column_string(object, "primaryDocument", index)?
                .map(SourceIdentifier::try_from)
                .transpose()?,
            size_bytes: optional_column_u64(object, "size", index)?,
            is_xbrl: optional_column_boolish(object, "isXBRL", index)?.unwrap_or(false),
            is_inline_xbrl: optional_column_boolish(object, "isInlineXBRL", index)?
                .unwrap_or(false),
        };
        retained.admit::<SecFiling>(filing_dynamic_bytes(&filing)?)?;
        retained.admit_btree_entry::<SourceIdentifier, ()>(filing.accession.retained_bytes())?;
        if page_accessions
            .insert(filing.accession.clone(), ())
            .is_some()
        {
            return Err(SecParserError::ConflictingAccession);
        }
        filings.push(filing);
    }
    Ok(filings)
}

fn parse_companions(
    value: Option<&Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
    retained: &mut RetainedJsonBudget,
) -> Result<Vec<SecSubmissionsCompanion>, SecParserError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let entries = as_array(value, "companion files")?;
    if entries.len() > limits.max_records {
        return Err(SecParserError::RecordLimitExceeded);
    }
    let mut companions = Vec::new();
    companions
        .try_reserve(entries.len())
        .map_err(|_| SecParserError::AllocationFailed)?;
    let mut names = BTreeMap::new();
    for entry in entries {
        check_parser_cancelled(cancellation)?;
        let object = as_object(entry, "companion file")?;
        let name = required_string(object, "name")?;
        if name.contains('/') || name.contains('\\') || !name.ends_with(".json") {
            return Err(SecParserError::InvalidCompanionName);
        }
        let name = SourceIdentifier::try_from(name)?;
        retained.admit_btree_entry::<SourceIdentifier, ()>(name.retained_bytes())?;
        if names.insert(name.clone(), ()).is_some() {
            return Err(SecParserError::InvalidCompanionCoverage);
        }
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
        retained.admit::<SecSubmissionsCompanion>(name.retained_bytes())?;
        companions.push(SecSubmissionsCompanion {
            name,
            filing_count,
            filing_from,
            filing_to,
        });
    }
    Ok(companions)
}

pub(crate) fn validate_companion_coverage(
    declaration: &SecSubmissionsCompanion,
    archive: &SubmissionsArchive,
) -> Result<(), SecParserError> {
    if u64::try_from(archive.filings.len()).ok() != Some(declaration.filing_count) {
        return Err(SecParserError::InvalidCompanionCoverage);
    }
    for filing in &archive.filings {
        if filing.filed_on < declaration.filing_from || filing.filed_on > declaration.filing_to {
            return Err(SecParserError::InvalidCompanionCoverage);
        }
    }
    Ok(())
}

fn admit_company_metadata(
    retained: &mut RetainedJsonBudget,
    metadata: &SecSubmissionCompanyMetadata,
) -> Result<(), SecParserError> {
    retained.admit::<SecSubmissionCompanyMetadata>(metadata.conformed_name.capacity())?;
    for former in &metadata.former_names {
        retained.admit::<SecFormerName>(former.name.capacity())?;
    }
    for value in [
        &metadata.entity_type,
        &metadata.sic,
        &metadata.sic_description,
    ]
    .into_iter()
    .flatten()
    {
        retained.admit::<String>(value.capacity())?;
    }
    for pair in &metadata.ticker_exchange_pairs {
        retained.admit::<SecTickerExchangePair>(
            pair.ticker
                .capacity()
                .checked_add(pair.exchange.capacity())
                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
        )?;
    }
    Ok(())
}

fn filing_dynamic_bytes(filing: &SecFiling) -> Result<usize, SecParserError> {
    [
        Some(&filing.accession),
        Some(&filing.form),
        filing.primary_document.as_ref(),
    ]
    .into_iter()
    .flatten()
    .try_fold(0usize, |total, value| {
        total
            .checked_add(value.retained_bytes())
            .ok_or(SecParserError::RetainedOutputLimitExceeded)
    })
}
