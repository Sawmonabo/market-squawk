//! SEC submissions and historical companion reconciliation.

use std::collections::{BTreeMap, btree_map::Entry};

use serde::Serialize;

use super::*;

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
}

/// Reconciled SEC submissions document with recent and historical accessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionsDocument {
    cik: SourceIdentifier,
    filings: Vec<SecFiling>,
    companion_files: Vec<SourceIdentifier>,
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
        let root = parse_bounded_json_with_cancellation(bytes, limits, cancellation)?;
        let object = as_object(&root, "submissions root")?;
        let cik = parse_cik(required(object, "cik")?)?;
        let filings_object = as_object(required(object, "filings")?, "filings")?;
        let filings = parse_filing_columns(
            as_object(required(filings_object, "recent")?, "recent filings")?,
            limits,
            cancellation,
        )?;
        let companion_files =
            parse_companion_files(filings_object.get("files"), limits, cancellation)?;
        Ok(Self {
            cik,
            filings,
            companion_files,
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
        let root = parse_bounded_json_with_cancellation(bytes, limits, cancellation)?;
        Ok(SubmissionsArchive {
            filings: parse_filing_columns(as_object(&root, "archive root")?, limits, cancellation)?,
        })
    }
    /// Returns the exact zero-padded ten-digit CIK.
    pub const fn cik(&self) -> &SourceIdentifier {
        &self.cik
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
    /// Returns provider-declared historical companion object names.
    pub fn companion_files(&self) -> &[SourceIdentifier] {
        &self.companion_files
    }
}

/// Parsed historical submissions companion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionsArchive {
    filings: Vec<SecFiling>,
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
    let mut filings = BTreeMap::new();
    for filing in recent
        .filings
        .iter()
        .chain(archives.iter().flat_map(|archive| archive.filings.iter()))
    {
        check_parser_cancelled(cancellation)?;
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
    let mut filings: Vec<_> = filings.into_values().collect();
    filings.sort_by_key(|filing| (filing.filed_on, filing.accession.as_str().to_owned()));
    Ok(SubmissionsDocument {
        cik: recent.cik.clone(),
        filings,
        companion_files: recent.companion_files.clone(),
    })
}

fn parse_filing_columns(
    object: &Map<String, Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
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
    for index in 0..accessions.len() {
        check_parser_cancelled(cancellation)?;
        let accession = SourceIdentifier::try_from(array_string(accessions, index)?)?;
        validate_accession(accession.as_str())?;
        filings.push(SecFiling {
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
        });
    }
    Ok(filings)
}

fn parse_companion_files(
    value: Option<&Value>,
    limits: SecParserLimits,
    cancellation: &CancellationToken,
) -> Result<Vec<SourceIdentifier>, SecParserError> {
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
    for entry in entries {
        check_parser_cancelled(cancellation)?;
        let name = required_string(as_object(entry, "companion file")?, "name")?;
        if name.contains('/') || name.contains('\\') || !name.ends_with(".json") {
            return Err(SecParserError::InvalidCompanionName);
        }
        companions.push(SourceIdentifier::try_from(name)?);
    }
    Ok(companions)
}
