//! SEC Company Facts JSON parsing.

use rust_decimal::Decimal;
use serde::Serialize;

use super::*;

const MAX_ENTITY_NAME_BYTES: usize = 512;

/// Instant or duration period from a Company Facts occurrence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CompanyFactPeriod {
    start: Option<CalendarDate>,
    end: CalendarDate,
}

impl CompanyFactPeriod {
    /// Returns the optional duration start.
    pub const fn start(self) -> Option<CalendarDate> {
        self.start
    }

    /// Returns the instant or duration end date.
    pub const fn end(self) -> CalendarDate {
        self.end
    }
}

/// One exact numeric occurrence from SEC Company Facts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CompanyFactOccurrence {
    concept: SourceIdentifier,
    unit: SourceIdentifier,
    source_ordinal: u32,
    value: Decimal,
    accession: SourceIdentifier,
    form: SourceIdentifier,
    filed_on: CalendarDate,
    period: CompanyFactPeriod,
    frame: Option<SourceIdentifier>,
    fiscal_year: Option<u16>,
    fiscal_period: Option<SourceIdentifier>,
}

impl CompanyFactOccurrence {
    /// Returns the qualified taxonomy concept.
    pub const fn concept(&self) -> &SourceIdentifier {
        &self.concept
    }
    /// Returns the source unit key.
    pub const fn unit(&self) -> &SourceIdentifier {
        &self.unit
    }
    /// Returns the zero-based occurrence coordinate inside the exact concept/unit source array.
    pub const fn source_ordinal(&self) -> u32 {
        self.source_ordinal
    }
    /// Returns the exact normalized decimal.
    pub const fn value(&self) -> Decimal {
        self.value
    }
    /// Returns the filing accession carrying this occurrence.
    pub const fn accession(&self) -> &SourceIdentifier {
        &self.accession
    }
    /// Returns instant or duration semantics.
    pub const fn period(&self) -> CompanyFactPeriod {
        self.period
    }
    /// Returns the filing date without inventing a publication time.
    pub const fn filed_on(&self) -> CalendarDate {
        self.filed_on
    }
    /// Returns the source filing form, including amendment suffixes.
    pub const fn form(&self) -> &SourceIdentifier {
        &self.form
    }
    /// Returns the optional SEC frame identity.
    pub const fn frame(&self) -> Option<&SourceIdentifier> {
        self.frame.as_ref()
    }
    /// Returns the source-reported fiscal year when supplied.
    pub const fn fiscal_year(&self) -> Option<u16> {
        self.fiscal_year
    }
    /// Returns the exact source fiscal-period code when supplied.
    pub const fn fiscal_period(&self) -> Option<&SourceIdentifier> {
        self.fiscal_period.as_ref()
    }
}

/// Parsed numeric Company Facts document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompanyFactsDocument {
    cik: SourceIdentifier,
    entity_name: String,
    occurrences: Vec<CompanyFactOccurrence>,
}

impl CompanyFactsDocument {
    /// Parses the SEC `api/xbrl/companyfacts/CIK##########.json` shape exactly.
    pub fn parse(bytes: &[u8], limits: SecParserLimits) -> Result<Self, SecParserError> {
        Self::parse_with_cancellation(bytes, limits, &CancellationToken::new())
    }

    /// Parses Company Facts with cooperative node and occurrence cancellation.
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
        let object = as_object(&root, "company facts root")?;
        let cik = parse_cik_with_allocation_authority(required(object, "cik")?, &retained)?;
        let entity_name_source = required_string(object, "entityName")?;
        super::admit_string_allocation(entity_name_source, &retained)?;
        let entity_name = validated_metadata_text(entity_name_source, MAX_ENTITY_NAME_BYTES)?;
        let taxonomies = as_object(required(object, "facts")?, "facts")?;
        let mut occurrences = Vec::new();
        for (taxonomy, concepts_value) in taxonomies {
            check_parser_cancelled(cancellation)?;
            validate_component(taxonomy)?;
            for (concept, concept_value) in as_object(concepts_value, "taxonomy concepts")? {
                check_parser_cancelled(cancellation)?;
                validate_component(concept)?;
                let qualified_len = taxonomy
                    .len()
                    .checked_add(1)
                    .and_then(|length| length.checked_add(concept.len()))
                    .ok_or(SecParserError::RetainedOutputLimitExceeded)?;
                retained.admit_bytes(
                    qualified_len
                        .checked_next_power_of_two()
                        .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
                )?;
                let qualified_text = format!("{taxonomy}:{concept}");
                let qualified = SourceIdentifier::try_from(qualified_text)?;
                let units = as_object(
                    required(as_object(concept_value, "concept")?, "units")?,
                    "concept units",
                )?;
                for (unit, facts_value) in units {
                    check_parser_cancelled(cancellation)?;
                    let unit = super::source_identifier_bounded(unit, &retained)?;
                    for (source_ordinal, fact_value) in
                        as_array(facts_value, "unit facts")?.iter().enumerate()
                    {
                        check_parser_cancelled(cancellation)?;
                        if occurrences.len() >= limits.max_records {
                            return Err(SecParserError::RecordLimitExceeded);
                        }
                        let source_ordinal = u32::try_from(source_ordinal)
                            .map_err(|_| SecParserError::RecordLimitExceeded)?;
                        retained.admit_bytes(
                            qualified
                                .retained_bytes()
                                .checked_add(unit.retained_bytes())
                                .ok_or(SecParserError::RetainedOutputLimitExceeded)?,
                        )?;
                        let occurrence = parse_company_fact(
                            fact_value,
                            qualified.clone(),
                            unit.clone(),
                            source_ordinal,
                            &retained,
                        )?;
                        validate_accession_owner(occurrence.accession(), &cik)?;
                        if occurrences.len() == occurrences.capacity() {
                            try_reserve_exact_bounded(&mut occurrences, 1, &retained)?;
                        }
                        occurrences.push(occurrence);
                    }
                }
            }
        }
        Ok(Self {
            cik,
            entity_name,
            occurrences,
        })
    }

    /// Returns the zero-padded CIK.
    pub const fn cik(&self) -> &SourceIdentifier {
        &self.cik
    }

    /// Returns Company Facts `entityName` as independent corroborating source evidence.
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Returns every retained numeric occurrence without collapsing amendments.
    pub fn occurrences(&self) -> &[CompanyFactOccurrence] {
        &self.occurrences
    }
}

fn parse_company_fact(
    value: &Value,
    concept: SourceIdentifier,
    unit: SourceIdentifier,
    source_ordinal: u32,
    retained: &RetainedJsonBudget,
) -> Result<CompanyFactOccurrence, SecParserError> {
    let object = as_object(value, "company fact occurrence")?;
    let lexical = match required(object, "val")? {
        Value::Number(value) => {
            // serde_json's lexical number representation is bounded by the decoded input. Admit
            // a conservative decimal rendering ceiling before allocating its owned form.
            retained.admit_bytes(128)?;
            value.to_string()
        }
        _ => return Err(SecParserError::NonNumericCompanyFact),
    };
    let value = lexical
        .parse::<Decimal>()
        .map_err(|_| SecParserError::InvalidDecimal)?
        .normalize();
    let end = parse_date(required_string(object, "end")?)?;
    let start = optional_string(object, "start")?
        .map(parse_date)
        .transpose()?;
    if start.is_some_and(|date| date > end) {
        return Err(SecParserError::InvalidPeriod);
    }
    Ok(CompanyFactOccurrence {
        concept,
        unit,
        source_ordinal,
        value,
        accession: super::source_identifier_bounded(required_string(object, "accn")?, retained)?,
        form: super::source_identifier_bounded(required_string(object, "form")?, retained)?,
        filed_on: parse_date(required_string(object, "filed")?)?,
        period: CompanyFactPeriod { start, end },
        frame: optional_string(object, "frame")?
            .map(|value| super::source_identifier_bounded(value, retained))
            .transpose()?,
        fiscal_year: parse_optional_fiscal_year(object)?,
        fiscal_period: parse_optional_fiscal_period(object, retained)?,
    })
}

fn parse_optional_fiscal_year(
    object: &serde_json::Map<String, Value>,
) -> Result<Option<u16>, SecParserError> {
    match object.get("fy") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .filter(|value| *value != 0)
            .map(Some)
            .ok_or(SecParserError::InvalidFiscalContext),
        Some(_) => Err(SecParserError::WrongType),
    }
}

fn parse_optional_fiscal_period(
    object: &serde_json::Map<String, Value>,
    retained: &RetainedJsonBudget,
) -> Result<Option<SourceIdentifier>, SecParserError> {
    match object.get("fp") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => super::source_identifier_bounded(value, retained)
            .map(Some)
            .map_err(Into::into),
        Some(_) => Err(SecParserError::WrongType),
    }
}
