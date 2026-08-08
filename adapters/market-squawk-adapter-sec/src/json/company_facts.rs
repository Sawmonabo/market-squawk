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
    value: Decimal,
    accession: SourceIdentifier,
    form: SourceIdentifier,
    filed_on: CalendarDate,
    period: CompanyFactPeriod,
    frame: Option<SourceIdentifier>,
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
        let root = parse_bounded_json_with_cancellation(bytes, limits, cancellation)?;
        let object = as_object(&root, "company facts root")?;
        let cik = parse_cik(required(object, "cik")?)?;
        let entity_name = validated_metadata_text(
            required_string(object, "entityName")?,
            MAX_ENTITY_NAME_BYTES,
        )?;
        let taxonomies = as_object(required(object, "facts")?, "facts")?;
        let mut occurrences = Vec::new();
        for (taxonomy, concepts_value) in taxonomies {
            check_parser_cancelled(cancellation)?;
            validate_component(taxonomy)?;
            for (concept, concept_value) in as_object(concepts_value, "taxonomy concepts")? {
                check_parser_cancelled(cancellation)?;
                validate_component(concept)?;
                let qualified = SourceIdentifier::try_from(format!("{taxonomy}:{concept}"))?;
                let units = as_object(
                    required(as_object(concept_value, "concept")?, "units")?,
                    "concept units",
                )?;
                for (unit, facts_value) in units {
                    check_parser_cancelled(cancellation)?;
                    let unit = SourceIdentifier::try_from(unit.clone())?;
                    for fact_value in as_array(facts_value, "unit facts")? {
                        check_parser_cancelled(cancellation)?;
                        if occurrences.len() >= limits.max_records {
                            return Err(SecParserError::RecordLimitExceeded);
                        }
                        if occurrences.len() == occurrences.capacity() {
                            occurrences
                                .try_reserve(1)
                                .map_err(|_| SecParserError::AllocationFailed)?;
                        }
                        occurrences.push(parse_company_fact(
                            fact_value,
                            qualified.clone(),
                            unit.clone(),
                        )?);
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
) -> Result<CompanyFactOccurrence, SecParserError> {
    let object = as_object(value, "company fact occurrence")?;
    let lexical = match required(object, "val")? {
        Value::Number(value) => value.to_string(),
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
        value,
        accession: SourceIdentifier::try_from(required_string(object, "accn")?)?,
        form: SourceIdentifier::try_from(required_string(object, "form")?)?,
        filed_on: parse_date(required_string(object, "filed")?)?,
        period: CompanyFactPeriod { start, end },
        frame: optional_string(object, "frame")?
            .map(SourceIdentifier::try_from)
            .transpose()?,
    })
}
