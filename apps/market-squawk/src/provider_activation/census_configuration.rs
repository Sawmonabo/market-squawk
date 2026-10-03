//! Key-free native Census configuration, validated by the existing adapter constructors.

use market_squawk_adapter_census::*;
use market_squawk_domain::{ResearchTemporalCoordinate, SourceIdentifier};
use serde::{Deserialize, Serialize};

/// Exact metadata-driven collection configuration; credentials and limits remain code-owned.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CensusRequestConfiguration {
    dataset: DatasetInput,
    selection: SelectionInput,
    predicates: Vec<PredicateInput>,
    geography: GeographyInput,
    time: Option<TimeInput>,
    mappings: Vec<MappingInput>,
    effective_time: EffectiveTimeInput,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetInput {
    vintage: VintageInput,
    path: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum VintageInput {
    Year { year: u16 },
    TimeSeries,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SelectionInput {
    Variables {
        primary: Vec<String>,
        wire: Vec<String>,
    },
    Group {
        group: String,
    },
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PredicateInput {
    variable: String,
    predicate_type: PredicateTypeInput,
    values: Vec<String>,
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PredicateTypeInput {
    String,
    Integer,
    Float,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClauseInput {
    level: String,
    codes: Vec<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum GeographyInput {
    Standard {
        #[serde(rename = "forClause")]
        for_clause: ClauseInput,
        #[serde(rename = "inClauses")]
        in_clauses: Vec<ClauseInput>,
    },
    Uniform {
        values: Vec<String>,
    },
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PointInput {
    Year { year: u16 },
    Month { year: u16, month: u8 },
    Quarter { year: u16, quarter: u8 },
}
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TimeInput {
    At { point: PointInput },
    From { start: PointInput },
    To { end: PointInput },
    Range { start: PointInput, end: PointInput },
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MappingInput {
    variable: String,
    series_namespace: String,
    unit: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum EffectiveTimeInput {
    RequireReportedTime,
    Fixed {
        coordinate: ResearchTemporalCoordinate,
    },
}

impl PointInput {
    fn typed(self) -> Result<CensusTimePoint, CensusAdapterError> {
        match self {
            Self::Year { year } => CensusTimePoint::year(year),
            Self::Month { year, month } => CensusTimePoint::month(year, month),
            Self::Quarter { year, quarter } => CensusTimePoint::quarter(year, quarter),
        }
    }
}
impl ClauseInput {
    fn typed(&self) -> Result<CensusGeographyClause, CensusAdapterError> {
        CensusGeographyClause::try_new(
            &self.level,
            self.codes
                .iter()
                .map(CensusGeographyCode::try_new)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}
impl CensusRequestConfiguration {
    /// Reconstructs the adapter contract without accepting transport URLs or secret material.
    pub fn into_contract(&self) -> Result<CensusDatasetContract, CensusSourceError> {
        let dataset = match self.dataset.vintage {
            VintageInput::Year { year } => CensusDataset::try_new(year, &self.dataset.path)?,
            VintageInput::TimeSeries => CensusDataset::try_time_series(&self.dataset.path)?,
        };
        let selection = match &self.selection {
            SelectionInput::Variables { primary, wire } => {
                let primary = CensusSelection::variables(primary)?
                    .primary_variables()
                    .to_vec();
                let wire = CensusSelection::variables(wire)?.wire_variables().to_vec();
                CensusSelection::Variables { primary, wire }
            }
            SelectionInput::Group { group } => CensusSelection::group(group)?,
        };
        let predicates = self
            .predicates
            .iter()
            .map(|p| {
                CensusPredicate::try_new(
                    &p.variable,
                    match p.predicate_type {
                        PredicateTypeInput::String => CensusPredicateType::String,
                        PredicateTypeInput::Integer => CensusPredicateType::Integer,
                        PredicateTypeInput::Float => CensusPredicateType::Float,
                    },
                    &p.values,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let geography = match &self.geography {
            GeographyInput::Standard {
                for_clause,
                in_clauses,
            } => CensusGeography::standard(
                for_clause.typed()?,
                in_clauses
                    .iter()
                    .map(ClauseInput::typed)
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
            GeographyInput::Uniform { values } => CensusGeography::uniform(
                values
                    .iter()
                    .map(CensusUcgid::try_new)
                    .collect::<Result<Vec<_>, _>>()?,
            )?,
        };
        let time = self
            .time
            .map(|time| -> Result<_, CensusAdapterError> {
                Ok(match time {
                    TimeInput::At { point } => CensusTimePredicate::At {
                        point: point.typed()?,
                    },
                    TimeInput::From { start } => CensusTimePredicate::From {
                        start: start.typed()?,
                    },
                    TimeInput::To { end } => CensusTimePredicate::To { end: end.typed()? },
                    TimeInput::Range { start, end } => {
                        CensusTimePredicate::range(start.typed()?, end.typed()?)?
                    }
                })
            })
            .transpose()?;
        let query = CensusDataQuery::try_new(dataset, selection, predicates, geography, time)?;
        let mappings = self
            .mappings
            .iter()
            .map(|m| {
                let id = |s: &str| {
                    SourceIdentifier::try_from(s)
                        .map_err(|_| CensusSourceError::InvalidConfiguration)
                };
                CensusVariableMapping::try_new(
                    id(&m.variable)?,
                    id(&m.series_namespace)?,
                    id(&m.unit)?,
                )
            })
            .collect::<Result<Vec<_>, CensusSourceError>>()?;
        CensusDatasetContract::try_new(
            query,
            mappings,
            match &self.effective_time {
                EffectiveTimeInput::RequireReportedTime => {
                    CensusEffectiveTimePolicy::RequireReportedTime
                }
                EffectiveTimeInput::Fixed { coordinate } => {
                    CensusEffectiveTimePolicy::Fixed(coordinate.clone())
                }
            },
        )
    }
}
