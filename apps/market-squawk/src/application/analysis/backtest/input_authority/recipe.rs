//! Canonical immutable governed-backtest recipes and their expected authority evidence.

mod corporate_actions;
mod manifest;
mod policy;

use std::fmt;

use market_squawk_backtesting::{
    BacktestLimits, BacktestLimitsInput, PortfolioSeed, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput,
};
use market_squawk_data::{
    CatalogLimit, DatasetManifestRef, DatasetSchemaRegistry, QueryLimits, QueryRequest,
};
use market_squawk_domain::{InstrumentId, SourceId, SourceIdentifier, Timestamp};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{BacktestExperimentPlan, PinnedBacktestInput};

use super::super::{BacktestScope, GovernedBacktestCommand};

pub use corporate_actions::GovernedBacktestCorporateActionsInput;
pub use policy::{GovernedBacktestPortfolioSeedInput, GovernedBacktestQueryLimitsInput};

use corporate_actions::CorporateActionsWire;
use manifest::ManifestWire;
pub(in crate::application::analysis::backtest::input_authority) use manifest::{
    ManifestAuthorityWire, sort_manifest_authorities, validate_manifest_authorities,
};
use policy::{
    BacktestLimitsWire, ExecutionAssumptionsWire, ExperimentWire, PortfolioSeedWire,
    QueryLimitsWire,
};

const INPUT_RECIPE_SCHEMA_VERSION: u16 = 1;
const INPUT_REGISTRATION_SCHEMA_VERSION: u16 = 1;
const MAX_INLINE_QUERY_BYTES: u64 = 256 * 1024;

/// Maximum accepted encoded size of one governed-backtest registration request.
pub const MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES: u64 = 8 * 1024 * 1024;

/// Complete untrusted registration request for one immutable governed-backtest input.
pub struct GovernedBacktestInputRegistrationInput {
    pub strategy_id: SourceIdentifier,
    pub manifest: DatasetManifestRef,
    pub table_name: String,
    pub sql: String,
    pub query_limits: GovernedBacktestQueryLimitsInput,
    pub instruments: Vec<InstrumentId>,
    pub starts_at: Timestamp,
    pub ends_at: Timestamp,
    pub definition_history_limit: usize,
    pub execution_assumptions: ResearchExecutionAssumptionsInput,
    pub portfolio: GovernedBacktestPortfolioSeedInput,
    pub corporate_actions: Option<GovernedBacktestCorporateActionsInput>,
    pub sources: Vec<SourceId>,
    pub seed: u64,
    pub limits: BacktestLimitsInput,
    pub experiment: BacktestExperimentPlan,
    /// Optional V1 cohort evidence recipe. Legacy registrations omit this field and continue to
    /// resolve as a single governed trial.
    pub cohort: Option<GovernedBacktestCohortRegistrationInput>,
}

/// Versioned predeclared cohort topology submitted at the same authority boundary as its inputs.
#[derive(Clone, Debug)]
pub struct GovernedBacktestCohortRegistrationInput {
    pub generator_version: SourceIdentifier,
    pub generator_parameters: Vec<market_squawk_backtesting::TrialParameter>,
    pub members: Vec<GovernedBacktestCohortMemberRegistrationInput>,
    pub folds: Vec<Vec<GovernedBacktestCohortCandidateRegistrationInput>>,
    pub selection_member_ids: Vec<SourceIdentifier>,
}

/// One independently queryable cohort member. Shared execution authority stays on the enclosing
/// registration; dataset, query, definition cutoff, and selected parameters are member-bound.
#[derive(Clone, Debug)]
pub struct GovernedBacktestCohortMemberRegistrationInput {
    pub member_id: SourceIdentifier,
    pub manifest: DatasetManifestRef,
    pub table_name: String,
    pub sql: String,
    pub query_limits: GovernedBacktestQueryLimitsInput,
    pub instruments: Vec<InstrumentId>,
    pub starts_at: Timestamp,
    pub ends_at: Timestamp,
    pub definition_history_limit: usize,
    pub experiment: BacktestExperimentPlan,
}

/// One deterministic member-key pairing for an in-sample/out-of-sample fold candidate.
#[derive(Clone, Debug)]
pub struct GovernedBacktestCohortCandidateRegistrationInput {
    pub in_sample_member_id: SourceIdentifier,
    pub out_of_sample_member_id: SourceIdentifier,
}

impl GovernedBacktestInputRegistrationInput {
    /// Decodes one versioned closed JSON request through the durable recipe wire contract.
    ///
    /// Collection order, financial representations, point-in-time scope, query policy, and all
    /// resource limits must already be canonical. Unknown fields and unsupported schema versions
    /// are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`GovernedBacktestInputRegistrationJsonError::Invalid`] when JSON or any nested
    /// domain value violates the closed registration contract. Returns
    /// [`GovernedBacktestInputRegistrationJsonError::ResourceExhausted`] before decoding an
    /// oversized request or when canonical reconstruction cannot reserve bounded memory.
    pub fn try_from_json(bytes: &[u8]) -> Result<Self, GovernedBacktestInputRegistrationJsonError> {
        let encoded_bytes = u64::try_from(bytes.len())
            .map_err(|_| GovernedBacktestInputRegistrationJsonError::ResourceExhausted)?;
        if bytes.is_empty() || encoded_bytes > MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES {
            return Err(GovernedBacktestInputRegistrationJsonError::ResourceExhausted);
        }
        let wire: InputRegistrationWire = serde_json::from_slice(bytes)
            .map_err(|_| GovernedBacktestInputRegistrationJsonError::Invalid)?;
        if wire.schema_version != INPUT_REGISTRATION_SCHEMA_VERSION {
            return Err(GovernedBacktestInputRegistrationJsonError::Invalid);
        }
        wire.input.into_registration_input().map_err(Into::into)
    }
}

impl fmt::Debug for GovernedBacktestInputRegistrationInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernedBacktestInputRegistrationInput")
            .field("strategy_id", &self.strategy_id)
            .field("manifest", &self.manifest)
            .field("table_name", &self.table_name)
            .field("sql", &"[READ-ONLY QUERY]")
            .field("instrument_count", &self.instruments.len())
            .field("source_count", &self.sources.len())
            .field(
                "cohort_member_count",
                &self
                    .cohort
                    .as_ref()
                    .map_or(0, |cohort| cohort.members.len()),
            )
            .finish_non_exhaustive()
    }
}

/// Closed JSON decoding failure at the governed-backtest registration boundary.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum GovernedBacktestInputRegistrationJsonError {
    /// JSON, schema version, canonical ordering, or a nested domain invariant was invalid.
    #[error("governed-backtest registration JSON is invalid or noncanonical")]
    Invalid,
    /// The encoded request or bounded canonical reconstruction exceeded its resource contract.
    #[error("governed-backtest registration JSON exceeded its resource contract")]
    ResourceExhausted,
}

impl From<RecipeError> for GovernedBacktestInputRegistrationJsonError {
    fn from(value: RecipeError) -> Self {
        match value {
            RecipeError::Invalid => Self::Invalid,
            RecipeError::ResourceExhausted => Self::ResourceExhausted,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct RegistrationRecipe {
    wire: InputCoreWire,
}

impl RegistrationRecipe {
    pub(super) fn try_new(
        input: GovernedBacktestInputRegistrationInput,
    ) -> Result<Self, RecipeError> {
        let mut instruments = input.instruments;
        instruments.sort_unstable();
        let mut sources = input.sources;
        sources.sort_unstable();
        let query_limits = QueryLimitsWire::try_from_input(input.query_limits)?;
        let wire = InputCoreWire {
            strategy_id: input.strategy_id,
            manifest: ManifestWire::from_manifest(&input.manifest),
            table_name: input.table_name,
            sql: input.sql,
            query_limits,
            instruments,
            starts_at_unix_nanos: input.starts_at.unix_nanos(),
            ends_at_unix_nanos: input.ends_at.unix_nanos(),
            definition_history_limit: input.definition_history_limit,
            execution_assumptions: ExecutionAssumptionsWire::try_from_input(
                input.execution_assumptions,
            )?,
            portfolio: PortfolioSeedWire::try_from_input(input.portfolio)?,
            corporate_actions: input
                .corporate_actions
                .map(CorporateActionsWire::try_from_input)
                .transpose()?,
            sources,
            seed: input.seed,
            limits: BacktestLimitsWire::try_from_input(input.limits)?,
            experiment: ExperimentWire::try_from_plan(input.experiment)?,
            cohort: input.cohort.map(CohortWire::try_from_input).transpose()?,
        };
        wire.validate()?;
        Ok(Self { wire })
    }

    pub(super) fn bind(self, expected: ExpectedEvidence) -> Result<InputRecipe, RecipeError> {
        let recipe = InputRecipe {
            wire: InputRecipeWire {
                schema_version: INPUT_RECIPE_SCHEMA_VERSION,
                core: self.wire,
                expected: ExpectedEvidenceWire::from_expected(expected),
            },
        };
        recipe.validate()?;
        Ok(recipe)
    }

    pub(super) fn core(&self) -> &InputCoreWire {
        &self.wire
    }

    /// Returns the canonical bytes bound by a guided-preparation receipt before materialization.
    pub(super) fn canonical_core_bytes(&self) -> Result<Vec<u8>, RecipeError> {
        serde_json::to_vec(&self.wire).map_err(|_| RecipeError::Invalid)
    }

    /// Reconstructs the exact registration after a preparation receipt has been consumed.
    pub(super) fn into_registration_input(
        self,
    ) -> Result<GovernedBacktestInputRegistrationInput, RecipeError> {
        self.wire.into_registration_input()
    }
}

#[derive(Clone, Debug)]
pub(super) struct InputRecipe {
    wire: InputRecipeWire,
}

impl InputRecipe {
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, RecipeError> {
        let wire: InputRecipeWire =
            serde_json::from_slice(bytes).map_err(|_| RecipeError::Invalid)?;
        let recipe = Self { wire };
        recipe.validate()?;
        if recipe.encode()? != bytes {
            return Err(RecipeError::Invalid);
        }
        Ok(recipe)
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, RecipeError> {
        serde_json::to_vec(&self.wire).map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn core(&self) -> &InputCoreWire {
        &self.wire.core
    }

    pub(super) fn expected(&self) -> Result<ExpectedEvidence, RecipeError> {
        self.wire.expected.to_expected()
    }

    fn validate(&self) -> Result<(), RecipeError> {
        if self.wire.schema_version != INPUT_RECIPE_SCHEMA_VERSION {
            return Err(RecipeError::Invalid);
        }
        self.wire.core.validate()?;
        self.wire.expected.validate(&self.wire.core)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputRecipeWire {
    schema_version: u16,
    core: InputCoreWire,
    expected: ExpectedEvidenceWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InputRegistrationWire {
    schema_version: u16,
    input: InputCoreWire,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InputCoreWire {
    strategy_id: SourceIdentifier,
    manifest: ManifestWire,
    table_name: String,
    sql: String,
    query_limits: QueryLimitsWire,
    instruments: Vec<InstrumentId>,
    starts_at_unix_nanos: i64,
    ends_at_unix_nanos: i64,
    definition_history_limit: usize,
    execution_assumptions: ExecutionAssumptionsWire,
    portfolio: PortfolioSeedWire,
    corporate_actions: Option<CorporateActionsWire>,
    sources: Vec<SourceId>,
    seed: u64,
    limits: BacktestLimitsWire,
    experiment: ExperimentWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cohort: Option<CohortWire>,
}

impl InputCoreWire {
    fn validate(&self) -> Result<(), RecipeError> {
        let manifest = self.manifest()?;
        let expected_schema = DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .map_err(|_| RecipeError::Invalid)?;
        self.query_limits()?;
        if manifest.schema() != &expected_schema
            || !valid_table_name(&self.table_name)
            || self.query_limits.max_bytes() > MAX_INLINE_QUERY_BYTES
            || self.instruments.is_empty()
            || !strictly_ordered(&self.instruments)
            || self.sources.is_empty()
            || !strictly_ordered(&self.sources)
            || self.starts_at_unix_nanos >= self.ends_at_unix_nanos
        {
            return Err(RecipeError::Invalid);
        }
        QueryRequest::try_new(manifest, self.sql.clone()).map_err(|_| RecipeError::Invalid)?;
        self.definition_limit()?;
        self.execution_assumptions()?;
        self.portfolio()?;
        self.corporate_actions()?;
        self.limits()?;
        self.experiment()?;
        self.cohort()?;
        Ok(())
    }

    fn into_registration_input(
        self,
    ) -> Result<GovernedBacktestInputRegistrationInput, RecipeError> {
        self.validate()?;
        let manifest = self.manifest.to_manifest()?;
        let query_limits = self.query_limits.into_input()?;
        let execution_assumptions = self.execution_assumptions.into_input()?;
        let portfolio = self.portfolio.into_input()?;
        let corporate_actions = self
            .corporate_actions
            .map(CorporateActionsWire::into_input)
            .transpose()?;
        let limits = self.limits.into_input()?;
        let experiment = self.experiment.build()?;
        let cohort = self
            .cohort
            .map(CohortWire::into_input)
            .transpose()?
            .flatten();
        Ok(GovernedBacktestInputRegistrationInput {
            strategy_id: self.strategy_id,
            manifest,
            table_name: self.table_name,
            sql: self.sql,
            query_limits,
            instruments: self.instruments,
            starts_at: Timestamp::from_unix_nanos(self.starts_at_unix_nanos),
            ends_at: Timestamp::from_unix_nanos(self.ends_at_unix_nanos),
            definition_history_limit: self.definition_history_limit,
            execution_assumptions,
            portfolio,
            corporate_actions,
            sources: self.sources,
            seed: self.seed,
            limits,
            experiment,
            cohort,
        })
    }

    pub(super) fn manifest(&self) -> Result<DatasetManifestRef, RecipeError> {
        self.manifest.to_manifest()
    }

    pub(super) fn query_request(&self) -> Result<QueryRequest, RecipeError> {
        QueryRequest::try_new(self.manifest()?, self.sql.clone()).map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn query_limits(&self) -> Result<QueryLimits, RecipeError> {
        self.query_limits.build()
    }

    pub(super) fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(super) fn instruments(&self) -> &[InstrumentId] {
        &self.instruments
    }

    pub(super) fn definition_limit(&self) -> Result<CatalogLimit, RecipeError> {
        CatalogLimit::new(self.definition_history_limit).map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn definition_as_of(&self) -> Timestamp {
        Timestamp::from_unix_nanos(self.ends_at_unix_nanos)
    }

    pub(super) fn execution_assumptions(
        &self,
    ) -> Result<ResearchExecutionAssumptions, RecipeError> {
        self.execution_assumptions.build()
    }

    pub(super) fn portfolio(&self) -> Result<PortfolioSeed, RecipeError> {
        self.portfolio.build()
    }

    pub(super) fn corporate_actions(
        &self,
    ) -> Result<Option<market_squawk_data::CorporateActionPlan>, RecipeError> {
        self.corporate_actions
            .as_ref()
            .map(CorporateActionsWire::build)
            .transpose()
    }

    pub(super) fn corporate_action_manifests(
        &self,
    ) -> Result<Vec<DatasetManifestRef>, RecipeError> {
        self.corporate_actions
            .as_ref()
            .map(CorporateActionsWire::source_manifests)
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub(super) fn sources(&self) -> Result<Vec<SourceIdentifier>, RecipeError> {
        self.sources
            .iter()
            .map(|source| {
                SourceIdentifier::try_from(source.as_str()).map_err(|_| RecipeError::Invalid)
            })
            .collect()
    }

    pub(super) fn scope(&self) -> Result<BacktestScope, RecipeError> {
        let Some(cohort) = self.cohort()? else {
            return BacktestScope::try_new(
                self.instruments.clone(),
                Some((
                    Timestamp::from_unix_nanos(self.starts_at_unix_nanos),
                    Timestamp::from_unix_nanos(self.ends_at_unix_nanos),
                )),
                self.sources.clone(),
            )
            .map_err(|_| RecipeError::Invalid);
        };
        let member_cores = cohort.member_cores(self)?;
        let mut instruments = Vec::new();
        let mut time_ranges = Vec::new();
        for (_, member) in member_cores {
            instruments.extend(member.instruments);
            time_ranges.push((
                Timestamp::from_unix_nanos(member.starts_at_unix_nanos),
                Timestamp::from_unix_nanos(member.ends_at_unix_nanos),
            ));
        }
        instruments.sort_unstable();
        instruments.dedup();
        time_ranges.sort_unstable();
        let mut union: Vec<(Timestamp, Timestamp)> = Vec::new();
        for (starts_at, ends_at) in time_ranges {
            match union.last_mut() {
                Some((_, prior_ends_at)) if starts_at <= *prior_ends_at => {
                    *prior_ends_at = (*prior_ends_at).max(ends_at);
                }
                _ => union.push((starts_at, ends_at)),
            }
        }
        BacktestScope::try_new_with_time_ranges(instruments, union, self.sources.clone())
            .map_err(|_| RecipeError::Invalid)
    }

    pub(super) const fn seed(&self) -> u64 {
        self.seed
    }

    pub(super) fn limits(&self) -> Result<BacktestLimits, RecipeError> {
        self.limits.build()
    }

    pub(super) fn experiment(&self) -> Result<BacktestExperimentPlan, RecipeError> {
        self.experiment.build()
    }

    pub(super) fn cohort(&self) -> Result<Option<&CohortWire>, RecipeError> {
        self.cohort
            .as_ref()
            .map(|cohort| {
                cohort.validate(&self.experiment, &self.sources)?;
                Ok(cohort)
            })
            .transpose()
    }

    pub(super) fn command(
        &self,
        input_id: SourceIdentifier,
    ) -> Result<GovernedBacktestCommand, RecipeError> {
        Ok(GovernedBacktestCommand::new(
            self.strategy_id.clone(),
            input_id,
            self.scope()?,
        ))
    }
}

const COHORT_RECIPE_VERSION: u16 = 1;
const MAX_COHORT_MEMBERS: usize = 131_072;
const MAX_COHORT_FOLDS: usize = 1_024;

/// Persisted cohort recipe. Its member records deliberately contain no caller-authored scores;
/// the backtesting inventory derives trial identities and diagnostics after fresh materialization.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CohortWire {
    version: u16,
    generator_version: SourceIdentifier,
    generator_parameters: Vec<policy::ParameterWire>,
    members: Vec<CohortMemberWire>,
    folds: Vec<Vec<CohortCandidateWire>>,
    selection_member_ids: Vec<SourceIdentifier>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CohortMemberWire {
    member_id: SourceIdentifier,
    manifest: ManifestWire,
    table_name: String,
    sql: String,
    query_limits: QueryLimitsWire,
    instruments: Vec<InstrumentId>,
    starts_at_unix_nanos: i64,
    ends_at_unix_nanos: i64,
    definition_history_limit: usize,
    experiment: ExperimentWire,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CohortCandidateWire {
    in_sample_member_id: SourceIdentifier,
    out_of_sample_member_id: SourceIdentifier,
}

impl CohortWire {
    fn try_from_input(input: GovernedBacktestCohortRegistrationInput) -> Result<Self, RecipeError> {
        let mut generator_parameters = input
            .generator_parameters
            .into_iter()
            .map(policy::ParameterWire::from)
            .collect::<Vec<_>>();
        generator_parameters.sort_unstable();
        let mut members = input
            .members
            .into_iter()
            .map(CohortMemberWire::try_from_input)
            .collect::<Result<Vec<_>, _>>()?;
        members.sort_unstable_by(|left, right| left.member_id.cmp(&right.member_id));
        let wire = Self {
            version: COHORT_RECIPE_VERSION,
            generator_version: input.generator_version,
            generator_parameters,
            members,
            folds: input
                .folds
                .into_iter()
                .map(|fold| {
                    fold.into_iter()
                        .map(|candidate| CohortCandidateWire {
                            in_sample_member_id: candidate.in_sample_member_id,
                            out_of_sample_member_id: candidate.out_of_sample_member_id,
                        })
                        .collect()
                })
                .collect(),
            selection_member_ids: input.selection_member_ids,
        };
        wire.validate_without_parent()?;
        Ok(wire)
    }

    fn validate(
        &self,
        parent_experiment: &ExperimentWire,
        sources: &[SourceId],
    ) -> Result<(), RecipeError> {
        self.validate_without_parent()?;
        for member in &self.members {
            member.validate(parent_experiment, sources)?;
        }
        Ok(())
    }

    fn validate_without_parent(&self) -> Result<(), RecipeError> {
        if self.version != COHORT_RECIPE_VERSION
            || self.generator_parameters.is_empty()
            || self
                .generator_parameters
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || !(2..=MAX_COHORT_MEMBERS).contains(&self.members.len())
            || !(2..=MAX_COHORT_FOLDS).contains(&self.folds.len())
            || self
                .members
                .windows(2)
                .any(|pair| pair[0].member_id >= pair[1].member_id)
            || self.selection_member_ids.len() < 2
            || self
                .selection_member_ids
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(RecipeError::Invalid);
        }
        let members = self
            .members
            .iter()
            .map(|member| &member.member_id)
            .collect::<Vec<_>>();
        if self
            .selection_member_ids
            .iter()
            .any(|member_id| !members.contains(&member_id))
            || self.folds.iter().any(|fold| {
                fold.len() != self.selection_member_ids.len()
                    || fold.iter().any(|candidate| {
                        !members.contains(&&candidate.in_sample_member_id)
                            || !members.contains(&&candidate.out_of_sample_member_id)
                    })
            })
        {
            return Err(RecipeError::Invalid);
        }
        Ok(())
    }

    fn into_input(self) -> Result<Option<GovernedBacktestCohortRegistrationInput>, RecipeError> {
        self.validate_without_parent()?;
        Ok(Some(GovernedBacktestCohortRegistrationInput {
            generator_version: self.generator_version,
            generator_parameters: self
                .generator_parameters
                .into_iter()
                .map(policy::ParameterWire::into_trial_parameter)
                .collect(),
            members: self
                .members
                .into_iter()
                .map(CohortMemberWire::into_input)
                .collect::<Result<Vec<_>, _>>()?,
            folds: self
                .folds
                .into_iter()
                .map(|fold| {
                    fold.into_iter()
                        .map(
                            |candidate| GovernedBacktestCohortCandidateRegistrationInput {
                                in_sample_member_id: candidate.in_sample_member_id,
                                out_of_sample_member_id: candidate.out_of_sample_member_id,
                            },
                        )
                        .collect()
                })
                .collect(),
            selection_member_ids: self.selection_member_ids,
        }))
    }

    pub(super) fn member_cores(
        &self,
        parent: &InputCoreWire,
    ) -> Result<Vec<(SourceIdentifier, InputCoreWire)>, RecipeError> {
        self.validate(&parent.experiment, &parent.sources)?;
        self.members
            .iter()
            .map(|member| {
                Ok((
                    member.member_id.clone(),
                    member.materialization_core(parent)?,
                ))
            })
            .collect()
    }

    pub(super) fn generator_version(&self) -> &SourceIdentifier {
        &self.generator_version
    }

    pub(super) fn generator_parameters(&self) -> Vec<market_squawk_backtesting::TrialParameter> {
        self.generator_parameters
            .iter()
            .cloned()
            .map(policy::ParameterWire::into_trial_parameter)
            .collect()
    }

    pub(super) fn folds(&self) -> Vec<Vec<(SourceIdentifier, SourceIdentifier)>> {
        self.folds
            .iter()
            .map(|fold| {
                fold.iter()
                    .map(|candidate| {
                        (
                            candidate.in_sample_member_id.clone(),
                            candidate.out_of_sample_member_id.clone(),
                        )
                    })
                    .collect()
            })
            .collect()
    }

    pub(super) fn selection_member_ids(&self) -> Vec<SourceIdentifier> {
        self.selection_member_ids.clone()
    }
}

impl CohortMemberWire {
    fn try_from_input(
        input: GovernedBacktestCohortMemberRegistrationInput,
    ) -> Result<Self, RecipeError> {
        let mut instruments = input.instruments;
        instruments.sort_unstable();
        Ok(Self {
            member_id: input.member_id,
            manifest: ManifestWire::from_manifest(&input.manifest),
            table_name: input.table_name,
            sql: input.sql,
            query_limits: QueryLimitsWire::try_from_input(input.query_limits)?,
            instruments,
            starts_at_unix_nanos: input.starts_at.unix_nanos(),
            ends_at_unix_nanos: input.ends_at.unix_nanos(),
            definition_history_limit: input.definition_history_limit,
            experiment: ExperimentWire::try_from_plan(input.experiment)?,
        })
    }

    fn validate(
        &self,
        parent_experiment: &ExperimentWire,
        _sources: &[SourceId],
    ) -> Result<(), RecipeError> {
        let manifest = self.manifest.to_manifest()?;
        if !valid_table_name(&self.table_name)
            || self.query_limits.max_bytes() > MAX_INLINE_QUERY_BYTES
            || self.instruments.is_empty()
            || !strictly_ordered(&self.instruments)
            || self.starts_at_unix_nanos >= self.ends_at_unix_nanos
            || !self.experiment.same_design(parent_experiment)
        {
            return Err(RecipeError::Invalid);
        }
        QueryRequest::try_new(manifest, self.sql.clone()).map_err(|_| RecipeError::Invalid)?;
        CatalogLimit::new(self.definition_history_limit).map_err(|_| RecipeError::Invalid)?;
        self.query_limits.build()?;
        self.experiment.build()?;
        Ok(())
    }

    fn materialization_core(&self, parent: &InputCoreWire) -> Result<InputCoreWire, RecipeError> {
        self.validate(&parent.experiment, &parent.sources)?;
        let mut core = parent.clone();
        core.manifest = self.manifest.clone();
        core.table_name = self.table_name.clone();
        core.sql = self.sql.clone();
        core.query_limits = self.query_limits;
        core.instruments = self.instruments.clone();
        core.starts_at_unix_nanos = self.starts_at_unix_nanos;
        core.ends_at_unix_nanos = self.ends_at_unix_nanos;
        core.definition_history_limit = self.definition_history_limit;
        core.experiment = self.experiment.clone();
        core.cohort = None;
        Ok(core)
    }

    fn into_input(self) -> Result<GovernedBacktestCohortMemberRegistrationInput, RecipeError> {
        Ok(GovernedBacktestCohortMemberRegistrationInput {
            member_id: self.member_id,
            manifest: self.manifest.to_manifest()?,
            table_name: self.table_name,
            sql: self.sql,
            query_limits: self.query_limits.into_input()?,
            instruments: self.instruments,
            starts_at: Timestamp::from_unix_nanos(self.starts_at_unix_nanos),
            ends_at: Timestamp::from_unix_nanos(self.ends_at_unix_nanos),
            definition_history_limit: self.definition_history_limit,
            experiment: self.experiment.build()?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExpectedEvidence {
    pub(super) query_identity: [u8; 32],
    pub(super) object_graph_digest: [u8; 32],
    pub(super) result_digest: [u8; 32],
    pub(super) definition_content_identity: [u8; 32],
    pub(super) definition_audit_identity: [u8; 32],
    pub(super) manifests: Vec<ManifestAuthorityWire>,
    pub(super) cohort_members: Vec<CohortMemberEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CohortMemberEvidence {
    pub(super) member_id: SourceIdentifier,
    pub(super) evidence: Box<ExpectedEvidence>,
}

impl ExpectedEvidence {
    pub(super) fn from_input(
        input: &PinnedBacktestInput,
        manifests: Vec<ManifestAuthorityWire>,
    ) -> Self {
        Self {
            query_identity: input.query.query_identity().bytes(),
            object_graph_digest: input.query.object_graph_digest().bytes(),
            result_digest: input.query.result_digest().bytes(),
            definition_content_identity: input.instrument_definitions.content_identity().bytes(),
            definition_audit_identity: input.instrument_definitions.audit_identity().bytes(),
            manifests,
            cohort_members: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExpectedEvidenceWire {
    query_identity: String,
    object_graph_digest: String,
    result_digest: String,
    definition_content_identity: String,
    definition_audit_identity: String,
    manifests: Vec<ManifestAuthorityWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    cohort_members: Vec<CohortMemberEvidenceWire>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CohortMemberEvidenceWire {
    member_id: SourceIdentifier,
    evidence: Box<ExpectedEvidenceWire>,
}

impl ExpectedEvidenceWire {
    fn from_expected(expected: ExpectedEvidence) -> Self {
        Self {
            query_identity: encode_digest(expected.query_identity),
            object_graph_digest: encode_digest(expected.object_graph_digest),
            result_digest: encode_digest(expected.result_digest),
            definition_content_identity: encode_digest(expected.definition_content_identity),
            definition_audit_identity: encode_digest(expected.definition_audit_identity),
            manifests: expected.manifests,
            cohort_members: expected
                .cohort_members
                .into_iter()
                .map(|member| CohortMemberEvidenceWire {
                    member_id: member.member_id,
                    evidence: Box::new(Self::from_expected(*member.evidence)),
                })
                .collect(),
        }
    }

    fn to_expected(&self) -> Result<ExpectedEvidence, RecipeError> {
        Ok(ExpectedEvidence {
            query_identity: decode_digest(&self.query_identity)?,
            object_graph_digest: decode_digest(&self.object_graph_digest)?,
            result_digest: decode_digest(&self.result_digest)?,
            definition_content_identity: decode_digest(&self.definition_content_identity)?,
            definition_audit_identity: decode_digest(&self.definition_audit_identity)?,
            manifests: self.manifests.clone(),
            cohort_members: self
                .cohort_members
                .iter()
                .map(|member| {
                    if !member.evidence.cohort_members.is_empty() {
                        return Err(RecipeError::Invalid);
                    }
                    let evidence = member.evidence.to_expected()?;
                    Ok(CohortMemberEvidence {
                        member_id: member.member_id.clone(),
                        evidence: Box::new(evidence),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    fn validate(&self, core: &InputCoreWire) -> Result<(), RecipeError> {
        let expected = self.to_expected()?;
        validate_manifest_authorities(&expected.manifests)?;
        let primary = core.manifest()?;
        if !expected
            .manifests
            .iter()
            .any(|authority| authority.manifest.to_manifest().as_ref() == Ok(&primary))
        {
            return Err(RecipeError::Invalid);
        }
        let mut source_ids = expected
            .manifests
            .iter()
            .map(|authority| authority.source_id.clone())
            .collect::<Vec<_>>();
        source_ids.sort_unstable();
        source_ids.dedup();
        if source_ids != core.sources {
            return Err(RecipeError::Invalid);
        }
        match core.cohort()? {
            None if !expected.cohort_members.is_empty() => return Err(RecipeError::Invalid),
            Some(cohort) => {
                if expected.cohort_members.len() != cohort.members.len()
                    || expected
                        .cohort_members
                        .windows(2)
                        .any(|pair| pair[0].member_id >= pair[1].member_id)
                    || expected
                        .cohort_members
                        .iter()
                        .zip(&cohort.members)
                        .any(|(evidence, member)| evidence.member_id != member.member_id)
                {
                    return Err(RecipeError::Invalid);
                }
                for (member, (_, member_core)) in expected
                    .cohort_members
                    .iter()
                    .zip(cohort.member_cores(core)?)
                {
                    ExpectedEvidenceWire::from_expected((*member.evidence).clone())
                        .validate(&member_core)?;
                }
            }
            None => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum RecipeError {
    #[error("governed-backtest input recipe is invalid or noncanonical")]
    Invalid,
    #[error("governed-backtest input recipe exceeded its resource contract")]
    ResourceExhausted,
}

pub(super) fn encode_digest(bytes: [u8; 32]) -> String {
    crate::application::domain_support::encode_hex(bytes)
}

pub(super) fn decode_digest(value: &str) -> Result<[u8; 32], RecipeError> {
    if value.len() != 64 {
        return Err(RecipeError::Invalid);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(RecipeError::Invalid)?;
        let low = hex_nibble(pair[1]).ok_or(RecipeError::Invalid)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn strictly_ordered<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn valid_table_name(value: &str) -> bool {
    value.len() <= 64
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}
