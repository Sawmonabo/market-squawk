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
const MAX_INLINE_QUERY_BYTES: u64 = 256 * 1024;

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
            .finish_non_exhaustive()
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
        Ok(())
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

    pub(super) fn scope(&self) -> BacktestScope {
        BacktestScope::new(
            self.instruments.clone(),
            Some((
                Timestamp::from_unix_nanos(self.starts_at_unix_nanos),
                Timestamp::from_unix_nanos(self.ends_at_unix_nanos),
            )),
            self.sources.clone(),
        )
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

    pub(super) fn command(&self, input_id: SourceIdentifier) -> GovernedBacktestCommand {
        GovernedBacktestCommand::new(self.strategy_id.clone(), input_id, self.scope())
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
