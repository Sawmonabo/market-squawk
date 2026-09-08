//! Authority-derived guided backtest preparation and one-use admission receipts.

use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_backtesting::{
    BacktestLimitsInput, ResearchExecutionAssumptionsInput, ResearchLiquidityPriority,
    TrialParameter, TrialSearchDimension,
};
use market_squawk_data::{DatasetManifestRef, DatasetSchemaRegistry};
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, InstrumentId, Money, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_portfolio::PortfolioLimitInput;
use market_squawk_services::{RequestOrigin, ServiceError};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use crate::{BacktestExperimentPlan, application::lifecycle::WorkspaceRuntimeIdentity};

use super::{
    GovernedBacktestCohortCandidateRegistrationInput,
    GovernedBacktestCohortMemberRegistrationInput, GovernedBacktestCohortRegistrationInput,
    GovernedBacktestInputRegistrationInput, GovernedBacktestPortfolioSeedInput,
    GovernedBacktestQueryLimitsInput,
    recipe::{RecipeError, RegistrationRecipe},
};

const DEFAULT_TABLE_NAME: &str = "feature_labels";
const MAXIMUM_DATASETS: usize = 4_096;
const MAXIMUM_INSTRUMENTS_PER_DATASET: usize = 4_096;
const MAXIMUM_SOURCES_PER_DATASET: usize = 256;
const MAXIMUM_RECEIPTS: usize = 1_024;
const MAXIMUM_RECEIPT_LIFETIME: Duration = Duration::from_secs(30 * 60);
const DEFINITION_HISTORY_LIMIT: usize = 4_096;

/// One immutable point-in-time dataset admitted by the application-owned preparation catalog.
#[derive(Clone, Debug)]
pub struct BacktestPreparationDatasetInput {
    option_id: SourceIdentifier,
    display_name: String,
    manifest: DatasetManifestRef,
    table_name: String,
    instruments: Vec<InstrumentId>,
    starts_at: Timestamp,
    ends_at: Timestamp,
    sources: Vec<SourceId>,
}

impl BacktestPreparationDatasetInput {
    /// Captures one exact authoritative dataset while keeping the physical relation code-owned.
    #[must_use]
    pub fn new(
        option_id: SourceIdentifier,
        display_name: impl Into<String>,
        manifest: DatasetManifestRef,
        instruments: Vec<InstrumentId>,
        starts_at: Timestamp,
        ends_at: Timestamp,
        sources: Vec<SourceId>,
    ) -> Self {
        Self {
            option_id,
            display_name: display_name.into(),
            manifest,
            table_name: DEFAULT_TABLE_NAME.to_owned(),
            instruments,
            starts_at,
            ends_at,
            sources,
        }
    }
}

/// Exact bounded snapshot from which every guided choice and registration is derived.
#[derive(Clone, Debug)]
pub struct BacktestPreparationCatalog {
    datasets: Box<[BacktestPreparationDatasetInput]>,
    digest: [u8; 32],
}

impl BacktestPreparationCatalog {
    /// Canonicalizes a current application-owned feature-dataset snapshot.
    pub fn try_new(
        mut datasets: Vec<BacktestPreparationDatasetInput>,
    ) -> Result<Self, BacktestPreparationError> {
        if datasets.len() > MAXIMUM_DATASETS {
            return Err(BacktestPreparationError::InvalidCatalog);
        }
        let expected_schema = DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .map_err(|_| BacktestPreparationError::InvalidCatalog)?;
        for dataset in &mut datasets {
            dataset.instruments.sort_unstable();
            dataset.sources.sort_unstable();
            if !valid_selection_token(dataset.option_id.as_str())
                || dataset.display_name.trim().is_empty()
                || dataset.display_name.len() > 160
                || dataset.manifest.schema() != &expected_schema
                || dataset.table_name != DEFAULT_TABLE_NAME
                || !valid_table_name(&dataset.table_name)
                || dataset.instruments.is_empty()
                || dataset.instruments.len() > MAXIMUM_INSTRUMENTS_PER_DATASET
                || dataset
                    .instruments
                    .windows(2)
                    .any(|pair| pair[0] == pair[1])
                || dataset.sources.is_empty()
                || dataset.sources.len() > MAXIMUM_SOURCES_PER_DATASET
                || dataset.sources.windows(2).any(|pair| pair[0] == pair[1])
                || dataset.starts_at >= dataset.ends_at
            {
                return Err(BacktestPreparationError::InvalidCatalog);
            }
        }
        datasets.sort_unstable_by(|left, right| left.option_id.cmp(&right.option_id));
        if datasets
            .windows(2)
            .any(|pair| pair[0].option_id == pair[1].option_id)
        {
            return Err(BacktestPreparationError::InvalidCatalog);
        }
        let digest = catalog_digest(&datasets)?;
        Ok(Self {
            datasets: datasets.into_boxed_slice(),
            digest,
        })
    }

    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    fn dataset(&self, option_id: &str) -> Option<&BacktestPreparationDatasetInput> {
        self.datasets
            .binary_search_by(|candidate| candidate.option_id.as_str().cmp(option_id))
            .ok()
            .and_then(|index| self.datasets.get(index))
    }
}

/// Fixed process bounds for expiring guided-preparation authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacktestPreparationLimits {
    maximum_receipts: usize,
    receipt_lifetime: Duration,
}

impl BacktestPreparationLimits {
    pub fn try_new(
        maximum_receipts: usize,
        receipt_lifetime: Duration,
    ) -> Result<Self, BacktestPreparationError> {
        if maximum_receipts == 0
            || maximum_receipts > MAXIMUM_RECEIPTS
            || receipt_lifetime.is_zero()
            || receipt_lifetime > MAXIMUM_RECEIPT_LIFETIME
        {
            return Err(BacktestPreparationError::InvalidLimits);
        }
        Ok(Self {
            maximum_receipts,
            receipt_lifetime,
        })
    }

    #[must_use]
    pub const fn standard() -> Self {
        Self {
            maximum_receipts: 256,
            receipt_lifetime: Duration::from_secs(15 * 60),
        }
    }
}

/// Closed caller selection. Every identifier resolves only inside the current catalog/policy set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BacktestPreparationSelection {
    pub dataset: String,
    pub period: String,
    pub strategy: String,
    pub cost_policy: String,
    pub seed: String,
    pub portfolio: String,
    pub comparison: String,
}

/// Bounded plain-language options returned to the installed product.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestPreparationOptions {
    pub datasets: Vec<BacktestDatasetOption>,
    pub strategies: Vec<BacktestNamedOption>,
    pub cost_policies: Vec<BacktestNamedOption>,
    pub seeds: Vec<BacktestNamedOption>,
    pub portfolios: Vec<BacktestNamedOption>,
    pub comparisons: Vec<BacktestNamedOption>,
    pub default_limit_policy: String,
}

impl BacktestPreparationOptions {
    /// Resolves the exact execution assumptions behind one admitted cost choice.
    pub fn execution_assumptions(
        &self,
        cost_policy: &str,
    ) -> Result<ResearchExecutionAssumptionsInput, BacktestPreparationError> {
        if !self
            .cost_policies
            .iter()
            .any(|option| option.id == cost_policy)
        {
            return Err(BacktestPreparationError::InvalidSelection);
        }
        execution_assumptions(cost_policy)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestDatasetOption {
    pub id: String,
    pub label: String,
    pub immutable_generation: String,
    pub instrument_count: usize,
    pub periods: Vec<BacktestPeriodOption>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestPeriodOption {
    pub id: String,
    pub label: String,
    pub starts_at: String,
    pub ends_at: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct BacktestNamedOption {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
}

/// Human-readable exact review returned before an explicit one-use start.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BacktestPreparationPreview {
    pub receipt: BacktestPreparationReceipt,
    pub expires_at: String,
    pub dataset: String,
    pub period: String,
    pub strategy: String,
    pub cost_policy: String,
    pub deterministic_seed: String,
    pub portfolio: String,
    pub comparison: String,
    pub evidence: Vec<String>,
    pub assumptions: Vec<String>,
}

/// Opaque selector and exact preparation digest accepted by the one-use registry.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct BacktestPreparationReceipt {
    receipt_id: Uuid,
    preparation_digest: [u8; 32],
}

impl BacktestPreparationReceipt {
    pub fn try_new(
        receipt_id: Uuid,
        preparation_digest: &str,
    ) -> Result<Self, BacktestPreparationError> {
        if receipt_id.is_nil() {
            return Err(BacktestPreparationError::InvalidReceipt);
        }
        Ok(Self {
            receipt_id,
            preparation_digest: decode_hex(preparation_digest)
                .ok_or(BacktestPreparationError::InvalidReceipt)?,
        })
    }

    #[must_use]
    pub const fn receipt_id(self) -> Uuid {
        self.receipt_id
    }

    #[must_use]
    pub fn preparation_digest(self) -> String {
        encode_hex(self.preparation_digest)
    }

    #[cfg(test)]
    const fn fixture(receipt_id: Uuid, preparation_digest: [u8; 32]) -> Self {
        Self {
            receipt_id,
            preparation_digest,
        }
    }
}

impl Serialize for BacktestPreparationReceipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        BacktestPreparationReceiptWire {
            receipt_id: self.receipt_id,
            preparation_digest: encode_hex(self.preparation_digest),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BacktestPreparationReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = BacktestPreparationReceiptWire::deserialize(deserializer)?;
        Self::try_new(wire.receipt_id, &wire.preparation_digest).map_err(serde::de::Error::custom)
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BacktestPreparationReceiptWire {
    receipt_id: Uuid,
    preparation_digest: String,
}

#[derive(Debug)]
struct StoredBacktestPreparation {
    owner: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
    catalog_digest: [u8; 32],
    registration_digest: [u8; 32],
    selection: BacktestPreparationSelection,
    expires_at: Instant,
}

impl StoredBacktestPreparation {
    #[cfg(test)]
    fn fixture(
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        catalog_digest: [u8; 32],
        expires_at: Instant,
    ) -> Self {
        Self {
            owner,
            workspace,
            catalog_digest,
            registration_digest: [5; 32],
            selection: BacktestPreparationSelection {
                dataset: "fixture".to_owned(),
                period: "full-history".to_owned(),
                strategy: "baseline-buy-once".to_owned(),
                cost_policy: "standard".to_owned(),
                seed: "fixed-7".to_owned(),
                portfolio: "research-usd-100k".to_owned(),
                comparison: "single-run".to_owned(),
            },
            expires_at,
        }
    }
}

#[derive(Debug)]
struct ReceiptRegistry {
    maximum_receipts: usize,
    entries: BTreeMap<Uuid, (BacktestPreparationReceipt, StoredBacktestPreparation)>,
}

impl ReceiptRegistry {
    fn new(maximum_receipts: usize) -> Self {
        Self {
            maximum_receipts,
            entries: BTreeMap::new(),
        }
    }

    fn insert(
        &mut self,
        receipt: BacktestPreparationReceipt,
        stored: StoredBacktestPreparation,
        now: Instant,
    ) -> Result<(), BacktestPreparationError> {
        self.entries.retain(|_, (_, entry)| {
            entry.expires_at > now
                && !(entry.owner == stored.owner && entry.workspace == stored.workspace)
        });
        if self.maximum_receipts == 0 || self.entries.len() >= self.maximum_receipts {
            return Err(BacktestPreparationError::Capacity);
        }
        if self.entries.contains_key(&receipt.receipt_id) {
            return Err(BacktestPreparationError::Conflict);
        }
        self.entries.insert(receipt.receipt_id, (receipt, stored));
        Ok(())
    }

    fn consume(
        &mut self,
        receipt: BacktestPreparationReceipt,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        catalog_digest: [u8; 32],
        now: Instant,
    ) -> Result<StoredBacktestPreparation, BacktestPreparationError> {
        let (_, retained) = self
            .entries
            .get(&receipt.receipt_id)
            .ok_or(BacktestPreparationError::NotFound)?;
        if retained.expires_at <= now {
            self.entries.remove(&receipt.receipt_id);
            return Err(BacktestPreparationError::Expired);
        }
        if receipt
            != self
                .entries
                .get(&receipt.receipt_id)
                .ok_or(BacktestPreparationError::NotFound)?
                .0
            || retained.owner != owner
            || retained.workspace != workspace
            || retained.catalog_digest != catalog_digest
        {
            return Err(BacktestPreparationError::Unauthorized);
        }
        self.entries
            .remove(&receipt.receipt_id)
            .map(|(_, retained)| retained)
            .ok_or(BacktestPreparationError::NotFound)
    }

    fn selection(
        &mut self,
        receipt: BacktestPreparationReceipt,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        catalog_digest: [u8; 32],
        now: Instant,
    ) -> Result<(BacktestPreparationSelection, [u8; 32]), BacktestPreparationError> {
        let (expected, retained) = self
            .entries
            .get(&receipt.receipt_id)
            .ok_or(BacktestPreparationError::NotFound)?;
        if retained.expires_at <= now {
            self.entries.remove(&receipt.receipt_id);
            return Err(BacktestPreparationError::Expired);
        }
        if expected != &receipt
            || retained.owner != owner
            || retained.workspace != workspace
            || retained.catalog_digest != catalog_digest
        {
            return Err(BacktestPreparationError::Unauthorized);
        }
        Ok((retained.selection.clone(), retained.registration_digest))
    }

    fn receipt(
        &mut self,
        receipt_id: Uuid,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        catalog_digest: [u8; 32],
        now: Instant,
    ) -> Result<BacktestPreparationReceipt, BacktestPreparationError> {
        let (receipt, retained) = self
            .entries
            .get(&receipt_id)
            .ok_or(BacktestPreparationError::NotFound)?;
        if retained.expires_at <= now {
            self.entries.remove(&receipt_id);
            return Err(BacktestPreparationError::Expired);
        }
        if retained.owner != owner
            || retained.workspace != workspace
            || retained.catalog_digest != catalog_digest
        {
            return Err(BacktestPreparationError::Unauthorized);
        }
        Ok(*receipt)
    }
}

/// Process-owned guided preparation authority. Restart invalidates every outstanding receipt.
#[derive(Debug)]
pub struct GovernedBacktestPreparationAuthority {
    limits: BacktestPreparationLimits,
    receipts: Mutex<ReceiptRegistry>,
}

impl GovernedBacktestPreparationAuthority {
    pub fn try_new(limits: BacktestPreparationLimits) -> Result<Self, BacktestPreparationError> {
        BacktestPreparationLimits::try_new(limits.maximum_receipts, limits.receipt_lifetime)?;
        Ok(Self {
            limits,
            receipts: Mutex::new(ReceiptRegistry::new(limits.maximum_receipts)),
        })
    }

    /// Projects only bounded, human-readable choices from the current authoritative snapshot.
    pub fn options(
        &self,
        catalog: &BacktestPreparationCatalog,
    ) -> Result<BacktestPreparationOptions, BacktestPreparationError> {
        let mut datasets = Vec::new();
        datasets
            .try_reserve_exact(catalog.datasets.len())
            .map_err(|_| BacktestPreparationError::Capacity)?;
        for dataset in &catalog.datasets {
            datasets.push(BacktestDatasetOption {
                id: dataset.option_id.as_str().to_owned(),
                label: dataset.display_name.clone(),
                immutable_generation: dataset.manifest.manifest_version().to_string(),
                instrument_count: dataset.instruments.len(),
                periods: period_options(dataset)?,
            });
        }
        Ok(BacktestPreparationOptions {
            datasets,
            strategies: strategy_options(),
            cost_policies: cost_options(),
            seeds: seed_options(),
            portfolios: portfolio_options(),
            comparisons: comparison_options(),
            default_limit_policy: "V1 bounded research defaults: at most 1,000,000 observations and fills; 256 KiB pinned query output; 512 MiB retained engine state.".to_owned(),
        })
    }

    /// Constructs the exact canonical registration and retains an expiring one-use receipt.
    pub fn preview(
        &self,
        catalog: &BacktestPreparationCatalog,
        selection: BacktestPreparationSelection,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
        observed_at: Timestamp,
    ) -> Result<BacktestPreparationPreview, BacktestPreparationError> {
        ensure_origin(owner, workspace)?;
        let recipe = build_registration(catalog, &selection)?;
        let registration_digest: [u8; 32] =
            Sha256::digest(recipe.canonical_core_bytes().map_err(map_recipe_error)?).into();
        let receipt_id = Uuid::new_v4();
        let receipt = BacktestPreparationReceipt {
            receipt_id,
            preparation_digest: receipt_digest(
                receipt_id,
                registration_digest,
                catalog.digest,
                owner,
                workspace,
            ),
        };
        let expires_at = now
            .checked_add(self.limits.receipt_lifetime)
            .ok_or(BacktestPreparationError::Capacity)?;
        let wall_nanos = i64::try_from(self.limits.receipt_lifetime.as_nanos())
            .map_err(|_| BacktestPreparationError::Capacity)?;
        let expires_at_wall = observed_at
            .checked_add_nanos(wall_nanos)
            .map_err(|_| BacktestPreparationError::Capacity)?;
        let dataset = catalog
            .dataset(&selection.dataset)
            .ok_or(BacktestPreparationError::InvalidSelection)?;
        let period = selected_period(dataset, &selection.period)?;
        let preview = BacktestPreparationPreview {
            receipt,
            expires_at: timestamp_text(expires_at_wall),
            dataset: format!(
                "{} — immutable generation {} across {} admitted instruments",
                dataset.display_name,
                dataset.manifest.manifest_version(),
                dataset.instruments.len()
            ),
            period: period.label,
            strategy: named_label(&strategy_options(), &selection.strategy)?,
            cost_policy: named_label(&cost_options(), &selection.cost_policy)?,
            deterministic_seed: named_label(&seed_options(), &selection.seed)?,
            portfolio: named_label(&portfolio_options(), &selection.portfolio)?,
            comparison: named_label(&comparison_options(), &selection.comparison)?,
            evidence: vec![
                "The dataset is an immutable point-in-time feature generation; historical universe membership and delisting state are read from its governed rows.".to_owned(),
                "Instrument definitions and the complete manifest graph will be pinned again at start and again by the existing job resolver.".to_owned(),
                "The query, strategy build, resource ceilings, cohort topology, and selection criterion are code-owned and cannot be supplied from this form.".to_owned(),
            ],
            assumptions: preview_assumptions(&selection)?,
        };
        self.receipts
            .lock()
            .map_err(|_| BacktestPreparationError::Unavailable)?
            .insert(
                receipt,
                StoredBacktestPreparation {
                    owner,
                    workspace,
                    catalog_digest: catalog.digest,
                    registration_digest,
                    selection,
                    expires_at,
                },
                now,
            )?;
        Ok(preview)
    }

    /// Consumes once, revalidates the current catalog fence, and returns only the exact input
    /// accepted by the existing registrar and job runner.
    pub fn consume(
        &self,
        catalog: &BacktestPreparationCatalog,
        receipt: BacktestPreparationReceipt,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
    ) -> Result<GovernedBacktestInputRegistrationInput, BacktestPreparationError> {
        ensure_origin(owner, workspace)?;
        let mut receipts = self
            .receipts
            .lock()
            .map_err(|_| BacktestPreparationError::Unavailable)?;
        let (selection, expected_registration_digest) =
            receipts.selection(receipt, owner, workspace, catalog.digest, now)?;
        let recipe = build_registration(catalog, &selection)?;
        let current_registration_digest: [u8; 32] =
            Sha256::digest(recipe.canonical_core_bytes().map_err(map_recipe_error)?).into();
        if current_registration_digest != expected_registration_digest
            || receipt.preparation_digest
                != receipt_digest(
                    receipt.receipt_id,
                    current_registration_digest,
                    catalog.digest,
                    owner,
                    workspace,
                )
        {
            return Err(BacktestPreparationError::Unauthorized);
        }
        receipts.consume(receipt, owner, workspace, catalog.digest, now)?;
        recipe.into_registration_input().map_err(map_recipe_error)
    }

    /// Resolves one opaque product confirmation token inside the process-owned receipt registry,
    /// then applies the same one-use catalog and registration revalidation as direct consumption.
    pub fn consume_token(
        &self,
        catalog: &BacktestPreparationCatalog,
        confirmation_token: Uuid,
        owner: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
    ) -> Result<GovernedBacktestInputRegistrationInput, BacktestPreparationError> {
        ensure_origin(owner, workspace)?;
        let receipt = {
            let mut receipts = self
                .receipts
                .lock()
                .map_err(|_| BacktestPreparationError::Unavailable)?;
            receipts.receipt(confirmation_token, owner, workspace, catalog.digest, now)?
        };
        self.consume(catalog, receipt, owner, workspace, now)
    }
}

impl From<BacktestPreparationError> for ServiceError {
    fn from(value: BacktestPreparationError) -> Self {
        match value {
            BacktestPreparationError::InvalidCatalog
            | BacktestPreparationError::InvalidSelection
            | BacktestPreparationError::InvalidLimits
            | BacktestPreparationError::InvalidReceipt => Self::InvalidRequest,
            BacktestPreparationError::NotFound | BacktestPreparationError::Expired => {
                Self::NotFound
            }
            BacktestPreparationError::Unauthorized => Self::Unauthorized,
            BacktestPreparationError::Conflict => Self::InvalidResult,
            BacktestPreparationError::Capacity => Self::ResourceExhausted,
            BacktestPreparationError::Unavailable => Self::Unavailable,
        }
    }
}

/// Closed preparation, receipt, and catalog-fence failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BacktestPreparationError {
    #[error("backtest preparation catalog is invalid")]
    InvalidCatalog,
    #[error("backtest preparation selection is invalid")]
    InvalidSelection,
    #[error("backtest preparation limits are invalid")]
    InvalidLimits,
    #[error("backtest preparation receipt is invalid")]
    InvalidReceipt,
    #[error("backtest preparation receipt was not found")]
    NotFound,
    #[error("backtest preparation receipt expired")]
    Expired,
    #[error("backtest preparation receipt is not authorized for this request")]
    Unauthorized,
    #[error("backtest preparation receipt conflicts with retained authority")]
    Conflict,
    #[error("backtest preparation authority exceeded its bounded capacity")]
    Capacity,
    #[error("backtest preparation authority is unavailable")]
    Unavailable,
}

#[derive(Clone)]
struct SelectedPeriod {
    id: &'static str,
    label: String,
    starts_at: Timestamp,
    ends_at: Timestamp,
}

fn build_registration(
    catalog: &BacktestPreparationCatalog,
    selection: &BacktestPreparationSelection,
) -> Result<RegistrationRecipe, BacktestPreparationError> {
    validate_named_selection(selection)?;
    let dataset = catalog
        .dataset(&selection.dataset)
        .ok_or(BacktestPreparationError::InvalidSelection)?;
    let period = selected_period(dataset, &selection.period)?;
    let experiment = experiment(&selection.comparison)?;
    let cohort = if selection.comparison == "walk-forward-robustness" {
        Some(walk_forward_cohort(dataset, &period)?)
    } else {
        None
    };
    let input = GovernedBacktestInputRegistrationInput {
        strategy_id: crate::backtest_strategy::baseline_backtest_build_id()
            .map_err(|_| BacktestPreparationError::InvalidSelection)?,
        manifest: dataset.manifest.clone(),
        table_name: dataset.table_name.clone(),
        sql: canonical_sql(&dataset.table_name, period.starts_at, period.ends_at),
        query_limits: query_limits(),
        instruments: dataset.instruments.clone(),
        starts_at: period.starts_at,
        ends_at: period.ends_at,
        definition_history_limit: DEFINITION_HISTORY_LIMIT,
        execution_assumptions: execution_assumptions(&selection.cost_policy)?,
        portfolio: portfolio_seed(&selection.portfolio)?,
        corporate_actions: None,
        sources: dataset.sources.clone(),
        seed: selected_seed(&selection.seed)?,
        limits: backtest_limits(),
        experiment,
        cohort,
    };
    RegistrationRecipe::try_new(input).map_err(map_recipe_error)
}

fn experiment(comparison: &str) -> Result<BacktestExperimentPlan, BacktestPreparationError> {
    let selection_criterion = SourceIdentifier::try_from("cost-adjusted-total-return")
        .map_err(|_| BacktestPreparationError::InvalidSelection)?;
    if comparison == "single-run" {
        return Ok(BacktestExperimentPlan {
            parameters: Vec::new(),
            search_space: Vec::new(),
            selection_criterion,
        });
    }
    if comparison != "walk-forward-robustness" {
        return Err(BacktestPreparationError::InvalidSelection);
    }
    let name = SourceIdentifier::try_from("robustness-variant")
        .map_err(|_| BacktestPreparationError::InvalidSelection)?;
    let baseline = SourceIdentifier::try_from("baseline")
        .map_err(|_| BacktestPreparationError::InvalidSelection)?;
    let stress = SourceIdentifier::try_from("stress")
        .map_err(|_| BacktestPreparationError::InvalidSelection)?;
    Ok(BacktestExperimentPlan {
        parameters: vec![TrialParameter::new(name.clone(), baseline.clone())],
        search_space: vec![
            TrialSearchDimension::try_new(name, vec![baseline, stress])
                .map_err(|_| BacktestPreparationError::InvalidSelection)?,
        ],
        selection_criterion,
    })
}

fn walk_forward_cohort(
    dataset: &BacktestPreparationDatasetInput,
    period: &SelectedPeriod,
) -> Result<GovernedBacktestCohortRegistrationInput, BacktestPreparationError> {
    let duration = period
        .ends_at
        .unix_nanos()
        .checked_sub(period.starts_at.unix_nanos())
        .ok_or(BacktestPreparationError::InvalidSelection)?;
    if duration < 8 {
        return Err(BacktestPreparationError::InvalidSelection);
    }
    let quarter = duration / 4;
    if quarter == 0 {
        return Err(BacktestPreparationError::InvalidSelection);
    }
    let boundaries = [
        period.starts_at.unix_nanos(),
        period.starts_at.unix_nanos() + quarter,
        period.starts_at.unix_nanos() + quarter * 2,
        period.starts_at.unix_nanos() + quarter * 3,
        period.ends_at.unix_nanos(),
    ];
    let variants = ["baseline", "stress"];
    let mut members = Vec::with_capacity(8);
    let mut folds = Vec::with_capacity(2);
    let parameter_name = SourceIdentifier::try_from("robustness-variant")
        .map_err(|_| BacktestPreparationError::InvalidSelection)?;
    let search_space = || {
        TrialSearchDimension::try_new(
            parameter_name.clone(),
            vec![
                SourceIdentifier::try_from("baseline")
                    .map_err(|_| BacktestPreparationError::InvalidSelection)?,
                SourceIdentifier::try_from("stress")
                    .map_err(|_| BacktestPreparationError::InvalidSelection)?,
            ],
        )
        .map_err(|_| BacktestPreparationError::InvalidSelection)
    };
    for (fold_index, (in_start, in_end, out_start, out_end)) in [
        (boundaries[0], boundaries[1], boundaries[1], boundaries[2]),
        (boundaries[2], boundaries[3], boundaries[3], boundaries[4]),
    ]
    .into_iter()
    .enumerate()
    {
        let mut candidates = Vec::with_capacity(2);
        for variant in variants {
            let in_id = SourceIdentifier::try_from(format!(
                "walk-forward-{}-{}-in",
                fold_index + 1,
                variant
            ))
            .map_err(|_| BacktestPreparationError::InvalidSelection)?;
            let out_id = SourceIdentifier::try_from(format!(
                "walk-forward-{}-{}-out",
                fold_index + 1,
                variant
            ))
            .map_err(|_| BacktestPreparationError::InvalidSelection)?;
            let parameter_value = SourceIdentifier::try_from(variant)
                .map_err(|_| BacktestPreparationError::InvalidSelection)?;
            for (member_id, starts_at, ends_at) in [
                (in_id.clone(), in_start, in_end),
                (out_id.clone(), out_start, out_end),
            ] {
                members.push(GovernedBacktestCohortMemberRegistrationInput {
                    member_id,
                    manifest: dataset.manifest.clone(),
                    table_name: dataset.table_name.clone(),
                    sql: canonical_sql(
                        &dataset.table_name,
                        Timestamp::from_unix_nanos(starts_at),
                        Timestamp::from_unix_nanos(ends_at),
                    ),
                    query_limits: query_limits(),
                    instruments: dataset.instruments.clone(),
                    starts_at: Timestamp::from_unix_nanos(starts_at),
                    ends_at: Timestamp::from_unix_nanos(ends_at),
                    definition_history_limit: DEFINITION_HISTORY_LIMIT,
                    experiment: BacktestExperimentPlan {
                        parameters: vec![TrialParameter::new(
                            parameter_name.clone(),
                            parameter_value.clone(),
                        )],
                        search_space: vec![search_space()?],
                        selection_criterion: SourceIdentifier::try_from(
                            "cost-adjusted-total-return",
                        )
                        .map_err(|_| BacktestPreparationError::InvalidSelection)?,
                    },
                });
            }
            candidates.push(GovernedBacktestCohortCandidateRegistrationInput {
                in_sample_member_id: in_id,
                out_of_sample_member_id: out_id,
            });
        }
        folds.push(candidates);
    }
    let mut selection_member_ids = variants
        .into_iter()
        .map(|variant| {
            SourceIdentifier::try_from(format!("walk-forward-2-{variant}-out"))
                .map_err(|_| BacktestPreparationError::InvalidSelection)
        })
        .collect::<Result<Vec<_>, _>>()?;
    selection_member_ids.sort_unstable();
    Ok(GovernedBacktestCohortRegistrationInput {
        generator_version: SourceIdentifier::try_from("market-squawk-walk-forward-v1")
            .map_err(|_| BacktestPreparationError::InvalidSelection)?,
        generator_parameters: vec![TrialParameter::new(
            SourceIdentifier::try_from("fold-policy")
                .map_err(|_| BacktestPreparationError::InvalidSelection)?,
            SourceIdentifier::try_from("two-fold-anchored")
                .map_err(|_| BacktestPreparationError::InvalidSelection)?,
        )],
        members,
        folds,
        selection_member_ids,
    })
}

fn query_limits() -> GovernedBacktestQueryLimitsInput {
    GovernedBacktestQueryLimitsInput {
        max_rows: 1_000_000,
        max_bytes: 256 * 1024,
        max_memory_bytes: 64 * 1024 * 1024,
        max_partitions: 64,
        max_ast_nodes: 512,
        max_plan_nodes: 2_048,
        deadline: Duration::from_secs(60),
    }
}

fn backtest_limits() -> BacktestLimitsInput {
    BacktestLimitsInput {
        max_observations: 1_000_000,
        max_pending_intents: 65_536,
        max_fills: 1_000_000,
        max_retained_bytes: 512 * 1024 * 1024,
    }
}

fn execution_assumptions(
    policy: &str,
) -> Result<ResearchExecutionAssumptionsInput, BacktestPreparationError> {
    let (fee, slippage, random, participation, latency) = match policy {
        "standard" => (5, 8, 3, 1_000, 1_000_000),
        "conservative" => (10, 15, 5, 500, 5_000_000),
        _ => return Err(BacktestPreparationError::InvalidSelection),
    };
    Ok(ResearchExecutionAssumptionsInput {
        version: 3,
        fee_basis_points: BasisPoints::new(fee),
        slippage_basis_points: BasisPoints::new(slippage),
        maximum_random_slippage_basis_points: BasisPoints::new(random),
        maximum_participation_basis_points: BasisPoints::new(participation),
        liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
        latency_nanos: latency,
        allow_partial_fills: true,
        fee_decimal_scale: 8,
    })
}

fn portfolio_seed(
    portfolio: &str,
) -> Result<GovernedBacktestPortfolioSeedInput, BacktestPreparationError> {
    let (account_uuid, cash) = match portfolio {
        "research-usd-100k" => (0x6cb85c46acb34f7494e1b8fba341aae1_u128, 100_000_u64),
        "research-usd-1m" => (0x37ce3733eb1747ccbd5d0f06ce4a7e98_u128, 1_000_000_u64),
        _ => return Err(BacktestPreparationError::InvalidSelection),
    };
    Ok(GovernedBacktestPortfolioSeedInput {
        account_id: AccountId::try_from(Uuid::from_u128(account_uuid))
            .map_err(|_| BacktestPreparationError::InvalidSelection)?,
        initial_cash: Money::new(
            Decimal::from(cash),
            Currency::try_from("USD").map_err(|_| BacktestPreparationError::InvalidSelection)?,
        ),
        limits: PortfolioLimitInput {
            max_accounts: 1,
            max_instruments: 100_000,
            max_lots: 1_000_000,
            max_transactions: 1_000_000,
            max_factors: 4_096,
            max_scenarios: 4_096,
            max_history: 4_096,
            max_results: 1_000_000,
            max_retained_bytes: 256 * 1024 * 1024,
        },
    })
}

fn selected_seed(seed: &str) -> Result<u64, BacktestPreparationError> {
    match seed {
        "fixed-7" => Ok(7),
        "fixed-42" => Ok(42),
        "release-baseline" => Ok(20_260_803),
        _ => Err(BacktestPreparationError::InvalidSelection),
    }
}

fn validate_named_selection(
    selection: &BacktestPreparationSelection,
) -> Result<(), BacktestPreparationError> {
    if [
        selection.dataset.as_str(),
        selection.period.as_str(),
        selection.strategy.as_str(),
        selection.cost_policy.as_str(),
        selection.seed.as_str(),
        selection.portfolio.as_str(),
        selection.comparison.as_str(),
    ]
    .into_iter()
    .any(|value| !valid_selection_token(value))
    {
        return Err(BacktestPreparationError::InvalidSelection);
    }
    named_label(&strategy_options(), &selection.strategy)?;
    named_label(&cost_options(), &selection.cost_policy)?;
    named_label(&seed_options(), &selection.seed)?;
    named_label(&portfolio_options(), &selection.portfolio)?;
    named_label(&comparison_options(), &selection.comparison)?;
    Ok(())
}

fn valid_selection_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn named_label(
    options: &[BacktestNamedOption],
    selected: &str,
) -> Result<String, BacktestPreparationError> {
    options
        .iter()
        .find(|option| option.id == selected)
        .map(|option| option.label.to_owned())
        .ok_or(BacktestPreparationError::InvalidSelection)
}

fn strategy_options() -> Vec<BacktestNamedOption> {
    vec![BacktestNamedOption {
        id: "baseline-buy-once",
        label: "Baseline buy once",
        description: "The installed, executable-bound V1 baseline strategy.",
    }]
}

fn cost_options() -> Vec<BacktestNamedOption> {
    vec![
        BacktestNamedOption {
            id: "standard",
            label: "Standard research costs",
            description: "5 bps fees, 8 bps deterministic slippage, up to 3 bps seeded slippage, and 10% participation.",
        },
        BacktestNamedOption {
            id: "conservative",
            label: "Conservative research costs",
            description: "10 bps fees, 15 bps deterministic slippage, up to 5 bps seeded slippage, and 5% participation.",
        },
    ]
}

fn seed_options() -> Vec<BacktestNamedOption> {
    vec![
        BacktestNamedOption {
            id: "fixed-7",
            label: "Seed 7",
            description: "Stable deterministic seed for reproducible research.",
        },
        BacktestNamedOption {
            id: "fixed-42",
            label: "Seed 42",
            description: "A second stable deterministic seed for comparison.",
        },
        BacktestNamedOption {
            id: "release-baseline",
            label: "Release baseline seed",
            description: "The fixed V1 release-baseline seed 20260803.",
        },
    ]
}

fn portfolio_options() -> Vec<BacktestNamedOption> {
    vec![
        BacktestNamedOption {
            id: "research-usd-100k",
            label: "$100,000 research portfolio",
            description: "A dedicated deterministic USD research account.",
        },
        BacktestNamedOption {
            id: "research-usd-1m",
            label: "$1,000,000 research portfolio",
            description: "A larger dedicated deterministic USD research account.",
        },
    ]
}

fn comparison_options() -> Vec<BacktestNamedOption> {
    vec![
        BacktestNamedOption {
            id: "single-run",
            label: "Single governed run",
            description: "Run the selected period once with no cohort diagnostic.",
        },
        BacktestNamedOption {
            id: "walk-forward-robustness",
            label: "Walk-forward robustness",
            description: "Run two fixed in-sample/out-of-sample folds with two predeclared variants for PBO and deflated-performance evidence.",
        },
    ]
}

fn preview_assumptions(
    selection: &BacktestPreparationSelection,
) -> Result<Vec<String>, BacktestPreparationError> {
    let assumptions = execution_assumptions(&selection.cost_policy)?;
    Ok(vec![
        format!(
            "Execution uses observed point-in-time half-spread, {} bps fees, {} bps deterministic slippage, and up to {} bps seeded random slippage.",
            assumptions.fee_basis_points.get(),
            assumptions.slippage_basis_points.get(),
            assumptions.maximum_random_slippage_basis_points.get(),
        ),
        format!(
            "Orders wait {} ns, may partially fill, and consume at most {} bps of evidenced executable depth.",
            assumptions.latency_nanos,
            assumptions.maximum_participation_basis_points.get(),
        ),
        "Corporate actions are not caller-authored: split, distribution, historical membership, and delisting behavior must already be represented by the immutable point-in-time dataset and registered instrument definitions.".to_owned(),
        "Historical results are research evidence, not a forecast or guarantee; paper outcomes should be compared before any execution decision.".to_owned(),
    ])
}

fn period_options(
    dataset: &BacktestPreparationDatasetInput,
) -> Result<Vec<BacktestPeriodOption>, BacktestPreparationError> {
    periods(dataset)
        .into_iter()
        .map(|period| {
            Ok(BacktestPeriodOption {
                id: period.id.to_owned(),
                label: period.label,
                starts_at: timestamp_text(period.starts_at),
                ends_at: timestamp_text(period.ends_at),
            })
        })
        .collect()
}

fn periods(dataset: &BacktestPreparationDatasetInput) -> Vec<SelectedPeriod> {
    let start = dataset.starts_at.unix_nanos();
    let end = dataset.ends_at.unix_nanos();
    let midpoint = i64::try_from((i128::from(start) + i128::from(end)) / 2).unwrap_or(start);
    let mut periods = vec![SelectedPeriod {
        id: "full-history",
        label: format!(
            "Full admitted history ({} to {})",
            calendar_date(dataset.starts_at),
            calendar_date(dataset.ends_at)
        ),
        starts_at: dataset.starts_at,
        ends_at: dataset.ends_at,
    }];
    if midpoint > start && midpoint < end {
        periods.push(SelectedPeriod {
            id: "recent-half",
            label: format!(
                "Recent half ({} to {})",
                calendar_date(Timestamp::from_unix_nanos(midpoint)),
                calendar_date(dataset.ends_at)
            ),
            starts_at: Timestamp::from_unix_nanos(midpoint),
            ends_at: dataset.ends_at,
        });
    }
    periods
}

fn selected_period(
    dataset: &BacktestPreparationDatasetInput,
    selected: &str,
) -> Result<SelectedPeriod, BacktestPreparationError> {
    periods(dataset)
        .into_iter()
        .find(|period| period.id == selected)
        .ok_or(BacktestPreparationError::InvalidSelection)
}

fn canonical_sql(table: &str, starts_at: Timestamp, ends_at: Timestamp) -> String {
    format!(
        "SELECT * FROM {table} WHERE cutoff_at >= TIMESTAMP '{}' AND cutoff_at < TIMESTAMP '{}' ORDER BY cutoff_at, instrument_id, example_id, component_kind, component_name",
        sql_timestamp(starts_at),
        sql_timestamp(ends_at),
    )
}

fn sql_timestamp(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .format("%Y-%m-%d %H:%M:%S%.9f")
        .to_string()
}

fn timestamp_text(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn calendar_date(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .format("%Y-%m-%d")
        .to_string()
}

fn valid_table_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
}

fn catalog_digest(
    datasets: &[BacktestPreparationDatasetInput],
) -> Result<[u8; 32], BacktestPreparationError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-preparation-catalog/v1");
    hash_count(&mut hash, datasets.len())?;
    for dataset in datasets {
        hash_text(&mut hash, dataset.option_id.as_str())?;
        hash_text(&mut hash, &dataset.display_name)?;
        hash_text(&mut hash, dataset.manifest.dataset_id().as_str())?;
        hash.update(dataset.manifest.manifest_version().to_be_bytes());
        hash.update(dataset.manifest.content_hash().bytes());
        hash.update(dataset.manifest.schema().fingerprint());
        hash_text(&mut hash, &dataset.table_name)?;
        hash.update(dataset.starts_at.unix_nanos().to_be_bytes());
        hash.update(dataset.ends_at.unix_nanos().to_be_bytes());
        hash_count(&mut hash, dataset.instruments.len())?;
        for instrument in &dataset.instruments {
            hash.update(instrument.as_uuid().as_bytes());
        }
        hash_count(&mut hash, dataset.sources.len())?;
        for source in &dataset.sources {
            hash_text(&mut hash, source.as_str())?;
        }
    }
    Ok(hash.finalize().into())
}

fn receipt_digest(
    receipt_id: Uuid,
    registration_digest: [u8; 32],
    catalog_digest: [u8; 32],
    owner: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-preparation-receipt/v1");
    hash.update(receipt_id.as_bytes());
    hash.update(registration_digest);
    hash.update(catalog_digest);
    hash.update(owner.workspace_id().as_bytes());
    hash.update(owner.client_id().as_bytes());
    hash.update(workspace.workspace_id().as_uuid().as_bytes());
    hash.update(workspace.generation().get().to_be_bytes());
    hash.finalize().into()
}

fn ensure_origin(
    owner: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
) -> Result<(), BacktestPreparationError> {
    if owner.workspace_id() == workspace.workspace_id().as_uuid() {
        Ok(())
    } else {
        Err(BacktestPreparationError::Unauthorized)
    }
}

fn hash_count(hash: &mut Sha256, value: usize) -> Result<(), BacktestPreparationError> {
    hash.update(
        u64::try_from(value)
            .map_err(|_| BacktestPreparationError::Capacity)?
            .to_be_bytes(),
    );
    Ok(())
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), BacktestPreparationError> {
    hash_count(hash, value.len())?;
    hash.update(value.as_bytes());
    Ok(())
}

fn encode_hex(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        decoded[index] = high << 4 | low;
    }
    Some(decoded)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn map_recipe_error(error: RecipeError) -> BacktestPreparationError {
    match error {
        RecipeError::Invalid => BacktestPreparationError::InvalidSelection,
        RecipeError::ResourceExhausted => BacktestPreparationError::Capacity,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use market_squawk_data::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};
    use market_squawk_domain::{InstrumentId, SourceId, SourceIdentifier, Timestamp};
    use market_squawk_runtime::WorkspaceId;
    use market_squawk_services::RequestOrigin;
    use uuid::Uuid;

    use super::{
        BacktestPreparationCatalog, BacktestPreparationDatasetInput, BacktestPreparationLimits,
        BacktestPreparationReceipt, BacktestPreparationSelection,
        GovernedBacktestPreparationAuthority, ReceiptRegistry, StoredBacktestPreparation,
    };
    use crate::application::lifecycle::WorkspaceRuntimeIdentity;

    #[test]
    fn an_exact_backtest_preparation_can_be_consumed_only_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())?;
        let workspace = WorkspaceRuntimeIdentity::try_new(workspace_id, 7)?;
        let origin = RequestOrigin::try_new(workspace_id.as_uuid(), Uuid::new_v4())?;
        let receipt = BacktestPreparationReceipt::fixture(Uuid::new_v4(), [9; 32]);
        let stored = StoredBacktestPreparation::fixture(
            origin,
            workspace,
            [7; 32],
            Instant::now() + Duration::from_secs(1),
        );
        let mut registry = ReceiptRegistry::new(1);
        registry.insert(receipt, stored, Instant::now())?;

        let retained = registry.consume(receipt, origin, workspace, [7; 32], Instant::now())?;
        assert_eq!(retained.catalog_digest, [7; 32]);
        assert!(
            registry
                .consume(receipt, origin, workspace, [7; 32], Instant::now())
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn receipt_rejects_foreign_origin_generation_catalog_and_digest_without_consuming()
    -> Result<(), Box<dyn std::error::Error>> {
        let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())?;
        let workspace = WorkspaceRuntimeIdentity::try_new(workspace_id, 7)?;
        let stale_workspace = WorkspaceRuntimeIdentity::try_new(workspace_id, 6)?;
        let origin = RequestOrigin::try_new(workspace_id.as_uuid(), Uuid::new_v4())?;
        let foreign_origin = RequestOrigin::try_new(workspace_id.as_uuid(), Uuid::new_v4())?;
        let receipt = BacktestPreparationReceipt::fixture(Uuid::new_v4(), [9; 32]);
        let mut registry = ReceiptRegistry::new(1);
        registry.insert(
            receipt,
            StoredBacktestPreparation::fixture(
                origin,
                workspace,
                [7; 32],
                Instant::now() + Duration::from_secs(1),
            ),
            Instant::now(),
        )?;

        assert!(
            registry
                .consume(receipt, foreign_origin, workspace, [7; 32], Instant::now())
                .is_err()
        );
        assert!(
            registry
                .consume(receipt, origin, stale_workspace, [7; 32], Instant::now())
                .is_err()
        );
        assert!(
            registry
                .consume(receipt, origin, workspace, [8; 32], Instant::now())
                .is_err()
        );
        assert!(
            registry
                .consume(
                    BacktestPreparationReceipt::fixture(receipt.receipt_id(), [8; 32]),
                    origin,
                    workspace,
                    [7; 32],
                    Instant::now(),
                )
                .is_err()
        );
        assert!(
            registry
                .consume(receipt, origin, workspace, [7; 32], Instant::now())
                .is_ok()
        );
        Ok(())
    }

    #[test]
    fn guided_preview_constructs_registration_server_side_and_consumes_it_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from("backtest-features")?,
            3,
            DatasetSchemaRegistry::local().canonical_feature_labels()?,
            Sha256Digest::new([3; 32]),
        )?;
        let dataset_id = SourceIdentifier::try_from("backtest-features")?;
        let catalog =
            BacktestPreparationCatalog::try_new(vec![BacktestPreparationDatasetInput::new(
                dataset_id.clone(),
                "Backtest features",
                manifest,
                vec![InstrumentId::try_from(Uuid::from_u128(1))?],
                Timestamp::from_unix_nanos(1_600_000_000_000_000_000),
                Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
                vec![SourceId::try_from("fixture-source")?],
            )])?;
        let authority = GovernedBacktestPreparationAuthority::try_new(
            BacktestPreparationLimits::try_new(2, Duration::from_secs(60))?,
        )?;
        let workspace_id = WorkspaceId::try_from_uuid(Uuid::new_v4())?;
        let workspace = WorkspaceRuntimeIdentity::try_new(workspace_id, 7)?;
        let origin = RequestOrigin::try_new(workspace_id.as_uuid(), Uuid::new_v4())?;
        let now = Instant::now();
        let preview = authority.preview(
            &catalog,
            BacktestPreparationSelection {
                dataset: dataset_id.as_str().to_owned(),
                period: "full-history".to_owned(),
                strategy: "baseline-buy-once".to_owned(),
                cost_policy: "standard".to_owned(),
                seed: "fixed-7".to_owned(),
                portfolio: "research-usd-100k".to_owned(),
                comparison: "single-run".to_owned(),
            },
            origin,
            workspace,
            now,
            Timestamp::from_unix_nanos(1_700_000_000_000_000_000),
        )?;

        let registration = authority.consume(
            &catalog,
            preview.receipt,
            origin,
            workspace,
            now + Duration::from_secs(1),
        )?;
        assert_eq!(
            registration.strategy_id.as_str(),
            "market-squawk-baseline-buy-once-v1"
        );
        assert_eq!(registration.table_name, "feature_labels");
        assert!(
            registration
                .sql
                .starts_with("SELECT * FROM feature_labels WHERE cutoff_at")
        );
        assert!(
            authority
                .consume(
                    &catalog,
                    preview.receipt,
                    origin,
                    workspace,
                    now + Duration::from_secs(2),
                )
                .is_err()
        );
        Ok(())
    }
}
