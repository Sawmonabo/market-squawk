//! Governed backtest resolution, execution, durable indexing, and bounded read contracts.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_backtesting::{BacktestOutcome, TrialStatus};
use market_squawk_domain::{InstrumentId, SourceId, SourceIdentifier, Timestamp};
use market_squawk_services::ServiceError;
use serde_json::{Map, Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    PinnedBacktestInput, ProductionBacktestService, ProductionBacktestServiceError,
    application::domain_support::encode_hex,
};

mod input_authority;
mod repository;

pub use input_authority::{
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputAuthorityLimits,
    GovernedBacktestInputRegistrationInput, GovernedBacktestInputRegistrationReceipt,
    GovernedBacktestPortfolioSeedInput, GovernedBacktestQueryLimitsInput,
    ProductionGovernedBacktestInputAuthority, ProductionGovernedBacktestInputAuthorityError,
};
pub use repository::{
    GovernedBacktestInputResolver, GovernedBacktestRepositoryLimits,
    ProductionGovernedBacktestRepository, ProductionGovernedBacktestRepositoryError,
    ResolvedGovernedBacktestInput,
};

const MAXIMUM_BACKTEST_RECORD_BYTES: usize = 1024 * 1024;
const MAXIMUM_BACKTEST_RECORD_METRICS: usize = 4_096;
const BACKTEST_RECORD_VERSION: u16 = 1;

/// Optional exact scope attached to one governed backtest request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestScope {
    instruments: Box<[InstrumentId]>,
    time_range: Option<(Timestamp, Timestamp)>,
    sources: Box<[SourceId]>,
}

impl BacktestScope {
    pub(super) fn new(
        instruments: Vec<InstrumentId>,
        time_range: Option<(Timestamp, Timestamp)>,
        sources: Vec<SourceId>,
    ) -> Self {
        Self {
            instruments: instruments.into_boxed_slice(),
            time_range,
            sources: sources.into_boxed_slice(),
        }
    }

    /// Exact instrument universe requested by the caller.
    #[must_use]
    pub fn instruments(&self) -> &[InstrumentId] {
        &self.instruments
    }

    /// Exact optional observation interval requested by the caller.
    #[must_use]
    pub const fn time_range(&self) -> Option<(Timestamp, Timestamp)> {
        self.time_range
    }

    /// Exact optional source coverage requested by the caller.
    #[must_use]
    pub fn sources(&self) -> &[SourceId] {
        &self.sources
    }
}

/// Fully admitted command whose input identity resolves to a non-forgeable pinned request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernedBacktestCommand {
    strategy_id: SourceIdentifier,
    input_id: SourceIdentifier,
    scope: BacktestScope,
}

impl GovernedBacktestCommand {
    pub(super) const fn new(
        strategy_id: SourceIdentifier,
        input_id: SourceIdentifier,
        scope: BacktestScope,
    ) -> Self {
        Self {
            strategy_id,
            input_id,
            scope,
        }
    }

    /// Registered immutable strategy-build identity.
    #[must_use]
    pub const fn strategy_id(&self) -> &SourceIdentifier {
        &self.strategy_id
    }

    /// Application-owned pinned input identity.
    #[must_use]
    pub const fn input_id(&self) -> &SourceIdentifier {
        &self.input_id
    }

    /// Caller-admitted data scope that the resolver must match exactly.
    #[must_use]
    pub const fn scope(&self) -> &BacktestScope {
        &self.scope
    }
}

/// Stable bounded representation of one durable governed backtest terminal.
#[derive(Clone)]
pub struct GovernedBacktestRecord {
    run_id: Box<str>,
    content: Value,
}

impl GovernedBacktestRecord {
    fn from_outcome(outcome: &BacktestOutcome) -> Result<Self, ServiceError> {
        let (trial, run) = match outcome {
            BacktestOutcome::Completed(result) => (result.trial(), Some(result.run())),
            BacktestOutcome::Failed(failure) => (failure.trial(), None),
        };
        let spec = trial.spec();
        let run_id = encode_hex(spec.id().digest().bytes());
        let status = match trial.status() {
            TrialStatus::Reserved => return Err(ServiceError::InvalidResult),
            TrialStatus::Completed(completion) => {
                let run = run.ok_or(ServiceError::InvalidResult)?;
                json!({
                    "state": "completed",
                    "resultDigest": encode_hex(completion.result_digest().bytes()),
                    "artifact": {
                        "reference": completion.artifact().reference(),
                        "digest": encode_hex(completion.artifact().digest().bytes()),
                        "byteCount": completion.artifact().byte_count()
                    },
                    "metrics": completion
                        .metrics()
                        .iter()
                        .map(|metric| json!({
                            "name": metric.name().as_str(),
                            "value": metric.value()
                        }))
                        .collect::<Vec<_>>(),
                    "datasetPartition": completion.dataset_partition().map(|partition| json!({
                        "startsAtUnixNanos": partition.starts_at().unix_nanos(),
                        "endsAtUnixNanos": partition.ends_at().unix_nanos()
                    })),
                    "fillCount": run.fills().len(),
                    "noActionCount": run.no_action_count(),
                    "accountingReconciliation": "independent"
                })
            }
            TrialStatus::Failed(_) => json!({"state": "failed"}),
        };
        let content = json!({
            "recordVersion": BACKTEST_RECORD_VERSION,
            "runId": run_id,
            "datasetIdentity": encode_hex(spec.dataset_identity().bytes()),
            "objectGraphDigest": encode_hex(spec.object_graph_digest().bytes()),
            "executionAssumptionDigest": encode_hex(spec.execution_assumption_digest().bytes()),
            "cohortAuthorityDigest": spec
                .cohort_authority_digest()
                .map(|digest| encode_hex(digest.bytes())),
            "cohortUniverseDigest": spec
                .cohort_universe_digest()
                .map(|digest| encode_hex(digest.bytes())),
            "seed": spec.seed(),
            "selectionCriterion": spec.selection_criterion().as_str(),
            "status": status
        });
        Self::try_from_persisted(content)
    }

    /// Reconstructs one bounded durable record after repository-level integrity verification.
    ///
    /// # Errors
    ///
    /// Rejects a malformed or non-canonical run identity, an unknown terminal state, and records
    /// whose metadata should have been emitted as a referenced artifact instead.
    pub fn try_from_persisted(content: Value) -> Result<Self, ServiceError> {
        let encoded_bytes = serde_json::to_vec(&content)
            .map_err(|_| ServiceError::InvalidResult)?
            .len();
        if encoded_bytes > MAXIMUM_BACKTEST_RECORD_BYTES {
            return Err(ServiceError::ResourceExhausted);
        }
        let object = content.as_object().ok_or(ServiceError::InvalidResult)?;
        if !has_exact_fields(
            object,
            &[
                "recordVersion",
                "runId",
                "datasetIdentity",
                "objectGraphDigest",
                "executionAssumptionDigest",
                "cohortAuthorityDigest",
                "cohortUniverseDigest",
                "seed",
                "selectionCriterion",
                "status",
            ],
        ) || object.get("recordVersion").and_then(Value::as_u64)
            != Some(u64::from(BACKTEST_RECORD_VERSION))
        {
            return Err(ServiceError::InvalidResult);
        }
        let run_id = object
            .get("runId")
            .and_then(Value::as_str)
            .filter(|value| canonical_run_id(value))
            .ok_or(ServiceError::InvalidResult)?;
        for field in [
            "datasetIdentity",
            "objectGraphDigest",
            "executionAssumptionDigest",
        ] {
            object
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| canonical_run_id(value))
                .ok_or(ServiceError::InvalidResult)?;
        }
        for field in ["cohortAuthorityDigest", "cohortUniverseDigest"] {
            if !optional_digest(object.get(field)) {
                return Err(ServiceError::InvalidResult);
            }
        }
        object
            .get("seed")
            .and_then(Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?;
        object
            .get("selectionCriterion")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidResult)
            .and_then(|value| {
                SourceIdentifier::try_from(value).map_err(|_| ServiceError::InvalidResult)
            })?;
        if !valid_terminal_status(object.get("status")) {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            run_id: run_id.to_owned().into_boxed_str(),
            content,
        })
    }

    /// Canonical lowercase SHA-256 trial identity.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Bounded transport-neutral record body.
    #[must_use]
    pub const fn content(&self) -> &Value {
        &self.content
    }
}

impl fmt::Debug for GovernedBacktestRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GovernedBacktestRecord")
            .field("run_id", &self.run_id)
            .field("content", &"[BACKTEST RECORD]")
            .finish()
    }
}

/// Durable application authority for resolving pinned inputs and indexing terminal records.
#[async_trait]
pub trait GovernedBacktestRepository: Send + Sync + 'static {
    /// Resolves a caller-inaccessible pinned query and experiment from one registered identity.
    async fn resolve(
        &self,
        command: &GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PinnedBacktestInput, ServiceError>;

    /// Publishes the stable application index after the underlying inventory is terminal.
    async fn publish(
        &self,
        command: &GovernedBacktestCommand,
        record: GovernedBacktestRecord,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError>;

    /// Loads one indexed terminal by its exact canonical run identity.
    async fn get(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Option<GovernedBacktestRecord>, ServiceError>;

    /// Atomically rejects new repository work and cancels owned activity.
    fn begin_shutdown(&self);

    /// Completes bounded repository reconciliation and task joining.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Backtest authority consumed by the transport-neutral Analysis domain service.
#[async_trait]
pub trait GovernedBacktestAuthority: Send + Sync + 'static {
    /// Runs and indexes one pinned, strategy-registered experiment.
    async fn run(
        &self,
        command: GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestRecord, ServiceError>;

    /// Loads one durable governed result.
    async fn get(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Option<GovernedBacktestRecord>, ServiceError>;

    /// Atomically rejects new work and cancels owned activity.
    fn begin_shutdown(&self);

    /// Completes bounded reconciliation and task joining.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Production adapter over the existing reserve-before-run service and a durable request index.
pub struct ProductionBacktestAuthority {
    service: Arc<ProductionBacktestService>,
    repository: Arc<dyn GovernedBacktestRepository>,
    shutdown: CancellationToken,
}

impl ProductionBacktestAuthority {
    /// Binds the governed engine to the sole application input/result repository.
    #[must_use]
    pub fn new(
        service: Arc<ProductionBacktestService>,
        repository: Arc<dyn GovernedBacktestRepository>,
    ) -> Self {
        Self {
            service,
            repository,
            shutdown: CancellationToken::new(),
        }
    }

    fn ensure_live(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        if self.shutdown.is_cancelled() {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }
}

impl fmt::Debug for ProductionBacktestAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionBacktestAuthority")
            .field("service", &self.service)
            .field("repository", &"[GOVERNED BACKTEST REPOSITORY]")
            .field("shutdown", &self.shutdown.is_cancelled())
            .finish()
    }
}

#[async_trait]
impl GovernedBacktestAuthority for ProductionBacktestAuthority {
    async fn run(
        &self,
        command: GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestRecord, ServiceError> {
        self.ensure_live(&cancellation, deadline)?;
        let linked = LinkedCancellation::new(cancellation, self.shutdown.child_token(), deadline);
        let input = self
            .repository
            .resolve(&command, linked.token().clone(), deadline)
            .await?;
        self.ensure_live(linked.token(), deadline)?;
        let service = Arc::clone(&self.service);
        let strategy_id = command.strategy_id().clone();
        let worker_cancellation = linked.token().clone();
        let worker = tokio::task::spawn_blocking(move || {
            service.run(input, &strategy_id, &worker_cancellation)
        });
        let outcome = join_backtest(worker, linked.token(), deadline).await?;
        let record = GovernedBacktestRecord::from_outcome(&outcome)?;
        self.repository
            .publish(&command, record.clone(), linked.token().clone(), deadline)
            .await?;
        self.ensure_live(linked.token(), deadline)?;
        Ok(record)
    }

    async fn get(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Option<GovernedBacktestRecord>, ServiceError> {
        if !canonical_run_id(run_id) {
            return Err(ServiceError::InvalidRequest);
        }
        self.ensure_live(&cancellation, deadline)?;
        let linked = LinkedCancellation::new(cancellation, self.shutdown.child_token(), deadline);
        let result = self
            .repository
            .get(run_id, linked.token().clone(), deadline)
            .await?;
        self.ensure_live(linked.token(), deadline)?;
        Ok(result)
    }

    fn begin_shutdown(&self) {
        self.shutdown.cancel();
        self.repository.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        self.repository.finish_shutdown(deadline).await
    }
}

impl Drop for ProductionBacktestAuthority {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct LinkedCancellation {
    token: CancellationToken,
    monitor: JoinHandle<()>,
}

impl LinkedCancellation {
    fn new(request: CancellationToken, domain: CancellationToken, deadline: Instant) -> Self {
        let token = CancellationToken::new();
        let output = token.clone();
        let monitored_output = output.clone();
        let monitor = tokio::spawn(async move {
            tokio::select! {
                () = request.cancelled() => monitored_output.cancel(),
                () = domain.cancelled() => monitored_output.cancel(),
                () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    monitored_output.cancel();
                }
            }
        });
        Self {
            token: output,
            monitor,
        }
    }

    const fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for LinkedCancellation {
    fn drop(&mut self) {
        self.token.cancel();
        self.monitor.abort();
    }
}

async fn join_backtest(
    worker: JoinHandle<Result<BacktestOutcome, ProductionBacktestServiceError>>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<BacktestOutcome, ServiceError> {
    let result = worker.await.map_err(|_| ServiceError::Internal)?;
    if Instant::now() >= deadline {
        return Err(ServiceError::DeadlineExceeded);
    }
    if cancellation.is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    result.map_err(map_backtest_error)
}

fn map_backtest_error(error: ProductionBacktestServiceError) -> ServiceError {
    match error {
        ProductionBacktestServiceError::Path(_) => ServiceError::Unavailable,
        ProductionBacktestServiceError::Admission(_) => ServiceError::NotFound,
        ProductionBacktestServiceError::Backtest(_)
        | ProductionBacktestServiceError::Experiment(_)
        | ProductionBacktestServiceError::Service(_) => ServiceError::Internal,
    }
}

pub(super) fn experiment_input_id(
    experiment: &Map<String, Value>,
) -> Result<SourceIdentifier, ServiceError> {
    if experiment.len() != 1 || !experiment.contains_key("inputId") {
        return Err(ServiceError::InvalidRequest);
    }
    experiment
        .get("inputId")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(|value| {
            SourceIdentifier::try_from(value).map_err(|_| ServiceError::InvalidRequest)
        })
}

pub(super) fn canonical_run_id(value: &str) -> bool {
    value.len() == 64
        && value.bytes().any(|byte| byte != b'0')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn optional_digest(value: Option<&Value>) -> bool {
    value.is_some_and(|value| value.is_null() || value.as_str().is_some_and(canonical_run_id))
}

fn valid_terminal_status(value: Option<&Value>) -> bool {
    let Some(status) = value.and_then(Value::as_object) else {
        return false;
    };
    match status.get("state").and_then(Value::as_str) {
        Some("failed") => has_exact_fields(status, &["state"]),
        Some("completed") => valid_completed_status(status),
        _ => false,
    }
}

fn valid_completed_status(status: &Map<String, Value>) -> bool {
    if !has_exact_fields(
        status,
        &[
            "state",
            "resultDigest",
            "artifact",
            "metrics",
            "datasetPartition",
            "fillCount",
            "noActionCount",
            "accountingReconciliation",
        ],
    ) || !status
        .get("resultDigest")
        .and_then(Value::as_str)
        .is_some_and(canonical_run_id)
        || status.get("fillCount").and_then(Value::as_u64).is_none()
        || status
            .get("noActionCount")
            .and_then(Value::as_u64)
            .is_none()
        || status
            .get("accountingReconciliation")
            .and_then(Value::as_str)
            != Some("independent")
    {
        return false;
    }
    let Some(artifact) = status.get("artifact").and_then(Value::as_object) else {
        return false;
    };
    if !has_exact_fields(artifact, &["reference", "digest", "byteCount"])
        || !artifact
            .get("reference")
            .and_then(Value::as_str)
            .is_some_and(canonical_artifact_reference)
        || !artifact
            .get("digest")
            .and_then(Value::as_str)
            .is_some_and(canonical_run_id)
        || !artifact
            .get("byteCount")
            .and_then(Value::as_u64)
            .is_some_and(|bytes| bytes > 0)
    {
        return false;
    }
    let Some(metrics) = status.get("metrics").and_then(Value::as_array) else {
        return false;
    };
    if !valid_metrics(metrics) {
        return false;
    }
    valid_dataset_partition(status.get("datasetPartition"))
}

fn valid_metrics(metrics: &[Value]) -> bool {
    if metrics.len() > MAXIMUM_BACKTEST_RECORD_METRICS {
        return false;
    }
    let mut previous = None;
    for metric in metrics {
        let Some(metric) = metric.as_object() else {
            return false;
        };
        let Some(name) = metric.get("name").and_then(Value::as_str) else {
            return false;
        };
        if !has_exact_fields(metric, &["name", "value"])
            || SourceIdentifier::try_from(name).is_err()
            || previous.is_some_and(|previous| previous >= name)
            || metric
                .get("value")
                .and_then(Value::as_f64)
                .is_none_or(|value| !value.is_finite())
        {
            return false;
        }
        previous = Some(name);
    }
    true
}

fn valid_dataset_partition(value: Option<&Value>) -> bool {
    value.is_some_and(|value| {
        if value.is_null() {
            return true;
        }
        let Some(partition) = value.as_object() else {
            return false;
        };
        if !has_exact_fields(partition, &["startsAtUnixNanos", "endsAtUnixNanos"]) {
            return false;
        }
        let Some(starts_at) = partition.get("startsAtUnixNanos").and_then(Value::as_i64) else {
            return false;
        };
        partition
            .get("endsAtUnixNanos")
            .and_then(Value::as_i64)
            .is_some_and(|ends_at| starts_at < ends_at)
    })
}

fn canonical_artifact_reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 8_192
        && !value.starts_with('/')
        && !value.contains('\\')
        && value.split('/').all(|component| {
            !component.is_empty()
                && !matches!(component, "." | "..")
                && component
                    .chars()
                    .all(|character| !character.is_control() && !character.is_whitespace())
        })
}

fn has_exact_fields(object: &Map<String, Value>, fields: &[&str]) -> bool {
    object.len() == fields.len() && fields.iter().all(|field| object.contains_key(*field))
}
