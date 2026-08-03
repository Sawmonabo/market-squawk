//! Governed backtest resolution, execution, durable indexing, and bounded read contracts.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_backtesting::{
    BacktestCohortEvaluation, BacktestOutcome, ResearchExecutionAssumptions,
    ResearchLiquidityPriority, TrialStatus,
};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, InstrumentId, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::ServiceError;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    PinnedBacktestInput, ProductionBacktestService, ProductionBacktestServiceError,
    application::domain_support::encode_hex,
};

mod input_authority;
mod repository;

pub use input_authority::{
    BacktestPreparationCatalog, BacktestPreparationDatasetInput, BacktestPreparationError,
    BacktestPreparationLimits, BacktestPreparationOptions, BacktestPreparationPreview,
    BacktestPreparationReceipt, BacktestPreparationSelection,
    GovernedBacktestCohortCandidateRegistrationInput,
    GovernedBacktestCohortMemberRegistrationInput, GovernedBacktestCohortRegistrationInput,
    GovernedBacktestCorporateActionsInput, GovernedBacktestInputAuthorityLimits,
    GovernedBacktestInputRegistrar, GovernedBacktestInputRegistrationInput,
    GovernedBacktestInputRegistrationJsonError, GovernedBacktestInputRegistrationReceipt,
    GovernedBacktestPortfolioSeedInput, GovernedBacktestPreparationAuthority,
    GovernedBacktestQueryLimitsInput, MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES,
    ProductionGovernedBacktestInputAuthority, ProductionGovernedBacktestInputAuthorityError,
};
pub use repository::{
    GovernedBacktestInputResolver, GovernedBacktestRepositoryLimits,
    ProductionGovernedBacktestRepository, ProductionGovernedBacktestRepositoryError,
    ResolvedGovernedBacktestInput,
};

const MAXIMUM_BACKTEST_RECORD_BYTES: usize = 1024 * 1024;
const MAXIMUM_BACKTEST_RECORD_METRICS: usize = 4_096;
const BACKTEST_RECORD_VERSION: u16 = 2;
const LEGACY_BACKTEST_RECORD_VERSION: u16 = 1;
const BACKTEST_REPORT_MEDIA_TYPE: &str = "application/json";
const BACKTEST_REPORT_ID_PREFIX: &str = "backtest-report-";

/// Optional exact scope attached to one governed backtest request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestScope {
    instruments: Box<[InstrumentId]>,
    time_ranges: Box<[(Timestamp, Timestamp)]>,
    sources: Box<[SourceId]>,
}

impl BacktestScope {
    /// Admits a canonical bounded scope for a registered immutable backtest input.
    pub fn try_new(
        instruments: Vec<InstrumentId>,
        time_range: Option<(Timestamp, Timestamp)>,
        sources: Vec<SourceId>,
    ) -> Result<Self, ServiceError> {
        Self::try_new_with_time_ranges(instruments, time_range.into_iter().collect(), sources)
    }

    /// Admits a canonical exact union of governed time intervals.
    pub fn try_new_with_time_ranges(
        instruments: Vec<InstrumentId>,
        time_ranges: Vec<(Timestamp, Timestamp)>,
        sources: Vec<SourceId>,
    ) -> Result<Self, ServiceError> {
        if instruments.windows(2).any(|pair| pair[0] >= pair[1])
            || sources.windows(2).any(|pair| pair[0] >= pair[1])
            || time_ranges
                .iter()
                .any(|(starts_at, ends_at)| starts_at >= ends_at)
            || time_ranges.windows(2).any(|pair| pair[0].1 >= pair[1].0)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self::new(instruments, time_ranges, sources))
    }

    pub(super) fn new(
        instruments: Vec<InstrumentId>,
        time_ranges: Vec<(Timestamp, Timestamp)>,
        sources: Vec<SourceId>,
    ) -> Self {
        Self {
            instruments: instruments.into_boxed_slice(),
            time_ranges: time_ranges.into_boxed_slice(),
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
        if self.time_ranges.len() == 1 {
            Some(self.time_ranges[0])
        } else {
            None
        }
    }

    /// Exact sorted, non-overlapping time coverage authorized for this command.
    #[must_use]
    pub fn time_ranges(&self) -> &[(Timestamp, Timestamp)] {
        &self.time_ranges
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
    /// Constructs an exact command whose opaque input identity is revalidated by the resolver.
    pub fn try_new(
        strategy_id: SourceIdentifier,
        input_id: SourceIdentifier,
        instruments: Vec<InstrumentId>,
        time_range: Option<(Timestamp, Timestamp)>,
        sources: Vec<SourceId>,
    ) -> Result<Self, ServiceError> {
        Ok(Self::new(
            strategy_id,
            input_id,
            BacktestScope::try_new(instruments, time_range, sources)?,
        ))
    }

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

    /// Content digest binding the registered strategy, input identity, and complete scope.
    pub fn evidence_digest(&self) -> Result<EvidenceDigest, ServiceError> {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/governed-backtest-command/v1");
        hash_text(&mut hash, self.strategy_id.as_str())?;
        hash_text(&mut hash, self.input_id.as_str())?;
        hash_count(&mut hash, self.scope.instruments.len())?;
        for instrument in &self.scope.instruments {
            hash.update(instrument.as_uuid().as_bytes());
        }
        match self.scope.time_ranges.as_ref() {
            [] => hash.update([0]),
            [(starts_at, ends_at)] => {
                hash.update([1]);
                hash.update(starts_at.unix_nanos().to_be_bytes());
                hash.update(ends_at.unix_nanos().to_be_bytes());
            }
            time_ranges => {
                hash.update([2]);
                hash_count(&mut hash, time_ranges.len())?;
                for (starts_at, ends_at) in time_ranges {
                    hash.update(starts_at.unix_nanos().to_be_bytes());
                    hash.update(ends_at.unix_nanos().to_be_bytes());
                }
            }
        }
        hash_count(&mut hash, self.scope.sources.len())?;
        for source in &self.scope.sources {
            hash_text(&mut hash, source.as_str())?;
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hash.finalize().into(),
        ))
    }
}

/// Stable bounded representation of one durable governed backtest terminal.
#[derive(Clone)]
pub struct GovernedBacktestRecord {
    run_id: Box<str>,
    content: Value,
}

impl GovernedBacktestRecord {
    fn from_outcome(
        outcome: &BacktestOutcome,
        assumptions: ResearchExecutionAssumptions,
        cohort: Option<&BacktestCohortEvaluation>,
    ) -> Result<Self, ServiceError> {
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
                let report_digest = encode_hex(completion.artifact().digest().bytes());
                json!({
                    "state": "completed",
                    "resultDigest": encode_hex(completion.result_digest().bytes()),
                    "artifact": {
                        "artifactId": format!("{BACKTEST_REPORT_ID_PREFIX}{report_digest}"),
                        "sha256": report_digest,
                        "byteCount": completion.artifact().byte_count(),
                        "mediaType": BACKTEST_REPORT_MEDIA_TYPE
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
                    "partialFillCount": run.fills().iter().filter(|fill| fill.partial()).count(),
                    "noActionCount": run.no_action_count(),
                    "accountingReconciliation": "independent",
                    "executionAssumptions": execution_assumptions_content(assumptions),
                    "cohortDiagnostics": cohort_diagnostics_content(cohort, spec.id())?
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
        let record_version = object
            .get("recordVersion")
            .and_then(Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?;
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
        ) || (record_version != u64::from(LEGACY_BACKTEST_RECORD_VERSION)
            && record_version != u64::from(BACKTEST_RECORD_VERSION))
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
        if !valid_terminal_status(object.get("status"), record_version) {
            return Err(ServiceError::InvalidResult);
        }
        if record_version == u64::from(BACKTEST_RECORD_VERSION)
            && object
                .get("status")
                .and_then(Value::as_object)
                .and_then(|status| status.get("cohortDiagnostics"))
                .and_then(Value::as_object)
                .and_then(|diagnostics| diagnostics.get("state"))
                .and_then(Value::as_str)
                == Some("completed")
            && (object
                .get("cohortAuthorityDigest")
                .and_then(Value::as_str)
                .is_none()
                || object
                    .get("cohortUniverseDigest")
                    .and_then(Value::as_str)
                    .is_none())
        {
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

    /// Exact digest of the canonical bounded terminal record.
    pub fn evidence_digest(&self) -> Result<EvidenceDigest, ServiceError> {
        let encoded = serde_json::to_vec(&self.content).map_err(|_| ServiceError::InvalidResult)?;
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(encoded).into(),
        ))
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

/// Opaque, content-addressed report identity accepted by the governed backtest authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernedBacktestReportReference {
    artifact_id: Box<str>,
    sha256: [u8; 32],
    byte_count: u64,
}

impl GovernedBacktestReportReference {
    /// Validates a V2 report reference without accepting a filesystem path or arbitrary media type.
    pub fn try_new(
        artifact_id: &str,
        sha256: &str,
        byte_count: u64,
        media_type: &str,
    ) -> Result<Self, ServiceError> {
        let digest = decode_digest(sha256).ok_or(ServiceError::InvalidRequest)?;
        if byte_count == 0
            || media_type != BACKTEST_REPORT_MEDIA_TYPE
            || artifact_id != format!("{BACKTEST_REPORT_ID_PREFIX}{sha256}")
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            artifact_id: artifact_id.into(),
            sha256: digest,
            byte_count,
        })
    }

    /// Returns the path-free published report identifier.
    #[must_use]
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Returns the exact SHA-256 content identity.
    #[must_use]
    pub const fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    /// Returns the exact bounded report length.
    #[must_use]
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    /// Returns the only report media type admitted by this authority.
    #[must_use]
    pub const fn media_type(&self) -> &'static str {
        BACKTEST_REPORT_MEDIA_TYPE
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

/// Additional application authority spanning one governed terminal publication boundary.
pub trait GovernedBacktestPrepublishAuthority: fmt::Debug + Send + Sync + 'static {
    /// Claims publication immediately before the durable governed repository commit.
    fn validate_prepublish(&self) -> Result<(), ServiceError>;

    /// Seals the claimed authority immediately after durable publication succeeds.
    fn commit_succeeded(&self);
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

    /// Runs with one additional exact prepublication authority used by durable jobs.
    async fn run_with_prepublish(
        &self,
        command: GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        prepublish: Arc<dyn GovernedBacktestPrepublishAuthority>,
    ) -> Result<GovernedBacktestRecord, ServiceError>;

    /// Loads one durable governed result.
    async fn get(
        &self,
        run_id: &str,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Option<GovernedBacktestRecord>, ServiceError>;

    /// Reads a completed report through the confined backtest artifact authority.
    ///
    /// The default preserves older test-only authorities while failing closed for all callers.
    async fn read_report(
        &self,
        _report: GovernedBacktestReportReference,
        _cancellation: CancellationToken,
        _deadline: Instant,
    ) -> Result<Vec<u8>, ServiceError> {
        Err(ServiceError::Unavailable)
    }

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

    async fn run_inner(
        &self,
        command: GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        prepublish: Option<Arc<dyn GovernedBacktestPrepublishAuthority>>,
    ) -> Result<GovernedBacktestRecord, ServiceError> {
        self.ensure_live(&cancellation, deadline)?;
        let linked = LinkedCancellation::new(cancellation, self.shutdown.child_token(), deadline);
        let mut input = self
            .repository
            .resolve(&command, linked.token().clone(), deadline)
            .await?;
        self.ensure_live(linked.token(), deadline)?;
        let assumptions = input.execution_assumptions;
        let service = Arc::clone(&self.service);
        let strategy_id = command.strategy_id().clone();
        let worker_cancellation = linked.token().clone();
        let cohort = input.cohort.take();
        let worker = tokio::task::spawn_blocking(move || match cohort {
            Some(cohort) => service
                .run_cohort(cohort, &strategy_id, &worker_cancellation)
                .map(|outcome| (outcome.outcome, Some(outcome.evaluation))),
            None => service
                .run(input, &strategy_id, &worker_cancellation)
                .map(|outcome| (outcome, None)),
        });
        let (outcome, cohort) = join_backtest(worker, linked.token(), deadline).await?;
        let record = GovernedBacktestRecord::from_outcome(&outcome, assumptions, cohort.as_ref())?;
        self.ensure_live(linked.token(), deadline)?;
        if let Some(prepublish) = &prepublish {
            prepublish.validate_prepublish()?;
        }
        self.repository
            .publish(&command, record.clone(), linked.token().clone(), deadline)
            .await?;
        if let Some(prepublish) = &prepublish {
            prepublish.commit_succeeded();
        }
        Ok(record)
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
        self.run_inner(command, cancellation, deadline, None).await
    }

    async fn run_with_prepublish(
        &self,
        command: GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
        prepublish: Arc<dyn GovernedBacktestPrepublishAuthority>,
    ) -> Result<GovernedBacktestRecord, ServiceError> {
        self.run_inner(command, cancellation, deadline, Some(prepublish))
            .await
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

    async fn read_report(
        &self,
        report: GovernedBacktestReportReference,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<Vec<u8>, ServiceError> {
        self.ensure_live(&cancellation, deadline)?;
        let linked = LinkedCancellation::new(cancellation, self.shutdown.child_token(), deadline);
        let service = Arc::clone(&self.service);
        let worker = tokio::task::spawn_blocking(move || {
            service.read_report(
                market_squawk_data::Sha256Digest::new(report.sha256()),
                report.byte_count(),
            )
        });
        let result = worker.await.map_err(|_| ServiceError::Internal)?;
        self.ensure_live(linked.token(), deadline)?;
        result.map_err(map_backtest_error)
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
    worker: JoinHandle<
        Result<(BacktestOutcome, Option<BacktestCohortEvaluation>), ProductionBacktestServiceError>,
    >,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(BacktestOutcome, Option<BacktestCohortEvaluation>), ServiceError> {
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

fn valid_terminal_status(value: Option<&Value>, record_version: u64) -> bool {
    let Some(status) = value.and_then(Value::as_object) else {
        return false;
    };
    match status.get("state").and_then(Value::as_str) {
        Some("failed") => has_exact_fields(status, &["state"]),
        Some("completed") => valid_completed_status(status, record_version),
        _ => false,
    }
}

fn valid_completed_status(status: &Map<String, Value>, record_version: u64) -> bool {
    if record_version == u64::from(LEGACY_BACKTEST_RECORD_VERSION) {
        return valid_legacy_completed_status(status);
    }
    if !has_exact_fields(
        status,
        &[
            "state",
            "resultDigest",
            "artifact",
            "metrics",
            "datasetPartition",
            "fillCount",
            "partialFillCount",
            "noActionCount",
            "accountingReconciliation",
            "executionAssumptions",
            "cohortDiagnostics",
        ],
    ) || !status
        .get("resultDigest")
        .and_then(Value::as_str)
        .is_some_and(canonical_run_id)
        || status.get("fillCount").and_then(Value::as_u64).is_none()
        || status
            .get("partialFillCount")
            .and_then(Value::as_u64)
            .is_none()
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
    if !has_exact_fields(
        artifact,
        &["artifactId", "sha256", "byteCount", "mediaType"],
    ) || GovernedBacktestReportReference::try_new(
        artifact
            .get("artifactId")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        artifact
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        artifact
            .get("byteCount")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        artifact
            .get("mediaType")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    )
    .is_err()
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
        && valid_execution_assumptions(status.get("executionAssumptions"))
        && valid_cohort_diagnostics(status.get("cohortDiagnostics"))
}

fn valid_cohort_diagnostics(value: Option<&Value>) -> bool {
    let Some(diagnostics) = value.and_then(Value::as_object) else {
        return false;
    };
    match diagnostics.get("state").and_then(Value::as_str) {
        Some("not-evaluated") => has_exact_fields(diagnostics, &["state"]),
        Some("completed") => {
            has_exact_fields(
                diagnostics,
                &[
                    "state",
                    "evaluationId",
                    "probabilityOfBacktestOverfitting",
                    "foldCount",
                    "deflatedPerformanceProbability",
                    "expectedMaximumSharpe",
                ],
            ) && diagnostics
                .get("evaluationId")
                .and_then(Value::as_str)
                .is_some_and(canonical_run_id)
                && diagnostics
                    .get("probabilityOfBacktestOverfitting")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value.is_finite() && (0.0..=1.0).contains(&value))
                && diagnostics
                    .get("foldCount")
                    .and_then(Value::as_u64)
                    .is_some_and(|value| value >= 2)
                && diagnostics
                    .get("deflatedPerformanceProbability")
                    .and_then(Value::as_f64)
                    .is_some_and(|value| value.is_finite() && (0.0..=1.0).contains(&value))
                && diagnostics
                    .get("expectedMaximumSharpe")
                    .and_then(Value::as_f64)
                    .is_some_and(f64::is_finite)
        }
        _ => false,
    }
}

fn valid_legacy_completed_status(status: &Map<String, Value>) -> bool {
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
    has_exact_fields(artifact, &["reference", "digest", "byteCount"])
        && artifact
            .get("reference")
            .and_then(Value::as_str)
            .is_some_and(canonical_artifact_reference)
        && artifact
            .get("digest")
            .and_then(Value::as_str)
            .is_some_and(canonical_run_id)
        && artifact
            .get("byteCount")
            .and_then(Value::as_u64)
            .is_some_and(|bytes| bytes > 0)
        && status
            .get("metrics")
            .and_then(Value::as_array)
            .is_some_and(|metrics| valid_metrics(metrics))
        && valid_dataset_partition(status.get("datasetPartition"))
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

fn cohort_diagnostics_content(
    cohort: Option<&BacktestCohortEvaluation>,
    trial_id: market_squawk_backtesting::TrialId,
) -> Result<Value, ServiceError> {
    let Some(cohort) = cohort else {
        return Ok(json!({"state": "not-evaluated"}));
    };
    if cohort.selected().trial_id() != trial_id {
        return Err(ServiceError::InvalidResult);
    }
    let pbo = cohort.probability_of_backtest_overfitting();
    let deflated = cohort.deflated_performance();
    if !pbo.probability().is_finite()
        || !deflated.probability().is_finite()
        || !deflated.expected_maximum_sharpe().is_finite()
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(json!({
        "state": "completed",
        "evaluationId": encode_hex(cohort.id().digest().bytes()),
        "probabilityOfBacktestOverfitting": pbo.probability(),
        "foldCount": pbo.fold_count(),
        "deflatedPerformanceProbability": deflated.probability(),
        "expectedMaximumSharpe": deflated.expected_maximum_sharpe(),
    }))
}

fn execution_assumptions_content(assumptions: ResearchExecutionAssumptions) -> Value {
    json!({
        "policyVersion": assumptions.version().get(),
        "feeBasisPoints": assumptions.fee_basis_points().get(),
        "spreadModel": "observed-point-in-time-half-spread",
        "slippageBasisPoints": assumptions.slippage_basis_points().get(),
        "maximumRandomSlippageBasisPoints": assumptions.maximum_random_slippage_basis_points().get(),
        "latencyNanos": assumptions.latency_nanos(),
        "maximumParticipationBasisPoints": assumptions.maximum_participation_basis_points().get(),
        "liquidityPriority": match assumptions.liquidity_priority() {
            ResearchLiquidityPriority::SignalTimeThenOrderId => "signal-time-then-order-id",
        },
        "partialFillsAllowed": assumptions.allow_partial_fills(),
        "feeDecimalScale": assumptions.fee_decimal_scale(),
    })
}

fn valid_execution_assumptions(value: Option<&Value>) -> bool {
    let Some(value) = value.and_then(Value::as_object) else {
        return false;
    };
    has_exact_fields(
        value,
        &[
            "policyVersion",
            "feeBasisPoints",
            "spreadModel",
            "slippageBasisPoints",
            "maximumRandomSlippageBasisPoints",
            "latencyNanos",
            "maximumParticipationBasisPoints",
            "liquidityPriority",
            "partialFillsAllowed",
            "feeDecimalScale",
        ],
    ) && value.get("policyVersion").and_then(Value::as_u64) == Some(3)
        && value
            .get("feeBasisPoints")
            .and_then(Value::as_i64)
            .is_some_and(|value| (0..=10_000).contains(&value))
        && value.get("spreadModel").and_then(Value::as_str)
            == Some("observed-point-in-time-half-spread")
        && value
            .get("slippageBasisPoints")
            .and_then(Value::as_i64)
            .is_some_and(|value| (0..=10_000).contains(&value))
        && value
            .get("maximumRandomSlippageBasisPoints")
            .and_then(Value::as_i64)
            .is_some_and(|value| (0..=10_000).contains(&value))
        && value
            .get("latencyNanos")
            .and_then(Value::as_i64)
            .is_some_and(|value| value > 0)
        && value
            .get("maximumParticipationBasisPoints")
            .and_then(Value::as_i64)
            .is_some_and(|value| (1..=10_000).contains(&value))
        && value.get("liquidityPriority").and_then(Value::as_str)
            == Some("signal-time-then-order-id")
        && value
            .get("partialFillsAllowed")
            .and_then(Value::as_bool)
            .is_some()
        && value
            .get("feeDecimalScale")
            .and_then(Value::as_u64)
            .is_some_and(|value| value <= 28)
}

fn decode_digest(value: &str) -> Option<[u8; 32]> {
    if !canonical_run_id(value) {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        output[index] = u8::try_from((high << 4) | low).ok()?;
    }
    Some(output)
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

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), ServiceError> {
    hash_count(hash, value.len())?;
    hash.update(value.as_bytes());
    Ok(())
}

fn hash_count(hash: &mut Sha256, value: usize) -> Result<(), ServiceError> {
    let value = u64::try_from(value).map_err(|_| ServiceError::ResourceExhausted)?;
    hash.update(value.to_be_bytes());
    Ok(())
}

fn has_exact_fields(object: &Map<String, Value>, fields: &[&str]) -> bool {
    object.len() == fields.len() && fields.iter().all(|field| object.contains_key(*field))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::GovernedBacktestRecord;

    #[test]
    fn governed_v2_record_binds_visible_execution_and_report_evidence() {
        let digest = |byte: &str| byte.repeat(32);
        let record = GovernedBacktestRecord::try_from_persisted(json!({
            "recordVersion": 2,
            "runId": digest("11"),
            "datasetIdentity": digest("22"),
            "objectGraphDigest": digest("33"),
            "executionAssumptionDigest": digest("44"),
            "cohortAuthorityDigest": null,
            "cohortUniverseDigest": null,
            "seed": 7,
            "selectionCriterion": "total-return",
            "status": {
                "state": "completed",
                "resultDigest": digest("55"),
                "artifact": {
                    "artifactId": format!("backtest-report-{}", digest("66")),
                    "sha256": digest("66"),
                    "byteCount": 128,
                    "mediaType": "application/json"
                },
                "metrics": [],
                "datasetPartition": {
                    "startsAtUnixNanos": 1,
                    "endsAtUnixNanos": 2
                },
                "fillCount": 2,
                "partialFillCount": 1,
                "noActionCount": 0,
                "accountingReconciliation": "independent",
                "executionAssumptions": {
                    "policyVersion": 3,
                    "feeBasisPoints": 1,
                    "spreadModel": "observed-point-in-time-half-spread",
                    "slippageBasisPoints": 2,
                    "maximumRandomSlippageBasisPoints": 3,
                    "latencyNanos": 4,
                    "maximumParticipationBasisPoints": 5,
                    "liquidityPriority": "signal-time-then-order-id",
                    "partialFillsAllowed": true,
                    "feeDecimalScale": 2
                },
                "cohortDiagnostics": {"state": "not-evaluated"}
            }
        }));
        assert!(record.is_ok());
    }

    #[test]
    fn governed_v2_record_accepts_only_bound_completed_cohort_diagnostics() {
        let digest = |byte: &str| byte.repeat(32);
        let record = GovernedBacktestRecord::try_from_persisted(json!({
            "recordVersion": 2,
            "runId": digest("11"),
            "datasetIdentity": digest("22"),
            "objectGraphDigest": digest("33"),
            "executionAssumptionDigest": digest("44"),
            "cohortAuthorityDigest": digest("55"),
            "cohortUniverseDigest": digest("66"),
            "seed": 7,
            "selectionCriterion": "total-return",
            "status": {
                "state": "completed",
                "resultDigest": digest("77"),
                "artifact": {
                    "artifactId": format!("backtest-report-{}", digest("88")),
                    "sha256": digest("88"),
                    "byteCount": 128,
                    "mediaType": "application/json"
                },
                "metrics": [],
                "datasetPartition": {
                    "startsAtUnixNanos": 1,
                    "endsAtUnixNanos": 2
                },
                "fillCount": 2,
                "partialFillCount": 1,
                "noActionCount": 0,
                "accountingReconciliation": "independent",
                "executionAssumptions": {
                    "policyVersion": 3,
                    "feeBasisPoints": 1,
                    "spreadModel": "observed-point-in-time-half-spread",
                    "slippageBasisPoints": 2,
                    "maximumRandomSlippageBasisPoints": 3,
                    "latencyNanos": 4,
                    "maximumParticipationBasisPoints": 5,
                    "liquidityPriority": "signal-time-then-order-id",
                    "partialFillsAllowed": true,
                    "feeDecimalScale": 2
                },
                "cohortDiagnostics": {
                    "state": "completed",
                    "evaluationId": digest("99"),
                    "probabilityOfBacktestOverfitting": 0.25,
                    "foldCount": 2,
                    "deflatedPerformanceProbability": 0.75,
                    "expectedMaximumSharpe": 0.5
                }
            }
        }));
        assert!(record.is_ok());
    }
}
