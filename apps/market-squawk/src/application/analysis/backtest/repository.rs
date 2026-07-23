//! Capability-confined governed-backtest input resolution and terminal indexing.

use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Instant,
};

use arrow::array::{Array as _, TimestampNanosecondArray};
use async_trait::async_trait;
use market_squawk_data::QueryResult;
use market_squawk_domain::{SourceId, SourceIdentifier, Timestamp};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use market_squawk_services::ServiceError;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    BacktestScope, GovernedBacktestCommand, GovernedBacktestRecord, GovernedBacktestRepository,
    canonical_run_id,
};
use crate::PinnedBacktestInput;

mod index;
pub(in crate::application::analysis::backtest) mod lifecycle;

use index::{StoredTerminal, TerminalIndex, command_digest, value_digest};
use lifecycle::{LinkedOperation, RepositoryLifecycle, await_blocking, ensure_operation_live};

const TERMINAL_INDEX_DIRECTORY: &str = "analysis/governed-backtests";
const HARD_MAXIMUM_TERMINALS: usize = 16_384;
const STANDARD_MAXIMUM_TERMINALS: usize = 4_096;
const STANDARD_MAXIMUM_INDEX_BYTES: usize = 7 * 1024 * 1024;

/// Explicit record-count and encoded-byte ceilings for the local terminal index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GovernedBacktestRepositoryLimits {
    maximum_terminals: usize,
    maximum_index_bytes: usize,
}

impl GovernedBacktestRepositoryLimits {
    /// Constructs limits no greater than the fixed process and persistence ceilings.
    ///
    /// # Errors
    ///
    /// Rejects zero values, more than 16,384 terminal records, or an encoded index larger than
    /// the crash-safe authority store can commit.
    pub fn try_new(
        maximum_terminals: usize,
        maximum_index_bytes: usize,
    ) -> Result<Self, ProductionGovernedBacktestRepositoryError> {
        if maximum_terminals == 0
            || maximum_terminals > HARD_MAXIMUM_TERMINALS
            || maximum_index_bytes == 0
            || maximum_index_bytes > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ProductionGovernedBacktestRepositoryError::InvalidLimits);
        }
        Ok(Self {
            maximum_terminals,
            maximum_index_bytes,
        })
    }

    /// Production local defaults bounded below the fixed authority-store payload ceiling.
    #[must_use]
    pub const fn standard() -> Self {
        Self {
            maximum_terminals: STANDARD_MAXIMUM_TERMINALS,
            maximum_index_bytes: STANDARD_MAXIMUM_INDEX_BYTES,
        }
    }
}

/// One resolver-returned input with its registered command binding.
///
/// Construction does not mint query or instrument authority: [`PinnedBacktestInput`] still
/// contains the non-forgeable catalog/query receipts. The repository compares this binding to the
/// admitted command and independently validates the available scope evidence before returning it
/// to the backtest service.
pub struct ResolvedGovernedBacktestInput {
    strategy_id: SourceIdentifier,
    input_id: SourceIdentifier,
    scope: BacktestScope,
    input: PinnedBacktestInput,
}

impl ResolvedGovernedBacktestInput {
    /// Binds a resolver-owned registered recipe to its freshly repinned input receipts.
    #[must_use]
    pub const fn new(
        strategy_id: SourceIdentifier,
        input_id: SourceIdentifier,
        scope: BacktestScope,
        input: PinnedBacktestInput,
    ) -> Self {
        Self {
            strategy_id,
            input_id,
            scope,
            input,
        }
    }

    fn validate(
        self,
        command: &GovernedBacktestCommand,
    ) -> Result<PinnedBacktestInput, ServiceError> {
        if &self.strategy_id != command.strategy_id()
            || &self.input_id != command.input_id()
            || &self.scope != command.scope()
        {
            return Err(ServiceError::InvalidResult);
        }
        validate_input_evidence(command, &self.input)?;
        Ok(self.input)
    }
}

impl fmt::Debug for ResolvedGovernedBacktestInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedGovernedBacktestInput")
            .field("strategy_id", &self.strategy_id)
            .field("input_id", &self.input_id)
            .field("scope", &self.scope)
            .field("input", &"[PINNED BACKTEST INPUT]")
            .finish()
    }
}

/// Least-authority resolver for one exact application-registered backtest input.
///
/// Implementations receive no arbitrary SQL, path, or mutation capability. They must resolve the
/// command's opaque `input_id` from an application-owned immutable recipe and return fresh pinned
/// query and instrument-definition receipts under the supplied cancellation and absolute deadline.
#[async_trait]
pub trait GovernedBacktestInputResolver: Send + Sync + 'static {
    /// Resolves one exact admitted command into a command-bound pinned input.
    async fn resolve(
        &self,
        command: &GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<ResolvedGovernedBacktestInput, ServiceError>;

    /// Atomically rejects new resolver work.
    fn begin_shutdown(&self);

    /// Completes resolver-owned reconciliation and task joining by the shared absolute deadline.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Capability-confined, restart-safe production repository for governed backtests.
pub struct ProductionGovernedBacktestRepository {
    resolver: Arc<dyn GovernedBacktestInputResolver>,
    store: Arc<LocalAuthorityStateStore>,
    index: Arc<Mutex<TerminalIndex>>,
    limits: GovernedBacktestRepositoryLimits,
    lifecycle: Arc<RepositoryLifecycle>,
}

impl ProductionGovernedBacktestRepository {
    /// Opens the fixed control-root namespace and validates the complete bounded terminal index.
    ///
    /// The caller cannot supply a path. The only ambient display path used during opening is
    /// derived from the already prepared [`LocalPaths`] control capability and a code-owned
    /// relative namespace; all later authority-state I/O stays relative to its retained directory
    /// handle.
    pub fn try_new(
        paths: &LocalPaths,
        resolver: Arc<dyn GovernedBacktestInputResolver>,
        limits: GovernedBacktestRepositoryLimits,
    ) -> Result<Self, ProductionGovernedBacktestRepositoryError> {
        GovernedBacktestRepositoryLimits::try_new(
            limits.maximum_terminals,
            limits.maximum_index_bytes,
        )?;
        let control = paths.control_root()?;
        control.try_clone_directory()?;
        let store = Arc::new(LocalAuthorityStateStore::try_open(
            control.root().join(TERMINAL_INDEX_DIRECTORY),
        )?);
        control.try_clone_directory()?;
        let index = store.load()?.map_or_else(
            || Ok(TerminalIndex::empty()),
            |bytes| TerminalIndex::decode(&bytes, limits),
        )?;
        Ok(Self {
            resolver,
            store,
            index: Arc::new(Mutex::new(index)),
            limits,
            lifecycle: RepositoryLifecycle::new(),
        })
    }
}

impl fmt::Debug for ProductionGovernedBacktestRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionGovernedBacktestRepository")
            .field("resolver", &"[GOVERNED INPUT RESOLVER]")
            .field("store", &self.store)
            .field("index", &"[BOUNDED TERMINAL INDEX]")
            .field("limits", &self.limits)
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

#[async_trait]
impl GovernedBacktestRepository for ProductionGovernedBacktestRepository {
    async fn resolve(
        &self,
        command: &GovernedBacktestCommand,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<PinnedBacktestInput, ServiceError> {
        let _call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let linked = LinkedOperation::new(
            cancellation.clone(),
            self.lifecycle.shutdown_token().clone(),
            deadline,
        );
        let resolved = tokio::select! {
            result = self.resolver.resolve(command, linked.token().clone(), deadline) => result,
            () = linked.token().cancelled() => {
                ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
                Err(ServiceError::Cancelled)
            }
        };
        ensure_operation_live(&cancellation, &self.lifecycle, deadline)?;
        resolved?.validate(command)
    }

    async fn publish(
        &self,
        command: &GovernedBacktestCommand,
        record: GovernedBacktestRecord,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<(), ServiceError> {
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let index = Arc::clone(&self.index);
        let store = Arc::clone(&self.store);
        let resolver = Arc::clone(&self.resolver);
        let lifecycle = Arc::clone(&self.lifecycle);
        let limits = self.limits;
        let command = command.clone();
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            publish_terminal(
                &index, &store, &resolver, &lifecycle, limits, command, record,
            )
        });
        await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
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
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let index = Arc::clone(&self.index);
        let run_id = run_id.to_owned();
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            let index = index.lock().map_err(|_| ServiceError::Unavailable)?;
            Ok(index.get(&run_id).cloned())
        });
        await_blocking(
            worker,
            &cancellation,
            self.lifecycle.shutdown_token(),
            deadline,
        )
        .await
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
        self.resolver.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.begin_shutdown();
        self.lifecycle.finish_shutdown(deadline).await?;
        self.resolver.finish_shutdown(deadline).await
    }
}

impl Drop for ProductionGovernedBacktestRepository {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// Construction or recovery failure before the repository can accept requests.
#[derive(Debug, Error)]
pub enum ProductionGovernedBacktestRepositoryError {
    /// Repository limits are zero or exceed a fixed process/persistence ceiling.
    #[error("governed backtest repository limits are invalid")]
    InvalidLimits,
    /// The prepared local control capability is unavailable or changed identity.
    #[error("governed backtest control path is unavailable: {0}")]
    Path(#[from] PathError),
    /// The two-copy authority store could not be opened or recovered.
    #[error("governed backtest authority state is unavailable: {0}")]
    Authority(#[from] LocalAuthorityStateStoreError),
    /// The durable terminal index is unsupported, malformed, noncanonical, or inconsistent.
    #[error("governed backtest terminal index is corrupt")]
    CorruptIndex,
    /// Bounded index allocation or encoding failed.
    #[error("governed backtest terminal index exceeded its resource contract")]
    ResourceExhausted,
}

fn validate_input_evidence(
    command: &GovernedBacktestCommand,
    input: &PinnedBacktestInput,
) -> Result<(), ServiceError> {
    let definition_count = input.instrument_definitions.instrument_count();
    if definition_count == 0
        || (!command.scope().instruments().is_empty()
            && (definition_count != command.scope().instruments().len()
                || !input.instrument_definitions.instrument_ids().eq(command
                    .scope()
                    .instruments()
                    .iter()
                    .copied())))
        || input.sources.is_empty()
        || !strictly_ordered(&input.sources)
        || (!command.scope().sources().is_empty()
            && !input
                .sources
                .iter()
                .map(SourceIdentifier::as_str)
                .eq(command.scope().sources().iter().map(SourceId::as_str)))
    {
        return Err(ServiceError::InvalidResult);
    }
    let QueryResult::Inline {
        batches,
        byte_count,
    } = input.query.result()
    else {
        return Err(ServiceError::InvalidResult);
    };
    if batches.is_empty() || *byte_count == 0 {
        return Err(ServiceError::InvalidResult);
    }
    if let Some((starts_at, ends_at)) = command.scope().time_range()
        && (input.instrument_definitions.as_of() < ends_at
            || batches
                .iter()
                .any(|batch| !batch_within_time_range(batch, starts_at, ends_at)))
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

pub(super) fn strictly_ordered<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn batch_within_time_range(
    batch: &arrow::record_batch::RecordBatch,
    starts_at: Timestamp,
    ends_at: Timestamp,
) -> bool {
    let Some(cutoffs) = batch
        .column_by_name("cutoff_at")
        .and_then(|column| column.as_any().downcast_ref::<TimestampNanosecondArray>())
    else {
        return false;
    };
    (0..cutoffs.len()).all(|row| {
        !cutoffs.is_null(row)
            && Timestamp::from_unix_nanos(cutoffs.value(row)) >= starts_at
            && Timestamp::from_unix_nanos(cutoffs.value(row)) <= ends_at
    })
}

fn publish_terminal(
    index: &Mutex<TerminalIndex>,
    store: &LocalAuthorityStateStore,
    resolver: &Arc<dyn GovernedBacktestInputResolver>,
    lifecycle: &RepositoryLifecycle,
    limits: GovernedBacktestRepositoryLimits,
    command: GovernedBacktestCommand,
    record: GovernedBacktestRecord,
) -> Result<(), ServiceError> {
    let record = GovernedBacktestRecord::try_from_persisted(record.content().clone())?;
    let command_digest = command_digest(&command)?;
    let record_digest = value_digest(record.content())?;
    let terminal = StoredTerminal {
        command,
        command_digest,
        record_digest,
        record,
    };
    let mut current = index.lock().map_err(|_| ServiceError::Unavailable)?;
    match current
        .entries
        .binary_search_by(|candidate| candidate.record.run_id().cmp(terminal.record.run_id()))
    {
        Ok(position) => {
            let existing = current
                .entries
                .get(position)
                .ok_or(ServiceError::Internal)?;
            return if existing == &terminal {
                Ok(())
            } else {
                Err(ServiceError::InvalidResult)
            };
        }
        Err(_) if current.entries.len() >= limits.maximum_terminals => {
            return Err(ServiceError::ResourceExhausted);
        }
        Err(_) => {}
    }
    let mut candidate = current.clone();
    candidate.insert(terminal)?;
    let encoded = candidate
        .encode(limits)
        .map_err(map_repository_error_to_service)?;
    if let Err(error) = store.store(&encoded) {
        lifecycle.begin_shutdown();
        resolver.begin_shutdown();
        return Err(map_authority_error_to_service(error));
    }
    *current = candidate;
    Ok(())
}

fn map_authority_error_to_service(error: LocalAuthorityStateStoreError) -> ServiceError {
    match error {
        LocalAuthorityStateStoreError::PayloadTooLarge { .. }
        | LocalAuthorityStateStoreError::EnvelopeTooLarge { .. }
        | LocalAuthorityStateStoreError::Allocation
        | LocalAuthorityStateStoreError::GenerationExhausted => ServiceError::ResourceExhausted,
        _ => ServiceError::Unavailable,
    }
}

fn map_repository_error_to_service(
    error: ProductionGovernedBacktestRepositoryError,
) -> ServiceError {
    match error {
        ProductionGovernedBacktestRepositoryError::InvalidLimits
        | ProductionGovernedBacktestRepositoryError::ResourceExhausted => {
            ServiceError::ResourceExhausted
        }
        ProductionGovernedBacktestRepositoryError::CorruptIndex => ServiceError::InvalidResult,
        ProductionGovernedBacktestRepositoryError::Path(_)
        | ProductionGovernedBacktestRepositoryError::Authority(_) => ServiceError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc, time::Instant};

    use async_trait::async_trait;
    use market_squawk_domain::SourceIdentifier;
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::ServiceError;
    use serde_json::{Value, json};
    use tokio_util::sync::CancellationToken;

    use super::{
        GovernedBacktestInputResolver, GovernedBacktestRepositoryLimits,
        ProductionGovernedBacktestRepository, ResolvedGovernedBacktestInput,
    };
    use crate::application::analysis::{
        BacktestScope, GovernedBacktestCommand, GovernedBacktestRecord, GovernedBacktestRepository,
    };

    type TestResult = Result<(), Box<dyn Error>>;

    struct MissingInputResolver;

    #[async_trait]
    impl GovernedBacktestInputResolver for MissingInputResolver {
        async fn resolve(
            &self,
            _command: &GovernedBacktestCommand,
            _cancellation: CancellationToken,
            _deadline: Instant,
        ) -> Result<ResolvedGovernedBacktestInput, ServiceError> {
            Err(ServiceError::NotFound)
        }

        fn begin_shutdown(&self) {}

        async fn finish_shutdown(&self, _deadline: Instant) -> Result<(), ServiceError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn terminal_index_survives_restart_and_rejects_conflicting_reuse() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path())?;
        let limits = GovernedBacktestRepositoryLimits::standard();
        let first = ProductionGovernedBacktestRepository::try_new(
            &paths,
            Arc::new(MissingInputResolver),
            limits,
        )?;
        let record = record_with_dataset("11")?;
        let command = GovernedBacktestCommand::new(
            SourceIdentifier::try_from("strategy-v1")?,
            SourceIdentifier::try_from("input-v1")?,
            BacktestScope::new(Vec::new(), None, Vec::new()),
        );
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        first
            .publish(&command, record.clone(), CancellationToken::new(), deadline)
            .await?;
        first
            .publish(&command, record, CancellationToken::new(), deadline)
            .await?;
        first.begin_shutdown();
        first.finish_shutdown(deadline).await?;
        drop(first);

        let restarted = ProductionGovernedBacktestRepository::try_new(
            &paths,
            Arc::new(MissingInputResolver),
            limits,
        )?;
        let restored = restarted
            .get(
                &"aa".repeat(32),
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        let conflict = restarted
            .publish(
                &command,
                record_with_dataset("22")?,
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await;

        assert_eq!(
            (
                restored.as_ref().map(GovernedBacktestRecord::content),
                conflict,
            ),
            (
                Some(record_with_dataset("11")?.content()),
                Err(ServiceError::InvalidResult),
            )
        );
        Ok(())
    }

    fn record_with_dataset(dataset_byte: &str) -> Result<GovernedBacktestRecord, ServiceError> {
        let digest = |byte: &str| byte.repeat(32);
        GovernedBacktestRecord::try_from_persisted(json!({
            "recordVersion": 1,
            "runId": digest("aa"),
            "datasetIdentity": digest(dataset_byte),
            "objectGraphDigest": digest("bb"),
            "executionAssumptionDigest": digest("cc"),
            "cohortAuthorityDigest": Value::Null,
            "cohortUniverseDigest": Value::Null,
            "seed": 7,
            "selectionCriterion": "risk-adjusted-return",
            "status": {"state": "failed"}
        }))
    }
}
