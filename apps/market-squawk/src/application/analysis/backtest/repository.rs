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
    BacktestScope, GovernedBacktestCommand, GovernedBacktestDiscoveryPage,
    GovernedBacktestDiscoveryQuery, GovernedBacktestRecord, GovernedBacktestRepository,
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

    async fn discover_completed(
        &self,
        query: GovernedBacktestDiscoveryQuery,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestDiscoveryPage, ServiceError> {
        let call = RepositoryLifecycle::enter(&self.lifecycle, &cancellation, deadline)?;
        let index = Arc::clone(&self.index);
        let worker = tokio::task::spawn_blocking(move || {
            let _call = call;
            let index = index.lock().map_err(|_| ServiceError::Unavailable)?;
            index.discover_completed(&query)
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
    if let Some(cohort) = &input.cohort {
        if cohort.members.is_empty()
            || cohort
                .members
                .iter()
                .any(|member| !input_within_command_scope(command, &member.input))
        {
            return Err(ServiceError::InvalidResult);
        }
        return Ok(());
    }
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
    if !command.scope().time_ranges().is_empty()
        && (input.instrument_definitions.as_of()
            < command
                .scope()
                .time_ranges()
                .iter()
                .map(|(_, ends_at)| *ends_at)
                .max()
                .ok_or(ServiceError::InvalidResult)?
            || batches.iter().any(|batch| {
                !command
                    .scope()
                    .time_ranges()
                    .iter()
                    .any(|(starts_at, ends_at)| {
                        batch_within_time_range(batch, *starts_at, *ends_at)
                    })
            }))
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn input_within_command_scope(
    command: &GovernedBacktestCommand,
    input: &PinnedBacktestInput,
) -> bool {
    let instruments = input
        .instrument_definitions
        .instrument_ids()
        .collect::<Vec<_>>();
    if instruments.is_empty()
        || instruments.iter().any(|instrument| {
            command
                .scope()
                .instruments()
                .binary_search(instrument)
                .is_err()
        })
        || input.sources.is_empty()
        || !strictly_ordered(&input.sources)
        || !input.sources.iter().all(|source| {
            SourceId::try_from(source.as_str())
                .ok()
                .is_some_and(|source| command.scope().sources().binary_search(&source).is_ok())
        })
    {
        return false;
    }
    let QueryResult::Inline {
        batches,
        byte_count,
    } = input.query.result()
    else {
        return false;
    };
    *byte_count > 0
        && !batches.is_empty()
        && batches.iter().all(|batch| {
            command
                .scope()
                .time_ranges()
                .iter()
                .any(|(starts_at, ends_at)| {
                    input.instrument_definitions.as_of() >= *ends_at
                        && batch_within_time_range(batch, *starts_at, *ends_at)
                })
        })
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
    use std::{error::Error, num::NonZeroUsize, sync::Arc, time::Instant};

    use async_trait::async_trait;
    use market_squawk_backtesting::{
        ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, ResearchLiquidityPriority,
    };
    use market_squawk_domain::{BasisPoints, Currency, InstrumentId, SourceIdentifier};
    use market_squawk_platform::LocalPaths;
    use market_squawk_services::ServiceError;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        GovernedBacktestInputResolver, GovernedBacktestRepositoryLimits,
        ProductionGovernedBacktestRepository, ResolvedGovernedBacktestInput, command_digest,
        value_digest,
    };
    use crate::application::analysis::{
        BacktestScope, GovernedBacktestCommand, GovernedBacktestRecord, GovernedBacktestRepository,
    };

    use super::super::{
        GovernedBacktestArtifactEvidence, GovernedBacktestCohortDiagnosticsEvidence,
        GovernedBacktestDiscoveryQuery, encode_hex, execution_assumptions_content,
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
    async fn terminal_index_survives_restart_and_discovers_only_exact_bound_scope() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path())?;
        let limits = GovernedBacktestRepositoryLimits::standard();
        let first = ProductionGovernedBacktestRepository::try_new(
            &paths,
            Arc::new(MissingInputResolver),
            limits,
        )?;
        let instrument = InstrumentId::try_from(Uuid::from_u128(1))?;
        let other_instrument = InstrumentId::try_from(Uuid::from_u128(2))?;
        let command = GovernedBacktestCommand::new(
            SourceIdentifier::try_from("strategy-v1")?,
            SourceIdentifier::try_from("input-v1")?,
            BacktestScope::new(vec![instrument], Vec::new(), Vec::new()),
        );
        let other_command = GovernedBacktestCommand::new(
            SourceIdentifier::try_from("strategy-v1")?,
            SourceIdentifier::try_from("input-other-instrument")?,
            BacktestScope::new(vec![other_instrument], Vec::new(), Vec::new()),
        );
        let other_strategy_command = GovernedBacktestCommand::new(
            SourceIdentifier::try_from("strategy-v2")?,
            SourceIdentifier::try_from("input-other-strategy")?,
            BacktestScope::new(vec![instrument], Vec::new(), Vec::new()),
        );
        let record = record_with_dataset("aa", "11")?;
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        first
            .publish(&command, record.clone(), CancellationToken::new(), deadline)
            .await?;
        first
            .publish(&command, record, CancellationToken::new(), deadline)
            .await?;
        first
            .publish(
                &other_command,
                record_with_dataset("bb", "33")?,
                CancellationToken::new(),
                deadline,
            )
            .await?;
        first
            .publish(
                &other_strategy_command,
                record_with_dataset("cc", "44")?,
                CancellationToken::new(),
                deadline,
            )
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
        let all_query = GovernedBacktestDiscoveryQuery::try_new(
            instrument,
            None,
            NonZeroUsize::new(8).ok_or("discovery bound")?,
        )?;
        let all_for_instrument = restarted
            .discover_completed(
                all_query.clone(),
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        let repeated_for_instrument = restarted
            .discover_completed(
                all_query,
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        let exact_strategy = restarted
            .discover_completed(
                GovernedBacktestDiscoveryQuery::try_new(
                    instrument,
                    Some(SourceIdentifier::try_from("strategy-v1")?),
                    NonZeroUsize::new(8).ok_or("discovery bound")?,
                )?,
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        let limited_for_instrument = restarted
            .discover_completed(
                GovernedBacktestDiscoveryQuery::try_new(instrument, None, NonZeroUsize::MIN)?,
                CancellationToken::new(),
                Instant::now() + std::time::Duration::from_secs(5),
            )
            .await?;
        let conflict = restarted
            .publish(
                &command,
                record_with_dataset("aa", "22")?,
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
                Some(record_with_dataset("aa", "11")?.content()),
                Err(ServiceError::InvalidResult),
            )
        );
        assert_eq!(
            all_for_instrument
                .entries()
                .iter()
                .map(|entry| entry.record().run_id())
                .collect::<Vec<_>>(),
            vec!["aa".repeat(32), "cc".repeat(32)]
        );
        assert!(!all_for_instrument.truncated());
        assert!(all_for_instrument.is_complete());
        assert_eq!(
            all_for_instrument.selection_digest(),
            repeated_for_instrument.selection_digest()
        );
        assert_eq!(exact_strategy.entries().len(), 1);
        assert_eq!(exact_strategy.entries()[0].command(), &command);
        let expected_command_digest = command_digest(&command)?;
        let expected_record_digest = value_digest(exact_strategy.entries()[0].record().content())?;
        assert_eq!(
            exact_strategy.entries()[0].command_digest(),
            expected_command_digest.as_str()
        );
        assert_eq!(
            exact_strategy.entries()[0].record_digest(),
            expected_record_digest.as_str()
        );
        assert!(limited_for_instrument.truncated());
        assert!(!limited_for_instrument.is_complete());
        assert_ne!(
            limited_for_instrument.selection_digest(),
            all_for_instrument.selection_digest()
        );
        let recommendation_evidence =
            all_for_instrument.recommendation_evidence(&all_for_instrument.entries()[0])?;
        assert_eq!(
            (
                recommendation_evidence.selection_digest(),
                recommendation_evidence.command_digest(),
                recommendation_evidence.record_digest(),
                recommendation_evidence.dataset_identity(),
                recommendation_evidence.command(),
                recommendation_evidence.terminal().metrics().len(),
                recommendation_evidence.terminal().reporting_currency(),
                recommendation_evidence.terminal().partial_fill_count(),
            ),
            (
                all_for_instrument.selection_digest(),
                command.evidence_digest()?,
                all_for_instrument.entries()[0].record().evidence_digest()?,
                market_squawk_domain::EvidenceDigest::new(
                    market_squawk_domain::DigestAlgorithm::Sha256,
                    [0x11; 32],
                ),
                &command,
                3,
                Currency::try_from("USD")?,
                0,
            )
        );
        assert!(matches!(
            recommendation_evidence.terminal().artifact(),
            GovernedBacktestArtifactEvidence::GovernedReport(_)
        ));
        assert_eq!(
            recommendation_evidence.terminal().execution_assumptions(),
            &execution_assumptions()?
        );
        let GovernedBacktestCohortDiagnosticsEvidence::Completed(cohort) =
            recommendation_evidence.terminal().cohort_diagnostics()
        else {
            return Err("expected completed cohort evidence".into());
        };
        assert_eq!(cohort.trial_count(), 12);
        assert!(
            recommendation_evidence
                .terminal()
                .dataset_partition()
                .starts_at()
                < recommendation_evidence
                    .terminal()
                    .dataset_partition()
                    .ends_at()
        );
        Ok(())
    }

    fn record_with_dataset(
        run_byte: &str,
        dataset_byte: &str,
    ) -> Result<GovernedBacktestRecord, ServiceError> {
        let digest = |byte: &str| byte.repeat(32);
        let assumptions = execution_assumptions()?;
        GovernedBacktestRecord::try_from_persisted(json!({
            "recordVersion": 2,
            "runId": digest(run_byte),
            "datasetIdentity": digest(dataset_byte),
            "objectGraphDigest": digest("bb"),
            "executionAssumptionDigest": encode_hex(assumptions.digest().bytes()),
            "cohortAuthorityDigest": digest("f1"),
            "cohortUniverseDigest": digest("f2"),
            "seed": 7,
            "selectionCriterion": "cost-adjusted-total-return",
            "status": {
                "state": "completed",
                "resultDigest": digest("dd"),
                "artifact": {
                    "artifactId": format!("backtest-report-{}", digest("ee")),
                    "sha256": digest("ee"),
                    "byteCount": 1,
                    "mediaType": "application/json"
                },
                "metrics": [
                    {"name": "cost-adjusted-total-return", "value": 0.1},
                    {"name": "maximum-drawdown", "value": 0.05},
                    {"name": "return-observations", "value": 3.0}
                ],
                "datasetPartition": {"startsAtUnixNanos": 1, "endsAtUnixNanos": 2},
                "fillCount": 0,
                "partialFillCount": 0,
                "noActionCount": 1,
                "reportingCurrency": "USD",
                "accountingReconciliation": "independent",
                "executionAssumptions": execution_assumptions_content(assumptions),
                "cohortDiagnostics": {
                    "state": "completed",
                    "evaluationId": digest("f3"),
                    "trialCount": 12,
                    "probabilityOfBacktestOverfitting": 0.25,
                    "foldCount": 2,
                    "deflatedPerformanceProbability": 0.75,
                    "expectedMaximumSharpe": 0.5
                }
            }
        }))
    }

    fn execution_assumptions() -> Result<ResearchExecutionAssumptions, ServiceError> {
        ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
            version: 3,
            fee_basis_points: BasisPoints::new(1),
            slippage_basis_points: BasisPoints::new(2),
            maximum_random_slippage_basis_points: BasisPoints::new(3),
            maximum_participation_basis_points: BasisPoints::new(5),
            liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
            latency_nanos: 4,
            allow_partial_fills: true,
            fee_decimal_scale: 2,
        })
        .map_err(|_| ServiceError::InvalidResult)
    }
}
