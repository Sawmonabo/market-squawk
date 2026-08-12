//! Governed backtest resolution, execution, durable indexing, and bounded read contracts.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_backtesting::{
    BacktestCohortEvaluation, BacktestOutcome, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput, ResearchLiquidityPriority, TrialDatasetPartition,
    TrialMetric, TrialStatus,
};
use market_squawk_domain::{
    BasisPoints, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceId,
    SourceIdentifier, Timestamp,
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
#[allow(
    unused_imports,
    reason = "a generic analysis consumer uses this least-authority seam after composition"
)]
pub(crate) use input_authority::{
    GovernedRecommendationInputMaterializerV1, GovernedRecommendationMaterializedInputV1,
};
pub use repository::{
    GovernedBacktestInputResolver, GovernedBacktestRepositoryLimits,
    ProductionGovernedBacktestRepository, ProductionGovernedBacktestRepositoryError,
    ResolvedGovernedBacktestInput,
};

const MAXIMUM_BACKTEST_RECORD_BYTES: usize = 1024 * 1024;
const MAXIMUM_BACKTEST_RECORD_METRICS: usize = 4_096;
const BACKTEST_RECORD_VERSION: u16 = 2;
const BACKTEST_REPORT_MEDIA_TYPE: &str = "application/json";
const BACKTEST_REPORT_ID_PREFIX: &str = "backtest-report-";
/// Fixed ceiling for one instrument-bound governed-backtest discovery response.
const MAX_GOVERNED_BACKTEST_DISCOVERY_RESULTS: usize = 256;

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

/// Least-authority selector for completed governed backtests involving one exact instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GovernedBacktestDiscoveryQuery {
    instrument_id: InstrumentId,
    strategy_id: Option<SourceIdentifier>,
    maximum_results: NonZeroUsize,
}

impl GovernedBacktestDiscoveryQuery {
    /// Admits one bounded exact-instrument query and optional exact strategy identity.
    pub fn try_new(
        instrument_id: InstrumentId,
        strategy_id: Option<SourceIdentifier>,
        maximum_results: NonZeroUsize,
    ) -> Result<Self, ServiceError> {
        if maximum_results.get() > MAX_GOVERNED_BACKTEST_DISCOVERY_RESULTS {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            instrument_id,
            strategy_id,
            maximum_results,
        })
    }

    /// Exact canonical instrument required in the retained command scope.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Optional exact registered strategy identity.
    #[must_use]
    pub const fn strategy_id(&self) -> Option<&SourceIdentifier> {
        self.strategy_id.as_ref()
    }

    /// Maximum number of records this query may return.
    #[must_use]
    pub const fn maximum_results(&self) -> NonZeroUsize {
        self.maximum_results
    }
}

/// One completed terminal together with the exact retained command and input scope that produced it.
#[derive(Clone, Debug)]
pub struct GovernedBacktestDiscoveryEntry {
    command: GovernedBacktestCommand,
    command_digest: Box<str>,
    record_digest: Box<str>,
    record: GovernedBacktestRecord,
}

impl GovernedBacktestDiscoveryEntry {
    pub(super) fn new(
        command: GovernedBacktestCommand,
        command_digest: &str,
        record_digest: &str,
        record: GovernedBacktestRecord,
    ) -> Self {
        Self {
            command,
            command_digest: command_digest.into(),
            record_digest: record_digest.into(),
            record,
        }
    }

    /// Registered strategy, opaque input identity, and complete admitted data scope.
    #[must_use]
    pub const fn command(&self) -> &GovernedBacktestCommand {
        &self.command
    }

    /// Exact SHA-256 digest authenticated by the recovered repository for the retained command.
    #[must_use]
    pub fn command_digest(&self) -> &str {
        &self.command_digest
    }

    /// Exact SHA-256 digest authenticated by the recovered repository for the terminal record.
    #[must_use]
    pub fn record_digest(&self) -> &str {
        &self.record_digest
    }

    /// Validated durable completed terminal.
    #[must_use]
    pub const fn record(&self) -> &GovernedBacktestRecord {
        &self.record
    }
}

/// Whether one governed-backtest discovery exhausted its exact retained-index selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GovernedBacktestDiscoveryCompleteness {
    /// Every exact match in the retained bounded index is represented.
    Complete,
    /// Additional exact matches exist beyond the query's admitted result bound.
    Truncated,
}

/// Deterministic bounded discovery receipt ordered by canonical run identity.
#[derive(Clone, Debug)]
pub struct GovernedBacktestDiscoveryPage {
    query: GovernedBacktestDiscoveryQuery,
    entries: Box<[GovernedBacktestDiscoveryEntry]>,
    completeness: GovernedBacktestDiscoveryCompleteness,
    selection_digest: EvidenceDigest,
}

impl GovernedBacktestDiscoveryPage {
    pub(super) fn try_new(
        query: GovernedBacktestDiscoveryQuery,
        entries: Vec<GovernedBacktestDiscoveryEntry>,
        truncated: bool,
    ) -> Result<Self, ServiceError> {
        let completeness = if truncated {
            GovernedBacktestDiscoveryCompleteness::Truncated
        } else {
            GovernedBacktestDiscoveryCompleteness::Complete
        };
        if entries.len() > query.maximum_results().get()
            || (truncated && entries.len() != query.maximum_results().get())
            || entries
                .windows(2)
                .any(|pair| pair[0].record().run_id() >= pair[1].record().run_id())
            || entries.iter().any(|entry| {
                !entry.record().is_completed()
                    || !canonical_run_id(entry.command_digest())
                    || !canonical_run_id(entry.record_digest())
                    || entry
                        .command()
                        .scope()
                        .instruments()
                        .binary_search(&query.instrument_id())
                        .is_err()
                    || query
                        .strategy_id()
                        .is_some_and(|strategy| entry.command().strategy_id() != strategy)
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        let selection_digest = backtest_discovery_selection_digest(&query, completeness, &entries)?;
        Ok(Self {
            query,
            entries: entries.into_boxed_slice(),
            completeness,
            selection_digest,
        })
    }

    /// Exact bounded selector authenticated by this receipt.
    #[must_use]
    pub const fn query(&self) -> &GovernedBacktestDiscoveryQuery {
        &self.query
    }

    /// Completed exact-scope matches in stable ascending run-identity order.
    #[must_use]
    pub fn entries(&self) -> &[GovernedBacktestDiscoveryEntry] {
        &self.entries
    }

    /// Explicit complete-or-truncated state bound into [`Self::selection_digest`].
    #[must_use]
    pub const fn completeness(&self) -> GovernedBacktestDiscoveryCompleteness {
        self.completeness
    }

    /// Whether every exact retained-index match is represented.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(
            self.completeness,
            GovernedBacktestDiscoveryCompleteness::Complete
        )
    }

    /// Whether additional exact matches existed beyond the admitted result bound.
    #[must_use]
    pub const fn truncated(&self) -> bool {
        matches!(
            self.completeness,
            GovernedBacktestDiscoveryCompleteness::Truncated
        )
    }

    /// Versioned digest binding the exact query, completeness, and ordered stored identities.
    #[must_use]
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    /// Produces revalidated research evidence for one entry bound to this exact selection.
    ///
    /// This projection carries no order, risk-approval, dispatch, or execution authority.
    pub fn recommendation_evidence(
        &self,
        entry: &GovernedBacktestDiscoveryEntry,
    ) -> Result<GovernedBacktestRecommendationEvidence, ServiceError> {
        project_backtest_recommendation_evidence(self, entry)
    }
}

fn backtest_discovery_selection_digest(
    query: &GovernedBacktestDiscoveryQuery,
    completeness: GovernedBacktestDiscoveryCompleteness,
    entries: &[GovernedBacktestDiscoveryEntry],
) -> Result<EvidenceDigest, ServiceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/governed-backtest-discovery-selection/v1");
    hash.update(query.instrument_id().as_uuid().as_bytes());
    match query.strategy_id() {
        Some(strategy_id) => {
            hash.update([1]);
            hash_text(&mut hash, strategy_id.as_str())?;
        }
        None => hash.update([0]),
    }
    hash_count(&mut hash, query.maximum_results().get())?;
    hash.update([match completeness {
        GovernedBacktestDiscoveryCompleteness::Complete => 0,
        GovernedBacktestDiscoveryCompleteness::Truncated => 1,
    }]);
    hash_count(&mut hash, entries.len())?;
    for entry in entries {
        hash_text(&mut hash, entry.record().run_id())?;
        hash_text(&mut hash, entry.command_digest())?;
        hash_text(&mut hash, entry.record_digest())?;
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
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
                let reporting_currency = run.portfolio().marked_equity().currency();
                let partition = completion.dataset_partition();
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
                    "datasetPartition": {
                        "startsAtUnixNanos": partition.starts_at().unix_nanos(),
                        "endsAtUnixNanos": partition.ends_at().unix_nanos()
                    },
                    "fillCount": run.fills().len(),
                    "partialFillCount": run.fills().iter().filter(|fill| fill.partial()).count(),
                    "noActionCount": run.no_action_count(),
                    "reportingCurrency": reporting_currency.as_str(),
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
            "cohortAuthorityDigest": encode_hex(spec.cohort_authority_digest().bytes()),
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
        ) || record_version != u64::from(BACKTEST_RECORD_VERSION)
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
        if !object
            .get("cohortAuthorityDigest")
            .and_then(Value::as_str)
            .is_some_and(canonical_run_id)
            || !optional_digest(object.get("cohortUniverseDigest"))
        {
            return Err(ServiceError::InvalidResult);
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
        if let Some(status) = object
            .get("status")
            .and_then(Value::as_object)
            .filter(|status| status.get("state").and_then(Value::as_str) == Some("completed"))
        {
            let fill_count = status
                .get("fillCount")
                .and_then(Value::as_u64)
                .ok_or(ServiceError::InvalidResult)?;
            let partial_fill_count = status
                .get("partialFillCount")
                .and_then(Value::as_u64)
                .ok_or(ServiceError::InvalidResult)?;
            if partial_fill_count > fill_count {
                return Err(ServiceError::InvalidResult);
            }
            let expected_digest = required_record_digest(object, "executionAssumptionDigest")?;
            parse_execution_assumptions_evidence(status, expected_digest)?;
        }
        if object
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

    /// Whether this validated terminal contains a successful completed run.
    #[must_use]
    pub fn is_completed(&self) -> bool {
        self.content
            .get("status")
            .and_then(Value::as_object)
            .and_then(|status| status.get("state"))
            .and_then(Value::as_str)
            == Some("completed")
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
    /// Validates a current report reference without accepting a filesystem path or arbitrary media type.
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

/// Why an optional recommendation-evidence field cannot be proven from the current record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GovernedBacktestEvidenceGap {
    /// The governed terminal explicitly retained no value for this optional evidence.
    NotReported,
}

/// A typed evidence value or an explicit reason that no value can be proven.
#[derive(Clone, Debug, PartialEq)]
pub enum GovernedBacktestEvidence<T> {
    /// Retained, validated evidence.
    Available(T),
    /// Evidence cannot be established from this governed terminal.
    Unavailable(GovernedBacktestEvidenceGap),
}

/// Current successful artifact identity without ambient filesystem authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernedBacktestArtifactEvidence {
    /// Path-free governed report identity readable through the confined report authority.
    GovernedReport(GovernedBacktestReportReference),
}

/// Exact retained diagnostics from a completed governed cohort evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct GovernedBacktestCohortEvaluationEvidence {
    evaluation_id: EvidenceDigest,
    trial_count: u64,
    probability_of_backtest_overfitting: f64,
    fold_count: u64,
    deflated_performance_probability: f64,
    expected_maximum_sharpe: f64,
}

impl GovernedBacktestCohortEvaluationEvidence {
    /// Content-addressed cohort-evaluation identity.
    #[must_use]
    pub const fn evaluation_id(&self) -> EvidenceDigest {
        self.evaluation_id
    }

    /// Exact number of unique completed trial members bound into the cohort evaluation.
    #[must_use]
    pub const fn trial_count(&self) -> u64 {
        self.trial_count
    }

    /// Declared probability of backtest overfitting; it is not profit confidence.
    #[must_use]
    pub const fn probability_of_backtest_overfitting(&self) -> f64 {
        self.probability_of_backtest_overfitting
    }

    /// Exact retained cross-validation fold count.
    #[must_use]
    pub const fn fold_count(&self) -> u64 {
        self.fold_count
    }

    /// Declared deflated-performance probability; it is not profit confidence.
    #[must_use]
    pub const fn deflated_performance_probability(&self) -> f64 {
        self.deflated_performance_probability
    }

    /// Declared expected maximum Sharpe diagnostic.
    #[must_use]
    pub const fn expected_maximum_sharpe(&self) -> f64 {
        self.expected_maximum_sharpe
    }
}

/// Explicit cohort-diagnostic state retained by the current terminal.
#[derive(Clone, Debug, PartialEq)]
pub enum GovernedBacktestCohortDiagnosticsEvidence {
    /// No governed cohort comparison was evaluated for this run.
    NotEvaluated,
    /// A complete governed cohort evaluation was retained.
    Completed(GovernedBacktestCohortEvaluationEvidence),
}

/// Revalidated successful terminal evidence for recommendation policy consumption.
#[derive(Clone, Debug, PartialEq)]
pub struct GovernedBacktestSuccessfulTerminalEvidence {
    result_digest: EvidenceDigest,
    artifact: GovernedBacktestArtifactEvidence,
    metrics: Box<[TrialMetric]>,
    dataset_partition: TrialDatasetPartition,
    fill_count: u64,
    partial_fill_count: u64,
    no_action_count: u64,
    reporting_currency: Currency,
    execution_assumptions: ResearchExecutionAssumptions,
    cohort_diagnostics: GovernedBacktestCohortDiagnosticsEvidence,
}

impl GovernedBacktestSuccessfulTerminalEvidence {
    /// Exact successful result identity.
    #[must_use]
    pub const fn result_digest(&self) -> EvidenceDigest {
        self.result_digest
    }

    /// Exact current artifact identity.
    #[must_use]
    pub const fn artifact(&self) -> &GovernedBacktestArtifactEvidence {
        &self.artifact
    }

    /// Declared finite metrics under their exact producer-owned names.
    ///
    /// This collection assigns no recommendation or profit-confidence meaning to any metric.
    #[must_use]
    pub fn metrics(&self) -> &[TrialMetric] {
        &self.metrics
    }

    /// Exact event-time dataset partition retained by every current successful terminal.
    #[must_use]
    pub const fn dataset_partition(&self) -> TrialDatasetPartition {
        self.dataset_partition
    }

    /// Count of simulated fills in the completed research run.
    #[must_use]
    pub const fn fill_count(&self) -> u64 {
        self.fill_count
    }

    /// Count of partial simulated fills.
    #[must_use]
    pub const fn partial_fill_count(&self) -> u64 {
        self.partial_fill_count
    }

    /// Count of deterministic no-action decisions in the completed research run.
    #[must_use]
    pub const fn no_action_count(&self) -> u64 {
        self.no_action_count
    }

    /// Exact reporting currency of the completed run portfolio.
    #[must_use]
    pub const fn reporting_currency(&self) -> Currency {
        self.reporting_currency
    }

    /// Detailed research-only cost/fill assumptions.
    #[must_use]
    pub const fn execution_assumptions(&self) -> &ResearchExecutionAssumptions {
        &self.execution_assumptions
    }

    /// Retained cohort diagnostics or a known not-evaluated state.
    #[must_use]
    pub const fn cohort_diagnostics(&self) -> &GovernedBacktestCohortDiagnosticsEvidence {
        &self.cohort_diagnostics
    }
}

/// Page-bound, revalidated research evidence for one recommendation candidate.
///
/// Exact metric values remain producer declarations. This projection contains no inferred latest
/// time, profit probability, recommendation semantics, or live-execution authority.
#[derive(Clone, Debug, PartialEq)]
pub struct GovernedBacktestRecommendationEvidence {
    record_version: u16,
    selection_query: GovernedBacktestDiscoveryQuery,
    selection_digest: EvidenceDigest,
    completeness: GovernedBacktestDiscoveryCompleteness,
    run_id: EvidenceDigest,
    command_digest: EvidenceDigest,
    record_digest: EvidenceDigest,
    command: GovernedBacktestCommand,
    dataset_identity: EvidenceDigest,
    object_graph_digest: EvidenceDigest,
    execution_assumption_digest: EvidenceDigest,
    cohort_authority_digest: EvidenceDigest,
    cohort_universe_digest: GovernedBacktestEvidence<EvidenceDigest>,
    seed: u64,
    selection_criterion: SourceIdentifier,
    terminal: GovernedBacktestSuccessfulTerminalEvidence,
}

impl GovernedBacktestRecommendationEvidence {
    /// Retained governed-terminal schema version.
    #[must_use]
    pub const fn record_version(&self) -> u16 {
        self.record_version
    }

    /// Exact instrument/optional-strategy selection and admitted result bound.
    #[must_use]
    pub const fn selection_query(&self) -> &GovernedBacktestDiscoveryQuery {
        &self.selection_query
    }

    /// Exact page-selection receipt identity.
    #[must_use]
    pub const fn selection_digest(&self) -> EvidenceDigest {
        self.selection_digest
    }

    /// Explicit complete-or-truncated state of the source selection.
    #[must_use]
    pub const fn completeness(&self) -> GovernedBacktestDiscoveryCompleteness {
        self.completeness
    }

    /// Exact governed run identity.
    #[must_use]
    pub const fn run_id(&self) -> EvidenceDigest {
        self.run_id
    }

    /// Exact retained command identity.
    #[must_use]
    pub const fn command_digest(&self) -> EvidenceDigest {
        self.command_digest
    }

    /// Exact retained terminal-record identity.
    #[must_use]
    pub const fn record_digest(&self) -> EvidenceDigest {
        self.record_digest
    }

    /// Exact registered strategy/input identity and complete instrument/source/PIT scope.
    #[must_use]
    pub const fn command(&self) -> &GovernedBacktestCommand {
        &self.command
    }

    /// Exact point-in-time dataset identity consumed by the experiment.
    #[must_use]
    pub const fn dataset_identity(&self) -> EvidenceDigest {
        self.dataset_identity
    }

    /// Exact retained experiment object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> EvidenceDigest {
        self.object_graph_digest
    }

    /// Exact research cost/fill-assumption identity.
    #[must_use]
    pub const fn execution_assumption_digest(&self) -> EvidenceDigest {
        self.execution_assumption_digest
    }

    /// Exact governed cohort authority identity.
    #[must_use]
    pub const fn cohort_authority_digest(&self) -> EvidenceDigest {
        self.cohort_authority_digest
    }

    /// Governed cohort universe identity, or an explicit not-reported gap.
    #[must_use]
    pub const fn cohort_universe_digest(&self) -> &GovernedBacktestEvidence<EvidenceDigest> {
        &self.cohort_universe_digest
    }

    /// Exact deterministic experiment seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Exact producer-owned selection criterion name.
    #[must_use]
    pub const fn selection_criterion(&self) -> &SourceIdentifier {
        &self.selection_criterion
    }

    /// Revalidated successful terminal evidence.
    #[must_use]
    pub const fn terminal(&self) -> &GovernedBacktestSuccessfulTerminalEvidence {
        &self.terminal
    }
}

fn project_backtest_recommendation_evidence(
    page: &GovernedBacktestDiscoveryPage,
    entry: &GovernedBacktestDiscoveryEntry,
) -> Result<GovernedBacktestRecommendationEvidence, ServiceError> {
    let expected_selection_digest =
        backtest_discovery_selection_digest(&page.query, page.completeness, &page.entries)?;
    if expected_selection_digest != page.selection_digest {
        return Err(ServiceError::InvalidResult);
    }
    let retained = page
        .entries
        .binary_search_by(|candidate| candidate.record().run_id().cmp(entry.record().run_id()))
        .ok()
        .and_then(|position| page.entries.get(position))
        .ok_or(ServiceError::InvalidRequest)?;
    if retained.command() != entry.command()
        || retained.command_digest() != entry.command_digest()
        || retained.record_digest() != entry.record_digest()
        || retained.record().content() != entry.record().content()
    {
        return Err(ServiceError::InvalidRequest);
    }

    let command_digest = parse_sha256_digest(entry.command_digest())?;
    if entry.command().evidence_digest()? != command_digest {
        return Err(ServiceError::InvalidResult);
    }
    let record = GovernedBacktestRecord::try_from_persisted(entry.record().content().clone())?;
    let record_digest = parse_sha256_digest(entry.record_digest())?;
    if record.run_id() != entry.record().run_id() || record.evidence_digest()? != record_digest {
        return Err(ServiceError::InvalidResult);
    }

    let object = record
        .content()
        .as_object()
        .ok_or(ServiceError::InvalidResult)?;
    let record_version = object
        .get("recordVersion")
        .and_then(Value::as_u64)
        .and_then(|version| u16::try_from(version).ok())
        .ok_or(ServiceError::InvalidResult)?;
    let run_id = required_record_digest(object, "runId")?;
    let dataset_identity = required_record_digest(object, "datasetIdentity")?;
    let object_graph_digest = required_record_digest(object, "objectGraphDigest")?;
    let execution_assumption_digest = required_record_digest(object, "executionAssumptionDigest")?;
    let cohort_authority_digest = required_record_digest(object, "cohortAuthorityDigest")?;
    let cohort_universe_digest = optional_record_digest(object, "cohortUniverseDigest")?;
    let seed = object
        .get("seed")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::InvalidResult)?;
    let selection_criterion = object
        .get("selectionCriterion")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)
        .and_then(|value| {
            SourceIdentifier::try_from(value).map_err(|_| ServiceError::InvalidResult)
        })?;
    let status = object
        .get("status")
        .and_then(Value::as_object)
        .filter(|status| status.get("state").and_then(Value::as_str) == Some("completed"))
        .ok_or(ServiceError::InvalidResult)?;
    let terminal = parse_successful_terminal_evidence(
        status,
        execution_assumption_digest,
        &cohort_universe_digest,
    )?;

    Ok(GovernedBacktestRecommendationEvidence {
        record_version,
        selection_query: page.query.clone(),
        selection_digest: page.selection_digest,
        completeness: page.completeness,
        run_id,
        command_digest,
        record_digest,
        command: entry.command().clone(),
        dataset_identity,
        object_graph_digest,
        execution_assumption_digest,
        cohort_authority_digest,
        cohort_universe_digest,
        seed,
        selection_criterion,
        terminal,
    })
}

fn parse_successful_terminal_evidence(
    status: &Map<String, Value>,
    execution_assumption_digest: EvidenceDigest,
    cohort_universe_digest: &GovernedBacktestEvidence<EvidenceDigest>,
) -> Result<GovernedBacktestSuccessfulTerminalEvidence, ServiceError> {
    let result_digest = required_record_digest(status, "resultDigest")?;
    let artifact = parse_artifact_evidence(status)?;
    let metrics = parse_declared_metrics(status)?;
    let dataset_partition = parse_dataset_partition_evidence(status)?;
    let fill_count = status
        .get("fillCount")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::InvalidResult)?;
    let no_action_count = status
        .get("noActionCount")
        .and_then(Value::as_u64)
        .ok_or(ServiceError::InvalidResult)?;
    let partial_fill_count = status
        .get("partialFillCount")
        .and_then(Value::as_u64)
        .filter(|count| *count <= fill_count)
        .ok_or(ServiceError::InvalidResult)?;
    let reporting_currency = parse_reporting_currency(status)?;
    let execution_assumptions =
        parse_execution_assumptions_evidence(status, execution_assumption_digest)?;
    let cohort_diagnostics = parse_cohort_diagnostics_evidence(status, cohort_universe_digest)?;
    Ok(GovernedBacktestSuccessfulTerminalEvidence {
        result_digest,
        artifact,
        metrics,
        dataset_partition,
        fill_count,
        partial_fill_count,
        no_action_count,
        reporting_currency,
        execution_assumptions,
        cohort_diagnostics,
    })
}

fn parse_artifact_evidence(
    status: &Map<String, Value>,
) -> Result<GovernedBacktestArtifactEvidence, ServiceError> {
    let artifact = status
        .get("artifact")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidResult)?;
    let report = GovernedBacktestReportReference::try_new(
        artifact
            .get("artifactId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidResult)?,
        artifact
            .get("sha256")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidResult)?,
        artifact
            .get("byteCount")
            .and_then(Value::as_u64)
            .ok_or(ServiceError::InvalidResult)?,
        artifact
            .get("mediaType")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidResult)?,
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    Ok(GovernedBacktestArtifactEvidence::GovernedReport(report))
}

fn parse_declared_metrics(status: &Map<String, Value>) -> Result<Box<[TrialMetric]>, ServiceError> {
    let declared = status
        .get("metrics")
        .and_then(Value::as_array)
        .ok_or(ServiceError::InvalidResult)?;
    if declared.len() > MAXIMUM_BACKTEST_RECORD_METRICS {
        return Err(ServiceError::InvalidResult);
    }
    let mut metrics = Vec::new();
    metrics
        .try_reserve_exact(declared.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for metric in declared {
        let metric = metric.as_object().ok_or(ServiceError::InvalidResult)?;
        let name = metric
            .get("name")
            .and_then(Value::as_str)
            .ok_or(ServiceError::InvalidResult)
            .and_then(|name| {
                SourceIdentifier::try_from(name).map_err(|_| ServiceError::InvalidResult)
            })?;
        let value = metric
            .get("value")
            .and_then(Value::as_f64)
            .filter(|value| value.is_finite())
            .ok_or(ServiceError::InvalidResult)?;
        metrics.push(TrialMetric::try_new(name, value).map_err(|_| ServiceError::InvalidResult)?);
    }
    if metrics
        .windows(2)
        .any(|pair| pair[0].name() >= pair[1].name())
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(metrics.into_boxed_slice())
}

fn parse_dataset_partition_evidence(
    status: &Map<String, Value>,
) -> Result<TrialDatasetPartition, ServiceError> {
    let value = status
        .get("datasetPartition")
        .ok_or(ServiceError::InvalidResult)?;
    let partition = value.as_object().ok_or(ServiceError::InvalidResult)?;
    let starts_at = partition
        .get("startsAtUnixNanos")
        .and_then(Value::as_i64)
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidResult)?;
    let ends_at = partition
        .get("endsAtUnixNanos")
        .and_then(Value::as_i64)
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidResult)?;
    TrialDatasetPartition::try_new(starts_at, ends_at).map_err(|_| ServiceError::InvalidResult)
}

fn parse_reporting_currency(status: &Map<String, Value>) -> Result<Currency, ServiceError> {
    let retained = status
        .get("reportingCurrency")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)?;
    let currency = Currency::try_from(retained).map_err(|_| ServiceError::InvalidResult)?;
    if currency.as_str() != retained {
        return Err(ServiceError::InvalidResult);
    }
    Ok(currency)
}

fn parse_execution_assumptions_evidence(
    status: &Map<String, Value>,
    expected_digest: EvidenceDigest,
) -> Result<ResearchExecutionAssumptions, ServiceError> {
    let assumptions = status
        .get("executionAssumptions")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidResult)?;
    if assumptions.get("spreadModel").and_then(Value::as_str)
        != Some("observed-point-in-time-half-spread")
        || assumptions.get("liquidityPriority").and_then(Value::as_str)
            != Some("signal-time-then-order-id")
    {
        return Err(ServiceError::InvalidResult);
    }
    let basis_points = |field: &str| -> Result<BasisPoints, ServiceError> {
        assumptions
            .get(field)
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .map(BasisPoints::new)
            .ok_or(ServiceError::InvalidResult)
    };
    let reconstructed = ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
        version: assumptions
            .get("policyVersion")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(ServiceError::InvalidResult)?,
        fee_basis_points: basis_points("feeBasisPoints")?,
        slippage_basis_points: basis_points("slippageBasisPoints")?,
        maximum_random_slippage_basis_points: basis_points("maximumRandomSlippageBasisPoints")?,
        maximum_participation_basis_points: basis_points("maximumParticipationBasisPoints")?,
        liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
        latency_nanos: assumptions
            .get("latencyNanos")
            .and_then(Value::as_i64)
            .ok_or(ServiceError::InvalidResult)?,
        allow_partial_fills: assumptions
            .get("partialFillsAllowed")
            .and_then(Value::as_bool)
            .ok_or(ServiceError::InvalidResult)?,
        fee_decimal_scale: assumptions
            .get("feeDecimalScale")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(ServiceError::InvalidResult)?,
    })
    .map_err(|_| ServiceError::InvalidResult)?;
    if reconstructed.digest().bytes() != expected_digest.bytes() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(reconstructed)
}

fn parse_cohort_diagnostics_evidence(
    status: &Map<String, Value>,
    cohort_universe_digest: &GovernedBacktestEvidence<EvidenceDigest>,
) -> Result<GovernedBacktestCohortDiagnosticsEvidence, ServiceError> {
    let diagnostics = status
        .get("cohortDiagnostics")
        .and_then(Value::as_object)
        .ok_or(ServiceError::InvalidResult)?;
    match diagnostics.get("state").and_then(Value::as_str) {
        Some("not-evaluated") => Ok(GovernedBacktestCohortDiagnosticsEvidence::NotEvaluated),
        Some("completed") => {
            if !matches!(
                cohort_universe_digest,
                GovernedBacktestEvidence::Available(_)
            ) {
                return Err(ServiceError::InvalidResult);
            }
            let probability_of_backtest_overfitting = diagnostics
                .get("probabilityOfBacktestOverfitting")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .ok_or(ServiceError::InvalidResult)?;
            let deflated_performance_probability = diagnostics
                .get("deflatedPerformanceProbability")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite() && (0.0..=1.0).contains(value))
                .ok_or(ServiceError::InvalidResult)?;
            let expected_maximum_sharpe = diagnostics
                .get("expectedMaximumSharpe")
                .and_then(Value::as_f64)
                .filter(|value| value.is_finite())
                .ok_or(ServiceError::InvalidResult)?;
            Ok(GovernedBacktestCohortDiagnosticsEvidence::Completed(
                GovernedBacktestCohortEvaluationEvidence {
                    evaluation_id: required_record_digest(diagnostics, "evaluationId")?,
                    trial_count: diagnostics
                        .get("trialCount")
                        .and_then(Value::as_u64)
                        .filter(|count| *count >= 2)
                        .ok_or(ServiceError::InvalidResult)?,
                    probability_of_backtest_overfitting,
                    fold_count: diagnostics
                        .get("foldCount")
                        .and_then(Value::as_u64)
                        .filter(|count| *count >= 2)
                        .ok_or(ServiceError::InvalidResult)?,
                    deflated_performance_probability,
                    expected_maximum_sharpe,
                },
            ))
        }
        _ => Err(ServiceError::InvalidResult),
    }
}

fn required_record_digest(
    object: &Map<String, Value>,
    field: &str,
) -> Result<EvidenceDigest, ServiceError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)
        .and_then(parse_sha256_digest)
}

fn optional_record_digest(
    object: &Map<String, Value>,
    field: &str,
) -> Result<GovernedBacktestEvidence<EvidenceDigest>, ServiceError> {
    let value = object.get(field).ok_or(ServiceError::InvalidResult)?;
    if value.is_null() {
        return Ok(GovernedBacktestEvidence::Unavailable(
            GovernedBacktestEvidenceGap::NotReported,
        ));
    }
    value
        .as_str()
        .ok_or(ServiceError::InvalidResult)
        .and_then(parse_sha256_digest)
        .map(GovernedBacktestEvidence::Available)
}

fn parse_sha256_digest(value: &str) -> Result<EvidenceDigest, ServiceError> {
    decode_digest(value)
        .map(|bytes| EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .ok_or(ServiceError::InvalidResult)
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

    /// Discovers completed terminals whose retained command includes one exact instrument.
    async fn discover_completed(
        &self,
        query: GovernedBacktestDiscoveryQuery,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestDiscoveryPage, ServiceError>;

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

    /// Discovers completed exact-instrument runs from the durable governed index.
    ///
    /// The default preserves older test-only authorities while failing closed for callers.
    async fn discover_completed(
        &self,
        _query: GovernedBacktestDiscoveryQuery,
        _cancellation: CancellationToken,
        _deadline: Instant,
    ) -> Result<GovernedBacktestDiscoveryPage, ServiceError> {
        Err(ServiceError::Unavailable)
    }

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

    async fn discover_completed(
        &self,
        query: GovernedBacktestDiscoveryQuery,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<GovernedBacktestDiscoveryPage, ServiceError> {
        self.ensure_live(&cancellation, deadline)?;
        let linked = LinkedCancellation::new(cancellation, self.shutdown.child_token(), deadline);
        let result = self
            .repository
            .discover_completed(query, linked.token().clone(), deadline)
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
            "partialFillCount",
            "noActionCount",
            "reportingCurrency",
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
        || parse_reporting_currency(status).is_err()
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
                    "trialCount",
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
                    .get("trialCount")
                    .and_then(Value::as_u64)
                    .is_some_and(|value| value >= 2)
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
    let trial_count =
        u64::try_from(cohort.members().len()).map_err(|_| ServiceError::ResourceExhausted)?;
    if !pbo.probability().is_finite()
        || !deflated.probability().is_finite()
        || !deflated.expected_maximum_sharpe().is_finite()
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(json!({
        "state": "completed",
        "evaluationId": encode_hex(cohort.id().digest().bytes()),
        "trialCount": trial_count,
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

    use super::{
        BasisPoints, GovernedBacktestRecord, ResearchExecutionAssumptions,
        ResearchExecutionAssumptionsInput, ResearchLiquidityPriority, encode_hex,
        execution_assumptions_content,
    };

    fn execution_assumptions() -> ResearchExecutionAssumptions {
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
        .expect("fixed research assumptions should be valid")
    }

    #[test]
    fn governed_current_record_binds_visible_execution_and_report_evidence() {
        let digest = |byte: &str| byte.repeat(32);
        let assumptions = execution_assumptions();
        let mut content = json!({
            "recordVersion": 2,
            "runId": digest("11"),
            "datasetIdentity": digest("22"),
            "objectGraphDigest": digest("33"),
            "executionAssumptionDigest": encode_hex(assumptions.digest().bytes()),
            "cohortAuthorityDigest": digest("77"),
            "cohortUniverseDigest": null,
            "seed": 7,
            "selectionCriterion": "cost-adjusted-total-return",
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
                "reportingCurrency": "USD",
                "accountingReconciliation": "independent",
                "executionAssumptions": execution_assumptions_content(assumptions),
                "cohortDiagnostics": {"state": "not-evaluated"}
            }
        });
        assert!(GovernedBacktestRecord::try_from_persisted(content.clone()).is_ok());

        let mut inconsistent_assumptions = content.clone();
        inconsistent_assumptions["executionAssumptionDigest"] = json!(digest("44"));
        assert!(GovernedBacktestRecord::try_from_persisted(inconsistent_assumptions).is_err());

        content["status"]["partialFillCount"] = json!(3);
        assert!(GovernedBacktestRecord::try_from_persisted(content).is_err());
    }

    #[test]
    fn governed_current_record_accepts_only_bound_completed_cohort_diagnostics() {
        let digest = |byte: &str| byte.repeat(32);
        let assumptions = execution_assumptions();
        let record = GovernedBacktestRecord::try_from_persisted(json!({
            "recordVersion": 2,
            "runId": digest("11"),
            "datasetIdentity": digest("22"),
            "objectGraphDigest": digest("33"),
            "executionAssumptionDigest": encode_hex(assumptions.digest().bytes()),
            "cohortAuthorityDigest": digest("55"),
            "cohortUniverseDigest": digest("66"),
            "seed": 7,
            "selectionCriterion": "cost-adjusted-total-return",
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
                "reportingCurrency": "USD",
                "accountingReconciliation": "independent",
                "executionAssumptions": execution_assumptions_content(assumptions),
                "cohortDiagnostics": {
                    "state": "completed",
                    "evaluationId": digest("99"),
                    "trialCount": 12,
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
