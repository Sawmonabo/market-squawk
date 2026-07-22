//! Immutable trial specification and terminal record model.

use market_squawk_data::Sha256Digest;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use sha2::{Digest as _, Sha256};

use super::ExperimentError;

const HARD_MAX_TRIALS: usize = 1_000_000;
const HARD_MAX_RECORD_BYTES: usize = 4 * 1024 * 1024;
const HARD_MAX_ARTIFACT_BYTES: usize = 512 * 1024 * 1024;
const HARD_MAX_METRICS: usize = 4_096;
const HARD_MAX_PARAMETERS: usize = 1_024;
const HARD_MAX_SEARCH_DIMENSIONS: usize = 1_024;
const HARD_MAX_CANDIDATES_PER_DIMENSION: usize = 16_384;

/// Stable identity and content hash for one model, strategy, or code artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrialComponentBinding {
    pub(super) name: SourceIdentifier,
    pub(super) digest: Sha256Digest,
}

impl TrialComponentBinding {
    /// Constructs a nonzero immutable component binding.
    pub fn try_new(name: SourceIdentifier, digest: Sha256Digest) -> Result<Self, ExperimentError> {
        require_digest(digest)?;
        Ok(Self { name, digest })
    }

    /// Returns the stable component name.
    #[must_use]
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the exact component bytes identity.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }
}

/// Immutable model, strategy, code, and configuration identity owned by an executable strategy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestExecutableIdentity {
    model: Option<TrialComponentBinding>,
    strategy: TrialComponentBinding,
    code: TrialComponentBinding,
    configuration_digest: Sha256Digest,
}

impl BacktestExecutableIdentity {
    /// Binds every executable component before the strategy can enter a governed run.
    pub(crate) fn try_new(
        model: Option<TrialComponentBinding>,
        strategy: TrialComponentBinding,
        code: TrialComponentBinding,
        configuration_digest: Sha256Digest,
    ) -> Result<Self, ExperimentError> {
        require_digest(configuration_digest)?;
        Ok(Self {
            model,
            strategy,
            code,
            configuration_digest,
        })
    }

    /// Returns the actual admitted model generation, when this strategy performs inference.
    #[must_use]
    pub const fn model(&self) -> Option<&TrialComponentBinding> {
        self.model.as_ref()
    }

    /// Returns the executable strategy implementation identity.
    #[must_use]
    pub const fn strategy(&self) -> &TrialComponentBinding {
        &self.strategy
    }

    /// Returns the compiled code revision identity.
    #[must_use]
    pub const fn code(&self) -> &TrialComponentBinding {
        &self.code
    }

    /// Returns the exact strategy configuration identity.
    #[must_use]
    pub const fn configuration_digest(&self) -> Sha256Digest {
        self.configuration_digest
    }
}

/// One selected value in a trial's immutable parameter vector.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TrialParameter {
    pub(super) name: SourceIdentifier,
    pub(super) value: SourceIdentifier,
}

impl TrialParameter {
    /// Constructs one bounded canonical name/value pair.
    #[must_use]
    pub const fn new(name: SourceIdentifier, value: SourceIdentifier) -> Self {
        Self { name, value }
    }

    /// Returns the parameter name.
    #[must_use]
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the selected canonical value.
    #[must_use]
    pub const fn value(&self) -> &SourceIdentifier {
        &self.value
    }
}

/// One bounded discrete dimension of the declared experiment search space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrialSearchDimension {
    pub(super) name: SourceIdentifier,
    pub(super) candidates: Box<[SourceIdentifier]>,
}

impl TrialSearchDimension {
    /// Constructs a nonempty, duplicate-free canonical candidate set.
    pub fn try_new(
        name: SourceIdentifier,
        mut candidates: Vec<SourceIdentifier>,
    ) -> Result<Self, ExperimentError> {
        if candidates.is_empty() || candidates.len() > HARD_MAX_CANDIDATES_PER_DIMENSION {
            return Err(ExperimentError::InvalidSpec);
        }
        candidates.sort_unstable();
        if candidates.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ExperimentError::InvalidSpec);
        }
        Ok(Self {
            name,
            candidates: candidates.into_boxed_slice(),
        })
    }

    /// Returns the parameter name.
    #[must_use]
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the complete canonical candidate set.
    #[must_use]
    pub fn candidates(&self) -> &[SourceIdentifier] {
        &self.candidates
    }
}

/// Untrusted complete trial specification.
#[derive(Clone, Debug)]
pub struct TrialSpecInput {
    pub dataset_identity: Sha256Digest,
    pub object_graph_digest: Sha256Digest,
    pub execution_assumption_digest: Sha256Digest,
    pub model: Option<TrialComponentBinding>,
    pub strategy: TrialComponentBinding,
    pub code: TrialComponentBinding,
    pub configuration_digest: Sha256Digest,
    pub seed: u64,
    pub parameters: Vec<TrialParameter>,
    pub search_space: Vec<TrialSearchDimension>,
    pub selection_criterion: SourceIdentifier,
}

/// Immutable data/model/strategy/code/configuration/search binding for one trial.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrialSpec {
    pub(super) dataset_identity: Sha256Digest,
    pub(super) object_graph_digest: Sha256Digest,
    pub(super) execution_assumption_digest: Sha256Digest,
    pub(super) model: Option<TrialComponentBinding>,
    pub(super) strategy: TrialComponentBinding,
    pub(super) code: TrialComponentBinding,
    pub(super) configuration_digest: Sha256Digest,
    pub(super) seed: u64,
    pub(super) parameters: Box<[TrialParameter]>,
    pub(super) search_space: Box<[TrialSearchDimension]>,
    pub(super) selection_criterion: SourceIdentifier,
    pub(super) identity: TrialId,
}

impl TrialSpec {
    /// Validates every immutable input and computes the collision-resistant trial identity.
    pub fn try_new(mut input: TrialSpecInput) -> Result<Self, ExperimentError> {
        for digest in [
            input.dataset_identity,
            input.object_graph_digest,
            input.execution_assumption_digest,
            input.strategy.digest,
            input.code.digest,
            input.configuration_digest,
        ] {
            require_digest(digest)?;
        }
        if let Some(model) = &input.model {
            require_digest(model.digest)?;
        }
        if input.parameters.len() > HARD_MAX_PARAMETERS
            || input.search_space.len() > HARD_MAX_SEARCH_DIMENSIONS
        {
            return Err(ExperimentError::InvalidSpec);
        }
        input.parameters.sort_unstable();
        if input
            .parameters
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ExperimentError::InvalidSpec);
        }
        input
            .search_space
            .sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if input
            .search_space
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ExperimentError::InvalidSpec);
        }
        for parameter in &input.parameters {
            let dimension = input
                .search_space
                .binary_search_by(|candidate| candidate.name.cmp(&parameter.name))
                .ok()
                .and_then(|index| input.search_space.get(index))
                .ok_or(ExperimentError::InvalidSpec)?;
            if dimension
                .candidates
                .binary_search(&parameter.value)
                .is_err()
            {
                return Err(ExperimentError::InvalidSpec);
            }
        }
        let mut spec = Self {
            dataset_identity: input.dataset_identity,
            object_graph_digest: input.object_graph_digest,
            execution_assumption_digest: input.execution_assumption_digest,
            model: input.model,
            strategy: input.strategy,
            code: input.code,
            configuration_digest: input.configuration_digest,
            seed: input.seed,
            parameters: input.parameters.into_boxed_slice(),
            search_space: input.search_space.into_boxed_slice(),
            selection_criterion: input.selection_criterion,
            identity: TrialId(Sha256Digest::new([0; 32])),
        };
        spec.identity = TrialId(spec_identity(&spec)?);
        Ok(spec)
    }

    /// Returns the deterministic identity of every immutable trial input.
    #[must_use]
    pub const fn id(&self) -> TrialId {
        self.identity
    }

    /// Returns the exact PIT dataset identity.
    #[must_use]
    pub const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset_identity
    }

    /// Returns the complete pinned object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> Sha256Digest {
        self.object_graph_digest
    }

    /// Returns the versioned research execution-policy identity.
    #[must_use]
    pub const fn execution_assumption_digest(&self) -> Sha256Digest {
        self.execution_assumption_digest
    }

    /// Returns the deterministic seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the exact selection criterion bound into this trial.
    #[must_use]
    pub const fn selection_criterion(&self) -> &SourceIdentifier {
        &self.selection_criterion
    }

    /// Hashes the declared search space and criterion independently of one selected parameter set.
    pub fn experiment_design_digest(&self) -> Result<Sha256Digest, ExperimentError> {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/backtest-cohort-family/v1");
        hash.update(self.execution_assumption_digest.bytes());
        update_optional_binding(&mut hash, self.model.as_ref())?;
        update_binding(&mut hash, &self.strategy)?;
        update_binding(&mut hash, &self.code)?;
        hash.update(self.configuration_digest.bytes());
        update_length(&mut hash, self.search_space.len())?;
        for dimension in &self.search_space {
            update_bytes(&mut hash, dimension.name.as_str().as_bytes())?;
            update_length(&mut hash, dimension.candidates.len())?;
            for candidate in &dimension.candidates {
                update_bytes(&mut hash, candidate.as_str().as_bytes())?;
            }
        }
        update_bytes(&mut hash, self.selection_criterion.as_str().as_bytes())?;
        Ok(Sha256Digest::new(hash.finalize().into()))
    }

    /// Hashes the candidate's exact parameter vector independently of its data partition.
    pub fn parameter_digest(&self) -> Result<Sha256Digest, ExperimentError> {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/backtest-parameter-vector/v1");
        update_length(&mut hash, self.parameters.len())?;
        for parameter in &self.parameters {
            update_bytes(&mut hash, parameter.name.as_str().as_bytes())?;
            update_bytes(&mut hash, parameter.value.as_str().as_bytes())?;
        }
        Ok(Sha256Digest::new(hash.finalize().into()))
    }
}

/// Content identity of one immutable trial specification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TrialId(pub(super) Sha256Digest);

impl TrialId {
    /// Returns the exact SHA-256 identity.
    #[must_use]
    pub const fn digest(self) -> Sha256Digest {
        self.0
    }
}

/// Caller-selected durable inventory and artifact bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExperimentLimitsInput {
    pub max_trials: usize,
    pub max_record_bytes: usize,
    pub max_artifact_bytes: usize,
    pub max_metrics: usize,
}

/// Validated durable experiment ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExperimentLimits {
    pub(super) max_trials: usize,
    pub(super) max_record_bytes: usize,
    pub(super) max_artifact_bytes: usize,
    pub(super) max_metrics: usize,
}

impl ExperimentLimits {
    /// Validates positive caller limits against process hard ceilings.
    pub fn try_new(input: ExperimentLimitsInput) -> Result<Self, ExperimentError> {
        if input.max_trials == 0
            || input.max_trials > HARD_MAX_TRIALS
            || input.max_record_bytes == 0
            || input.max_record_bytes > HARD_MAX_RECORD_BYTES
            || input.max_artifact_bytes == 0
            || input.max_artifact_bytes > HARD_MAX_ARTIFACT_BYTES
            || input.max_metrics == 0
            || input.max_metrics > HARD_MAX_METRICS
        {
            return Err(ExperimentError::InvalidLimits);
        }
        Ok(Self {
            max_trials: input.max_trials,
            max_record_bytes: input.max_record_bytes,
            max_artifact_bytes: input.max_artifact_bytes,
            max_metrics: input.max_metrics,
        })
    }

    pub(crate) const fn max_artifact_bytes(self) -> usize {
        self.max_artifact_bytes
    }
}

/// One finite named trial metric.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialMetric {
    pub(super) name: SourceIdentifier,
    pub(super) value: f64,
}

impl TrialMetric {
    /// Constructs one finite metric.
    pub fn try_new(name: SourceIdentifier, value: f64) -> Result<Self, ExperimentError> {
        if !value.is_finite() {
            return Err(ExperimentError::InvalidCompletion);
        }
        Ok(Self { name, value })
    }

    /// Returns the metric name.
    #[must_use]
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the finite metric value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }
}

/// Immutable content-addressed detailed-output reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestArtifact {
    pub(super) reference: Box<str>,
    pub(super) digest: Sha256Digest,
    pub(super) byte_count: u64,
}

/// Exact event-time interval represented by one completed trial dataset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TrialDatasetPartition {
    starts_at: Timestamp,
    ends_at: Timestamp,
}

impl TrialDatasetPartition {
    /// Requires a nonempty exact timestamp interval.
    pub fn try_new(starts_at: Timestamp, ends_at: Timestamp) -> Result<Self, ExperimentError> {
        if starts_at >= ends_at {
            return Err(ExperimentError::InvalidCompletion);
        }
        Ok(Self { starts_at, ends_at })
    }

    /// Returns the first admitted decision instant.
    #[must_use]
    pub const fn starts_at(self) -> Timestamp {
        self.starts_at
    }

    /// Returns the last admitted decision instant.
    #[must_use]
    pub const fn ends_at(self) -> Timestamp {
        self.ends_at
    }
}

impl BacktestArtifact {
    /// Returns the capability-relative controlled reference.
    #[must_use]
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Returns the exact artifact digest.
    #[must_use]
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the durable byte count.
    #[must_use]
    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }
}

/// Validated successful terminal trial evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialCompletion {
    pub(super) result_digest: Sha256Digest,
    pub(super) artifact: BacktestArtifact,
    pub(super) metrics: Box<[TrialMetric]>,
    pub(super) dataset_partition: Option<TrialDatasetPartition>,
}

/// Untrusted successful terminal trial input.
#[derive(Clone, Debug)]
pub(crate) struct TrialCompletionInput {
    pub(crate) result_digest: Sha256Digest,
    pub(crate) artifact: BacktestArtifact,
    pub(crate) metrics: Vec<TrialMetric>,
    pub(crate) dataset_partition: Option<TrialDatasetPartition>,
}

impl TrialCompletion {
    pub(super) fn try_new(
        mut input: TrialCompletionInput,
        limits: ExperimentLimits,
    ) -> Result<Self, ExperimentError> {
        require_digest(input.result_digest)?;
        if input.metrics.len() > limits.max_metrics {
            return Err(ExperimentError::InvalidCompletion);
        }
        input
            .metrics
            .sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if input
            .metrics
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(ExperimentError::InvalidCompletion);
        }
        Ok(Self {
            result_digest: input.result_digest,
            artifact: input.artifact,
            metrics: input.metrics.into_boxed_slice(),
            dataset_partition: input.dataset_partition,
        })
    }

    /// Returns the complete deterministic backtest result identity.
    #[must_use]
    pub const fn result_digest(&self) -> Sha256Digest {
        self.result_digest
    }

    /// Returns the bounded detailed-output artifact.
    #[must_use]
    pub const fn artifact(&self) -> &BacktestArtifact {
        &self.artifact
    }

    /// Returns canonical metrics computed by the backtest service after the run completed.
    #[must_use]
    pub fn metrics(&self) -> &[TrialMetric] {
        &self.metrics
    }

    /// Returns the exact dataset interval, or `None` only for migrated schema-v1 terminals.
    #[must_use]
    pub const fn dataset_partition(&self) -> Option<TrialDatasetPartition> {
        self.dataset_partition
    }
}

/// Stable typed terminal failure retained even for losing or invalid trials.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrialFailure {
    pub(super) code: SourceIdentifier,
    pub(super) evidence_digest: Sha256Digest,
}

impl TrialFailure {
    /// Constructs a nonzero failure evidence binding.
    pub fn try_new(
        code: SourceIdentifier,
        evidence_digest: Sha256Digest,
    ) -> Result<Self, ExperimentError> {
        require_digest(evidence_digest)?;
        Ok(Self {
            code,
            evidence_digest,
        })
    }
}

/// Durable trial lifecycle state. Terminal variants are immutable.
#[derive(Clone, Debug, PartialEq)]
pub enum TrialStatus {
    Reserved,
    Completed(TrialCompletion),
    Failed(TrialFailure),
}

/// Complete durable trial record.
#[derive(Clone, Debug, PartialEq)]
pub struct TrialRecord {
    pub(super) spec: TrialSpec,
    pub(super) status: TrialStatus,
}

impl TrialRecord {
    /// Returns the complete immutable trial specification.
    #[must_use]
    pub const fn spec(&self) -> &TrialSpec {
        &self.spec
    }

    /// Returns the current durable lifecycle state.
    #[must_use]
    pub const fn status(&self) -> &TrialStatus {
        &self.status
    }
}

pub(super) fn require_digest(digest: Sha256Digest) -> Result<(), ExperimentError> {
    if digest.bytes() == [0; 32] {
        Err(ExperimentError::InvalidSpec)
    } else {
        Ok(())
    }
}

fn spec_identity(spec: &TrialSpec) -> Result<Sha256Digest, ExperimentError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/experiment-trial/v1");
    hash.update(spec.dataset_identity.bytes());
    hash.update(spec.object_graph_digest.bytes());
    hash.update(spec.execution_assumption_digest.bytes());
    update_optional_binding(&mut hash, spec.model.as_ref())?;
    update_binding(&mut hash, &spec.strategy)?;
    update_binding(&mut hash, &spec.code)?;
    hash.update(spec.configuration_digest.bytes());
    hash.update(spec.seed.to_be_bytes());
    update_length(&mut hash, spec.parameters.len())?;
    for parameter in &spec.parameters {
        update_bytes(&mut hash, parameter.name.as_str().as_bytes())?;
        update_bytes(&mut hash, parameter.value.as_str().as_bytes())?;
    }
    update_length(&mut hash, spec.search_space.len())?;
    for dimension in &spec.search_space {
        update_bytes(&mut hash, dimension.name.as_str().as_bytes())?;
        update_length(&mut hash, dimension.candidates.len())?;
        for candidate in &dimension.candidates {
            update_bytes(&mut hash, candidate.as_str().as_bytes())?;
        }
    }
    update_bytes(&mut hash, spec.selection_criterion.as_str().as_bytes())?;
    Ok(Sha256Digest::new(hash.finalize().into()))
}

fn update_optional_binding(
    hash: &mut Sha256,
    binding: Option<&TrialComponentBinding>,
) -> Result<(), ExperimentError> {
    match binding {
        Some(binding) => {
            hash.update([1]);
            update_binding(hash, binding)
        }
        None => {
            hash.update([0]);
            Ok(())
        }
    }
}

fn update_binding(
    hash: &mut Sha256,
    binding: &TrialComponentBinding,
) -> Result<(), ExperimentError> {
    update_bytes(hash, binding.name.as_str().as_bytes())?;
    hash.update(binding.digest.bytes());
    Ok(())
}

fn update_length(hash: &mut Sha256, length: usize) -> Result<(), ExperimentError> {
    hash.update(
        u64::try_from(length)
            .map_err(|_| ExperimentError::Encoding)?
            .to_be_bytes(),
    );
    Ok(())
}

fn update_bytes(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ExperimentError> {
    update_length(hash, bytes.len())?;
    hash.update(bytes);
    Ok(())
}
