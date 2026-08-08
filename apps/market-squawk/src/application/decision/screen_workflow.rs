//! Service-owned saved-screen preparation over one exact point-in-time feature generation.

use std::{collections::BTreeMap, fmt, num::NonZeroU64, time::Instant};

use market_squawk_analytics::{FeatureRegistry, StatisticalF64};
use market_squawk_data::{
    AnalyticalReadCapability, CatalogEndpointIdentity, DatasetId, DatasetManifestRef,
    DatasetSchemaRef, DatasetSchemaRegistry, ForecastDatasetEvidence, ForecastDatasetReadLimits,
    ForecastFeatureRow, ForecastFeatureValue, Sha256Digest,
};
use market_squawk_decisions::{
    CandidateFlag, CandidateId, CandidateInput, DecisionContentDigest, SavedScreen,
    ScreenFeatureObservation, ScreenId, ScreenRun, ScreenRunId,
};
use market_squawk_domain::{
    DataQuality, DigestAlgorithm, EvidenceDigest, RevisionNumber, SchemaVersion, SourceIdentifier,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::DecisionApplicationError;
use super::codec::candidate::CandidateInputWire;
use super::codec::screen::RunWire;

const MAXIMUM_SCREEN_DATASET_ROWS: usize = 100_000;
const MAXIMUM_SCREEN_DATASET_BYTES: usize = 256 * 1024 * 1024;
const MAXIMUM_SCREEN_OBSERVATIONS: usize = 32_768;
const LIQUIDITY_FEATURE_NAME: &str = "liquidity.available-quantity";
const SCREEN_INPUT_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-job-input/v1\0";
const SCREEN_DATASET_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-dataset-evidence/v1\0";
const SCREEN_CANDIDATE_DIGEST_DOMAIN: &[u8] = b"market-squawk/screen-candidate-evidence/v1\0";
const SCREEN_RUN_ID_DOMAIN: &[u8] = b"market-squawk/screen-run-id/v1\0";

/// Minimal presentation request for a service-owned saved-screen execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScreenJobRequest {
    screen_id: ScreenId,
    screen_revision: RevisionNumber,
    dataset_manifest: DatasetManifestRef,
    as_of: Timestamp,
}

impl ScreenJobRequest {
    /// Selects one retained screen revision and one exact feature-dataset generation.
    #[must_use]
    pub const fn new(
        screen_id: ScreenId,
        screen_revision: RevisionNumber,
        dataset_manifest: DatasetManifestRef,
        as_of: Timestamp,
    ) -> Self {
        Self {
            screen_id,
            screen_revision,
            dataset_manifest,
            as_of,
        }
    }

    pub(super) const fn screen_id(&self) -> &ScreenId {
        &self.screen_id
    }

    pub(super) const fn screen_revision(&self) -> RevisionNumber {
        self.screen_revision
    }

    pub(super) const fn dataset_manifest(&self) -> &DatasetManifestRef {
        &self.dataset_manifest
    }

    pub(super) const fn as_of(&self) -> Timestamp {
        self.as_of
    }
}

/// Durable locator returned only after the complete immutable job input is committed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedScreenJob {
    input_identity: SourceIdentifier,
    input_digest: EvidenceDigest,
    run_id: ScreenRunId,
}

impl AdmittedScreenJob {
    /// Exact durable screen-input identity used by the job authority.
    #[must_use]
    pub const fn input_identity(&self) -> &SourceIdentifier {
        &self.input_identity
    }

    /// Commitment to the complete service-derived screen input.
    #[must_use]
    pub const fn input_digest(&self) -> EvidenceDigest {
        self.input_digest
    }

    /// Exact immutable run that will be published by the decision authority.
    #[must_use]
    pub const fn run_id(&self) -> &ScreenRunId {
        &self.run_id
    }
}

/// Screen preparation or durable-input lookup failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScreenWorkflowError {
    /// The selected screen, cutoff, or supported constraint policy is invalid.
    InvalidRequest,
    /// The retained screen or exact prepared input does not exist.
    NotFound,
    /// The pinned dataset is unavailable, malformed, or does not match the screen universe.
    DatasetUnavailable,
    /// A stable run or input identity names different content.
    Conflict,
    /// A fixed preparation or retained-memory bound was exceeded.
    Capacity,
    /// The durable decision application could not complete the operation.
    Application(DecisionApplicationError),
}

impl fmt::Display for ScreenWorkflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest => formatter.write_str("screen job request is invalid"),
            Self::NotFound => formatter.write_str("screen job input was not found"),
            Self::DatasetUnavailable => {
                formatter.write_str("screen feature dataset is unavailable or inconsistent")
            }
            Self::Conflict => formatter.write_str("screen job input conflicts with retained state"),
            Self::Capacity => formatter.write_str("screen job preparation capacity is exhausted"),
            Self::Application(error) => fmt::Display::fmt(error, formatter),
        }
    }
}

impl std::error::Error for ScreenWorkflowError {}

impl From<DecisionApplicationError> for ScreenWorkflowError {
    fn from(error: DecisionApplicationError) -> Self {
        Self::Application(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ScreenDatasetFence {
    manifest: DatasetManifestRef,
    catalog_identity: CatalogEndpointIdentity,
    export_sha256: Sha256Digest,
    selection_sha256: Sha256Digest,
    selected_rows: NonZeroU64,
    policy_sha256: Sha256Digest,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct ScreenJobPlan {
    run: ScreenRun,
    candidates: Vec<CandidateInput>,
    selected_at: Timestamp,
    dataset: ScreenDatasetFence,
    input_digest: EvidenceDigest,
}

impl ScreenJobPlan {
    pub(super) const fn run(&self) -> &ScreenRun {
        &self.run
    }

    pub(super) const fn input_digest(&self) -> EvidenceDigest {
        self.input_digest
    }

    pub(super) fn into_execution(self) -> (ScreenRun, Vec<CandidateInput>, Timestamp) {
        (self.run, self.candidates, self.selected_at)
    }

    pub(super) fn admitted(&self) -> Result<AdmittedScreenJob, ScreenWorkflowError> {
        Ok(AdmittedScreenJob {
            input_identity: SourceIdentifier::try_from(self.run.id().as_str())
                .map_err(|_error| ScreenWorkflowError::InvalidRequest)?,
            input_digest: self.input_digest,
            run_id: self.run.id().clone(),
        })
    }

    pub(super) fn matches_request(&self, screen: &SavedScreen, request: &ScreenJobRequest) -> bool {
        self.run.screen() == screen.revision()
            && self.run.as_of() == request.as_of()
            && self.dataset.manifest == *request.dataset_manifest()
    }
}

struct LatestFeatureRows<'a> {
    cutoff_at: Timestamp,
    components: BTreeMap<(&'a str, u32), &'a ForecastFeatureRow>,
}

pub(super) async fn prepare(
    screen: &SavedScreen,
    request: &ScreenJobRequest,
    reader: &AnalyticalReadCapability,
    selected_at: Timestamp,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<ScreenJobPlan, ScreenWorkflowError> {
    if screen.revision().id() != request.screen_id()
        || screen.revision().revision() != request.screen_revision()
        || request.as_of() > selected_at
        || (screen.constraints().minimum_liquidity().get() > 0.0
            && screen
                .feature_bindings()
                .iter()
                .filter(|binding| binding.key().name() == LIQUIDITY_FEATURE_NAME)
                .count()
                != 1)
        || !screen
            .constraints()
            .admitted_data_qualities()
            .contains(&DataQuality::Modeled)
    {
        return Err(ScreenWorkflowError::InvalidRequest);
    }
    let limits = ForecastDatasetReadLimits::try_new(
        MAXIMUM_SCREEN_DATASET_ROWS,
        MAXIMUM_SCREEN_DATASET_BYTES,
    )
    .map_err(|_error| ScreenWorkflowError::Capacity)?;
    let evidence = reader
        .forecast_dataset_evidence(
            request.dataset_manifest(),
            request.as_of(),
            limits,
            deadline,
            cancellation,
        )
        .await
        .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?;
    build_plan(screen, evidence, selected_at)
}

fn build_plan(
    screen: &SavedScreen,
    evidence: ForecastDatasetEvidence,
    selected_at: Timestamp,
) -> Result<ScreenJobPlan, ScreenWorkflowError> {
    let universe_identity = content_identity(evidence.dataset().universe_digest().bytes())?;
    if universe_identity != screen.universe_identity() {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    let dataset_identity = dataset_identity(&evidence)?;
    let run_id = expected_run_id(
        screen,
        evidence.fence().manifest(),
        evidence.fence().as_of(),
    )?;
    let run = ScreenRun::try_new(
        run_id,
        screen.revision().clone(),
        evidence.fence().as_of(),
        dataset_identity,
        universe_identity,
        screen.feature_bindings().to_vec(),
    )
    .map_err(|_error| ScreenWorkflowError::InvalidRequest)?;
    let candidates = candidate_inputs(&run, screen, &evidence)?;
    let dataset = ScreenDatasetFence {
        manifest: evidence.fence().manifest().clone(),
        catalog_identity: evidence.fence().catalog_identity(),
        export_sha256: evidence.fence().export_sha256(),
        selection_sha256: evidence.fence().selection_sha256(),
        selected_rows: evidence.fence().selected_rows(),
        policy_sha256: evidence.dataset().policy_digest(),
    };
    let input_digest = plan_digest(&run, &candidates, selected_at, &dataset)?;
    Ok(ScreenJobPlan {
        run,
        candidates,
        selected_at,
        dataset,
        input_digest,
    })
}

pub(super) fn expected_request_run_id(
    screen: &SavedScreen,
    request: &ScreenJobRequest,
) -> Result<ScreenRunId, ScreenWorkflowError> {
    expected_run_id(screen, request.dataset_manifest(), request.as_of())
}

fn expected_run_id(
    screen: &SavedScreen,
    manifest: &DatasetManifestRef,
    as_of: Timestamp,
) -> Result<ScreenRunId, ScreenWorkflowError> {
    let mut hash = Sha256::new();
    hash.update(SCREEN_RUN_ID_DOMAIN);
    hash_bytes(&mut hash, screen.revision().id().as_str().as_bytes())?;
    hash.update(screen.revision().revision().get().to_be_bytes());
    hash_manifest(&mut hash, manifest)?;
    hash.update(as_of.unix_nanos().to_be_bytes());
    let digest: [u8; 32] = hash.finalize().into();
    ScreenRunId::try_new(format!("run.{}", hex(&digest)))
        .map_err(|_error| ScreenWorkflowError::Capacity)
}

fn candidate_inputs(
    run: &ScreenRun,
    screen: &SavedScreen,
    evidence: &ForecastDatasetEvidence,
) -> Result<Vec<CandidateInput>, ScreenWorkflowError> {
    let mut latest = BTreeMap::<_, LatestFeatureRows<'_>>::new();
    for row in evidence.rows() {
        if row.component_kind() != 1 {
            continue;
        }
        if row.component_version() == 0 {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
        let entry = latest
            .entry(row.instrument_id())
            .or_insert_with(|| LatestFeatureRows {
                cutoff_at: row.cutoff_at(),
                components: BTreeMap::new(),
            });
        if row.cutoff_at() > entry.cutoff_at {
            entry.cutoff_at = row.cutoff_at();
            entry.components.clear();
        }
        if row.cutoff_at() == entry.cutoff_at
            && entry
                .components
                .insert((row.component_name(), row.component_version()), row)
                .is_some()
        {
            return Err(ScreenWorkflowError::DatasetUnavailable);
        }
    }
    if latest.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS {
        return Err(ScreenWorkflowError::Capacity);
    }
    if latest.is_empty() {
        return Err(ScreenWorkflowError::DatasetUnavailable);
    }
    if latest
        .len()
        .checked_mul(screen.feature_bindings().len())
        .is_none_or(|observations| observations > MAXIMUM_SCREEN_OBSERVATIONS)
    {
        return Err(ScreenWorkflowError::Capacity);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(latest.len())
        .map_err(|_error| ScreenWorkflowError::Capacity)?;
    for (instrument_id, rows) in latest {
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(screen.feature_bindings().len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?;
        let mut present = 0_usize;
        let mut liquidity = None;
        let mut candidate_hash = Sha256::new();
        candidate_hash.update(SCREEN_CANDIDATE_DIGEST_DOMAIN);
        candidate_hash.update(run.dataset_identity().evidence_digest().bytes());
        candidate_hash.update(instrument_id.as_uuid().as_bytes());
        candidate_hash.update(rows.cutoff_at.unix_nanos().to_be_bytes());
        for binding in screen.feature_bindings() {
            hash_bytes(&mut candidate_hash, binding.key().name().as_bytes())?;
            candidate_hash.update(binding.key().version().get().to_be_bytes());
            candidate_hash.update(binding.semantic_digest().as_bytes());
            let value = match rows
                .components
                .get(&(binding.key().name(), binding.key().version().get()))
            {
                Some(row) => match row.value() {
                    ForecastFeatureValue::Float(value) => {
                        let value = StatisticalF64::try_new(*value)
                            .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?;
                        candidate_hash.update([1]);
                        candidate_hash.update(row.lineage_sha256().bytes());
                        candidate_hash.update(value.get().to_bits().to_be_bytes());
                        present = present
                            .checked_add(1)
                            .ok_or(ScreenWorkflowError::Capacity)?;
                        Some(value)
                    }
                    ForecastFeatureValue::Missing => {
                        candidate_hash.update([0]);
                        candidate_hash.update(row.lineage_sha256().bytes());
                        None
                    }
                    ForecastFeatureValue::Decimal { .. } => {
                        return Err(ScreenWorkflowError::DatasetUnavailable);
                    }
                },
                None => {
                    candidate_hash.update([0]);
                    candidate_hash.update([0; 32]);
                    None
                }
            };
            if binding.key().name() == LIQUIDITY_FEATURE_NAME {
                if liquidity.is_some() {
                    return Err(ScreenWorkflowError::InvalidRequest);
                }
                liquidity = value;
            }
            observations.push(ScreenFeatureObservation::new(binding.clone(), value));
        }
        let coverage =
            StatisticalF64::try_new(present as f64 / screen.feature_bindings().len() as f64)
                .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?;
        let liquidity = liquidity.unwrap_or(
            StatisticalF64::try_new(0.0)
                .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?,
        );
        let evidence_identity = content_identity(candidate_hash.finalize().into())?;
        let mut id_hasher = Sha256::new();
        id_hasher.update(b"market-squawk/screen-candidate-id/v1\0");
        hash_bytes(&mut id_hasher, run.id().as_str().as_bytes())?;
        id_hasher.update(instrument_id.as_uuid().as_bytes());
        id_hasher.update(rows.cutoff_at.unix_nanos().to_be_bytes());
        let id_hash: [u8; 32] = id_hasher.finalize().into();
        let candidate_id = CandidateId::try_new(format!("candidate.{}", hex(&id_hash)))
            .map_err(|_error| ScreenWorkflowError::Capacity)?;
        candidates.push(
            CandidateInput::try_new(
                candidate_id,
                instrument_id,
                observations,
                coverage,
                liquidity,
                DataQuality::Modeled,
                None,
                Vec::new(),
                evidence_identity,
            )
            .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)?,
        );
    }
    Ok(candidates)
}

fn dataset_identity(
    evidence: &ForecastDatasetEvidence,
) -> Result<DecisionContentDigest, ScreenWorkflowError> {
    let fence = evidence.fence();
    let manifest = fence.manifest();
    let mut hash = Sha256::new();
    hash.update(SCREEN_DATASET_DIGEST_DOMAIN);
    hash_manifest(&mut hash, manifest)?;
    hash.update(fence.catalog_identity().bytes());
    hash.update(fence.export_sha256().bytes());
    hash.update(fence.selection_sha256().bytes());
    hash.update(fence.selected_rows().get().to_be_bytes());
    hash.update(fence.as_of().unix_nanos().to_be_bytes());
    hash.update(evidence.dataset().policy_digest().bytes());
    hash.update(evidence.dataset().universe_digest().bytes());
    content_identity(hash.finalize().into())
}

fn plan_digest(
    run: &ScreenRun,
    candidates: &[CandidateInput],
    selected_at: Timestamp,
    dataset: &ScreenDatasetFence,
) -> Result<EvidenceDigest, ScreenWorkflowError> {
    let mut hash = Sha256::new();
    hash.update(SCREEN_INPUT_DIGEST_DOMAIN);
    hash_bytes(&mut hash, run.id().as_str().as_bytes())?;
    hash_bytes(&mut hash, run.screen().id().as_str().as_bytes())?;
    hash.update(run.screen().revision().get().to_be_bytes());
    hash.update(run.as_of().unix_nanos().to_be_bytes());
    hash.update(run.dataset_identity().evidence_digest().bytes());
    hash.update(run.universe_identity().evidence_digest().bytes());
    hash.update(
        u64::try_from(run.feature_bindings().len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    for binding in run.feature_bindings() {
        hash_bytes(&mut hash, binding.key().name().as_bytes())?;
        hash.update(binding.key().version().get().to_be_bytes());
        hash.update(binding.semantic_digest().as_bytes());
    }
    hash.update(selected_at.unix_nanos().to_be_bytes());
    hash_manifest(&mut hash, &dataset.manifest)?;
    hash.update(dataset.catalog_identity.bytes());
    hash.update(dataset.export_sha256.bytes());
    hash.update(dataset.selection_sha256.bytes());
    hash.update(dataset.selected_rows.get().to_be_bytes());
    hash.update(dataset.policy_sha256.bytes());
    hash.update(
        u64::try_from(candidates.len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    for candidate in candidates {
        hash_bytes(&mut hash, candidate.id().as_str().as_bytes())?;
        hash.update(candidate.instrument_id().as_uuid().as_bytes());
        hash.update(candidate.coverage().get().to_bits().to_be_bytes());
        hash.update(candidate.liquidity().get().to_bits().to_be_bytes());
        hash.update([data_quality_tag(candidate.data_quality())]);
        match candidate.portfolio_impact() {
            Some(portfolio) => {
                hash.update([1]);
                hash.update(portfolio.bytes());
            }
            None => hash.update([0]),
        }
        hash.update(
            u64::try_from(candidate.flags().len())
                .map_err(|_error| ScreenWorkflowError::Capacity)?
                .to_be_bytes(),
        );
        for flag in candidate.flags() {
            hash.update([candidate_flag_tag(*flag)]);
        }
        hash.update(candidate.evidence_identity().evidence_digest().bytes());
        for observation in candidate.observations() {
            hash_bytes(&mut hash, observation.binding().key().name().as_bytes())?;
            hash.update(observation.binding().key().version().get().to_be_bytes());
            hash.update(observation.binding().semantic_digest().as_bytes());
            match observation.value() {
                Some(value) => {
                    hash.update([1]);
                    hash.update(value.get().to_bits().to_be_bytes());
                }
                None => hash.update([0]),
            }
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_manifest(
    hash: &mut Sha256,
    manifest: &DatasetManifestRef,
) -> Result<(), ScreenWorkflowError> {
    hash_bytes(hash, manifest.dataset_id().as_str().as_bytes())?;
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_bytes(hash, manifest.schema().name().as_bytes())?;
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    Ok(())
}

fn hash_bytes(hash: &mut Sha256, value: &[u8]) -> Result<(), ScreenWorkflowError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_error| ScreenWorkflowError::Capacity)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

fn content_identity(bytes: [u8; 32]) -> Result<DecisionContentDigest, ScreenWorkflowError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_error| ScreenWorkflowError::DatasetUnavailable)
}

fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScreenJobPlanWire {
    run: RunWire,
    candidates: Vec<CandidateInputWire>,
    selected_at: Timestamp,
    dataset: ScreenDatasetFenceWire,
    input_digest: EvidenceDigest,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScreenDatasetFenceWire {
    manifest: ManifestWire,
    catalog_identity: [u8; 32],
    export_sha256: [u8; 32],
    selection_sha256: [u8; 32],
    selected_rows: u64,
    policy_sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    dataset_id: String,
    manifest_version: u64,
    schema_name: String,
    schema_version: u16,
    schema_fingerprint: [u8; 32],
    content_hash: [u8; 32],
}

impl ScreenJobPlanWire {
    pub(super) fn key(&self) -> &str {
        self.run.key()
    }

    pub(super) fn from_plan(plan: &ScreenJobPlan) -> Self {
        Self {
            run: (&plan.run).into(),
            candidates: plan.candidates.iter().map(Into::into).collect(),
            selected_at: plan.selected_at,
            dataset: ScreenDatasetFenceWire {
                manifest: ManifestWire::from_manifest(&plan.dataset.manifest),
                catalog_identity: plan.dataset.catalog_identity.bytes(),
                export_sha256: plan.dataset.export_sha256.bytes(),
                selection_sha256: plan.dataset.selection_sha256.bytes(),
                selected_rows: plan.dataset.selected_rows.get(),
                policy_sha256: plan.dataset.policy_sha256.bytes(),
            },
            input_digest: plan.input_digest,
        }
    }

    pub(super) fn decode(
        &self,
        registry: &FeatureRegistry,
    ) -> Result<ScreenJobPlan, DecisionApplicationError> {
        let run = self.run.decode(registry)?;
        let candidates = self
            .candidates
            .iter()
            .map(|candidate| candidate.decode(run.feature_bindings(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        let manifest = self.dataset.manifest.decode()?;
        let dataset = ScreenDatasetFence {
            manifest,
            catalog_identity: CatalogEndpointIdentity::try_from_bytes(
                self.dataset.catalog_identity,
            )
            .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            export_sha256: nonzero_sha256(self.dataset.export_sha256)?,
            selection_sha256: nonzero_sha256(self.dataset.selection_sha256)?,
            selected_rows: NonZeroU64::new(self.dataset.selected_rows)
                .ok_or(DecisionApplicationError::InvalidPersistentState)?,
            policy_sha256: nonzero_sha256(self.dataset.policy_sha256)?,
        };
        let expected = plan_digest(&run, &candidates, self.selected_at, &dataset)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        if expected != self.input_digest {
            return Err(DecisionApplicationError::InvalidPersistentState);
        }
        Ok(ScreenJobPlan {
            run,
            candidates,
            selected_at: self.selected_at,
            dataset,
            input_digest: self.input_digest,
        })
    }
}

impl ManifestWire {
    fn from_manifest(manifest: &DatasetManifestRef) -> Self {
        Self {
            dataset_id: manifest.dataset_id().as_str().to_owned(),
            manifest_version: manifest.manifest_version(),
            schema_name: manifest.schema().name().to_owned(),
            schema_version: manifest.schema_version().get(),
            schema_fingerprint: manifest.schema().fingerprint(),
            content_hash: manifest.content_hash().bytes(),
        }
    }

    fn decode(&self) -> Result<DatasetManifestRef, DecisionApplicationError> {
        let schema = DatasetSchemaRef::try_new(
            &self.schema_name,
            SchemaVersion::new(self.schema_version)
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.schema_fingerprint,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str())
                .map_err(|_error| DecisionApplicationError::InvalidPersistentState)?,
            self.manifest_version,
            schema,
            nonzero_sha256(self.content_hash)?,
        )
        .map_err(|_error| DecisionApplicationError::InvalidPersistentState)
    }
}

fn nonzero_sha256(bytes: [u8; 32]) -> Result<Sha256Digest, DecisionApplicationError> {
    if bytes == [0; 32] {
        Err(DecisionApplicationError::InvalidPersistentState)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}

const fn data_quality_tag(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn candidate_flag_tag(flag: CandidateFlag) -> u8 {
    match flag {
        CandidateFlag::MissingFeatureIncluded => 1,
        CandidateFlag::ModelDependent => 2,
        CandidateFlag::PortfolioImpactBound => 3,
        CandidateFlag::NonDirectData => 4,
    }
}

pub(super) fn validate_fence(
    plan: &ScreenJobPlan,
    screen: &SavedScreen,
) -> Result<(), ScreenWorkflowError> {
    if plan.run.screen() != screen.revision()
        || plan.run.universe_identity() != screen.universe_identity()
        || plan.run.feature_bindings() != screen.feature_bindings()
        || plan.selected_at < plan.run.as_of()
        || plan.candidates.len() > market_squawk_decisions::MAX_SCREEN_INPUT_ROWS
    {
        return Err(ScreenWorkflowError::Conflict);
    }
    Ok(())
}
