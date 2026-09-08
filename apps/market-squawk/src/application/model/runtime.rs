//! Durable production model admission and restart-safe backend composition.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use market_squawk_data::{PythonDatasetVerificationLimits, Sha256Digest};
use market_squawk_domain::ModelId;
use market_squawk_modeling::{
    BundleId, BundleMetadataRef, BundleRegistration, ControlledModelRoot, InferenceBackend,
    MAX_BUNDLE_AUTHORITY_BYTES, ModelAdmissionError, ModelBundle, ModelFormat, ModelRegistry,
    ModelRegistryError, NativeBackendError, NativeLinearBackend, OnnxBackendError, OnnxModelPolicy,
    OnnxWorkerProgram, ProductionFeatureRegistry, PythonDatasetAdmissionAuthority,
    TractOnnxBackend, VerifiedTrainingEnvironment, recover_model_candidate, verify_model_candidate,
};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths, PathError,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{ModelDomainServiceError, ModelReadImage, ModelReadImageState};

pub use index::{ModelRuntimeIndexError, ModelRuntimeIndexLimits};

use self::index::{
    IndexAdmission, ModelRuntimeIndex, StoredRuntimePolicy, validate_candidate_directory,
};

mod admission_request;
mod index;

const MODEL_RUNTIME_AUTHORITY_DIRECTORY: &str = "model/runtime-admissions";
const MAXIMUM_VALIDATION_TIME: Duration = Duration::from_secs(60);
const STANDARD_VALIDATION_TIME: Duration = Duration::from_secs(30);
const STANDARD_REGISTRY_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Closed backend policy attached to one exact bundle admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelBackendAdmission {
    /// Build the native linear or logistic backend declared by bundle metadata.
    Native,
    /// Build the exact closed ONNX policy through the supplied tract worker authority.
    Onnx(OnnxModelPolicy),
}

/// Capability-relative model candidate and its independent verified authorities.
#[derive(Debug)]
pub struct ModelAdmissionRequest {
    candidate_directory: Box<str>,
    metadata: BundleMetadataRef,
    authority_bytes: Box<[u8]>,
    authority_sha256: Sha256Digest,
    dataset: PythonDatasetAdmissionAuthority,
    backend: ModelBackendAdmission,
    worker_expectation: Option<WorkerCandidateExpectation>,
}

#[derive(Debug)]
struct WorkerCandidateExpectation {
    metadata_sha256: [u8; 32],
    artifact_sha256: [u8; 32],
    training_run_sha256: [u8; 32],
    authority_sha256: [u8; 32],
    dataset_export_sha256: [u8; 32],
    dataset_selection_sha256: [u8; 32],
    catalog_identity_sha256: [u8; 32],
    training_environment_sha256: [u8; 32],
    training_code_revision: Box<str>,
}

impl ModelAdmissionRequest {
    /// Constructs an admission request with no ambient path or executable authority.
    ///
    /// `candidate_directory` is relative to the prepared Market Squawk artifact root. Bundle
    /// metadata, artifact, and training-run paths remain relative to that directory capability.
    ///
    /// # Errors
    ///
    /// Rejects an unsafe candidate directory, empty or oversized authority bytes, or a digest
    /// that does not name those exact bytes.
    pub fn try_new(
        candidate_directory: impl AsRef<str>,
        metadata: BundleMetadataRef,
        authority_bytes: Box<[u8]>,
        authority_sha256: Sha256Digest,
        dataset: PythonDatasetAdmissionAuthority,
        backend: ModelBackendAdmission,
    ) -> Result<Self, ProductionModelRuntimeError> {
        let candidate_directory = candidate_directory.as_ref();
        validate_candidate_directory(candidate_directory)?;
        if authority_bytes.is_empty()
            || authority_bytes.len() > MAX_BUNDLE_AUTHORITY_BYTES
            || Sha256Digest::new(Sha256::digest(&authority_bytes).into()) != authority_sha256
            || matches!(
                &backend,
                ModelBackendAdmission::Onnx(policy) if !policy.output_semantics_bound()
            )
        {
            return Err(ProductionModelRuntimeError::InvalidAdmission);
        }
        Ok(Self {
            candidate_directory: candidate_directory.into(),
            metadata,
            authority_bytes,
            authority_sha256,
            dataset,
            backend,
            worker_expectation: None,
        })
    }
}

/// Fixed count, memory, dataset, and elapsed-time bounds for model runtime recovery.
#[derive(Clone, Copy, Debug)]
pub struct ProductionModelRuntimeLimits {
    index: ModelRuntimeIndexLimits,
    registry_retained_bytes: NonZeroUsize,
    dataset_verification: PythonDatasetVerificationLimits,
    validation_time: Duration,
}

impl ProductionModelRuntimeLimits {
    /// Constructs bounds no greater than the model, dataset, and authority-store ceilings.
    ///
    /// # Errors
    ///
    /// Rejects an empty or longer-than-60-second aggregate validation window and invalid index or
    /// model-registry bounds.
    pub fn try_new(
        index: ModelRuntimeIndexLimits,
        registry_retained_bytes: NonZeroUsize,
        dataset_verification: PythonDatasetVerificationLimits,
        validation_time: Duration,
    ) -> Result<Self, ProductionModelRuntimeError> {
        ModelRuntimeIndexLimits::try_new(index.maximum_generations(), index.maximum_index_bytes())
            .map_err(|_| ProductionModelRuntimeError::InvalidLimits)?;
        ModelRegistry::try_new(index.maximum_generations(), registry_retained_bytes)?;
        if validation_time.is_zero() || validation_time > MAXIMUM_VALIDATION_TIME {
            return Err(ProductionModelRuntimeError::InvalidLimits);
        }
        Ok(Self {
            index,
            registry_retained_bytes,
            dataset_verification,
            validation_time,
        })
    }

    /// Returns bounded local production defaults.
    ///
    /// # Errors
    ///
    /// Returns a typed error if fixed model or dataset limits no longer compose.
    pub fn standard() -> Result<Self, ProductionModelRuntimeError> {
        Self::try_new(
            ModelRuntimeIndexLimits::standard(),
            NonZeroUsize::new(STANDARD_REGISTRY_RETAINED_BYTES)
                .ok_or(ProductionModelRuntimeError::InvalidLimits)?,
            PythonDatasetVerificationLimits::try_new(100_000, 256 * 1024 * 1024)
                .map_err(ModelAdmissionError::from)?,
            STANDARD_VALIDATION_TIME,
        )
    }
}

/// Immutable admission disposition returned to CLI or MCP composition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelAdmissionDisposition {
    /// A new immutable generation became durable.
    Inserted,
    /// The exact already-durable generation was independently revalidated.
    AlreadyAdmitted,
}

/// Exact durable model-admission receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelAdmissionReceipt {
    model_id: ModelId,
    bundle_id: BundleId,
    bundle_version: std::num::NonZeroU64,
    metadata_sha256: Sha256Digest,
    artifact_sha256: Sha256Digest,
    training_run_sha256: Sha256Digest,
    authority_sha256: Sha256Digest,
    dataset_selection_sha256: Sha256Digest,
    disposition: ModelAdmissionDisposition,
}

impl ModelAdmissionReceipt {
    /// Returns the stable model identity.
    #[must_use]
    pub const fn model_id(&self) -> ModelId {
        self.model_id
    }

    /// Returns the immutable bundle series.
    #[must_use]
    pub const fn bundle_id(&self) -> &BundleId {
        &self.bundle_id
    }

    /// Returns the immutable bundle generation.
    #[must_use]
    pub const fn bundle_version(&self) -> std::num::NonZeroU64 {
        self.bundle_version
    }

    /// Returns the complete admission disposition.
    #[must_use]
    pub const fn disposition(&self) -> ModelAdmissionDisposition {
        self.disposition
    }

    /// Returns the exact metadata digest.
    #[must_use]
    pub const fn metadata_sha256(&self) -> Sha256Digest {
        self.metadata_sha256
    }

    /// Returns the exact model artifact digest.
    #[must_use]
    pub const fn artifact_sha256(&self) -> Sha256Digest {
        self.artifact_sha256
    }

    /// Returns the exact training-run digest.
    #[must_use]
    pub const fn training_run_sha256(&self) -> Sha256Digest {
        self.training_run_sha256
    }

    /// Returns the independent authority-document digest.
    #[must_use]
    pub const fn authority_sha256(&self) -> Sha256Digest {
        self.authority_sha256
    }

    /// Returns the independently rederived point-in-time dataset selection.
    #[must_use]
    pub const fn dataset_selection_sha256(&self) -> Sha256Digest {
        self.dataset_selection_sha256
    }
}

/// Exact nonempty registry/backend set consumed by [`super::ModelDomainService`].
pub struct ModelRuntimeSnapshot {
    read_image: Arc<ModelReadImageState>,
}

impl ModelRuntimeSnapshot {
    /// Consumes this immutable snapshot into the existing model-domain constructor arguments.
    #[must_use]
    pub fn into_parts(self) -> (Arc<ModelRegistry>, Vec<Arc<dyn InferenceBackend>>) {
        let image = self.read_image.load();
        (Arc::clone(&image.registry), image.backends.to_vec())
    }

    pub(super) fn into_read_image(self) -> Arc<ModelReadImageState> {
        self.read_image
    }

    /// Returns the exact number of admitted runtime generations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read_image.load().len()
    }

    /// Returns whether the snapshot contains no generation.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.read_image.load().is_empty()
    }
}

impl fmt::Debug for ModelRuntimeSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelRuntimeSnapshot")
            .field("generation_count", &self.read_image.load().len())
            .finish()
    }
}

struct RuntimeGate {
    index: ModelRuntimeIndex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RuntimeBackupCoordinate {
    pub(super) candidate_directory: Box<str>,
    pub(super) metadata_path: Box<str>,
    pub(super) model_id: ModelId,
    pub(super) bundle_id: BundleId,
    pub(super) bundle_version: std::num::NonZeroU64,
}

pub(super) struct RetainedRuntimeBackup {
    pub(super) canonical_index: Box<[u8]>,
    pub(super) models: Vec<(RuntimeBackupCoordinate, Arc<ModelBundle>)>,
}

pub(super) struct RetainedForecastRuntime {
    pub(super) generation_sha256: Sha256Digest,
    pub(super) backends: Box<[Arc<dyn InferenceBackend>]>,
}

impl fmt::Debug for RetainedForecastRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedForecastRuntime")
            .field("generation_sha256", &self.generation_sha256)
            .field("backend_count", &self.backends.len())
            .finish()
    }
}

impl fmt::Debug for RetainedRuntimeBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedRuntimeBackup")
            .field("canonical_index", &"[CANONICAL MODEL RUNTIME INDEX]")
            .field("model_count", &self.models.len())
            .finish()
    }
}

/// Application-owned durable model admission and backend recovery authority.
pub struct ProductionModelRuntime {
    paths: LocalPaths,
    store: LocalAuthorityStateStore,
    feature_registry: ProductionFeatureRegistry,
    training_environment: Option<VerifiedTrainingEnvironment>,
    onnx_worker: Option<OnnxWorkerProgram>,
    limits: ProductionModelRuntimeLimits,
    gate: Mutex<RuntimeGate>,
    read_image: Arc<ModelReadImageState>,
}

impl ProductionModelRuntime {
    pub(super) fn empty_backup(
        limits: ProductionModelRuntimeLimits,
    ) -> Result<RetainedRuntimeBackup, ProductionModelRuntimeError> {
        Ok(RetainedRuntimeBackup {
            canonical_index: ModelRuntimeIndex::empty()
                .encode(limits.index)?
                .into_boxed_slice(),
            models: Vec::new(),
        })
    }

    /// Returns the exact sealed environment shared by training execution and candidate admission.
    ///
    /// # Errors
    ///
    /// Returns a typed unavailable error only for an in-crate test runtime that deliberately has
    /// no signed installed-release capability.
    pub fn training_environment(
        &self,
    ) -> Result<&VerifiedTrainingEnvironment, ProductionModelRuntimeError> {
        self.training_environment
            .as_ref()
            .ok_or(ProductionModelRuntimeError::RuntimeUnavailable)
    }

    /// Reports whether the fixed durable runtime index contains any admitted generation.
    ///
    /// The complete canonical index is decoded under the supplied production limits. This lets
    /// application composition distinguish a genuinely fresh model namespace from an existing
    /// runtime that must not be hidden when its verified training-environment capability is
    /// unavailable.
    ///
    /// # Errors
    ///
    /// Returns a typed local-path, persistence, index-validation, or resource error.
    pub fn has_durable_admissions(
        paths: &LocalPaths,
        limits: ProductionModelRuntimeLimits,
    ) -> Result<bool, ProductionModelRuntimeError> {
        let (_store, index) = open_runtime_index(paths, limits)?;
        Ok(!index.entries().is_empty())
    }

    /// Constructs the truthful empty model inventory used only for a fresh local namespace.
    ///
    /// Callers must first establish with [`Self::has_durable_admissions`] that no durable model
    /// generation exists. Inference over this snapshot returns not-found through the normal model
    /// domain service; it never represents an unavailable admitted generation as empty.
    ///
    /// # Errors
    ///
    /// Returns a registry error if the code-owned production limits cannot construct an empty
    /// bounded registry.
    pub fn empty_snapshot(
        limits: ProductionModelRuntimeLimits,
    ) -> Result<ModelRuntimeSnapshot, ProductionModelRuntimeError> {
        let registry = Arc::new(ModelRegistry::try_new(
            limits.index.maximum_generations(),
            limits.registry_retained_bytes,
        )?);
        let image = Arc::new(ModelReadImage::try_new(registry, Vec::new())?);
        Ok(ModelRuntimeSnapshot {
            read_image: Arc::new(ModelReadImageState::new(image)),
        })
    }

    /// Opens the fixed model-control namespace and reconstructs every durable runtime generation.
    ///
    /// An empty index is a valid admission owner but [`Self::snapshot`] refuses to represent it as
    /// a usable model runtime. Every persisted record is revalidated; one bad record, missing ONNX
    /// worker capability, or backend load failure rejects the complete constructor.
    ///
    /// # Errors
    ///
    /// Returns a typed local-path, persistence, dataset, bundle, registry, policy, worker, or
    /// aggregate-deadline error without publishing a partial runtime.
    pub fn try_open(
        paths: &LocalPaths,
        training_environment: VerifiedTrainingEnvironment,
        onnx_worker: Option<OnnxWorkerProgram>,
        limits: ProductionModelRuntimeLimits,
    ) -> Result<Self, ProductionModelRuntimeError> {
        let (store, index) = open_runtime_index(paths, limits)?;
        let feature_registry = ProductionFeatureRegistry::try_new()?;
        let image = build_runtime(
            paths,
            &index,
            &feature_registry,
            onnx_worker.as_ref(),
            limits,
            None,
        )?;
        let read_image = Arc::new(ModelReadImageState::new(image));
        Ok(Self {
            paths: paths.clone(),
            store,
            feature_registry,
            training_environment: Some(training_environment),
            onnx_worker,
            limits,
            gate: Mutex::new(RuntimeGate { index }),
            read_image,
        })
    }

    /// Durably admits one exact candidate or recognizes a fully identical replay.
    ///
    /// The method accepts no arbitrary local path or executable. It revalidates the configured
    /// catalog selection and current training release, builds a complete proposed runtime, commits
    /// the canonical two-copy index, then atomically swaps process state.
    ///
    /// # Errors
    ///
    /// Any authority, immutable-coordinate, resource, runtime, persistence, or deadline failure
    /// leaves the current durable and in-process runtime unchanged.
    pub fn admit(
        &self,
        request: ModelAdmissionRequest,
    ) -> Result<ModelAdmissionReceipt, ProductionModelRuntimeError> {
        let deadline = validation_deadline(self.limits.validation_time)?;
        let root = open_candidate_root(&self.paths, &request.candidate_directory)?;
        let candidate = self.verify_candidate(&root, &request, deadline)?;
        let RuntimeValidatedCandidate {
            bundle,
            authority_bytes,
            authority_sha256,
            dataset,
        } = candidate;
        let metadata = bundle.metadata();
        if metadata.metadata_hash() != request.metadata.content_hash()
            || metadata.dataset().export_digest() != dataset.export_sha256()
            || metadata.dataset().selection_digest() != dataset.selection_sha256()
            || metadata.dataset().selection_as_of() != dataset.as_of()
            || metadata.dataset().catalog_identity() != dataset.catalog_identity()
        {
            return Err(ProductionModelRuntimeError::CandidateEvidenceMismatch);
        }
        if let Some(expected) = &request.worker_expectation
            && (metadata.metadata_hash().bytes() != expected.metadata_sha256
                || metadata.artifact_hash().bytes() != expected.artifact_sha256
                || metadata.training_run_hash().bytes() != expected.training_run_sha256
                || authority_sha256.bytes() != expected.authority_sha256
                || dataset.export_sha256().bytes() != expected.dataset_export_sha256
                || dataset.selection_sha256().bytes() != expected.dataset_selection_sha256
                || dataset.catalog_identity().bytes() != expected.catalog_identity_sha256
                || metadata.training_environment_hash().bytes()
                    != expected.training_environment_sha256
                || metadata.training_code_revision() != expected.training_code_revision.as_ref())
        {
            return Err(ProductionModelRuntimeError::CandidateEvidenceMismatch);
        }
        let runtime_policy = stored_policy(&bundle, request.backend)?;
        let admission = IndexAdmission {
            candidate_directory: request.candidate_directory,
            metadata_path: request.metadata.relative_path().into(),
            metadata_sha256: metadata.metadata_hash(),
            authority_bytes,
            authority_sha256,
            dataset_export_sha256: dataset.export_sha256(),
            dataset_as_of: dataset.as_of(),
            dataset_selection_sha256: dataset.selection_sha256(),
            catalog_identity: dataset.catalog_identity(),
            model_id: metadata.model_id(),
            bundle_id: metadata.bundle_id().clone(),
            bundle_version: metadata.bundle_version(),
            artifact_sha256: metadata.artifact_hash(),
            training_run_sha256: metadata.training_run_hash(),
            training_environment_sha256: metadata.training_environment_hash(),
            output_binding_sha256: metadata.output_binding().identity(),
            runtime_policy,
        };
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let current = gate.index.encode(self.limits.index)?;
        let mut proposed = ModelRuntimeIndex::decode(&current, self.limits.index)?;
        let inserted = proposed.try_insert(admission.clone(), self.limits.index)?;
        if !inserted {
            return Ok(receipt(
                &admission,
                ModelAdmissionDisposition::AlreadyAdmitted,
            ));
        }
        let image = build_runtime(
            &self.paths,
            &proposed,
            &self.feature_registry,
            self.onnx_worker.as_ref(),
            self.limits,
            Some(PreparedRuntimeCandidate {
                candidate_directory: admission.candidate_directory.clone(),
                metadata_sha256: admission.metadata_sha256,
                bundle,
            }),
        )?;
        let encoded = proposed.encode(self.limits.index)?;
        self.store.store(&encoded)?;
        gate.index = proposed;
        self.read_image.publish(image);
        Ok(receipt(&admission, ModelAdmissionDisposition::Inserted))
    }

    /// Returns the shared atomically published runtime image, including a truthful empty image.
    ///
    /// The empty image must remain shared: the first later durable admission publishes through
    /// this same capability and becomes visible to the already-composed model service.
    pub fn snapshot(&self) -> Result<ModelRuntimeSnapshot, ProductionModelRuntimeError> {
        Ok(ModelRuntimeSnapshot {
            read_image: Arc::clone(&self.read_image),
        })
    }

    pub(super) fn retain_backup(
        &self,
    ) -> Result<RetainedRuntimeBackup, ProductionModelRuntimeError> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let canonical_index = gate.index.encode(self.limits.index)?.into_boxed_slice();
        let image = self.read_image.load();
        if image.registry.len()? != gate.index.entries().len() {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        let mut models = Vec::new();
        models
            .try_reserve_exact(gate.index.entries().len())
            .map_err(|_| ProductionModelRuntimeError::ResourceExhausted)?;
        for admission in gate.index.entries() {
            let bundle = image
                .registry
                .get(&admission.bundle_id, admission.bundle_version)?
                .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
            validate_recovered_bundle(&bundle, admission)?;
            models.push((
                RuntimeBackupCoordinate {
                    candidate_directory: admission.candidate_directory.clone(),
                    metadata_path: admission.metadata_path.clone(),
                    model_id: admission.model_id,
                    bundle_id: admission.bundle_id.clone(),
                    bundle_version: admission.bundle_version,
                },
                bundle,
            ));
        }
        Ok(RetainedRuntimeBackup {
            canonical_index,
            models,
        })
    }

    pub(super) fn retain_forecast_runtime(
        &self,
    ) -> Result<RetainedForecastRuntime, ProductionModelRuntimeError> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let encoded = gate.index.encode(self.limits.index)?;
        let image = self.read_image.load();
        if image.backends.len() != gate.index.entries().len()
            || image.registry.len()? != image.backends.len()
        {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        for admission in gate.index.entries() {
            let matching = image.backends.iter().filter(|backend| {
                let metadata = backend.metadata();
                metadata.model_id() == admission.model_id
                    && metadata.bundle_id() == &admission.bundle_id
                    && metadata.bundle_version() == admission.bundle_version
                    && metadata.metadata_hash() == admission.metadata_sha256
                    && metadata.artifact_hash() == admission.artifact_sha256
                    && metadata.training_run_hash() == admission.training_run_sha256
                    && metadata.dataset().export_digest() == admission.dataset_export_sha256
                    && metadata.dataset().selection_digest() == admission.dataset_selection_sha256
                    && metadata.output_binding().identity() == admission.output_binding_sha256
            });
            if matching.count() != 1 {
                return Err(ProductionModelRuntimeError::CorruptRuntime);
            }
        }
        Ok(RetainedForecastRuntime {
            generation_sha256: Sha256Digest::new(Sha256::digest(encoded).into()),
            backends: image.backends.to_vec().into_boxed_slice(),
        })
    }

    pub(super) fn validate_forecast_runtime_generation(
        &self,
        expected: Sha256Digest,
    ) -> Result<(), ProductionModelRuntimeError> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let observed =
            Sha256Digest::new(Sha256::digest(gate.index.encode(self.limits.index)?).into());
        if observed != expected {
            return Err(ProductionModelRuntimeError::StaleForecastGeneration);
        }
        Ok(())
    }

    pub(super) fn restore_capabilities(
        &self,
    ) -> Result<
        (
            VerifiedTrainingEnvironment,
            Option<OnnxWorkerProgram>,
            ProductionModelRuntimeLimits,
        ),
        ProductionModelRuntimeError,
    > {
        Ok((
            self.training_environment()?.clone(),
            self.onnx_worker.clone(),
            self.limits,
        ))
    }

    pub(super) fn backup_coordinates(
        canonical_index: &[u8],
        limits: ProductionModelRuntimeLimits,
    ) -> Result<Vec<RuntimeBackupCoordinate>, ProductionModelRuntimeError> {
        let index = ModelRuntimeIndex::decode(canonical_index, limits.index)?;
        Ok(index
            .entries()
            .iter()
            .map(|admission| RuntimeBackupCoordinate {
                candidate_directory: admission.candidate_directory.clone(),
                metadata_path: admission.metadata_path.clone(),
                model_id: admission.model_id,
                bundle_id: admission.bundle_id.clone(),
                bundle_version: admission.bundle_version,
            })
            .collect())
    }

    pub(super) fn stage_backup_index(
        paths: &LocalPaths,
        canonical_index: &[u8],
        limits: ProductionModelRuntimeLimits,
    ) -> Result<(), ProductionModelRuntimeError> {
        let decoded = ModelRuntimeIndex::decode(canonical_index, limits.index)?;
        if decoded.encode(limits.index)? != canonical_index {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        let (store, existing) = open_runtime_index(paths, limits)?;
        if !existing.entries().is_empty() || store.load()?.is_some() {
            return Err(ProductionModelRuntimeError::RestoreTargetNotFresh);
        }
        store.store(canonical_index)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_fixture(
        paths: &LocalPaths,
        candidate: Option<ModelBundle>,
    ) -> Result<Self, ProductionModelRuntimeError> {
        if candidate.is_some() {
            return Err(ProductionModelRuntimeError::InvalidAdmission);
        }
        let limits = ProductionModelRuntimeLimits::standard()?;
        let (store, index) = open_runtime_index(paths, limits)?;
        if !index.entries().is_empty() {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        let feature_registry = ProductionFeatureRegistry::try_new()?;
        let image = build_runtime(paths, &index, &feature_registry, None, limits, None)?;
        Ok(Self {
            paths: paths.clone(),
            store,
            feature_registry,
            training_environment: None,
            onnx_worker: None,
            limits,
            gate: Mutex::new(RuntimeGate { index }),
            read_image: Arc::new(ModelReadImageState::new(image)),
        })
    }

    fn verify_candidate(
        &self,
        root: &ControlledModelRoot,
        request: &ModelAdmissionRequest,
        deadline: Instant,
    ) -> Result<RuntimeValidatedCandidate, ProductionModelRuntimeError> {
        let environment = self.training_environment()?;
        let candidate = verify_model_candidate(
            root,
            &request.metadata,
            &request.authority_bytes,
            request.authority_sha256,
            self.paths.root(),
            request.dataset,
            environment,
            &self.feature_registry,
            self.limits.dataset_verification,
            deadline,
            &CancellationToken::new(),
        )?;
        let (bundle, authority, dataset) = candidate.into_parts();
        Ok(RuntimeValidatedCandidate {
            bundle,
            authority_sha256: authority.sha256(),
            authority_bytes: authority.into_bytes(),
            dataset,
        })
    }
}

struct RuntimeValidatedCandidate {
    bundle: ModelBundle,
    authority_bytes: Box<[u8]>,
    authority_sha256: Sha256Digest,
    dataset: PythonDatasetAdmissionAuthority,
}

struct PreparedRuntimeCandidate {
    candidate_directory: Box<str>,
    metadata_sha256: Sha256Digest,
    bundle: ModelBundle,
}

impl PreparedRuntimeCandidate {
    fn matches(&self, admission: &IndexAdmission) -> bool {
        self.candidate_directory == admission.candidate_directory
            && self.metadata_sha256 == admission.metadata_sha256
    }
}

fn open_runtime_index(
    paths: &LocalPaths,
    limits: ProductionModelRuntimeLimits,
) -> Result<(LocalAuthorityStateStore, ModelRuntimeIndex), ProductionModelRuntimeError> {
    let control = paths.control_root()?;
    control.try_clone_directory()?;
    let store =
        LocalAuthorityStateStore::try_open(control.root().join(MODEL_RUNTIME_AUTHORITY_DIRECTORY))?;
    control.try_clone_directory()?;
    let index = store.load()?.map_or_else(
        || Ok(ModelRuntimeIndex::empty()),
        |bytes| ModelRuntimeIndex::decode(&bytes, limits.index),
    )?;
    Ok((store, index))
}

impl fmt::Debug for ProductionModelRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionModelRuntime")
            .field("paths", &"[PREPARED LOCAL PATHS]")
            .field("store", &self.store)
            .field("feature_registry", &self.feature_registry)
            .field("training_environment", &"[VERIFIED TRAINING ENVIRONMENT]")
            .field("onnx_worker", &self.onnx_worker.is_some())
            .field("limits", &self.limits)
            .field("gate", &"[DURABLE MODEL RUNTIME]")
            .finish()
    }
}

fn build_runtime(
    paths: &LocalPaths,
    index: &ModelRuntimeIndex,
    feature_registry: &ProductionFeatureRegistry,
    onnx_worker: Option<&OnnxWorkerProgram>,
    limits: ProductionModelRuntimeLimits,
    mut prepared: Option<PreparedRuntimeCandidate>,
) -> Result<Arc<ModelReadImage>, ProductionModelRuntimeError> {
    let deadline = validation_deadline(limits.validation_time)?;
    let registry = Arc::new(ModelRegistry::try_new(
        limits.index.maximum_generations(),
        limits.registry_retained_bytes,
    )?);
    let mut backends = Vec::new();
    backends
        .try_reserve_exact(index.entries().len())
        .map_err(|_| ProductionModelRuntimeError::ResourceExhausted)?;
    let cancellation = CancellationToken::new();
    for admission in index.entries() {
        if Instant::now() >= deadline {
            return Err(ProductionModelRuntimeError::ValidationDeadline);
        }
        let bundle = if prepared
            .as_ref()
            .is_some_and(|candidate| candidate.matches(admission))
        {
            match prepared.take() {
                Some(candidate) => candidate.bundle,
                None => return Err(ProductionModelRuntimeError::CorruptRuntime),
            }
        } else {
            let root = open_candidate_root(paths, &admission.candidate_directory)?;
            let metadata =
                BundleMetadataRef::try_new(&admission.metadata_path, admission.metadata_sha256)
                    .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
            let dataset = PythonDatasetAdmissionAuthority::try_new(
                admission.dataset_export_sha256,
                admission.dataset_as_of,
                admission.dataset_selection_sha256,
                admission.catalog_identity,
            )?;
            recover_model_candidate(
                &root,
                &metadata,
                &admission.authority_bytes,
                admission.authority_sha256,
                paths.root(),
                dataset,
                feature_registry,
                limits.dataset_verification,
                deadline,
                &cancellation,
            )?
            .into_bundle()
        };
        validate_recovered_bundle(&bundle, admission)?;
        let bundle_id = bundle.metadata().bundle_id().clone();
        let bundle_version = bundle.metadata().bundle_version();
        if registry.try_register(bundle)? != BundleRegistration::Inserted {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        let retained = registry
            .get(&bundle_id, bundle_version)?
            .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
        backends.push(build_backend(
            retained,
            &admission.runtime_policy,
            onnx_worker,
        )?);
    }
    if prepared.is_some() {
        return Err(ProductionModelRuntimeError::CorruptRuntime);
    }
    Ok(Arc::new(ModelReadImage::try_new(registry, backends)?))
}

fn validate_recovered_bundle(
    bundle: &ModelBundle,
    admission: &IndexAdmission,
) -> Result<(), ProductionModelRuntimeError> {
    let metadata = bundle.metadata();
    if metadata.model_id() != admission.model_id
        || metadata.bundle_id() != &admission.bundle_id
        || metadata.bundle_version() != admission.bundle_version
        || metadata.metadata_hash() != admission.metadata_sha256
        || metadata.artifact_hash() != admission.artifact_sha256
        || metadata.training_run_hash() != admission.training_run_sha256
        || metadata.training_environment_hash() != admission.training_environment_sha256
        || metadata.output_binding().identity() != admission.output_binding_sha256
    {
        return Err(ProductionModelRuntimeError::CorruptRuntime);
    }
    Ok(())
}

fn stored_policy(
    bundle: &ModelBundle,
    policy: ModelBackendAdmission,
) -> Result<StoredRuntimePolicy, ProductionModelRuntimeError> {
    match (bundle.metadata().format(), policy) {
        (
            ModelFormat::NativeLinear | ModelFormat::NativeLogistic,
            ModelBackendAdmission::Native,
        ) => Ok(StoredRuntimePolicy::Native),
        (ModelFormat::Onnx, ModelBackendAdmission::Onnx(policy))
            if policy.model_digest() == bundle.metadata().artifact_hash()
                && policy.output_semantics_bound()
                && policy.output_semantics() == bundle.metadata().output_semantics() =>
        {
            StoredRuntimePolicy::try_onnx(policy).map_err(Into::into)
        }
        _ => Err(ProductionModelRuntimeError::BackendPolicyMismatch),
    }
}

fn build_backend(
    bundle: Arc<ModelBundle>,
    policy: &StoredRuntimePolicy,
    onnx_worker: Option<&OnnxWorkerProgram>,
) -> Result<Arc<dyn InferenceBackend>, ProductionModelRuntimeError> {
    match (bundle.metadata().format(), policy) {
        (ModelFormat::NativeLinear | ModelFormat::NativeLogistic, StoredRuntimePolicy::Native) => {
            Ok(Arc::new(NativeLinearBackend::try_from_bundle(bundle)?))
        }
        (ModelFormat::Onnx, StoredRuntimePolicy::Onnx { policy, .. }) => {
            let worker = onnx_worker.ok_or(ProductionModelRuntimeError::MissingOnnxWorker)?;
            Ok(Arc::new(TractOnnxBackend::try_from_bundle(
                bundle,
                policy.clone(),
                worker,
            )?))
        }
        _ => Err(ProductionModelRuntimeError::BackendPolicyMismatch),
    }
}

fn open_candidate_root(
    paths: &LocalPaths,
    relative: &str,
) -> Result<ControlledModelRoot, ProductionModelRuntimeError> {
    validate_candidate_directory(relative)?;
    let artifacts = paths.artifacts()?;
    let mut directory = artifacts.try_clone_directory()?;
    for component in relative.split('/') {
        let metadata = directory
            .symlink_metadata(component)
            .map_err(|_| ProductionModelRuntimeError::CandidateRootUnavailable)?;
        if !metadata.file_type().is_dir() {
            return Err(ProductionModelRuntimeError::CandidateRootUnavailable);
        }
        directory = directory
            .open_dir(component)
            .map_err(|_| ProductionModelRuntimeError::CandidateRootUnavailable)?;
    }
    artifacts.try_clone_directory()?;
    Ok(ControlledModelRoot::from_directory(directory))
}

fn validation_deadline(duration: Duration) -> Result<Instant, ProductionModelRuntimeError> {
    Instant::now()
        .checked_add(duration)
        .ok_or(ProductionModelRuntimeError::ValidationDeadline)
}

fn receipt(
    admission: &IndexAdmission,
    disposition: ModelAdmissionDisposition,
) -> ModelAdmissionReceipt {
    ModelAdmissionReceipt {
        model_id: admission.model_id,
        bundle_id: admission.bundle_id.clone(),
        bundle_version: admission.bundle_version,
        metadata_sha256: admission.metadata_sha256,
        artifact_sha256: admission.artifact_sha256,
        training_run_sha256: admission.training_run_sha256,
        authority_sha256: admission.authority_sha256,
        dataset_selection_sha256: admission.dataset_selection_sha256,
        disposition,
    }
}

/// Durable production model runtime construction, recovery, or admission failure.
#[derive(Debug, Error)]
pub enum ProductionModelRuntimeError {
    /// Fixed resource limits are invalid.
    #[error("production model runtime limits are invalid")]
    InvalidLimits,
    /// The typed admission request is malformed.
    #[error("production model admission request is invalid")]
    InvalidAdmission,
    /// A worker claim disagreed with the independently decoded and verified candidate.
    #[error("production model worker candidate evidence does not match admission")]
    CandidateEvidenceMismatch,
    /// Prepared local path authority failed.
    #[error("production model local path authority failed: {0}")]
    Path(#[from] PathError),
    /// Two-copy model runtime authority failed.
    #[error("production model durable authority failed: {0}")]
    State(#[from] LocalAuthorityStateStoreError),
    /// Canonical model runtime index validation failed.
    #[error("production model runtime index failed: {0}")]
    Index(#[from] ModelRuntimeIndexError),
    /// Dataset, training, or bundle authority failed.
    #[error("production model candidate admission failed: {0}")]
    Admission(#[from] ModelAdmissionError),
    /// Model registry construction or immutable registration failed.
    #[error("production model registry failed: {0}")]
    Registry(#[from] ModelRegistryError),
    /// The complete immutable registry/backend read image was inconsistent.
    #[error("production model read image failed: {0}")]
    ReadImage(#[from] ModelDomainServiceError),
    /// Native backend construction failed.
    #[error("production native model backend failed: {0}")]
    Native(#[from] NativeBackendError),
    /// ONNX policy, runtime load, or warm-up failed.
    #[error("production ONNX model backend failed: {0}")]
    Onnx(#[from] OnnxBackendError),
    /// An admitted ONNX generation has no exact worker capability.
    #[error("production ONNX worker authority is required")]
    MissingOnnxWorker,
    /// Bundle format and persisted backend policy disagree.
    #[error("production model backend policy differs from bundle format")]
    BackendPolicyMismatch,
    /// Prepared candidate directory authority is unavailable or not a real directory.
    #[error("production model candidate directory is unavailable")]
    CandidateRootUnavailable,
    /// Persisted record fields disagree with the independently reloaded bundle.
    #[error("production model runtime state is corrupt")]
    CorruptRuntime,
    /// A prepared forecast names a model generation that is no longer current.
    #[error("production model forecast generation is stale")]
    StaleForecastGeneration,
    /// Aggregate startup or admission verification exceeded its bound.
    #[error("production model validation deadline elapsed")]
    ValidationDeadline,
    /// Registry/backend state synchronization failed closed.
    #[error("production model runtime is unavailable")]
    RuntimeUnavailable,
    /// Restore attempted to reuse an authority outside a fresh inactive workspace.
    #[error("production model restore target is not fresh")]
    RestoreTargetNotFresh,
    /// No admitted generation exists; a usable model service cannot be composed.
    #[error("production model runtime has no admitted generation")]
    EmptyRuntime,
    /// Bounded runtime allocation failed.
    #[error("production model runtime resource ceiling was exceeded")]
    ResourceExhausted,
}
