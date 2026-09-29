//! Durable production model admission and restart-safe backend composition.

use std::fmt;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use market_squawk_data::{
    ModelInventoryCatalogCapability, ModelInventoryError, ModelInventoryHead, ModelInventoryRecord,
    PythonDatasetVerificationLimits, Sha256Digest,
};
use market_squawk_domain::ModelId;
use market_squawk_modeling::{
    BundleId, BundleMetadataRef, ControlledModelRoot, InferenceBackend, MAX_BUNDLE_AUTHORITY_BYTES,
    ModelAdmissionError, ModelBundle, ModelFormat, ModelRegistry, ModelRegistryError,
    NativeBackendError, NativeLinearBackend, OnnxBackendError, OnnxModelPolicy, OnnxWorkerProgram,
    ProductionFeatureRegistry, PythonDatasetAdmissionAuthority, TractOnnxBackend,
    VerifiedTrainingEnvironment, verify_model_candidate,
};
use market_squawk_platform::{LocalPaths, PathError};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{ModelDomainServiceError, ModelReadImage, ModelReadImageState};

pub use index::ModelRuntimeIndexError;

use self::index::{IndexAdmission, StoredRuntimePolicy, validate_candidate_directory};

mod admission_request;
mod index;
pub(super) mod inventory;
use inventory::RuntimeInventory;

const MAXIMUM_VALIDATION_TIME: Duration = Duration::from_secs(60);
const STANDARD_VALIDATION_TIME: Duration = Duration::from_secs(30);

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
    training_job: Option<index::TrainingJobBinding>,
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
            training_job: None,
        })
    }
}

/// Dataset verification and elapsed-time bounds for each admitted model operation.
#[derive(Clone, Copy, Debug)]
pub struct ProductionModelRuntimeLimits {
    dataset_verification: PythonDatasetVerificationLimits,
    validation_time: Duration,
}

impl ProductionModelRuntimeLimits {
    /// Constructs per-operation dataset verification and validation bounds.
    ///
    /// # Errors
    ///
    /// Rejects an empty or longer-than-60-second validation window.
    pub fn try_new(
        dataset_verification: PythonDatasetVerificationLimits,
        validation_time: Duration,
    ) -> Result<Self, ProductionModelRuntimeError> {
        if validation_time.is_zero() || validation_time > MAXIMUM_VALIDATION_TIME {
            return Err(ProductionModelRuntimeError::InvalidLimits);
        }
        Ok(Self {
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
    head: ModelInventoryHead,
    publication_unresolved: bool,
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
    pub(super) image: Arc<ModelReadImage>,
}

pub(super) struct RetainedRuntimeBackupEntry {
    pub(super) coordinate: RuntimeBackupCoordinate,
    pub(super) record: ModelInventoryRecord,
    pub(super) sequence: u64,
}

impl RetainedRuntimeBackup {
    pub(super) fn contains(
        &self,
        model_id: &str,
        bundle_id: &str,
        version: u64,
    ) -> Result<bool, ProductionModelRuntimeError> {
        let id = BundleId::try_new(bundle_id)
            .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
        let version =
            NonZeroU64::new(version).ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
        match &self.image.registry {
            super::read_image::ModelBundleInventory::Disk(inventory) => Ok(inventory
                .admission(&id, version)?
                .is_some_and(|entry| entry.model_id.to_string() == model_id)),
            super::read_image::ModelBundleInventory::Memory(registry) => Ok(registry
                .get(&id, version)?
                .is_some_and(|bundle| bundle.metadata().model_id().to_string() == model_id)),
        }
    }
    pub(super) fn page(
        &self,
        after: u64,
    ) -> Result<Vec<RetainedRuntimeBackupEntry>, ProductionModelRuntimeError> {
        let super::read_image::ModelBundleInventory::Disk(inventory) = &self.image.registry else {
            return Ok(Vec::new());
        };
        inventory
            .catalog
            .page(inventory.head, after)?
            .into_iter()
            .map(|entry| {
                let record = entry.admission.clone();
                let sequence = entry.head.sequence;
                let admission = RuntimeInventory::decode(entry)?;
                Ok(RetainedRuntimeBackupEntry {
                    coordinate: RuntimeBackupCoordinate {
                        candidate_directory: admission.candidate_directory,
                        metadata_path: admission.metadata_path,
                        model_id: admission.model_id,
                        bundle_id: admission.bundle_id,
                        bundle_version: admission.bundle_version,
                    },
                    record,
                    sequence,
                })
            })
            .collect()
    }
    pub(super) fn bundle(
        &self,
        coordinate: &RuntimeBackupCoordinate,
    ) -> Result<Arc<ModelBundle>, ProductionModelRuntimeError> {
        self.image
            .registry
            .get(&coordinate.bundle_id, coordinate.bundle_version)?
            .ok_or(ProductionModelRuntimeError::CorruptRuntime)
    }
}

pub(super) struct RetainedForecastRuntime {
    pub(super) generation_sha256: Sha256Digest,
    pub(super) image: Arc<super::read_image::ModelReadImage>,
}

impl fmt::Debug for RetainedForecastRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedForecastRuntime")
            .field("generation_sha256", &self.generation_sha256)
            .field("backend_count", &self.image.backends.len())
            .finish()
    }
}

impl fmt::Debug for RetainedRuntimeBackup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedRuntimeBackup")
            .field("canonical_index", &"[CANONICAL MODEL RUNTIME INDEX]")
            .field("model_count", &self.image.len())
            .finish()
    }
}

/// Application-owned durable model admission and backend recovery authority.
pub struct ProductionModelRuntime {
    paths: LocalPaths,
    catalog: Option<ModelInventoryCatalogCapability>,
    feature_registry: Arc<ProductionFeatureRegistry>,
    training_environment: Option<VerifiedTrainingEnvironment>,
    onnx_worker: Option<OnnxWorkerProgram>,
    limits: ProductionModelRuntimeLimits,
    gate: Mutex<RuntimeGate>,
    admission_validation: Mutex<()>,
    read_image: Arc<ModelReadImageState>,
}

impl ProductionModelRuntime {
    pub(super) fn empty_backup() -> Result<RetainedRuntimeBackup, ProductionModelRuntimeError> {
        let snapshot = Self::empty_snapshot()?;
        Ok(RetainedRuntimeBackup {
            canonical_index: serde_json::to_vec(&ModelInventoryHead::empty())
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
                .into_boxed_slice(),
            image: snapshot.read_image.load(),
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
    /// The durable catalog head is read without loading admitted models. This lets
    /// application composition distinguish a genuinely fresh model namespace from an existing
    /// runtime that must not be hidden when its verified training-environment capability is
    /// unavailable.
    ///
    /// # Errors
    ///
    /// Returns a typed local-path, persistence, index-validation, or resource error.
    pub fn has_durable_admissions(
        catalog: ModelInventoryCatalogCapability,
    ) -> Result<bool, ProductionModelRuntimeError> {
        Ok(catalog.head()?.sequence != 0)
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
    pub fn empty_snapshot() -> Result<ModelRuntimeSnapshot, ProductionModelRuntimeError> {
        let registry = Arc::new(ModelRegistry::empty());
        let image = Arc::new(ModelReadImage::try_new(registry, Vec::new())?);
        Ok(ModelRuntimeSnapshot {
            read_image: Arc::new(ModelReadImageState::new(image)),
        })
    }

    /// Opens and verifies the durable inventory without materializing historical model artifacts.
    ///
    /// A selected generation is independently recovered on demand. Compilation and retirement
    /// share one explicit execution slot across all retained inventory images.
    ///
    /// # Errors
    ///
    /// Returns a typed local-path, persistence, dataset, bundle, registry, policy, worker, or
    /// aggregate-deadline error without publishing a partial runtime.
    pub fn try_open(
        paths: &LocalPaths,
        catalog: ModelInventoryCatalogCapability,
        training_environment: VerifiedTrainingEnvironment,
        onnx_worker: Option<OnnxWorkerProgram>,
        limits: ProductionModelRuntimeLimits,
    ) -> Result<Self, ProductionModelRuntimeError> {
        let head = catalog.head()?;
        catalog.verify(head)?;
        let feature_registry = Arc::new(ProductionFeatureRegistry::try_new()?);
        let inventory = Arc::new(RuntimeInventory::new(
            paths.clone(),
            catalog.clone(),
            head,
            Arc::clone(&feature_registry),
            onnx_worker.clone(),
            limits,
        ));
        let image = Arc::new(ModelReadImage::from_inventory(inventory));
        Ok(Self {
            paths: paths.clone(),
            catalog: Some(catalog),
            feature_registry,
            training_environment: Some(training_environment),
            onnx_worker,
            limits,
            admission_validation: Mutex::new(()),
            gate: Mutex::new(RuntimeGate {
                head,
                publication_unresolved: false,
            }),
            read_image: Arc::new(ModelReadImageState::new(image)),
        })
    }

    /// Durably admits one exact candidate or recognizes a fully identical replay.
    ///
    /// The method accepts no arbitrary local path or executable. It revalidates the configured
    /// catalog selection and current training release, validates the new candidate in the shared
    /// execution slot, commits one immutable catalog row, then publishes its inventory fence.
    ///
    /// # Errors
    ///
    /// A prepublication failure leaves the runtime unchanged. An uncertain durable write retains
    /// the candidate and blocks further admissions and backups until exact restart reconciliation.
    pub fn admit(
        &self,
        request: ModelAdmissionRequest,
    ) -> Result<ModelAdmissionReceipt, ProductionModelRuntimeError> {
        self.admit_with_cancellation(request, &CancellationToken::new())
    }

    /// The owning job retains its terminal publication permit through this existing commit.
    pub(crate) fn admit_with_cancellation(
        &self,
        request: ModelAdmissionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ModelAdmissionReceipt, ProductionModelRuntimeError> {
        let deadline = validation_deadline(self.limits.validation_time)?;
        check_admission_control(deadline, cancellation)?;
        // Reserve admission validation before reading artifact payloads; concurrent admissions
        // cannot each allocate an independently verified candidate.
        let _admission_validation = self
            .admission_validation
            .try_lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let root = open_candidate_root(&self.paths, &request.candidate_directory)?;
        let candidate = self.verify_candidate(&root, &request, deadline, cancellation)?;
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
            dataset_product_contract: dataset.product_contract(),
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
            training_job: request.training_job,
            product_summary: super::product_model_summary(&bundle)
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?,
        };
        check_admission_control(deadline, cancellation)?;
        let mut gate = self
            .gate
            .try_lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        if gate.publication_unresolved {
            return Err(ProductionModelRuntimeError::PublicationUnresolved);
        }
        let bundle = Arc::new(bundle);
        let current = self.read_image.load();
        let super::read_image::ModelBundleInventory::Disk(previous) = &current.registry else {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        };
        // Admission and inference share the same execution slot, including compilation and retirement.
        let _validation = previous.validate_candidate(
            Arc::clone(&bundle),
            &admission.runtime_policy,
            deadline,
            cancellation,
        )?;
        check_admission_control(deadline, cancellation)?;
        let record = ModelInventoryRecord {
            model_id: admission.model_id,
            model_token: super::forecast_model_evidence_projection(&bundle)
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
                .model_token(),
            bundle_id: admission.bundle_id.as_str().to_owned(),
            bundle_version: admission.bundle_version,
            candidate_directory: admission.candidate_directory.to_string(),
            record: admission.encode_record()?,
        };
        let (head, inserted) = match self
            .catalog
            .as_ref()
            .ok_or(ProductionModelRuntimeError::RuntimeUnavailable)?
            .publish(&record)
        {
            Ok(receipt) => receipt,
            Err(ModelInventoryError::Storage(_)) => {
                gate.publication_unresolved = true;
                return Err(ProductionModelRuntimeError::PublicationUnresolved);
            }
            Err(error) => return Err(error.into()),
        };
        let inventory = Arc::new(previous.with_head(head));
        inventory.remember(&bundle)?;
        gate.head = head;
        self.read_image
            .publish(Arc::new(ModelReadImage::from_inventory(inventory)));
        if !inserted {
            return Ok(receipt(
                &admission,
                ModelAdmissionDisposition::AlreadyAdmitted,
            ));
        }

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
        if gate.publication_unresolved {
            return Err(ProductionModelRuntimeError::PublicationUnresolved);
        }
        Ok(RetainedRuntimeBackup {
            canonical_index: serde_json::to_vec(&gate.head)
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
                .into_boxed_slice(),
            image: self.read_image.load(),
        })
    }

    pub(super) fn retain_forecast_runtime(
        &self,
    ) -> Result<RetainedForecastRuntime, ProductionModelRuntimeError> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        if gate.publication_unresolved {
            return Err(ProductionModelRuntimeError::PublicationUnresolved);
        }
        Ok(RetainedForecastRuntime {
            generation_sha256: Sha256Digest::new(gate.head.sha256),
            image: self.read_image.load(),
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
        let observed = Sha256Digest::new(gate.head.sha256);
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

    pub(super) fn decode_backup_head(
        bytes: &[u8],
    ) -> Result<ModelInventoryHead, ProductionModelRuntimeError> {
        let head: ModelInventoryHead = serde_json::from_slice(bytes)
            .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
        if serde_json::to_vec(&head).map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
            != bytes
        {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        Ok(head)
    }

    pub(super) fn validate_backup_record(
        record: &ModelInventoryRecord,
    ) -> Result<RuntimeBackupCoordinate, ProductionModelRuntimeError> {
        let admission = IndexAdmission::decode_record(&record.record)?;
        if admission
            .product_summary
            .get("modelToken")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse::<uuid::Uuid>().ok())
            != Some(record.model_token)
            || admission.model_id != record.model_id
            || admission.bundle_id.as_str() != record.bundle_id
            || admission.bundle_version != record.bundle_version
            || admission.candidate_directory.as_ref() != record.candidate_directory
        {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        Ok(RuntimeBackupCoordinate {
            candidate_directory: admission.candidate_directory,
            metadata_path: admission.metadata_path,
            model_id: admission.model_id,
            bundle_id: admission.bundle_id,
            bundle_version: admission.bundle_version,
        })
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
        let snapshot = Self::empty_snapshot()?;
        Ok(Self {
            paths: paths.clone(),
            catalog: None,
            feature_registry: Arc::new(ProductionFeatureRegistry::try_new()?),
            training_environment: None,
            onnx_worker: None,
            limits,
            admission_validation: Mutex::new(()),
            gate: Mutex::new(RuntimeGate {
                head: ModelInventoryHead::empty(),
                publication_unresolved: false,
            }),
            read_image: snapshot.read_image,
        })
    }

    fn verify_candidate(
        &self,
        root: &ControlledModelRoot,
        request: &ModelAdmissionRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
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
            cancellation,
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

impl fmt::Debug for ProductionModelRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionModelRuntime")
            .field("paths", &"[PREPARED LOCAL PATHS]")
            .field("catalog", &self.catalog)
            .field("feature_registry", &self.feature_registry)
            .field("training_environment", &"[VERIFIED TRAINING ENVIRONMENT]")
            .field("onnx_worker", &self.onnx_worker.is_some())
            .field("limits", &self.limits)
            .field("gate", &"[DURABLE MODEL RUNTIME]")
            .finish()
    }
}

fn validate_recovered_bundle(
    bundle: &ModelBundle,
    admission: &IndexAdmission,
) -> Result<(), ProductionModelRuntimeError> {
    validate_recovered_metadata(bundle.metadata(), admission)?;
    if super::product_model_summary(bundle)
        .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
        != admission.product_summary
    {
        return Err(ProductionModelRuntimeError::CorruptRuntime);
    }
    Ok(())
}

fn validate_recovered_metadata(
    metadata: &market_squawk_modeling::ModelMetadata,
    admission: &IndexAdmission,
) -> Result<(), ProductionModelRuntimeError> {
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
            let derived = OnnxModelPolicy::try_new_for_bundle(
                bundle,
                policy.opset(),
                policy.input_shape(),
                policy.output_shape(),
                policy.inference_deadline(),
                policy.fallback(),
            )
            .map_err(|_| ProductionModelRuntimeError::BackendPolicyMismatch)?;
            if derived != policy {
                return Err(ProductionModelRuntimeError::BackendPolicyMismatch);
            }
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

impl ModelAdmissionRequest {
    pub(crate) fn bind_training_job(
        &mut self,
        context: &market_squawk_jobs::JobRunContext,
        stderr: &market_squawk_modeling::TrainingWorkerStderrEvidence,
    ) -> Result<(), ProductionModelRuntimeError> {
        let snapshot = context.snapshot();
        if self.training_job.is_some()
            || self.candidate_directory.as_ref()
                != format!(
                    "models/training-{}/generation-{}/candidate",
                    snapshot.id().as_uuid(),
                    snapshot.generation().get()
                )
        {
            return Err(ProductionModelRuntimeError::InvalidAdmission);
        }
        self.training_job = Some(index::TrainingJobBinding {
            id: snapshot.id().as_uuid(),
            generation: NonZeroU64::new(snapshot.generation().get())
                .ok_or(ProductionModelRuntimeError::InvalidAdmission)?,
            input_sha256: snapshot.spec().input().digest().bytes(),
            stderr_bytes: u64::from(stderr.captured_bytes()),
            stderr_sha256: stderr.sha256(),
        });
        Ok(())
    }

    pub(crate) fn require_dataset_authority(
        &self,
        expected: PythonDatasetAdmissionAuthority,
    ) -> Result<(), ProductionModelRuntimeError> {
        if self.dataset == expected {
            Ok(())
        } else {
            Err(ProductionModelRuntimeError::CandidateEvidenceMismatch)
        }
    }
}

impl ProductionModelRuntime {
    pub(crate) fn training_model_token(
        &self,
        receipt: &ModelAdmissionReceipt,
    ) -> Result<uuid::Uuid, ProductionModelRuntimeError> {
        let image = self.read_image.load();
        let bundle = image
            .registry
            .get(receipt.bundle_id(), receipt.bundle_version())?
            .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
        if bundle.metadata().metadata_hash() != receipt.metadata_sha256() {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        super::forecast_model_evidence_projection(&bundle)
            .map(|value| value.model_token())
            .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)
    }

    /// Resolves a durable admission only when its owning job binding also matches.
    pub(crate) fn training_admission(
        &self,
        snapshot: &market_squawk_jobs::JobSnapshot,
    ) -> Result<Option<(ModelAdmissionReceipt, Sha256Digest)>, ProductionModelRuntimeError> {
        let gate = self
            .gate
            .try_lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        if gate.publication_unresolved {
            return Err(ProductionModelRuntimeError::PublicationUnresolved);
        }
        let Some(catalog) = &self.catalog else {
            return Ok(None);
        };
        let mut after = 0;
        let mut value = None;
        loop {
            let page = catalog.page(gate.head, after)?;
            if page.is_empty() {
                break;
            }
            for record in page {
                after = record.head.sequence;
                let entry = RuntimeInventory::decode(record)?;
                if entry.training_job.as_ref().is_some_and(|job| {
                    job.id == snapshot.id().as_uuid()
                        && job.generation.get() == snapshot.generation().get()
                        && job.input_sha256 == snapshot.spec().input().digest().bytes()
                }) {
                    if value.is_some() {
                        return Err(ProductionModelRuntimeError::CorruptRuntime);
                    }
                    value = Some((
                        receipt(&entry, ModelAdmissionDisposition::AlreadyAdmitted),
                        entry
                            .training_result_sha256()
                            .ok_or(ProductionModelRuntimeError::CorruptRuntime)?,
                    ));
                }
            }
        }

        Ok(value)
    }
}

fn check_admission_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductionModelRuntimeError> {
    if cancellation.is_cancelled() {
        Err(market_squawk_modeling::ModelAdmissionError::Dataset(
            market_squawk_data::PythonDatasetCatalogError::Cancelled,
        )
        .into())
    } else if Instant::now() >= deadline {
        Err(ProductionModelRuntimeError::ValidationDeadline)
    } else {
        Ok(())
    }
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
    /// Durable indexed inventory publication or integrity failed.
    #[error(transparent)]
    Inventory(#[from] ModelInventoryError),
    /// A durable index write has an unknown outcome; its candidate files must be retained.
    #[error("production model publication requires exact restart reconciliation")]
    PublicationUnresolved,
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
