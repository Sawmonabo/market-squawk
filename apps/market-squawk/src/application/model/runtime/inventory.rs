//! Disk inventory reads and explicit, serialized active inference leases.

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{Arc, Condvar, Mutex, Weak},
    time::{Duration, Instant},
};

use market_squawk_data::{
    ModelInventoryCatalogCapability, ModelInventoryEntry, ModelInventoryHead,
};
use market_squawk_domain::ModelId;
use market_squawk_modeling::{
    BundleId, BundleMetadataRef, InferenceBackend, ModelBundle, OnnxWorkerProgram,
    ProductionFeatureRegistry, recover_model_candidate,
};
use market_squawk_platform::LocalPaths;
use tokio_util::sync::CancellationToken;

use super::{
    ProductionModelRuntimeError, ProductionModelRuntimeLimits, build_backend,
    check_admission_control, index::IndexAdmission, open_candidate_root, validate_recovered_bundle,
    validation_deadline,
};

pub(in crate::application::model) struct RuntimeInventory {
    pub(in crate::application::model) catalog: ModelInventoryCatalogCapability,
    pub(in crate::application::model) head: ModelInventoryHead,
    paths: LocalPaths,
    features: Arc<ProductionFeatureRegistry>,
    worker: Option<OnnxWorkerProgram>,
    limits: ProductionModelRuntimeLimits,
    shared: Arc<SharedModels>,
}

struct SharedModels {
    bundles: Mutex<BTreeMap<(String, u64), Weak<ModelBundle>>>,
    execution: Mutex<ExecutionState>,
    ready: Condvar,
}

#[derive(Default)]
struct ExecutionState {
    active: Option<Arc<ActiveModel>>,
    busy: bool,
    failed: bool,
}

struct ActiveModel {
    bundle: Arc<ModelBundle>,
    backend: Arc<dyn InferenceBackend>,
}

/// Holds one execution slot; acquisition and model compilation precede inference.
pub(in crate::application::model) struct ActiveModelLease {
    active: Arc<ActiveModel>,
    owner: Option<Arc<SharedModels>>,
}

impl ActiveModelLease {
    pub(in crate::application::model) fn fixture(
        bundle: Arc<ModelBundle>,
        backend: Arc<dyn InferenceBackend>,
    ) -> Self {
        Self {
            active: Arc::new(ActiveModel { bundle, backend }),
            owner: None,
        }
    }
    pub(in crate::application::model) fn backend(&self) -> &dyn InferenceBackend {
        self.active.backend.as_ref()
    }
    pub(in crate::application::model) fn bundle(&self) -> &Arc<ModelBundle> {
        &self.active.bundle
    }
}

impl Drop for ActiveModelLease {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            if let Ok(mut state) = owner.execution.lock() {
                // Idle models remain durable in the catalog, not in a worker process.
                // Retirement must finish before another model receives this execution slot.
                if self.active.backend.retire().is_err() {
                    state.failed = true;
                }
                state.active = None;
                state.busy = false;
            }
            owner.ready.notify_all();
        }
    }
}

impl RuntimeInventory {
    pub(super) fn new(
        paths: LocalPaths,
        catalog: ModelInventoryCatalogCapability,
        head: ModelInventoryHead,
        features: Arc<ProductionFeatureRegistry>,
        worker: Option<OnnxWorkerProgram>,
        limits: ProductionModelRuntimeLimits,
    ) -> Self {
        Self {
            paths,
            catalog,
            head,
            features,
            worker,
            limits,
            shared: Arc::new(SharedModels {
                bundles: Mutex::new(BTreeMap::new()),
                execution: Mutex::new(ExecutionState::default()),
                ready: Condvar::new(),
            }),
        }
    }

    pub(super) fn with_head(&self, head: ModelInventoryHead) -> Self {
        Self {
            paths: self.paths.clone(),
            catalog: self.catalog.clone(),
            head,
            features: Arc::clone(&self.features),
            worker: self.worker.clone(),
            limits: self.limits,
            shared: Arc::clone(&self.shared),
        }
    }

    pub(in crate::application::model) fn decode(
        entry: ModelInventoryEntry,
    ) -> Result<IndexAdmission, ProductionModelRuntimeError> {
        let record = entry.admission;
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
        Ok(admission)
    }

    pub(in crate::application::model) fn page(
        &self,
        after: u64,
    ) -> Result<Vec<(u64, IndexAdmission)>, ProductionModelRuntimeError> {
        self.catalog
            .page(self.head, after)?
            .into_iter()
            .map(|entry| {
                let sequence = entry.head.sequence;
                Ok((sequence, Self::decode(entry)?))
            })
            .collect()
    }

    pub(in crate::application::model) fn page_window(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<
        (Vec<IndexAdmission>, Option<String>, ModelInventoryHead),
        ProductionModelRuntimeError,
    > {
        if limit == 0 || limit > 100 {
            return Err(ProductionModelRuntimeError::InvalidAdmission);
        }
        let position = match cursor {
            None => InventoryCursor {
                fence: self.head,
                after: 0,
            },
            Some(value) if value.len() <= 512 => {
                let decoded: InventoryCursor = serde_json::from_str(value)
                    .map_err(|_| ProductionModelRuntimeError::InvalidAdmission)?;
                if serde_json::to_string(&decoded)
                    .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?
                    != value
                    || decoded.fence.sequence > self.head.sequence
                    || decoded.after > decoded.fence.sequence
                {
                    return Err(ProductionModelRuntimeError::InvalidAdmission);
                }
                decoded
            }
            Some(_) => return Err(ProductionModelRuntimeError::InvalidAdmission),
        };
        let mut after = position.after;
        let mut admissions = Vec::with_capacity(limit);
        while admissions.len() < limit {
            let page = self.catalog.page(position.fence, after)?;
            if page.is_empty() {
                break;
            }
            for entry in page.into_iter().take(limit - admissions.len()) {
                after = entry.head.sequence;
                admissions.push(Self::decode(entry)?);
            }
        }
        let next = if after < position.fence.sequence {
            Some(
                serde_json::to_string(&InventoryCursor {
                    fence: position.fence,
                    after,
                })
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?,
            )
        } else {
            None
        };
        Ok((admissions, next, position.fence))
    }

    pub(in crate::application::model) fn admission(
        &self,
        id: &BundleId,
        version: NonZeroU64,
    ) -> Result<Option<IndexAdmission>, ProductionModelRuntimeError> {
        self.catalog
            .get(self.head, id.as_str(), version)?
            .map(Self::decode)
            .transpose()
    }

    pub(in crate::application::model) fn latest(
        &self,
        model: ModelId,
    ) -> Result<Option<Arc<ModelBundle>>, ProductionModelRuntimeError> {
        self.catalog
            .latest(self.head, model)?
            .map(Self::decode)
            .transpose()?
            .map(|entry| self.load(&entry))
            .transpose()
    }

    pub(in crate::application::model) fn get(
        &self,
        id: &BundleId,
        version: NonZeroU64,
    ) -> Result<Option<Arc<ModelBundle>>, ProductionModelRuntimeError> {
        self.admission(id, version)?
            .map(|entry| self.load(&entry))
            .transpose()
    }

    pub(in crate::application::model) fn selection_controlled(
        &self,
        admission: &IndexAdmission,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_modeling::ModelSelectionMetadata, ProductionModelRuntimeError> {
        check_admission_control(deadline, cancellation)?;
        let root = open_candidate_root(&self.paths, &admission.candidate_directory)?;
        let reference =
            BundleMetadataRef::try_new(&admission.metadata_path, admission.metadata_sha256)
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
        let selection = market_squawk_modeling::recover_model_selection_metadata(
            &root,
            &reference,
            &admission.authority_bytes,
            admission.authority_sha256,
            self.paths.root(),
            admission.dataset_authority()?,
            &self.features,
            self.limits.dataset_verification,
            deadline,
            cancellation,
        )?;
        super::validate_recovered_metadata(selection.metadata(), admission)?;
        let evidence = super::super::forecast_model_evidence_projection_parts(
            selection.metadata(),
            selection.training_run_bytes(),
            selection.residual_distribution_available(),
            None,
        )
        .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
        if serde_json::json!({"modelToken":evidence.model_token(),"label":selection.metadata().label().name(),"evidenceState":evidence.overall().as_str()})
            != admission.product_summary
        {
            return Err(ProductionModelRuntimeError::CorruptRuntime);
        }
        Ok(selection)
    }

    pub(in crate::application::model) fn load(
        &self,
        admission: &IndexAdmission,
    ) -> Result<Arc<ModelBundle>, ProductionModelRuntimeError> {
        self.load_controlled(
            admission,
            validation_deadline(self.limits.validation_time)?,
            &CancellationToken::new(),
        )
    }

    pub(in crate::application::model) fn load_controlled(
        &self,
        admission: &IndexAdmission,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<ModelBundle>, ProductionModelRuntimeError> {
        let key = (
            admission.bundle_id.as_str().to_owned(),
            admission.bundle_version.get(),
        );
        let mut bundles = lock_controlled(&self.shared.bundles, deadline, cancellation)?;
        bundles.retain(|_, value| value.strong_count() != 0);
        if let Some(bundle) = bundles.get(&key).and_then(Weak::upgrade) {
            validate_recovered_bundle(&bundle, admission)?;
            return Ok(bundle);
        }
        let root = open_candidate_root(&self.paths, &admission.candidate_directory)?;
        let reference =
            BundleMetadataRef::try_new(&admission.metadata_path, admission.metadata_sha256)
                .map_err(|_| ProductionModelRuntimeError::CorruptRuntime)?;
        let bundle = Arc::new(
            recover_model_candidate(
                &root,
                &reference,
                &admission.authority_bytes,
                admission.authority_sha256,
                self.paths.root(),
                admission.dataset_authority()?,
                &self.features,
                self.limits.dataset_verification,
                deadline,
                cancellation,
            )?
            .into_bundle(),
        );
        validate_recovered_bundle(&bundle, admission)?;
        bundles.insert(key, Arc::downgrade(&bundle));
        Ok(bundle)
    }

    pub(super) fn remember(
        &self,
        bundle: &Arc<ModelBundle>,
    ) -> Result<(), ProductionModelRuntimeError> {
        let mut bundles = self
            .shared
            .bundles
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        bundles.retain(|_, value| value.strong_count() != 0);
        bundles.insert(
            (
                bundle.metadata().bundle_id().as_str().to_owned(),
                bundle.metadata().bundle_version().get(),
            ),
            Arc::downgrade(bundle),
        );
        Ok(())
    }

    pub(in crate::application::model) fn activate(
        &self,
        id: &BundleId,
        version: NonZeroU64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ActiveModelLease, ProductionModelRuntimeError> {
        let admission = self
            .admission(id, version)?
            .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
        let mut state = self.reserve(deadline, cancellation)?;
        if let Some(active) = &state.active {
            if active.bundle.metadata().bundle_id() == id
                && active.bundle.metadata().bundle_version() == version
            {
                let active = Arc::clone(active);
                state.busy = true;
                return Ok(ActiveModelLease {
                    active,
                    owner: Some(Arc::clone(&self.shared)),
                });
            }
        }
        self.retire_active(&mut state)?;
        let bundle = self.load_controlled(&admission, deadline, cancellation)?;
        self.compile_reserved(
            state,
            bundle,
            &admission.runtime_policy,
            deadline,
            cancellation,
        )
    }

    pub(super) fn validate_candidate(
        &self,
        bundle: Arc<ModelBundle>,
        policy: &super::index::StoredRuntimePolicy,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ActiveModelLease, ProductionModelRuntimeError> {
        let mut state = self.reserve(deadline, cancellation)?;
        if let Some(active) = &state.active {
            if active.bundle.metadata() == bundle.metadata() {
                let active = Arc::clone(active);
                state.busy = true;
                return Ok(ActiveModelLease {
                    active,
                    owner: Some(Arc::clone(&self.shared)),
                });
            }
        }
        self.retire_active(&mut state)?;
        self.compile_reserved(state, bundle, policy, deadline, cancellation)
    }

    pub(in crate::application::model) fn shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), ProductionModelRuntimeError> {
        let cancellation = CancellationToken::new();
        let mut state = self.reserve(deadline, &cancellation)?;
        self.retire_active(&mut state)?;
        state.failed = true;
        Ok(())
    }

    fn reserve(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<std::sync::MutexGuard<'_, ExecutionState>, ProductionModelRuntimeError> {
        let mut state = lock_controlled(&self.shared.execution, deadline, cancellation)?;
        while state.busy {
            check_admission_control(deadline, cancellation)?;
            state = self
                .shared
                .ready
                .wait_timeout(
                    state,
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(20)),
                )
                .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?
                .0;
        }
        check_admission_control(deadline, cancellation)?;
        if state.failed {
            return Err(ProductionModelRuntimeError::RuntimeUnavailable);
        }
        Ok(state)
    }

    fn retire_active(&self, state: &mut ExecutionState) -> Result<(), ProductionModelRuntimeError> {
        if let Some(active) = &state.active {
            if active.backend.retire().is_err() {
                state.failed = true;
                return Err(ProductionModelRuntimeError::RuntimeUnavailable);
            }
        }
        state.active = None;
        Ok(())
    }

    fn compile_reserved(
        &self,
        mut state: std::sync::MutexGuard<'_, ExecutionState>,
        bundle: Arc<ModelBundle>,
        policy: &super::index::StoredRuntimePolicy,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ActiveModelLease, ProductionModelRuntimeError> {
        // Compilation owns the slot, while waiters can still observe cancellation and deadlines.
        state.busy = true;
        drop(state);
        let result = build_backend(Arc::clone(&bundle), policy, self.worker.as_ref());
        let mut state = self
            .shared
            .execution
            .lock()
            .map_err(|_| ProductionModelRuntimeError::RuntimeUnavailable)?;
        let backend = match result {
            Ok(backend) => backend,
            Err(error) => {
                if matches!(
                    &error,
                    ProductionModelRuntimeError::Onnx(
                        market_squawk_modeling::OnnxBackendError::TerminationUncertain
                    )
                ) {
                    state.failed = true;
                }
                state.busy = false;
                self.shared.ready.notify_all();
                return Err(error);
            }
        };
        if let Err(error) = check_admission_control(deadline, cancellation) {
            if backend.retire().is_err() {
                state.failed = true;
            }
            state.busy = false;
            self.shared.ready.notify_all();
            return Err(error);
        }
        let active = Arc::new(ActiveModel { bundle, backend });
        state.active = Some(Arc::clone(&active));
        Ok(ActiveModelLease {
            active,
            owner: Some(Arc::clone(&self.shared)),
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryCursor {
    fence: ModelInventoryHead,
    after: u64,
}

fn lock_controlled<'a, T>(
    mutex: &'a Mutex<T>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<std::sync::MutexGuard<'a, T>, ProductionModelRuntimeError> {
    loop {
        check_admission_control(deadline, cancellation)?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(ProductionModelRuntimeError::RuntimeUnavailable);
            }
            Err(std::sync::TryLockError::WouldBlock) => std::thread::sleep(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10)),
            ),
        }
    }
}
