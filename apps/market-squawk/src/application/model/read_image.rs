use std::sync::Arc;

use super::runtime::{
    ProductionModelRuntimeError,
    inventory::{ActiveModelLease, RuntimeInventory},
};
use arc_swap::ArcSwap;
use market_squawk_domain::ModelId;
use market_squawk_modeling::{BundleId, InferenceBackend, ModelBundle, ModelRegistry};
use std::{num::NonZeroU64, time::Instant};
use tokio_util::sync::CancellationToken;

use super::{ModelDomainServiceError, bundle_coordinate, model_coordinate};

pub(super) struct ModelReadImage {
    pub(super) registry: ModelBundleInventory,
    pub(super) backends: Box<[Arc<dyn InferenceBackend>]>,
}

impl ModelReadImage {
    pub(super) fn try_new(
        registry: Arc<ModelRegistry>,
        mut backends: Vec<Arc<dyn InferenceBackend>>,
    ) -> Result<Self, ModelDomainServiceError> {
        backends.sort_unstable_by(|left, right| {
            model_coordinate(left.metadata()).cmp(&model_coordinate(right.metadata()))
        });
        if backends.windows(2).any(|pair| {
            bundle_coordinate(pair[0].metadata()) == bundle_coordinate(pair[1].metadata())
        }) {
            return Err(ModelDomainServiceError::DuplicateBackend);
        }
        let registry_length = registry
            .len()
            .map_err(|_| ModelDomainServiceError::Registry)?;
        if registry_length != backends.len() {
            return Err(ModelDomainServiceError::IncompleteBackendSet);
        }
        for backend in &backends {
            let metadata = backend.metadata();
            let registered = registry
                .get(metadata.bundle_id(), metadata.bundle_version())
                .map_err(|_| ModelDomainServiceError::Registry)?
                .ok_or(ModelDomainServiceError::IncompleteBackendSet)?;
            if registered.metadata() != metadata {
                return Err(ModelDomainServiceError::BackendIdentityMismatch);
            }
        }
        Ok(Self {
            registry: ModelBundleInventory::Memory(registry),
            backends: backends.into_boxed_slice(),
        })
    }

    pub(super) fn from_inventory(inventory: Arc<RuntimeInventory>) -> Self {
        Self {
            registry: ModelBundleInventory::Disk(inventory),
            backends: Box::new([]),
        }
    }

    pub(super) fn activate(
        &self,
        id: &BundleId,
        version: NonZeroU64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<ActiveModelLease, ProductionModelRuntimeError> {
        match &self.registry {
            ModelBundleInventory::Disk(inventory) => {
                inventory.activate(id, version, deadline, cancellation)
            }
            ModelBundleInventory::Memory(registry) => {
                let bundle = registry
                    .get(id, version)?
                    .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
                let backend = self
                    .backends
                    .iter()
                    .find(|backend| backend.metadata() == bundle.metadata())
                    .cloned()
                    .ok_or(ProductionModelRuntimeError::CorruptRuntime)?;
                Ok(ActiveModelLease::fixture(bundle, backend))
            }
        }
    }

    pub(super) fn len(&self) -> usize {
        self.registry.len().unwrap_or(0)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(super) enum ModelBundleInventory {
    Memory(Arc<ModelRegistry>),
    Disk(Arc<RuntimeInventory>),
}

impl ModelBundleInventory {
    pub(super) fn get(
        &self,
        id: &BundleId,
        version: NonZeroU64,
    ) -> Result<Option<Arc<ModelBundle>>, ProductionModelRuntimeError> {
        match self {
            Self::Memory(registry) => registry.get(id, version).map_err(Into::into),
            Self::Disk(inventory) => inventory.get(id, version),
        }
    }
    pub(super) fn selection(
        &self,
        id: &BundleId,
        version: NonZeroU64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<market_squawk_modeling::ModelSelectionMetadata>, ProductionModelRuntimeError>
    {
        match self {
            Self::Memory(registry) => Ok(registry
                .get(id, version)?
                .map(|bundle| bundle.selection_metadata())),
            Self::Disk(inventory) => inventory
                .admission(id, version)?
                .map(|admission| inventory.selection_controlled(&admission, deadline, cancellation))
                .transpose(),
        }
    }
    pub(super) fn latest(
        &self,
        id: ModelId,
    ) -> Result<Option<Arc<ModelBundle>>, ProductionModelRuntimeError> {
        match self {
            Self::Memory(registry) => registry.latest(id).map_err(Into::into),
            Self::Disk(inventory) => inventory.latest(id),
        }
    }
    pub(super) fn len(&self) -> Result<usize, ProductionModelRuntimeError> {
        match self {
            Self::Memory(registry) => registry.len().map_err(Into::into),
            Self::Disk(inventory) => usize::try_from(inventory.head.sequence)
                .map_err(|_| ProductionModelRuntimeError::ResourceExhausted),
        }
    }
}

pub(super) struct ModelReadImageState {
    current: ArcSwap<ModelReadImage>,
}

impl ModelReadImageState {
    pub(super) fn new(image: Arc<ModelReadImage>) -> Self {
        Self {
            current: ArcSwap::from(image),
        }
    }

    pub(super) fn load(&self) -> Arc<ModelReadImage> {
        self.current.load_full()
    }

    pub(super) fn publish(&self, image: Arc<ModelReadImage>) {
        self.current.store(image);
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc};

    use market_squawk_modeling::ModelRegistry;

    use super::{ModelReadImage, ModelReadImageState};

    #[test]
    fn publication_preserves_existing_readers_and_replaces_future_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        let maximum_bundles = NonZeroUsize::new(2).ok_or("nonzero test bundle ceiling")?;
        let maximum_bytes = NonZeroUsize::new(1_024).ok_or("nonzero test byte ceiling")?;
        let first = Arc::new(ModelReadImage::try_new(
            Arc::new(ModelRegistry::try_new(maximum_bundles, maximum_bytes)?),
            Vec::new(),
        )?);
        let state = ModelReadImageState::new(Arc::clone(&first));
        let retained = state.load();
        let replacement = Arc::new(ModelReadImage::try_new(
            Arc::new(ModelRegistry::try_new(maximum_bundles, maximum_bytes)?),
            Vec::new(),
        )?);

        state.publish(Arc::clone(&replacement));

        assert!(Arc::ptr_eq(&retained, &first));
        assert!(Arc::ptr_eq(&state.load(), &replacement));
        assert!(!Arc::ptr_eq(&retained, &replacement));
        Ok(())
    }
}
