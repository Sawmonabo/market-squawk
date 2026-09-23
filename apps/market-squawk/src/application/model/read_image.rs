use std::sync::Arc;

use arc_swap::ArcSwap;
use market_squawk_modeling::{InferenceBackend, ModelRegistry};

use super::{ModelDomainServiceError, bundle_coordinate, model_coordinate};

pub(super) struct ModelReadImage {
    pub(super) registry: Arc<ModelRegistry>,
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
            registry,
            backends: backends.into_boxed_slice(),
        })
    }

    pub(super) fn len(&self) -> usize {
        self.backends.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.backends.is_empty()
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
