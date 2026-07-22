//! Atomic, immutable, count- and byte-bounded model generation registry.

use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::{Arc, RwLock};

use market_squawk_domain::ModelId;
use thiserror::Error;

use crate::{BundleId, ModelBundle};

/// Maximum immutable generations in one process registry.
pub const MAX_MODEL_REGISTRY_GENERATIONS: usize = 4_096;
/// Maximum configured retained footprint for one process registry.
pub const MAX_MODEL_REGISTRY_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Result of an immutable generation registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BundleRegistration {
    /// A new immutable coordinate was inserted.
    Inserted,
    /// The exact metadata and artifact bytes were already retained.
    AlreadyRegistered,
}

/// Thread-safe immutable generation owner.
///
/// Registration holds one write lock across conflict and resource preflight and the final slot
/// mutation. Readers receive `Arc` snapshots, so every prior generation remains reproducible.
#[derive(Debug)]
pub struct ModelRegistry {
    state: RwLock<RegistryState>,
    retained_byte_limit: NonZeroUsize,
}

#[derive(Debug)]
struct RegistryState {
    slots: Box<[Option<Arc<ModelBundle>>]>,
    len: usize,
    retained_bytes: usize,
}

impl ModelRegistry {
    /// Creates an empty registry with fixed slot and retained-byte ceilings.
    ///
    /// # Errors
    ///
    /// Rejects limits above production ceilings or below fixed registry storage.
    pub fn try_new(
        maximum_generations: NonZeroUsize,
        retained_byte_limit: NonZeroUsize,
    ) -> Result<Self, ModelRegistryError> {
        if maximum_generations.get() > MAX_MODEL_REGISTRY_GENERATIONS {
            return Err(ModelRegistryError::RegistryCapacityTooLarge);
        }
        if retained_byte_limit.get() > MAX_MODEL_REGISTRY_RETAINED_BYTES {
            return Err(ModelRegistryError::RetainedByteLimitTooLarge);
        }
        let fixed = size_of::<Self>()
            .checked_add(size_of::<RegistryState>())
            .and_then(|bytes| {
                size_of::<Option<Arc<ModelBundle>>>()
                    .checked_mul(maximum_generations.get())
                    .and_then(|slots| bytes.checked_add(slots))
            })
            .ok_or(ModelRegistryError::RetainedSizeOverflow)?;
        if fixed > retained_byte_limit.get() {
            return Err(ModelRegistryError::RetainedByteLimitTooSmall);
        }
        let slots = std::iter::repeat_with(|| None)
            .take(maximum_generations.get())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            state: RwLock::new(RegistryState {
                slots,
                len: 0,
                retained_bytes: fixed,
            }),
            retained_byte_limit,
        })
    }

    /// Atomically retains one immutable generation or recognizes an exact replay.
    ///
    /// # Errors
    ///
    /// A coordinate conflict, count/byte exhaustion, accounting overflow, or poisoned registry
    /// leaves the registry unchanged.
    pub fn try_register(
        &self,
        bundle: ModelBundle,
    ) -> Result<BundleRegistration, ModelRegistryError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| ModelRegistryError::RegistryUnavailable)?;
        for existing in state.slots[..state.len].iter().filter_map(Option::as_ref) {
            if existing.metadata().bundle_id() == bundle.metadata().bundle_id()
                && existing.metadata().model_id() != bundle.metadata().model_id()
            {
                return Err(ModelRegistryError::BundleSeriesConflict);
            }
            if existing.metadata().model_id() == bundle.metadata().model_id()
                && existing.metadata().bundle_id() != bundle.metadata().bundle_id()
            {
                return Err(ModelRegistryError::ModelSeriesConflict);
            }
        }
        let position = position(
            &state,
            bundle.metadata().bundle_id(),
            bundle.metadata().bundle_version(),
        );
        match position {
            Ok(index) => {
                let existing = state.slots[index]
                    .as_ref()
                    .ok_or(ModelRegistryError::RegistryUnavailable)?;
                if existing.metadata().metadata_hash() == bundle.metadata().metadata_hash()
                    && existing.metadata().artifact_hash() == bundle.metadata().artifact_hash()
                    && existing.metadata().model_id() == bundle.metadata().model_id()
                {
                    return Ok(BundleRegistration::AlreadyRegistered);
                }
                return Err(ModelRegistryError::GenerationConflict);
            }
            Err(_) if state.len == state.slots.len() => {
                return Err(ModelRegistryError::RegistryFull);
            }
            Err(_) => {}
        }
        let retained_bytes = state
            .retained_bytes
            .checked_add(bundle.retained_bytes())
            .ok_or(ModelRegistryError::RetainedSizeOverflow)?;
        if retained_bytes > self.retained_byte_limit.get() {
            return Err(ModelRegistryError::RetainedByteLimitExceeded);
        }
        let index = match position {
            Ok(_) => return Err(ModelRegistryError::GenerationConflict),
            Err(index) => index,
        };
        for current in (index..state.len).rev() {
            state.slots[current + 1] = state.slots[current].take();
        }
        state.slots[index] = Some(Arc::new(bundle));
        state.len += 1;
        state.retained_bytes = retained_bytes;
        Ok(BundleRegistration::Inserted)
    }

    /// Returns one exact immutable generation without selecting a fallback version.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRegistryError::RegistryUnavailable`] if registry synchronization failed.
    pub fn get(
        &self,
        bundle_id: &BundleId,
        bundle_version: NonZeroU64,
    ) -> Result<Option<Arc<ModelBundle>>, ModelRegistryError> {
        let state = self
            .state
            .read()
            .map_err(|_| ModelRegistryError::RegistryUnavailable)?;
        Ok(position(&state, bundle_id, bundle_version)
            .ok()
            .and_then(|index| state.slots[index].as_ref())
            .map(Arc::clone))
    }

    /// Returns the highest retained generation for one exact model identity.
    ///
    /// This does not delete, replace, or reinterpret earlier generations.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRegistryError::RegistryUnavailable`] if registry synchronization failed.
    pub fn latest(
        &self,
        model_id: ModelId,
    ) -> Result<Option<Arc<ModelBundle>>, ModelRegistryError> {
        let state = self
            .state
            .read()
            .map_err(|_| ModelRegistryError::RegistryUnavailable)?;
        Ok(state.slots[..state.len]
            .iter()
            .filter_map(Option::as_ref)
            .filter(|bundle| bundle.metadata().model_id() == model_id)
            .max_by_key(|bundle| bundle.metadata().bundle_version())
            .map(Arc::clone))
    }

    /// Returns the number of retained immutable generations.
    ///
    /// # Errors
    ///
    /// Returns a typed synchronization failure.
    pub fn len(&self) -> Result<usize, ModelRegistryError> {
        self.state
            .read()
            .map(|state| state.len)
            .map_err(|_| ModelRegistryError::RegistryUnavailable)
    }

    /// Returns whether no generations are retained.
    ///
    /// # Errors
    ///
    /// Returns a typed synchronization failure.
    pub fn is_empty(&self) -> Result<bool, ModelRegistryError> {
        self.len().map(|length| length == 0)
    }

    /// Returns the exact current retained footprint.
    ///
    /// # Errors
    ///
    /// Returns a typed synchronization failure.
    pub fn retained_bytes(&self) -> Result<usize, ModelRegistryError> {
        self.state
            .read()
            .map(|state| state.retained_bytes)
            .map_err(|_| ModelRegistryError::RegistryUnavailable)
    }

    /// Returns the configured retained-byte ceiling.
    #[must_use]
    pub const fn retained_byte_limit(&self) -> NonZeroUsize {
        self.retained_byte_limit
    }
}

/// Model registry construction, registration, or read failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ModelRegistryError {
    /// Configured generation capacity exceeds the production ceiling.
    #[error("model registry generation capacity is too large")]
    RegistryCapacityTooLarge,
    /// Configured byte capacity exceeds the production ceiling.
    #[error("model registry retained-byte capacity is too large")]
    RetainedByteLimitTooLarge,
    /// Configured byte capacity cannot contain fixed registry storage.
    #[error("model registry retained-byte capacity is too small")]
    RetainedByteLimitTooSmall,
    /// Registry has no unoccupied immutable generation slot.
    #[error("model registry is full")]
    RegistryFull,
    /// A generation would exceed the retained-byte ceiling.
    #[error("model registry retained-byte ceiling would be exceeded")]
    RetainedByteLimitExceeded,
    /// An immutable coordinate was reused with different exact bytes or model identity.
    #[error("model registry immutable generation conflicts")]
    GenerationConflict,
    /// One bundle series was associated with more than one model identity.
    #[error("model registry bundle series changed model identity")]
    BundleSeriesConflict,
    /// One model identity was associated with more than one bundle series.
    #[error("model registry model identity changed bundle series")]
    ModelSeriesConflict,
    /// Checked retained-byte arithmetic overflowed.
    #[error("model registry retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// Registry synchronization was poisoned and failed closed.
    #[error("model registry is unavailable")]
    RegistryUnavailable,
}

fn position(
    state: &RegistryState,
    bundle_id: &BundleId,
    bundle_version: NonZeroU64,
) -> Result<usize, usize> {
    state.slots[..state.len].binary_search_by(|slot| {
        slot.as_ref().map_or(std::cmp::Ordering::Less, |bundle| {
            bundle
                .metadata()
                .bundle_id()
                .cmp(bundle_id)
                .then_with(|| bundle.metadata().bundle_version().cmp(&bundle_version))
        })
    })
}
