//! Fixed-capacity feature metadata registration and read-only live feature access.

use std::fmt::Debug;
use std::mem::size_of;
use std::num::NonZeroUsize;

use thiserror::Error;

use crate::catalog::is_known_local_implementation;
use crate::{FeatureError, FeatureKey, FeatureMetadata, FeatureScalar, FeatureValue};

/// Maximum entries accepted by one in-process metadata registry.
pub const MAX_FEATURE_REGISTRY_ENTRIES: usize = 4_096;
/// Maximum configured retained-byte limit for one registry.
pub const MAX_FEATURE_REGISTRY_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// Execution plane required by a feature metadata resolution request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum FeatureCompatibility {
    /// Resolve only a feature version declared safe for bounded live execution.
    Live,
    /// Resolve only a feature version that preserves point-in-time research semantics.
    PointInTime,
}

/// Result of an accepted deterministic registration attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationOutcome {
    /// A new key was inserted.
    Inserted,
    /// Identical metadata for this key was already present.
    AlreadyRegistered,
}

/// Aggregate result of one atomically validated metadata batch registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchRegistrationOutcome {
    inserted: usize,
    already_registered: usize,
}

impl BatchRegistrationOutcome {
    /// Returns the number of newly inserted records.
    #[must_use]
    pub const fn inserted(self) -> usize {
        self.inserted
    }

    /// Returns the number of identical records already present.
    #[must_use]
    pub const fn already_registered(self) -> usize {
        self.already_registered
    }
}

/// Fixed-capacity, byte-bounded metadata registry.
///
/// Entries are retained in [`FeatureKey`] order inside a preallocated boxed slice. Registration
/// allocates no registry storage, and duplicate behavior is independent of insertion order.
#[derive(Debug)]
pub struct FeatureRegistry {
    slots: Box<[Option<FeatureMetadata>]>,
    len: usize,
    retained_bytes: usize,
    retained_byte_limit: NonZeroUsize,
}

impl FeatureRegistry {
    /// Constructs an empty registry with fixed count and byte bounds.
    ///
    /// # Errors
    ///
    /// Returns a typed error for excessive limits, accounting overflow, or a byte limit smaller
    /// than the registry's exact fixed allocation.
    pub fn try_new(
        maximum_entries: NonZeroUsize,
        retained_byte_limit: NonZeroUsize,
    ) -> Result<Self, FeatureRegistryError> {
        if maximum_entries.get() > MAX_FEATURE_REGISTRY_ENTRIES {
            return Err(FeatureRegistryError::RegistryCapacityTooLarge);
        }
        if retained_byte_limit.get() > MAX_FEATURE_REGISTRY_RETAINED_BYTES {
            return Err(FeatureRegistryError::RetainedByteLimitTooLarge);
        }
        let fixed = size_of::<Option<FeatureMetadata>>()
            .checked_mul(maximum_entries.get())
            .and_then(|slots| size_of::<Self>().checked_add(slots))
            .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
        if fixed > retained_byte_limit.get() {
            return Err(FeatureRegistryError::RetainedByteLimitTooSmall);
        }
        let slots = std::iter::repeat_with(|| None)
            .take(maximum_entries.get())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            slots,
            len: 0,
            retained_bytes: fixed,
            retained_byte_limit,
        })
    }

    /// Registers metadata idempotently and rejects a conflicting record for the same key.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureRegistryError::MetadataConflict`] for a same-key mismatch,
    /// [`FeatureRegistryError::RegistryFull`] at the count bound, or a retained-size error before
    /// exceeding the byte bound.
    pub fn try_register(
        &mut self,
        metadata: FeatureMetadata,
    ) -> Result<RegistrationOutcome, FeatureRegistryError> {
        if !is_known_local_implementation(&metadata) {
            return Err(FeatureRegistryError::UnknownImplementationDigest);
        }
        match self.position(metadata.key()) {
            Ok(index) => {
                if self.slots[index].as_ref() == Some(&metadata) {
                    Ok(RegistrationOutcome::AlreadyRegistered)
                } else {
                    Err(FeatureRegistryError::MetadataConflict)
                }
            }
            Err(index) => {
                if self.len == self.slots.len() {
                    return Err(FeatureRegistryError::RegistryFull);
                }
                let dynamic = metadata
                    .checked_dynamic_retained_bytes()
                    .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
                let retained_bytes = self
                    .retained_bytes
                    .checked_add(dynamic)
                    .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
                if retained_bytes > self.retained_byte_limit.get() {
                    return Err(FeatureRegistryError::RetainedByteLimitExceeded);
                }
                for position in (index..self.len).rev() {
                    self.slots[position + 1] = self.slots[position].take();
                }
                self.slots[index] = Some(metadata);
                self.len += 1;
                self.retained_bytes = retained_bytes;
                Ok(RegistrationOutcome::Inserted)
            }
        }
    }

    /// Atomically preflights and registers one bounded metadata batch.
    ///
    /// The batch must contain unique keys. Existing identical records are idempotent, while any
    /// duplicate batch key, existing conflict, count overflow, or retained-byte failure leaves the
    /// registry unchanged.
    ///
    /// # Errors
    ///
    /// Returns a typed duplicate, conflict, capacity, or retained-size error before mutation.
    pub fn try_register_batch(
        &mut self,
        metadata: &[FeatureMetadata],
    ) -> Result<BatchRegistrationOutcome, FeatureRegistryError> {
        if metadata
            .iter()
            .any(|candidate| !is_known_local_implementation(candidate))
        {
            return Err(FeatureRegistryError::UnknownImplementationDigest);
        }
        if metadata.len() > self.slots.len() {
            return Err(FeatureRegistryError::RegistryFull);
        }
        for (index, candidate) in metadata.iter().enumerate() {
            if metadata[index + 1..]
                .iter()
                .any(|other| other.key() == candidate.key())
            {
                return Err(FeatureRegistryError::DuplicateBatchKey);
            }
        }

        let mut already_registered = 0_usize;
        let mut dynamic_retained_bytes = 0_usize;
        let mut pending = Vec::new();
        for candidate in metadata {
            match self.position(candidate.key()) {
                Ok(index) if self.slots[index].as_ref() == Some(candidate) => {
                    already_registered += 1;
                }
                Ok(_) => return Err(FeatureRegistryError::MetadataConflict),
                Err(_) => {
                    let owned = candidate.clone();
                    dynamic_retained_bytes = dynamic_retained_bytes
                        .checked_add(
                            owned
                                .checked_dynamic_retained_bytes()
                                .ok_or(FeatureRegistryError::RetainedSizeOverflow)?,
                        )
                        .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
                    pending.push(owned);
                }
            }
        }

        let final_len = self
            .len
            .checked_add(pending.len())
            .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
        if final_len > self.slots.len() {
            return Err(FeatureRegistryError::RegistryFull);
        }
        let final_retained_bytes = self
            .retained_bytes
            .checked_add(dynamic_retained_bytes)
            .ok_or(FeatureRegistryError::RetainedSizeOverflow)?;
        if final_retained_bytes > self.retained_byte_limit.get() {
            return Err(FeatureRegistryError::RetainedByteLimitExceeded);
        }

        let inserted = pending.len();
        for candidate in pending {
            self.insert_prevalidated(candidate);
        }
        self.retained_bytes = final_retained_bytes;
        Ok(BatchRegistrationOutcome {
            inserted,
            already_registered,
        })
    }

    /// Returns metadata for an exact feature key without allocation.
    #[must_use]
    pub fn metadata(&self, key: &FeatureKey) -> Option<&FeatureMetadata> {
        self.position(key)
            .ok()
            .and_then(|index| self.slots.get(index))
            .and_then(Option::as_ref)
    }

    /// Resolves one exact feature version for a required execution plane.
    ///
    /// Resolution never selects a different version implicitly. A known feature name with a
    /// missing requested version is distinguished from an entirely unknown feature, and an exact
    /// version that is unsafe for the requested plane fails closed.
    ///
    /// # Errors
    ///
    /// Returns a typed unknown-feature, unknown-version, or compatibility error.
    pub fn try_resolve(
        &self,
        key: &FeatureKey,
        compatibility: FeatureCompatibility,
    ) -> Result<&FeatureMetadata, FeatureRegistryError> {
        if let Some(metadata) = self.metadata(key) {
            let is_compatible = match compatibility {
                FeatureCompatibility::Live => metadata.is_live_compatible(),
                FeatureCompatibility::PointInTime => metadata.is_point_in_time_compatible(),
            };
            return if is_compatible {
                Ok(metadata)
            } else {
                Err(FeatureRegistryError::IncompatibleRequestedVersion)
            };
        }
        if self
            .entries()
            .any(|metadata| metadata.key().name() == key.name())
        {
            Err(FeatureRegistryError::UnknownRequestedVersion)
        } else {
            Err(FeatureRegistryError::UnknownFeature)
        }
    }

    /// Iterates metadata in deterministic [`FeatureKey`] order without allocation.
    pub fn entries(&self) -> impl Iterator<Item = &FeatureMetadata> {
        self.slots[..self.len].iter().filter_map(Option::as_ref)
    }

    /// Returns the number of registered metadata records.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the registry contains no metadata records.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the exact current retained footprint.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns the configured retained-byte ceiling.
    #[must_use]
    pub const fn retained_byte_limit(&self) -> NonZeroUsize {
        self.retained_byte_limit
    }

    fn position(&self, key: &FeatureKey) -> Result<usize, usize> {
        for (index, slot) in self.slots[..self.len].iter().enumerate() {
            let Some(metadata) = slot.as_ref() else {
                return Err(index);
            };
            match metadata.key().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Ok(index),
                std::cmp::Ordering::Greater => return Err(index),
            }
        }
        Err(self.len)
    }

    fn insert_prevalidated(&mut self, metadata: FeatureMetadata) {
        let mut index = self.len;
        for (position, slot) in self.slots[..self.len].iter().enumerate() {
            if slot
                .as_ref()
                .is_some_and(|existing| existing.key() > metadata.key())
            {
                index = position;
                break;
            }
        }
        for position in (index..self.len).rev() {
            self.slots[position + 1] = self.slots[position].take();
        }
        self.slots[index] = Some(metadata);
        self.len += 1;
    }
}

/// Allocation-free, authority-free feature values exposed to a live action consumer.
pub trait LiveFeatureView: Debug {
    /// Returns one immutable exact feature observation, if present.
    fn feature(&self, key: &FeatureKey) -> Option<&FeatureValue<FeatureScalar>>;

    /// Returns the complete retained footprint of the view's owned graph.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::RetainedSizeOverflow`] if exact accounting is unrepresentable.
    fn retained_bytes(&self) -> Result<usize, FeatureError>;
}

/// Feature registration or retained-accounting failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FeatureRegistryError {
    /// The configured registry entry count exceeded the production bound.
    #[error("feature registry capacity exceeds its production bound")]
    RegistryCapacityTooLarge,
    /// The configured retained-byte limit exceeded the production bound.
    #[error("feature registry retained-byte limit exceeds its production bound")]
    RetainedByteLimitTooLarge,
    /// The retained-byte limit could not contain fixed registry storage.
    #[error("feature registry retained-byte limit is below its fixed storage")]
    RetainedByteLimitTooSmall,
    /// The registry has no remaining entry slots.
    #[error("feature registry is full")]
    RegistryFull,
    /// A new record would exceed the configured retained-byte ceiling.
    #[error("feature registry retained-byte limit would be exceeded")]
    RetainedByteLimitExceeded,
    /// The same feature key was registered with different metadata.
    #[error("conflicting metadata for an existing feature key")]
    MetadataConflict,
    /// Metadata referenced an implementation identity not authorized by this local build.
    #[error("feature implementation digest is not in the code-owned local catalog")]
    UnknownImplementationDigest,
    /// No registered feature has the requested canonical name.
    #[error("requested feature is unknown")]
    UnknownFeature,
    /// The feature name is registered, but the exact requested version is not.
    #[error("requested feature version is unknown")]
    UnknownRequestedVersion,
    /// The exact requested version is incompatible with the required execution plane.
    #[error("requested feature version is incompatible with the required execution plane")]
    IncompatibleRequestedVersion,
    /// One submitted batch repeated a feature key.
    #[error("feature metadata batch contains a duplicate key")]
    DuplicateBatchKey,
    /// Checked retained-size arithmetic overflowed.
    #[error("feature registry retained-byte accounting overflowed")]
    RetainedSizeOverflow,
}
