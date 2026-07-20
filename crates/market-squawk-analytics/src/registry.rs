//! Fixed-capacity feature metadata registration and read-only live feature access.

use std::fmt::Debug;
use std::mem::size_of;
use std::num::NonZeroUsize;

use thiserror::Error;

use crate::{FeatureError, FeatureKey, FeatureMetadata, FeatureScalar, FeatureValue};

/// Maximum entries accepted by one in-process metadata registry.
pub const MAX_FEATURE_REGISTRY_ENTRIES: usize = 4_096;
/// Maximum configured retained-byte limit for one registry.
pub const MAX_FEATURE_REGISTRY_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// Result of an accepted deterministic registration attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistrationOutcome {
    /// A new key was inserted.
    Inserted,
    /// Identical metadata for this key was already present.
    AlreadyRegistered,
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

    /// Returns metadata for an exact feature key without allocation.
    #[must_use]
    pub fn metadata(&self, key: &FeatureKey) -> Option<&FeatureMetadata> {
        self.position(key)
            .ok()
            .and_then(|index| self.slots.get(index))
            .and_then(Option::as_ref)
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
    /// Checked retained-size arithmetic overflowed.
    #[error("feature registry retained-byte accounting overflowed")]
    RetainedSizeOverflow,
}
