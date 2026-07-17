use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};

use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MAX_CAPTURE_DESTINATION_LABEL_BYTES: usize = 1_024;
const MAX_ACTIVE_CAPTURE_DESTINATIONS: usize = 1_024;
const CAPTURE_DESTINATION_DOMAIN: &[u8] = b"MSQKCAPTUREDESTINATION\x01";

/// Redacted exact identity for one capture storage destination.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CaptureDestination([u8; 32]);

impl CaptureDestination {
    /// Constructs a destination from one bounded non-secret alternative-sink label.
    ///
    /// Every handle for the same underlying physical endpoint in this process must use the same
    /// stable, collision-resistant label. Per-instance or random aliases are valid only for truly
    /// independent storage. This identity provides no cross-process exclusion; custom sinks shared
    /// by multiple processes must also enforce an operating-system or storage-level ownership
    /// primitive.
    ///
    /// # Errors
    ///
    /// Rejects an empty label or one larger than 1,024 bytes.
    pub fn try_named(label: &str) -> Result<Self, CaptureDestinationError> {
        if label.is_empty() {
            return Err(CaptureDestinationError::Empty);
        }
        if label.len() > MAX_CAPTURE_DESTINATION_LABEL_BYTES {
            return Err(CaptureDestinationError::TooLong {
                max: MAX_CAPTURE_DESTINATION_LABEL_BYTES,
            });
        }
        Ok(Self::from_bytes(b"named", label.as_bytes()))
    }

    pub(crate) fn for_journal(path: &std::path::Path) -> Self {
        Self::from_bytes(b"journal", path.as_os_str().as_encoded_bytes())
    }

    pub(super) fn unique_memory() -> Self {
        Self::from_bytes(b"memory", Uuid::new_v4().as_bytes())
    }

    fn from_bytes(kind: &[u8], value: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(CAPTURE_DESTINATION_DOMAIN);
        hasher.update(
            u64::try_from(kind.len())
                .map_or(u64::MAX, |length| length)
                .to_be_bytes(),
        );
        hasher.update(kind);
        hasher.update(
            u64::try_from(value.len())
                .map_or(u64::MAX, |length| length)
                .to_be_bytes(),
        );
        hasher.update(value);
        Self(hasher.finalize().into())
    }
}

impl fmt::Debug for CaptureDestination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("CaptureDestination")
            .field(&self.0)
            .finish()
    }
}

/// Capture destination construction failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureDestinationError {
    /// An empty destination cannot establish a stable fence.
    #[error("capture destination label cannot be empty")]
    Empty,
    /// The destination label exceeded its retained input bound.
    #[error("capture destination label exceeds maximum {max} bytes")]
    TooLong {
        /// Maximum accepted label bytes.
        max: usize,
    },
}

#[derive(Debug)]
pub(super) struct CaptureDestinationLease {
    destination: CaptureDestination,
}

impl Drop for CaptureDestinationLease {
    fn drop(&mut self) {
        let Some(registry) = CAPTURE_DESTINATION_FENCES.get() else {
            return;
        };
        let mut registry = match registry.lock() {
            Ok(registry) => registry,
            Err(poisoned) => poisoned.into_inner(),
        };
        registry.remove_if_matches(&self.destination, self as *const Self);
    }
}

#[derive(Debug, Default)]
struct CaptureDestinationFenceRegistry {
    leases: HashMap<CaptureDestination, Weak<CaptureDestinationLease>>,
}

impl CaptureDestinationFenceRegistry {
    fn try_acquire(
        &mut self,
        destination: &CaptureDestination,
    ) -> Result<
        (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
        CaptureDestinationFenceError,
    > {
        if self
            .leases
            .get(destination)
            .is_some_and(|lease| lease.strong_count() > 0)
        {
            return Err(CaptureDestinationFenceError::Busy);
        }
        self.leases.remove(destination);
        if self.leases.len() >= MAX_ACTIVE_CAPTURE_DESTINATIONS {
            return Err(CaptureDestinationFenceError::Capacity);
        }
        let lease = Arc::new(CaptureDestinationLease {
            destination: destination.clone(),
        });
        self.leases
            .insert(destination.clone(), Arc::downgrade(&lease));
        Ok((Arc::clone(&lease), lease))
    }

    fn remove_if_matches(
        &mut self,
        destination: &CaptureDestination,
        lease: *const CaptureDestinationLease,
    ) {
        if self
            .leases
            .get(destination)
            .is_some_and(|retained| std::ptr::eq(retained.as_ptr(), lease))
        {
            self.leases.remove(destination);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CaptureDestinationFenceError {
    Busy,
    Capacity,
}

static CAPTURE_DESTINATION_FENCES: OnceLock<std::sync::Mutex<CaptureDestinationFenceRegistry>> =
    OnceLock::new();

pub(super) fn acquire_destination_fence(
    destination: &CaptureDestination,
) -> Result<
    (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
    CaptureDestinationFenceError,
> {
    let registry = CAPTURE_DESTINATION_FENCES
        .get_or_init(|| std::sync::Mutex::new(CaptureDestinationFenceRegistry::default()));
    let mut registry = match registry.lock() {
        Ok(registry) => registry,
        Err(poisoned) => poisoned.into_inner(),
    };
    registry.try_acquire(destination)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    use super::{
        CaptureDestination, CaptureDestinationFenceError, CaptureDestinationFenceRegistry,
        MAX_ACTIVE_CAPTURE_DESTINATIONS, acquire_destination_fence,
    };

    #[test]
    fn destination_registry_rejects_capacity_without_unbounded_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut registry = CaptureDestinationFenceRegistry::default();
        let mut retained = Vec::with_capacity(MAX_ACTIVE_CAPTURE_DESTINATIONS);
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS {
            let destination = CaptureDestination::try_named(&format!("registry-capacity-{index}"))?;
            let leases = registry
                .try_acquire(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            retained.push(leases);
        }
        let overflow = CaptureDestination::try_named("registry-capacity-overflow")?;
        assert!(matches!(
            registry.try_acquire(&overflow),
            Err(CaptureDestinationFenceError::Capacity)
        ));
        assert_eq!(registry.leases.len(), MAX_ACTIVE_CAPTURE_DESTINATIONS);
        drop(retained);
        Ok(())
    }

    #[test]
    fn destination_registry_churn_removes_each_exact_dead_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS.saturating_mul(2) {
            let destination = CaptureDestination::try_named(&format!("registry-churn-{index}"))?;
            let (worker, owner) = acquire_destination_fence(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            drop(worker);
            drop(owner);
            let registry = super::CAPTURE_DESTINATION_FENCES
                .get()
                .ok_or("destination registry was not initialized")?;
            let registry = match registry.lock() {
                Ok(registry) => registry,
                Err(poisoned) => poisoned.into_inner(),
            };
            assert!(!registry.leases.contains_key(&destination));
        }
        Ok(())
    }

    #[test]
    fn final_lease_drop_can_race_same_destination_acquisition_without_deadlock()
    -> Result<(), Box<dyn std::error::Error>> {
        for index in 0..64 {
            let destination =
                CaptureDestination::try_named(&format!("registry-drop-race-{index}"))?;
            let (worker, owner) = acquire_destination_fence(&destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            drop(worker);
            let race_start = Arc::new(Barrier::new(2));
            let drop_race_start = Arc::clone(&race_start);
            let (drop_complete_sender, drop_complete_receiver) = std::sync::mpsc::sync_channel(1);
            let drop_thread = std::thread::spawn(move || {
                drop_race_start.wait();
                drop(owner);
                let _sent = drop_complete_sender.send(());
            });

            race_start.wait();
            let acquisition_deadline = Instant::now() + Duration::from_secs(1);
            let replacement = loop {
                match acquire_destination_fence(&destination) {
                    Ok(leases) => break leases,
                    Err(CaptureDestinationFenceError::Busy)
                        if Instant::now() < acquisition_deadline =>
                    {
                        std::thread::yield_now();
                    }
                    Err(error) => {
                        return Err(format!(
                            "same-destination race did not acquire before deadline: {error:?}"
                        )
                        .into());
                    }
                }
            };
            drop_complete_receiver.recv_timeout(Duration::from_secs(1))?;
            drop_thread
                .join()
                .map_err(|_panic| "destination lease drop thread panicked")?;
            drop(replacement);
        }
        Ok(())
    }
}
