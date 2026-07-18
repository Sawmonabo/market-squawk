use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock, Weak};

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
    registry: &'static Mutex<CaptureDestinationFenceRegistry>,
}

pub(super) fn destination_lease_allocation_bytes()
-> Result<usize, market_squawk_domain::RetainedLayoutError> {
    market_squawk_domain::checked_arc_value_allocation_bytes::<CaptureDestinationLease>(0)
}

impl Drop for CaptureDestinationLease {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.remove_if_matches(&self.destination, self as *const Self);
        }
    }
}

#[derive(Debug)]
struct CaptureDestinationFenceEntry {
    destination: CaptureDestination,
    lease: Weak<CaptureDestinationLease>,
}

#[derive(Debug)]
struct CaptureDestinationFenceRegistry {
    entries: Vec<Option<CaptureDestinationFenceEntry>>,
    observed_entry_capacity: usize,
    retained_process_bytes: usize,
}

impl CaptureDestinationFenceRegistry {
    fn try_new(
        ceiling: NonZeroUsize,
    ) -> Result<Self, DestinationFenceRegistryPermanentInitializationError> {
        Self::try_new_with_capacity(MAX_ACTIVE_CAPTURE_DESTINATIONS, ceiling)
    }

    fn try_new_with_capacity(
        logical_capacity: usize,
        ceiling: NonZeroUsize,
    ) -> Result<Self, DestinationFenceRegistryPermanentInitializationError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(logical_capacity)
            .map_err(|_error| {
                DestinationFenceRegistryPermanentInitializationError::AllocationFailed
            })?;
        entries.resize_with(logical_capacity, || None);
        let observed_entry_capacity = entries.capacity();
        let vector_bytes = observed_entry_capacity
            .checked_mul(std::mem::size_of::<Option<CaptureDestinationFenceEntry>>())
            .ok_or(DestinationFenceRegistryPermanentInitializationError::ArithmeticOverflow)?;
        let retained_process_bytes =
            std::mem::size_of::<OnceLock<DestinationFenceRegistryInitializationState>>()
                .checked_add(vector_bytes)
                .ok_or(DestinationFenceRegistryPermanentInitializationError::ArithmeticOverflow)?;
        if retained_process_bytes > ceiling.get() {
            return Err(
                DestinationFenceRegistryPermanentInitializationError::FixedStorageBudgetExceeded {
                    required: retained_process_bytes,
                    ceiling: ceiling.get(),
                },
            );
        }
        Ok(Self {
            entries,
            observed_entry_capacity,
            retained_process_bytes,
        })
    }

    fn try_acquire(
        &mut self,
        registry: &'static Mutex<CaptureDestinationFenceRegistry>,
        destination: &CaptureDestination,
    ) -> Result<
        (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
        CaptureDestinationFenceError,
    > {
        for slot in &mut self.entries {
            if let Some(entry) = slot
                && entry.destination == *destination
            {
                if entry.lease.strong_count() > 0 {
                    return Err(CaptureDestinationFenceError::Busy);
                }
                *slot = None;
                break;
            }
        }
        let vacant = self
            .entries
            .iter()
            .position(Option::is_none)
            .ok_or(CaptureDestinationFenceError::Capacity)?;
        let lease = Arc::new(CaptureDestinationLease {
            destination: destination.clone(),
            registry,
        });
        self.entries[vacant] = Some(CaptureDestinationFenceEntry {
            destination: destination.clone(),
            lease: Arc::downgrade(&lease),
        });
        Ok((Arc::clone(&lease), lease))
    }

    fn remove_if_matches(
        &mut self,
        destination: &CaptureDestination,
        lease: *const CaptureDestinationLease,
    ) {
        if let Some(slot) = self.entries.iter_mut().find(|slot| {
            slot.as_ref().is_some_and(|entry| {
                entry.destination == *destination && std::ptr::eq(entry.lease.as_ptr(), lease)
            })
        }) {
            *slot = None;
        }
    }

    #[cfg(test)]
    fn active_entries(&self) -> usize {
        self.entries.iter().filter(|entry| entry.is_some()).count()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureDestinationFenceError {
    /// Another live or unreaped writer owns this exact destination.
    Busy,
    /// The fixed process registry has no vacant logical entry.
    Capacity,
    /// The process registry mutex was poisoned and cannot resume lease service.
    RegistryPoisoned,
}

impl fmt::Display for CaptureDestinationFenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => formatter.write_str("capture destination is already fenced"),
            Self::Capacity => formatter.write_str("capture destination registry is full"),
            Self::RegistryPoisoned => {
                formatter.write_str("capture destination registry is poisoned")
            }
        }
    }
}

impl std::error::Error for CaptureDestinationFenceError {}

/// Explicit process-lifetime limit for the fixed destination-fence registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CaptureProcessInfrastructureLimits {
    destination_registry_memory_ceiling_bytes: NonZeroUsize,
}

impl CaptureProcessInfrastructureLimits {
    /// Creates an explicit process registry limit without applying a hidden default.
    pub const fn new(destination_registry_memory_ceiling_bytes: NonZeroUsize) -> Self {
        Self {
            destination_registry_memory_ceiling_bytes,
        }
    }

    /// Returns the complete process-lifetime registry ceiling in bytes.
    pub const fn destination_registry_memory_ceiling_bytes(self) -> NonZeroUsize {
        self.destination_registry_memory_ceiling_bytes
    }
}

#[derive(Debug)]
struct ReadyCaptureProcessInfrastructure {
    admitted_limits: CaptureProcessInfrastructureLimits,
    registry: Mutex<CaptureDestinationFenceRegistry>,
}

#[derive(Debug)]
enum DestinationFenceRegistryInitializationState {
    Ready(ReadyCaptureProcessInfrastructure),
    Failed {
        attempted_limits: CaptureProcessInfrastructureLimits,
        error: DestinationFenceRegistryPermanentInitializationError,
    },
}

/// Allocation-free proof that process capture infrastructure initialized successfully.
#[derive(Clone, Copy)]
pub struct CaptureProcessInfrastructure {
    ready: &'static ReadyCaptureProcessInfrastructure,
}

impl fmt::Debug for CaptureProcessInfrastructure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CaptureProcessInfrastructure")
            .field("admitted_limits", &self.ready.admitted_limits)
            .finish_non_exhaustive()
    }
}

impl CaptureProcessInfrastructure {
    /// Returns the exact immutable limits admitted by the process-global winner.
    pub const fn limits(self) -> CaptureProcessInfrastructureLimits {
        self.ready.admitted_limits
    }

    /// Returns the allocator-observed fixed registry backing capacity.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureDestinationFenceError::RegistryPoisoned`] after mutex poison.
    pub fn destination_registry_observed_capacity(
        self,
    ) -> Result<usize, CaptureDestinationFenceError> {
        self.ready
            .registry
            .lock()
            .map(|registry| registry.observed_entry_capacity)
            .map_err(|_poisoned| CaptureDestinationFenceError::RegistryPoisoned)
    }

    /// Returns exact Rust-visible process bytes retained by the initialized registry.
    ///
    /// # Errors
    ///
    /// Returns [`CaptureDestinationFenceError::RegistryPoisoned`] after mutex poison.
    pub fn destination_registry_retained_bytes(
        self,
    ) -> Result<usize, CaptureDestinationFenceError> {
        self.ready
            .registry
            .lock()
            .map(|registry| registry.retained_process_bytes)
            .map_err(|_poisoned| CaptureDestinationFenceError::RegistryPoisoned)
    }
}

/// Permanent first-attempt failure to allocate or admit the process registry.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DestinationFenceRegistryPermanentInitializationError {
    /// The fixed retained-size formula overflowed.
    #[error("capture destination registry retained-size accounting overflowed")]
    ArithmeticOverflow,
    /// The dominant fixed vector allocation was refused recoverably.
    #[error("capture destination registry allocation failed")]
    AllocationFailed,
    /// The fixed process registry exceeded its configured ceiling.
    #[error(
        "capture destination registry requires {required} bytes but ceiling is {ceiling} bytes"
    )]
    FixedStorageBudgetExceeded {
        /// Complete bytes required by the registry.
        required: usize,
        /// Configured process ceiling.
        ceiling: usize,
    },
}

/// Failure to initialize or reproduce process capture infrastructure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum DestinationFenceRegistryInitializationError {
    /// The first same-limit attempt failed permanently and its exact result is replayed.
    #[error("capture destination registry initialization failed permanently: {0}")]
    Permanent(DestinationFenceRegistryPermanentInitializationError),
    /// Process infrastructure already has a different immutable limit.
    #[error("capture process infrastructure was already initialized with different limits")]
    AlreadyInitializedWithDifferentLimits,
}

static CAPTURE_DESTINATION_FENCES: OnceLock<DestinationFenceRegistryInitializationState> =
    OnceLock::new();

/// Initializes process-lifetime capture infrastructure exactly once.
///
/// Same-limit calls replay the first success or permanent failure. Different limits are rejected;
/// no failed call publishes a proof and no retry can replace the first terminal state.
///
/// # Errors
///
/// Returns the exact permanent allocation/accounting refusal from the first attempt, or a typed
/// mismatch when a later caller supplies different immutable limits.
pub fn initialize_capture_process_infrastructure(
    limits: CaptureProcessInfrastructureLimits,
) -> Result<CaptureProcessInfrastructure, DestinationFenceRegistryInitializationError> {
    let state = CAPTURE_DESTINATION_FENCES.get_or_init(|| {
        match CaptureDestinationFenceRegistry::try_new(
            limits.destination_registry_memory_ceiling_bytes(),
        ) {
            Ok(registry) => DestinationFenceRegistryInitializationState::Ready(
                ReadyCaptureProcessInfrastructure {
                    admitted_limits: limits,
                    registry: Mutex::new(registry),
                },
            ),
            Err(error) => DestinationFenceRegistryInitializationState::Failed {
                attempted_limits: limits,
                error,
            },
        }
    });
    resolve_initialization_state(state, limits).map(|ready| CaptureProcessInfrastructure { ready })
}

fn resolve_initialization_state(
    state: &DestinationFenceRegistryInitializationState,
    limits: CaptureProcessInfrastructureLimits,
) -> Result<&ReadyCaptureProcessInfrastructure, DestinationFenceRegistryInitializationError> {
    match state {
        DestinationFenceRegistryInitializationState::Ready(ready)
            if ready.admitted_limits == limits =>
        {
            Ok(ready)
        }
        DestinationFenceRegistryInitializationState::Failed {
            attempted_limits,
            error,
        } if *attempted_limits == limits => Err(
            DestinationFenceRegistryInitializationError::Permanent(*error),
        ),
        DestinationFenceRegistryInitializationState::Ready(_)
        | DestinationFenceRegistryInitializationState::Failed { .. } => {
            Err(DestinationFenceRegistryInitializationError::AlreadyInitializedWithDifferentLimits)
        }
    }
}

pub(super) fn acquire_destination_fence(
    process: CaptureProcessInfrastructure,
    destination: &CaptureDestination,
) -> Result<
    (Arc<CaptureDestinationLease>, Arc<CaptureDestinationLease>),
    CaptureDestinationFenceError,
> {
    let registry = &process.ready.registry;
    let mut registry = registry
        .lock()
        .map_err(|_poisoned| CaptureDestinationFenceError::RegistryPoisoned)?;
    registry.try_acquire(&process.ready.registry, destination)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::{Duration, Instant};

    use super::{
        CaptureDestination, CaptureDestinationFenceError, CaptureDestinationFenceRegistry,
        CaptureProcessInfrastructure, CaptureProcessInfrastructureLimits,
        DestinationFenceRegistryInitializationError, DestinationFenceRegistryInitializationState,
        DestinationFenceRegistryPermanentInitializationError, MAX_ACTIVE_CAPTURE_DESTINATIONS,
        ReadyCaptureProcessInfrastructure, acquire_destination_fence,
        initialize_capture_process_infrastructure, resolve_initialization_state,
    };

    fn process() -> Result<CaptureProcessInfrastructure, Box<dyn std::error::Error>> {
        Ok(initialize_capture_process_infrastructure(
            CaptureProcessInfrastructureLimits::new(
                std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
            ),
        )?)
    }

    #[test]
    fn registry_fixed_storage_accepts_exact_ceiling_and_rejects_one_under()
    -> Result<(), Box<dyn std::error::Error>> {
        let generous = CaptureDestinationFenceRegistry::try_new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
        )?;
        let required = generous.retained_process_bytes;
        let exact = CaptureDestinationFenceRegistry::try_new(
            std::num::NonZeroUsize::new(required).ok_or("registry bytes must be nonzero")?,
        )?;
        assert_eq!(exact.retained_process_bytes, required);
        let one_under = std::num::NonZeroUsize::new(required.saturating_sub(1))
            .ok_or("registry exact bytes must exceed one")?;
        assert!(matches!(
            CaptureDestinationFenceRegistry::try_new(one_under),
            Err(
                DestinationFenceRegistryPermanentInitializationError::FixedStorageBudgetExceeded {
                    required: observed_required,
                    ceiling: observed_ceiling,
                }
            ) if observed_required == required && observed_ceiling == one_under.get()
        ));
        Ok(())
    }

    #[test]
    fn impossible_registry_capacity_is_a_recoverable_allocation_failure() {
        assert!(matches!(
            CaptureDestinationFenceRegistry::try_new_with_capacity(
                usize::MAX,
                std::num::NonZeroUsize::MAX,
            ),
            Err(DestinationFenceRegistryPermanentInitializationError::AllocationFailed)
        ));
    }

    #[test]
    fn terminal_initialization_state_replays_same_limits_and_rejects_different_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = CaptureProcessInfrastructureLimits::new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
        );
        let different = CaptureProcessInfrastructureLimits::new(
            std::num::NonZeroUsize::new(1024 * 1024 + 1).unwrap_or(std::num::NonZeroUsize::MIN),
        );
        let ready =
            DestinationFenceRegistryInitializationState::Ready(ReadyCaptureProcessInfrastructure {
                admitted_limits: limits,
                registry: std::sync::Mutex::new(CaptureDestinationFenceRegistry::try_new(
                    limits.destination_registry_memory_ceiling_bytes(),
                )?),
            });
        assert!(resolve_initialization_state(&ready, limits).is_ok());
        assert_eq!(
            resolve_initialization_state(&ready, different).err(),
            Some(
                DestinationFenceRegistryInitializationError::AlreadyInitializedWithDifferentLimits
            )
        );
        let permanent = DestinationFenceRegistryPermanentInitializationError::AllocationFailed;
        let failed = DestinationFenceRegistryInitializationState::Failed {
            attempted_limits: limits,
            error: permanent,
        };
        assert_eq!(
            resolve_initialization_state(&failed, limits).err(),
            Some(DestinationFenceRegistryInitializationError::Permanent(
                permanent
            ))
        );
        assert_eq!(
            resolve_initialization_state(&failed, different).err(),
            Some(
                DestinationFenceRegistryInitializationError::AlreadyInitializedWithDifferentLimits
            )
        );
        Ok(())
    }

    #[test]
    fn lease_drop_cleans_the_exact_registry_that_admitted_it()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry: &'static std::sync::Mutex<CaptureDestinationFenceRegistry> =
            Box::leak(Box::new(std::sync::Mutex::new(
                CaptureDestinationFenceRegistry::try_new_with_capacity(
                    1,
                    std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
                )?,
            )));
        let destination = CaptureDestination::try_named("exact-admitting-registry")?;
        let leases = {
            let mut locked = registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?;
            locked.try_acquire(registry, &destination)?
        };
        assert_eq!(
            registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?
                .active_entries(),
            1
        );
        drop(leases);
        assert_eq!(
            registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?
                .active_entries(),
            0
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::panic,
        reason = "this test must deliberately poison the private registry mutex"
    )]
    fn poisoned_registry_refuses_new_leases_and_does_not_recover_on_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = CaptureProcessInfrastructureLimits::new(
            std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
        );
        let ready: &'static ReadyCaptureProcessInfrastructure =
            Box::leak(Box::new(ReadyCaptureProcessInfrastructure {
                admitted_limits: limits,
                registry: std::sync::Mutex::new(
                    CaptureDestinationFenceRegistry::try_new_with_capacity(
                        2,
                        limits.destination_registry_memory_ceiling_bytes(),
                    )?,
                ),
            }));
        let process = CaptureProcessInfrastructure { ready };
        let destination = CaptureDestination::try_named("poisoned-registry-existing")?;
        let existing = acquire_destination_fence(process, &destination)?;
        let registry = &ready.registry;
        let poisoner = std::thread::spawn(move || {
            let Ok(_guard) = registry.lock() else {
                return;
            };
            std::panic::panic_any("intentional destination registry poison");
        });
        assert!(poisoner.join().is_err());

        let next = CaptureDestination::try_named("poisoned-registry-next")?;
        assert_eq!(
            acquire_destination_fence(process, &next).err(),
            Some(CaptureDestinationFenceError::RegistryPoisoned)
        );
        drop(existing);
        assert!(ready.registry.lock().is_err());
        Ok(())
    }

    #[test]
    fn destination_registry_rejects_capacity_without_unbounded_growth()
    -> Result<(), Box<dyn std::error::Error>> {
        let _process = process()?;
        let registry: &'static std::sync::Mutex<CaptureDestinationFenceRegistry> =
            Box::leak(Box::new(std::sync::Mutex::new(
                CaptureDestinationFenceRegistry::try_new(
                    std::num::NonZeroUsize::new(1024 * 1024).unwrap_or(std::num::NonZeroUsize::MIN),
                )?,
            )));
        let mut retained = Vec::with_capacity(MAX_ACTIVE_CAPTURE_DESTINATIONS);
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS {
            let destination = CaptureDestination::try_named(&format!("registry-capacity-{index}"))?;
            let leases = registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?
                .try_acquire(registry, &destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            retained.push(leases);
        }
        let overflow = CaptureDestination::try_named("registry-capacity-overflow")?;
        assert!(matches!(
            registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?
                .try_acquire(registry, &overflow),
            Err(CaptureDestinationFenceError::Capacity)
        ));
        assert_eq!(
            registry
                .lock()
                .map_err(|_poisoned| "test destination registry unexpectedly poisoned")?
                .active_entries(),
            MAX_ACTIVE_CAPTURE_DESTINATIONS
        );
        drop(retained);
        Ok(())
    }

    #[test]
    fn destination_registry_churn_removes_each_exact_dead_lease()
    -> Result<(), Box<dyn std::error::Error>> {
        let process = process()?;
        for index in 0..MAX_ACTIVE_CAPTURE_DESTINATIONS.saturating_mul(2) {
            let destination = CaptureDestination::try_named(&format!("registry-churn-{index}"))?;
            let (worker, owner) = acquire_destination_fence(process, &destination)
                .map_err(|error| format!("unexpected registry acquisition failure: {error:?}"))?;
            drop(worker);
            drop(owner);
            let state = super::CAPTURE_DESTINATION_FENCES
                .get()
                .ok_or("destination registry was not initialized")?;
            let DestinationFenceRegistryInitializationState::Ready(ready) = state else {
                return Err("destination registry initialization failed".into());
            };
            let registry = match ready.registry.lock() {
                Ok(registry) => registry,
                Err(poisoned) => poisoned.into_inner(),
            };
            assert!(!registry.entries.iter().any(|entry| {
                entry
                    .as_ref()
                    .is_some_and(|entry| entry.destination == destination)
            }));
        }
        Ok(())
    }

    #[test]
    fn final_lease_drop_can_race_same_destination_acquisition_without_deadlock()
    -> Result<(), Box<dyn std::error::Error>> {
        let process = process()?;
        for index in 0..64 {
            let destination =
                CaptureDestination::try_named(&format!("registry-drop-race-{index}"))?;
            let (worker, owner) = acquire_destination_fence(process, &destination)
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
                match acquire_destination_fence(process, &destination) {
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
