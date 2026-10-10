//! Bounded, generation-local idempotency for application mutations.

use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use market_squawk_domain::EvidenceDigest;
use market_squawk_services::RequestId;
use thiserror::Error;

use crate::{AppResponseEnvelope, ClientId};

/// Maximum distinct mutation identities retained by one running service generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayLimits(NonZeroUsize);

impl ReplayLimits {
    /// Creates a positive bounded replay capacity.
    pub fn try_new(value: usize) -> Result<Self, ReplayError> {
        NonZeroUsize::new(value)
            .map(Self)
            .ok_or(ReplayError::InvalidLimits)
    }

    const fn get(self) -> usize {
        self.0.get()
    }
}

/// Mutation replay identity scoped to one authenticated client.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReplayKey {
    client_id: ClientId,
    request_id: RequestId,
}

impl ReplayKey {
    /// Binds a request identity to its authenticated client.
    #[must_use]
    pub const fn new(client_id: ClientId, request_id: RequestId) -> Self {
        Self {
            client_id,
            request_id,
        }
    }
}

/// Admission outcome for a mutation request.
pub enum ReplayAdmission {
    /// The caller owns the only execution permit and must publish its terminal disposition.
    Execute(ReplayPermit),
    /// The exact mutation already reached this terminal response.
    Completed(AppResponseEnvelope),
}

impl fmt::Debug for ReplayAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Execute(_) => formatter.write_str("Execute([REPLAY PERMIT])"),
            Self::Completed(response) => {
                formatter.debug_tuple("Completed").field(response).finish()
            }
        }
    }
}

impl PartialEq for ReplayAdmission {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Completed(left), Self::Completed(right)) => left == right,
            _ => false,
        }
    }
}

/// Bounded replay state for one running service generation.
#[derive(Debug)]
pub struct MutationReplayGuard {
    limits: ReplayLimits,
    state: Arc<Mutex<HashMap<ReplayKey, ReplayEntry>>>,
}

#[derive(Clone, Debug)]
enum ReplayEntry {
    InFlight(EvidenceDigest),
    Completed {
        digest: EvidenceDigest,
        response: AppResponseEnvelope,
    },
}

impl MutationReplayGuard {
    /// Creates an empty fail-closed replay guard.
    pub fn try_new(limits: ReplayLimits) -> Result<Self, ReplayError> {
        let mut entries = HashMap::new();
        entries
            .try_reserve(limits.get())
            .map_err(|_error| ReplayError::CapacityUnavailable)?;
        Ok(Self {
            limits,
            state: Arc::new(Mutex::new(entries)),
        })
    }

    /// Admits one new digest or returns the exact previously completed disposition.
    pub fn begin(
        &self,
        key: ReplayKey,
        digest: EvidenceDigest,
    ) -> Result<ReplayAdmission, ReplayError> {
        let mut state = self.state.lock().map_err(|_| ReplayError::Unavailable)?;
        match state.get(&key) {
            Some(ReplayEntry::InFlight(existing)) if *existing == digest => {
                return Err(ReplayError::InFlight);
            }
            Some(ReplayEntry::Completed {
                digest: existing,
                response,
            }) if *existing == digest => {
                return Ok(ReplayAdmission::Completed(response.clone()));
            }
            Some(_) => return Err(ReplayError::DigestConflict),
            None => {}
        }
        if state.len() >= self.limits.get() {
            return Err(ReplayError::CapacityExceeded);
        }
        state.insert(key.clone(), ReplayEntry::InFlight(digest));
        drop(state);
        Ok(ReplayAdmission::Execute(ReplayPermit {
            state: Arc::clone(&self.state),
            key,
            digest,
            completed: false,
        }))
    }
}

/// One-owner capability to publish a mutation's terminal disposition.
pub struct ReplayPermit {
    state: Arc<Mutex<HashMap<ReplayKey, ReplayEntry>>>,
    key: ReplayKey,
    digest: EvidenceDigest,
    completed: bool,
}

impl fmt::Debug for ReplayPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ReplayPermit([ONE-SHOT])")
    }
}

impl ReplayPermit {
    /// Atomically replaces the matching in-flight record with its immutable response.
    pub fn complete(mut self, response: AppResponseEnvelope) -> Result<(), ReplayError> {
        let mut state = self.state.lock().map_err(|_| ReplayError::Unavailable)?;
        match state.get(&self.key) {
            Some(ReplayEntry::InFlight(digest)) if *digest == self.digest => {
                state.insert(
                    self.key.clone(),
                    ReplayEntry::Completed {
                        digest: self.digest,
                        response,
                    },
                );
                self.completed = true;
                Ok(())
            }
            _ => Err(ReplayError::Unavailable),
        }
    }
}

impl Drop for ReplayPermit {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Ok(mut state) = self.state.lock()
            && matches!(
                state.get(&self.key),
                Some(ReplayEntry::InFlight(digest)) if *digest == self.digest
            )
        {
            state.remove(&self.key);
        }
    }
}

/// Mutation replay admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ReplayError {
    /// Replay capacity must be positive.
    #[error("replay limits are invalid")]
    InvalidLimits,
    /// Reserved storage could not be allocated.
    #[error("replay storage is unavailable")]
    CapacityUnavailable,
    /// The same mutation is still being processed.
    #[error("mutation is already in flight")]
    InFlight,
    /// A request identity was reused with different mutation bytes.
    #[error("mutation request identity was reused with a different digest")]
    DigestConflict,
    /// The bounded guard is full and cannot safely forget prior mutations.
    #[error("mutation replay capacity is exhausted")]
    CapacityExceeded,
    /// Replay serialization is unavailable.
    #[error("mutation replay guard is unavailable")]
    Unavailable,
}
