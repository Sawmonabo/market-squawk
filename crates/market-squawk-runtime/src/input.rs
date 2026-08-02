//! Capability-confined staging for native-streamed user input.

use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::{ArtifactRoot, ResolvedArtifactPath};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::{fs::File, io::AsyncWriteExt as _};
use uuid::Uuid;

use crate::{ClientId, InputAdmission, InputTicket, InputTicketId, RuntimeIdentity};

/// Fixed concurrent-ticket and per-input byte ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputStagingLimits {
    maximum_tickets: NonZeroUsize,
    maximum_bytes: u64,
}

impl InputStagingLimits {
    /// Creates positive bounded staging limits.
    pub fn try_new(maximum_tickets: usize, maximum_bytes: u64) -> Result<Self, InputStagingError> {
        Ok(Self {
            maximum_tickets: NonZeroUsize::new(maximum_tickets)
                .ok_or(InputStagingError::InvalidLimits)?,
            maximum_bytes: if maximum_bytes == 0 {
                return Err(InputStagingError::InvalidLimits);
            } else {
                maximum_bytes
            },
        })
    }
}

/// Short-lived staged-input repository rooted in the controlled artifact capability.
#[derive(Debug)]
pub struct InputStager {
    root: ArtifactRoot,
    runtime: RuntimeIdentity,
    limits: InputStagingLimits,
    state: Arc<Mutex<StagingState>>,
}

#[derive(Debug)]
struct StagedInput {
    ticket: InputTicket,
    path: ResolvedArtifactPath,
}

#[derive(Debug, Default)]
struct StagingState {
    staged: HashMap<InputTicketId, StagedInput>,
    reservations: usize,
}

impl InputStager {
    /// Creates a stager with no ambient filesystem path authority.
    #[must_use]
    pub fn new(root: ArtifactRoot, runtime: RuntimeIdentity, limits: InputStagingLimits) -> Self {
        Self {
            root,
            runtime,
            limits,
            state: Arc::new(Mutex::new(StagingState::default())),
        }
    }

    /// Reserves one unique staging file after enforcing the declared size and digest algorithm.
    pub fn begin(
        &self,
        client_id: ClientId,
        admission: InputAdmission,
        expires_at: Timestamp,
        now: Timestamp,
    ) -> Result<InputStage, InputStagingError> {
        if admission.expected_bytes() > self.limits.maximum_bytes
            || admission.expected_digest().algorithm() != DigestAlgorithm::Sha256
            || expires_at <= now
        {
            return Err(InputStagingError::AdmissionRejected);
        }
        self.reap_expired(now)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| InputStagingError::Unavailable)?;
        let occupied = state
            .staged
            .len()
            .checked_add(state.reservations)
            .ok_or(InputStagingError::CapacityExceeded)?;
        if occupied >= self.limits.maximum_tickets.get() {
            return Err(InputStagingError::CapacityExceeded);
        }
        state.reservations = state
            .reservations
            .checked_add(1)
            .ok_or(InputStagingError::CapacityExceeded)?;
        drop(state);

        let id = InputTicketId::try_from_uuid(Uuid::new_v4())
            .map_err(|_| self.release_reservation(InputStagingError::Unavailable))?;
        let reference = format!("inputs/{}.staged", id.as_uuid().simple());
        let path = self
            .root
            .resolve(reference)
            .map_err(|_| self.release_reservation(InputStagingError::Storage))?;
        let file = path
            .create_new()
            .map_err(|_| self.release_reservation(InputStagingError::Storage))?
            .into_std();
        Ok(InputStage {
            owner: Arc::clone(&self.state),
            root: self.root.clone(),
            id,
            path: Some(path),
            file: Some(File::from_std(file)),
            runtime: self.runtime,
            client_id,
            admission,
            expires_at,
            bytes_written: 0,
            hasher: Sha256::new(),
            reservation_active: true,
        })
    }

    /// Opens one exact, unexpired ticket after checking client and runtime ownership.
    pub fn open(
        &self,
        ticket: &InputTicket,
        client_id: ClientId,
        now: Timestamp,
    ) -> Result<std::fs::File, InputStagingError> {
        if ticket.installation_id() != self.runtime.installation_id()
            || ticket.workspace_id() != self.runtime.workspace_id()
            || ticket.generation() != self.runtime.service_generation()
            || ticket.client_id() != client_id
            || ticket.expires_at() <= now
        {
            return Err(InputStagingError::TicketRejected);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| InputStagingError::Unavailable)?;
        let stored = state
            .staged
            .get(&ticket.id())
            .ok_or(InputStagingError::TicketRejected)?;
        if stored.ticket != *ticket {
            return Err(InputStagingError::TicketRejected);
        }
        stored
            .path
            .open_read()
            .map_err(|_| InputStagingError::Storage)
    }

    fn reap_expired(&self, now: Timestamp) -> Result<(), InputStagingError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| InputStagingError::Unavailable)?;
        let expired: Vec<_> = state
            .staged
            .iter()
            .filter(|(_, input)| input.ticket.expires_at() <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            if let Some(input) = state.staged.remove(&id) {
                remove_staged(&self.root, &input.path);
            }
        }
        Ok(())
    }

    fn release_reservation(&self, error: InputStagingError) -> InputStagingError {
        if let Ok(mut state) = self.state.lock()
            && state.reservations > 0
        {
            state.reservations -= 1;
        }
        error
    }
}

/// One-owner streamed staging transaction.
pub struct InputStage {
    owner: Arc<Mutex<StagingState>>,
    root: ArtifactRoot,
    id: InputTicketId,
    path: Option<ResolvedArtifactPath>,
    file: Option<File>,
    runtime: RuntimeIdentity,
    client_id: ClientId,
    admission: InputAdmission,
    expires_at: Timestamp,
    bytes_written: u64,
    hasher: Sha256,
    reservation_active: bool,
}

impl fmt::Debug for InputStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InputStage([ONE-SHOT CONTROLLED FILE])")
    }
}

impl InputStage {
    /// Writes one bounded chunk while hashing the exact bytes.
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), InputStagingError> {
        let chunk = u64::try_from(bytes.len()).map_err(|_| InputStagingError::LengthMismatch)?;
        let next = self
            .bytes_written
            .checked_add(chunk)
            .ok_or(InputStagingError::LengthMismatch)?;
        if next > self.admission.expected_bytes() {
            return Err(InputStagingError::LengthMismatch);
        }
        self.file
            .as_mut()
            .ok_or(InputStagingError::Unavailable)?
            .write_all(bytes)
            .await
            .map_err(|_| InputStagingError::Storage)?;
        self.hasher.update(bytes);
        self.bytes_written = next;
        Ok(())
    }

    /// Flushes, fsyncs, verifies exact evidence, and exposes only the opaque ticket.
    pub async fn finish(mut self, now: Timestamp) -> Result<InputTicket, InputStagingError> {
        if self.bytes_written != self.admission.expected_bytes() || self.expires_at <= now {
            return Err(InputStagingError::LengthMismatch);
        }
        let digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            self.hasher.clone().finalize().into(),
        );
        if digest != self.admission.expected_digest() {
            return Err(InputStagingError::DigestMismatch);
        }
        let mut file = self.file.take().ok_or(InputStagingError::Unavailable)?;
        file.flush().await.map_err(|_| InputStagingError::Storage)?;
        file.sync_all()
            .await
            .map_err(|_| InputStagingError::Storage)?;
        drop(file);
        let ticket = InputTicket::try_new(
            self.id,
            self.runtime.installation_id(),
            self.runtime.workspace_id(),
            self.runtime.service_generation(),
            self.client_id,
            self.admission.media_type().clone(),
            self.bytes_written,
            digest,
            self.expires_at,
            now,
        )
        .map_err(|_| InputStagingError::AdmissionRejected)?;
        let mut state = self
            .owner
            .lock()
            .map_err(|_| InputStagingError::Unavailable)?;
        if state.reservations == 0 {
            return Err(InputStagingError::Unavailable);
        }
        if state.staged.contains_key(&self.id) {
            return Err(InputStagingError::Storage);
        }
        let path = self.path.take().ok_or(InputStagingError::Unavailable)?;
        state.reservations -= 1;
        state.staged.insert(
            self.id,
            StagedInput {
                ticket: ticket.clone(),
                path,
            },
        );
        self.reservation_active = false;
        Ok(ticket)
    }
}

impl Drop for InputStage {
    fn drop(&mut self) {
        if self.reservation_active
            && let Ok(mut state) = self.owner.lock()
            && state.reservations > 0
        {
            state.reservations -= 1;
        }
        if let Some(path) = self.path.take() {
            remove_staged(&self.root, &path);
        }
    }
}

fn remove_staged(root: &ArtifactRoot, path: &ResolvedArtifactPath) {
    if let Ok(directory) = root.try_clone_directory() {
        let _ignored = directory.remove_file(path.relative());
    }
}

/// Stream staging or ticket-resolution failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InputStagingError {
    /// Ticket/byte bounds must be positive.
    #[error("input staging limits are invalid")]
    InvalidLimits,
    /// Declared input exceeded limits, used an unsupported digest, or already expired.
    #[error("input staging admission was rejected")]
    AdmissionRejected,
    /// Fixed staging capacity is exhausted.
    #[error("input staging capacity is exhausted")]
    CapacityExceeded,
    /// Received byte count differs from the declared exact length.
    #[error("staged input length does not match")]
    LengthMismatch,
    /// Received SHA-256 evidence differs from the declared exact digest.
    #[error("staged input digest does not match")]
    DigestMismatch,
    /// Ticket is missing, expired, or belongs to another runtime/client.
    #[error("input ticket was rejected")]
    TicketRejected,
    /// Capability-confined staging storage failed.
    #[error("input staging storage is unavailable")]
    Storage,
    /// Staging state serialization is unavailable.
    #[error("input staging state is unavailable")]
    Unavailable,
}
