//! Single-owner capture-allocation lifecycle control.

use std::sync::{Arc, atomic::Ordering};

use market_squawk_domain::{CaptureIntegrityState, ConnectionGeneration};
use thiserror::Error;

use super::{
    CaptureGenerationKey, CaptureHealthReason, CaptureState, GENERATION_INVALIDATED,
    GenerationCaptureState, HEALTHY, WRITER_RUNNING,
};

/// Generation-reset failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CaptureGenerationError {
    /// Source, metadata revision, and session must not be transplanted into an existing channel.
    #[error("capture generation reset changed the registered source/session binding")]
    BindingMismatch {
        /// Active exact binding.
        current: Arc<CaptureGenerationKey>,
        /// Requested exact binding.
        received: Arc<CaptureGenerationKey>,
    },
    /// Recovery requires a strictly newer capture allocation.
    #[error("capture generation must advance beyond {current}; received {received}")]
    NotNewer {
        /// Active generation.
        current: ConnectionGeneration,
        /// Requested generation.
        received: ConnectionGeneration,
    },
    /// A new connection generation must use a distinct raw-wire connection identity.
    #[error("capture generation rotation must change the raw connection identity")]
    ConnectionNotRotated,
    /// A dead writer cannot be made healthy by changing generation state.
    #[error("capture writer is unavailable; create and synchronize a new capture channel")]
    WriterUnavailable,
    /// A capture fault invalidated this generation; recovery requires rotation first.
    #[error("capture generation was invalidated and must advance before synchronization")]
    GenerationMustAdvance,
}

/// Non-clone owner of positive capture-allocation lifecycle transitions.
#[derive(Debug)]
pub struct RawCaptureControl {
    pub(super) state: Arc<CaptureState>,
}

impl RawCaptureControl {
    /// Irreversibly invalidates the current allocation without creating a successor.
    pub fn invalidate_current(&mut self) {
        let active = self.state.active.load_full();
        active.accepting.store(false, Ordering::Release);
        self.state
            .mark_incomplete_for_generation(&active, CaptureHealthReason::SupervisorStopped);
    }

    /// Activates the exact initial allocation after its supervised writer is running.
    pub fn activate_initial(
        &mut self,
        key: &CaptureGenerationKey,
    ) -> Result<(), CaptureGenerationError> {
        let active = self.state.active.load_full();
        if active.key.as_ref() != key {
            return Err(CaptureGenerationError::BindingMismatch {
                current: Arc::clone(&active.key),
                received: Arc::new(key.clone()),
            });
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        if active.integrity.load(Ordering::Acquire) == GENERATION_INVALIDATED {
            return Err(CaptureGenerationError::GenerationMustAdvance);
        }
        active.integrity.store(HEALTHY, Ordering::Release);
        Ok(())
    }

    /// Replaces the active allocation with a strictly newer healthy capture generation.
    ///
    /// RCU replacement never waits for a publisher. Receipts from the prior allocation retain its
    /// exact key and cannot become evidence for this allocation.
    pub fn rotate_generation(
        &mut self,
        key: CaptureGenerationKey,
    ) -> Result<(), CaptureGenerationError> {
        let active = self.state.active.load_full();
        if !active.key.same_binding_except_generation(&key) {
            return Err(CaptureGenerationError::BindingMismatch {
                current: Arc::clone(&active.key),
                received: Arc::new(key),
            });
        }
        if key.generation() <= active.key.generation() {
            return Err(CaptureGenerationError::NotNewer {
                current: active.key.generation(),
                received: key.generation(),
            });
        }
        if key.connection_id() == active.key.connection_id() {
            return Err(CaptureGenerationError::ConnectionNotRotated);
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        active.accepting.store(false, Ordering::Release);
        active
            .integrity
            .store(GENERATION_INVALIDATED, Ordering::Release);
        self.state
            .active
            .store(Arc::new(GenerationCaptureState::new(
                key,
                CaptureIntegrityState::Healthy,
            )));
        Ok(())
    }

    /// Returns the exact active allocation.
    pub fn key(&self) -> Arc<CaptureGenerationKey> {
        Arc::clone(&self.state.active.load_full().key)
    }
}

impl Drop for RawCaptureControl {
    fn drop(&mut self) {
        self.invalidate_current();
    }
}
