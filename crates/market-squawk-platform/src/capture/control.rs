//! Single-owner capture-allocation lifecycle control.

use std::sync::{Arc, atomic::Ordering};

use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity, CaptureDegradation,
    CaptureInitializer, CaptureIntegrityState,
};
use thiserror::Error;

use super::{CaptureHealthReason, CaptureState, GenerationCaptureState, WRITER_RUNNING};

/// Whole-bundle generation transition failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CaptureGenerationError {
    /// Source, metadata revision, or session changed within one channel.
    #[error("capture generation rotation changed the registered source/session binding")]
    BindingMismatch {
        /// Active exact audit identity.
        current: Arc<CaptureAuthorityIdentity>,
        /// Received exact audit identity.
        received: Arc<CaptureAuthorityIdentity>,
    },
    /// Recovery requires a strictly newer registry-issued generation.
    #[error("capture generation must strictly advance")]
    NotNewer,
    /// A dead writer cannot be made healthy by changing authority bundles.
    #[error("capture writer is unavailable; create and synchronize a new capture channel")]
    WriterUnavailable,
    /// Concrete registry initialization failed closed.
    #[error("capture generation initialization failed: {0}")]
    Authority(#[from] CaptureAuthorityError),
}

/// Non-clone owner of positive capture-allocation lifecycle transitions.
#[derive(Debug)]
pub struct RawCaptureControl<B: CaptureAuthorityBundle> {
    pub(super) state: Arc<CaptureState<B>>,
    pub(super) initializer: Option<B::Initializer>,
}

impl<B: CaptureAuthorityBundle> RawCaptureControl<B> {
    /// Irreversibly invalidates the current allocation without creating a successor.
    pub fn invalidate_current(&mut self) {
        let active = self.state.active.load_full();
        self.state
            .mark_incomplete_for_generation(&active, CaptureHealthReason::SupervisorStopped);
    }

    /// Activates the initial registry allocation after its supervised writer is running.
    pub fn activate_initial(&mut self) -> Result<(), CaptureGenerationError> {
        let _transition = match self.state.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        let active = self.state.active.load_full();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        let Some(initializer) = self.initializer.as_mut() else {
            return if active.integrity() == CaptureIntegrityState::Healthy {
                Ok(())
            } else {
                Err(CaptureGenerationError::Authority(
                    CaptureAuthorityError::GenerationIncomplete,
                ))
            };
        };
        if let Err(error) = initializer.mark_healthy() {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityRejected);
            return Err(error.into());
        }
        self.initializer = None;
        Ok(())
    }

    /// Replaces the active allocation using a new whole registry-issued authority bundle.
    ///
    /// The complete successor is validated and initialized before the old allocation is
    /// Release-invalidated and RCU-replaced. A rejected successor is degraded without disturbing
    /// a still-current healthy predecessor.
    pub fn rotate_generation(&mut self, bundle: B) -> Result<(), CaptureGenerationError> {
        let identity = bundle.identity();
        let (mut initializer, admission, degradation) = bundle.into_parts();
        let _transition = match self.state.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        let active = self.state.active.load_full();
        if !same_session(active.identity.as_ref(), &identity) {
            degradation.mark_incomplete();
            return Err(CaptureGenerationError::BindingMismatch {
                current: Arc::clone(&active.identity),
                received: Arc::new(identity),
            });
        }
        if identity.connection_generation() <= active.identity.connection_generation() {
            degradation.mark_incomplete();
            return Err(CaptureGenerationError::NotNewer);
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            degradation.mark_incomplete();
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        if let Err(error) = initializer.mark_healthy() {
            degradation.mark_incomplete();
            return Err(error.into());
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            degradation.mark_incomplete();
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        self.state
            .mark_incomplete_for_generation(&active, CaptureHealthReason::SupervisorStopped);
        self.state
            .active
            .store(Arc::new(GenerationCaptureState::new(
                identity,
                admission,
                degradation,
            )));
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            let installed = self.state.active.load_full();
            self.state
                .mark_incomplete_for_generation(&installed, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        self.initializer = None;
        Ok(())
    }

    /// Returns active audit identity.
    pub fn identity(&self) -> Arc<CaptureAuthorityIdentity> {
        Arc::clone(&self.state.active.load_full().identity)
    }
}

fn same_session(current: &CaptureAuthorityIdentity, received: &CaptureAuthorityIdentity) -> bool {
    current.source_id() == received.source_id()
        && current.metadata_revision() == received.metadata_revision()
        && current.session_identifier() == received.session_identifier()
}

impl<B: CaptureAuthorityBundle> Drop for RawCaptureControl<B> {
    fn drop(&mut self) {
        self.invalidate_current();
    }
}
