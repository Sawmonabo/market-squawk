//! One-way connection-generation synchronization state.

use market_squawk_domain::ConnectionGeneration;
use thiserror::Error;

/// Explicit synchronization phase for one mutable provider stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GenerationPhase {
    /// No generation is currently connected.
    Disconnected,
    /// A new generation exists but has not supplied a snapshot.
    AwaitingSnapshot,
    /// A complete snapshot candidate is being converted and validated.
    Synchronizing,
    /// The generation owns a validated snapshot and may apply deltas.
    Healthy,
    /// Integrity failed and this allocation can never become healthy again.
    Quarantined,
}

/// Checked one-way synchronization state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationStateMachine {
    phase: GenerationPhase,
    generation: Option<ConnectionGeneration>,
    high_water: Option<ConnectionGeneration>,
}

impl GenerationStateMachine {
    /// Creates a disconnected state with no generation history.
    pub const fn new() -> Self {
        Self {
            phase: GenerationPhase::Disconnected,
            generation: None,
            high_water: None,
        }
    }

    /// Starts a strictly newer connection generation in `AwaitingSnapshot`.
    ///
    /// # Errors
    ///
    /// Rejects a reused or non-advancing generation.
    pub fn begin_generation(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), GenerationStateError> {
        if self
            .high_water
            .is_some_and(|high_water| generation <= high_water)
        {
            return Err(GenerationStateError::GenerationNotAdvanced);
        }
        self.generation = Some(generation);
        self.high_water = Some(generation);
        self.phase = GenerationPhase::AwaitingSnapshot;
        Ok(())
    }

    /// Begins validating a full snapshot for the current allocation.
    ///
    /// # Errors
    ///
    /// Only `AwaitingSnapshot` may transition to `Synchronizing`.
    pub fn begin_snapshot(&mut self) -> Result<(), GenerationStateError> {
        self.transition(
            GenerationPhase::AwaitingSnapshot,
            GenerationPhase::Synchronizing,
            "begin_snapshot",
        )
    }

    /// Commits a fully validated snapshot and marks the generation healthy.
    ///
    /// # Errors
    ///
    /// Only `Synchronizing` may transition to `Healthy`.
    pub fn commit_snapshot(&mut self) -> Result<(), GenerationStateError> {
        self.transition(
            GenerationPhase::Synchronizing,
            GenerationPhase::Healthy,
            "commit_snapshot",
        )
    }

    /// Marks a non-book stream healthy when metadata explicitly makes snapshots inapplicable.
    ///
    /// # Errors
    ///
    /// Only a fresh `AwaitingSnapshot` generation can establish this state.
    pub fn establish_snapshot_not_applicable(&mut self) -> Result<(), GenerationStateError> {
        self.transition(
            GenerationPhase::AwaitingSnapshot,
            GenerationPhase::Healthy,
            "establish_snapshot_not_applicable",
        )
    }

    /// Irreversibly quarantines the current allocation.
    pub fn quarantine(&mut self) {
        self.phase = GenerationPhase::Quarantined;
    }

    /// Marks transport disconnection without permitting generation reuse.
    pub fn disconnect(&mut self) {
        self.phase = GenerationPhase::Disconnected;
        self.generation = None;
    }

    /// Returns the current synchronization phase.
    pub const fn phase(&self) -> GenerationPhase {
        self.phase
    }

    /// Returns the active generation, when connected or quarantined.
    pub const fn generation(&self) -> Option<ConnectionGeneration> {
        self.generation
    }

    fn transition(
        &mut self,
        expected: GenerationPhase,
        next: GenerationPhase,
        operation: &'static str,
    ) -> Result<(), GenerationStateError> {
        if self.phase != expected {
            return Err(GenerationStateError::TransitionDenied {
                from: self.phase,
                operation,
            });
        }
        self.phase = next;
        Ok(())
    }
}

impl Default for GenerationStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

/// Invalid generation-state transition.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GenerationStateError {
    /// A session generation was reused or did not strictly advance.
    #[error("connection generation did not strictly advance")]
    GenerationNotAdvanced,
    /// Operation is not valid from the current phase.
    #[error("operation {operation} is not allowed from {from:?}")]
    TransitionDenied {
        /// Current state.
        from: GenerationPhase,
        /// Attempted operation.
        operation: &'static str,
    },
}
