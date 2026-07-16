//! Closed current-observation application errors.

use thiserror::Error;

use crate::authority::AuthorityError;
use crate::provider_book::ProviderBookError;
use crate::qualification::QualificationBuildError;
use crate::snapshot::SnapshotBuildError;
use crate::{GenerationStateError, NormalizationError, SequenceValidationError};

#[derive(Debug, Error)]
pub(crate) enum LiveApplyError {
    #[error("invalid stream capacity {requested}; maximum is {maximum}")]
    InvalidStreamCapacity { requested: usize, maximum: usize },
    #[error("generation authority capacity must be positive")]
    InvalidGenerationCapacity,
    #[error("maximum capability lifetime must be positive")]
    InvalidCapabilityLifetime,
    #[error("processor snapshot limits are zero or exceed local hard bounds")]
    InvalidSnapshotLimits,
    #[error("processor snapshot retained-byte accounting overflowed")]
    SnapshotRetainedSizeOverflow,
    #[error(transparent)]
    SnapshotBuild(#[from] SnapshotBuildError),
    #[error("bounded live allocation failed")]
    Allocation,
    #[error("batch instrument does not match the instrument owner")]
    InstrumentMismatch,
    #[error("batch venue is absent from the current instrument definition")]
    VenueMismatch,
    #[error("current observation evidence binding is inconsistent")]
    BindingMismatch,
    #[error("full stream-key capacity is exhausted")]
    StreamCapacityExhausted,
    #[error("registered source-generation capacity is exhausted")]
    GenerationCapacityExhausted,
    #[error("shared source/venue/instrument status capacity is exhausted")]
    StatusCapacityExhausted,
    #[error("shared status belongs to a different connection generation")]
    StatusGenerationMismatch,
    #[error("shared status allocation revision exhausted")]
    StatusRevisionExhausted,
    #[error("shared status changed between staging and commit")]
    StatusCommitConflict,
    #[error("pre-admission generation authority was transplanted")]
    GenerationAdmissionTransplant,
    #[error("stream state was not found after admission")]
    StreamStateMissing,
    #[error("stream state revision exhausted before candidate construction")]
    StateRevisionExhausted,
    #[error("stream state revision changed before candidate commit")]
    StateRevisionConflict,
    #[error("stream candidate event was already consumed")]
    CandidateEventAlreadyBuilt,
    #[error("connection generation did not strictly advance")]
    GenerationNotAdvanced,
    #[error("stream generation is quarantined")]
    Quarantined,
    #[error("current generation requires a validated snapshot")]
    SnapshotRequired,
    #[error("snapshot applicability conflicts with event class")]
    SnapshotPolicyMismatch,
    #[error("payload class conflicts with the apply path")]
    PayloadClassMismatch,
    #[error("provider checksum profile and evidence differ")]
    ChecksumProfileMismatch,
    #[error("provider checksum text is invalid")]
    InvalidChecksumValue,
    #[error("payload checksum canonicalization is not a supported closed implementation")]
    UnsupportedPayloadChecksum,
    #[error("capability deadline is already expired or cannot be represented")]
    CapabilityExpired,
    #[error(transparent)]
    Source(#[from] market_squawk_sources::RegistryError),
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    GenerationState(#[from] GenerationStateError),
    #[error(transparent)]
    Sequence(#[from] SequenceValidationError),
    #[error(transparent)]
    Checksum(#[from] crate::ChecksumValidationError),
    #[error(transparent)]
    ProviderBook(#[from] ProviderBookError),
    #[error(transparent)]
    Normalization(#[from] NormalizationError),
    #[error(transparent)]
    Qualification(#[from] QualificationBuildError),
    #[error(transparent)]
    Integrity(#[from] market_squawk_domain::IntegrityEvidenceError),
    #[error(transparent)]
    Market(#[from] market_squawk_domain::MarketEventError),
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
}

impl LiveApplyError {
    /// Classifies irrecoverable owner-state/configuration failures separately from exact-source
    /// rejection. The actor invalidates shard/runtime authority for every `true` variant.
    pub(crate) const fn is_fatal_to_actor(&self) -> bool {
        match self {
            Self::InvalidStreamCapacity { .. }
            | Self::InvalidGenerationCapacity
            | Self::InvalidCapabilityLifetime
            | Self::InvalidSnapshotLimits
            | Self::SnapshotRetainedSizeOverflow
            | Self::SnapshotBuild(_)
            | Self::Allocation
            | Self::GenerationCapacityExhausted
            | Self::StatusCapacityExhausted
            | Self::StatusRevisionExhausted
            | Self::StatusCommitConflict
            | Self::StreamStateMissing
            | Self::StateRevisionExhausted
            | Self::StateRevisionConflict
            | Self::CandidateEventAlreadyBuilt => true,
            Self::InstrumentMismatch
            | Self::StreamCapacityExhausted
            | Self::VenueMismatch
            | Self::BindingMismatch
            | Self::StatusGenerationMismatch
            | Self::GenerationAdmissionTransplant
            | Self::GenerationNotAdvanced
            | Self::Quarantined
            | Self::SnapshotRequired
            | Self::SnapshotPolicyMismatch
            | Self::PayloadClassMismatch
            | Self::ChecksumProfileMismatch
            | Self::InvalidChecksumValue
            | Self::UnsupportedPayloadChecksum
            | Self::CapabilityExpired
            | Self::Source(_)
            | Self::GenerationState(_)
            | Self::Sequence(_)
            | Self::Checksum(_)
            | Self::ProviderBook(_)
            | Self::Normalization(_)
            | Self::Qualification(_)
            | Self::Integrity(_)
            | Self::Market(_)
            | Self::Identity(_) => false,
            Self::Authority(error) => matches!(
                error,
                AuthorityError::RevisionExhausted
                    | AuthorityError::NonceRegistryInitialization
                    | AuthorityError::NonceInvariant
                    | AuthorityError::ClockRange
            ),
        }
    }
}
