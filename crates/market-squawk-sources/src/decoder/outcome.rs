//! Closed capture-bound decoder outcomes for market data and protocol control flow.

use std::mem::size_of;

use market_squawk_domain::SourceIdentifier;
use thiserror::Error;

use super::{DecodeError, DecodedProviderBatch, DecoderEvidence};

/// Result of decoding one exact captured provider frame.
///
/// Provider-input failures are represented as recovery or quarantine outcomes. Only allocation,
/// accounting overflow, and impossible implementation state use [`DecodeInternalError`].
#[derive(Debug)]
pub enum DecodeOutcome {
    /// One or more provider-normalized market observations.
    Data(DecodedProviderBatch),
    /// A validated protocol-control message with no market observation.
    Control(DecodedControlFrame),
    /// A documented no-op or forward-compatible extension.
    Ignored(DecodedIgnoredFrame),
    /// A valid provider message that requires a new snapshot or decoder reset.
    Resynchronize(DecodedRecoveryAction),
    /// A provider-input violation that quarantines the affected generation.
    Quarantine(DecodedQuarantineAction),
}

impl DecodeOutcome {
    /// Returns exact raw-frame and decoder-rule evidence for every disposition.
    pub const fn evidence(&self) -> &DecoderEvidence {
        match self {
            Self::Data(value) => value.evidence(),
            Self::Control(value) => value.evidence(),
            Self::Ignored(value) => value.evidence(),
            Self::Resynchronize(value) => value.evidence(),
            Self::Quarantine(value) => value.evidence(),
        }
    }

    /// Returns the exact retained footprint of the closed outcome graph.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeInternalError::RetainedSizeOverflow`] on checked accounting overflow.
    pub fn retained_bytes(&self) -> Result<usize, DecodeInternalError> {
        let dynamic = match self {
            Self::Data(value) => value.dynamic_retained_bytes().map_err(map_retained_error),
            Self::Control(value) => value.dynamic_retained_bytes(),
            Self::Ignored(value) => value.dynamic_retained_bytes(),
            Self::Resynchronize(value) => value.dynamic_retained_bytes(),
            Self::Quarantine(value) => value.dynamic_retained_bytes(),
        }?;
        size_of::<Self>()
            .checked_add(dynamic)
            .ok_or(DecodeInternalError::RetainedSizeOverflow)
    }
}

/// Protocol-control message kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ControlFrameKind {
    SubscriptionAcknowledgement,
    Heartbeat,
    Ping,
    Pong,
    ProviderFlowControl,
}

/// Reason a documented provider frame was intentionally ignored.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IgnoredFrameReason {
    DocumentedForwardCompatibleExtension,
    DocumentedNoOp,
}

/// Required decoder or source recovery action.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResynchronizationReason {
    SnapshotRequired,
    ProviderRequestedReset,
    DecoderStateDiscontinuity,
}

/// Provider-input violation that makes the current stream unsafe to use.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum QuarantineReason {
    MalformedPayload,
    SchemaViolation,
    WrongProduct,
    WrongChannel,
    InvalidTimestamp,
    InexactNumericValue,
    NegativeQuantity,
    UnsupportedSemanticChange,
    ProtocolInvariantViolation,
}

macro_rules! decoded_disposition {
    ($name:ident, $kind:ty, $kind_accessor:ident) => {
        #[doc = "Capture-bound non-data decoder disposition."]
        #[derive(Debug)]
        pub struct $name {
            evidence: DecoderEvidence,
            kind: $kind,
            provider_code: Option<SourceIdentifier>,
        }

        impl $name {
            /// Constructs a disposition bound to exact frame and decoder evidence.
            pub const fn new(
                evidence: DecoderEvidence,
                kind: $kind,
                provider_code: Option<SourceIdentifier>,
            ) -> Self {
                Self {
                    evidence,
                    kind,
                    provider_code,
                }
            }

            /// Returns exact raw-frame and decoder-rule evidence.
            pub const fn evidence(&self) -> &DecoderEvidence {
                &self.evidence
            }

            /// Returns the closed disposition kind.
            pub const fn $kind_accessor(&self) -> $kind {
                self.kind
            }

            /// Returns a bounded provider-defined control or error code when supplied.
            pub const fn provider_code(&self) -> Option<&SourceIdentifier> {
                self.provider_code.as_ref()
            }

            fn dynamic_retained_bytes(&self) -> Result<usize, DecodeInternalError> {
                self.evidence
                    .dynamic_retained_bytes()
                    .map_err(map_retained_error)?
                    .checked_add(
                        self.provider_code
                            .as_ref()
                            .map_or(0, SourceIdentifier::retained_bytes),
                    )
                    .ok_or(DecodeInternalError::RetainedSizeOverflow)
            }
        }
    };
}

decoded_disposition!(DecodedControlFrame, ControlFrameKind, kind);
decoded_disposition!(DecodedIgnoredFrame, IgnoredFrameReason, reason);
decoded_disposition!(DecodedRecoveryAction, ResynchronizationReason, reason);
decoded_disposition!(DecodedQuarantineAction, QuarantineReason, reason);

/// Decoder implementation failure distinct from a typed provider-input disposition.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum DecodeInternalError {
    /// A bounded allocation could not be reserved.
    #[error("decoder could not reserve bounded storage")]
    Allocation,
    /// Exact retained-size accounting overflowed.
    #[error("decoder retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    /// Decoder control flow reached an impossible state.
    #[error("decoder implementation invariant failed")]
    InvariantViolation,
}

fn map_retained_error(error: DecodeError) -> DecodeInternalError {
    match error {
        DecodeError::RetainedSizeOverflow => DecodeInternalError::RetainedSizeOverflow,
        _ => DecodeInternalError::InvariantViolation,
    }
}
