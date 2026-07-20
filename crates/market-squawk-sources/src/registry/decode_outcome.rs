// Exact-session and capture-receipt binding for every decoder disposition.

use crate::{
    CaptureAdmissionReceipt, ControlFrameKind, DecodeOutcome, DecodedControlFrame,
    DecodedIgnoredFrame, DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction,
    DecoderEvidence, IgnoredFrameReason, QuarantineReason, ResynchronizationReason,
};

/// Session- and capture-validated decoder disposition.
///
/// Only [`Self::Data`] can be passed to current health and coverage qualification. No control,
/// ignored, recovery, or quarantine disposition contains market observations.
#[derive(Debug)]
pub enum ValidatedSessionDecodeOutcome {
    Data(CapturedDecodedProviderBatch),
    Control(SessionControlDisposition),
    Ignored(SessionIgnoredDisposition),
    Resynchronize(SessionRecoveryDisposition),
    Quarantine(SessionQuarantineDisposition),
}

/// Data batch proven to originate from one exact captured session frame.
#[derive(Debug)]
pub struct CapturedDecodedProviderBatch {
    batch: DecodedProviderBatch,
    receipt: CaptureAdmissionReceipt,
}

impl CapturedDecodedProviderBatch {
    /// Returns exact raw-frame and decoder-rule evidence.
    pub const fn evidence(&self) -> &DecoderEvidence {
        self.batch.evidence()
    }

    pub(crate) fn into_parts(self) -> (DecodedProviderBatch, CaptureAdmissionReceipt) {
        (self.batch, self.receipt)
    }
}

macro_rules! session_disposition {
    ($name:ident, $decoded:ty, $kind:ty, $accessor:ident) => {
        #[doc = "Session- and capture-validated non-data decoder disposition."]
        #[derive(Debug)]
        pub struct $name {
            decoded: $decoded,
            _receipt: CaptureAdmissionReceipt,
        }

        impl $name {
            /// Returns the closed disposition kind.
            pub const fn $accessor(&self) -> $kind {
                self.decoded.$accessor()
            }

            /// Returns exact raw-frame and decoder-rule evidence.
            pub const fn evidence(&self) -> &DecoderEvidence {
                self.decoded.evidence()
            }
        }
    };
}

session_disposition!(
    SessionControlDisposition,
    DecodedControlFrame,
    ControlFrameKind,
    kind
);
session_disposition!(
    SessionIgnoredDisposition,
    DecodedIgnoredFrame,
    IgnoredFrameReason,
    reason
);
session_disposition!(
    SessionRecoveryDisposition,
    DecodedRecoveryAction,
    ResynchronizationReason,
    reason
);
session_disposition!(
    SessionQuarantineDisposition,
    DecodedQuarantineAction,
    QuarantineReason,
    reason
);

impl ValidatedSourceSession<'_> {
    /// Binds every decoder disposition to this exact current session and capture receipt.
    ///
    /// This step requires current session and capture integrity but not current market coverage or
    /// health. Only a returned [`CapturedDecodedProviderBatch`] can enter the separate data upgrade.
    ///
    /// # Errors
    ///
    /// Rejects session, generation, frame, receipt, digest, trusted-time, capture, metadata, or
    /// decoder-rule transplant.
    pub fn validate_decode_outcome_owned(
        &self,
        outcome: DecodeOutcome,
        receipt: CaptureAdmissionReceipt,
    ) -> Result<ValidatedSessionDecodeOutcome, RegistryError> {
        self.validate_decode_evidence(outcome.evidence(), &receipt)?;
        Ok(match outcome {
            DecodeOutcome::Data(batch) => {
                ValidatedSessionDecodeOutcome::Data(CapturedDecodedProviderBatch {
                    batch,
                    receipt,
                })
            }
            DecodeOutcome::Control(decoded) => {
                ValidatedSessionDecodeOutcome::Control(SessionControlDisposition {
                    decoded,
                    _receipt: receipt,
                })
            }
            DecodeOutcome::Ignored(decoded) => {
                ValidatedSessionDecodeOutcome::Ignored(SessionIgnoredDisposition {
                    decoded,
                    _receipt: receipt,
                })
            }
            DecodeOutcome::Resynchronize(decoded) => {
                ValidatedSessionDecodeOutcome::Resynchronize(SessionRecoveryDisposition {
                    decoded,
                    _receipt: receipt,
                })
            }
            DecodeOutcome::Quarantine(decoded) => {
                ValidatedSessionDecodeOutcome::Quarantine(SessionQuarantineDisposition {
                    decoded,
                    _receipt: receipt,
                })
            }
        })
    }

    fn validate_decode_evidence(
        &self,
        evidence: &DecoderEvidence,
        receipt: &CaptureAdmissionReceipt,
    ) -> Result<(), RegistryError> {
        self.session.validate_current_lease()?;
        if !self
            .session
            .binding
            .shares_allocation_with(evidence.binding())
        {
            return Err(RegistryError::HandleTransplanted);
        }
        if !receipt.binding().shares_allocation_with(evidence.binding())
            || receipt.received_at() != evidence.received_at()
            || receipt.trusted_receipt() != evidence.trusted_receipt()
            || receipt.frame_id() != evidence.frame_id()
            || receipt.payload_digest() != evidence.payload_digest()
            || !receipt.lease().is_healthy()
            || !receipt
                .lease()
                .shares_allocation_with(&self.session.capture)
        {
            return Err(RegistryError::CaptureReceiptMismatch);
        }
        self.session
            .lease
            .validate_receipt(receipt.trusted_receipt())?;
        let crate::SourceProtocolProfile::Live(protocol) = self.metadata.protocol_profile() else {
            return Err(RegistryError::DecoderProfileMismatch);
        };
        if evidence.decoder_rule() != protocol.decoder_rule() {
            return Err(RegistryError::DecoderProfileMismatch);
        }
        Ok(())
    }
}
