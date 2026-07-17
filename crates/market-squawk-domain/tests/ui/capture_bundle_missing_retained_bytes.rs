use std::num::NonZeroU64;

use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureFrameFootprint, CaptureInitializer, CaptureIntegrityState,
    CapturePayload, CaptureResidentGenerationLease, CaptureRetainedReceipt,
    CaptureRetainedSizeError, ConnectionGeneration, MetadataRevision, RawCaptureFrameView,
    SourceId, SourceIdentifier, Timestamp,
};

#[derive(Clone)]
struct Frame;

impl RawCaptureFrameView for Frame {
    fn source_id(&self) -> &SourceId {
        unimplemented!()
    }

    fn metadata_revision(&self) -> &MetadataRevision {
        unimplemented!()
    }

    fn session_identifier(&self) -> &SourceIdentifier {
        unimplemented!()
    }

    fn connection_generation(&self) -> ConnectionGeneration {
        unimplemented!()
    }

    fn frame_ordinal(&self) -> NonZeroU64 {
        unimplemented!()
    }

    fn received_at(&self) -> Timestamp {
        unimplemented!()
    }

    fn payload(&self) -> &[u8] {
        unimplemented!()
    }

    fn capture_payload(&self) -> &CapturePayload {
        unimplemented!()
    }

    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct Initializer;

impl CaptureInitializer for Initializer {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError> {
        Ok(())
    }
}

#[derive(Debug)]
struct Admission;

#[derive(Debug)]
struct Receipt(CaptureResidentGenerationLease);

impl CaptureRetainedReceipt for Receipt {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        &self.0
    }

    fn checked_additional_dynamic_retained_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError> {
        Ok(0)
    }
}

impl CaptureAdmission<Frame> for Admission {
    type Receipt = Receipt;

    fn checked_resident_shared_frame_bytes(
        &self,
        _frame: &Frame,
    ) -> Result<usize, CaptureRetainedSizeError> {
        Ok(0)
    }

    fn preflight(&self, _frame: &Frame) -> Result<(), CaptureAuthorityError> {
        Ok(())
    }

    fn issue_after_enqueue(
        &mut self,
        _frame: &Frame,
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        Ok(Receipt(resident))
    }

    fn validate_active(&self, _frame: &Frame) -> Result<(), CaptureAuthorityError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Degradation;

impl CaptureDegradation for Degradation {
    fn mark_incomplete(&self) {}

    fn integrity(&self) -> CaptureIntegrityState {
        CaptureIntegrityState::Incomplete
    }
}

#[derive(Debug)]
struct MissingRetainedBytes;

impl CaptureAuthorityBundle for MissingRetainedBytes {
    type Frame = Frame;
    type Receipt = Receipt;
    type Initializer = Initializer;
    type Admission = Admission;
    type Degradation = Degradation;

    fn identity(&self) -> CaptureAuthorityIdentity {
        unimplemented!()
    }

    fn into_parts(self) -> (Self::Initializer, Self::Admission, Self::Degradation) {
        unimplemented!()
    }
}

fn main() {}
