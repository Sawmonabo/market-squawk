use std::num::NonZeroU64;

use market_squawk_domain::{
    CapturePayload, ConnectionGeneration, MetadataRevision, RawCaptureFrameView, SourceId,
    SourceIdentifier, Timestamp,
};

#[derive(Clone)]
struct MissingRetainedFootprint;

impl RawCaptureFrameView for MissingRetainedFootprint {
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
}

fn main() {}
