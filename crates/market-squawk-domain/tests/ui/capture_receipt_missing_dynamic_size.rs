use market_squawk_domain::{
    CaptureResidentGenerationLease, CaptureRetainedReceipt,
};

#[derive(Debug)]
struct MissingDynamicSize(CaptureResidentGenerationLease);

impl CaptureRetainedReceipt for MissingDynamicSize {
    fn resident_generation_lease(&self) -> &CaptureResidentGenerationLease {
        &self.0
    }
}

fn main() {}
