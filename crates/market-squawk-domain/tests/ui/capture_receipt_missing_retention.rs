use market_squawk_domain::{CaptureRetainedReceipt, CaptureRetainedSizeError};

#[derive(Debug)]
struct MissingRetention;

impl CaptureRetainedReceipt for MissingRetention {
    fn checked_additional_dynamic_retained_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError> {
        Ok(0)
    }
}

fn main() {}
