use market_squawk_domain::{
    CaptureAdmission, CaptureAuthorityError, CaptureResidentGenerationLease,
    CaptureRetainedReceipt, CaptureRetainedSizeError,
};

#[derive(Debug)]
struct MissingResidentFrameBytes;

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

impl CaptureAdmission<()> for MissingResidentFrameBytes {
    type Receipt = Receipt;

    fn preflight(&self, _frame: &()) -> Result<(), CaptureAuthorityError> {
        Ok(())
    }

    fn issue_after_enqueue(
        &mut self,
        _frame: &(),
        resident: CaptureResidentGenerationLease,
    ) -> Result<Self::Receipt, CaptureAuthorityError> {
        Ok(Receipt(resident))
    }

    fn validate_active(&self, _frame: &()) -> Result<(), CaptureAuthorityError> {
        Ok(())
    }
}

fn main() {}
