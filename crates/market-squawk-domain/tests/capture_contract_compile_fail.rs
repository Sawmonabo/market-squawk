#[test]
fn capture_bundle_requires_retained_bytes() {
    trybuild::TestCases::new().compile_fail("tests/ui/capture_bundle_missing_retained_bytes.rs");
}

#[test]
fn raw_frame_requires_retained_footprint() {
    trybuild::TestCases::new().compile_fail("tests/ui/raw_frame_missing_retained_footprint.rs");
}

#[test]
fn raw_frame_requires_owned_capture_payload() {
    trybuild::TestCases::new().compile_fail("tests/ui/raw_frame_missing_capture_payload.rs");
}

#[test]
fn admission_requires_resident_frame_bytes() {
    trybuild::TestCases::new().compile_fail("tests/ui/admission_missing_resident_frame_bytes.rs");
}

#[test]
fn receipt_requires_resident_generation_retention() {
    trybuild::TestCases::new().compile_fail("tests/ui/capture_receipt_missing_retention.rs");
}

#[test]
fn receipt_requires_additional_dynamic_retained_bytes() {
    trybuild::TestCases::new().compile_fail("tests/ui/capture_receipt_missing_dynamic_size.rs");
}
