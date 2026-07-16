use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(market_squawk_live::LiveExecutionCapability: Send);
assert_not_impl_any!(market_squawk_live::LiveExecutionCapability: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_live::ConsumedLiveAuthority: Send);
assert_not_impl_any!(market_squawk_live::ConsumedLiveAuthority: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);

#[test]
fn live_execution_capability_has_no_dependent_crate_minting_surface() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/current_capability_is_opaque.rs");
    cases.compile_fail("tests/ui/current_capability_is_not_clone.rs");
    cases.compile_fail("tests/ui/current_capability_is_not_serde.rs");
    cases.compile_fail("tests/ui/domain_assessment_is_not_capability.rs");
}
