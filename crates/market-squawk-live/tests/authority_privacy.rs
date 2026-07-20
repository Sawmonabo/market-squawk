use static_assertions::{assert_impl_all, assert_not_impl_any};

assert_impl_all!(market_squawk_live::LiveExecutionCapability: Send);
assert_not_impl_any!(market_squawk_live::LiveExecutionCapability: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_live::ConsumedLiveAuthority: Send);
assert_not_impl_any!(market_squawk_live::ConsumedLiveAuthority: Clone, Sync, serde::Serialize, serde::de::DeserializeOwned);
assert_impl_all!(market_squawk_live::ShardSnapshot: serde::Serialize);
assert_not_impl_any!(market_squawk_live::ShardSnapshot: Clone);
assert_impl_all!(market_squawk_live::RouteSnapshot: serde::Serialize);
assert_not_impl_any!(market_squawk_live::RouteSnapshot: Clone);
assert_impl_all!(market_squawk_live::StreamSnapshot: serde::Serialize);
assert_not_impl_any!(market_squawk_live::StreamSnapshot: Clone);
assert_impl_all!(market_squawk_live::StatusSnapshot: serde::Serialize);
assert_not_impl_any!(market_squawk_live::StatusSnapshot: Clone);
assert_impl_all!(market_squawk_live::BookLevelSnapshot: serde::Serialize);
assert_impl_all!(market_squawk_live::SnapshotDimension: serde::Serialize);

#[test]
fn live_execution_capability_has_no_dependent_crate_minting_surface() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/current_capability_is_opaque.rs");
    cases.compile_fail("tests/ui/current_capability_is_not_clone.rs");
    cases.compile_fail("tests/ui/current_capability_is_not_serde.rs");
    cases.compile_fail("tests/ui/current_action_gate_is_private.rs");
    cases.compile_fail("tests/ui/domain_assessment_is_not_capability.rs");
    cases.compile_fail("tests/ui/snapshot_dtos_are_output_only.rs");
    cases.compile_fail("tests/ui/snapshot_dtos_are_not_clone.rs");
}
