use market_squawk_execution::{ApprovedOrder, DispatchOrder};
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(ApprovedOrder: Clone, serde::Serialize, serde::de::DeserializeOwned);
assert_not_impl_any!(DispatchOrder: Clone, serde::Serialize, serde::de::DeserializeOwned);

#[test]
fn approval_and_dispatch_construction_remain_crate_owned() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/approved_order_is_private.rs");
    cases.compile_fail("tests/ui/dispatch_order_is_private.rs");
}
