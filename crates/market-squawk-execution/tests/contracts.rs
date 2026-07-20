use market_squawk_execution::{AccountRiskReservation, OrderIntent, PreAuthorityRiskOutcome};
use static_assertions::assert_not_impl_any;

#[test]
fn intent_and_reservation_authority_cannot_be_duplicated() {
    assert_not_impl_any!(OrderIntent: Clone);
    assert_not_impl_any!(AccountRiskReservation: Clone);
    assert_not_impl_any!(PreAuthorityRiskOutcome: Clone);
}
