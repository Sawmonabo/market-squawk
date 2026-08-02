//! Fatal permit-release branch coverage.

use super::*;

#[derive(Clone, Copy, Debug)]
enum PermitFatalCase {
    ClockUnavailable,
    SessionUnavailable,
    StatePoisoned,
    InFlightUnderflow,
    PersistenceFailure,
}

#[test]
fn permit_release_fatal_branch_matrix_invalidates_global_durability() -> TestResult {
    for case in [
        PermitFatalCase::ClockUnavailable,
        PermitFatalCase::SessionUnavailable,
        PermitFatalCase::StatePoisoned,
        PermitFatalCase::InFlightUnderflow,
        PermitFatalCase::PersistenceFailure,
    ] {
        let fixture = global_fault_fixture()?;
        let permit = match fixture.budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            other => return Err(format!("permit setup failed: {other:?}").into()),
        };
        match case {
            PermitFatalCase::ClockUnavailable => fixture.clock.fail(),
            PermitFatalCase::SessionUnavailable => {
                fixture.clock.fail();
                let _decision = fixture.peer.try_acquire();
                fixture.clock.recover();
            }
            PermitFatalCase::StatePoisoned => poison_budget_state(&fixture.budget),
            PermitFatalCase::InFlightUnderflow => {
                fixture
                    .budget
                    .allocation
                    .state
                    .lock()
                    .map_err(|_| "budget state lock poisoned")?
                    .in_flight = 0;
            }
            PermitFatalCase::PersistenceFailure => {
                fixture.store.reject_stores.store(true, Ordering::Release);
            }
        }
        permit.release();
        assert_global_terminal(
            fixture,
            matches!(
                case,
                PermitFatalCase::ClockUnavailable
                    | PermitFatalCase::StatePoisoned
                    | PermitFatalCase::InFlightUnderflow
            ),
        )?;
    }
    Ok(())
}
