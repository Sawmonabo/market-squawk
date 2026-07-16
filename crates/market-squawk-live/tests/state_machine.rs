use std::error::Error;

use market_squawk_domain::ConnectionGeneration;
use market_squawk_live::{GenerationPhase, GenerationStateError, GenerationStateMachine};

fn generation(value: u64) -> Result<ConnectionGeneration, Box<dyn Error>> {
    Ok(ConnectionGeneration::new(value)?)
}

#[test]
fn healthy_requires_new_generation_snapshot_and_validation() -> Result<(), Box<dyn Error>> {
    let mut state = GenerationStateMachine::new();
    assert_eq!(state.phase(), GenerationPhase::Disconnected);

    state.begin_generation(generation(1)?)?;
    assert_eq!(state.phase(), GenerationPhase::AwaitingSnapshot);
    state.begin_snapshot()?;
    assert_eq!(state.phase(), GenerationPhase::Synchronizing);
    state.commit_snapshot()?;
    assert_eq!(state.phase(), GenerationPhase::Healthy);
    Ok(())
}

#[test]
fn quarantine_is_one_way_for_the_allocation() -> Result<(), Box<dyn Error>> {
    let mut state = GenerationStateMachine::new();
    state.begin_generation(generation(4)?)?;
    state.begin_snapshot()?;
    state.commit_snapshot()?;
    state.quarantine();

    assert_eq!(state.phase(), GenerationPhase::Quarantined);
    assert_eq!(
        state.begin_snapshot(),
        Err(GenerationStateError::TransitionDenied {
            from: GenerationPhase::Quarantined,
            operation: "begin_snapshot",
        })
    );
    assert_eq!(
        state.begin_generation(generation(4)?),
        Err(GenerationStateError::GenerationNotAdvanced)
    );

    state.begin_generation(generation(5)?)?;
    assert_eq!(state.phase(), GenerationPhase::AwaitingSnapshot);
    Ok(())
}

#[test]
fn transition_table_rejects_out_of_order_operations() -> Result<(), Box<dyn Error>> {
    let mut state = GenerationStateMachine::new();
    assert!(matches!(
        state.commit_snapshot(),
        Err(GenerationStateError::TransitionDenied { .. })
    ));
    state.begin_generation(generation(1)?)?;
    assert!(matches!(
        state.commit_snapshot(),
        Err(GenerationStateError::TransitionDenied { .. })
    ));
    state.begin_snapshot()?;
    assert!(matches!(
        state.begin_snapshot(),
        Err(GenerationStateError::TransitionDenied { .. })
    ));
    Ok(())
}

#[test]
fn non_book_snapshot_non_applicability_is_a_one_way_initialization() -> Result<(), Box<dyn Error>> {
    let mut state = GenerationStateMachine::new();
    state.begin_generation(generation(1)?)?;
    state.establish_snapshot_not_applicable()?;
    assert_eq!(state.phase(), GenerationPhase::Healthy);
    assert!(state.establish_snapshot_not_applicable().is_err());
    Ok(())
}
