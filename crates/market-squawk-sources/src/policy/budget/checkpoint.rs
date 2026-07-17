//! Durable checkpoint conversion and restore validation.

use super::*;

pub(in crate::policy) fn checkpoint_from_runtime(
    policy: &ProviderBudgetPolicy,
    state: &BudgetState,
    observation: ClockObservation,
    availability_generation: u64,
    terminal: bool,
) -> Result<BudgetCheckpointState, AuthorityPersistenceError> {
    let window_ends_at = match state.restored_window_ends_at {
        Some(ends_at) => ends_at,
        None => state
            .window_started_at
            .checked_add(policy.window_nanos())
            .ok_or(AuthorityPersistenceError::InvalidState)?,
    };
    let window_ends_wall = monotonic_deadline_to_wall(observation, window_ends_at)?;
    let window = i64::try_from(policy.window_nanos())
        .map_err(|_| AuthorityPersistenceError::InvalidState)?;
    let window_started_wall = window_ends_wall
        .checked_sub_nanos(window)
        .map_err(|_| AuthorityPersistenceError::InvalidState)?;
    let unavailable_until_wall = state
        .unavailable_until
        .map(|deadline| monotonic_deadline_to_wall(observation, deadline))
        .transpose()?;
    Ok(BudgetCheckpointState {
        window_started_wall,
        window_ends_wall,
        requests_used: state.requests_used,
        in_flight: state.in_flight,
        unavailable_until_wall,
        disabled: state.disabled,
        consecutive_refusals: state.consecutive_refusals,
        availability_generation,
        terminal,
        poisoned: false,
    })
}

pub(in crate::policy) fn monotonic_deadline_to_wall(
    observation: ClockObservation,
    deadline: MonotonicInstant,
) -> Result<Timestamp, AuthorityPersistenceError> {
    let delta = i128::from(deadline.as_nanos()) - i128::from(observation.monotonic.as_nanos());
    let delta = i64::try_from(delta).map_err(|_| AuthorityPersistenceError::InvalidState)?;
    observation
        .wall_clock
        .checked_add_nanos(delta)
        .map_err(|_| AuthorityPersistenceError::InvalidState)
}

pub(in crate::policy) fn validate_checkpoint(
    policy: &ProviderBudgetPolicy,
    checkpoint: &BudgetCheckpointState,
    observation: ClockObservation,
) -> Result<(), AuthorityPersistenceError> {
    let window = checkpoint
        .window_ends_wall
        .unix_nanos()
        .checked_sub(checkpoint.window_started_wall.unix_nanos())
        .and_then(|value| u64::try_from(value).ok());
    if window != Some(policy.window_nanos())
        || checkpoint.requests_used > policy.requests_per_window()
        || checkpoint.in_flight > policy.max_concurrent()
        || checkpoint.availability_generation == 0
        || checkpoint.poisoned && !checkpoint.terminal
    {
        return Err(AuthorityPersistenceError::InvalidState);
    }
    let latest_window_end = observation
        .wall_clock
        .checked_add_nanos(
            i64::try_from(policy.window_nanos())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?,
        )
        .map_err(|_| AuthorityPersistenceError::InvalidState)?;
    if checkpoint.window_started_wall > observation.wall_clock
        || checkpoint.window_ends_wall > latest_window_end
    {
        return Err(AuthorityPersistenceError::FutureState);
    }
    if let Some(until) = checkpoint.unavailable_until_wall {
        let latest_cooldown = observation
            .wall_clock
            .checked_add_nanos(
                i64::try_from(policy.backoff().maximum_nanos())
                    .map_err(|_| AuthorityPersistenceError::InvalidState)?,
            )
            .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        if until > latest_cooldown {
            return Err(AuthorityPersistenceError::InvalidState);
        }
    }
    Ok(())
}
