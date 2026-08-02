//! Durable checkpoint conversion and restore validation.

use super::*;

pub(in crate::policy) fn checkpoint_from_runtime(
    policy: &ProviderBudgetPolicy,
    state: &BudgetState,
    observation: ClockObservation,
    availability_generation: u64,
    terminal: bool,
) -> Result<BudgetCheckpointState, AuthorityPersistenceError> {
    if state.additional_windows.len() + 1 != policy.window_count() {
        return Err(AuthorityPersistenceError::InvalidState);
    }
    let primary = policy
        .window(0)
        .ok_or(AuthorityPersistenceError::InvalidState)?;
    let mut windows = Vec::new();
    windows
        .try_reserve(policy.window_count())
        .map_err(|_| AuthorityPersistenceError::StateTooLarge)?;
    windows.push(window_checkpoint_from_runtime(
        primary,
        state.window_started_at,
        state.restored_window_ends_at,
        state.requests_used,
        &state.primary_sliding_releases,
        observation,
    )?);
    for (window, runtime) in policy.windows().skip(1).zip(&state.additional_windows) {
        windows.push(window_checkpoint_from_runtime(
            window,
            runtime.window_started_at,
            runtime.restored_window_ends_at,
            runtime.requests_used,
            &runtime.sliding_releases,
            observation,
        )?);
    }
    let unavailable_until_wall = state
        .unavailable_until
        .map(|deadline| monotonic_deadline_to_wall(observation, deadline))
        .transpose()?;
    Ok(BudgetCheckpointState {
        windows: BoundedVec::try_new(windows)
            .map_err(|_| AuthorityPersistenceError::StateTooLarge)?,
        in_flight: state.in_flight,
        unavailable_until_wall,
        disabled: state.disabled,
        consecutive_refusals: state.consecutive_refusals,
        availability_generation,
        terminal,
        poisoned: false,
    })
}

fn window_checkpoint_from_runtime(
    window: ProviderBudgetWindow,
    window_started_at: MonotonicInstant,
    restored_window_ends_at: Option<MonotonicInstant>,
    requests_used: u32,
    sliding_releases: &VecDeque<MonotonicInstant>,
    observation: ClockObservation,
) -> Result<BudgetWindowCheckpointState, AuthorityPersistenceError> {
    match window.semantics() {
        BudgetWindowSemantics::Tumbling => {
            if !sliding_releases.is_empty()
                || sliding_releases.capacity() != 0
                || requests_used > window.requests_per_window()
            {
                return Err(AuthorityPersistenceError::InvalidState);
            }
            let window_ends_at = restored_window_ends_at
                .or_else(|| window_started_at.checked_add(window.window_nanos()))
                .ok_or(AuthorityPersistenceError::InvalidState)?;
            let window_ends_wall = monotonic_deadline_to_wall(observation, window_ends_at)?;
            let duration = i64::try_from(window.window_nanos())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            let window_started_wall = window_ends_wall
                .checked_sub_nanos(duration)
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            Ok(BudgetWindowCheckpointState::Tumbling {
                window_started_wall,
                window_ends_wall,
                requests_used,
            })
        }
        BudgetWindowSemantics::Sliding => {
            let retained = u32::try_from(sliding_releases.len())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            let capacity = usize::try_from(window.requests_per_window())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            if restored_window_ends_at.is_some()
                || requests_used != retained
                || retained > window.requests_per_window()
                || sliding_releases.capacity() < capacity
                || sliding_releases
                    .iter()
                    .zip(sliding_releases.iter().skip(1))
                    .any(|(left, right)| left > right)
            {
                return Err(AuthorityPersistenceError::InvalidState);
            }
            let deadlines = sliding_releases
                .iter()
                .map(|deadline| monotonic_deadline_to_wall(observation, *deadline))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(BudgetWindowCheckpointState::Sliding {
                release_deadlines_wall: BoundedVec::try_new(deadlines)
                    .map_err(|_| AuthorityPersistenceError::StateTooLarge)?,
            })
        }
    }
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

fn wall_deadline_to_monotonic(
    observation: ClockObservation,
    deadline: Timestamp,
) -> Result<Option<MonotonicInstant>, AuthorityPersistenceError> {
    if deadline <= observation.wall_clock {
        return Ok(None);
    }
    let remaining = deadline
        .unix_nanos()
        .checked_sub(observation.wall_clock.unix_nanos())
        .ok_or(AuthorityPersistenceError::InvalidState)?;
    let remaining =
        u64::try_from(remaining).map_err(|_| AuthorityPersistenceError::InvalidState)?;
    observation
        .monotonic
        .checked_add(remaining)
        .map(Some)
        .ok_or(AuthorityPersistenceError::InvalidState)
}

pub(in crate::policy) fn runtime_state_from_checkpoint(
    policy: &ProviderBudgetPolicy,
    checkpoint: &BudgetCheckpointState,
    observation: ClockObservation,
) -> Result<BudgetState, AuthorityPersistenceError> {
    validate_checkpoint(policy, checkpoint, observation)?;
    let mut state = BudgetState::new(policy, observation.monotonic);
    restore_window_state(
        policy
            .window(0)
            .ok_or(AuthorityPersistenceError::InvalidState)?,
        checkpoint
            .windows
            .as_slice()
            .first()
            .ok_or(AuthorityPersistenceError::InvalidState)?,
        &mut state.window_started_at,
        &mut state.restored_window_ends_at,
        &mut state.requests_used,
        &mut state.primary_sliding_releases,
        observation,
    )?;
    for ((window, checkpoint_window), runtime) in policy
        .windows()
        .skip(1)
        .zip(checkpoint.windows.as_slice().iter().skip(1))
        .zip(&mut state.additional_windows)
    {
        restore_window_state(
            window,
            checkpoint_window,
            &mut runtime.window_started_at,
            &mut runtime.restored_window_ends_at,
            &mut runtime.requests_used,
            &mut runtime.sliding_releases,
            observation,
        )?;
    }
    state.in_flight = checkpoint.in_flight;
    state.unavailable_until = checkpoint
        .unavailable_until_wall
        .map(|deadline| wall_deadline_to_monotonic(observation, deadline))
        .transpose()?
        .flatten();
    state.disabled = checkpoint.disabled || checkpoint.poisoned;
    state.consecutive_refusals = checkpoint.consecutive_refusals;
    Ok(state)
}

#[allow(clippy::too_many_arguments)]
fn restore_window_state(
    window: ProviderBudgetWindow,
    checkpoint: &BudgetWindowCheckpointState,
    window_started_at: &mut MonotonicInstant,
    restored_window_ends_at: &mut Option<MonotonicInstant>,
    requests_used: &mut u32,
    sliding_releases: &mut VecDeque<MonotonicInstant>,
    observation: ClockObservation,
) -> Result<(), AuthorityPersistenceError> {
    match (window.semantics(), checkpoint) {
        (
            BudgetWindowSemantics::Tumbling,
            BudgetWindowCheckpointState::Tumbling {
                window_started_wall: _,
                window_ends_wall,
                requests_used: saved_requests,
            },
        ) => {
            *window_started_at = observation.monotonic;
            *restored_window_ends_at = wall_deadline_to_monotonic(observation, *window_ends_wall)?;
            *requests_used = if restored_window_ends_at.is_some() {
                *saved_requests
            } else {
                0
            };
        }
        (
            BudgetWindowSemantics::Sliding,
            BudgetWindowCheckpointState::Sliding {
                release_deadlines_wall,
            },
        ) => {
            for deadline in release_deadlines_wall.as_slice() {
                if let Some(deadline) = wall_deadline_to_monotonic(observation, *deadline)? {
                    sliding_releases.push_back(deadline);
                }
            }
            *window_started_at = observation.monotonic;
            *restored_window_ends_at = None;
            *requests_used = u32::try_from(sliding_releases.len())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
        }
        (BudgetWindowSemantics::Tumbling, BudgetWindowCheckpointState::Sliding { .. })
        | (BudgetWindowSemantics::Sliding, BudgetWindowCheckpointState::Tumbling { .. }) => {
            return Err(AuthorityPersistenceError::InvalidState);
        }
    }
    Ok(())
}

pub(in crate::policy) fn validate_checkpoint(
    policy: &ProviderBudgetPolicy,
    checkpoint: &BudgetCheckpointState,
    observation: ClockObservation,
) -> Result<(), AuthorityPersistenceError> {
    if checkpoint.windows.len() != policy.window_count()
        || checkpoint.in_flight > policy.max_concurrent()
        || checkpoint.availability_generation == 0
        || checkpoint.poisoned && !checkpoint.terminal
    {
        return Err(AuthorityPersistenceError::InvalidState);
    }
    for (window, saved) in policy.windows().zip(checkpoint.windows.as_slice()) {
        validate_window_checkpoint(window, saved, observation)?;
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

fn validate_window_checkpoint(
    window: ProviderBudgetWindow,
    checkpoint: &BudgetWindowCheckpointState,
    observation: ClockObservation,
) -> Result<(), AuthorityPersistenceError> {
    let latest_deadline = observation
        .wall_clock
        .checked_add_nanos(
            i64::try_from(window.window_nanos())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?,
        )
        .map_err(|_| AuthorityPersistenceError::InvalidState)?;
    match (window.semantics(), checkpoint) {
        (
            BudgetWindowSemantics::Tumbling,
            BudgetWindowCheckpointState::Tumbling {
                window_started_wall,
                window_ends_wall,
                requests_used,
            },
        ) => {
            let duration = window_ends_wall
                .unix_nanos()
                .checked_sub(window_started_wall.unix_nanos())
                .and_then(|value| u64::try_from(value).ok());
            if duration != Some(window.window_nanos())
                || *requests_used > window.requests_per_window()
            {
                return Err(AuthorityPersistenceError::InvalidState);
            }
            if *window_started_wall > observation.wall_clock || *window_ends_wall > latest_deadline
            {
                return Err(AuthorityPersistenceError::FutureState);
            }
        }
        (
            BudgetWindowSemantics::Sliding,
            BudgetWindowCheckpointState::Sliding {
                release_deadlines_wall,
            },
        ) => {
            let count = u32::try_from(release_deadlines_wall.len())
                .map_err(|_| AuthorityPersistenceError::InvalidState)?;
            if count > window.requests_per_window()
                || release_deadlines_wall
                    .as_slice()
                    .iter()
                    .zip(release_deadlines_wall.as_slice().iter().skip(1))
                    .any(|(left, right)| left > right)
            {
                return Err(AuthorityPersistenceError::InvalidState);
            }
            if release_deadlines_wall
                .as_slice()
                .iter()
                .any(|deadline| *deadline > latest_deadline)
            {
                return Err(AuthorityPersistenceError::FutureState);
            }
        }
        (BudgetWindowSemantics::Tumbling, BudgetWindowCheckpointState::Sliding { .. })
        | (BudgetWindowSemantics::Sliding, BudgetWindowCheckpointState::Tumbling { .. }) => {
            return Err(AuthorityPersistenceError::InvalidState);
        }
    }
    Ok(())
}
