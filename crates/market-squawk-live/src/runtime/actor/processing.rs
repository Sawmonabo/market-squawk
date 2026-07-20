//! Registration and message-atomic committed observation processing.

use super::super::admission::{RegistrationCommand, RegistrationFailure, ShardCommand};
use super::super::system_timestamp;
use super::{ActorError, RouteOwner, ShardActor};
use crate::processor::{AppliedLiveObservation, LiveApplyError};
use crate::{LiveRuntimeHealthKind, ShardLifecycleSnapshot};

impl ShardActor {
    pub(super) fn register(&mut self, command: RegistrationCommand) {
        let result = self.register_inner(&command);
        if result.is_err() {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(
                LiveRuntimeHealthKind::GenerationRejected,
                Some(command.route.clone()),
            );
        }
        if let Err(Ok(admission)) = command.response.send(result) {
            admission.invalidate_on_admission_failure();
        }
    }

    fn register_inner(
        &mut self,
        command: &RegistrationCommand,
    ) -> Result<crate::processor::GenerationAdmission, RegistrationFailure> {
        let now = system_timestamp().map_err(|_| RegistrationFailure::NotCurrent)?;
        let owner = self
            .routes
            .get_mut(&command.route)
            .ok_or(RegistrationFailure::UnknownRoute)?;
        owner
            .generations
            .bind_current(&command.source, now)
            .map_err(|error| match error {
                LiveApplyError::GenerationCapacityExhausted => RegistrationFailure::Capacity,
                _ => RegistrationFailure::NotCurrent,
            })
    }

    pub(super) fn process(&mut self, command: ShardCommand) -> Result<(), ActorError> {
        let admission = command.admission.clone();
        match self.process_inner(command) {
            Ok(()) => Ok(()),
            Err(error) => {
                admission.invalidate_on_admission_failure();
                self.health_revision = self.health_revision.saturating_add(1);
                self.emit_health(LiveRuntimeHealthKind::ProcessingRejected, None);
                if error.is_fatal() { Err(error) } else { Ok(()) }
            }
        }
    }

    fn process_inner(&mut self, command: ShardCommand) -> Result<(), ActorError> {
        self._guard.validate()?;
        let now = system_timestamp().map_err(|_| ActorError::ClockRange)?;
        command
            .admission
            .validate_at(now)
            .map_err(|_| ActorError::GenerationNotCurrent)?;
        let key = crate::ShardKey::new(
            command.batch.key().venue().clone(),
            command.batch.key().instrument(),
        );
        let admission = command.admission.clone();
        let _retained_bytes = command.retained_bytes;
        let mut publish_after_batch = false;
        {
            let Some(owner) = self.routes.get_mut(&key) else {
                admission.invalidate_on_admission_failure();
                return Err(ActorError::UnknownRoute);
            };
            let mut cursor = match owner
                .processor
                .accept_batch(command.batch, &command.admission)
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    admission.invalidate_on_admission_failure();
                    return Err(error.into());
                }
            };
            loop {
                let applied = match owner
                    .processor
                    .apply_next(&mut cursor, &mut self.book_scratch)
                {
                    Ok(Some(applied)) => applied,
                    Ok(None) => break,
                    Err(error) => {
                        admission.invalidate_on_admission_failure();
                        return Err(error.into());
                    }
                };
                process_applied_observation(owner, applied)?;
                self.events_since_snapshot = self.events_since_snapshot.saturating_add(1);
                self.dirty = true;
                publish_after_batch |= self.events_since_snapshot >= self.snapshot_event_trigger;
            }
        }
        if publish_after_batch {
            self.publish_snapshot(ShardLifecycleSnapshot::Ready)?;
        }
        Ok(())
    }
}

fn process_applied_observation(
    owner: &mut RouteOwner,
    applied: AppliedLiveObservation,
) -> Result<(), ActorError> {
    if let Some(authority) = applied.authority.as_ref() {
        owner.processor.validate_applied_current(authority)?;
        // The bounded feature/action hook linearizes here after the analytics contract is frozen.
        owner.processor.validate_applied_current(authority)?;
        // NoStrategy produces no order intent, so no capability is minted.
    }
    Ok(())
}
