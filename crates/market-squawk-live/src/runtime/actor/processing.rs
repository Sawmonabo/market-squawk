//! Registration and message-atomic committed observation processing.

use super::super::admission::{RegistrationCommand, RegistrationFailure, ShardCommand};
use super::super::system_timestamp;
use super::{ActorError, RouteOwner, ShardActor};
use crate::features::{CommittedFeatureInput, FeatureInvalidationReason, FeatureUpdateDisposition};
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
        let mut feature_unavailable = false;
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
                feature_unavailable |= process_applied_observation(owner, applied)?;
                self.events_since_snapshot = self.events_since_snapshot.saturating_add(1);
                self.dirty = true;
                publish_after_batch |= self.events_since_snapshot >= self.snapshot_event_trigger;
            }
        }
        if feature_unavailable {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(LiveRuntimeHealthKind::FeatureUnavailable, Some(key));
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
) -> Result<bool, ActorError> {
    if let Some(authority) = applied.authority.as_ref() {
        owner.processor.validate_applied_current(authority)?;
    }
    let observed_at = event_received_at(&applied.event);
    let quality = applied.assessment.recorded_quality();
    let result = {
        let RouteOwner {
            processor,
            features,
            generations: _,
            cross_venue_publisher: _,
            cross_venue_reader: _,
        } = owner;
        let book = processor.committed_book(&applied.stream)?;
        features.apply_committed(CommittedFeatureInput {
            stream: &applied.stream,
            generation: applied.generation,
            event: &applied.event,
            observed_at,
            quality,
            bids: book.scaled_bid_iter(),
            asks: book.scaled_ask_iter(),
        })
    };
    let mut unavailable = match result {
        Ok(FeatureUpdateDisposition::Updated) => false,
        Ok(FeatureUpdateDisposition::Unavailable | FeatureUpdateDisposition::Overflow) => true,
        Err(_) => {
            owner
                .features
                .invalidate_all(FeatureInvalidationReason::Overflow, observed_at)?;
            true
        }
    };
    if !unavailable
        && matches!(
            applied.event,
            market_squawk_domain::MarketEvent::BookSnapshot(_)
                | market_squawk_domain::MarketEvent::BookDelta(_)
                | market_squawk_domain::MarketEvent::Quote(_)
        )
        && let (Some(publisher), Some(reader)) = (
            owner.cross_venue_publisher.clone(),
            owner.cross_venue_reader.clone(),
        )
        && let Some(midpoint) = owner
            .features
            .cross_venue_midpoint(&applied.stream, applied.generation)?
    {
        let value = if publisher
            .try_publish(applied.generation, midpoint, observed_at)
            .is_err()
        {
            unavailable = true;
            market_squawk_analytics::FeatureValue::invalid(
                market_squawk_analytics::FeatureValidity::Unavailable,
                observed_at,
            )
            .map_err(crate::RouteFeatureError::from)?
        } else {
            match reader.load(publisher.instrument(), observed_at) {
                Ok(value) => {
                    unavailable = !value.validity().is_ready();
                    value
                }
                Err(_) => {
                    unavailable = true;
                    market_squawk_analytics::FeatureValue::invalid(
                        market_squawk_analytics::FeatureValidity::Unavailable,
                        observed_at,
                    )
                    .map_err(crate::RouteFeatureError::from)?
                }
            }
        };
        owner
            .features
            .apply_cross_venue(&applied.stream, applied.generation, value)?;
    }
    if !unavailable && let Some(authority) = applied.authority.as_ref() {
        owner.processor.validate_applied_current(authority)?;
        // Task 11 installs the bounded action hook at this exact post-feature authority recheck.
    }
    Ok(unavailable)
}

fn event_received_at(event: &market_squawk_domain::MarketEvent) -> market_squawk_domain::Timestamp {
    match event {
        market_squawk_domain::MarketEvent::Trade(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::Quote(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::BookSnapshot(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::BookDelta(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::Auction(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::TradingHalt(value) => value.provenance().received_at(),
        market_squawk_domain::MarketEvent::InstrumentStatus(value) => {
            value.provenance().received_at()
        }
        market_squawk_domain::MarketEvent::CorporateAction(value) => {
            value.provenance().received_at()
        }
    }
}
