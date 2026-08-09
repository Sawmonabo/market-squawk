//! Registration and message-atomic committed observation processing.

use super::super::admission::{
    ActionHookControlFailure, ActionHookInstallCommand, ActionHookRemoveCommand,
    ActorControlCommand, RegistrationCommand, RegistrationFailure, RegistrationGrant, ShardCommand,
};
use super::super::system_timestamp;
use super::{ActorError, RouteOwner, ShardActor};
use crate::features::{CommittedFeatureInput, FeatureInvalidationReason, FeatureUpdateDisposition};
use crate::processor::{AppliedLiveObservation, LiveApplyError};
use crate::{
    ActionHookDisposition, CommittedActionContext, CommittedMarketReference, LiveRuntimeHealthKind,
    ShardLifecycleSnapshot,
};

impl ShardActor {
    pub(super) fn control(&mut self, command: ActorControlCommand) {
        match command {
            ActorControlCommand::Register(command) => self.register(command),
            ActorControlCommand::InstallActionHooks(mut command) => {
                let result = self.install_action_hooks(&mut command);
                if result.is_err() {
                    self.health_revision = self.health_revision.saturating_add(1);
                    self.emit_health(LiveRuntimeHealthKind::ProcessingRejected, None);
                }
                drop(command.response.send(result));
            }
            ActorControlCommand::RemoveActionHooks(command) => {
                let result = self.remove_action_hooks(&command);
                if result.is_err() {
                    self.health_revision = self.health_revision.saturating_add(1);
                    self.emit_health(LiveRuntimeHealthKind::ProcessingRejected, None);
                }
                drop(command.response.send(result));
            }
        }
    }

    pub(super) fn register(&mut self, command: RegistrationCommand) {
        let result = self.register_inner(&command).map(RegistrationGrant::new);
        if result.is_err() {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(
                LiveRuntimeHealthKind::GenerationRejected,
                Some(command.route.clone()),
            );
        }
        drop(command.response.send(result));
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

    fn install_action_hooks(
        &mut self,
        command: &mut ActionHookInstallCommand,
    ) -> Result<usize, ActionHookControlFailure> {
        if command.runtime_incarnation != self.runtime_incarnation {
            return Err(ActionHookControlFailure::RuntimeMismatch);
        }
        command
            .activation
            .validate_prepared(command.runtime_incarnation, command.generation)
            .map_err(|_| ActionHookControlFailure::InvalidActivation)?;
        if command.hooks.is_empty() {
            return Err(ActionHookControlFailure::EmptyGroup);
        }
        for (index, hook) in command.hooks.iter().enumerate() {
            hook.validate_retained_bytes(self.maximum_action_hook_bytes_per_route)?;
            if command.hooks[..index]
                .iter()
                .any(|prior| prior.route() == hook.route())
            {
                return Err(ActionHookControlFailure::DuplicateRoute);
            }
            let owner = self
                .routes
                .get(hook.route())
                .ok_or(ActionHookControlFailure::UnknownRoute)?;
            if owner.action_hook.is_some() {
                return Err(ActionHookControlFailure::HookAlreadyInstalled);
            }
        }
        let hooks = std::mem::take(&mut command.hooks);
        let installed = hooks.len();
        for hook in hooks {
            let Some(owner) = self.routes.get_mut(hook.route()) else {
                for owner in self.routes.values_mut() {
                    if owner
                        .action_hook
                        .as_ref()
                        .is_some_and(|hook| hook.belongs_to_dynamic_group(&command.activation))
                    {
                        owner.action_hook = None;
                    }
                }
                return Err(ActionHookControlFailure::UnknownRoute);
            };
            owner.action_hook = Some(hook.into_prepared_dynamic(command.activation.clone()));
        }
        Ok(installed)
    }

    fn remove_action_hooks(
        &mut self,
        command: &ActionHookRemoveCommand,
    ) -> Result<usize, ActionHookControlFailure> {
        if command.runtime_incarnation != self.runtime_incarnation {
            return Err(ActionHookControlFailure::RuntimeMismatch);
        }
        command
            .activation
            .validate_disabled(command.runtime_incarnation, command.generation)
            .map_err(|_| ActionHookControlFailure::InvalidActivation)?;
        let installed = self
            .routes
            .values()
            .filter(|owner| {
                owner
                    .action_hook
                    .as_ref()
                    .is_some_and(|hook| hook.belongs_to_dynamic_group(&command.activation))
            })
            .count();
        if installed != 0 && installed != command.expected_hooks {
            return Err(ActionHookControlFailure::PartialGroup);
        }
        if installed == command.expected_hooks {
            for owner in self.routes.values_mut() {
                if owner
                    .action_hook
                    .as_ref()
                    .is_some_and(|hook| hook.belongs_to_dynamic_group(&command.activation))
                {
                    owner.action_hook = None;
                }
            }
        }
        Ok(installed)
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
        let mut action_failed = false;
        let mut qualified_market_export_dropped = false;
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
                let disposition =
                    process_applied_observation(&key, owner, applied, _retained_bytes)?;
                feature_unavailable |= disposition.feature_unavailable;
                action_failed |= disposition.action_failed;
                qualified_market_export_dropped |= disposition.qualified_market_export_dropped;
                self.events_since_snapshot = self.events_since_snapshot.saturating_add(1);
                self.dirty = true;
                publish_after_batch |= self.events_since_snapshot >= self.snapshot_event_trigger;
            }
        }
        if feature_unavailable {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(LiveRuntimeHealthKind::FeatureUnavailable, Some(key.clone()));
        }
        if action_failed {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(LiveRuntimeHealthKind::ActionFailed, Some(key.clone()));
        }
        if qualified_market_export_dropped {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(
                LiveRuntimeHealthKind::QualifiedMarketExportDropped,
                Some(key.clone()),
            );
            return Err(ActorError::QualifiedMarketExportUnavailable);
        }
        if publish_after_batch {
            self.publish_snapshot(ShardLifecycleSnapshot::Ready)?;
        }
        Ok(())
    }
}

fn process_applied_observation(
    route: &crate::ShardKey,
    owner: &mut RouteOwner,
    applied: AppliedLiveObservation,
    conservative_retained_bytes: u32,
) -> Result<AppliedObservationDisposition, ActorError> {
    if let Some(authority) = applied.authority.as_ref() {
        owner.processor.validate_applied_current(authority)?;
    }
    let observed_at = event_received_at(&applied.event);
    let quality = applied.assessment.recorded_quality();
    let result = {
        let RouteOwner {
            processor,
            features,
            action_hook: _,
            qualified_market_export: _,
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
    let mut action_failed = false;
    if !unavailable
        && let Some(authority) = applied.authority.as_ref()
        && let Some(action_hook) = owner.action_hook.as_mut()
        && action_hook.action_enabled()
    {
        let feature_view = owner
            .features
            .action_view(&applied.stream, applied.generation)?;
        if feature_view.required_ready(action_hook.required_features()) {
            owner.processor.validate_applied_current(authority)?;
            let market = CommittedMarketReference::try_new(
                owner.processor.execution_terms(),
                feature_view.bids(),
                feature_view.asks(),
                observed_at,
            );
            let context = market.and_then(|market| {
                CommittedActionContext::try_new(
                    route,
                    &applied.event,
                    authority,
                    market,
                    feature_view,
                )
            });
            match context {
                Ok(context) => {
                    let mut gate = crate::action::current_authority_gate(
                        &mut owner.processor,
                        authority,
                        action_hook.issue_limit(),
                    );
                    action_failed = matches!(
                        action_hook.hook_mut().on_committed(context, &mut gate),
                        ActionHookDisposition::Failed
                    );
                }
                Err(_) => action_failed = true,
            }
        }
    }
    let qualified_market_export_dropped =
        if let Some(exporter) = owner.qualified_market_export.as_ref() {
            let observation = crate::CommittedQualifiedMarketObservation::from_committed(
                applied.event,
                applied.assessment,
                applied.binding_digest,
                applied.committed_state_revision,
                owner.processor.execution_terms(),
                applied.stable_trade_id,
            );
            observation.is_some_and(|observation| {
                exporter
                    .try_export(observation, conservative_retained_bytes)
                    .is_err()
            })
        } else {
            false
        };
    Ok(AppliedObservationDisposition {
        feature_unavailable: unavailable,
        action_failed,
        qualified_market_export_dropped,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AppliedObservationDisposition {
    feature_unavailable: bool,
    action_failed: bool,
    qualified_market_export_dropped: bool,
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
