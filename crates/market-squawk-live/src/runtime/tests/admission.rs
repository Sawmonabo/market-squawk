use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_sources::CurrentSourceAuthorityLease;
use market_squawk_sources::ProviderBookLevel;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[allow(
    dead_code,
    reason = "the shared fixture also provides public runtime constructors used by overflow.rs"
)]
#[path = "../../../tests/support/current_source.rs"]
mod current_source;

use self::current_source::{INSTRUMENT_ONE, INSTRUMENT_TWO, SourceHarness, TestResult, now, route};
use super::{
    ActorControlCommand, BoundShardIngress, COMMAND_SHARED_ALLOCATION_CHARGE, LiveIngressBindError,
    LiveIngressError, LiveRuntimeHealthEvent, LiveRuntimeHealthKind, LiveRuntimeIngress,
    RegistrationFailure, RegistrationGrant, RouteIngressChannels, ShardCommand,
    checked_command_retained_bytes,
};
use crate::authority::{RuntimeLeaseOwner, ShardLeaseOwner};
use crate::processor::{GenerationAdmission, GenerationAuthorityRegistry};
use crate::{ShardId, ShardKey};

#[derive(Debug)]
struct AdmissionHarness {
    ingress: BoundShardIngress,
    admission: GenerationAdmission,
    receiver: mpsc::Receiver<ShardCommand>,
    health: mpsc::Receiver<LiveRuntimeHealthEvent>,
    byte_budget: Arc<Semaphore>,
    _registry: GenerationAuthorityRegistry,
    _runtime_owner: RuntimeLeaseOwner,
    _shard_owner: ShardLeaseOwner,
}

#[derive(Debug)]
struct ReservationHarness {
    ingress: LiveRuntimeIngress,
    controls: mpsc::Receiver<ActorControlCommand>,
    _mailbox: mpsc::Receiver<ShardCommand>,
    _health: mpsc::Receiver<LiveRuntimeHealthEvent>,
    _runtime_owner: RuntimeLeaseOwner,
    _shard_owner: ShardLeaseOwner,
}

fn reservation_harness(route: ShardKey) -> TestResult<ReservationHarness> {
    let runtime_owner = RuntimeLeaseOwner::new(11);
    let shard_owner = ShardLeaseOwner::new(12);
    let runtime = runtime_owner.lease();
    let shard_liveness = shard_owner.lease();
    let (mailbox, mailbox_receiver) = mpsc::channel(1);
    let (control, controls) = mpsc::channel(1);
    let (health, health_receiver) = mpsc::channel(1);
    let channels = RouteIngressChannels {
        shard: ShardId::new(0, 1)?,
        runtime: runtime.clone(),
        shard_liveness,
        mailbox,
        byte_budget: Arc::new(Semaphore::new(65_536)),
        control: control.clone(),
        registration_deadline: Duration::from_secs(1),
        maximum_message_bytes: 65_536,
        health,
    };
    let mut routes = HashMap::new();
    routes.insert(route, channels);
    Ok(ReservationHarness {
        ingress: LiveRuntimeIngress {
            routes: Arc::new(routes),
            runtime,
        },
        controls,
        _mailbox: mailbox_receiver,
        _health: health_receiver,
        _runtime_owner: runtime_owner,
        _shard_owner: shard_owner,
    })
}

#[test]
fn reserve_route_owns_exactly_one_control_slot_before_source_authority_exists() -> TestResult {
    let route = route(INSTRUMENT_ONE)?;
    let harness = reservation_harness(route.clone())?;

    let dormant = harness.ingress.reserve_route(route.clone())?;

    assert_eq!(dormant.route(), &route);
    assert_eq!(dormant.shard(), ShardId::new(0, 1)?);
    assert_eq!(
        harness.ingress.reserve_route(route.clone()).err(),
        Some(LiveIngressBindError::ControlCapacityFull)
    );
    drop(dormant);
    assert!(harness.ingress.reserve_route(route).is_ok());
    Ok(())
}

#[tokio::test]
async fn consuming_activation_binds_once_and_cancelled_activation_releases_reservation()
-> TestResult {
    let route = route(INSTRUMENT_ONE)?;
    let mut harness = reservation_harness(route.clone())?;
    let source = SourceHarness::try_new("dormant-source", 1, INSTRUMENT_ONE)?;
    let dormant = harness.ingress.reserve_route(route.clone())?;
    let activation = dormant.activate(source.current_lease()?, CancellationToken::new());
    let registration = async {
        let command = harness.controls.recv().await.ok_or("registration closed")?;
        let ActorControlCommand::Register(command) = command else {
            return Err("unexpected non-registration control".into());
        };
        let (registry, admission) = admission(&command.source)?;
        command
            .response
            .send(Ok(RegistrationGrant::new(admission)))
            .map_err(|_| "activation receiver dropped")?;
        TestResult::Ok(registry)
    };
    let (bound, registry) = tokio::join!(activation, registration);
    let bound = bound?;
    let _registry = registry?;
    assert_eq!(bound.route(), &route);
    assert_eq!(bound.shard(), ShardId::new(0, 1)?);

    let cancelled = harness.ingress.reserve_route(route.clone())?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        cancelled
            .activate(source.current_lease()?, cancellation)
            .await
            .err(),
        Some(LiveIngressBindError::Cancelled)
    );
    assert!(harness.ingress.reserve_route(route).is_ok());
    Ok(())
}

#[test]
fn dropped_buffered_registration_response_invalidates_minted_admission() -> TestResult {
    let source = SourceHarness::try_new("dropped-registration", 1, INSTRUMENT_ONE)?;
    let (_registry, admission) = admission(&source.current_lease()?)?;
    let probe = admission.clone();
    let (response, receiver) = oneshot::channel::<Result<RegistrationGrant, RegistrationFailure>>();

    response
        .send(Ok(RegistrationGrant::new(admission)))
        .map_err(|_| "registration receiver dropped before send")?;
    drop(receiver);

    assert!(probe.validate_at(now()?).is_err());
    Ok(())
}

fn admission(
    source: &CurrentSourceAuthorityLease,
) -> TestResult<(GenerationAuthorityRegistry, GenerationAdmission)> {
    let mut registry = GenerationAuthorityRegistry::try_new(4)?;
    let admission = registry.bind_current(source, now()?)?;
    Ok((registry, admission))
}

fn harness(
    source: &CurrentSourceAuthorityLease,
    route: ShardKey,
    mailbox_capacity: usize,
    byte_capacity: u32,
    maximum_message_bytes: u32,
) -> TestResult<AdmissionHarness> {
    let (registry, admission) = admission(source)?;
    let runtime_owner = RuntimeLeaseOwner::new(1);
    let shard_owner = ShardLeaseOwner::new(1);
    let (mailbox, receiver) = mpsc::channel(mailbox_capacity);
    let (health_sender, health) = mpsc::channel(8);
    let byte_budget = Arc::new(Semaphore::new(usize::try_from(byte_capacity)?));
    let ingress = BoundShardIngress {
        route,
        shard: ShardId::new(0, 1)?,
        runtime: runtime_owner.lease(),
        shard_liveness: shard_owner.lease(),
        mailbox,
        byte_budget: Arc::clone(&byte_budget),
        maximum_message_bytes,
        admission: admission.clone(),
        health: health_sender,
    };
    Ok(AdmissionHarness {
        ingress,
        admission,
        receiver,
        health,
        byte_budget,
        _registry: registry,
        _runtime_owner: runtime_owner,
        _shard_owner: shard_owner,
    })
}

fn command_cost(
    batch: &market_squawk_sources::CurrentDecodedProviderBatch,
    admission: &GenerationAdmission,
) -> Result<u32, LiveIngressError> {
    ShardCommand::checked_retained_bytes(batch, admission)
}

#[test]
fn retained_size_arithmetic_accepts_u32_max_and_rejects_every_overflow() -> TestResult {
    let fixed = size_of::<ShardCommand>()
        .checked_add(COMMAND_SHARED_ALLOCATION_CHARGE)
        .ok_or("fixed command cost overflow")?;
    let u32_max = usize::try_from(u32::MAX)?;
    let exact_batch = u32_max
        .checked_sub(fixed)
        .ok_or("fixed command cost exceeds u32")?;
    assert_eq!(checked_command_retained_bytes(exact_batch, 0)?, u32::MAX);
    if let Some(over_u32) = u32_max.checked_add(1) {
        let over_batch = over_u32
            .checked_sub(fixed)
            .ok_or("fixed command cost exceeds u32 plus one")?;
        assert_eq!(
            checked_command_retained_bytes(over_batch, 0),
            Err(LiveIngressError::RetainedSizeOverflow)
        );
    }
    assert_eq!(
        checked_command_retained_bytes(usize::MAX, 0),
        Err(LiveIngressError::RetainedSizeOverflow)
    );
    assert_eq!(
        checked_command_retained_bytes(0, usize::MAX),
        Err(LiveIngressError::RetainedSizeOverflow)
    );
    Ok(())
}

#[test]
fn exact_command_cost_and_permit_live_through_dequeue_until_drop() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, batch) = source.batch("trade-1", 1)?;
    let (registry, admission) = admission(&lease)?;
    let retained = command_cost(&batch, &admission)?;
    let expected = batch
        .retained_bytes()
        .checked_add(size_of::<ShardCommand>())
        .and_then(|value| value.checked_add(admission.retained_bytes().ok()?))
        .and_then(|value| value.checked_add(COMMAND_SHARED_ALLOCATION_CHARGE))
        .ok_or("expected command cost overflow")?;
    assert_eq!(usize::try_from(retained)?, expected);
    drop(registry);

    let mut harness = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    harness.ingress.try_publish(batch)?;
    assert_eq!(harness.byte_budget.available_permits(), 0);

    let command = harness.receiver.try_recv()?;
    assert_eq!(command.retained_bytes, retained);
    assert_eq!(harness.byte_budget.available_permits(), 0);
    command.admission.validate_at(now()?)?;
    drop(command);
    assert_eq!(
        harness.byte_budget.available_permits(),
        usize::try_from(retained)?
    );
    Ok(())
}

#[test]
fn nested_book_storage_is_rejected_between_old_and_closed_shape_estimates() -> TestResult {
    const LEVEL_COUNT: usize = 10_000;

    let mut source = SourceHarness::try_new("source-nested", 1, INSTRUMENT_ONE)?;
    let (lease, batch) = source.book_snapshot_batch("snapshot-1", 1, LEVEL_COUNT)?;
    let (registry, preview) = admission(&lease)?;
    let corrected = command_cost(&batch, &preview)?;
    let nested_allocation = LEVEL_COUNT
        .checked_mul(size_of::<ProviderBookLevel>())
        .ok_or("nested allocation overflow")?;
    let former = usize::try_from(corrected)?
        .checked_sub(nested_allocation)
        .ok_or("corrected charge did not include nested allocation")?;
    let maximum = former
        .checked_add(nested_allocation / 2)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or("message limit overflow")?;
    assert!(former < usize::try_from(maximum)?);
    assert!(maximum < corrected);
    drop(registry);

    let mut harness = harness(&lease, route(INSTRUMENT_ONE)?, 1, corrected, maximum)?;
    assert_eq!(
        harness.ingress.try_publish(batch),
        Err(LiveIngressError::MessageTooLarge {
            retained: corrected,
            maximum,
        })
    );
    assert!(harness.receiver.try_recv().is_err());
    assert!(harness.admission.validate_at(now()?).is_err());
    Ok(())
}

#[test]
fn count_full_invalidates_before_return_and_reclaims_candidate_permit() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, first) = source.batch("trade-1", 1)?;
    let (_, second) = source.batch("trade-2", 2)?;
    let (_, preview) = admission(&lease)?;
    let first_cost = command_cost(&first, &preview)?;
    let second_cost = command_cost(&second, &preview)?;
    let byte_capacity = first_cost
        .checked_add(second_cost)
        .ok_or("byte capacity overflow")?;
    let mut harness = harness(
        &lease,
        route(INSTRUMENT_ONE)?,
        1,
        byte_capacity,
        first_cost.max(second_cost),
    )?;

    harness.ingress.try_publish(first)?;
    assert_eq!(
        harness.ingress.try_publish(second),
        Err(LiveIngressError::CountCapacityFull)
    );
    assert!(harness.admission.validate_at(now()?).is_err());
    assert_eq!(
        harness.byte_budget.available_permits(),
        usize::try_from(second_cost)?
    );
    let event = harness.health.try_recv()?;
    assert_eq!(event.kind(), LiveRuntimeHealthKind::IngressRejected);

    let queued = harness.receiver.try_recv()?;
    assert!(queued.admission.validate_at(now()?).is_err());
    drop(queued);
    assert_eq!(
        harness.byte_budget.available_permits(),
        usize::try_from(byte_capacity)?
    );
    Ok(())
}

#[test]
fn byte_full_invalidates_without_enqueuing_and_releases_after_queued_drop() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, first) = source.batch("trade-1", 1)?;
    let (_, second) = source.batch("trade-2", 2)?;
    let (_, preview) = admission(&lease)?;
    let first_cost = command_cost(&first, &preview)?;
    let second_cost = command_cost(&second, &preview)?;
    let byte_capacity = first_cost
        .checked_add(second_cost)
        .and_then(|value| value.checked_sub(1))
        .ok_or("byte capacity overflow")?;
    let mut harness = harness(
        &lease,
        route(INSTRUMENT_ONE)?,
        2,
        byte_capacity,
        first_cost.max(second_cost),
    )?;

    harness.ingress.try_publish(first)?;
    assert_eq!(
        harness.ingress.try_publish(second),
        Err(LiveIngressError::ByteCapacityFull)
    );
    assert!(harness.admission.validate_at(now()?).is_err());
    assert_eq!(harness.receiver.len(), 1);
    assert_eq!(
        harness.byte_budget.available_permits(),
        usize::try_from(second_cost - 1)?
    );
    drop(harness.receiver.try_recv()?);
    assert_eq!(
        harness.byte_budget.available_permits(),
        usize::try_from(byte_capacity)?
    );
    Ok(())
}

#[test]
fn exact_message_and_byte_edge_accepts_then_one_under_rejects_overweight() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, accepted) = source.batch("trade-1", 1)?;
    let (_, preview) = admission(&lease)?;
    let retained = command_cost(&accepted, &preview)?;
    let mut exact = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    exact.ingress.try_publish(accepted)?;
    assert_eq!(exact.byte_budget.available_permits(), 0);
    drop(exact.receiver.try_recv()?);
    assert_eq!(
        exact.byte_budget.available_permits(),
        usize::try_from(retained)?
    );

    let (_, oversized) = source.batch("trade-2", 2)?;
    let too_small = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained - 1)?;
    assert_eq!(
        too_small.ingress.try_publish(oversized),
        Err(LiveIngressError::MessageTooLarge {
            retained,
            maximum: retained - 1,
        })
    );
    assert!(too_small.admission.validate_at(now()?).is_err());
    assert_eq!(
        too_small.byte_budget.available_permits(),
        usize::try_from(retained)?
    );
    assert!(too_small.receiver.is_empty());
    Ok(())
}

#[test]
fn closed_receiver_and_closed_byte_budget_reclaim_and_invalidate() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, closed_batch) = source.batch("trade-1", 1)?;
    let (_, preview) = admission(&lease)?;
    let retained = command_cost(&closed_batch, &preview)?;
    let receiver_closed = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    drop(receiver_closed.receiver);
    assert_eq!(
        receiver_closed.ingress.try_publish(closed_batch),
        Err(LiveIngressError::MailboxClosed)
    );
    assert!(receiver_closed.admission.validate_at(now()?).is_err());
    assert_eq!(
        receiver_closed.byte_budget.available_permits(),
        usize::try_from(retained)?
    );

    let (_, semaphore_batch) = source.batch("trade-2", 2)?;
    let semaphore_closed = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    semaphore_closed.byte_budget.close();
    assert_eq!(
        semaphore_closed.ingress.try_publish(semaphore_batch),
        Err(LiveIngressError::MailboxClosed)
    );
    assert!(semaphore_closed.admission.validate_at(now()?).is_err());
    Ok(())
}

#[test]
fn wrong_route_and_source_transplant_fail_before_byte_admission() -> TestResult {
    let mut primary = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, _) = primary.batch("primary", 1)?;

    let mut wrong_route = SourceHarness::try_new("source-b", 1, INSTRUMENT_TWO)?;
    let (_, wrong_batch) = wrong_route.batch("wrong-route", 1)?;
    let (_, preview) = admission(&lease)?;
    let wrong_cost = command_cost(&wrong_batch, &preview)?;
    let wrong = harness(&lease, route(INSTRUMENT_ONE)?, 1, wrong_cost, wrong_cost)?;
    assert_eq!(
        wrong.ingress.try_publish(wrong_batch),
        Err(LiveIngressError::WrongRoute)
    );
    assert!(wrong.admission.validate_at(now()?).is_err());
    assert_eq!(
        wrong.byte_budget.available_permits(),
        usize::try_from(wrong_cost)?
    );

    let mut transplanted = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (_, transplant_batch) = transplanted.batch("transplant", 1)?;
    let transplant_cost = command_cost(&transplant_batch, &preview)?;
    let transplant = harness(
        &lease,
        route(INSTRUMENT_ONE)?,
        1,
        transplant_cost,
        transplant_cost,
    )?;
    assert_eq!(
        transplant.ingress.try_publish(transplant_batch),
        Err(LiveIngressError::SourceLeaseTransplant)
    );
    assert!(transplant.admission.validate_at(now()?).is_err());
    assert_eq!(
        transplant.byte_budget.available_permits(),
        usize::try_from(transplant_cost)?
    );
    Ok(())
}

#[test]
fn runtime_shard_and_source_validation_fail_closed_in_order() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (lease, runtime_batch) = source.batch("runtime", 1)?;
    let (_, preview) = admission(&lease)?;
    let retained = command_cost(&runtime_batch, &preview)?;
    let mut runtime_closed = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    runtime_closed._runtime_owner.invalidate();
    runtime_closed._shard_owner.invalidate();
    assert_eq!(
        runtime_closed.ingress.try_publish(runtime_batch),
        Err(LiveIngressError::RuntimeClosed)
    );
    assert!(runtime_closed.admission.validate_at(now()?).is_err());

    let (_, shard_batch) = source.batch("shard", 2)?;
    let mut shard_closed = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    shard_closed._shard_owner.invalidate();
    assert_eq!(
        shard_closed.ingress.try_publish(shard_batch),
        Err(LiveIngressError::ShardClosed)
    );
    assert!(shard_closed.admission.validate_at(now()?).is_err());

    let (_, source_batch) = source.batch("source", 3)?;
    let source_closed = harness(&lease, route(INSTRUMENT_ONE)?, 1, retained, retained)?;
    source.capture_degradation.mark_incomplete();
    assert_eq!(
        source_closed.ingress.try_publish(source_batch),
        Err(LiveIngressError::GenerationNotCurrent)
    );
    assert!(source_closed.admission.validate_at(now()?).is_err());
    Ok(())
}

#[test]
fn health_rebind_and_rollover_replace_generation_without_cross_invalidation() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let old_lease = source.current_lease()?;
    let mut registry = GenerationAuthorityRegistry::try_new(2)?;
    let old = registry.bind_current(&old_lease, now()?)?;

    source.refresh_health()?;
    let refreshed_lease = source.current_lease()?;
    let refreshed = registry.bind_current(&refreshed_lease, now()?)?;
    old.validate_at(now()?)?;
    refreshed.validate_at(now()?)?;
    old.invalidate_on_admission_failure();
    refreshed.validate_at(now()?)?;

    let source = source.rollover(2)?;
    let successor_lease = source.current_lease()?;
    let successor = registry.bind_current(&successor_lease, now()?)?;
    successor.validate_at(now()?)?;
    old.invalidate_on_admission_failure();
    successor.validate_at(now()?)?;
    Ok(())
}
