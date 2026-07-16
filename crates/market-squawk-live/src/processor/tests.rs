#[path = "tests/fixture.rs"]
mod fixture;

use std::time::{Duration, Instant};

use market_squawk_domain::{SourceId, SourceIdentifier, Timestamp, TradingStatus, VenueId};
use market_squawk_sources::{CurrentDecodedProviderBatch, ProviderSnapshotEvidence, RegistryError};

use self::fixture::{
    EVALUATED_AT, FRAME_AT, HEALTH_AT, SourceHarness, TestResult, VALID_UNTIL, definition,
    non_book_snapshot, snapshot, status, trade, valid_multi_change_delta,
};
use super::error::LiveApplyError;
use super::snapshot::{StatusSnapshotSeed, StreamSnapshotSeed};
use super::{
    AppliedLiveObservation, GenerationAdmission, GenerationAuthorityRegistry,
    InstrumentLiveProcessor, ProcessorLivenessBinding, ProcessorSnapshotLimits,
    ProcessorSnapshotSeed,
};
use crate::authority::{
    AppliedObservationAuthority, AuthorityError, ClockReading, RuntimeLeaseOwner,
    ScriptedTrustedClock, ShardLeaseOwner,
};
use crate::{DepthLimit, GenerationPhase};

fn fixed_clock() -> TestResult<ScriptedTrustedClock> {
    let reading = ClockReading::new(Timestamp::from_unix_nanos(EVALUATED_AT), Instant::now());
    Ok(ScriptedTrustedClock::try_new(vec![reading])?)
}

fn processor(
    clock: ScriptedTrustedClock,
    nonce_capacity: usize,
) -> TestResult<(
    InstrumentLiveProcessor<ScriptedTrustedClock>,
    ShardLeaseOwner,
    RuntimeLeaseOwner,
)> {
    processor_with_lifetime(clock, nonce_capacity, Duration::from_nanos(100_000))
}

fn processor_with_lifetime(
    clock: ScriptedTrustedClock,
    nonce_capacity: usize,
    maximum_capability_lifetime: Duration,
) -> TestResult<(
    InstrumentLiveProcessor<ScriptedTrustedClock>,
    ShardLeaseOwner,
    RuntimeLeaseOwner,
)> {
    let shard = ShardLeaseOwner::new(11);
    let runtime = RuntimeLeaseOwner::new(13);
    let processor = InstrumentLiveProcessor::try_new(
        definition()?,
        DepthLimit::new(4)?,
        8,
        nonce_capacity,
        1,
        maximum_capability_lifetime,
        ProcessorLivenessBinding::new(shard.lease(), runtime.lease()),
        clock,
    )?;
    Ok((processor, shard, runtime))
}

fn apply_one(
    processor: &mut InstrumentLiveProcessor<ScriptedTrustedClock>,
    admission: &GenerationAdmission,
    batch: CurrentDecodedProviderBatch,
) -> Result<AppliedLiveObservation, LiveApplyError> {
    let mut cursor = processor.accept_batch(batch, admission)?;
    let applied = processor
        .apply_next(&mut cursor)?
        .ok_or(LiveApplyError::BindingMismatch)?;
    if processor.apply_next(&mut cursor)?.is_some() {
        return Err(LiveApplyError::BindingMismatch);
    }
    Ok(applied)
}

#[derive(Debug)]
struct ReadyTrade {
    processor: InstrumentLiveProcessor<ScriptedTrustedClock>,
    source: SourceHarness,
    registry: GenerationAuthorityRegistry,
    admission: GenerationAdmission,
    shard: ShardLeaseOwner,
    runtime: RuntimeLeaseOwner,
    applied: AppliedObservationAuthority,
}

fn ready_trade(clock: ScriptedTrustedClock, nonce_capacity: usize) -> TestResult<ReadyTrade> {
    let mut source = SourceHarness::try_new("source-a", 1)?;
    let (lease, batch) = source.batch("trade-1", 1, trade()?, non_book_snapshot()?)?;
    let mut registry = GenerationAuthorityRegistry::try_new(2)?;
    let admission = registry.bind_current(&lease, Timestamp::from_unix_nanos(EVALUATED_AT))?;
    let (mut processor, shard, runtime) = processor(clock, nonce_capacity)?;
    let applied = apply_one(&mut processor, &admission, batch)?;
    let authority = applied.authority.ok_or("trade did not produce authority")?;
    Ok(ReadyTrade {
        processor,
        source,
        registry,
        admission,
        shard,
        runtime,
        applied: authority,
    })
}

#[test]
fn generation_registry_rebinds_refresh_and_replaces_rollover_atomically() -> TestResult {
    let mut source = SourceHarness::try_new("source-a", 1)?;
    let first_lease = source.current_lease(HEALTH_AT)?;
    let mut registry = GenerationAuthorityRegistry::try_new(2)?;
    let first = registry.bind_current(&first_lease, Timestamp::from_unix_nanos(HEALTH_AT))?;

    source.refresh_health(HEALTH_AT + 1)?;
    let refreshed_lease = source.current_lease(HEALTH_AT + 1)?;
    let refreshed =
        registry.bind_current(&refreshed_lease, Timestamp::from_unix_nanos(HEALTH_AT + 1))?;
    assert!(
        first
            .generation()
            .shares_allocation_with(&refreshed.generation())
    );
    assert!(
        first
            .validate_at(Timestamp::from_unix_nanos(HEALTH_AT + 1))
            .is_err()
    );
    refreshed.validate_at(Timestamp::from_unix_nanos(HEALTH_AT + 1))?;

    let source = source.rollover(2, HEALTH_AT + 2)?;
    let successor_lease = source.current_lease(HEALTH_AT + 3)?;
    let successor =
        registry.bind_current(&successor_lease, Timestamp::from_unix_nanos(HEALTH_AT + 3))?;
    assert!(
        !refreshed
            .generation()
            .shares_allocation_with(&successor.generation())
    );
    assert!(refreshed.generation().validate().is_err());
    successor.validate_at(Timestamp::from_unix_nanos(HEALTH_AT + 3))?;
    Ok(())
}

#[test]
fn generation_registry_rejects_transplant_capacity_and_exit_resurrection() -> TestResult {
    let first_source = SourceHarness::try_new("source-a", 1)?;
    let transplanted_source = SourceHarness::try_new("source-a", 1)?;
    let second_source = SourceHarness::try_new("source-b", 1)?;
    let first_lease = first_source.current_lease(HEALTH_AT)?;
    let transplanted_lease = transplanted_source.current_lease(HEALTH_AT)?;
    let second_lease = second_source.current_lease(HEALTH_AT)?;
    let mut registry = GenerationAuthorityRegistry::try_new(1)?;
    let admission = registry.bind_current(&first_lease, Timestamp::from_unix_nanos(HEALTH_AT))?;
    assert!(matches!(
        registry.bind_current(&transplanted_lease, Timestamp::from_unix_nanos(HEALTH_AT)),
        Err(LiveApplyError::GenerationAdmissionTransplant)
    ));
    assert!(matches!(
        registry.bind_current(&second_lease, Timestamp::from_unix_nanos(HEALTH_AT)),
        Err(LiveApplyError::GenerationCapacityExhausted)
    ));

    let exit = registry.exit_handle();
    exit.invalidate();
    assert!(matches!(
        admission.validate_at(Timestamp::from_unix_nanos(HEALTH_AT)),
        Err(LiveApplyError::Authority(AuthorityError::Revoked))
    ));
    assert!(matches!(
        registry.bind_current(&first_lease, Timestamp::from_unix_nanos(HEALTH_AT)),
        Err(LiveApplyError::Authority(AuthorityError::Revoked))
    ));
    Ok(())
}

#[test]
fn committed_seed_is_sorted_truncated_and_exactly_base_charged() -> TestResult {
    let mut source_b = SourceHarness::try_new("source-b", 1)?;
    let mut source_a = SourceHarness::try_new("source-a", 1)?;
    let (lease_b, batch_b) = source_b.batch("trade-b", 1, trade()?, non_book_snapshot()?)?;
    let (lease_a, batch_a) = source_a.batch("trade-a", 1, trade()?, non_book_snapshot()?)?;
    let mut registry = GenerationAuthorityRegistry::try_new(2)?;
    let admission_b = registry.bind_current(&lease_b, Timestamp::from_unix_nanos(EVALUATED_AT))?;
    let admission_a = registry.bind_current(&lease_a, Timestamp::from_unix_nanos(EVALUATED_AT))?;
    let (mut processor, _shard, _runtime) = processor(fixed_clock()?, 4)?;
    let _ = apply_one(&mut processor, &admission_b, batch_b)?;
    let applied_a = apply_one(&mut processor, &admission_a, batch_a)?;
    assert!(applied_a.authority.is_some());

    let seed = processor.snapshot_seed(ProcessorSnapshotLimits::try_new(1, 1, 1, 64 * 1024)?)?;
    let stream = seed.streams.first().ok_or("missing stream seed")?;
    let status = seed.statuses.first().ok_or("missing status seed")?;
    assert_eq!(seed.total_streams, 2);
    assert_eq!(seed.total_statuses, 2);
    assert_eq!(seed.output_stream_count, 1);
    assert_eq!(seed.output_status_count, 1);
    assert!(!seed.streams_complete);
    assert!(!seed.statuses_complete);
    assert_eq!(stream.key.source_id().as_str(), "source-a");
    assert_eq!(status.source_id.as_str(), "source-a");
    assert_eq!(stream.phase, GenerationPhase::Healthy);
    assert_eq!(stream.revision, 1);
    assert_eq!(stream.health_epoch, 1);
    assert_eq!(
        stream.source_valid_until,
        Some(Timestamp::from_unix_nanos(VALID_UNTIL))
    );
    assert_eq!(
        stream.received_at,
        Some(Timestamp::from_unix_nanos(FRAME_AT))
    );
    assert_eq!(
        stream.evaluated_at,
        Some(Timestamp::from_unix_nanos(EVALUATED_AT))
    );
    let expected_retained = std::mem::size_of::<ProcessorSnapshotSeed>()
        + std::mem::size_of::<StreamSnapshotSeed>()
        + SourceId::MAX_LENGTH
        + VenueId::MAX_LENGTH
        + SourceIdentifier::MAX_LENGTH * 2
        + std::mem::size_of::<StatusSnapshotSeed>()
        + SourceId::MAX_LENGTH
        + VenueId::MAX_LENGTH;
    assert_eq!(seed.retained_bytes, expected_retained);
    Ok(())
}

#[test]
fn late_delta_expiry_rolls_back_book_sequence_revision_and_status() -> TestResult {
    let base_mono = Instant::now();
    let current = ClockReading::new(Timestamp::from_unix_nanos(EVALUATED_AT), base_mono);
    let expired = ClockReading::new(
        Timestamp::from_unix_nanos(VALID_UNTIL + 1),
        base_mono
            .checked_add(Duration::from_nanos(u64::try_from(
                VALID_UNTIL - EVALUATED_AT + 1,
            )?))
            .ok_or("expired instant overflow")?,
    );
    let clock = ScriptedTrustedClock::try_new(vec![
        current, current, current, current, current, current, expired,
    ])?;
    let mut source = SourceHarness::try_new("source-a", 1)?;
    let (lease, snapshot_batch) = source.batch(
        "snapshot-1",
        1,
        snapshot()?,
        ProviderSnapshotEvidence::InitializingSnapshot {
            provider_reference: Some(fixture::id("snapshot-origin")?),
        },
    )?;
    let mut registry = GenerationAuthorityRegistry::try_new(1)?;
    let admission = registry.bind_current(&lease, Timestamp::from_unix_nanos(EVALUATED_AT))?;
    let (mut processor, _shard, _runtime) = processor(clock, 4)?;
    let applied = apply_one(&mut processor, &admission, snapshot_batch)?;
    let authority = applied
        .authority
        .ok_or("snapshot did not produce authority")?;
    let limits = ProcessorSnapshotLimits::try_new(8, 8, 8, 64 * 1024)?;
    let before = processor.snapshot_seed(limits)?;
    let before_stream = before.streams.first().ok_or("missing prior stream")?;
    let before_bids = before_stream.bids.to_vec();
    let before_asks = before_stream.asks.to_vec();
    let before_revision = before_stream.revision;
    let before_sequence = before_stream.last_sequence;
    let before_status_revision = before_stream.trading_status_revision;

    let (_, delta_batch) = source.batch(
        "delta-2",
        2,
        valid_multi_change_delta()?,
        ProviderSnapshotEvidence::Delta {
            provider_snapshot_reference: Some(fixture::id("snapshot-origin")?),
        },
    )?;
    let mut cursor = processor.accept_batch(delta_batch, &admission)?;
    assert!(matches!(
        processor.apply_next(&mut cursor),
        Err(LiveApplyError::Source(RegistryError::HealthNotQualified))
    ));

    let after = processor.snapshot_seed(limits)?;
    let after_stream = after.streams.first().ok_or("missing rolled-back stream")?;
    assert_eq!(after_stream.phase, GenerationPhase::Quarantined);
    assert!(!after_stream.generation_current);
    assert_eq!(after_stream.revision, before_revision);
    assert_eq!(after_stream.last_sequence, before_sequence);
    assert_eq!(after_stream.bids.as_ref(), before_bids.as_slice());
    assert_eq!(after_stream.asks.as_ref(), before_asks.as_slice());
    assert_eq!(after_stream.trading_status_revision, before_status_revision);
    assert_eq!(after.total_statuses, before.total_statuses);
    assert_eq!(
        processor.validate_applied_current(&authority),
        Err(AuthorityError::SourceRevoked)
    );
    Ok(())
}

#[test]
fn status_and_stream_revision_transitions_revoke_prior_authority() -> TestResult {
    let mut ready = ready_trade(fixed_clock()?, 4)?;
    let limits = ProcessorSnapshotLimits::try_new(8, 8, 8, 64 * 1024)?;
    let initial = ready.processor.snapshot_seed(limits)?;
    assert_eq!(
        initial
            .statuses
            .first()
            .ok_or("missing initial status")?
            .revision,
        1
    );
    let (_, second_trade) = ready
        .source
        .batch("trade-2", 2, trade()?, non_book_snapshot()?)?;
    let next = apply_one(&mut ready.processor, &ready.admission, second_trade)?;
    assert_eq!(
        ready.processor.validate_applied_current(&ready.applied),
        Err(AuthorityError::StaleRevision {
            expected: 1,
            current: 2,
        })
    );
    let next_authority = next.authority.ok_or("second trade lost authority")?;

    let (_, halted) = ready.source.batch(
        "status-3",
        3,
        status(TradingStatus::Halted)?,
        non_book_snapshot()?,
    )?;
    let halt = apply_one(&mut ready.processor, &ready.admission, halted)?;
    assert!(halt.authority.is_none());
    assert_eq!(
        ready.processor.validate_applied_current(&next_authority),
        Err(AuthorityError::Revoked)
    );
    let halted_seed = ready.processor.snapshot_seed(limits)?;
    let stream = halted_seed.streams.first().ok_or("missing status stream")?;
    assert_eq!(stream.trading_status, Some(TradingStatus::Halted));
    assert_eq!(stream.trading_status_revision, Some(2));
    assert_eq!(stream.revision, 3);
    assert_eq!(
        halted_seed
            .statuses
            .first()
            .ok_or("missing halted status")?
            .revision,
        2
    );

    let (_, active) = ready.source.batch(
        "status-4",
        4,
        status(TradingStatus::Active)?,
        non_book_snapshot()?,
    )?;
    let resumed = apply_one(&mut ready.processor, &ready.admission, active)?;
    assert!(resumed.authority.is_none());
    let active_seed = ready.processor.snapshot_seed(limits)?;
    let active_stream = active_seed
        .streams
        .first()
        .ok_or("missing resumed stream")?;
    assert_eq!(active_stream.trading_status, Some(TradingStatus::Active));
    assert_eq!(active_stream.trading_status_revision, Some(3));
    assert_eq!(
        active_seed
            .statuses
            .first()
            .ok_or("missing resumed status")?
            .revision,
        3
    );
    Ok(())
}

#[test]
fn status_allocation_overflow_preserves_last_good_status_and_stream_revision() -> TestResult {
    let mut ready = ready_trade(fixed_clock()?, 2)?;
    let limits = ProcessorSnapshotLimits::try_new(8, 8, 8, 64 * 1024)?;
    let before = ready.processor.snapshot_seed(limits)?;
    let stream = before.streams.first().ok_or("missing current stream")?;
    let key = stream.key.clone();
    let before_revision = stream.revision;
    ready
        .processor
        .statuses
        .set_allocation_version_for_test(&key, u64::MAX)?;

    let (_, halted) = ready.source.batch(
        "status-2",
        2,
        status(TradingStatus::Halted)?,
        non_book_snapshot()?,
    )?;
    let mut cursor = ready.processor.accept_batch(halted, &ready.admission)?;
    assert!(matches!(
        ready.processor.apply_next(&mut cursor),
        Err(LiveApplyError::StatusRevisionExhausted)
    ));
    let after = ready.processor.snapshot_seed(limits)?;
    let after_stream = after.streams.first().ok_or("missing overflow stream")?;
    let after_status = after.statuses.first().ok_or("missing overflow status")?;
    assert_eq!(after_stream.phase, GenerationPhase::Quarantined);
    assert_eq!(after_stream.revision, before_revision);
    assert_eq!(after_stream.trading_status, Some(TradingStatus::Active));
    assert_eq!(after_stream.trading_status_revision, Some(u64::MAX));
    assert_eq!(after_status.status, TradingStatus::Active);
    assert_eq!(after_status.revision, u64::MAX);
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum RevocationPoint {
    Capture,
    Generation,
    Shard,
}

#[test]
fn issue_and_consume_recheck_source_generation_and_shard() -> TestResult {
    for point in [
        RevocationPoint::Capture,
        RevocationPoint::Generation,
        RevocationPoint::Shard,
    ] {
        let mut ready = ready_trade(fixed_clock()?, 2)?;
        let capability = ready.processor.issue(&ready.applied)?;
        let expected = match point {
            RevocationPoint::Capture => {
                ready.source.capture_degradation.mark_incomplete();
                AuthorityError::SourceRevoked
            }
            RevocationPoint::Generation => {
                ready.admission.invalidate_on_admission_failure();
                AuthorityError::Revoked
            }
            RevocationPoint::Shard => {
                ready.shard.invalidate();
                AuthorityError::Revoked
            }
        };
        assert!(matches!(
            ready.processor.consume(capability),
            Err(error) if error == expected
        ));
    }
    Ok(())
}

#[test]
fn consumed_authority_rechecks_runtime_and_processor_exit() -> TestResult {
    let mut ready = ready_trade(fixed_clock()?, 2)?;
    let capability = ready.processor.issue(&ready.applied)?;
    let consumed = ready.processor.consume(capability)?;
    ready.runtime.invalidate();
    assert_eq!(
        consumed.validate_at_for_test(ClockReading::new(
            Timestamp::from_unix_nanos(EVALUATED_AT),
            Instant::now(),
        )),
        Err(AuthorityError::Revoked)
    );

    let mut exit_ready = ready_trade(fixed_clock()?, 2)?;
    let capability = exit_ready.processor.issue(&exit_ready.applied)?;
    exit_ready.processor.invalidate_for_exit();
    assert!(matches!(
        exit_ready.processor.consume(capability),
        Err(AuthorityError::Revoked)
    ));
    Ok(())
}

#[test]
fn inclusive_deadline_passes_and_plus_one_nanosecond_fails_queued_issue() -> TestResult {
    let base_mono = Instant::now();
    let to_deadline = u64::try_from(VALID_UNTIL - EVALUATED_AT)?;
    let deadline_mono = base_mono
        .checked_add(Duration::from_nanos(to_deadline))
        .ok_or("deadline instant overflow")?;
    let expired_mono = deadline_mono
        .checked_add(Duration::from_nanos(1))
        .ok_or("expired instant overflow")?;
    let current = ClockReading::new(Timestamp::from_unix_nanos(EVALUATED_AT), base_mono);
    let deadline = ClockReading::new(Timestamp::from_unix_nanos(VALID_UNTIL), deadline_mono);
    let clock = ScriptedTrustedClock::try_new(vec![
        current,
        current,
        current,
        deadline,
        deadline,
        deadline,
        ClockReading::new(Timestamp::from_unix_nanos(VALID_UNTIL + 1), expired_mono),
    ])?;
    let mut ready = ready_trade(clock, 2)?;
    ready.processor.validate_applied_current(&ready.applied)?;
    let _inclusive_capability = ready.processor.issue(&ready.applied)?;
    assert!(matches!(
        ready.processor.issue(&ready.applied),
        Err(AuthorityError::SourceRevoked)
    ));
    Ok(())
}

#[test]
fn monotonic_cap_expiry_fails_while_source_wall_deadline_remains_current() -> TestResult {
    let base_mono = Instant::now();
    let current = ClockReading::new(Timestamp::from_unix_nanos(EVALUATED_AT), base_mono);
    let monotonic_expired = ClockReading::new(
        Timestamp::from_unix_nanos(EVALUATED_AT),
        base_mono
            .checked_add(Duration::from_nanos(6))
            .ok_or("monotonic instant overflow")?,
    );
    let clock =
        ScriptedTrustedClock::try_new(vec![current, current, current, current, monotonic_expired])?;
    let mut source = SourceHarness::try_new("source-a", 1)?;
    let (lease, batch) = source.batch("trade-1", 1, trade()?, non_book_snapshot()?)?;
    let mut registry = GenerationAuthorityRegistry::try_new(1)?;
    let admission = registry.bind_current(&lease, Timestamp::from_unix_nanos(EVALUATED_AT))?;
    let (mut processor, _shard, _runtime) =
        processor_with_lifetime(clock, 1, Duration::from_nanos(5))?;
    let applied = apply_one(&mut processor, &admission, batch)?;
    let authority = applied.authority.ok_or("trade did not produce authority")?;

    assert!(matches!(
        processor.issue(&authority),
        Err(AuthorityError::Expired)
    ));
    Ok(())
}

#[test]
fn nonce_capacity_reclaims_retired_slot_with_bounded_progress() -> TestResult {
    let mut ready = ready_trade(fixed_clock()?, 1)?;
    let first = ready.processor.issue(&ready.applied)?;
    assert!(matches!(
        ready.processor.issue(&ready.applied),
        Err(AuthorityError::NonceCapacityExhausted)
    ));
    let _consumed = ready.processor.consume(first)?;
    let successor = ready.processor.issue(&ready.applied)?;
    let _consumed = ready.processor.consume(successor)?;
    ready.registry.invalidate_all();
    assert_eq!(
        ready.processor.validate_applied_current(&ready.applied),
        Err(AuthorityError::Revoked)
    );
    Ok(())
}
