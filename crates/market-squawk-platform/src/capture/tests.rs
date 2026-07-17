use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use bytes::Bytes;
use market_squawk_domain::{
    CaptureAuthorityIdentity, ConnectionGeneration, MetadataRevision, RawCaptureFrameView,
    SourceId, SourceIdentifier, Timestamp, checked_arc_str_allocation_bytes,
    checked_arc_value_allocation_bytes,
};

use super::{
    CaptureCompletionAccounting, DiagnosticCaptureBundle, DiagnosticCaptureFrame,
    RecordReservationQuote, saturating_atomic_increment,
};

#[test]
fn standard_record_reservation_names_every_zero_copy_allocation_term()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = CaptureAuthorityIdentity::new(
        SourceId::try_from("reservation-source")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        SourceIdentifier::try_from("session-1")?,
        ConnectionGeneration::new(1)?,
    );
    let frame = DiagnosticCaptureFrame::try_new(
        identity,
        NonZeroU64::MIN,
        Timestamp::from_unix_nanos(1),
        Bytes::from_static(b"payload"),
    )?;
    let complete_frame = frame
        .checked_retained_footprint()?
        .checked_complete_bytes()?;
    let quote =
        RecordReservationQuote::try_for_frame::<DiagnosticCaptureBundle>(&frame, complete_frame)?;
    let source = checked_arc_str_allocation_bytes(frame.source_id().as_str().len())?;
    assert_eq!(quote.complete_frame, complete_frame);
    let queued_frame_allocation_overhead =
        checked_arc_value_allocation_bytes::<DiagnosticCaptureFrame>(0)?
            - std::mem::size_of::<DiagnosticCaptureFrame>();
    assert_eq!(
        quote.queued_frame_allocation_overhead,
        queued_frame_allocation_overhead
    );
    assert_eq!(quote.conversion_source_allocation, source);
    assert_eq!(
        quote.checked_total()?,
        complete_frame + queued_frame_allocation_overhead + source
    );
    Ok(())
}

#[test]
fn standard_record_reservation_overflow_is_typed() {
    let quote = RecordReservationQuote {
        complete_frame: usize::MAX,
        queued_frame_allocation_overhead: 1,
        conversion_source_allocation: 1,
    };
    assert!(quote.checked_total().is_err());
}

#[test]
fn diagnostic_counter_increment_saturates_at_the_numeric_limit() {
    let counter = AtomicU64::new(u64::MAX);
    saturating_atomic_increment(&counter);
    assert_eq!(counter.load(Ordering::Acquire), u64::MAX);
}

#[test]
fn revocation_linearizes_before_accounting_commit_after_sink_success()
-> Result<(), Box<dyn std::error::Error>> {
    let accounting = Arc::new(std::sync::Mutex::new(CaptureCompletionAccounting::default()));
    let sink_succeeded = Arc::new(Barrier::new(2));
    let accounting_allowed = Arc::new(Barrier::new(2));
    let worker_accounting = Arc::clone(&accounting);
    let worker_sink_succeeded = Arc::clone(&sink_succeeded);
    let worker_accounting_allowed = Arc::clone(&accounting_allowed);
    let worker = std::thread::spawn(move || {
        worker_sink_succeeded.wait();
        worker_accounting_allowed.wait();
        let mut accounting = match worker_accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => poisoned.into_inner(),
        };
        accounting.record_completed_append()
    });

    sink_succeeded.wait();
    let at_revocation = {
        let mut accounting = match accounting.lock() {
            Ok(accounting) => accounting,
            Err(poisoned) => poisoned.into_inner(),
        };
        accounting.revoke()
    };
    accounting_allowed.wait();
    let committed = worker
        .join()
        .map_err(|_panic| "completion-accounting worker panicked")?;
    let final_snapshot = match accounting.lock() {
        Ok(accounting) => accounting.snapshot(),
        Err(poisoned) => poisoned.into_inner().snapshot(),
    };

    assert_eq!(committed, Some(1));
    assert_eq!(at_revocation.records_written_at_revocation, 0);
    assert_eq!(final_snapshot.records_written, 1);
    assert_eq!(final_snapshot.late_records_written, 1);
    Ok(())
}
