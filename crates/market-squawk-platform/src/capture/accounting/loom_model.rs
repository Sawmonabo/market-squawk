use std::sync::atomic::Ordering;
use std::time::Duration;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize};
use loom::thread;

use super::{
    CaptureAccountingStatus, CaptureSnapshotRead, checked_transition_enter,
    checked_transition_leave,
};

#[derive(Debug)]
struct TransitionProtocol {
    active: AtomicUsize,
    completed_epoch: AtomicU64,
    status: AtomicU8,
}

impl TransitionProtocol {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            completed_epoch: AtomicU64::new(0),
            status: AtomicU8::new(CaptureAccountingStatus::Healthy as u8),
        }
    }

    fn enter(protocol: &Arc<Self>) -> Option<TransitionGuard> {
        if protocol.status.load(Ordering::SeqCst) != CaptureAccountingStatus::Healthy as u8 {
            return None;
        }
        if protocol
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, checked_transition_enter)
            .is_err()
        {
            protocol.publish_invariant();
            return None;
        }
        if protocol.status.load(Ordering::SeqCst) != CaptureAccountingStatus::Healthy as u8 {
            protocol.leave();
            return None;
        }
        Some(TransitionGuard {
            protocol: Arc::clone(protocol),
            completed: false,
        })
    }

    fn publish_invariant(&self) {
        let _first = self.status.compare_exchange(
            CaptureAccountingStatus::Healthy as u8,
            CaptureAccountingStatus::InvariantViolated as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    fn leave(&self) {
        if self
            .active
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, checked_transition_leave)
            .is_err()
        {
            self.publish_invariant();
        }
    }
}

#[derive(Debug)]
struct TransitionGuard {
    protocol: Arc<TransitionProtocol>,
    completed: bool,
}

impl TransitionGuard {
    fn finish(mut self) {
        let _previous = self.protocol.completed_epoch.fetch_add(1, Ordering::SeqCst);
        self.protocol.leave();
        self.completed = true;
    }
}

impl Drop for TransitionGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.protocol.publish_invariant();
            self.protocol.leave();
        }
    }
}

#[test]
fn live_transition_abandonment_and_checked_drop_fallback() {
    let mut model = loom::model::Builder::new();
    model.max_threads = 3;
    model.max_branches = 1_000;
    model.max_permutations = Some(25_000);
    model.max_duration = Some(Duration::from_secs(20));
    model.preemption_bound = Some(2);
    model.checkpoint_file = None;
    model.checkpoint_interval = 20_000;
    model.expect_explicit_explore = false;
    model.location = false;
    model.log = false;
    model.check(|| {
        let protocol = Arc::new(TransitionProtocol::new());
        let abandoned_protocol = Arc::clone(&protocol);
        let abandoned = thread::spawn(move || {
            if let Some(guard) = TransitionProtocol::enter(&abandoned_protocol) {
                drop(guard);
            }
        });
        let completed_protocol = Arc::clone(&protocol);
        let completed = thread::spawn(move || {
            if let Some(guard) = TransitionProtocol::enter(&completed_protocol) {
                guard.finish();
            }
        });

        assert!(abandoned.join().is_ok());
        assert!(completed.join().is_ok());
        assert_eq!(protocol.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            protocol.status.load(Ordering::SeqCst),
            CaptureAccountingStatus::InvariantViolated as u8
        );

        // The exact checked-leave function used by production refuses underflow instead of
        // wrapping the live-transition count.
        assert_eq!(checked_transition_leave(0), None);
    });
}

#[derive(Debug)]
struct SnapshotProtocol {
    status: AtomicU8,
    active: AtomicUsize,
    epoch: AtomicU64,
    fixed: AtomicUsize,
    resident: AtomicUsize,
    record: AtomicUsize,
    total: AtomicUsize,
}

impl SnapshotProtocol {
    fn new() -> Self {
        Self {
            status: AtomicU8::new(CaptureAccountingStatus::Healthy as u8),
            active: AtomicUsize::new(0),
            epoch: AtomicU64::new(0),
            fixed: AtomicUsize::new(1),
            resident: AtomicUsize::new(0),
            record: AtomicUsize::new(0),
            total: AtomicUsize::new(1),
        }
    }

    fn read(&self) -> CaptureSnapshotRead {
        CaptureSnapshotRead {
            status_before: self.status.load(Ordering::SeqCst),
            epoch_before: self.epoch.load(Ordering::SeqCst),
            active_before: self.active.load(Ordering::SeqCst),
            fixed: self.fixed.load(Ordering::SeqCst),
            resident: self.resident.load(Ordering::SeqCst),
            record: self.record.load(Ordering::SeqCst),
            total: self.total.load(Ordering::SeqCst),
            active_after_components: self.active.load(Ordering::SeqCst),
            epoch_after: self.epoch.load(Ordering::SeqCst),
            active_final: self.active.load(Ordering::SeqCst),
            status_after: self.status.load(Ordering::SeqCst),
        }
    }

    fn reserve_and_release_record(&self) {
        let entered =
            self.active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, checked_transition_enter);
        assert!(entered.is_ok());
        self.total.store(2, Ordering::SeqCst);
        self.record.store(1, Ordering::SeqCst);
        self.record.store(0, Ordering::SeqCst);
        self.total.store(1, Ordering::SeqCst);
        let _previous_epoch = self.epoch.fetch_add(1, Ordering::SeqCst);
        let left =
            self.active
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, checked_transition_leave);
        assert!(left.is_ok());
    }
}

#[test]
fn coherent_snapshot_rejects_aba() {
    let mut model = loom::model::Builder::new();
    model.max_threads = 3;
    model.max_branches = 1_000;
    model.max_permutations = Some(50_000);
    model.max_duration = Some(Duration::from_secs(30));
    model.preemption_bound = Some(2);
    model.checkpoint_file = None;
    model.checkpoint_interval = 20_000;
    model.expect_explicit_explore = false;
    model.location = false;
    model.log = false;
    model.check(|| {
        let protocol = Arc::new(SnapshotProtocol::new());
        let writer_protocol = Arc::clone(&protocol);
        let writer = thread::spawn(move || writer_protocol.reserve_and_release_record());
        let reader_protocol = Arc::clone(&protocol);
        let reader = thread::spawn(move || reader_protocol.read());

        assert!(writer.join().is_ok());
        let read = reader.join();
        assert!(read.is_ok());
        if let Ok(read) = read
            && read.is_coherent(2)
        {
            assert!(read.is_quiescent());
            assert!(read.reconciles(2));
            assert_eq!(read.epoch_before, read.epoch_after);
        }
    });
}
