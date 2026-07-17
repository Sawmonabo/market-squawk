use std::sync::atomic::Ordering;
use std::time::Duration;

use loom::sync::Arc;
use loom::sync::atomic::AtomicUsize;
use loom::thread;

#[derive(Debug)]
struct RetainedOwner {
    final_releases: Arc<AtomicUsize>,
}

impl Drop for RetainedOwner {
    fn drop(&mut self) {
        let previous = self.final_releases.fetch_add(1, Ordering::SeqCst);
        assert_eq!(previous, 0);
    }
}

#[derive(Debug)]
struct WriterHandleOwnership {
    fixed_storage: Option<Arc<RetainedOwner>>,
    destination_fence: Option<Arc<RetainedOwner>>,
}

impl WriterHandleOwnership {
    fn shutdown(mut self) -> PendingWriterOwnership {
        PendingWriterOwnership {
            fixed_storage: self.fixed_storage.take(),
            destination_fence: self.destination_fence.take(),
        }
    }
}

#[derive(Debug)]
struct PendingWriterOwnership {
    fixed_storage: Option<Arc<RetainedOwner>>,
    destination_fence: Option<Arc<RetainedOwner>>,
}

impl PendingWriterOwnership {
    fn reap(mut self) {
        self.destination_fence.take();
        self.fixed_storage.take();
    }
}

#[test]
fn fixed_storage_transfer_and_final_drop() {
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
        let fixed_releases = Arc::new(AtomicUsize::new(0));
        let fence_releases = Arc::new(AtomicUsize::new(0));
        let fixed = Arc::new(RetainedOwner {
            final_releases: Arc::clone(&fixed_releases),
        });
        let fence = Arc::new(RetainedOwner {
            final_releases: Arc::clone(&fence_releases),
        });

        // The production start packet gives one share to the worker and one to the lifecycle
        // owner. Shutdown moves the lifecycle shares into Pending without cloning them.
        let worker_fixed = Arc::clone(&fixed);
        let worker_fence = Arc::clone(&fence);
        let handle = WriterHandleOwnership {
            fixed_storage: Some(fixed),
            destination_fence: Some(fence),
        };

        let worker = thread::spawn(move || {
            drop(worker_fence);
            drop(worker_fixed);
        });
        let lifecycle = thread::spawn(move || handle.shutdown().reap());

        assert!(worker.join().is_ok());
        assert!(lifecycle.join().is_ok());
        assert_eq!(fixed_releases.load(Ordering::SeqCst), 1);
        assert_eq!(fence_releases.load(Ordering::SeqCst), 1);
    });
}
