use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use loom::thread;

use super::{FixedQueue, TryCloneError, TryRecvError, TrySendError};

#[test]
fn clone_drop_overflow_poison_and_last_close() {
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
        let overflow_queue = FixedQueue::try_new(NonZeroUsize::MIN);
        assert!(overflow_queue.is_ok());
        let Ok((overflow_sender, overflow_receiver, _control, _receipt)) = overflow_queue else {
            return;
        };
        assert_eq!(overflow_sender.try_send(1_u8), Ok(()));
        assert_eq!(overflow_sender.try_send(2_u8), Err(TrySendError::Full(2)));
        assert_eq!(overflow_receiver.try_recv(), Ok(1));

        let poison_queue = FixedQueue::try_new(NonZeroUsize::MIN);
        assert!(poison_queue.is_ok());
        let Ok((poison_sender, _receiver, _control, _receipt)) = poison_queue else {
            return;
        };
        {
            let state = poison_sender.core.state.lock();
            assert!(state.is_ok());
            let Ok(mut state) = state else {
                return;
            };
            state.slots[0] = Some(3_u8);
        }
        assert_eq!(poison_sender.try_send(4_u8), Err(TrySendError::Poisoned(4)));
        assert!(poison_sender.core.closed_hint.load(Ordering::Acquire));

        let count_queue = FixedQueue::<u8>::try_new(NonZeroUsize::MIN);
        assert!(count_queue.is_ok());
        let Ok((count_sender, _receiver, _control, _receipt)) = count_queue else {
            return;
        };
        let count_state = count_sender.core.state.lock();
        assert!(count_state.is_ok());
        let Ok(mut count_state) = count_state else {
            return;
        };
        count_state.sender_count = usize::MAX;
        drop(count_state);
        assert!(matches!(
            count_sender.try_clone(),
            Err(TryCloneError::CountOverflow)
        ));

        let close_queue = FixedQueue::<u8>::try_new(NonZeroUsize::MIN);
        assert!(close_queue.is_ok());
        let Ok((first, receiver, _control, _receipt)) = close_queue else {
            return;
        };
        let second = first.try_clone();
        assert!(second.is_ok());
        let Ok(second) = second else {
            return;
        };
        let first_drop = thread::spawn(move || drop(first));
        let second_drop = thread::spawn(move || drop(second));
        assert!(first_drop.join().is_ok());
        assert!(second_drop.join().is_ok());
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    });
}

#[test]
fn shutdown_before_wait() {
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
        let queue = FixedQueue::<u8>::try_new(NonZeroUsize::MIN);
        assert!(queue.is_ok());
        let Ok((_sender, receiver, control, _receipt)) = queue else {
            return;
        };
        let waiter = thread::spawn(move || receiver.recv_timeout(Duration::from_secs(1)));
        let closer = thread::spawn(move || control.close());
        assert!(matches!(closer.join(), Ok(Ok(()))));
        assert!(matches!(
            waiter.join(),
            Ok(Err(super::RecvTimeoutError::Closed))
        ));
    });
}

#[test]
fn send_close_and_drain_races() {
    let mut model = loom::model::Builder::new();
    model.max_threads = 4;
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
        let queue = FixedQueue::<u8>::try_new(NonZeroUsize::MIN);
        assert!(queue.is_ok());
        let Ok((sender, receiver, control, _receipt)) = queue else {
            return;
        };
        let racing_sender = sender.try_clone();
        assert!(racing_sender.is_ok());
        let Ok(racing_sender) = racing_sender else {
            return;
        };
        let drain_control = control.clone();

        let send = thread::spawn(move || racing_sender.try_send(7_u8));
        let close = thread::spawn(move || control.close());
        let drain = thread::spawn(move || drain_control.close_and_drain());

        assert!(matches!(
            send.join(),
            Ok(Ok(())) | Ok(Err(TrySendError::Closed(7))) | Ok(Err(TrySendError::Contended(7)))
        ));
        assert!(matches!(close.join(), Ok(Ok(()))));
        assert!(matches!(drain.join(), Ok(Ok(()))));
        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    });
}
