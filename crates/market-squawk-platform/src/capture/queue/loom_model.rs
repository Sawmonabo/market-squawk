use std::num::NonZeroUsize;
use std::time::Duration;

use loom::sync::Arc;
use loom::thread;

use super::{
    FixedQueue, OperationLifecycle, OperationRegistrationError, TryRecvError, TrySendError,
};

#[test]
fn shared_operation_lifecycle_registration_and_close_are_one_modification_order() {
    loom::model(|| {
        let lifecycle = Arc::new(OperationLifecycle::new());
        let registering = Arc::clone(&lifecycle);
        let closing = Arc::clone(&lifecycle);

        let operation = thread::spawn(move || match registering.begin() {
            Ok(()) => {
                thread::yield_now();
                assert!(registering.finish().is_ok());
                Ok(())
            }
            Err(error) => Err(error),
        });
        let close = thread::spawn(move || closing.close_registration());

        assert!(close.join().is_ok());
        assert!(matches!(
            operation.join(),
            Ok(Ok(())) | Ok(Err(OperationRegistrationError::Closed))
        ));
        assert_eq!(lifecycle.active_operations(), 0);
        assert!(lifecycle.is_terminally_closed());
    });
}

#[test]
fn concurrent_last_sender_drop_closes_receiver() {
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
        let Ok((first, receiver, _control, _receipt)) = queue else {
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
        let registration_control = control.clone();
        let drain_control = control.clone();

        let send = thread::spawn(move || racing_sender.try_send(7_u8));
        let close_registration = thread::spawn(move || registration_control.core.fail_closed());

        assert!(matches!(
            send.join(),
            Ok(Ok(())) | Ok(Err(TrySendError::Closed(7)))
        ));
        assert!(close_registration.join().is_ok());
        assert_eq!(control.close(), Ok(()));
        assert_eq!(drain_control.close_and_drain(), Ok(()));
        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    });
}

#[test]
fn receiver_drop_linearizes_with_registered_send() {
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
        let Ok((sender, receiver, _control, _receipt)) = queue else {
            return;
        };
        let later = sender.try_clone();
        assert!(later.is_ok());
        let Ok(later) = later else {
            return;
        };

        let send = thread::spawn(move || sender.try_send(17_u8));
        let drop_receiver = thread::spawn(move || drop(receiver));

        let send_result = send.join();
        assert!(
            matches!(send_result, Ok(Ok(())) | Ok(Err(TrySendError::Closed(17)))),
            "unexpected send result: {send_result:?}"
        );
        assert!(drop_receiver.join().is_ok());
        assert_eq!(later.try_send(19_u8), Err(TrySendError::Closed(19)));
    });
}
