use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::{FixedQueue, QueueConstructionError, TryRecvError, TrySendError};

#[derive(Debug)]
struct ActiveOperationFixtureReset<'a, T> {
    sender: &'a super::FixedSender<T>,
}

impl<T> Drop for ActiveOperationFixtureReset<'_, T> {
    fn drop(&mut self) {
        self.sender.set_active_operations_for_test(0);
    }
}

#[cfg(feature = "capture-benchmark")]
#[test]
fn standard_reference_transport_is_bounded_fifo_and_never_contended()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::transport::{
        CaptureQueueReceiver, CaptureQueueSender, CaptureQueueTransport, StandardReferenceTransport,
    };

    let (sender, receiver, control, receipt) =
        StandardReferenceTransport::try_new(NonZeroUsize::new(2).ok_or("capacity")?)?;
    assert!(receipt.is_standard_reference_opaque());
    assert_eq!(receipt.logical_capacity(), 2);
    assert_eq!(receipt.retained_queue_bytes(), None);
    let clone = sender.try_clone()?;
    sender.try_send(10)?;
    clone.try_send(20)?;
    assert!(matches!(sender.try_send(30), Err(TrySendError::Full(30))));
    assert_eq!(receiver.try_recv()?, 10);
    assert_eq!(receiver.try_recv()?, 20);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    let left = sender.try_clone()?;
    let right = sender.try_clone()?;
    let (left_result, right_result) = std::thread::scope(|scope| {
        let left = scope.spawn(move || left.try_send(31));
        let right = scope.spawn(move || right.try_send(37));
        (left.join(), right.join())
    });
    assert!(matches!(left_result, Ok(Ok(()))));
    assert!(matches!(right_result, Ok(Ok(()))));
    let mut concurrent = [receiver.try_recv()?, receiver.try_recv()?];
    concurrent.sort_unstable();
    assert_eq!(concurrent, [31, 37]);
    StandardReferenceTransport::close(&control)?;
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    assert!(matches!(sender.try_send(40), Err(TrySendError::Closed(40))));
    Ok(())
}

#[cfg(feature = "capture-benchmark")]
#[test]
fn standard_reference_close_linearizes_after_registered_clone()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::transport::{
        CaptureQueueSender, CaptureQueueTransport, StandardReferenceTransport,
    };

    let (sender, _receiver, control, _receipt) =
        StandardReferenceTransport::try_new::<u8>(NonZeroUsize::MIN)?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let entered_worker = Arc::clone(&entered);
    let release_worker = Arc::clone(&release);
    let worker = std::thread::spawn(move || {
        sender.try_clone_registered_for_test(&entered_worker, &release_worker)
    });
    entered.wait();
    let closing = control.clone();
    let closer = std::thread::spawn(move || StandardReferenceTransport::close(&closing));
    let closing_observed = control.wait_until_closing_for_test(Duration::from_secs(1));
    release.wait();

    let clone_result = worker.join();
    let close_result = closer.join();
    closing_observed?;
    let clone = clone_result.map_err(|_| "clone worker panicked")??;
    assert_eq!(close_result.map_err(|_| "close worker panicked")?, Ok(()));
    assert!(matches!(clone.try_send(9), Err(TrySendError::Closed(9))));
    assert!(matches!(
        clone.try_clone(),
        Err(super::TryCloneError::Closed)
    ));
    Ok(())
}

#[cfg(feature = "capture-benchmark")]
#[test]
fn standard_reference_receiver_drop_linearizes_after_registered_send()
-> Result<(), Box<dyn std::error::Error>> {
    use super::super::transport::{
        CaptureQueueSender, CaptureQueueTransport, StandardReferenceTransport,
    };

    let (sender, receiver, control, _receipt) =
        StandardReferenceTransport::try_new(NonZeroUsize::MIN)?;
    let later = sender.try_clone()?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let entered_worker = Arc::clone(&entered);
    let release_worker = Arc::clone(&release);
    let worker = std::thread::spawn(move || {
        sender.try_send_registered_for_test(11_u8, &entered_worker, &release_worker)
    });
    entered.wait();
    let dropper = std::thread::spawn(move || drop(receiver));
    let closing_observed = control.wait_until_closing_for_test(Duration::from_secs(1));
    release.wait();

    let send_result = worker.join();
    let drop_result = dropper.join();
    closing_observed?;
    assert_eq!(send_result.map_err(|_| "send worker panicked")?, Ok(()));
    drop_result.map_err(|_| "receiver drop worker panicked")?;
    assert!(matches!(later.try_send(12), Err(TrySendError::Closed(12))));
    Ok(())
}

#[test]
fn fixed_queue_is_exact_capacity_fifo_and_delete_on_receive()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, receipt) =
        FixedQueue::try_new(NonZeroUsize::new(2).ok_or("capacity")?)?;
    assert_eq!(receipt.logical_capacity(), 2);
    assert!(receipt.observed_slot_capacity() >= 2);
    sender.try_send(10)?;
    sender.try_send(20)?;
    assert_eq!(sender.try_send(30), Err(TrySendError::Full(30)));
    assert_eq!(receiver.try_recv()?, 10);
    assert_eq!(receiver.try_recv()?, 20);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    Ok(())
}

#[test]
fn capacity_one_distinguishes_a_published_item_from_a_reusable_slot()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;

    sender.try_send(41_u8)?;
    assert_eq!(sender.try_send(43_u8), Err(TrySendError::Full(43)));
    assert_eq!(receiver.try_recv(), Ok(41));
    sender.try_send(47_u8)?;
    assert_eq!(receiver.try_recv(), Ok(47));
    Ok(())
}

#[test]
fn sender_clone_and_last_drop_close_the_receiver() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) = FixedQueue::<u8>::try_new(NonZeroUsize::MIN)?;
    let second = sender.try_clone()?;
    assert_eq!(sender.sender_count()?, 2);
    drop(sender);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    drop(second);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    Ok(())
}

#[test]
fn receiver_drop_rejects_future_sends_and_releases_items() -> Result<(), Box<dyn std::error::Error>>
{
    let (sender, receiver, _control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;
    sender.try_send(Arc::new(()))?;
    drop(receiver);
    let value = Arc::new(());
    assert!(matches!(
        sender.try_send(Arc::clone(&value)),
        Err(TrySendError::Closed(_))
    ));
    assert_eq!(Arc::strong_count(&value), 1);
    Ok(())
}

#[test]
#[cfg(not(loom))]
fn receiver_drop_releases_the_registered_thread_from_fixed_queue_storage()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, control, receipt) = FixedQueue::<u8>::try_new(NonZeroUsize::MIN)?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let receiver_worker = {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        std::thread::spawn(move || {
            receiver.recv_timeout_with_registration_paused_for_test(
                Duration::from_secs(1),
                &entered,
                &release,
            )
        })
    };
    entered.wait();
    release.wait();
    drop(sender);
    assert_eq!(
        receiver_worker
            .join()
            .map_err(|_panic| "receiver worker panicked")?,
        Err(super::RecvTimeoutError::Closed)
    );

    assert!(receipt.retained_queue_bytes() > 0);
    assert_eq!(Arc::strong_count(&control.core), 1);
    assert!(
        control
            .core
            .receiver_thread
            .lock()
            .map_err(|_poisoned| "receiver thread registry poisoned")?
            .is_none(),
        "terminal receiver drop retained an uncharged thread handle"
    );
    Ok(())
}

#[test]
fn timeout_and_forced_slot_lock_invariant_are_distinct_fail_closed_results()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;
    assert!(matches!(
        receiver.recv_timeout(Duration::from_millis(1)),
        Err(error) if error.is_timeout()
    ));
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let held_sender = sender.try_clone()?;
    let entered_thread = Arc::clone(&entered);
    let release_thread = Arc::clone(&release);
    let thread = std::thread::spawn(move || {
        held_sender.hold_state_for_test(&entered_thread, &release_thread);
    });
    entered.wait();
    assert_eq!(sender.try_send(7), Err(TrySendError::Invariant(7)));
    release.wait();
    thread.join().map_err(|_| "queue fixture panicked")?;
    Ok(())
}

#[test]
fn active_operation_counter_overflow_is_typed_as_queue_invariant()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, _receiver, _control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;
    sender.set_active_operations_for_test(sender.maximum_active_operations_for_test());
    let reset = ActiveOperationFixtureReset { sender: &sender };

    let result = sender.try_send(13_u8);
    drop(reset);
    assert_eq!(result, Err(TrySendError::Invariant(13)));
    Ok(())
}

#[test]
fn ordinary_consumer_slot_overlap_never_refuses_capacity_permitted_send()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) =
        FixedQueue::try_new(NonZeroUsize::new(2).ok_or("capacity")?)?;
    sender.try_send(1_u8)?;

    let result = receiver.with_next_slot_locked_for_test(|| sender.try_send(2_u8))?;

    assert_eq!(result, Ok(()));
    assert_eq!(receiver.try_recv(), Ok(1));
    assert_eq!(receiver.try_recv(), Ok(2));
    Ok(())
}

#[test]
#[cfg(not(loom))]
fn waiter_registration_try_lock_miss_is_closed_by_pre_park_recheck()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let entered_worker = Arc::clone(&entered);
    let release_worker = Arc::clone(&release);
    let waiter = std::thread::spawn(move || {
        receiver.recv_timeout_with_registration_paused_for_test(
            Duration::from_secs(1),
            &entered_worker,
            &release_worker,
        )
    });

    entered.wait();
    sender.try_send(17_u8)?;
    release.wait();

    assert_eq!(waiter.join().map_err(|_| "waiter panicked")?, Ok(17));
    Ok(())
}

#[test]
#[cfg(not(loom))]
fn receiver_pause_request_cannot_be_lost_during_waiter_registration()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, control, _receipt) = FixedQueue::try_new(NonZeroUsize::MIN)?;
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let entered_worker = Arc::clone(&entered);
    let release_worker = Arc::clone(&release);
    let waiter = std::thread::spawn(move || {
        receiver.recv_timeout_with_registration_paused_for_test(
            Duration::from_secs(1),
            &entered_worker,
            &release_worker,
        )
    });

    entered.wait();
    let request_probe = control.clone();
    let requester = std::thread::spawn(move || {
        control.with_receiver_paused_for_test(Duration::from_millis(250), || sender.try_send(23_u8))
    });
    let request_deadline = std::time::Instant::now()
        .checked_add(Duration::from_secs(1))
        .ok_or("request deadline overflow")?;
    while !request_probe
        .core
        .receiver_test_coordination
        .requested_hint
        .load(std::sync::atomic::Ordering::Acquire)
    {
        if std::time::Instant::now() >= request_deadline {
            return Err("receiver pause request was not published".into());
        }
        std::thread::yield_now();
    }
    release.wait();

    assert_eq!(
        requester.join().map_err(|_| "requester panicked")?,
        Ok(Ok(()))
    );
    assert_eq!(waiter.join().map_err(|_| "waiter panicked")?, Ok(23));
    Ok(())
}

#[test]
#[cfg(not(loom))]
fn final_failed_operation_wakes_a_receiver_that_reparked_before_terminal_close()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, control, _receipt) = FixedQueue::<u8>::try_new(NonZeroUsize::MIN)?;
    let held_sender = sender.try_clone()?;
    let failing_sender = sender.try_clone()?;
    let lock_entered = Arc::new(Barrier::new(2));
    let lock_release = Arc::new(Barrier::new(2));
    let lock_entered_worker = Arc::clone(&lock_entered);
    let lock_release_worker = Arc::clone(&lock_release);
    let holder = std::thread::spawn(move || {
        held_sender.hold_state_for_test(&lock_entered_worker, &lock_release_worker);
    });
    lock_entered.wait();

    let park_entered = Arc::new(Barrier::new(2));
    let park_release = Arc::new(Barrier::new(2));
    let park_entered_worker = Arc::clone(&park_entered);
    let park_release_worker = Arc::clone(&park_release);
    let waiter = std::thread::spawn(move || {
        receiver.recv_timeout_with_each_park_paused_for_test(
            Duration::from_secs(1),
            &park_entered_worker,
            &park_release_worker,
        )
    });
    park_entered.wait();
    park_release.wait();

    let send_entered = Arc::new(Barrier::new(2));
    let send_release = Arc::new(Barrier::new(2));
    let send_entered_worker = Arc::clone(&send_entered);
    let send_release_worker = Arc::clone(&send_release);
    let send = std::thread::spawn(move || {
        failing_sender.try_send_after_registration_paused_for_test(
            23_u8,
            &send_entered_worker,
            &send_release_worker,
        )
    });
    send_entered.wait();
    control.request_close()?;
    park_entered.wait();

    send_release.wait();
    let send_result = send.join();
    park_release.wait();
    lock_release.wait();
    let waiter_result = waiter.join();
    let holder_result = holder.join();

    assert_eq!(
        send_result.map_err(|_| "send worker panicked")?,
        Err(TrySendError::Invariant(23))
    );
    assert_eq!(
        waiter_result.map_err(|_| "receiver worker panicked")?,
        Err(super::RecvTimeoutError::Closed)
    );
    holder_result.map_err(|_| "queue fixture panicked")?;
    Ok(())
}

#[test]
fn control_close_wakes_receiver_and_preserves_queued_drain()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, control, _receipt) =
        FixedQueue::try_new(NonZeroUsize::new(2).ok_or("capacity")?)?;
    sender.try_send(11)?;
    control.close()?;
    assert_eq!(receiver.try_recv()?, 11);
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    assert!(matches!(sender.try_send(12), Err(TrySendError::Closed(12))));
    Ok(())
}

#[test]
fn multi_producer_wraparound_preserves_every_item_without_invariant_refusal()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 2_000;
    let capacity = NonZeroUsize::new(17).ok_or("capacity")?;
    let (sender, receiver, _control, _receipt) = FixedQueue::try_new(capacity)?;
    let mut producers = Vec::new();
    producers.try_reserve_exact(PRODUCERS)?;
    for producer in 0..PRODUCERS {
        producers.push((producer, sender.try_clone()?));
    }
    drop(sender);
    let expected = PRODUCERS
        .checked_mul(ITEMS_PER_PRODUCER)
        .ok_or("item total overflow")?;

    let (received, results) = std::thread::scope(|scope| {
        let consumer = scope.spawn(move || {
            let mut values = Vec::new();
            loop {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(value) => values.push(value),
                    Err(super::RecvTimeoutError::Closed) => return Ok(values),
                    Err(error) => return Err(error),
                }
            }
        });
        let workers = producers
            .into_iter()
            .map(|(producer, sender)| {
                scope.spawn(move || {
                    for ordinal in 0..ITEMS_PER_PRODUCER {
                        let mut value = producer * ITEMS_PER_PRODUCER + ordinal;
                        loop {
                            match sender.try_send(value) {
                                Ok(()) => break,
                                Err(TrySendError::Full(returned)) => {
                                    value = returned;
                                    std::thread::yield_now();
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    Ok(())
                })
            })
            .map(|worker| worker.join())
            .collect::<Vec<_>>();
        (consumer.join(), workers)
    });
    for result in results {
        result.map_err(|_| "producer panicked")??;
    }
    let mut received = received.map_err(|_| "consumer panicked")??;
    received.sort_unstable();
    assert_eq!(received, (0..expected).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn non_power_of_two_ring_crosses_position_modulus_without_aliasing()
-> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver, _control, _receipt) =
        FixedQueue::try_new(NonZeroUsize::new(3).ok_or("capacity")?)?;
    sender.seed_empty_near_position_wrap_for_test()?;

    sender.try_send(1_u8)?;
    sender.try_send(2_u8)?;
    assert_eq!(receiver.try_recv(), Ok(1));
    sender.try_send(3_u8)?;
    sender.try_send(4_u8)?;
    assert_eq!(sender.try_send(5_u8), Err(TrySendError::Full(5)));
    assert_eq!(receiver.try_recv(), Ok(2));
    assert_eq!(receiver.try_recv(), Ok(3));
    assert_eq!(receiver.try_recv(), Ok(4));
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    Ok(())
}

#[test]
fn impossible_slot_allocation_is_recoverable() {
    let capacity = NonZeroUsize::new(usize::MAX).unwrap_or(NonZeroUsize::MIN);
    assert!(matches!(
        FixedQueue::<u8>::try_new(capacity),
        Err(QueueConstructionError::AllocationFailed)
    ));
}
