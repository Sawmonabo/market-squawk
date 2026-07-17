use std::num::NonZeroUsize;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::{FixedQueue, QueueConstructionError, TryRecvError, TrySendError};

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
fn timeout_and_contention_are_distinct_fail_closed_results()
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
    assert_eq!(sender.try_send(7), Err(TrySendError::Contended(7)));
    release.wait();
    thread.join().map_err(|_| "queue fixture panicked")?;
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
fn impossible_slot_allocation_is_recoverable() {
    let capacity = NonZeroUsize::new(usize::MAX).unwrap_or(NonZeroUsize::MIN);
    assert!(matches!(
        FixedQueue::<u8>::try_new(capacity),
        Err(QueueConstructionError::AllocationFailed)
    ));
}
