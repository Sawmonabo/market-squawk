//! Fair bounded selection across control, market, snapshot, and cancellation work.

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FairTurn {
    Control,
    Market,
    Snapshot,
}

impl FairTurn {
    pub(super) const fn next(self) -> Self {
        match self {
            Self::Control => Self::Market,
            Self::Market => Self::Snapshot,
            Self::Snapshot => Self::Control,
        }
    }
}

#[derive(Debug)]
pub(super) enum FairEvent<R, M> {
    Cancelled,
    Control(Option<R>),
    Market(Option<M>),
    SnapshotDue,
    SnapshotPublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotSchedule {
    Due,
    Publish,
}

pub(super) struct FairSources<'a, R, M> {
    pub(super) cancellation: &'a CancellationToken,
    pub(super) controls: &'a mut mpsc::Receiver<R>,
    pub(super) mailbox: &'a mut mpsc::Receiver<M>,
    pub(super) controls_open: bool,
    pub(super) mailbox_open: bool,
    pub(super) interval: &'a mut tokio::time::Interval,
}

pub(super) async fn select_fair_event<R, M>(
    turn: FairTurn,
    snapshot_pending: bool,
    sources: FairSources<'_, R, M>,
) -> FairEvent<R, M> {
    async fn snapshot_event(
        snapshot_pending: bool,
        interval: &mut tokio::time::Interval,
    ) -> SnapshotSchedule {
        if snapshot_pending {
            SnapshotSchedule::Publish
        } else {
            interval.tick().await;
            SnapshotSchedule::Due
        }
    }

    match turn {
        FairTurn::Control => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                command = sources.controls.recv(), if sources.controls_open => {
                    FairEvent::Control(command)
                }
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
            }
        }
        FairTurn::Market => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
                command = sources.controls.recv(), if sources.controls_open => {
                    FairEvent::Control(command)
                }
            }
        }
        FairTurn::Snapshot => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
                command = sources.controls.recv(), if sources.controls_open => {
                    FairEvent::Control(command)
                }
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FairEvent, FairSources, FairTurn, select_fair_event};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, advance};
    use tokio_util::sync::CancellationToken;

    #[tokio::test(start_paused = true)]
    async fn perpetually_ready_snapshot_work_services_both_queues_within_one_rotation() {
        let (registrations, mut registration_rx) = mpsc::channel(1);
        let (market, mut market_rx) = mpsc::channel(1);
        assert!(registrations.send(11_u8).await.is_ok());
        assert!(market.send(22_u8).await.is_ok());
        let cancellation = CancellationToken::new();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        interval.tick().await;
        advance(Duration::from_secs(1)).await;

        let mut turn = FairTurn::Snapshot;
        let mut saw_registration = false;
        let mut saw_market = false;
        let mut unexpected = None;
        for _ in 0..3 {
            let event = select_fair_event(
                turn,
                true,
                FairSources {
                    cancellation: &cancellation,
                    controls: &mut registration_rx,
                    mailbox: &mut market_rx,
                    controls_open: true,
                    mailbox_open: true,
                    interval: &mut interval,
                },
            )
            .await;
            turn = turn.next();
            match event {
                FairEvent::SnapshotPublish => {}
                FairEvent::Control(Some(11)) => saw_registration = true,
                FairEvent::Market(Some(22)) => saw_market = true,
                other => unexpected = Some(format!("{other:?}")),
            }
        }
        assert!(unexpected.is_none(), "unexpected event: {unexpected:?}");
        assert!(saw_registration);
        assert!(saw_market);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wins_when_snapshot_and_both_queues_are_ready() {
        let (registrations, mut registration_rx) = mpsc::channel(1);
        let (market, mut market_rx) = mpsc::channel(1);
        assert!(registrations.send(1_u8).await.is_ok());
        assert!(market.send(2_u8).await.is_ok());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        interval.tick().await;
        advance(Duration::from_secs(1)).await;

        assert!(matches!(
            select_fair_event(
                FairTurn::Snapshot,
                true,
                FairSources {
                    cancellation: &cancellation,
                    controls: &mut registration_rx,
                    mailbox: &mut market_rx,
                    controls_open: true,
                    mailbox_open: true,
                    interval: &mut interval,
                },
            )
            .await,
            FairEvent::Cancelled
        ));
    }
}
