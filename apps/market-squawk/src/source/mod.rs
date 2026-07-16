pub mod coinbase;
pub mod mock;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

use crate::{domain::MarketEvent, journal::JournalSink};

#[async_trait]
pub trait MarketSource: Send {
    async fn run(
        self: Box<Self>,
        journal: JournalSink,
        events: mpsc::Sender<MarketEvent>,
        cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<()>;
}
