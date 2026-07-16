use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::RawEnvelope,
    engine::{Engine, EngineSnapshot},
    journal::JournalReader,
    source::coinbase::decode_message,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReplaySummary {
    pub records: u64,
    pub bytes: u64,
    pub first_received_at: Option<DateTime<Utc>>,
    pub last_received_at: Option<DateTime<Utc>>,
    pub records_by_source: BTreeMap<String, u64>,
}

impl ReplaySummary {
    fn observe(&mut self, record: &RawEnvelope) -> Result<()> {
        self.records = self.records.saturating_add(1);
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(record.payload().len())?);
        self.first_received_at.get_or_insert(record.received_at());
        self.last_received_at = Some(record.received_at());
        *self
            .records_by_source
            .entry(record.source().to_owned())
            .or_default() += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub summary: ReplaySummary,
    pub snapshot: EngineSnapshot,
}

pub fn summarize_journal(path: impl AsRef<Path>) -> Result<ReplaySummary> {
    let mut reader = JournalReader::open(path)?;
    let mut summary = ReplaySummary::default();

    while let Some(record) = reader.next_record()? {
        summary.observe(&record)?;
    }

    Ok(summary)
}

pub fn replay_coinbase_journal(
    path: impl AsRef<Path>,
    stale_after_ms: i64,
    paper_bot_enabled: bool,
) -> Result<ReplayResult> {
    let mut reader = JournalReader::open(path)?;
    let mut summary = ReplaySummary::default();
    let mut engine = Engine::new(stale_after_ms, paper_bot_enabled);

    while let Some(record) = reader.next_record()? {
        summary.observe(&record)?;
        if record.source() != "coinbase-exchange" {
            continue;
        }

        let value = serde_json::from_slice(record.payload())
            .context("journal contains invalid Coinbase JSON")?;
        if let Some(event) = decode_message(&value, record.received_at())? {
            engine.handle(event);
        }
    }

    Ok(ReplayResult {
        summary,
        snapshot: engine.snapshot(),
    })
}
