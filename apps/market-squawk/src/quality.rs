use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QualityState {
    Initializing,
    Valid,
    Stale,
    GapDetected,
    ChecksumFailed,
    Divergent,
    Quarantined,
}

impl QualityState {
    #[must_use]
    pub const fn tradable(self) -> bool {
        matches!(self, Self::Valid)
    }

    #[must_use]
    const fn requires_snapshot_recovery(self) -> bool {
        matches!(
            self,
            Self::GapDetected | Self::ChecksumFailed | Self::Divergent | Self::Quarantined
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedQuality {
    pub state: QualityState,
    pub reason: Option<String>,
    /// Timestamp of the newest accepted order-book snapshot or delta.
    pub last_book_at: Option<DateTime<Utc>>,
    /// Timestamp of the newest heartbeat. This never refreshes book freshness.
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_sequence: Option<u64>,
    pub gap_count: u64,
}

impl Default for FeedQuality {
    fn default() -> Self {
        Self {
            state: QualityState::Initializing,
            reason: None,
            last_book_at: None,
            last_heartbeat_at: None,
            last_sequence: None,
            gap_count: 0,
        }
    }
}

impl FeedQuality {
    /// Record a heartbeat sequence without treating the heartbeat as a fresh book update.
    pub fn observe_heartbeat(&mut self, at: DateTime<Utc>, sequence: u64) {
        self.last_heartbeat_at = Some(at);

        if let Some(previous) = self.last_sequence
            && sequence <= previous
        {
            self.mark_quarantined(format!(
                "non-monotonic heartbeat sequence: previous={previous}, current={sequence}"
            ));
            return;
        }

        self.last_sequence = Some(sequence);
    }

    /// Record a contiguous sequence for sources that explicitly guarantee contiguous numbering.
    pub fn observe_contiguous(&mut self, at: DateTime<Utc>, sequence: u64) {
        self.last_heartbeat_at = Some(at);
        if let Some(previous) = self.last_sequence
            && sequence != previous.saturating_add(1)
        {
            self.state = QualityState::GapDetected;
            self.reason = Some(format!(
                "sequence gap: expected={}, received={sequence}",
                previous.saturating_add(1)
            ));
            self.gap_count = self.gap_count.saturating_add(1);
            self.last_sequence = Some(sequence);
            return;
        }

        self.last_sequence = Some(sequence);
    }

    /// A fresh source snapshot is the only operation allowed to clear a hard-invalid state.
    pub fn accept_snapshot(&mut self, at: DateTime<Utc>) {
        self.last_book_at = Some(at);
        self.state = QualityState::Valid;
        self.reason = None;
    }

    /// Accept an incremental book update only when the stream does not require resynchronization.
    #[must_use]
    pub fn accept_delta(&mut self, at: DateTime<Utc>) -> bool {
        if self.state.requires_snapshot_recovery() || self.state == QualityState::Initializing {
            return false;
        }

        self.last_book_at = Some(at);
        self.state = QualityState::Valid;
        self.reason = None;
        true
    }

    pub fn mark_quarantined(&mut self, reason: impl Into<String>) {
        self.state = QualityState::Quarantined;
        self.reason = Some(reason.into());
    }

    pub fn refresh_staleness(&mut self, now: DateTime<Utc>, stale_after_ms: i64) {
        if self.state != QualityState::Valid {
            return;
        }
        let last_book_at = match self.last_book_at {
            Some(last_book_at) => last_book_at,
            None => return,
        };

        if now.signed_duration_since(last_book_at).num_milliseconds() > stale_after_ms {
            self.state = QualityState::Stale;
            self.reason = Some(format!(
                "no accepted order-book update for more than {stale_after_ms} ms"
            ));
        }
    }
}
