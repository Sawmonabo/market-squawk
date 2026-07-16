use chrono::{Duration, Utc};
use market_engine::quality::{FeedQuality, QualityState};

#[test]
fn heartbeat_does_not_make_an_uninitialized_book_tradable() {
    let mut quality = FeedQuality::default();
    let now = Utc::now();

    quality.observe_heartbeat(now, 10);

    assert_eq!(quality.state, QualityState::Initializing);
    assert_eq!(quality.last_book_at, None);
    assert_eq!(quality.last_heartbeat_at, Some(now));
}

#[test]
fn heartbeat_does_not_refresh_book_freshness() {
    let mut quality = FeedQuality::default();
    let book_at = Utc::now();
    let heartbeat_at = book_at + Duration::seconds(10);

    quality.accept_snapshot(book_at);
    quality.observe_heartbeat(heartbeat_at, 11);
    quality.refresh_staleness(heartbeat_at, 1_000);

    assert_eq!(quality.state, QualityState::Stale);
    assert_eq!(quality.last_book_at, Some(book_at));
    assert_eq!(quality.last_heartbeat_at, Some(heartbeat_at));
}

#[test]
fn hard_invalid_state_requires_a_fresh_snapshot_to_recover() {
    let mut quality = FeedQuality::default();
    let now = Utc::now();

    quality.mark_quarantined("test failure");
    assert!(!quality.accept_delta(now));
    assert_eq!(quality.state, QualityState::Quarantined);

    quality.accept_snapshot(now);
    assert_eq!(quality.state, QualityState::Valid);
}
