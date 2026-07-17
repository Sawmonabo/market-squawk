use std::io::{Cursor, Read};
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_platform::{
    JournalError, JournalReader, JournalReplayAuthority, LocalPaths, RawCaptureRecord,
};
use tempfile::tempdir;
use uuid::Uuid;

fn record() -> Result<RawCaptureRecord, Box<dyn std::error::Error>> {
    Ok(RawCaptureRecord::try_new_live(
        Uuid::from_u128(11),
        Arc::from("fixture"),
        Uuid::from_u128(9),
        Some(42),
        None,
        "2026-07-15T20:00:01Z".parse::<DateTime<Utc>>()?,
        Bytes::from_static(br#"{"fixture":true}"#),
    )?)
}

fn fixture_with_magic(magic: [u8; 4]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("fixture")?;
    let mut writer = paths.open_journal_writer("fixture")?;
    writer.append(&record()?)?;
    writer.flush()?;
    drop(writer);
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.read_to_end(&mut bytes)?;
    bytes[..4].copy_from_slice(&magic);
    Ok(bytes)
}

fn literal_legacy_fixture(magic: [u8; 4]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let payload = br#"{"event_id":"00000000-0000-0000-0000-000000000001","source":"legacy-source","connection_id":"00000000-0000-0000-0000-000000000000","source_sequence":42,"exchange_at":"2026-07-15T20:00:00Z","received_at":"2026-07-15T20:00:01Z","payload":[123,34,111,108,100,34,58,116,114,117,101,125]}"#;
    let mut fixture = Vec::with_capacity(12 + payload.len());
    fixture.extend_from_slice(&magic);
    fixture.extend_from_slice(&u32::try_from(payload.len())?.to_le_bytes());
    fixture.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    fixture.extend_from_slice(payload);
    Ok(fixture)
}

#[test]
fn reads_current_and_legacy_journal_magic() -> Result<(), Box<dyn std::error::Error>> {
    for magic in [*b"MSJ1", *b"MEJ1"] {
        let reader = JournalReader::new(Cursor::new(fixture_with_magic(magic)?));
        assert_eq!(
            reader.replay_authority(),
            JournalReplayAuthority::UnavailableByFormat
        );
        let records = reader.read_all()?;
        assert_eq!(records, vec![record()?]);
    }
    Ok(())
}

#[test]
fn reads_a_literal_pre_migration_raw_envelope_fixture() -> Result<(), Box<dyn std::error::Error>> {
    for magic in [*b"MEJ1", *b"MSJ1"] {
        let records = JournalReader::new(Cursor::new(literal_legacy_fixture(magic)?)).read_all()?;
        let legacy = records.first().ok_or("literal legacy fixture was empty")?;

        assert_eq!(legacy.event_id(), Uuid::from_u128(1));
        assert_eq!(legacy.source(), "legacy-source");
        assert!(legacy.connection_id().is_nil());
        assert_eq!(legacy.source_sequence(), Some(42));
        assert_eq!(
            legacy.exchange_at(),
            Some("2026-07-15T20:00:00Z".parse::<DateTime<Utc>>()?)
        );
        assert_eq!(
            legacy.received_at(),
            "2026-07-15T20:00:01Z".parse::<DateTime<Utc>>()?
        );
        assert_eq!(legacy.payload(), br#"{"old":true}"#);
    }
    Ok(())
}

#[test]
fn aggregate_and_record_limits_reject_before_allocating_or_parsing_the_body() {
    let mut truncated_large_frame = b"MSJ1".to_vec();
    truncated_large_frame.extend_from_slice(&(32_u32 * 1024 * 1024).to_le_bytes());
    truncated_large_frame.extend_from_slice(&0_u32.to_le_bytes());

    assert!(matches!(
        JournalReader::new(Cursor::new(truncated_large_frame.clone()))
            .read_all_bounded(0, u64::MAX),
        Err(JournalError::RecordLimitExceeded { limit: 0 })
    ));
    assert!(matches!(
        JournalReader::new(Cursor::new(truncated_large_frame)).read_all_bounded(1, 0),
        Err(JournalError::AggregateLimitExceeded { .. })
    ));
}

#[test]
fn rejects_unknown_magic_crc_corruption_and_collection_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(matches!(
        JournalReader::new(Cursor::new(fixture_with_magic(*b"XXXX")?)).read_all(),
        Err(JournalError::UnsupportedMagic(_))
    ));
    let mut corrupted = fixture_with_magic(*b"MSJ1")?;
    let last = corrupted.last_mut().ok_or("empty fixture")?;
    *last ^= 1;
    assert!(matches!(
        JournalReader::new(Cursor::new(corrupted)).read_all(),
        Err(JournalError::InvalidRecord(_))
    ));
    assert!(matches!(
        JournalReader::new(Cursor::new(fixture_with_magic(*b"MSJ1")?))
            .read_all_bounded(0, u64::MAX),
        Err(JournalError::RecordLimitExceeded { limit: 0 })
    ));
    Ok(())
}

#[test]
fn historically_accepted_nil_identifiers_remain_readable() -> Result<(), Box<dyn std::error::Error>>
{
    let legacy = br#"{"event_id":"00000000-0000-0000-0000-000000000000","source":"fixture","connection_id":"00000000-0000-0000-0000-000000000000","source_sequence":null,"exchange_at":null,"received_at":"2026-07-15T20:00:01Z","payload":[]}"#;
    let mut fixture = Vec::new();
    fixture.extend_from_slice(b"MSJ1");
    fixture.extend_from_slice(&u32::try_from(legacy.len())?.to_le_bytes());
    fixture.extend_from_slice(&crc32fast::hash(legacy).to_le_bytes());
    fixture.extend_from_slice(legacy);

    let records = JournalReader::new(Cursor::new(fixture)).read_all()?;
    let record = records.first().ok_or("legacy fixture was empty")?;
    assert!(record.event_id().is_nil());
    assert!(record.connection_id().is_nil());
    Ok(())
}

#[test]
fn writer_is_current_only_validates_existing_content_and_locks_exclusively()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let current = paths.journal_write_file("capture")?;
    std::fs::write(&current, fixture_with_magic(*b"MEJ1")?)?;

    assert!(matches!(
        paths.open_journal_writer("capture"),
        Err(JournalError::LegacyFormatReadOnly)
    ));
    std::fs::remove_file(&current)?;
    let _first = paths.open_journal_writer("capture")?;
    assert!(matches!(
        paths.open_journal_writer("capture"),
        Err(JournalError::AlreadyLocked { .. })
    ));
    Ok(())
}

#[test]
fn active_writer_does_not_block_readers_of_committed_records()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let current = paths.journal_write_file("capture")?;
    let expected = record()?;

    let mut writer = paths.open_journal_writer("capture")?;
    writer.append(&expected)?;
    writer.flush()?;

    let records = JournalReader::open(current)?.read_all()?;
    assert_eq!(records, vec![expected]);
    Ok(())
}
