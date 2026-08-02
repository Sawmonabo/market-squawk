use std::io::{Cursor, Read};
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_platform::{
    JournalError, JournalReader, JournalReplayAuthority, JournalSinkConstructionError,
    JournalSinkLimits, JournalWriter, LocalPaths, RawCaptureRecord,
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
        Err(JournalSinkConstructionError::Journal(
            JournalError::LegacyFormatReadOnly
        ))
    ));
    std::fs::remove_file(&current)?;
    let _first = paths.open_journal_writer("capture")?;
    assert!(matches!(
        paths.open_journal_writer("capture"),
        Err(JournalSinkConstructionError::Journal(
            JournalError::AlreadyLocked { .. }
        ))
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

#[test]
fn journal_sink_fixed_ledger_accepts_exact_and_refuses_one_under_before_creation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("bounded")?;
    let buffer_capacity = NonZeroUsize::new(4_096).ok_or("invalid journal test buffer")?;
    let generous_limits = JournalSinkLimits::new(
        buffer_capacity,
        NonZeroUsize::new(1024 * 1024).ok_or("invalid generous journal test ceiling")?,
    );
    let probe = paths.open_journal_writer_with_limits("bounded", generous_limits)?;
    let exact_fixed = probe.fixed_retained_bytes();
    assert_eq!(probe.buffer_capacity(), buffer_capacity.get());
    assert_eq!(
        exact_fixed,
        std::mem::size_of::<JournalWriter>() + path.capacity() + probe.buffer_capacity()
    );
    drop(probe);
    std::fs::remove_file(&path)?;

    let exact_limits = JournalSinkLimits::new(
        buffer_capacity,
        NonZeroUsize::new(exact_fixed).ok_or("observed journal fixed storage must be nonzero")?,
    );
    let exact = paths.open_journal_writer_with_limits("bounded", exact_limits)?;
    assert_eq!(exact.fixed_retained_bytes(), exact_fixed);
    assert_eq!(exact.retained_byte_ceiling(), exact_fixed);
    drop(exact);
    std::fs::remove_file(&path)?;

    let one_under = exact_fixed
        .checked_sub(1)
        .and_then(NonZeroUsize::new)
        .ok_or("observed journal fixed storage must exceed one byte")?;
    assert!(matches!(
        paths.open_journal_writer_with_limits(
            "bounded",
            JournalSinkLimits::new(buffer_capacity, one_under)
        ),
        Err(JournalSinkConstructionError::FixedStorageBudgetExceeded {
            required,
            limit
        }) if required == exact_fixed && limit == exact_fixed - 1
    ));
    assert!(!path.exists());
    Ok(())
}

#[test]
fn journal_sink_limit_arithmetic_fails_before_creating_the_endpoint()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("overflow")?;
    assert!(matches!(
        paths.open_journal_writer_with_limits(
            "overflow",
            JournalSinkLimits::new(NonZeroUsize::MAX, NonZeroUsize::MAX)
        ),
        Err(JournalSinkConstructionError::ArithmeticOverflow)
    ));
    assert!(!path.exists());
    Ok(())
}

#[test]
fn journal_append_streams_exact_length_and_crc_without_growing_the_sink_graph()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("streaming")?;
    let mut writer = paths.open_journal_writer_with_limits(
        "streaming",
        JournalSinkLimits::new(
            NonZeroUsize::new(257).ok_or("invalid journal test buffer")?,
            NonZeroUsize::new(1024 * 1024).ok_or("invalid journal test ceiling")?,
        ),
    )?;
    let fixed_before = writer.fixed_retained_bytes();
    let record = RawCaptureRecord::try_new_live(
        Uuid::from_u128(101),
        Arc::from("streaming-fixture"),
        Uuid::from_u128(102),
        Some(103),
        None,
        "2026-07-15T20:00:01Z".parse::<DateTime<Utc>>()?,
        Bytes::from(vec![255_u8; 32 * 1024]),
    )?;
    let expected_payload = serde_json::to_vec(&record)?;

    writer.append(&record)?;
    assert_eq!(writer.fixed_retained_bytes(), fixed_before);
    assert_eq!(writer.buffer_capacity(), 257);
    writer.flush()?;
    drop(record);
    assert_eq!(writer.fixed_retained_bytes(), fixed_before);
    drop(writer);

    let bytes = std::fs::read(path)?;
    assert_eq!(&bytes[..4], b"MSJ1");
    let length = u32::from_le_bytes(bytes[4..8].try_into()?) as usize;
    let crc = u32::from_le_bytes(bytes[8..12].try_into()?);
    assert_eq!(length, expected_payload.len());
    assert_eq!(crc, crc32fast::hash(&expected_payload));
    assert_eq!(&bytes[12..], expected_payload);
    Ok(())
}
