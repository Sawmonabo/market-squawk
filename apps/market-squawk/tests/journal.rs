use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use anyhow::{Context, Result, bail};
use market_squawk::{
    AppPaths, DiagnosticRawEnvelope,
    journal::{JournalError, JournalReader},
};
use tempfile::tempdir;
use uuid::Uuid;

fn envelope(
    source: &str,
    connection_id: Uuid,
    source_sequence: Option<u64>,
    payload: Vec<u8>,
) -> Result<DiagnosticRawEnvelope> {
    Ok(DiagnosticRawEnvelope::try_from_compatibility_parts(
        Uuid::new_v4(),
        source.to_owned(),
        connection_id,
        source_sequence,
        None,
        chrono::Utc::now(),
        payload,
    )?)
}

fn fixture_with_magic(magic: [u8; 4]) -> Result<Vec<u8>> {
    let envelope = envelope(
        "fixture-source",
        Uuid::nil(),
        Some(1),
        br#"{"price":"100.00"}"#.to_vec(),
    )?;
    let payload = serde_json::to_vec(&envelope)?;
    let length = u32::try_from(payload.len())?;

    let mut bytes = magic.to_vec();
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

#[test]
fn reads_legacy_mej1_header() -> Result<()> {
    let bytes = fixture_with_magic(*b"MEJ1")?;
    let records = JournalReader::new(Cursor::new(bytes)).read_all()?;
    assert_eq!(records.len(), 1);
    Ok(())
}

#[test]
fn reads_current_msj1_header() -> Result<()> {
    let bytes = fixture_with_magic(*b"MSJ1")?;
    let records = JournalReader::new(Cursor::new(bytes)).read_all()?;
    assert_eq!(records.len(), 1);
    Ok(())
}

#[test]
fn new_journals_are_identified_as_msj1() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("new")?;
    let mut writer = paths.open_journal_writer("new")?;
    writer.flush()?;
    drop(writer);

    let mut header = [0_u8; 4];
    std::fs::File::open(path)?.read_exact(&mut header)?;
    assert_eq!(&header, b"MSJ1");
    Ok(())
}

#[test]
fn writer_refuses_an_unconfined_or_invalid_source_filename() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;

    let Err(error) = paths.open_journal_writer("../new") else {
        bail!("writer must enforce a confined source filename");
    };

    assert!(error.to_string().contains("source filename is invalid"));
    Ok(())
}

#[test]
fn writer_refuses_to_append_to_legacy_mej1() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("legacy")?;
    let original = fixture_with_magic(*b"MEJ1")?;
    std::fs::write(&path, &original)?;

    let Err(error) = paths.open_journal_writer("legacy") else {
        bail!("legacy journal must remain read-only");
    };
    assert!(error.to_string().contains("legacy journal is read-only"));
    assert_eq!(std::fs::read(path)?, original);
    Ok(())
}

#[test]
fn rejects_unknown_journal_magic() -> Result<()> {
    let bytes = fixture_with_magic(*b"XXXX")?;
    assert!(matches!(
        JournalReader::new(Cursor::new(bytes)).read_all(),
        Err(JournalError::UnsupportedMagic(_))
    ));
    Ok(())
}

#[test]
fn bounded_collection_rejects_record_count_overflow() -> Result<()> {
    let bytes = fixture_with_magic(*b"MSJ1")?;
    assert!(matches!(
        JournalReader::new(Cursor::new(bytes)).read_all_bounded(0, u64::MAX),
        Err(JournalError::RecordLimitExceeded { limit: 0 })
    ));
    Ok(())
}

#[test]
fn journal_round_trip_preserves_raw_envelope() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("test")?;
    let expected = envelope(
        "test-source",
        Uuid::new_v4(),
        Some(42),
        br#"{"price":"100.00"}"#.to_vec(),
    )?;

    let mut writer = paths.open_journal_writer("test")?;
    writer.append(&expected)?;
    writer.flush()?;

    let actual = JournalReader::open(&path)?.read_all()?;
    assert_eq!(actual, vec![expected]);
    Ok(())
}

#[test]
fn journal_detects_payload_corruption() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("test")?;
    let record = envelope("test-source", Uuid::new_v4(), Some(1), b"original".to_vec())?;

    let mut writer = paths.open_journal_writer("test")?;
    writer.append(&record)?;
    writer.flush()?;
    drop(writer);

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let last = bytes.last_mut().context("journal is not empty")?;
    *last ^= 0x01;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&bytes)?;
    file.flush()?;

    let Err(error) = JournalReader::open(&path)?.read_all() else {
        bail!("corruption must fail");
    };
    assert!(error.to_string().contains("checksum mismatch"));
    Ok(())
}

#[test]
fn journal_rejects_a_truncated_record_length() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("test")?;
    let mut writer = paths.open_journal_writer("test")?;
    writer.flush()?;
    drop(writer);

    let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
    file.write_all(&[0x01, 0x02])?;
    file.flush()?;

    let Err(error) = JournalReader::open(&path)?.read_all() else {
        bail!("partial record length must fail");
    };
    assert!(error.to_string().contains("truncated record length"));
    Ok(())
}

#[test]
fn journal_writer_refuses_to_append_to_a_truncated_existing_journal() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let path = paths.journal_write_file("test")?;
    let record = envelope("test-source", Uuid::new_v4(), Some(7), b"original".to_vec())?;

    let mut writer = paths.open_journal_writer("test")?;
    writer.append(&record)?;
    writer.flush()?;
    drop(writer);

    let length = std::fs::metadata(&path)?.len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(length - 1)?;

    let Err(error) = paths.open_journal_writer("test") else {
        bail!("writer must reject a truncated existing journal");
    };
    assert!(error.to_string().contains("truncated record payload"));
    Ok(())
}

#[test]
fn journal_allows_only_one_writer_per_file() -> Result<()> {
    let directory = tempdir()?;
    let paths = AppPaths::prepare(directory.path().join("data"))?;
    let _first = paths.open_journal_writer("test")?;

    let Err(error) = paths.open_journal_writer("test") else {
        bail!("second writer must be rejected");
    };
    assert!(error.to_string().contains("already has an active writer"));
    Ok(())
}
