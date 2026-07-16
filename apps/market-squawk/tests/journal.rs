use std::io::{Cursor, Read, Seek, SeekFrom, Write};

use anyhow::{Context, Result, bail};
use market_squawk::{
    domain::RawEnvelope,
    journal::{JournalError, JournalReader, JournalWriter},
};
use tempfile::tempdir;
use uuid::Uuid;

fn fixture_with_magic(magic: [u8; 4]) -> Result<Vec<u8>> {
    let envelope = RawEnvelope::new(
        "fixture-source",
        Uuid::nil(),
        Some(1),
        None,
        br#"{"price":"100.00"}"#.to_vec(),
    );
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
fn rejects_unknown_journal_magic() -> Result<()> {
    let bytes = fixture_with_magic(*b"XXXX")?;
    assert!(matches!(
        JournalReader::new(Cursor::new(bytes)).read_all(),
        Err(JournalError::UnsupportedMagic(_))
    ));
    Ok(())
}

#[test]
fn journal_round_trip_preserves_raw_envelope() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("test.msj");
    let expected = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(42),
        None,
        br#"{"price":"100.00"}"#.to_vec(),
    );

    let mut writer = JournalWriter::open(&path)?;
    writer.append(&expected)?;
    writer.flush()?;

    let actual = JournalReader::open(&path)?.read_all()?;
    assert_eq!(actual, vec![expected]);
    Ok(())
}

#[test]
fn journal_detects_payload_corruption() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("test.msj");
    let record = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(1),
        None,
        b"original".to_vec(),
    );

    let mut writer = JournalWriter::open(&path)?;
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
    let path = directory.path().join("test.msj");
    let mut writer = JournalWriter::open(&path)?;
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
    let path = directory.path().join("test.msj");
    let record = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(7),
        None,
        b"original".to_vec(),
    );

    let mut writer = JournalWriter::open(&path)?;
    writer.append(&record)?;
    writer.flush()?;
    drop(writer);

    let length = std::fs::metadata(&path)?.len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(length - 1)?;

    let Err(error) = JournalWriter::open(&path) else {
        bail!("writer must reject a truncated existing journal");
    };
    assert!(error.to_string().contains("truncated record payload"));
    Ok(())
}

#[test]
fn journal_allows_only_one_writer_per_file() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("test.msj");
    let _first = JournalWriter::open(&path)?;

    let Err(error) = JournalWriter::open(&path) else {
        bail!("second writer must be rejected");
    };
    assert!(error.to_string().contains("already has an active writer"));
    Ok(())
}
