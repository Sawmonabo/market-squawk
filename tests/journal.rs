use std::io::{Read, Seek, SeekFrom, Write};

use market_engine::{
    domain::RawEnvelope,
    journal::{JournalReader, JournalWriter},
};
use tempfile::tempdir;
use uuid::Uuid;

#[test]
fn journal_round_trip_preserves_raw_envelope() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("test.mej");
    let expected = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(42),
        None,
        br#"{"price":"100.00"}"#.to_vec(),
    );

    let mut writer = JournalWriter::open(&path).expect("open writer");
    writer.append(&expected).expect("append record");
    writer.flush().expect("flush record");

    let actual = JournalReader::open(&path)
        .expect("open reader")
        .read_all()
        .expect("read records");
    assert_eq!(actual, vec![expected]);
}

#[test]
fn journal_detects_payload_corruption() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("test.mej");
    let record = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(1),
        None,
        b"original".to_vec(),
    );

    let mut writer = JournalWriter::open(&path).expect("open writer");
    writer.append(&record).expect("append record");
    writer.flush().expect("flush record");
    drop(writer);

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open journal for corruption");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).expect("read journal bytes");
    let last = bytes.last_mut().expect("journal is not empty");
    *last ^= 0x01;
    file.seek(SeekFrom::Start(0)).expect("seek start");
    file.write_all(&bytes).expect("write corrupted journal");
    file.flush().expect("flush corruption");

    let error = JournalReader::open(&path)
        .expect("header remains valid")
        .read_all()
        .expect_err("corruption must fail");
    assert!(error.to_string().contains("checksum mismatch"));
}

#[test]
fn journal_rejects_a_truncated_record_length() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("test.mej");
    let mut writer = JournalWriter::open(&path).expect("open writer");
    writer.flush().expect("flush header");
    drop(writer);

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open journal for truncation");
    file.write_all(&[0x01, 0x02]).expect("write partial length");
    file.flush().expect("flush truncation");

    let error = JournalReader::open(&path)
        .expect("header remains valid")
        .read_all()
        .expect_err("partial record length must fail");
    assert!(error.to_string().contains("truncated record length"));
}

#[test]
fn journal_writer_refuses_to_append_to_a_truncated_existing_journal() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("test.mej");
    let record = RawEnvelope::new(
        "test-source",
        Uuid::new_v4(),
        Some(7),
        None,
        b"original".to_vec(),
    );

    let mut writer = JournalWriter::open(&path).expect("open writer");
    writer.append(&record).expect("append record");
    writer.flush().expect("flush record");
    drop(writer);

    let length = std::fs::metadata(&path).expect("journal metadata").len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open journal for truncation")
        .set_len(length - 1)
        .expect("truncate final byte");

    let error = JournalWriter::open(&path)
        .err()
        .expect("writer must reject a truncated existing journal");
    assert!(error.to_string().contains("truncated record payload"));
}

#[test]
fn journal_allows_only_one_writer_per_file() {
    let directory = tempdir().expect("temp directory");
    let path = directory.path().join("test.mej");
    let _first = JournalWriter::open(&path).expect("first writer obtains lock");

    let error = JournalWriter::open(&path)
        .err()
        .expect("second writer must be rejected");
    assert!(error.to_string().contains("already has an active writer"));
}
