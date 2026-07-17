use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Barrier};

use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};

const CANONICAL_FILE: &str = "authority-state.bin";
const TEMP_FILE: &str = ".authority-state.tmp";
const LOCK_FILE: &str = ".authority-state.lock";
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAGIC_BYTES: usize = 8;
const VERSION_BYTES: usize = 2;
const LENGTH_BYTES: usize = 8;
const DIGEST_BYTES: usize = 32;
const HEADER_BYTES: usize = MAGIC_BYTES + VERSION_BYTES + LENGTH_BYTES + DIGEST_BYTES;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
type CorruptionCase = (&'static str, fn(&mut Vec<u8>));

#[test]
fn round_trip_replacement_and_reopen_preserve_only_the_latest_canonical_payload() -> TestResult {
    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        assert_eq!(store.load()?, None);
        store.store(b"first-authority-state")?;
        assert_eq!(store.load()?, Some(b"first-authority-state".to_vec()));
        store.store(b"second-authority-state")?;
        assert_eq!(store.load()?, Some(b"second-authority-state".to_vec()));
    }

    let reopened = LocalAuthorityStateStore::try_open(directory.path())?;
    assert_eq!(reopened.load()?, Some(b"second-authority-state".to_vec()));
    assert!(!directory.path().join(TEMP_FILE).exists());
    Ok(())
}

#[test]
fn exact_maximum_payload_is_accepted_and_one_byte_over_is_rejected_atomically() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    let exact = vec![0x5a; MAX_PAYLOAD_BYTES];
    store.store(&exact)?;
    assert_eq!(store.load()?.as_deref(), Some(exact.as_slice()));

    let over = vec![0x6b; MAX_PAYLOAD_BYTES + 1];
    assert!(matches!(
        store.store(&over),
        Err(LocalAuthorityStateStoreError::PayloadTooLarge {
            bytes,
            maximum: MAX_PAYLOAD_BYTES,
        }) if bytes == MAX_PAYLOAD_BYTES + 1
    ));
    assert_eq!(store.load()?.as_deref(), Some(exact.as_slice()));
    Ok(())
}

#[test]
fn lifetime_lock_rejects_a_second_store_until_the_first_owner_drops() -> TestResult {
    let directory = tempfile::tempdir()?;
    let first = LocalAuthorityStateStore::try_open(directory.path())?;
    assert!(matches!(
        LocalAuthorityStateStore::try_open(directory.path()),
        Err(LocalAuthorityStateStoreError::AlreadyLocked)
    ));
    drop(first);
    let _successor = LocalAuthorityStateStore::try_open(directory.path())?;
    Ok(())
}

#[test]
fn concurrent_in_process_writers_are_serialized_without_losing_canonical_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(LocalAuthorityStateStore::try_open(directory.path())?);
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();
    for payload in [b"writer-one".to_vec(), b"writer-two".to_vec()] {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            store.store(&payload)
        }));
    }
    barrier.wait();
    for writer in writers {
        writer.join().map_err(|_| "writer thread panicked")??;
    }

    let final_state = store.load()?.ok_or("canonical state is missing")?;
    assert!(final_state == b"writer-one" || final_state == b"writer-two");
    assert!(!directory.path().join(TEMP_FILE).exists());
    Ok(())
}

#[test]
fn debug_output_redacts_directory_handles_and_authority_payloads() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    store.store(b"sentinel-authority-payload")?;
    let rendered = format!("{store:?}");
    assert!(rendered.contains("LocalAuthorityStateStore"));
    assert!(!rendered.contains("sentinel-authority-payload"));
    assert!(!rendered.contains(&directory.path().display().to_string()));
    Ok(())
}

#[test]
fn orphan_temp_is_never_authority_and_is_recovered_before_the_next_store() -> TestResult {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join(TEMP_FILE), b"interrupted-write")?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    assert_eq!(store.load()?, None);
    store.store(b"canonical")?;
    assert_eq!(store.load()?, Some(b"canonical".to_vec()));
    assert!(!directory.path().join(TEMP_FILE).exists());
    Ok(())
}

#[test]
fn truncated_hash_version_and_declared_length_corruption_fail_closed() -> TestResult {
    let cases: &[CorruptionCase] = &[
        ("truncated", |bytes| bytes.truncate(HEADER_BYTES - 1)),
        ("hash", |bytes| {
            bytes[MAGIC_BYTES + VERSION_BYTES + LENGTH_BYTES] ^= 0xff
        }),
        ("version", |bytes| bytes[MAGIC_BYTES + 1] ^= 0x01),
        ("length", |bytes| {
            bytes[MAGIC_BYTES + VERSION_BYTES + LENGTH_BYTES - 1] ^= 0x01
        }),
    ];

    for (name, corrupt) in cases {
        let directory = tempfile::tempdir()?;
        {
            let store = LocalAuthorityStateStore::try_open(directory.path())?;
            store.store(format!("payload-{name}").as_bytes())?;
        }
        let path = directory.path().join(CANONICAL_FILE);
        let mut bytes = fs::read(&path)?;
        corrupt(&mut bytes);
        fs::write(path, bytes)?;

        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        assert!(matches!(
            store.load(),
            Err(LocalAuthorityStateStoreError::CorruptEnvelope)
        ));
    }
    Ok(())
}

#[test]
fn oversized_envelope_is_rejected_before_payload_allocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let canonical = directory.path().join(CANONICAL_FILE);
    let file = fs::File::create(&canonical)?;
    file.set_len(u64::try_from(HEADER_BYTES + MAX_PAYLOAD_BYTES + 1)?)?;
    drop(file);
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    assert!(matches!(
        store.load(),
        Err(LocalAuthorityStateStoreError::EnvelopeTooLarge { .. })
    ));
    Ok(())
}

#[test]
fn failed_new_store_does_not_replace_the_last_canonical_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    store.store(b"last-good")?;
    let before = fs::read(directory.path().join(CANONICAL_FILE))?;

    assert!(matches!(
        store.store(&vec![0; MAX_PAYLOAD_BYTES + 1]),
        Err(LocalAuthorityStateStoreError::PayloadTooLarge { .. })
    ));
    assert_eq!(fs::read(directory.path().join(CANONICAL_FILE))?, before);
    assert_eq!(store.load()?, Some(b"last-good".to_vec()));
    Ok(())
}

#[cfg(unix)]
#[test]
fn canonical_temp_lock_and_root_symlinks_are_rejected_without_following() -> TestResult {
    use std::os::unix::fs::symlink;

    for protected_name in [CANONICAL_FILE, TEMP_FILE, LOCK_FILE] {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("authority");
        let outside = parent.path().join("outside");
        fs::create_dir(&root)?;
        fs::write(&outside, b"outside")?;
        symlink(&outside, root.join(protected_name))?;

        let opened = LocalAuthorityStateStore::try_open(&root);
        if protected_name == CANONICAL_FILE {
            let store = opened?;
            assert!(matches!(
                store.load(),
                Err(LocalAuthorityStateStoreError::UnsafeFileType)
            ));
        } else {
            assert!(matches!(
                opened,
                Err(LocalAuthorityStateStoreError::UnsafeFileType)
            ));
        }
        assert_eq!(fs::read(&outside)?, b"outside");
    }

    let parent = tempfile::tempdir()?;
    let real = parent.path().join("real-root");
    let alias = parent.path().join("root-alias");
    fs::create_dir(&real)?;
    symlink(&real, &alias)?;
    assert!(matches!(
        LocalAuthorityStateStore::try_open(alias),
        Err(LocalAuthorityStateStoreError::UnsafeRoot)
    ));
    Ok(())
}

#[cfg(windows)]
#[test]
fn reparse_root_is_rejected_from_the_opened_windows_handle() -> TestResult {
    use std::os::windows::fs::symlink_dir;

    let parent = tempfile::tempdir()?;
    let real = parent.path().join("real-root");
    let alias = parent.path().join("root-alias");
    fs::create_dir(&real)?;
    symlink_dir(&real, &alias)?;

    assert!(matches!(
        LocalAuthorityStateStore::try_open(alias),
        Err(LocalAuthorityStateStoreError::UnsafeRoot)
    ));
    Ok(())
}

#[test]
fn non_regular_canonical_file_is_rejected_as_ambiguous_authority() -> TestResult {
    let directory = tempfile::tempdir()?;
    fs::create_dir(directory.path().join(CANONICAL_FILE))?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    assert!(matches!(
        store.load(),
        Err(LocalAuthorityStateStoreError::UnsafeFileType)
    ));
    Ok(())
}

#[test]
fn hard_linked_reserved_files_are_rejected_as_ambiguous_authority() -> TestResult {
    for protected_name in [CANONICAL_FILE, TEMP_FILE, LOCK_FILE] {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("authority");
        fs::create_dir(&root)?;
        let outside = parent.path().join("outside");
        fs::write(&outside, b"outside")?;
        fs::hard_link(&outside, root.join(protected_name))?;

        let opened = LocalAuthorityStateStore::try_open(&root);
        if protected_name == CANONICAL_FILE {
            let store = opened?;
            assert!(matches!(
                store.load(),
                Err(LocalAuthorityStateStoreError::UnsafeFileType)
            ));
        } else {
            assert!(matches!(
                opened,
                Err(LocalAuthorityStateStoreError::UnsafeFileType)
            ));
        }
        assert_eq!(fs::read(&outside)?, b"outside");
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn existing_root_and_new_state_files_receive_private_permissions() -> TestResult {
    use std::os::unix::fs::PermissionsExt as _;

    let parent = tempfile::tempdir()?;
    let root = parent.path().join("authority");
    fs::create_dir(&root)?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o777))?;
    let store = LocalAuthorityStateStore::try_open(&root)?;
    store.store(b"private")?;

    assert_eq!(fs::metadata(&root)?.permissions().mode() & 0o777, 0o700);
    assert_eq!(
        fs::metadata(root.join(CANONICAL_FILE))?
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(root.join(LOCK_FILE))?.permissions().mode() & 0o777,
        0o600
    );
    Ok(())
}

#[test]
fn absent_nested_root_is_created_before_capability_confined_use() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("local").join("authority");
    let store = LocalAuthorityStateStore::try_open(&root)?;
    store.store(b"created-root")?;
    assert_eq!(store.load()?, Some(b"created-root".to_vec()));
    assert!(root.join(CANONICAL_FILE).is_file());
    Ok(())
}

#[test]
fn trailing_bytes_after_the_declared_payload_are_rejected() -> TestResult {
    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        store.store(b"canonical")?;
    }
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join(CANONICAL_FILE))?;
    file.write_all(b"trailing")?;
    file.sync_all()?;

    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    assert!(matches!(
        store.load(),
        Err(LocalAuthorityStateStoreError::CorruptEnvelope)
    ));
    Ok(())
}

#[test]
fn a_length_field_larger_than_the_file_fails_without_growth_or_retry() -> TestResult {
    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        store.store(b"short")?;
    }
    let canonical = directory.path().join(CANONICAL_FILE);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&canonical)?;
    file.seek(SeekFrom::Start(u64::try_from(MAGIC_BYTES + VERSION_BYTES)?))?;
    file.write_all(&(MAX_PAYLOAD_BYTES as u64).to_be_bytes())?;
    file.sync_all()?;
    drop(file);

    let before = fs::metadata(&canonical)?.len();
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    assert!(matches!(
        store.load(),
        Err(LocalAuthorityStateStoreError::CorruptEnvelope)
    ));
    assert_eq!(fs::metadata(canonical)?.len(), before);
    Ok(())
}
