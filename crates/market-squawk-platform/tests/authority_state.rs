use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Barrier};

use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};

const SLOT_A_FILE: &str = "authority-state-a.bin";
const SLOT_B_FILE: &str = "authority-state-b.bin";
const TEMP_A_FILE: &str = ".authority-state-a.tmp";
const TEMP_B_FILE: &str = ".authority-state-b.tmp";
const LOCK_FILE: &str = ".authority-state.lock";
const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const MAGIC_BYTES: usize = 8;
const VERSION_BYTES: usize = 2;
const GENERATION_BYTES: usize = 8;
const LENGTH_BYTES: usize = 8;
const DIGEST_BYTES: usize = 32;
const LENGTH_OFFSET: usize =
    MAGIC_BYTES + VERSION_BYTES + GENERATION_BYTES + DIGEST_BYTES + GENERATION_BYTES + DIGEST_BYTES;
const HEADER_BYTES: usize = LENGTH_OFFSET + LENGTH_BYTES + DIGEST_BYTES + DIGEST_BYTES;

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

    #[cfg(unix)]
    fs::hard_link(
        directory.path().join(SLOT_A_FILE),
        directory.path().join(TEMP_A_FILE),
    )?;

    let reopened = LocalAuthorityStateStore::try_open(directory.path())?;
    assert_eq!(reopened.load()?, Some(b"second-authority-state".to_vec()));
    assert!(!directory.path().join(TEMP_A_FILE).exists());
    assert!(!directory.path().join(TEMP_B_FILE).exists());
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
    assert!(!directory.path().join(TEMP_A_FILE).exists());
    assert!(!directory.path().join(TEMP_B_FILE).exists());
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
fn unproven_orphan_temp_is_rejected_as_ambiguous_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join(TEMP_A_FILE), b"interrupted-write")?;
    assert!(matches!(
        LocalAuthorityStateStore::try_open(directory.path()),
        Err(LocalAuthorityStateStoreError::UnsafeFileType)
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn valid_pre_replace_temporary_is_discarded_without_advancing_committed_state() -> TestResult {
    let canonical = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(canonical.path())?;
        store.store(b"committed")?;
    }

    let successor = tempfile::tempdir()?;
    fs::copy(
        canonical.path().join(SLOT_A_FILE),
        successor.path().join(SLOT_A_FILE),
    )?;
    fs::copy(
        canonical.path().join(SLOT_B_FILE),
        successor.path().join(SLOT_B_FILE),
    )?;
    {
        let store = LocalAuthorityStateStore::try_open(successor.path())?;
        store.store(b"not-committed")?;
    }
    fs::copy(
        successor.path().join(SLOT_A_FILE),
        canonical.path().join(TEMP_A_FILE),
    )?;

    let reopened = LocalAuthorityStateStore::try_open(canonical.path())?;
    assert_eq!(reopened.load()?, Some(b"committed".to_vec()));
    assert!(!canonical.path().join(TEMP_A_FILE).exists());
    Ok(())
}

#[test]
fn one_content_invalid_peer_is_repaired_but_two_valid_unrelated_peers_fail_closed() -> TestResult {
    let cases: &[CorruptionCase] = &[
        ("truncated", |bytes| bytes.truncate(HEADER_BYTES - 1)),
        ("hash", |bytes| bytes[LENGTH_OFFSET + LENGTH_BYTES] ^= 0xff),
        ("version", |bytes| bytes[MAGIC_BYTES + 1] ^= 0x01),
        ("length", |bytes| {
            bytes[LENGTH_OFFSET + LENGTH_BYTES - 1] ^= 0x01
        }),
    ];

    for (name, corrupt) in cases {
        let directory = tempfile::tempdir()?;
        {
            let store = LocalAuthorityStateStore::try_open(directory.path())?;
            store.store(format!("payload-{name}").as_bytes())?;
        }
        let path = directory.path().join(SLOT_B_FILE);
        let mut bytes = fs::read(&path)?;
        corrupt(&mut bytes);
        fs::write(path, bytes)?;

        let reopened = LocalAuthorityStateStore::try_open(directory.path())?;
        assert_eq!(
            reopened.load()?,
            Some(format!("payload-{name}").into_bytes())
        );
    }

    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        store.store(b"canonical")?;
    }
    fs::copy(
        directory.path().join(SLOT_A_FILE),
        directory.path().join(SLOT_B_FILE),
    )?;
    assert!(matches!(
        LocalAuthorityStateStore::try_open(directory.path()),
        Err(LocalAuthorityStateStoreError::GenerationConflict)
    ));
    Ok(())
}

#[test]
fn oversized_envelope_is_rejected_before_payload_allocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let canonical = directory.path().join(SLOT_B_FILE);
    let file = fs::File::create(&canonical)?;
    file.set_len(u64::try_from(HEADER_BYTES + MAX_PAYLOAD_BYTES + 1)?)?;
    drop(file);
    assert!(matches!(
        LocalAuthorityStateStore::try_open(directory.path()),
        Err(LocalAuthorityStateStoreError::EnvelopeTooLarge { .. })
    ));
    Ok(())
}

#[test]
fn failed_new_store_does_not_replace_the_last_canonical_state() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = LocalAuthorityStateStore::try_open(directory.path())?;
    store.store(b"last-good")?;
    let before_a = fs::read(directory.path().join(SLOT_A_FILE))?;
    let before_b = fs::read(directory.path().join(SLOT_B_FILE))?;

    assert!(matches!(
        store.store(&vec![0; MAX_PAYLOAD_BYTES + 1]),
        Err(LocalAuthorityStateStoreError::PayloadTooLarge { .. })
    ));
    assert_eq!(fs::read(directory.path().join(SLOT_A_FILE))?, before_a);
    assert_eq!(fs::read(directory.path().join(SLOT_B_FILE))?, before_b);
    assert_eq!(store.load()?, Some(b"last-good".to_vec()));
    Ok(())
}

#[cfg(unix)]
#[test]
fn canonical_temp_lock_and_root_symlinks_are_rejected_without_following() -> TestResult {
    use std::os::unix::fs::symlink;

    for protected_name in [
        SLOT_A_FILE,
        SLOT_B_FILE,
        TEMP_A_FILE,
        TEMP_B_FILE,
        LOCK_FILE,
    ] {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("authority");
        let outside = parent.path().join("outside");
        fs::create_dir(&root)?;
        fs::write(&outside, b"outside")?;
        symlink(&outside, root.join(protected_name))?;

        let opened = LocalAuthorityStateStore::try_open(&root);
        assert!(matches!(
            opened,
            Err(LocalAuthorityStateStoreError::UnsafeFileType)
        ));
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
    fs::create_dir(directory.path().join(SLOT_B_FILE))?;
    assert!(matches!(
        LocalAuthorityStateStore::try_open(directory.path()),
        Err(LocalAuthorityStateStoreError::UnsafeFileType)
    ));
    Ok(())
}

#[test]
fn hard_linked_reserved_files_are_rejected_as_ambiguous_authority() -> TestResult {
    for protected_name in [
        SLOT_A_FILE,
        SLOT_B_FILE,
        TEMP_A_FILE,
        TEMP_B_FILE,
        LOCK_FILE,
    ] {
        let parent = tempfile::tempdir()?;
        let root = parent.path().join("authority");
        fs::create_dir(&root)?;
        let outside = parent.path().join("outside");
        fs::write(&outside, b"outside")?;
        fs::hard_link(&outside, root.join(protected_name))?;

        let opened = LocalAuthorityStateStore::try_open(&root);
        assert!(matches!(
            opened,
            Err(LocalAuthorityStateStoreError::UnsafeFileType)
        ));
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
        fs::metadata(root.join(SLOT_A_FILE))?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(
        fs::metadata(root.join(SLOT_B_FILE))?.permissions().mode() & 0o777,
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
    assert!(root.join(SLOT_A_FILE).is_file());
    assert!(root.join(SLOT_B_FILE).is_file());
    Ok(())
}

#[test]
fn trailing_bytes_on_one_peer_are_repaired_from_the_verified_peer() -> TestResult {
    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        store.store(b"canonical")?;
    }
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(directory.path().join(SLOT_B_FILE))?;
    file.write_all(b"trailing")?;
    file.sync_all()?;

    let reopened = LocalAuthorityStateStore::try_open(directory.path())?;
    assert_eq!(reopened.load()?, Some(b"canonical".to_vec()));
    Ok(())
}

#[test]
fn invalid_declared_length_on_one_peer_is_repaired_without_file_growth() -> TestResult {
    let directory = tempfile::tempdir()?;
    {
        let store = LocalAuthorityStateStore::try_open(directory.path())?;
        store.store(b"short")?;
    }
    let canonical = directory.path().join(SLOT_B_FILE);
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&canonical)?;
    file.seek(SeekFrom::Start(u64::try_from(LENGTH_OFFSET)?))?;
    file.write_all(&(MAX_PAYLOAD_BYTES as u64).to_be_bytes())?;
    file.sync_all()?;
    drop(file);

    let before = fs::metadata(&canonical)?.len();
    let reopened = LocalAuthorityStateStore::try_open(directory.path())?;
    assert_eq!(reopened.load()?, Some(b"short".to_vec()));
    assert_eq!(fs::metadata(canonical)?.len(), before);
    Ok(())
}
