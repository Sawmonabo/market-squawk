use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use market_squawk_platform::{
    AccessControlledSecretStore, EncryptedFileFallbackStatus, EncryptedFileSecretFallback,
    EncryptedFileSecretStore, EncryptedFileUnlockCapability, LocalAuthorityStateStore,
    LocalSecretStoreError, PreferredSecretStore, SecretAccessPolicy, SecretAccessState,
    SecretBackend, SecretCancellation, SecretGeneration, SecretInteractionPolicy, SecretKey,
    SecretOperationControl, SecretStore, SecretValue,
};
use sha2::{Digest as _, Sha256};

const SLOT_A_FILE: &str = "authority-state-a.bin";
const SLOT_B_FILE: &str = "authority-state-b.bin";
const TEMP_A_FILE: &str = ".authority-state-a.tmp";
const TEMP_B_FILE: &str = ".authority-state-b.tmp";
const GENERATION_OFFSET: usize = 10;
const PREDECESSOR_OFFSET: usize = GENERATION_OFFSET + size_of::<u64>();
const LENGTH_OFFSET: usize = 90;
const LENGTH_BYTES: usize = 8;
const DIGEST_BYTES: usize = 32;
const ENVELOPE_DIGEST_OFFSET: usize = LENGTH_OFFSET + LENGTH_BYTES + DIGEST_BYTES;
const HEADER_BYTES: usize = ENVELOPE_DIGEST_OFFSET + DIGEST_BYTES;
const ENVELOPE_DIGEST_DOMAIN: &[u8] = b"market-squawk-authority-envelope-v3\0";

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn managed_secret_lifecycle_is_generation_exact_and_fail_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let store = EncryptedFileSecretStore::try_open(
        directory.path().join("managed-secrets"),
        SecretValue::new("managed unlock phrase".to_owned())?,
    )?;
    let key = SecretKey::try_new("provider", "account-read")?;
    let active = SecretOperationControl::try_new(
        "provider-onboarding",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )?;
    let generation_one = SecretGeneration::new(1)?;
    let reference_one = store.create(
        &key,
        generation_one,
        SecretValue::new("credential-generation-one".to_owned())?,
        &active,
    )?;

    assert_eq!(reference_one.backend(), SecretBackend::EncryptedFile);
    assert_eq!(reference_one.generation(), generation_one);
    assert_eq!(
        store.read(&reference_one, &active)?.expose_secret(),
        "credential-generation-one"
    );
    assert!(matches!(
        store.create(
            &key,
            generation_one,
            SecretValue::new("must-not-overwrite".to_owned())?,
            &active,
        ),
        Err(LocalSecretStoreError::Conflict)
    ));

    let reference_two = store.replace(
        &key,
        &reference_one,
        SecretGeneration::new(2)?,
        SecretValue::new("credential-generation-two".to_owned())?,
        &active,
    )?;
    assert_eq!(
        store.read(&reference_one, &active)?.expose_secret(),
        "credential-generation-one"
    );
    assert_eq!(
        store.read(&reference_two, &active)?.expose_secret(),
        "credential-generation-two"
    );
    store.delete(&reference_one, &active)?;
    assert!(matches!(
        store.read(&reference_one, &active),
        Err(LocalSecretStoreError::NotFound)
    ));

    let cancelled = SecretCancellation::new();
    cancelled.cancel();
    let cancelled_control = SecretOperationControl::try_new(
        "provider-onboarding",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        cancelled,
    )?;
    assert!(matches!(
        store.delete(&reference_two, &cancelled_control),
        Err(LocalSecretStoreError::OperationCancelled)
    ));
    assert_eq!(
        store.read(&reference_two, &active)?.expose_secret(),
        "credential-generation-two"
    );

    let expired = SecretOperationControl::try_new(
        "provider-onboarding",
        Instant::now() - Duration::from_millis(1),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )?;
    assert!(matches!(
        store.read(&reference_two, &expired),
        Err(LocalSecretStoreError::DeadlineExceeded)
    ));
    optional_access_reuses_generations_and_preserves_explicit_lock()?;
    Ok(())
}

fn optional_access_reuses_generations_and_preserves_explicit_lock() -> TestResult {
    let stage = std::cell::Cell::new("create automatic vault");
    let outcome = (|| -> TestResult {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("managed-access");
        let control = SecretOperationControl::try_new(
            "optional-access-critical",
            Instant::now() + Duration::from_secs(120),
            0,
            SecretInteractionPolicy::Forbid,
            SecretCancellation::new(),
        )?;
        let key = SecretKey::try_new("provider", "retained-oauth-session")?;
        let store =
            AccessControlledSecretStore::try_open(&root, "market-squawk-access-test", &control)?;
        assert_eq!(store.access_status()?.access, SecretAccessState::Ready);
        assert_eq!(store.access_status()?.policy, SecretAccessPolicy::default());
        let reference = store.create(
            &key,
            SecretGeneration::new(7)?,
            SecretValue::new("fixture-retained-token".to_owned())?,
            &control,
        )?;
        assert_eq!(reference.backend(), SecretBackend::EncryptedFile);
        drop(store);

        stage.set("read retained automatic authority");
        let automatic_unlock = {
            let authority = LocalAuthorityStateStore::try_open(root.join("access-policy"))?;
            let bytes = zeroize::Zeroizing::new(authority.load()?.ok_or("missing access policy")?);
            let document: serde_json::Value = serde_json::from_slice(&bytes)?;
            zeroize::Zeroizing::new(
                document["active"]["automaticUnlock"]
                    .as_str()
                    .ok_or("missing automatic key")?
                    .to_owned(),
            )
        };
        stage.set("restart automatic vault and reuse credential");
        let store =
            AccessControlledSecretStore::try_open(&root, "market-squawk-access-test", &control)?;
        assert_eq!(
            store.read(&reference, &control)?.expose_secret(),
            "fixture-retained-token"
        );
        let policy = SecretAccessPolicy {
            enabled: true,
            remember_in_keychain: false,
            reauthenticate_after_seconds: Some(3_600),
        };
        let cancelled = SecretCancellation::new();
        cancelled.cancel();
        let cancelled_control = SecretOperationControl::try_new(
            "cancelled-access-change",
            Instant::now() + Duration::from_secs(30),
            0,
            SecretInteractionPolicy::Forbid,
            cancelled,
        )?;
        assert!(matches!(
            store.configure_access(
                policy,
                Some(EncryptedFileUnlockCapability::new(SecretValue::new(
                    "fixture-password".to_owned()
                )?)),
                &cancelled_control
            ),
            Err(LocalSecretStoreError::OperationCancelled)
        ));
        assert_eq!(store.access_status()?.policy, SecretAccessPolicy::default());
        stage.set("enable optional locking");
        let enabled = store.configure_access(
            policy,
            Some(EncryptedFileUnlockCapability::new(SecretValue::new(
                "fixture-password".to_owned(),
            )?)),
            &control,
        )?;
        assert_eq!(enabled.access, SecretAccessState::Ready);
        assert!(enabled.reauthenticate_at_unix_seconds.is_some());
        let receipt = serde_json::to_string(&enabled)?;
        assert!(!receipt.contains("fixture-password"));
        assert!(!format!("{store:?}").contains("fixture-password"));
        for slot in [SLOT_A_FILE, SLOT_B_FILE] {
            let bytes = fs::read(root.join("access-policy").join(slot))?;
            assert!(
                !bytes
                    .windows(automatic_unlock.len())
                    .any(|window| window == automatic_unlock.as_bytes()),
                "automatic unlock remained in a live authority slot"
            );
            assert!(
                !bytes
                    .windows("fixture-password".len())
                    .any(|window| window == b"fixture-password")
            );
        }
        stage.set("cancel and persist explicit lock");
        assert!(matches!(
            store.lock_access(&cancelled_control),
            Err(LocalSecretStoreError::OperationCancelled)
        ));
        assert_eq!(store.access_status()?.access, SecretAccessState::Ready);
        assert_eq!(
            store.lock_access(&control)?.access,
            SecretAccessState::Locked
        );
        drop(store);

        stage.set("restart explicitly locked vault");
        let store =
            AccessControlledSecretStore::try_open(&root, "market-squawk-access-test", &control)?;
        assert_eq!(store.access_status()?.access, SecretAccessState::Locked);
        assert!(matches!(
            store.read(&reference, &control),
            Err(LocalSecretStoreError::Locked)
        ));
        // The gate rejects an OS reference before it can touch the user's actual Keychain.
        let os_reference = serde_json::from_value(
            serde_json::json!({"version": 1, "backend": "apple_keychain", "locator": "a".repeat(64), "generation": 1}),
        )?;
        assert!(matches!(
            store.read(&os_reference, &control),
            Err(LocalSecretStoreError::Locked)
        ));
        assert!(matches!(
            store.unlock_access(
                EncryptedFileUnlockCapability::new(SecretValue::new(
                    "incorrect-password".to_owned()
                )?),
                &control
            ),
            Err(LocalSecretStoreError::AuthenticationFailed)
        ));
        assert_eq!(store.access_status()?.access, SecretAccessState::Locked);
        stage.set("unlock explicit lock and forget remembered access");
        store.unlock_access(
            EncryptedFileUnlockCapability::new(SecretValue::new("fixture-password".to_owned())?),
            &control,
        )?;
        assert_eq!(
            store.forget_remembered_access(&control)?.access,
            SecretAccessState::Ready
        );
        assert_eq!(
            store.read(&reference, &control)?.expose_secret(),
            "fixture-retained-token"
        );

        // Only an explicitly selected interval expires access; ordinary access had none.
        stage.set("select five-second reauthentication interval");
        store.configure_access(
            SecretAccessPolicy {
                reauthenticate_after_seconds: Some(5),
                ..policy
            },
            None,
            &control,
        )?;
        std::thread::sleep(Duration::from_secs(5));
        assert_eq!(store.access_status()?.access, SecretAccessState::Locked);
        assert!(matches!(
            store.read(&reference, &control),
            Err(LocalSecretStoreError::Locked)
        ));
        stage.set("unlock after timed expiry");
        let reauthenticated = store.unlock_access(
            EncryptedFileUnlockCapability::new(SecretValue::new("fixture-password".to_owned())?),
            &control,
        )?;
        assert_eq!(reauthenticated.access, SecretAccessState::Ready);
        stage.set("restore interval after timed unlock");
        store.configure_access(policy, None, &control)?;

        // An authority publication failure cannot claim that a policy change completed.
        stage.set("reject failed policy publication");
        let interrupted = root.join("access-policy").join(TEMP_A_FILE);
        fs::create_dir(&interrupted)?;
        assert!(
            store
                .configure_access(SecretAccessPolicy::default(), None, &control)
                .is_err()
        );
        assert_eq!(
            store.access_status()?.access,
            SecretAccessState::RecoveryRequired
        );
        fs::remove_dir(interrupted)?;
        drop(store);
        stage.set("reopen after failed policy publication");
        let store =
            AccessControlledSecretStore::try_open(&root, "market-squawk-access-test", &control)?;
        store.unlock_access(
            EncryptedFileUnlockCapability::new(SecretValue::new("fixture-password".to_owned())?),
            &control,
        )?;
        stage.set("disable locking after publication recovery");
        store.configure_access(SecretAccessPolicy::default(), None, &control)?;
        drop(store);
        stage.set("restart with locking disabled");
        let store =
            AccessControlledSecretStore::try_open(&root, "market-squawk-access-test", &control)?;
        assert_eq!(store.access_status()?.access, SecretAccessState::Ready);
        assert_eq!(
            store.read(&reference, &control)?.expose_secret(),
            "fixture-retained-token"
        );
        assert_eq!(reference.generation().get(), 7);
        drop(store);

        // An existing password vault is preserved, adopted once, and then reopens automatically.
        stage.set("seed existing password vault");
        let legacy_root = directory.path().join("existing-password-vault");
        let legacy = EncryptedFileSecretStore::try_open(
            &legacy_root,
            SecretValue::new("existing-owner-unlock".to_owned())?,
        )?;
        let legacy_reference = legacy.create(
            &key,
            SecretGeneration::new(9)?,
            SecretValue::new("retained-existing-token".to_owned())?,
            &control,
        )?;
        drop(legacy);
        stage.set("open existing password vault for recovery");
        let legacy = AccessControlledSecretStore::try_open(
            &legacy_root,
            "market-squawk-access-test",
            &control,
        )?;
        assert_eq!(
            legacy.access_status()?.access,
            SecretAccessState::RecoveryRequired
        );
        stage.set("adopt existing password vault once");
        legacy.unlock_access(
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "existing-owner-unlock".to_owned(),
            )?),
            &control,
        )?;
        drop(legacy);
        stage.set("restart adopted vault automatically");
        let legacy = AccessControlledSecretStore::try_open(
            &legacy_root,
            "market-squawk-access-test",
            &control,
        )?;
        assert_eq!(legacy.access_status()?.access, SecretAccessState::Ready);
        assert_eq!(
            legacy.read(&legacy_reference, &control)?.expose_secret(),
            "retained-existing-token"
        );
        Ok(())
    })();
    outcome.map_err(|error| format!("optional access [{}]: {error}", stage.get()).into())
}

#[test]
fn preferred_store_requires_explicit_memory_only_fallback_unlock() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("preferred-secrets");
    let key = SecretKey::try_new("provider", "fallback-readiness")?;
    let control = SecretOperationControl::try_new(
        "provider-onboarding",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )?;
    let reference = {
        let seeded = EncryptedFileSecretStore::try_open(
            &root,
            SecretValue::new("correct unlock phrase".to_owned())?,
        )?;
        seeded.create(
            &key,
            SecretGeneration::new(1)?,
            SecretValue::new("fallback-only credential".to_owned())?,
            &control,
        )?
    };
    assert!(matches!(
        EncryptedFileSecretFallback::try_open(
            &root,
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "incorrect unlock phrase".to_owned()
            )?),
            &control,
        ),
        Err(LocalSecretStoreError::AuthenticationFailed)
    ));
    let preopened = PreferredSecretStore::try_new(
        "market-squawk-test",
        Some(EncryptedFileSecretFallback::try_open(
            &root,
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "correct unlock phrase".to_owned(),
            )?),
            &control,
        )?),
    )?;
    assert_eq!(
        preopened.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Ready
    );
    assert_eq!(
        preopened.read(&reference, &control)?.expose_secret(),
        "fallback-only credential"
    );
    drop(preopened);
    let preferred = PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
        "market-squawk-test",
        &root,
    )?;

    assert_eq!(
        preferred.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Locked
    );
    assert!(matches!(
        preferred.read(&reference, &control),
        Err(LocalSecretStoreError::Locked)
    ));
    let cancelled_unlock = SecretCancellation::new();
    cancelled_unlock.cancel();
    let cancelled_unlock_control = SecretOperationControl::try_new(
        "provider-onboarding-cancelled-unlock",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        cancelled_unlock,
    )?;
    assert!(matches!(
        preferred.unlock_encrypted_file_fallback(
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "correct unlock phrase".to_owned()
            )?),
            &cancelled_unlock_control,
        ),
        Err(LocalSecretStoreError::OperationCancelled)
    ));
    assert_eq!(
        preferred.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Locked
    );
    assert!(matches!(
        preferred.unlock_encrypted_file_fallback(
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "incorrect unlock phrase".to_owned()
            )?),
            &control,
        ),
        Err(LocalSecretStoreError::AuthenticationFailed)
    ));
    assert_eq!(
        preferred.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Locked
    );

    assert_eq!(
        preferred.unlock_encrypted_file_fallback(
            EncryptedFileUnlockCapability::new(SecretValue::new(
                "correct unlock phrase".to_owned()
            )?),
            &control,
        )?,
        EncryptedFileFallbackStatus::Ready
    );
    assert_eq!(
        preferred.read(&reference, &control)?.expose_secret(),
        "fallback-only credential"
    );
    let cancelled_lock = SecretCancellation::new();
    cancelled_lock.cancel();
    let cancelled_lock_control = SecretOperationControl::try_new(
        "provider-onboarding-cancelled-lock",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        cancelled_lock,
    )?;
    assert!(matches!(
        preferred.lock_encrypted_file_fallback(&cancelled_lock_control),
        Err(LocalSecretStoreError::OperationCancelled)
    ));
    assert_eq!(
        preferred.encrypted_file_fallback_status()?,
        EncryptedFileFallbackStatus::Ready
    );
    assert_eq!(
        preferred.read(&reference, &control)?.expose_secret(),
        "fallback-only credential"
    );
    assert_eq!(
        preferred.lock_encrypted_file_fallback(&control)?,
        EncryptedFileFallbackStatus::Locked
    );
    assert!(matches!(
        preferred.read(&reference, &control),
        Err(LocalSecretStoreError::Locked)
    ));
    Ok(())
}

#[test]
fn forbid_policy_plans_a_new_generation_without_platform_interaction() -> TestResult {
    let directory = tempfile::tempdir()?;
    let control = SecretOperationControl::try_new(
        "provider-unattended-planning",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )?;
    let fallback = EncryptedFileSecretFallback::try_open(
        directory.path().join("preferred-planning"),
        EncryptedFileUnlockCapability::new(SecretValue::new(
            "explicit fallback unlock phrase".to_owned(),
        )?),
        &control,
    )?;
    let store = PreferredSecretStore::try_new("market-squawk-planning-test", Some(fallback))?;
    let generation = SecretGeneration::new(1)?;

    let plan = store.plan_create(
        &SecretKey::try_new("provider", "unattended-create")?,
        generation,
        &control,
    )?;

    assert_eq!(plan.target().generation(), generation);
    #[cfg(target_os = "windows")]
    assert_eq!(
        plan.target().backend(),
        SecretBackend::WindowsCredentialManager
    );
    #[cfg(target_os = "linux")]
    assert_eq!(plan.target().backend(), SecretBackend::EncryptedFile);
    #[cfg(target_os = "macos")]
    assert!(matches!(
        plan.target().backend(),
        SecretBackend::AppleKeychain | SecretBackend::EncryptedFile
    ));
    Ok(())
}

#[test]
fn encrypted_store_confines_authenticates_redacts_and_rotates_secrets() -> TestResult {
    let directory = tempfile::tempdir()?;
    let root = directory.path().join("secrets");
    let key = SecretKey::try_new("provider", "fred-api-key")?;
    let mut store = EncryptedFileSecretStore::try_open(
        &root,
        SecretValue::new("first unlock phrase".to_owned())?,
    )?;

    store.store(&key, SecretValue::new("credential-value-1".to_owned())?)?;
    assert_eq!(store.load(&key)?.expose_secret(), "credential-value-1");
    assert!(!format!("{store:?}{key:?}").contains("fred-api-key"));
    for slot in [SLOT_A_FILE, SLOT_B_FILE] {
        let vault = fs::read(root.join(slot))?;
        for sensitive in ["credential-value-1", "first unlock phrase", "fred-api-key"] {
            assert!(
                !vault
                    .windows(sensitive.len())
                    .any(|window| window == sensitive.as_bytes())
            );
        }
    }

    let (lower, _) = ordered_slots(&root)?;
    fs::remove_file(lower)?;
    assert_eq!(store.load(&key)?.expose_secret(), "credential-value-1");
    assert!(root.join(SLOT_A_FILE).is_file());
    assert!(root.join(SLOT_B_FILE).is_file());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let outside = directory.path().join("outside");
        fs::write(&outside, b"must remain untouched")?;
        let (_, second_slot) = ordered_slots(&root)?;
        let substitution = if second_slot.ends_with(SLOT_A_FILE) {
            root.join(TEMP_A_FILE)
        } else {
            root.join(TEMP_B_FILE)
        };
        symlink(&outside, &substitution)?;
        assert!(matches!(
            store.rotate_unlock(SecretValue::new("rejected unlock phrase".to_owned())?),
            Err(LocalSecretStoreError::UnsafeStorage)
        ));
        assert_eq!(fs::read(&outside)?, b"must remain untouched");
        fs::remove_file(substitution)?;
        store.recover_rotation()?;
        assert_eq!(store.load(&key)?.expose_secret(), "credential-value-1");
    }

    store.rotate_unlock(SecretValue::new("second unlock phrase".to_owned())?)?;
    assert_eq!(store.load(&key)?.expose_secret(), "credential-value-1");
    drop(store);

    let completed_a = fs::read(root.join(SLOT_A_FILE))?;
    let completed_b = fs::read(root.join(SLOT_B_FILE))?;
    for removed in [SLOT_A_FILE, SLOT_B_FILE] {
        fs::write(root.join(SLOT_A_FILE), &completed_a)?;
        fs::write(root.join(SLOT_B_FILE), &completed_b)?;
        fs::remove_file(root.join(removed))?;

        let old = EncryptedFileSecretStore::try_open(
            &root,
            SecretValue::new("first unlock phrase".to_owned())?,
        )?;
        assert!(matches!(
            old.load(&key),
            Err(LocalSecretStoreError::AuthenticationFailed)
        ));
        drop(old);

        let current = EncryptedFileSecretStore::try_open(
            &root,
            SecretValue::new("second unlock phrase".to_owned())?,
        )?;
        assert_eq!(current.load(&key)?.expose_secret(), "credential-value-1");
        drop(current);
    }
    fs::write(root.join(SLOT_A_FILE), completed_a)?;
    fs::write(root.join(SLOT_B_FILE), completed_b)?;

    remove_active_entry_and_reseal_outer_envelope(&root)?;
    let tampered = EncryptedFileSecretStore::try_open(
        &root,
        SecretValue::new("second unlock phrase".to_owned())?,
    )?;
    assert!(matches!(
        tampered.load(&key),
        Err(LocalSecretStoreError::AuthenticationFailed)
    ));
    Ok(())
}

fn ordered_slots(root: &Path) -> TestResult<(PathBuf, PathBuf)> {
    let a = root.join(SLOT_A_FILE);
    let b = root.join(SLOT_B_FILE);
    let a_generation = generation(&fs::read(&a)?)?;
    let b_generation = generation(&fs::read(&b)?)?;
    if a_generation < b_generation {
        Ok((a, b))
    } else {
        Ok((b, a))
    }
}

fn generation(envelope: &[u8]) -> TestResult<u64> {
    Ok(u64::from_be_bytes(
        envelope[GENERATION_OFFSET..GENERATION_OFFSET + size_of::<u64>()].try_into()?,
    ))
}

fn remove_active_entry_and_reseal_outer_envelope(root: &Path) -> TestResult {
    rewrite_vault_pair(root, |vault| {
        vault
            .pointer_mut("/state/active/entries")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or("stable vault entries are missing")?
            .clear();
        Ok(())
    })
}

fn rewrite_vault_pair(
    root: &Path,
    update: impl FnOnce(&mut serde_json::Value) -> TestResult,
) -> TestResult {
    let (low_path, high_path) = ordered_slots(root)?;
    let low = fs::read(&low_path)?;
    let mut high = fs::read(&high_path)?;
    let mut vault: serde_json::Value = serde_json::from_slice(&high[HEADER_BYTES..])?;
    update(&mut vault)?;
    let payload = serde_json::to_vec(&vault)?;
    let low = reseal_outer_envelope(low, &payload)?;
    high[PREDECESSOR_OFFSET..PREDECESSOR_OFFSET + DIGEST_BYTES]
        .copy_from_slice(&low[ENVELOPE_DIGEST_OFFSET..HEADER_BYTES]);
    let high = reseal_outer_envelope(high, &payload)?;
    fs::write(low_path, low)?;
    fs::write(high_path, high)?;
    Ok(())
}

fn reseal_outer_envelope(mut envelope: Vec<u8>, payload: &[u8]) -> TestResult<Vec<u8>> {
    envelope[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_BYTES]
        .copy_from_slice(&u64::try_from(payload.len())?.to_be_bytes());
    envelope[LENGTH_OFFSET + LENGTH_BYTES..ENVELOPE_DIGEST_OFFSET]
        .copy_from_slice(&Sha256::digest(payload));
    let mut digest = Sha256::new();
    digest.update(ENVELOPE_DIGEST_DOMAIN);
    digest.update(&envelope[..ENVELOPE_DIGEST_OFFSET]);
    digest.update(payload);
    envelope[ENVELOPE_DIGEST_OFFSET..HEADER_BYTES].copy_from_slice(&digest.finalize());
    envelope.truncate(HEADER_BYTES);
    envelope.extend_from_slice(payload);
    Ok(envelope)
}
