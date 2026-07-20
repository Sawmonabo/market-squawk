use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{RawEvidenceError, RawEvidenceStore};
use tokio_util::sync::CancellationToken;

#[test]
fn raw_evidence_is_content_addressed_idempotent_and_restart_readable() -> Result<(), Box<dyn Error>>
{
    let temporary = tempfile::tempdir()?;
    let store = RawEvidenceStore::new(Dir::open_ambient_dir(
        temporary.path(),
        ambient_authority(),
    )?);
    let first = store.persist(b"immutable SEC evidence")?;
    let second = store.persist(b"immutable SEC evidence")?;
    assert_eq!(first, second);
    assert_eq!(store.read_verified(&first)?, b"immutable SEC evidence");

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        store.persist_cancellable(b"cancelled SEC evidence", &cancellation),
        Err(RawEvidenceError::Cancelled)
    ));

    let restored = RawEvidenceStore::new(Dir::open_ambient_dir(
        temporary.path(),
        ambient_authority(),
    )?);
    assert_eq!(restored.read_verified(&first)?, b"immutable SEC evidence");
    Ok(())
}
