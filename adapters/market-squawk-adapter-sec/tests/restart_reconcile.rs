use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{
    SecHttpValidators, SecRepresentationLimits, SecRepresentationRegistry,
};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use tokio_util::sync::CancellationToken;

#[test]
fn representation_restart_reuses_identical_and_304_but_revises_changed_bytes()
-> Result<(), Box<dyn Error>> {
    let temporary = tempfile::tempdir()?;
    let open = || {
        Dir::open_ambient_dir(temporary.path(), ambient_authority())
            .map(|directory| (directory, SecRepresentationLimits::production_defaults()))
    };
    let url = "https://data.sec.gov/submissions/CIK0000320193.json";
    let validators = SecHttpValidators::try_new(
        Some("\"submissions-v1\""),
        Some("Mon, 20 Jul 2026 12:00:00 GMT"),
    )?;
    let (directory, limits) = open()?;
    let registry = SecRepresentationRegistry::open(directory, limits)?;
    let first = registry.record_success(url, digest(1), 128, validators.clone())?;
    assert_eq!(first.retrieval_revision(), 1);
    drop(registry);

    let (directory, limits) = open()?;
    let restarted = SecRepresentationRegistry::open(directory, limits)?;
    let identical = restarted.record_success(url, digest(1), 128, validators.clone())?;
    assert_eq!(identical, first);
    let conditional = restarted
        .conditional_request(url)?
        .ok_or("missing validators")?;
    assert_eq!(conditional, validators);

    let changed = restarted.record_success(
        url,
        digest(2),
        144,
        SecHttpValidators::try_new(Some("\"submissions-v2\""), None)?,
    )?;
    assert_eq!(changed.retrieval_revision(), 2);
    assert!(changed.first_observed_at() > first.first_observed_at());
    let not_modified = restarted.record_not_modified(url, SecHttpValidators::default())?;
    assert_eq!(not_modified, changed);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        restarted.record_success_cancellable(
            url,
            digest(3),
            160,
            SecHttpValidators::default(),
            &cancellation,
        ),
        Err(market_squawk_adapter_sec::SecRepresentationError::Cancelled)
    ));
    Ok(())
}

fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}
