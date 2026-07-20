//! Critical hostile-input and local capability boundary proofs.

use std::error::Error;
use std::fs;

use market_squawk_domain::DigestAlgorithm;
use market_squawk_platform::{InputFileError, UserAuthorizedInputRoot};
use sha2::{Digest as _, Sha256};

#[test]
fn bounded_input_is_exact_and_rejects_growth_or_replacement() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("source.csv"), b"abc")?;
    let root_path = fs::canonicalize(directory.path())?;
    let root = UserAuthorizedInputRoot::open(&root_path)?;

    let exact = root
        .resolve("source.csv")?
        .open_bounded(3)?
        .read_bounded()?;
    assert_eq!(exact.as_bytes(), b"abc");
    assert_eq!(exact.identity().size_bytes(), 3);
    assert_eq!(exact.digest().algorithm(), DigestAlgorithm::Sha256);
    assert_eq!(exact.digest().bytes(), Sha256::digest(b"abc").as_slice());

    let oversized = root.resolve("source.csv")?.open_bounded(2);
    assert!(matches!(
        oversized,
        Err(InputFileError::ByteLimitExceeded { max: 2 })
    ));

    let pending = root.resolve("source.csv")?;
    fs::rename(
        directory.path().join("source.csv"),
        directory.path().join("old.csv"),
    )?;
    fs::write(directory.path().join("source.csv"), b"abc")?;
    assert!(matches!(
        pending.open_bounded(3),
        Err(InputFileError::IdentityChanged)
    ));

    fs::write(directory.path().join("locked.csv"), b"locked")?;
    let locked = fs::File::open(directory.path().join("locked.csv"))?;
    fs2::FileExt::try_lock_exclusive(&locked)?;
    let verified = root.resolve("locked.csv")?.open_bounded(6)?;
    assert!(matches!(
        verified.read_bounded(),
        Err(InputFileError::FileBusy)
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn input_root_rejects_symlinks_at_root_intermediate_and_file() -> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir()?;
    let outside = tempfile::tempdir()?;
    fs::write(outside.path().join("secret.csv"), b"secret")?;

    symlink(outside.path(), directory.path().join("linked-root"))?;
    assert!(matches!(
        UserAuthorizedInputRoot::open(directory.path().join("linked-root")),
        Err(InputFileError::SymlinkOrReparsePoint)
    ));

    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    symlink(outside.path(), directory.path().join("linked-directory"))?;
    assert!(matches!(
        root.resolve("linked-directory/secret.csv"),
        Err(InputFileError::SymlinkOrReparsePoint)
    ));

    symlink(
        outside.path().join("secret.csv"),
        directory.path().join("linked-file.csv"),
    )?;
    assert!(matches!(
        root.resolve("linked-file.csv"),
        Err(InputFileError::SymlinkOrReparsePoint)
    ));
    Ok(())
}

#[test]
fn input_capability_diagnostics_do_not_disclose_paths() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("sensitive.csv"), b"value")?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let capability = root.resolve("sensitive.csv")?;
    let diagnostics = format!("{root:?} {capability:?}");
    assert!(!diagnostics.contains(directory.path().to_string_lossy().as_ref()));
    assert!(!diagnostics.contains("sensitive.csv"));
    Ok(())
}
