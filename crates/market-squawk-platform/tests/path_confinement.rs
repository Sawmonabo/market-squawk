use std::path::Path;

use market_squawk_platform::{ArtifactPathError, JournalError, LocalPaths};
use tempfile::tempdir;

#[test]
fn artifact_paths_reject_absolute_and_traversal_components()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let artifacts = paths.artifacts()?;

    assert!(matches!(
        artifacts.resolve(Path::new("../escape.json")),
        Err(ArtifactPathError::UnsafeComponent { .. })
    ));
    assert!(matches!(
        artifacts.resolve(directory.path().join("absolute.json")),
        Err(ArtifactPathError::AbsolutePath { .. })
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn existing_symlink_escape_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let outside = directory.path().join("outside");
    std::fs::create_dir(&outside)?;
    symlink(&outside, paths.artifacts()?.root().join("escape"))?;

    assert!(matches!(
        paths
            .artifacts()?
            .resolve(Path::new("escape/file.json"))?
            .create_new(),
        Err(ArtifactPathError::EscapesRoot { .. }) | Err(ArtifactPathError::Io { .. })
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn non_utf8_artifact_components_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let relative = PathBuf::from(OsString::from_vec(vec![0xff, b'.', b'j', b's', b'o', b'n']));

    assert!(matches!(
        paths.artifacts()?.resolve(relative),
        Err(ArtifactPathError::NonUtf8Component)
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn preparation_reports_read_only_directory_failures() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempdir()?;
    let parent = directory.path().join("readonly");
    std::fs::create_dir(&parent)?;
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o500))?;
    let result = LocalPaths::prepare(parent.join("data"));
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700))?;

    assert!(result.is_err());
    Ok(())
}

#[test]
fn resolved_artifact_revalidates_before_create_new() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let resolved = paths
        .artifacts()?
        .resolve(Path::new("reports/result.json"))?;
    let mut file = resolved.create_new()?;
    use std::io::Write;
    file.write_all(b"{}")?;

    assert!(resolved.create_new().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn ancestor_symlink_substitution_cannot_redirect_capability_relative_creation()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let outside = directory.path().join("outside");
    std::fs::create_dir(&outside)?;
    let resolved = paths
        .artifacts()?
        .resolve(Path::new("reports/private/result.json"))?;
    symlink(&outside, paths.artifacts()?.root().join("reports"))?;

    assert!(resolved.create_new().is_err());
    assert!(!outside.join("private/result.json").exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn journal_writer_rejects_a_final_symlink_even_when_the_target_exists()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let outside = directory.path().join("outside.msj");
    std::fs::write(&outside, b"outside")?;
    symlink(&outside, paths.journal_dir().join("source-a.msj"))?;

    assert!(matches!(
        paths.open_journal_writer("source-a"),
        Err(JournalError::SymlinkNotAllowed)
    ));
    assert_eq!(std::fs::read(outside)?, b"outside");
    Ok(())
}

#[cfg(unix)]
#[test]
fn journal_directory_substitution_cannot_redirect_the_retained_capability()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::symlink;

    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;
    let original_journal = paths.journal_dir().to_path_buf();
    let retained_journal = directory.path().join("retained-journal");
    let outside = directory.path().join("outside");
    std::fs::create_dir(&outside)?;
    std::fs::rename(&original_journal, &retained_journal)?;
    symlink(&outside, &original_journal)?;

    let mut writer = paths.open_journal_writer("source-a")?;
    writer.flush()?;
    drop(writer);

    assert!(retained_journal.join("source-a.msj").is_file());
    assert!(!outside.join("source-a.msj").exists());
    Ok(())
}

#[test]
fn journal_source_names_are_single_bounded_portable_components()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::for_read(directory.path().join("missing"));

    for invalid in [
        "",
        ".",
        "..",
        "../escape",
        "nested/source",
        "source.msj",
        "bad\nname",
    ] {
        assert!(
            paths.journal_write_file(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
    assert!(paths.journal_write_file("coinbase-exchange").is_ok());
    assert!(!directory.path().join("missing").exists());
    Ok(())
}

#[test]
fn artifact_names_reject_cross_platform_ambiguous_components()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("data"))?;

    for invalid in [
        "CON",
        "nul.txt",
        "report.",
        "report ",
        "stream:name",
        "back\\slash",
    ] {
        assert!(paths.artifacts()?.resolve(Path::new(invalid)).is_err());
    }
    Ok(())
}

#[test]
fn a_nonexistent_relative_default_root_is_created_from_the_current_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new()
        .prefix("platform-relative-")
        .tempdir_in(".")?;
    let parent_name = directory
        .path()
        .file_name()
        .ok_or("temporary directory has no filename")?;
    let relative = std::path::PathBuf::from(parent_name).join("relative-data-root");
    assert!(relative.is_relative());
    let paths = LocalPaths::prepare(&relative)?;
    assert!(paths.root().ends_with("relative-data-root"));
    Ok(())
}
