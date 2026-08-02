//! Capability-confined, no-follow, no-clobber benchmark evidence I/O.

use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

static TEMPORARY_ORDINAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
pub(crate) struct EvidenceDirectory {
    path: PathBuf,
    directory: Dir,
    #[cfg(unix)]
    owner_uid: u32,
}

impl EvidenceDirectory {
    pub(crate) fn try_open(requested: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !requested.is_absolute()
            || requested
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err("benchmark output must be an absolute traversal-free path".into());
        }
        let canonical = fs::canonicalize(requested)?;
        if canonical != requested {
            return Err("benchmark output path is not canonical".into());
        }
        let metadata = requested.symlink_metadata()?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("benchmark output is not a real directory".into());
        }
        let executable = fs::canonicalize(std::env::current_exe()?)?;
        if executable.parent() != Some(canonical.as_path()) {
            return Err("authoritative benchmark executable is not evidence-local".into());
        }
        let executable_metadata = executable.symlink_metadata()?;
        require_private_directory(&metadata, &executable_metadata)?;
        let directory = Dir::open_ambient_dir(&canonical, ambient_authority())?;
        Ok(Self {
            path: canonical,
            directory,
            #[cfg(unix)]
            owner_uid: unix_uid(&metadata),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn entry_names(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        self.entry_names_at(None)
    }

    pub(crate) fn entry_names_at(
        &self,
        relative: Option<&Path>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut names = Vec::new();
        let nested;
        let directory = if let Some(relative) = relative {
            validate_relative_path(relative)?;
            let before = self.directory.symlink_metadata(relative)?;
            if !before.is_dir() || before.file_type().is_symlink() {
                return Err("nested evidence directory is unsafe".into());
            }
            nested = self.directory.open_dir(relative)?;
            let opened = nested.dir_metadata()?;
            if opened.dev() != before.dev() || opened.ino() != before.ino() {
                return Err("nested evidence directory changed before open".into());
            }
            &nested
        } else {
            &self.directory
        };
        for entry in directory.entries()? {
            let name = entry?
                .file_name()
                .into_string()
                .map_err(|_name| "benchmark artifact name is not UTF-8")?;
            names.push(name);
        }
        names.sort_unstable();
        Ok(names)
    }

    pub(crate) fn require_directory(
        &self,
        relative: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        validate_relative_path(relative)?;
        let metadata = self.directory.symlink_metadata(relative)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("benchmark artifact is not an expected real directory".into());
        }
        Ok(())
    }

    pub(crate) fn read_json<T: DeserializeOwned>(
        &self,
        relative: &Path,
        maximum: u64,
    ) -> Result<T, Box<dyn std::error::Error>> {
        Ok(serde_json::from_slice(&self.read(relative, maximum)?)?)
    }

    pub(crate) fn read_json_and_hash<T: DeserializeOwned>(
        &self,
        relative: &Path,
        maximum: u64,
    ) -> Result<(T, String), Box<dyn std::error::Error>> {
        let bytes = self.read(relative, maximum)?;
        let digest = Sha256::digest(&bytes);
        Ok((serde_json::from_slice(&bytes)?, format!("{digest:x}")))
    }

    pub(crate) fn hash_file(
        &self,
        relative: &Path,
        maximum: u64,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let finalized = Sha256::digest(self.read(relative, maximum)?);
        Ok(format!("{finalized:x}"))
    }

    pub(crate) fn write_json<T: Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), Box<dyn std::error::Error>> {
        validate_artifact_name(name)?;
        let ordinal = TEMPORARY_ORDINAL
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_error| "benchmark temporary ordinal overflowed")?;
        let temporary = format!(".capture-evidence-{}-{ordinal}", std::process::id());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        set_private_creation_mode(&mut options);
        let mut file = self.directory.open_with(&temporary, &options)?;
        let publication = (|| -> Result<(), Box<dyn std::error::Error>> {
            serde_json::to_writer(&mut file, value)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            let before = file.metadata()?;
            if !before.is_file() || before.nlink() != 1 {
                return Err("temporary benchmark artifact is ambiguous".into());
            }
            self.directory
                .hard_link(&temporary, &self.directory, name)?;
            let published = self.directory.symlink_metadata(name)?;
            if !published.is_file()
                || published.file_type().is_symlink()
                || published.dev() != before.dev()
                || published.ino() != before.ino()
            {
                return Err("published benchmark artifact changed identity".into());
            }
            self.directory.remove_file(&temporary)?;
            let final_metadata = self.directory.symlink_metadata(name)?;
            if final_metadata.nlink() != 1 {
                return Err("published benchmark artifact has an ambiguous link count".into());
            }
            sync_directory(&self.directory)?;
            Ok(())
        })();
        if publication.is_err() {
            let _cleanup = self.directory.remove_file(&temporary);
        }
        publication
    }

    fn read(&self, relative: &Path, maximum: u64) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        validate_relative_path(relative)?;
        let before = self.directory.symlink_metadata(relative)?;
        if !before.is_file()
            || before.file_type().is_symlink()
            || before.nlink() != 1
            || before.len() > maximum
        {
            return Err("benchmark input is not a bounded unambiguous regular file".into());
        }
        require_private_file(
            &before,
            #[cfg(unix)]
            self.owner_uid,
        )?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = self.directory.open_with(relative, &options)?;
        let opened = file.metadata()?;
        if opened.dev() != before.dev() || opened.ino() != before.ino() || opened.nlink() != 1 {
            return Err("benchmark input changed before open".into());
        }
        let capacity = usize::try_from(opened.len())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity)?;
        file.take(maximum.saturating_add(1))
            .read_to_end(&mut bytes)?;
        let after = self.directory.symlink_metadata(relative)?;
        if bytes.len() != capacity
            || after.dev() != opened.dev()
            || after.ino() != opened.ino()
            || after.len() != opened.len()
            || after.nlink() != 1
        {
            return Err("benchmark input changed during read".into());
        }
        Ok(bytes)
    }
}

fn validate_artifact_name(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    if name.is_empty()
        || name.starts_with('.')
        || name.len() > 128
        || Path::new(name).components().count() != 1
        || name.contains('\\')
    {
        return Err("benchmark artifact name is not a portable component".into());
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.is_absolute()
        || path.components().next().is_none()
        || path.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || component.as_os_str().as_encoded_bytes().len() > 255
        })
    {
        return Err("benchmark input path is not capability-relative".into());
    }
    Ok(())
}

fn sync_directory(directory: &Dir) -> Result<(), Box<dyn std::error::Error>> {
    directory.try_clone()?.into_std_file().sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn require_private_directory(
    metadata: &fs::Metadata,
    executable: &fs::Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if metadata.uid() != executable.uid() || metadata.permissions().mode() & 0o077 != 0 {
        return Err("benchmark output ownership or mode is unsafe".into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_private_directory(
    _metadata: &fs::Metadata,
    _executable: &fs::Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[cfg(unix)]
fn require_private_file(
    metadata: &cap_std::fs::Metadata,
    owner_uid: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use cap_std::fs::MetadataExt as _;

    if metadata.uid() != owner_uid || metadata.mode() & 0o077 != 0 {
        return Err("benchmark input ownership or mode is unsafe".into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn require_private_file(
    _metadata: &cap_std::fs::Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

#[cfg(unix)]
fn unix_uid(metadata: &fs::Metadata) -> u32 {
    use std::os::unix::fs::MetadataExt as _;

    metadata.uid()
}

#[cfg(unix)]
fn set_private_creation_mode(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.mode(0o600);
}

#[cfg(not(unix))]
fn set_private_creation_mode(_options: &mut OpenOptions) {}
