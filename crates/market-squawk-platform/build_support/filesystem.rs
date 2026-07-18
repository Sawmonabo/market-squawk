//! Capability-relative, bounded build-input hashing.

use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundSourceFile {
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
}

pub(crate) fn hash_regular_file(path: &Path, maximum: u64) -> Result<String, Box<dyn Error>> {
    hash_file_with_after_read(path, maximum, true, || {})
}

pub(crate) fn hash_bound_executable(path: &Path, maximum: u64) -> Result<String, Box<dyn Error>> {
    hash_file_with_after_read(path, maximum, false, || {})
}

fn hash_file_with_after_read<F>(
    path: &Path,
    maximum: u64,
    require_single_link: bool,
    after_read: F,
) -> Result<String, Box<dyn Error>>
where
    F: FnOnce(),
{
    let before_path = fs::symlink_metadata(path)?;
    if before_path.file_type().is_symlink() || !before_path.is_file() || before_path.len() > maximum
    {
        return Err("build input is not a bounded regular file".into());
    }
    let mut file = File::open(path)?;
    let before = file.metadata()?;
    if !same_identity(&before_path, &before, require_single_link) {
        return Err("build input path and descriptor identities differ".into());
    }
    let (digest, observed, after) = hash_open_descriptor(&mut file, &before, maximum)?;
    after_read();
    let current = fs::symlink_metadata(path)?;
    if observed != before.len()
        || !same_identity(&before, &after, require_single_link)
        || !same_identity(&before, &current, require_single_link)
    {
        return Err("build input changed during descriptor hashing".into());
    }
    Ok(digest)
}

fn hash_open_descriptor(
    file: &mut File,
    before: &fs::Metadata,
    maximum: u64,
) -> Result<(String, u64, fs::Metadata), Box<dyn Error>> {
    if !before.is_file() || before.len() > maximum {
        return Err("build input descriptor is not a bounded regular file".into());
    }
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut observed = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read)?)
            .ok_or("build input length overflowed")?;
        if observed > maximum || observed > before.len() {
            return Err("build input grew beyond its descriptor bound".into());
        }
        digest.update(&buffer[..read]);
    }
    Ok((
        format!("{:x}", digest.finalize()),
        observed,
        file.metadata()?,
    ))
}

#[cfg(test)]
pub(crate) fn hash_regular_file_with_test_mutation<F>(
    path: &Path,
    maximum: u64,
    mutate_after_read: F,
) -> Result<String, Box<dyn Error>>
where
    F: FnOnce(),
{
    hash_file_with_after_read(path, maximum, true, mutate_after_read)
}

pub(crate) fn collect_rust_files(
    root: &Path,
    maximum_entries: usize,
    maximum_depth: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
) -> Result<Vec<BoundSourceFile>, Box<dyn Error>> {
    collect_rust_files_with_callbacks(
        root,
        maximum_entries,
        maximum_depth,
        maximum_file_bytes,
        maximum_total_bytes,
        || {},
        |_| {},
    )
}

fn collect_rust_files_with_callbacks<R, F>(
    root: &Path,
    maximum_entries: usize,
    maximum_depth: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
    after_root_metadata: R,
    mut before_open: F,
) -> Result<Vec<BoundSourceFile>, Box<dyn Error>>
where
    R: FnOnce(),
    F: FnMut(&Path),
{
    use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
    use cap_std::ambient_authority;
    use cap_std::fs::{Dir, OpenOptions};

    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err("Rust source root is not a real directory".into());
    }
    after_root_metadata();
    let parent = root
        .parent()
        .ok_or("Rust source root has no capability parent")?;
    let name = root
        .file_name()
        .ok_or("Rust source root has no final component")?;
    let parent = Dir::open_ambient_dir(parent, ambient_authority())?;
    let root_directory = parent.open_dir_nofollow(name)?;
    let root_descriptor_metadata = root_directory.dir_metadata()?;
    let root_path_metadata = cap_std::fs::Metadata::from_just_metadata(root_metadata);
    if !same_directory_identity(&root_path_metadata, &root_descriptor_metadata) {
        return Err("Rust source root descriptor is not a directory".into());
    }
    let mut files = Vec::new();
    let mut pending = vec![(root_directory, root.to_owned(), 0_usize)];
    let mut observed_entries = 0_usize;
    let mut observed_bytes = 0_u64;
    while let Some((directory, display_path, depth)) = pending.pop() {
        if depth > maximum_depth {
            return Err("Rust source inventory exceeds its depth bound".into());
        }
        let mut entries = Vec::new();
        for entry in directory.entries()? {
            observed_entries = observed_entries
                .checked_add(1)
                .ok_or("Rust source entry count overflowed")?;
            if observed_entries > maximum_entries {
                return Err("Rust source inventory exceeds its entry bound".into());
            }
            entries.push(entry?);
        }
        entries.sort_by_key(cap_std::fs::DirEntry::file_name);
        for entry in entries {
            let name = entry.file_name();
            let path = display_path.join(&name);
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err("Rust source inventory contains a symlink".into());
            }
            if file_type.is_dir() {
                let before = directory.symlink_metadata(&name)?;
                before_open(&path);
                let child = directory.open_dir_nofollow(&name)?;
                if !same_directory_identity(&before, &child.dir_metadata()?) {
                    return Err("Rust source directory identity changed during traversal".into());
                }
                pending.push((
                    child,
                    path,
                    depth.checked_add(1).ok_or("Rust source depth overflowed")?,
                ));
            } else if file_type.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("rs")
            {
                let mut options = OpenOptions::new();
                options.read(true).follow(FollowSymlinks::No);
                let mut file = entry.open_with(&options)?.into_std();
                let before = file.metadata()?;
                if before.len() > maximum_file_bytes {
                    return Err("Rust source file exceeds its per-file byte bound".into());
                }
                observed_bytes = observed_bytes
                    .checked_add(before.len())
                    .ok_or("Rust source inventory byte count overflowed")?;
                if observed_bytes > maximum_total_bytes {
                    return Err("Rust source inventory exceeds its total byte bound".into());
                }
                let (sha256, observed, after) =
                    hash_open_descriptor(&mut file, &before, maximum_file_bytes)?;
                let current = entry.open_with(&options)?.into_std().metadata()?;
                if observed != before.len()
                    || !same_identity(&before, &after, true)
                    || !same_identity(&before, &current, true)
                {
                    return Err("Rust source changed during capability hashing".into());
                }
                files.push(BoundSourceFile { path, sha256 });
            } else if !file_type.is_file() {
                return Err("Rust source inventory contains a special filesystem entry".into());
            }
        }
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

#[cfg(test)]
pub(crate) fn collect_rust_files_with_test_replacement<F>(
    root: &Path,
    maximum_entries: usize,
    maximum_depth: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
    before_open: F,
) -> Result<Vec<BoundSourceFile>, Box<dyn Error>>
where
    F: FnMut(&Path),
{
    collect_rust_files_with_callbacks(
        root,
        maximum_entries,
        maximum_depth,
        maximum_file_bytes,
        maximum_total_bytes,
        || {},
        before_open,
    )
}

#[cfg(test)]
pub(crate) fn collect_rust_files_with_test_root_replacement<F>(
    root: &Path,
    maximum_entries: usize,
    maximum_depth: usize,
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
    after_root_metadata: F,
) -> Result<Vec<BoundSourceFile>, Box<dyn Error>>
where
    F: FnOnce(),
{
    collect_rust_files_with_callbacks(
        root,
        maximum_entries,
        maximum_depth,
        maximum_file_bytes,
        maximum_total_bytes,
        after_root_metadata,
        |_| {},
    )
}

#[cfg(unix)]
fn same_identity(left: &fs::Metadata, right: &fs::Metadata, require_single_link: bool) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.nlink() == right.nlink()
        && (!require_single_link || left.nlink() == 1)
        && left.mode() == right.mode()
        && left.uid() == right.uid()
        && left.gid() == right.gid()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(unix)]
fn same_directory_identity(left: &cap_std::fs::Metadata, right: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt as _;

    left.is_dir()
        && right.is_dir()
        && left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.nlink() == right.nlink()
        && left.mode() == right.mode()
        && left.uid() == right.uid()
        && left.gid() == right.gid()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_identity(left: &fs::Metadata, right: &fs::Metadata, _require_single_link: bool) -> bool {
    left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && left.permissions().readonly() == right.permissions().readonly()
}

#[cfg(not(unix))]
fn same_directory_identity(left: &cap_std::fs::Metadata, right: &cap_std::fs::Metadata) -> bool {
    left.is_dir()
        && right.is_dir()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
        && left.permissions().readonly() == right.permissions().readonly()
}
