//! Filesystem identity for durable catalog and artifact endpoint bindings.

use std::fs::File;
use std::io;
use std::path::Path;

use sha2::{Digest as _, Sha256};

/// Identifies an opened endpoint for durable binding without persisting macOS mount numbers.
///
/// macOS uses the volume's persistent UUID and the opened file ID. Other supported platforms
/// retain their existing device/file identity inputs. Callers retain their confined file/root
/// capabilities and bind the canonical path separately; this digest grants no filesystem access.
///
/// # Errors
///
/// Fails when identity cannot be read, a macOS volume has no persistent UUID, or the macOS
/// displayed endpoint no longer names the retained handle. It never substitutes a mount number
/// when persistent volume identity is unavailable.
pub fn persistent_endpoint_identity(file: &File, path: &Path) -> io::Result<[u8; 32]> {
    platform_identity(file, path)
}

#[cfg(target_os = "macos")]
fn platform_identity(file: &File, path: &Path) -> io::Result<[u8; 32]> {
    let opened = MacEndpoint::read(file)?;
    opened.validate_name(file, path)?;
    let volume = macos_volume_uuid(path, opened.directory)?;
    opened.validate_name(file, path)?;
    Ok(opened.persistent_identity(volume))
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Eq, PartialEq)]
struct MacEndpoint {
    device: u64,
    inode: u64,
    directory: bool,
}

#[cfg(target_os = "macos")]
impl MacEndpoint {
    fn read(file: &File) -> io::Result<Self> {
        Self::from_metadata(&file.metadata()?)
    }

    fn from_metadata(metadata: &std::fs::Metadata) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt as _;

        if !metadata.is_file() && !metadata.is_dir() {
            return Err(endpoint_changed());
        }
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            directory: metadata.is_dir(),
        })
    }

    fn validate_name(self, file: &File, path: &Path) -> io::Result<()> {
        // symlink_metadata rejects a final symlink, including one pointing at this same inode.
        // The caller's existing capability owner validates the complete parent path.
        if !path.is_absolute()
            || Self::from_metadata(&std::fs::symlink_metadata(path)?)? != self
            || Self::read(file)? != self
        {
            return Err(endpoint_changed());
        }
        Ok(())
    }

    fn persistent_identity(self, volume: uuid::Uuid) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/persistent-endpoint/macos/v1");
        digest.update(volume.as_bytes());
        digest.update(self.inode.to_be_bytes());
        digest.finalize().into()
    }
}

#[cfg(target_os = "macos")]
fn endpoint_changed() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "retained filesystem endpoint changed",
    )
}

#[cfg(target_os = "macos")]
fn macos_volume_uuid(path: &Path, directory: bool) -> io::Result<uuid::Uuid> {
    use objc2_foundation::{NSArray, NSString, NSURL};

    // Rust worker threads do not own an AppKit autorelease pool. Drain Foundation temporaries
    // here; only owned Rust values leave the scope.
    objc2::rc::autoreleasepool(|_| {
        let unavailable = || {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "persistent volume identity unavailable",
            )
        };
        let url = NSURL::from_path(path, directory, None).ok_or_else(unavailable)?;
        // Apple's Foundation declares this exact raw resource-key value in NSURL.swift.
        // Use the safe dictionary API rather than an unsafe extern-static or output-pointer read.
        // https://github.com/swiftlang/swift-corelibs-foundation/blob/00cf342ad7a43ae7059149a87f49c696c1e50219/Foundation/NSURL.swift
        let key = NSString::from_str("NSURLVolumeUUIDStringKey");
        let values = url
            .resourceValuesForKeys_error(&NSArray::from_slice(&[&*key]))
            .map_err(|_| unavailable())?;
        let value = values.objectForKey(&key).ok_or_else(unavailable)?;
        let value = value.downcast_ref::<NSString>().ok_or_else(unavailable)?;
        let volume = uuid::Uuid::parse_str(&value.to_string()).map_err(|_| unavailable())?;
        if volume.is_nil() {
            return Err(unavailable());
        }
        Ok(volume)
    })
}

#[cfg(all(not(target_os = "macos"), any(unix, windows)))]
fn platform_identity(file: &File, _path: &Path) -> io::Result<[u8; 32]> {
    use cap_fs_ext::MetadataExt as _;

    let metadata = cap_std::fs::File::from_std(file.try_clone()?).metadata()?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/persistent-endpoint/device-file/v1");
    digest.update(metadata.dev().to_be_bytes());
    digest.update(metadata.ino().to_be_bytes());
    Ok(digest.finalize().into())
}

#[cfg(not(any(unix, windows)))]
fn platform_identity(_file: &File, _path: &Path) -> io::Result<[u8; 32]> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem endpoint identity unsupported",
    ))
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::{MacEndpoint, macos_volume_uuid, persistent_endpoint_identity};
    use std::fs::File;

    #[test]
    fn durable_identity_ignores_mount_number_but_rejects_endpoint_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().canonicalize()?.join("catalog");
        let file = File::create(&path)?;
        let opened = MacEndpoint::read(&file)?;
        let volume = macos_volume_uuid(&path, false)?;
        let identity = persistent_endpoint_identity(&file, &path)?;
        let remounted = MacEndpoint {
            device: opened.device ^ 4,
            ..opened
        };
        assert_eq!(identity, remounted.persistent_identity(volume));
        assert_eq!(
            identity,
            persistent_endpoint_identity(&File::open(&path)?, &path)?
        );
        assert_ne!(identity, opened.persistent_identity(uuid::Uuid::new_v4()));

        let displaced = path.with_file_name("displaced");
        std::fs::rename(&path, &displaced)?;
        let replacement = File::create(&path)?;
        assert!(persistent_endpoint_identity(&file, &path).is_err());
        assert_ne!(identity, persistent_endpoint_identity(&replacement, &path)?);
        std::fs::remove_file(&path)?;
        std::os::unix::fs::symlink(&displaced, &path)?;
        assert!(persistent_endpoint_identity(&file, &path).is_err());
        Ok(())
    }
}
