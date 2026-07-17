//! Local directory layout and capability-confined artifact publication.

use std::{
    fmt,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use thiserror::Error;

use crate::journal::ParentDirectorySync;
use crate::{JournalError, JournalWriter};

const MAX_ARTIFACT_COMPONENT_BYTES: usize = 255;
const MAX_ARTIFACT_DEPTH: usize = 32;
const MAX_SOURCE_FILENAME_BYTES: usize = 128;

/// Local-path setup failure.
#[derive(Debug, Error)]
pub enum PathError {
    /// A required filesystem operation failed.
    #[error("{context}: {source}")]
    Io {
        /// Non-secret operation context.
        context: &'static str,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The configured root or parent is explicitly read-only.
    #[error("configured local data path is read-only")]
    ReadOnly,
    /// Artifact publication was requested from a read-only/no-create path view.
    #[error("artifact root is unavailable in read-only path mode")]
    ArtifactRootUnavailable,
}

impl PathError {
    fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }
}

/// Invalid artifact reference or confined creation failure.
#[derive(Debug, Error)]
pub enum ArtifactPathError {
    /// Artifact references must be relative to the controlled root.
    #[error("artifact path must be relative: {path}")]
    AbsolutePath {
        /// Rejected path.
        path: PathBuf,
    },
    /// Empty, noncanonical, non-portable, reserved, or oversized components are rejected.
    #[error("artifact path contains an unsafe component: {component}")]
    UnsafeComponent {
        /// Rejected lossy component for diagnostics.
        component: String,
    },
    /// Controlled artifact references are UTF-8 portable names.
    #[error("artifact path contains a non-UTF-8 component")]
    NonUtf8Component,
    /// A capability-relative operation detected an escape through a symlink or rename.
    #[error("artifact path escapes the controlled root: {path}")]
    EscapesRoot {
        /// Rejected relative path.
        path: PathBuf,
    },
    /// Capability-relative creation failed without exposing ambient paths.
    #[error("artifact creation failed: {source}")]
    Io {
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
}

/// Open directory capability for the controlled artifact root.
#[derive(Clone)]
pub struct ArtifactRoot {
    display_root: PathBuf,
    directory: Arc<Dir>,
}

impl fmt::Debug for ArtifactRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactRoot")
            .field("display_root", &self.display_root)
            .field("directory", &"[DIRECTORY CAPABILITY]")
            .finish()
    }
}

impl ArtifactRoot {
    fn from_open_directory(display_root: PathBuf, directory: Dir) -> Self {
        Self {
            display_root,
            directory: Arc::new(directory),
        }
    }

    /// Returns the canonical display path; it is not used as the creation authority.
    pub fn root(&self) -> &Path {
        &self.display_root
    }

    /// Validates a canonical portable reference and binds it to this open directory capability.
    ///
    /// References contain at most 32 `/`-separated UTF-8 components. Each component contains at
    /// most 255 bytes, starts with an ASCII lowercase letter or digit, continues with only ASCII
    /// lowercase letters, digits, `-`, `_`, or `.`, and does not end in `.`. Empty, `.`, `..`,
    /// repeated-separator, trailing-separator, alternate-separator, Windows device-name, and
    /// platform-reinterpreted inputs are rejected rather than normalized into aliases.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactPathError::AbsolutePath`] for an absolute host path,
    /// [`ArtifactPathError::NonUtf8Component`] when the complete reference is not UTF-8, and
    /// [`ArtifactPathError::UnsafeComponent`] when the reference violates the canonical grammar,
    /// its component or depth bounds, or the host platform would parse it differently.
    pub fn resolve(
        &self,
        relative: impl AsRef<Path>,
    ) -> Result<ResolvedArtifactPath, ArtifactPathError> {
        let relative = relative.as_ref();
        validate_artifact_reference(relative)?;
        Ok(ResolvedArtifactPath {
            root: self.clone(),
            relative: relative.to_path_buf(),
        })
    }
}

/// Validated artifact reference retaining its open root capability.
#[derive(Clone, Debug)]
pub struct ResolvedArtifactPath {
    root: ArtifactRoot,
    relative: PathBuf,
}

impl ResolvedArtifactPath {
    /// Creates a new immutable artifact through the retained directory capability.
    ///
    /// Intermediate traversal and final creation remain relative to the open root directory, so
    /// an ambient ancestor rename or symlink substitution cannot redirect creation outside it.
    /// Existing targets are never overwritten.
    pub fn create_new(&self) -> Result<cap_std::fs::File, ArtifactPathError> {
        if let Some(parent) = self.relative.parent()
            && !parent.as_os_str().is_empty()
        {
            self.root
                .directory
                .create_dir_all(parent)
                .map_err(|source| classify_capability_error(&self.relative, source))?;
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        self.root
            .directory
            .open_with(&self.relative, &options)
            .map_err(|source| classify_capability_error(&self.relative, source))
    }

    /// Returns the portable relative artifact reference.
    pub fn relative(&self) -> &Path {
        &self.relative
    }
}

fn classify_capability_error(path: &Path, source: std::io::Error) -> ArtifactPathError {
    if matches!(
        source.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::NotADirectory
    ) {
        ArtifactPathError::EscapesRoot {
            path: path.to_path_buf(),
        }
    } else {
        ArtifactPathError::Io { source }
    }
}

fn validate_artifact_reference(path: &Path) -> Result<(), ArtifactPathError> {
    if path.is_absolute() {
        return Err(ArtifactPathError::AbsolutePath {
            path: PathBuf::from("[ABSOLUTE PATH REDACTED]"),
        });
    }
    let portable_path = path.to_str().ok_or(ArtifactPathError::NonUtf8Component)?;
    if portable_path.is_empty() {
        return Err(ArtifactPathError::UnsafeComponent {
            component: "[EMPTY]".to_owned(),
        });
    }
    let mut depth = 0_usize;
    let mut platform_components = path.components();
    for portable_component in portable_path.split('/') {
        depth = depth.saturating_add(1);
        if depth > MAX_ARTIFACT_DEPTH {
            return Err(ArtifactPathError::UnsafeComponent {
                component: "[PATH TOO DEEP]".to_owned(),
            });
        }
        if !is_portable_artifact_component(portable_component) {
            return Err(ArtifactPathError::UnsafeComponent {
                component: "[UNSAFE COMPONENT]".to_owned(),
            });
        }
        if !matches!(
            platform_components.next(),
            Some(Component::Normal(component)) if component.to_str() == Some(portable_component)
        ) {
            return Err(ArtifactPathError::UnsafeComponent {
                component: "[PLATFORM PATH NORMALIZATION]".to_owned(),
            });
        }
    }
    if platform_components.next().is_some() {
        return Err(ArtifactPathError::UnsafeComponent {
            component: "[PLATFORM PATH NORMALIZATION]".to_owned(),
        });
    }
    Ok(())
}

fn is_portable_artifact_component(component: &str) -> bool {
    let mut characters = component.chars();
    let Some(first) = characters.next() else {
        return false;
    };
    component.len() <= MAX_ARTIFACT_COMPONENT_BYTES
        && (first.is_ascii_lowercase() || first.is_ascii_digit())
        && characters.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '-' | '_' | '.')
        })
        && !component.ends_with('.')
        && !is_windows_reserved_name(component)
}

fn is_windows_reserved_name(component: &str) -> bool {
    let base = component.split('.').next().unwrap_or_default();
    let upper = base.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || upper
            .strip_prefix("COM")
            .or_else(|| upper.strip_prefix("LPT"))
            .is_some_and(|suffix| {
                matches!(
                    suffix,
                    "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
                )
            })
}

/// Current and legacy journal filename formats.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalFileFormat {
    /// Current `MSJ1/.msj` format.
    Current,
    /// Legacy `MEJ1/.mej` format.
    Legacy,
}

impl JournalFileFormat {
    const fn extension(self) -> &'static str {
        match self {
            Self::Current => "msj",
            Self::Legacy => "mej",
        }
    }
}

/// Journal selection failure.
#[derive(Debug, Error)]
pub enum JournalSelectionError {
    /// Both formats exist and the caller did not explicitly choose one.
    #[error(
        "journal selection is ambiguous because current and legacy journals both exist; choose \
         one with --journal-format current or --journal-format legacy"
    )]
    Ambiguous {
        /// Current path.
        current: PathBuf,
        /// Legacy path.
        legacy: PathBuf,
    },
    /// An explicitly selected file does not exist.
    #[error("selected journal format {format:?} does not exist")]
    SelectedFormatNotFound {
        /// Requested format.
        format: JournalFileFormat,
        /// Missing path.
        path: PathBuf,
    },
    /// Source text cannot safely become one filename component.
    #[error("journal source filename is invalid")]
    InvalidSource,
    /// Read-only inspection failed.
    #[error("failed to inspect journal: {source}")]
    Io {
        /// Inspected path.
        path: PathBuf,
        /// Underlying failure.
        #[source]
        source: std::io::Error,
    },
}

/// Prepared local layout or read-only no-create view.
#[derive(Clone, Debug)]
pub struct LocalPaths {
    root: PathBuf,
    journal_dir: PathBuf,
    artifacts: Option<ArtifactRoot>,
    journal_capability: Option<Arc<Dir>>,
}

impl LocalPaths {
    /// Creates the controlled local layout and opens the artifact directory capability.
    pub fn prepare(root: impl AsRef<Path>) -> Result<Self, PathError> {
        let root = root.as_ref();
        reject_explicitly_read_only_existing_parent(root)?;
        std::fs::create_dir_all(root)
            .map_err(|source| PathError::io("failed to create local data root", source))?;
        reject_explicitly_read_only(root)?;
        let root_capability = Dir::open_ambient_dir(root, ambient_authority())
            .map_err(|source| PathError::io("failed to open local data root capability", source))?;
        root_capability
            .create_dir_all("journal")
            .map_err(|source| PathError::io("failed to create journal directory", source))?;
        root_capability
            .create_dir_all("artifacts")
            .map_err(|source| PathError::io("failed to create artifact directory", source))?;
        let journal_capability = root_capability.open_dir("journal").map_err(|source| {
            PathError::io("failed to open journal directory capability", source)
        })?;
        let artifact_capability = root_capability.open_dir("artifacts").map_err(|source| {
            PathError::io("failed to open artifact directory capability", source)
        })?;
        let root = std::fs::canonicalize(root)
            .map_err(|source| PathError::io("failed to canonicalize local data root", source))?;
        let journal_dir = root.join("journal");
        let artifacts =
            ArtifactRoot::from_open_directory(root.join("artifacts"), artifact_capability);
        Ok(Self {
            root,
            journal_dir,
            artifacts: Some(artifacts),
            journal_capability: Some(Arc::new(journal_capability)),
        })
    }

    /// Constructs a no-create view for replay and offline selection.
    pub fn for_read(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let journal_dir = root.join("journal");
        Self {
            root,
            journal_dir,
            artifacts: None,
            journal_capability: None,
        }
    }

    /// Returns the local data root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the journal directory.
    pub fn journal_dir(&self) -> &Path {
        &self.journal_dir
    }

    /// Returns the controlled artifact root capability in prepared mode.
    pub fn artifacts(&self) -> Result<&ArtifactRoot, PathError> {
        self.artifacts
            .as_ref()
            .ok_or(PathError::ArtifactRootUnavailable)
    }

    /// Returns the current-format journal path for a validated source filename.
    pub fn journal_write_file(&self, source: &str) -> Result<PathBuf, JournalSelectionError> {
        self.journal_path(source, JournalFileFormat::Current)
    }

    /// Opens a current journal through the prepared directory capability, then locks and
    /// validates that exact file handle before append.
    pub fn open_journal_writer(&self, source: &str) -> Result<JournalWriter, JournalError> {
        validate_source_filename(source).map_err(|_error| JournalError::InvalidSourceFilename)?;
        let capability = self
            .journal_capability
            .as_ref()
            .ok_or(JournalError::InvalidWriterExtension)?;
        let filename = format!("{source}.msj");
        match capability.symlink_metadata(&filename) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(JournalError::SymlinkNotAllowed);
            }
            Ok(_metadata) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(JournalError::io(
                    "failed to inspect confined journal endpoint",
                    source,
                ));
            }
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        #[cfg(unix)]
        {
            use cap_std::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        #[cfg(windows)]
        {
            use cap_std::fs::OpenOptionsExt as _;

            // Windows byte-range exclusive locks deny readers as well as writers. Deny only
            // competing write opens at handle creation so diagnostic readers can consume already
            // committed journal bytes while the single writer remains active. Opening the reparse
            // point itself lets the handle-derived validation below reject it without following.
            const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
            const FILE_SHARE_READ: u32 = 0x0000_0001;
            options.share_mode(FILE_SHARE_READ);
            options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        }
        let path = self.journal_dir.join(&filename);
        let file = match capability.open_with(&filename, &options) {
            Ok(file) => file.into_std(),
            #[cfg(windows)]
            Err(source) if is_windows_writer_contention(&source) => {
                return Err(JournalError::AlreadyLocked { path });
            }
            Err(source) => {
                return Err(JournalError::io("failed to open confined journal", source));
            }
        };
        #[cfg(windows)]
        validate_windows_journal_handle(&file)?;
        let parent_directory = open_parent_directory_sync(capability)?;
        JournalWriter::from_open_file(path, file, parent_directory)
    }

    /// Returns an initialization path unless doing so would shadow a sole legacy journal.
    pub fn journal_initialization_file(
        &self,
        source: &str,
    ) -> Result<Option<PathBuf>, JournalSelectionError> {
        let legacy = self.journal_path(source, JournalFileFormat::Legacy)?;
        let current = self.journal_write_file(source)?;
        let legacy_exists = try_exists(&legacy)?;
        let current_exists = try_exists(&current)?;
        Ok((!legacy_exists || current_exists).then_some(current))
    }

    /// Selects one journal for read without creating directories or files.
    pub fn select_journal_for_read(
        &self,
        source: &str,
        requested: Option<JournalFileFormat>,
    ) -> Result<PathBuf, JournalSelectionError> {
        let current = self.journal_path(source, JournalFileFormat::Current)?;
        let legacy = self.journal_path(source, JournalFileFormat::Legacy)?;
        if let Some(format) = requested {
            let path = self.journal_path(source, format)?;
            return if try_exists(&path)? {
                Ok(path)
            } else {
                Err(JournalSelectionError::SelectedFormatNotFound { format, path })
            };
        }
        match (try_exists(&current)?, try_exists(&legacy)?) {
            (true, true) => Err(JournalSelectionError::Ambiguous { current, legacy }),
            (true, false) | (false, false) => Ok(current),
            (false, true) => Ok(legacy),
        }
    }

    /// Returns the local control-plane state file.
    pub fn state_file(&self) -> PathBuf {
        self.root.join("state.json")
    }

    fn journal_path(
        &self,
        source: &str,
        format: JournalFileFormat,
    ) -> Result<PathBuf, JournalSelectionError> {
        validate_source_filename(source)?;
        Ok(self
            .journal_dir
            .join(format!("{source}.{}", format.extension())))
    }
}

#[cfg(windows)]
fn is_windows_writer_contention(source: &std::io::Error) -> bool {
    // ERROR_SHARING_VIOLATION is produced by the atomic deny-write share contract. Retain
    // ERROR_LOCK_VIOLATION compatibility for a file held by an older byte-range-locking process.
    matches!(source.raw_os_error(), Some(32 | 33))
}

#[cfg(windows)]
fn validate_windows_journal_handle(file: &std::fs::File) -> Result<(), JournalError> {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let metadata = file
        .metadata()
        .map_err(|source| JournalError::io("failed to inspect opened journal handle", source))?;
    if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(JournalError::SymlinkNotAllowed);
    }
    Ok(())
}

#[cfg(unix)]
fn open_parent_directory_sync(directory: &Dir) -> Result<ParentDirectorySync, JournalError> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let handle = directory
        .open_with(".", &options)
        .map_err(|source| {
            JournalError::io("failed to open syncable journal directory handle", source)
        })?
        .into_std();
    Ok(ParentDirectorySync::required(handle))
}

#[cfg(windows)]
fn open_parent_directory_sync(_directory: &Dir) -> Result<ParentDirectorySync, JournalError> {
    Ok(ParentDirectorySync::file_sync_is_authoritative())
}

#[cfg(not(any(unix, windows)))]
fn open_parent_directory_sync(_directory: &Dir) -> Result<ParentDirectorySync, JournalError> {
    Err(JournalError::DirectoryDurabilityUnsupported)
}

fn validate_source_filename(source: &str) -> Result<(), JournalSelectionError> {
    if source.is_empty()
        || source.len() > MAX_SOURCE_FILENAME_BYTES
        || source == "."
        || source == ".."
        || !source
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(JournalSelectionError::InvalidSource);
    }
    Ok(())
}

fn try_exists(path: &Path) -> Result<bool, JournalSelectionError> {
    path.try_exists()
        .map_err(|source| JournalSelectionError::Io {
            path: path.to_path_buf(),
            source,
        })
}

#[cfg(unix)]
fn reject_explicitly_read_only(path: &Path) -> Result<(), PathError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .map_err(|source| PathError::io("failed to inspect local data permissions", source))?
        .permissions()
        .mode();
    if mode & 0o222 == 0 {
        Err(PathError::ReadOnly)
    } else {
        Ok(())
    }
}

#[cfg(not(unix))]
fn reject_explicitly_read_only(path: &Path) -> Result<(), PathError> {
    let readonly = std::fs::metadata(path)
        .map_err(|source| PathError::io("failed to inspect local data permissions", source))?
        .permissions()
        .readonly();
    if readonly {
        Err(PathError::ReadOnly)
    } else {
        Ok(())
    }
}

fn reject_explicitly_read_only_existing_parent(path: &Path) -> Result<(), PathError> {
    let candidate = path
        .ancestors()
        .find(|candidate| !candidate.as_os_str().is_empty() && candidate.exists())
        .unwrap_or_else(|| Path::new("."));
    reject_explicitly_read_only(candidate)
}
