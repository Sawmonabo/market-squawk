//! Local directory layout and capability-confined artifact publication.

mod catalog;
mod decisions;
mod jobs;
mod sqlite;

use std::{
    fmt,
    fs::File,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use thiserror::Error;

use crate::input::{ControlledImportInputRoot, InputFileError};
use crate::journal::ParentDirectorySync;
use crate::{
    JournalError, JournalReader, JournalSinkConstructionError, JournalSinkLimits, JournalWriter,
    SealedResearchJournalStore, SealedResearchJournalStoreError,
};

pub use self::catalog::{
    CatalogFileGuard, CatalogLocation, CatalogRestoreScanGuard, CatalogWriterGuard,
};
pub use self::catalog::{CatalogRestoreStage, CatalogRestoreTarget, InstalledCatalogFile};
pub use self::decisions::{
    DecisionDatabaseFileGuard, DecisionDatabaseLocation, DecisionDatabaseWriterGuard,
};
pub use self::jobs::{JobDatabaseFileGuard, JobDatabaseLocation, JobDatabaseWriterGuard};
use self::sqlite::open_prepared_root;

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
    /// Control-plane storage requires a prepared local path capability.
    #[error("control root is unavailable in read-only path mode")]
    ControlRootUnavailable,
    /// Catalog placement requires a prepared local path capability.
    #[error("catalog location is unavailable in read-only path mode")]
    CatalogLocationUnavailable,
    /// The prepared local root path no longer names the retained directory capability.
    #[error("prepared local root identity changed")]
    PreparedRootChanged,
    /// Another process owns the prepared catalog writer lock.
    #[error("prepared catalog already has an active writer")]
    CatalogAlreadyLocked,
    /// Another process owns the prepared job-database writer lock.
    #[error("prepared job database already has an active writer")]
    JobDatabaseAlreadyLocked,
    /// Another process owns the prepared decision-database writer lock.
    #[error("prepared decision database already has an active writer")]
    DecisionDatabaseAlreadyLocked,
    /// A catalog restore stage or final target contains different immutable bytes.
    #[error("prepared catalog restore target conflicts with retained state")]
    CatalogRestoreConflict,
    /// Catalog restore publication may have reached durable storage and requires exact retry.
    #[error("prepared catalog restore publication is indeterminate")]
    CatalogRestoreIndeterminate,
}

/// Open directory capability for the exact prepared control-plane root.
#[derive(Clone)]
pub struct ControlRoot {
    display_root: PathBuf,
    directory: Arc<Dir>,
}

impl fmt::Debug for ControlRoot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlRoot")
            .field("display_root", &self.display_root)
            .field("directory", &"[DIRECTORY CAPABILITY]")
            .finish()
    }
}

impl ControlRoot {
    fn from_open_directory(display_root: PathBuf, directory: Dir) -> Self {
        Self {
            display_root,
            directory: Arc::new(directory),
        }
    }

    /// Returns the canonical display path; it is not filesystem authority.
    pub fn root(&self) -> &Path {
        &self.display_root
    }

    /// Clones the retained capability after proving its display path still names that directory.
    ///
    /// # Errors
    ///
    /// Returns [`PathError::PreparedRootChanged`] when the displayed directory was renamed or
    /// substituted, or [`PathError::Io`] when capability cloning or identity inspection fails.
    pub fn try_clone_directory(&self) -> Result<Dir, PathError> {
        use cap_fs_ext::MetadataExt as _;

        let directory = self
            .directory
            .try_clone()
            .map_err(|source| PathError::io("failed to clone control root", source))?;
        let retained = directory
            .dir_metadata()
            .map_err(|source| PathError::io("failed to inspect retained control root", source))?;
        let reopened = open_prepared_root(&self.display_root)?;
        let displayed = reopened
            .dir_metadata()
            .map_err(|source| PathError::io("failed to inspect displayed control root", source))?;
        if !retained.is_dir()
            || !displayed.is_dir()
            || (retained.dev(), retained.ino()) != (displayed.dev(), displayed.ino())
        {
            return Err(PathError::PreparedRootChanged);
        }
        Ok(directory)
    }
}

impl PathError {
    fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }
}

/// Invalid artifact reference or confined creation failure.
#[derive(Debug, Error)]
pub enum ArtifactPathError {
    /// The retained prepared root identity changed or could not be revalidated.
    #[error("artifact root identity is unavailable: {0}")]
    Root(#[from] PathError),
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

    /// Clones the retained directory capability after proving its display path still names it.
    ///
    /// The returned directory is derived from the retained handle, never reopened as ambient
    /// authority. The display path check exists only to reject renamed or substituted configured
    /// roots before a path-bound durable identity is accepted.
    ///
    /// # Errors
    ///
    /// Returns [`PathError::PreparedRootChanged`] if the canonical display path no longer names
    /// the retained directory, or [`PathError::Io`] if cloning or identity inspection fails.
    pub fn try_clone_directory(&self) -> Result<Dir, PathError> {
        use cap_fs_ext::MetadataExt as _;

        let directory = self
            .directory
            .try_clone()
            .map_err(|source| PathError::io("failed to clone artifact root", source))?;
        let retained = directory
            .dir_metadata()
            .map_err(|source| PathError::io("failed to inspect retained artifact root", source))?;
        let reopened = open_prepared_root(&self.display_root)?;
        let displayed = reopened
            .dir_metadata()
            .map_err(|source| PathError::io("failed to inspect displayed artifact root", source))?;
        if !retained.is_dir()
            || !displayed.is_dir()
            || (retained.dev(), retained.ino()) != (displayed.dev(), displayed.ino())
        {
            return Err(PathError::PreparedRootChanged);
        }
        Ok(directory)
    }

    /// Opens one existing controlled-import directory beneath this retained artifact root.
    ///
    /// The relative directory chain is bounded and every component is opened relative to the
    /// preceding retained handle with no-follow semantics. The result retains this artifact-root
    /// authority and the exact component identities for revalidation before every file operation.
    /// It is deliberately distinct from a user-authorized input root and cannot issue original
    /// local-ownership evidence.
    ///
    /// # Errors
    ///
    /// Rejects empty, absolute, parent-traversing, over-deep, symlinked, reparsed, replaced, or
    /// non-directory references and path-redacted capability failures.
    pub fn open_controlled_import_root(
        &self,
        relative: impl AsRef<Path>,
    ) -> Result<ControlledImportInputRoot, InputFileError> {
        ControlledImportInputRoot::open_beneath_artifact_root(self, relative.as_ref())
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
    /// Opens an existing exact regular artifact for read through the retained root capability.
    ///
    /// # Errors
    ///
    /// Returns a typed path error when the root identity changed, a link would be followed, or
    /// the resolved entry is not a regular file.
    pub fn open_read(&self) -> Result<std::fs::File, ArtifactPathError> {
        self.root.try_clone_directory()?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = self
            .root
            .directory
            .open_with(&self.relative, &options)
            .map_err(|source| classify_capability_error(&self.relative, source))?
            .into_std();
        let metadata = file
            .metadata()
            .map_err(|source| ArtifactPathError::Io { source })?;
        if !metadata.is_file() {
            return Err(ArtifactPathError::EscapesRoot {
                path: self.relative.clone(),
            });
        }
        self.root.try_clone_directory()?;
        Ok(file)
    }

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
    /// Capability-relative configured reads require a prepared local path layout.
    #[error("configured journal reads require prepared local paths")]
    PreparedPathsRequired,
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

/// Configured journal read target bound to the retained journal-directory capability.
#[derive(Clone)]
pub struct ConfiguredJournalReadTarget {
    directory: Arc<Dir>,
    filename: Arc<str>,
    format: JournalFileFormat,
}

impl fmt::Debug for ConfiguredJournalReadTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguredJournalReadTarget")
            .field("directory", &"[DIRECTORY CAPABILITY]")
            .field("filename", &self.filename)
            .field("format", &self.format)
            .finish()
    }
}

impl ConfiguredJournalReadTarget {
    /// Opens the selected configured journal relative to the retained directory capability.
    ///
    /// A missing default current-format journal is represented explicitly without creating it.
    /// The final endpoint is opened without following links and the exact opened handle must be a
    /// regular non-reparse file.
    ///
    /// # Errors
    ///
    /// Returns [`JournalOpenError::NotRegular`] for links, reparse points, FIFOs, devices, or
    /// other non-regular endpoints, and [`JournalOpenError::Io`] for other open failures.
    pub fn open(&self) -> Result<ConfiguredJournalRead, JournalOpenError> {
        match self.directory.symlink_metadata(self.filename.as_ref()) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(JournalOpenError::NotRegular);
            }
            Ok(_metadata) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ConfiguredJournalRead::Missing);
            }
            Err(source) => {
                return Err(JournalOpenError::io(
                    "failed to inspect configured journal endpoint",
                    source,
                ));
            }
        }

        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        configure_journal_read_open(&mut options);
        let file = match self.directory.open_with(self.filename.as_ref(), &options) {
            Ok(file) => file.into_std(),
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ConfiguredJournalRead::Missing);
            }
            Err(source) if is_unsafe_journal_endpoint_error(&source) => {
                return Err(JournalOpenError::NotRegular);
            }
            Err(source) => {
                return Err(JournalOpenError::io(
                    "failed to open configured journal",
                    source,
                ));
            }
        };
        validate_configured_journal_handle(&file)?;
        Ok(ConfiguredJournalRead::Reader(JournalReader::new(file)))
    }

    /// Selected compatible journal format.
    pub const fn format(&self) -> JournalFileFormat {
        self.format
    }
}

/// Explicit configured journal read outcome.
#[derive(Debug)]
pub enum ConfiguredJournalRead {
    /// No journal exists at the selected default target.
    Missing,
    /// Exact regular opened journal reader.
    Reader(JournalReader<File>),
}

/// Capability-relative configured journal open failure.
#[derive(Debug, Error)]
pub enum JournalOpenError {
    /// The configured endpoint is not an exact regular non-reparse file.
    #[error("configured journal endpoint is not a regular file")]
    NotRegular,
    /// Capability-relative inspection or opening failed.
    #[error("{context}: {source}")]
    Io {
        /// Non-secret operation context.
        context: &'static str,
        /// Underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
}

impl JournalOpenError {
    fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }
}

/// Prepared local layout or read-only no-create view.
#[derive(Clone, Debug)]
pub struct LocalPaths {
    root: PathBuf,
    journal_dir: PathBuf,
    control: Option<ControlRoot>,
    artifacts: Option<ArtifactRoot>,
    catalog: Option<CatalogLocation>,
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
        let root_capability = Arc::new(open_prepared_root(root)?);
        root_capability
            .create_dir_all("journal")
            .map_err(|source| PathError::io("failed to create journal directory", source))?;
        root_capability
            .create_dir_all("artifacts")
            .map_err(|source| PathError::io("failed to create artifact directory", source))?;
        root_capability
            .create_dir_all("control")
            .map_err(|source| PathError::io("failed to create control directory", source))?;
        let journal_capability = root_capability.open_dir("journal").map_err(|source| {
            PathError::io("failed to open journal directory capability", source)
        })?;
        let artifact_capability = root_capability.open_dir("artifacts").map_err(|source| {
            PathError::io("failed to open artifact directory capability", source)
        })?;
        let control_capability = root_capability.open_dir("control").map_err(|source| {
            PathError::io("failed to open control directory capability", source)
        })?;
        let root = std::fs::canonicalize(root)
            .map_err(|source| PathError::io("failed to canonicalize local data root", source))?;
        let journal_dir = root.join("journal");
        let artifacts =
            ArtifactRoot::from_open_directory(root.join("artifacts"), artifact_capability);
        let control = ControlRoot::from_open_directory(root.join("control"), control_capability);
        let catalog = CatalogLocation::from_prepared(root.clone(), Arc::clone(&root_capability));
        Ok(Self {
            root,
            journal_dir,
            control: Some(control),
            artifacts: Some(artifacts),
            catalog: Some(catalog),
            journal_capability: Some(Arc::new(journal_capability)),
        })
    }

    /// Opens an already prepared local layout without creating or modifying any entry.
    pub fn open_existing(root: impl AsRef<Path>) -> Result<Self, PathError> {
        let root = root.as_ref();
        let root_capability = Arc::new(open_prepared_root(root)?);
        let journal_capability = root_capability.open_dir("journal").map_err(|source| {
            PathError::io(
                "failed to open existing journal directory capability",
                source,
            )
        })?;
        let artifact_capability = root_capability.open_dir("artifacts").map_err(|source| {
            PathError::io(
                "failed to open existing artifact directory capability",
                source,
            )
        })?;
        let control_capability = root_capability.open_dir("control").map_err(|source| {
            PathError::io(
                "failed to open existing control directory capability",
                source,
            )
        })?;
        let root = std::fs::canonicalize(root)
            .map_err(|source| PathError::io("failed to canonicalize local data root", source))?;
        let journal_dir = root.join("journal");
        let artifacts =
            ArtifactRoot::from_open_directory(root.join("artifacts"), artifact_capability);
        let control = ControlRoot::from_open_directory(root.join("control"), control_capability);
        let catalog = CatalogLocation::from_prepared(root.clone(), Arc::clone(&root_capability));
        Ok(Self {
            root,
            journal_dir,
            control: Some(control),
            artifacts: Some(artifacts),
            catalog: Some(catalog),
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
            control: None,
            artifacts: None,
            catalog: None,
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

    /// Returns the exact prepared control-plane directory capability.
    pub fn control_root(&self) -> Result<&ControlRoot, PathError> {
        self.control
            .as_ref()
            .ok_or(PathError::ControlRootUnavailable)
    }

    /// Returns the controlled artifact root capability in prepared mode.
    pub fn artifacts(&self) -> Result<&ArtifactRoot, PathError> {
        self.artifacts
            .as_ref()
            .ok_or(PathError::ArtifactRootUnavailable)
    }

    /// Returns the catalog placement capability in prepared mode.
    pub fn catalog(&self) -> Result<&CatalogLocation, PathError> {
        self.catalog
            .as_ref()
            .ok_or(PathError::CatalogLocationUnavailable)
    }

    /// Returns the current-format journal path for a validated source filename.
    pub fn journal_write_file(&self, source: &str) -> Result<PathBuf, JournalSelectionError> {
        self.journal_path(source, JournalFileFormat::Current)
    }

    /// Opens a current journal through the prepared directory capability, then locks and
    /// validates that exact file handle before append.
    pub fn open_journal_writer(
        &self,
        source: &str,
    ) -> Result<JournalWriter, JournalSinkConstructionError> {
        self.open_journal_writer_with_limits(source, JournalSinkLimits::standard())
    }

    /// Opens the single-owner sealed research-segment authority under the prepared journal root.
    ///
    /// The returned store retains an exclusive cross-process owner lock. Construct it once during
    /// application composition and share that owner; a live append journal remains a separate,
    /// non-authoritative diagnostic sink.
    pub fn sealed_research_journal_store(
        &self,
    ) -> Result<SealedResearchJournalStore, SealedResearchJournalStoreError> {
        let directory = self
            .journal_capability
            .as_ref()
            .ok_or(SealedResearchJournalStoreError::PreparedCapabilityRequired)?;
        SealedResearchJournalStore::try_from_journal_directory(Arc::clone(directory))
    }

    /// Opens a current journal under explicit, separate fixed sink limits.
    ///
    /// # Errors
    ///
    /// Returns a typed fixed-storage refusal or the existing confined journal-path/lock error.
    pub fn open_journal_writer_with_limits(
        &self,
        source: &str,
        limits: JournalSinkLimits,
    ) -> Result<JournalWriter, JournalSinkConstructionError> {
        validate_source_filename(source).map_err(|_error| JournalError::InvalidSourceFilename)?;
        let capability = self
            .journal_capability
            .as_ref()
            .ok_or(JournalError::InvalidWriterExtension)?;
        let filename = format!("{source}.msj");
        match capability.symlink_metadata(&filename) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(JournalError::SymlinkNotAllowed.into());
            }
            Ok(_metadata) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(JournalError::io(
                    "failed to inspect confined journal endpoint",
                    source,
                )
                .into());
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
        JournalWriter::validate_limits_for_path(&path, limits)?;
        let file = match capability.open_with(&filename, &options) {
            Ok(file) => file.into_std(),
            #[cfg(windows)]
            Err(source) if is_windows_writer_contention(&source) => {
                return Err(JournalError::AlreadyLocked { path }.into());
            }
            Err(source) => {
                return Err(JournalError::io("failed to open confined journal", source).into());
            }
        };
        #[cfg(windows)]
        validate_windows_journal_handle(&file)?;
        let parent_directory = open_parent_directory_sync(capability)?;
        JournalWriter::from_open_file(path, file, parent_directory, limits)
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

    /// Selects a configured journal filename under the retained prepared directory capability.
    ///
    /// With no explicit format, a missing current and legacy journal binds a missing current
    /// target without creating it. Explicitly selected missing formats remain selection errors.
    ///
    /// # Errors
    ///
    /// Returns [`JournalSelectionError::PreparedPathsRequired`] for a read-only ambient path view,
    /// or the existing validation, ambiguity, and explicit-missing selection errors.
    pub fn configured_journal_read_target(
        &self,
        source: &str,
        requested: Option<JournalFileFormat>,
    ) -> Result<ConfiguredJournalReadTarget, JournalSelectionError> {
        validate_source_filename(source)?;
        let directory = self
            .journal_capability
            .as_ref()
            .ok_or(JournalSelectionError::PreparedPathsRequired)?;
        let current_filename = journal_filename(source, JournalFileFormat::Current);
        let legacy_filename = journal_filename(source, JournalFileFormat::Legacy);
        let format = if let Some(format) = requested {
            let filename = journal_filename(source, format);
            if !capability_entry_exists(directory, &filename)? {
                return Err(JournalSelectionError::SelectedFormatNotFound {
                    format,
                    path: self.journal_dir.join(filename),
                });
            }
            format
        } else {
            match (
                capability_entry_exists(directory, &current_filename)?,
                capability_entry_exists(directory, &legacy_filename)?,
            ) {
                (true, true) => {
                    return Err(JournalSelectionError::Ambiguous {
                        current: self.journal_dir.join(current_filename),
                        legacy: self.journal_dir.join(legacy_filename),
                    });
                }
                (true, false) | (false, false) => JournalFileFormat::Current,
                (false, true) => JournalFileFormat::Legacy,
            }
        };
        Ok(ConfiguredJournalReadTarget {
            directory: Arc::clone(directory),
            filename: Arc::from(journal_filename(source, format)),
            format,
        })
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

fn journal_filename(source: &str, format: JournalFileFormat) -> String {
    format!("{source}.{}", format.extension())
}

fn capability_entry_exists(directory: &Dir, filename: &str) -> Result<bool, JournalSelectionError> {
    match directory.symlink_metadata(filename) {
        Ok(_metadata) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(JournalSelectionError::Io {
            path: PathBuf::from(filename),
            source,
        }),
    }
}

#[cfg(unix)]
fn configure_journal_read_open(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    options.custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC);
}

#[cfg(windows)]
fn configure_journal_read_open(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;

    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    options.share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_journal_read_open(_options: &mut OpenOptions) {}

fn validate_configured_journal_handle(file: &File) -> Result<(), JournalOpenError> {
    let metadata = file.metadata().map_err(|source| {
        JournalOpenError::io("failed to inspect configured journal handle", source)
    })?;
    if !metadata.is_file() || is_windows_reparse_file(&metadata) {
        return Err(JournalOpenError::NotRegular);
    }
    Ok(())
}

#[cfg(windows)]
fn is_windows_reparse_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt as _;

    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
const fn is_windows_reparse_file(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn is_unsafe_journal_endpoint_error(source: &std::io::Error) -> bool {
    source.raw_os_error() == Some(libc::ELOOP)
}

#[cfg(not(unix))]
const fn is_unsafe_journal_endpoint_error(_source: &std::io::Error) -> bool {
    false
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
