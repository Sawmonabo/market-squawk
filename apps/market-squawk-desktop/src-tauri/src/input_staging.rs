//! Native-only file selection and evidence-bound service staging.

use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt, ambient_authority};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_runtime::{ApplicationClient, InputAdmission, InputTicket};
use sha2::{Digest as _, Sha256};
use tauri::{AppHandle, State};
use tauri_plugin_dialog::DialogExt as _;

use crate::{
    bridge::DesktopState,
    contracts::{DesktopCommandError, TrainingInputKind},
};

const MAXIMUM_TRAINING_CONFIG_BYTES: u64 = 256 * 1024;
const MAXIMUM_MODEL_AUTHORITY_BYTES: u64 = 4 * 1024 * 1024;

#[tauri::command]
pub(crate) async fn stage_training_input(
    kind: TrainingInputKind,
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<Option<InputTicket>, DesktopCommandError> {
    let selected = tauri::async_runtime::spawn_blocking(move || {
        app.dialog()
            .file()
            .add_filter(kind.dialog_label(), &["json"])
            .blocking_pick_file()
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = selected.into_path().map_err(|_error| {
        DesktopCommandError::invalid_request("The selected input is not a local file.")
    })?;
    let admitted = tauri::async_runtime::spawn_blocking(move || open_and_hash(kind, path))
        .await
        .map_err(|_error| DesktopCommandError::internal())??;
    let admission = InputAdmission::try_new(
        SourceIdentifier::try_from(kind.media_type())
            .map_err(|_error| DesktopCommandError::internal())?,
        admitted.byte_length,
        admitted.digest,
    )
    .map_err(|_error| DesktopCommandError::internal())?;
    let mut input = tokio::fs::File::from_std(admitted.file);
    state
        .application()
        .stage_input(admission, &mut input, state.cancellation())
        .await
        .map(Some)
        .map_err(super::bridge::map_application_client_error)
}

struct AdmittedFile {
    file: std::fs::File,
    byte_length: u64,
    digest: EvidenceDigest,
}

fn open_and_hash(
    kind: TrainingInputKind,
    path: PathBuf,
) -> Result<AdmittedFile, DesktopCommandError> {
    validate_extension(&path)?;
    let parent = path.parent().ok_or_else(|| {
        DesktopCommandError::invalid_request("The selected input path is invalid.")
    })?;
    let name = path.file_name().ok_or_else(|| {
        DesktopCommandError::invalid_request("The selected input path is invalid.")
    })?;
    let directory = Dir::open_ambient_dir(parent, ambient_authority()).map_err(map_file_error)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = directory
        .open_with(Path::new(name), &options)
        .map(cap_std::fs::File::into_std)
        .map_err(map_file_error)?;
    let metadata = file.metadata().map_err(map_file_error)?;
    let byte_length = metadata.len();
    if !metadata.is_file() || byte_length == 0 || byte_length > kind.maximum_bytes() {
        return Err(DesktopCommandError::invalid_request(
            "The selected input is empty, too large, or not a regular file.",
        ));
    }
    let mut hasher = Sha256::new();
    let copied = std::io::copy(&mut file, &mut hasher).map_err(map_file_error)?;
    if copied != byte_length {
        return Err(DesktopCommandError::new(
            "input_changed",
            "The selected input changed while Market Squawk was reading it. Select it again.",
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(map_file_error)?;
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(AdmittedFile {
        file,
        byte_length,
        digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
    })
}

fn validate_extension(path: &Path) -> Result<(), DesktopCommandError> {
    let valid = path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"));
    if valid {
        Ok(())
    } else {
        Err(DesktopCommandError::invalid_request(
            "Select a JSON file for this training input.",
        ))
    }
}

fn map_file_error(_error: std::io::Error) -> DesktopCommandError {
    DesktopCommandError::new(
        "input_unavailable",
        "Market Squawk could not safely open the selected input.",
    )
}

impl TrainingInputKind {
    const fn dialog_label(self) -> &'static str {
        match self {
            Self::Configuration => "Training configuration",
            Self::ModelAuthority => "Model authority",
        }
    }

    const fn media_type(self) -> &'static str {
        match self {
            Self::Configuration => "market-squawk.training-config.v1",
            Self::ModelAuthority => "market-squawk.model-authority.v1",
        }
    }

    const fn maximum_bytes(self) -> u64 {
        match self {
            Self::Configuration => MAXIMUM_TRAINING_CONFIG_BYTES,
            Self::ModelAuthority => MAXIMUM_MODEL_AUTHORITY_BYTES,
        }
    }
}
