//! Native-only file selection and evidence-bound service staging.

use std::{
    ffi::OsStr,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt, ambient_authority};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_runtime::{ApplicationClient, InputAdmission, InputTicket};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use tauri::{AppHandle, State, WebviewWindow};
use tauri_plugin_dialog::DialogExt as _;

use crate::{
    bridge::{
        DesktopGeneration, DesktopState, InvocationAuthority, invoke_application,
        invoke_private_application,
    },
    contracts::{ApplicationInvocation, DesktopCommandError, TrainingInputKind},
};

const MAXIMUM_TRAINING_CONFIG_BYTES: u64 = 256 * 1024;
const MAXIMUM_MODEL_AUTHORITY_BYTES: u64 = 4 * 1024 * 1024;
const MAXIMUM_BACKTEST_REGISTRATION_BYTES: u64 = 1024 * 1024;
const MAXIMUM_PORTFOLIO_IMPORT_BYTES: u64 = 8 * 1024 * 1024;
const MAXIMUM_RESEARCH_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_PROVIDER_CREDENTIAL_BUNDLE_BYTES: u64 = 64 * 1024;
const PORTFOLIO_IMPORT_MEDIA_TYPE: &str = "market-squawk.portfolio-extraction-batch.v1";
const RESEARCH_FILE_MEDIA_TYPE: &str = "market-squawk.research-source-file.v1";
const PROVIDER_CREDENTIAL_BUNDLE_MEDIA_TYPE: &str = "market-squawk.provider-credentials.v1";
const PROVIDER_CREDENTIAL_BUNDLE_SCHEMA: &str = "market-squawk-provider-credentials/v1";
const IMPORT_PROVIDER_CREDENTIAL_BUNDLE: &str = "Source.ImportCredentialBundle";
const PREVIEW_PORTFOLIO_IMPORT: &str = "Portfolio.PreviewStagedImport";
const APPROVE_PORTFOLIO_IMPORT: &str = "Portfolio.ApproveStagedImport";
const COMMIT_PORTFOLIO_IMPORT: &str = "Portfolio.CommitStagedImport";
const DISCARD_PORTFOLIO_IMPORT: &str = "Portfolio.DiscardStagedImport";
const PREVIEW_RESEARCH_FILE: &str = "Research.PreviewStagedFile";
const COMMIT_RESEARCH_FILE: &str = "Research.CommitStagedFile";
const DISCARD_RESEARCH_FILE: &str = "Research.DiscardStagedFile";

const PROVIDER_CREDENTIAL_BUNDLE_PROVIDERS: [ProviderCredentialBundleProvider; 17] = [
    ProviderCredentialBundleProvider::Schwab,
    ProviderCredentialBundleProvider::Alpaca,
    ProviderCredentialBundleProvider::YahooFinanceExperimental,
    ProviderCredentialBundleProvider::NasdaqTraderReference,
    ProviderCredentialBundleProvider::OccOptionsReference,
    ProviderCredentialBundleProvider::CboeOptionsReference,
    ProviderCredentialBundleProvider::IexHist,
    ProviderCredentialBundleProvider::Bls,
    ProviderCredentialBundleProvider::Bea,
    ProviderCredentialBundleProvider::Census,
    ProviderCredentialBundleProvider::Eia,
    ProviderCredentialBundleProvider::FredAlfred,
    ProviderCredentialBundleProvider::Tiingo,
    ProviderCredentialBundleProvider::Sec,
    ProviderCredentialBundleProvider::TreasuryFiscalData,
    ProviderCredentialBundleProvider::TreasuryDailyRates,
    ProviderCredentialBundleProvider::FederalReserveBoardDirect,
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProviderCredentialBundleProvider {
    Schwab,
    Alpaca,
    YahooFinanceExperimental,
    NasdaqTraderReference,
    OccOptionsReference,
    CboeOptionsReference,
    IexHist,
    Bls,
    Bea,
    Census,
    Eia,
    FredAlfred,
    Tiingo,
    Sec,
    TreasuryFiscalData,
    TreasuryDailyRates,
    FederalReserveBoardDirect,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProviderCredentialImportDisposition {
    CredentialStoredUnverified,
    ProbeRequired,
    Disabled,
    ProfileUnavailable,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderCredentialImportProvider {
    provider: ProviderCredentialBundleProvider,
    enabled: bool,
    disposition: ProviderCredentialImportDisposition,
    onboarding_session_id: Option<uuid::Uuid>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderCredentialImportResult {
    schema: String,
    providers: [ProviderCredentialImportProvider; 17],
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ResearchFileFormat {
    Csv,
    Json,
    Ndjson,
    Parquet,
}

impl ResearchFileFormat {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    const fn extensions(self) -> &'static [&'static str] {
        match self {
            Self::Csv => &["csv"],
            Self::Json => &["json"],
            Self::Ndjson => &["ndjson"],
            Self::Parquet => &["parquet"],
        }
    }
}

#[tauri::command]
pub(crate) async fn import_provider_credential_bundle(
    confirmed: bool,
    app: AppHandle,
    window: WebviewWindow,
    state: State<'_, DesktopState>,
) -> Result<Option<Value>, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm the protected credential import before selecting its one-time bundle.",
    )?;
    if window.label() != "main" {
        return Err(DesktopCommandError::new(
            "unauthorized",
            "Provider credentials can be selected only from the main product window.",
        ));
    }
    let selected = tauri::async_runtime::spawn_blocking(move || {
        app.dialog()
            .file()
            .add_filter("Market Squawk provider credentials", &["env"])
            .blocking_pick_file()
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = selected.into_path().map_err(|_error| {
        DesktopCommandError::invalid_request(
            "The selected provider credential bundle is not a local file.",
        )
    })?;
    let admitted = tauri::async_runtime::spawn_blocking(move || {
        open_and_hash_with_extensions(
            path,
            MAXIMUM_PROVIDER_CREDENTIAL_BUNDLE_BYTES,
            &["env"],
            "Select a filled Market Squawk provider-credentials .env file.",
        )
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())??;
    let ticket = stage_admitted_input(
        admitted,
        PROVIDER_CREDENTIAL_BUNDLE_MEDIA_TYPE,
        &state,
        &generation,
    )
    .await?;
    let arguments = Map::from_iter([(
        "inputTicketId".to_owned(),
        Value::String(ticket.id().as_uuid().to_string()),
    )]);
    let result = invoke_private_application(
        IMPORT_PROVIDER_CREDENTIAL_BUNDLE,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(IMPORT_PROVIDER_CREDENTIAL_BUNDLE),
    )
    .await?;
    strictly_redacted_provider_import(result).map(Some)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct PortfolioImportInterpretationInput {
    record_token: uuid::Uuid,
    interpretation: String,
    rationale: String,
    #[serde(default)]
    selected_lot_indexes: Vec<usize>,
}

#[tauri::command]
pub(crate) async fn preview_portfolio_import(
    account_id: String,
    confirmed: bool,
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<Option<Value>, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm the account before selecting a portfolio extraction file.",
    )?;
    let selected = tauri::async_runtime::spawn_blocking(move || {
        app.dialog()
            .file()
            .add_filter("Portfolio extraction batch", &["json"])
            .blocking_pick_file()
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = selected.into_path().map_err(|_error| {
        DesktopCommandError::invalid_request("The selected portfolio input is not a local file.")
    })?;
    let admitted = tauri::async_runtime::spawn_blocking(move || {
        open_and_hash(
            path,
            MAXIMUM_PORTFOLIO_IMPORT_BYTES,
            "Select a JSON portfolio extraction batch.",
        )
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())??;
    let ticket =
        stage_admitted_input(admitted, PORTFOLIO_IMPORT_MEDIA_TYPE, &state, &generation).await?;
    let arguments = Map::from_iter([
        ("accountId".to_owned(), Value::String(account_id)),
        (
            "inputTicketId".to_owned(),
            Value::String(ticket.id().as_uuid().to_string()),
        ),
    ]);
    invoke_private_application(
        PREVIEW_PORTFOLIO_IMPORT,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(PREVIEW_PORTFOLIO_IMPORT),
    )
    .await
    .map(Some)
}

#[tauri::command]
pub(crate) async fn commit_portfolio_import(
    review_token: uuid::Uuid,
    interpretations: Vec<PortfolioImportInterpretationInput>,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm the exact preview and interpretations before committing this portfolio import.",
    )?;
    let mut approval_arguments = Map::new();
    approval_arguments.insert("reviewToken".to_owned(), json!(review_token));
    approval_arguments.insert(
        "interpretations".to_owned(),
        serde_json::to_value(interpretations).map_err(|_error| DesktopCommandError::internal())?,
    );
    let approval = invoke_private_application(
        APPROVE_PORTFOLIO_IMPORT,
        approval_arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(APPROVE_PORTFOLIO_IMPORT),
    )
    .await?;
    let approved = approval
        .get("data")
        .and_then(Value::as_object)
        .ok_or_else(DesktopCommandError::internal)?;
    let approval_token = approved
        .get("approvalToken")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(DesktopCommandError::internal)?;
    if approved.get("reviewToken").and_then(Value::as_str)
        != Some(review_token.to_string().as_str())
        || !matches!(
            approved.get("status").and_then(Value::as_str),
            Some("approved" | "promoting")
        )
    {
        return Err(DesktopCommandError::internal());
    }
    let commit_arguments = Map::from_iter([(
        "approvalToken".to_owned(),
        Value::String(approval_token.to_owned()),
    )]);
    invoke_private_application(
        COMMIT_PORTFOLIO_IMPORT,
        commit_arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(COMMIT_PORTFOLIO_IMPORT),
    )
    .await
}

#[tauri::command]
pub(crate) async fn discard_portfolio_import(
    review_token: uuid::Uuid,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm that this uncommitted portfolio preview should be discarded.",
    )?;
    let arguments = Map::from_iter([("reviewToken".to_owned(), json!(review_token))]);
    invoke_private_application(
        DISCARD_PORTFOLIO_IMPORT,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(DISCARD_PORTFOLIO_IMPORT),
    )
    .await
}

#[tauri::command]
pub(crate) async fn preview_research_file_import(
    format: ResearchFileFormat,
    confirmed: bool,
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<Option<Value>, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm the research format before selecting a file to preview.",
    )?;
    let extensions = format.extensions();
    let selected = tauri::async_runtime::spawn_blocking(move || {
        app.dialog()
            .file()
            .add_filter("Research data", extensions)
            .blocking_pick_file()
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = selected.into_path().map_err(|_error| {
        DesktopCommandError::invalid_request("The selected research input is not a local file.")
    })?;
    let admitted = tauri::async_runtime::spawn_blocking(move || {
        open_and_hash_with_extensions(
            path,
            MAXIMUM_RESEARCH_FILE_BYTES,
            extensions,
            "Select a file matching the chosen research-data format.",
        )
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())??;
    let ticket =
        stage_admitted_input(admitted, RESEARCH_FILE_MEDIA_TYPE, &state, &generation).await?;
    let arguments = Map::from_iter([
        (
            "inputTicketId".to_owned(),
            Value::String(ticket.id().as_uuid().to_string()),
        ),
        (
            "format".to_owned(),
            Value::String(format.as_str().to_owned()),
        ),
    ]);
    invoke_private_application(
        PREVIEW_RESEARCH_FILE,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(PREVIEW_RESEARCH_FILE),
    )
    .await
    .map(Some)
}

#[tauri::command]
pub(crate) async fn commit_research_file_import(
    preview_id: String,
    mapping: Value,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm the preview and field mapping before importing this research file.",
    )?;
    if !mapping.is_object() {
        return Err(DesktopCommandError::invalid_request(
            "The research file mapping must be an object.",
        ));
    }
    let arguments = Map::from_iter([
        ("previewId".to_owned(), Value::String(preview_id)),
        ("mapping".to_owned(), mapping),
    ]);
    invoke_private_application(
        COMMIT_RESEARCH_FILE,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(COMMIT_RESEARCH_FILE),
    )
    .await
}

#[tauri::command]
pub(crate) async fn discard_research_file_import(
    preview_id: String,
    confirmed: bool,
    state: State<'_, DesktopState>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    require_confirmation(
        confirmed,
        "Confirm that this uncommitted research preview should be discarded.",
    )?;
    let arguments = Map::from_iter([("previewId".to_owned(), Value::String(preview_id))]);
    invoke_private_application(
        DISCARD_RESEARCH_FILE,
        arguments,
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed(DISCARD_RESEARCH_FILE),
    )
    .await
}

#[tauri::command]
pub(crate) async fn start_backtest_from_file(
    confirmed: bool,
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<Option<Value>, DesktopCommandError> {
    let generation = state.generation()?;
    if !confirmed {
        return Err(DesktopCommandError::new(
            "confirmation_required",
            "Confirm the governed backtest before selecting its registration.",
        ));
    }
    let selected = tauri::async_runtime::spawn_blocking(move || {
        app.dialog()
            .file()
            .add_filter("Governed backtest registration", &["json"])
            .blocking_pick_file()
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())?;
    let Some(selected) = selected else {
        return Ok(None);
    };
    let path = selected.into_path().map_err(|_error| {
        DesktopCommandError::invalid_request("The selected registration is not a local file.")
    })?;
    let admitted = tauri::async_runtime::spawn_blocking(move || {
        open_and_hash(
            path,
            MAXIMUM_BACKTEST_REGISTRATION_BYTES,
            "Select a JSON governed backtest registration.",
        )
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())??;
    let registration = serde_json::from_reader::<_, Value>(admitted.file)
        .map_err(|_error| {
            DesktopCommandError::invalid_request(
                "The selected registration is not valid canonical JSON.",
            )
        })?
        .as_object()
        .cloned()
        .ok_or_else(|| {
            DesktopCommandError::invalid_request(
                "The governed backtest registration must be a JSON object.",
            )
        })?;
    let mut arguments = serde_json::Map::new();
    arguments.insert("registration".to_owned(), Value::Object(registration));
    invoke_application(
        ApplicationInvocation {
            operation: "Analysis.StartBacktest".to_owned(),
            arguments,
        },
        &state,
        &generation,
        InvocationAuthority::ExactConfirmed("Analysis.StartBacktest"),
    )
    .await
    .map(Some)
}

#[tauri::command]
pub(crate) async fn stage_training_input(
    kind: TrainingInputKind,
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<Option<Value>, DesktopCommandError> {
    let generation = state.generation()?;
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
    let admitted = tauri::async_runtime::spawn_blocking(move || {
        open_and_hash(
            path,
            kind.maximum_bytes(),
            "Select a JSON file for this training input.",
        )
    })
    .await
    .map_err(|_error| DesktopCommandError::internal())??;
    let ticket = stage_admitted_input(admitted, kind.media_type(), &state, &generation).await?;
    let value = serde_json::to_value(ticket).map_err(|_error| DesktopCommandError::internal())?;
    Ok(Some(super::bridge::lossless_webview_value(value)))
}

struct AdmittedFile {
    file: std::fs::File,
    byte_length: u64,
    digest: [u8; 32],
}

fn open_and_hash(
    path: PathBuf,
    maximum_bytes: u64,
    invalid_extension_message: &'static str,
) -> Result<AdmittedFile, DesktopCommandError> {
    open_and_hash_with_extensions(path, maximum_bytes, &["json"], invalid_extension_message)
}

fn open_and_hash_with_extensions(
    path: PathBuf,
    maximum_bytes: u64,
    allowed_extensions: &[&str],
    invalid_extension_message: &'static str,
) -> Result<AdmittedFile, DesktopCommandError> {
    validate_extension(&path, allowed_extensions, invalid_extension_message)?;
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
    if !metadata.is_file() || byte_length == 0 || byte_length > maximum_bytes {
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
        digest,
    })
}

fn validate_extension(
    path: &Path,
    allowed_extensions: &[&str],
    invalid_extension_message: &'static str,
) -> Result<(), DesktopCommandError> {
    let valid = path
        .extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| {
            allowed_extensions
                .iter()
                .any(|allowed| extension.eq_ignore_ascii_case(allowed))
        });
    if valid {
        Ok(())
    } else {
        Err(DesktopCommandError::invalid_request(
            invalid_extension_message,
        ))
    }
}

async fn stage_admitted_input(
    admitted: AdmittedFile,
    media_type: &'static str,
    state: &DesktopState,
    generation: &Arc<DesktopGeneration>,
) -> Result<InputTicket, DesktopCommandError> {
    state.admit_current(generation)?;
    let admission = InputAdmission::try_sha256(media_type, admitted.byte_length, admitted.digest)
        .map_err(|_error| DesktopCommandError::internal())?;
    let mut input = tokio::fs::File::from_std(admitted.file);
    let ticket = generation
        .application()
        .stage_input(admission, &mut input, generation.cancellation())
        .await
        .map_err(super::bridge::map_application_client_error)?;
    state.admit_current(generation)?;
    Ok(ticket)
}

fn strictly_redacted_provider_import(result: Value) -> Result<Value, DesktopCommandError> {
    let data = result
        .get("data")
        .cloned()
        .ok_or_else(DesktopCommandError::internal)?;
    let redacted = serde_json::from_value::<ProviderCredentialImportResult>(data)
        .map_err(|_error| DesktopCommandError::internal())?;
    let exact_provider_order = redacted
        .providers
        .iter()
        .map(|provider| provider.provider)
        .eq(PROVIDER_CREDENTIAL_BUNDLE_PROVIDERS);
    let enabled_dispositions_match = redacted.providers.iter().all(|provider| {
        provider.enabled
            != matches!(
                provider.disposition,
                ProviderCredentialImportDisposition::Disabled
            )
    });
    let returned_items = result
        .pointer("/metadata/returnedItems")
        .and_then(Value::as_u64);
    let available_items = result
        .pointer("/metadata/availableItems")
        .and_then(Value::as_u64);
    if redacted.schema != PROVIDER_CREDENTIAL_BUNDLE_SCHEMA
        || !exact_provider_order
        || !enabled_dispositions_match
        || returned_items != Some(PROVIDER_CREDENTIAL_BUNDLE_PROVIDERS.len() as u64)
        || available_items != returned_items
        || result
            .pointer("/metadata/completeness")
            .and_then(Value::as_str)
            != Some("complete")
    {
        return Err(DesktopCommandError::internal());
    }
    serde_json::to_value(redacted).map_err(|_error| DesktopCommandError::internal())
}

fn require_confirmation(confirmed: bool, message: &'static str) -> Result<(), DesktopCommandError> {
    if confirmed {
        Ok(())
    } else {
        Err(DesktopCommandError::new("confirmation_required", message))
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
