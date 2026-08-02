//! Bounded CLI registration boundary for governed point-in-time backtests.

use std::path::{Component, Path, PathBuf};

use market_squawk_platform::{InputFileError, UserAuthorizedInputRoot};
use serde_json::{Value, json};
use thiserror::Error;

use crate::application::analysis::{
    GovernedBacktestInputRegistrationInput, GovernedBacktestInputRegistrationJsonError,
    MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES,
};

/// Request-file admission, schema decoding, or governed registration failure.
#[derive(Debug, Error)]
pub enum CliBacktestRegistrationError {
    /// Durable input registration was not explicitly confirmed.
    #[error("backtest input registration requires explicit confirmation")]
    ConfirmationRequired,
    /// The request path could not be made absolute without changing its meaning.
    #[error("backtest registration request path is invalid: {0}")]
    RequestPath(#[source] std::io::Error),
    /// The request path was not an explicit safe regular-file coordinate.
    #[error("backtest registration request path is not a safe regular-file coordinate")]
    UnsafeRequestPath,
    /// The request was not an unchanged, bounded, no-follow regular file.
    #[error("backtest registration request file is not admissible: {0}")]
    RequestFile(#[source] InputFileError),
    /// The request violated the versioned closed governed-input contract.
    #[error("backtest registration request is invalid: {0}")]
    RequestJson(#[source] GovernedBacktestInputRegistrationJsonError),
}

/// Admits one closed request file for registration inside the shared Analysis authority.
pub(super) async fn register_backtest_input(
    request_path: &Path,
    confirmed: bool,
) -> Result<Value, CliBacktestRegistrationError> {
    if !confirmed {
        return Err(CliBacktestRegistrationError::ConfirmationRequired);
    }
    let input = read_request(request_path)?;
    let _registration = GovernedBacktestInputRegistrationInput::try_from_json(input.as_bytes())
        .map_err(CliBacktestRegistrationError::RequestJson)?;
    let registration = serde_json::from_slice::<Value>(input.as_bytes()).map_err(|_| {
        CliBacktestRegistrationError::RequestJson(
            GovernedBacktestInputRegistrationJsonError::Invalid,
        )
    })?;
    Ok(json!({
        "registration": registration,
        "confirm": true,
    }))
}

fn read_request(
    path: &Path,
) -> Result<market_squawk_platform::BoundedInput, CliBacktestRegistrationError> {
    let absolute = std::path::absolute(path).map_err(CliBacktestRegistrationError::RequestPath)?;
    if !absolute.is_absolute()
        || absolute
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(CliBacktestRegistrationError::UnsafeRequestPath);
    }
    let parent = absolute
        .parent()
        .ok_or(CliBacktestRegistrationError::UnsafeRequestPath)?;
    let name = absolute
        .file_name()
        .ok_or(CliBacktestRegistrationError::UnsafeRequestPath)?;
    let root =
        UserAuthorizedInputRoot::open(parent).map_err(CliBacktestRegistrationError::RequestFile)?;
    root.resolve(PathBuf::from(name))
        .and_then(|file| file.open_bounded(MAX_GOVERNED_BACKTEST_REGISTRATION_REQUEST_BYTES))
        .and_then(|file| file.read_bounded())
        .map_err(CliBacktestRegistrationError::RequestFile)
}
