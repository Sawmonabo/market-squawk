//! Selected history and financial preparation share one native admission/recovery path.

use market_squawk_services::RequestId;
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use tauri::State;
use uuid::Uuid;

use super::{
    canonicalize_job_result, invoke_narrow, job_mutation_arguments, map_with_job_id,
    parse_job_generation, project_product_metadata, require_confirmation,
};
use crate::{
    bridge::{
        DesktopGeneration, DesktopState, InvocationAuthority, invoke_analytical_operation,
        invoke_read_application, prepare_analytical_arguments,
    },
    contracts::{
        DesktopCommandError, InvestmentFinancialPreparationCommand,
        MarketHistoryPreparationCommand, ProductSessionToken,
    },
};

#[derive(Clone, Copy)]
enum Family {
    History,
    Financial,
}

impl Family {
    fn start(self) -> &'static str {
        match self {
            Self::History => "Market.StartHistoryPreparation",
            Self::Financial => "Research.StartInvestmentFinancialPreparation",
        }
    }

    fn get(self) -> &'static str {
        match self {
            Self::History => "Market.GetHistoryPreparation",
            Self::Financial => "Research.GetInvestmentFinancialPreparation",
        }
    }

    fn cancel(self) -> &'static str {
        match self {
            Self::History => "Market.CancelHistoryPreparation",
            Self::Financial => "Research.CancelInvestmentFinancialPreparation",
        }
    }

    fn token_field(self) -> &'static str {
        match self {
            Self::History => "historyToken",
            Self::Financial => "selectionToken",
        }
    }

    fn request_id(self, original: Uuid) -> Result<RequestId, DesktopCommandError> {
        if original.is_nil() {
            return Err(DesktopCommandError::invalid_request(
                "The selected preparation requires its original request identity.",
            ));
        }
        let prefix = match self {
            // Preserve already-retained history identities across this extraction.
            Self::History => "desktop-history",
            Self::Financial => "desktop-financial",
        };
        RequestId::try_string(format!("{prefix}-{}", original.simple()))
            .map_err(|_error| DesktopCommandError::internal())
    }
}

enum Action {
    Start {
        arguments: Map<String, Value>,
        original: Uuid,
    },
    Get {
        token: String,
        job_id: Uuid,
        generation: String,
    },
    Cancel {
        token: String,
        job_id: Uuid,
        generation: String,
        expected_sequence: String,
    },
    ReconcileStart {
        arguments: Map<String, Value>,
        original: Uuid,
    },
    CancelStart {
        arguments: Map<String, Value>,
        original: Uuid,
    },
}

pub(super) struct SelectedPreparation {
    family: Family,
    action: Action,
}

impl From<MarketHistoryPreparationCommand> for SelectedPreparation {
    fn from(request: MarketHistoryPreparationCommand) -> Self {
        let action = match request {
            MarketHistoryPreparationCommand::Start {
                history_token,
                lookback_days,
                start_request_id,
            } => Action::Start {
                arguments: history_arguments(history_token, lookback_days),
                original: start_request_id,
            },
            MarketHistoryPreparationCommand::Get {
                history_token,
                job_id,
                generation,
            } => Action::Get {
                token: history_token,
                job_id,
                generation,
            },
            MarketHistoryPreparationCommand::Cancel {
                history_token,
                job_id,
                generation,
                expected_sequence,
            } => Action::Cancel {
                token: history_token,
                job_id,
                generation,
                expected_sequence,
            },
            MarketHistoryPreparationCommand::ReconcileStart {
                history_token,
                lookback_days,
                start_request_id,
            } => Action::ReconcileStart {
                arguments: history_arguments(history_token, lookback_days),
                original: start_request_id,
            },
            MarketHistoryPreparationCommand::CancelStart {
                history_token,
                lookback_days,
                start_request_id,
            } => Action::CancelStart {
                arguments: history_arguments(history_token, lookback_days),
                original: start_request_id,
            },
        };
        Self {
            family: Family::History,
            action,
        }
    }
}

impl From<InvestmentFinancialPreparationCommand> for SelectedPreparation {
    fn from(request: InvestmentFinancialPreparationCommand) -> Self {
        let action = match request {
            InvestmentFinancialPreparationCommand::Start {
                selection_token,
                start_request_id,
            } => Action::Start {
                arguments: financial_arguments(selection_token),
                original: start_request_id,
            },
            InvestmentFinancialPreparationCommand::Get {
                selection_token,
                job_id,
                generation,
            } => Action::Get {
                token: selection_token,
                job_id,
                generation,
            },
            InvestmentFinancialPreparationCommand::Cancel {
                selection_token,
                job_id,
                generation,
                expected_sequence,
            } => Action::Cancel {
                token: selection_token,
                job_id,
                generation,
                expected_sequence,
            },
            InvestmentFinancialPreparationCommand::ReconcileStart {
                selection_token,
                start_request_id,
            } => Action::ReconcileStart {
                arguments: financial_arguments(selection_token),
                original: start_request_id,
            },
            InvestmentFinancialPreparationCommand::CancelStart {
                selection_token,
                start_request_id,
            } => Action::CancelStart {
                arguments: financial_arguments(selection_token),
                original: start_request_id,
            },
        };
        Self {
            family: Family::Financial,
            action,
        }
    }
}

pub(super) async fn invoke(
    request: SelectedPreparation,
    confirmed: bool,
    state: State<'_, DesktopState>,
    request_id: Option<Uuid>,
    product_session_token: Option<ProductSessionToken>,
) -> Result<Value, DesktopCommandError> {
    let generation = state.generation()?;
    let read = if matches!(
        &request.action,
        Action::Get { .. } | Action::ReconcileStart { .. }
    ) {
        Some(generation.begin_read(
            request_id.ok_or_else(|| {
                DesktopCommandError::invalid_request("The screen read requires a request identity.")
            })?,
            product_session_token.ok_or_else(|| {
                DesktopCommandError::invalid_request(
                    "The screen read requires its current session.",
                )
            })?,
        )?)
    } else {
        if request_id.is_some() || product_session_token.is_some() {
            return Err(DesktopCommandError::invalid_request(
                "A selected preparation change is not a cancellable read.",
            ));
        }
        None
    };
    let family = request.family;
    let (operation, arguments, mutation) = match request.action {
        Action::Start {
            arguments,
            original,
        } => {
            require_confirmation(confirmed)?;
            let request_id = family.request_id(original)?;
            state.admit_current(&generation)?;
            // The renderer retains this UUID before invocation; recovery never repeats Start.
            let mut result = invoke_analytical_operation(
                &generation,
                family.start(),
                arguments,
                InvocationAuthority::ExactConfirmed(family.start()),
                request_id,
                generation.cancellation(),
            )
            .await?;
            state.admit_current(&generation)?;
            canonicalize_job_result(family.start(), &mut result)?;
            project_product_metadata(&mut result)?;
            return Ok(result);
        }
        Action::Get {
            token,
            job_id,
            generation,
        } => {
            let mut arguments = map_with_job_id(job_id);
            arguments.insert(family.token_field().to_owned(), json!(token));
            arguments.insert(
                "generation".to_owned(),
                json!(parse_job_generation(generation)?),
            );
            (family.get(), arguments, false)
        }
        Action::Cancel {
            token,
            job_id,
            generation,
            expected_sequence,
        } => {
            let mut arguments = job_mutation_arguments(job_id, generation, expected_sequence)?;
            arguments.insert(family.token_field().to_owned(), json!(token));
            (family.cancel(), arguments, true)
        }
        Action::ReconcileStart {
            arguments,
            original,
        } => (
            "Job.ReconcileStart",
            reconciliation_arguments(&generation, family, arguments, original)?,
            false,
        ),
        Action::CancelStart {
            arguments,
            original,
        } => (
            "Job.CancelStart",
            reconciliation_arguments(&generation, family, arguments, original)?,
            true,
        ),
    };
    if let Some(read) = read {
        let mut result = invoke_read_application(operation, arguments, &state, &read).await?;
        canonicalize_job_result(operation, &mut result)?;
        project_product_metadata(&mut result)?;
        Ok(result)
    } else {
        let mut result = invoke_narrow(
            operation,
            arguments,
            mutation,
            confirmed,
            &state,
            &generation,
        )
        .await?;
        project_product_metadata(&mut result)?;
        Ok(result)
    }
}

fn history_arguments(token: String, lookback_days: u16) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("historyToken".to_owned(), json!(token));
    arguments.insert("lookbackDays".to_owned(), json!(lookback_days));
    arguments
}

fn financial_arguments(token: String) -> Map<String, Value> {
    let mut arguments = Map::new();
    arguments.insert("selectionToken".to_owned(), json!(token));
    arguments
}

fn reconciliation_arguments(
    generation: &DesktopGeneration,
    family: Family,
    arguments: Map<String, Value>,
    original: Uuid,
) -> Result<Map<String, Value>, DesktopCommandError> {
    let request_id = family.request_id(original)?;
    // Hash the exact admitted argument map, including native confirmation/result limits.
    // Neither the operation nor the digest is chosen by the WebView.
    let original = prepare_analytical_arguments(
        generation,
        family.start(),
        arguments,
        InvocationAuthority::ExactConfirmed(family.start()),
    )?;
    let encoded =
        serde_json::to_vec(&original).map_err(|_error| DesktopCommandError::internal())?;
    let mut arguments = Map::new();
    arguments.insert("requestId".to_owned(), json!(request_id));
    arguments.insert("operation".to_owned(), json!(family.start()));
    arguments.insert(
        "argumentsSha256".to_owned(),
        json!(format!("{:x}", Sha256::digest(encoded))),
    );
    Ok(arguments)
}
