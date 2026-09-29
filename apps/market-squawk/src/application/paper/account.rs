//! Explicit virtual cash initialization without market, session or execution authority.
use super::*;
use market_squawk_adapter_paper::PaperCheckpointRepository;
use std::num::NonZeroUsize;

const ACCOUNT_LABEL: &str = "Virtual portfolio";
const SAFEGUARDS: [&str; 3] = [
    "This account uses virtual cash and cannot place brokerage orders.",
    "Creating this account does not start a session or place an order.",
    "Orders still require current market information and the active account safeguards.",
];

pub(super) struct PreparedPaperAccount {
    pub(super) origin: RequestOrigin,
    pub(super) expires_at: Instant,
    currency: Currency,
    initial_cash: Decimal,
    fee_basis_points: u32,
}
pub(super) struct ProductAccountPreparation {
    pub(super) token: Box<str>,
    pub(super) prepared: PreparedPaperAccount,
}

impl PaperController {
    pub(super) async fn original_start_choices(
        &self,
        context: &RequestContext,
    ) -> Result<Option<(Currency, PaperCashChoice, PaperCostChoice)>, ServiceError> {
        let account = manual_paper_account_id().map_err(|_| ServiceError::Unavailable)?;
        let original = match self.portfolio_publisher.original_start_terms(
            account, context.deadline(), context.cancellation().clone(),
        ).await {
            Ok(original) => original,
            Err(crate::portfolio_application::PortfolioApplicationServiceError::InvalidRequest) => return Ok(None),
            Err(crate::portfolio_application::PortfolioApplicationServiceError::Cancelled) => return Err(ServiceError::Cancelled),
            Err(crate::portfolio_application::PortfolioApplicationServiceError::DeadlineExceeded) => return Err(ServiceError::DeadlineExceeded),
            Err(crate::portfolio_application::PortfolioApplicationServiceError::ResourceExhausted) => return Err(ServiceError::ResourceExhausted),
            Err(_) => return Err(ServiceError::Unavailable),
        };
        let Some((cash, fee)) = original else { return Ok(None); };
        let cash_choice = PAPER_CASH_CHOICES.iter().copied().find(|choice| {
            choice.amount.parse::<Decimal>().is_ok_and(|amount| amount == cash.amount())
        });
        let cost_choice = PAPER_COST_CHOICES.iter().copied().find(|choice| choice.basis_points == fee);
        Ok(cash_choice.zip(cost_choice).map(|(cash_choice,cost_choice)| (cash.currency(),cash_choice,cost_choice)))
    }

    pub(super) async fn account_preparation(
        &self,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let account = manual_paper_account_id().map_err(|_| ServiceError::Unavailable)?;
        if self
            .portfolio_publisher
            .current_revision(account)
            .map_err(|_| ServiceError::Unavailable)?
            .is_some()
        {
            return Ok(
                json!({"availability":"already_created", "accountLabel":ACCOUNT_LABEL,
                "message":"Your virtual account is available on the Portfolio page. Starting a paper session still requires current market information."}),
            );
        }
        let currency = match configured_paper_currency(&self.config) {
            Ok(currency) => currency,
            Err(_) => {
                return Ok(json!({"availability":"unavailable",
                "message":"The configured paper reporting currency is unavailable. Review workspace settings before creating a virtual account."}));
            }
        };
        let mut choices = paper_start_preparation(currency)?;
        choices
            .as_object_mut()
            .ok_or(ServiceError::Internal)?
            .remove("modeChoices");
        choices["accountLabel"] = json!(ACCOUNT_LABEL);
        choices["currencyChoices"] = json!([{"choiceToken": paper_choice_token("account-currency", currency.as_str())?,
            "label": currency.as_str(), "currency": currency.as_str()}]);
        choices["safeguards"] = json!(SAFEGUARDS);
        ensure_live(context)?;
        Ok(choices)
    }

    pub(super) async fn prepare_account(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let origin = required_origin(context)?;
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        if !matches!(&*state, PaperState::Stopped { .. }) {
            return Err(ServiceError::InvalidRequest);
        }
        drop(state);
        let account = manual_paper_account_id().map_err(|_| ServiceError::Unavailable)?;
        if self
            .portfolio_publisher
            .current_revision(account)
            .map_err(|_| ServiceError::Unavailable)?
            .is_some()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let currency = configured_paper_currency(&self.config)?;
        if required_string(request, "currencyChoice")?
            != paper_choice_token("account-currency", currency.as_str())?.as_ref()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let cash = resolve_paper_cash_choice(required_string(request, "cashChoice")?)?;
        let cost = resolve_paper_cost_choice(required_string(request, "costChoice")?)?;
        let initial_cash = cash
            .amount
            .parse::<Decimal>()
            .map_err(|_| ServiceError::Internal)?;
        let expires_at = current_timestamp()?
            .checked_add_nanos(
                i64::try_from(PAPER_START_PREPARATION_LIFETIME.as_nanos())
                    .map_err(|_| ServiceError::Unavailable)?,
            )
            .map_err(|_| ServiceError::Unavailable)?;
        let prepared = PreparedPaperAccount {
            origin,
            expires_at: Instant::now()
                .checked_add(PAPER_START_PREPARATION_LIFETIME)
                .ok_or(ServiceError::Unavailable)?,
            currency,
            initial_cash,
            fee_basis_points: cost.basis_points,
        };
        let confirmation = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .insert_account(prepared)?;
        Ok(
            json!({"confirmationToken":confirmation,"expiresAt":product::timestamp(expires_at),
            "accountLabel":ACCOUNT_LABEL,"virtualCash":product::money(Money::new(initial_cash,currency)),
            "estimatedTradingCost":product::percentage(BasisPoints::new(i32::try_from(cost.basis_points).map_err(|_| ServiceError::Internal)?)),
            "safeguards":SAFEGUARDS}),
        )
    }

    pub(super) async fn create_account(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        ensure_live(context)?;
        let origin = required_origin(context)?;
        let prepared = bounded_lock(
            &self.product_tokens,
            context.deadline(),
            context.cancellation(),
        )
        .await?
        .consume_account(
            required_string(request, "confirmationToken")?,
            origin,
            Instant::now(),
        )?;
        let owner = tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(context.deadline().into()) => return Err(ServiceError::DeadlineExceeded),
            owner = Arc::clone(&self.owner_gate).lock_owned() => owner,
        };
        if !self.accepting.load(Ordering::Acquire) || self.lifecycle.is_cancelled() {
            return Err(ServiceError::Unavailable);
        }
        let state = bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
        if !matches!(&*state, PaperState::Stopped { .. }) {
            return Err(ServiceError::InvalidRequest);
        }
        drop(state);
        if Instant::now() >= prepared.expires_at
            || configured_paper_currency(&self.config)? != prepared.currency
        {
            return Err(ServiceError::InvalidRequest);
        }
        let account = manual_paper_account_id().map_err(|_| ServiceError::Unavailable)?;
        let configuration = crate::paper_bot::local_paper_account_configuration(
            prepared.currency,
            prepared.fee_basis_points,
        )
        .map_err(|_| ServiceError::Unavailable)?;
        let config = self.config.clone();
        let deadline = context.deadline();
        let cancellation = context.cancellation().clone();
        let worker_cancellation = cancellation.clone();
        self.portfolio_publisher
            .publish_initialized_account(account, deadline, cancellation, move || {
                use crate::portfolio_application::PortfolioApplicationServiceError as Error;
                // The original start fence moves into the retained publisher worker before any I/O.
                let owner = owner;
                if worker_cancellation.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                if Instant::now() >= deadline || Instant::now() >= prepared.expires_at {
                    return Err(Error::DeadlineExceeded);
                }
                let paths = market_squawk_platform::LocalPaths::prepare(config.data_dir())
                    .map_err(|_| Error::Publication)?;
                let root = paths.artifacts().map_err(|_| Error::Publication)?.clone();
                let maximum =
                    NonZeroUsize::new(crate::paper_bot::LOCAL_PAPER_CHECKPOINT_MAXIMUM_BYTES)
                        .ok_or(Error::ResourceExhausted)?;
                let mut repository =
                    match PaperCheckpointRepository::open_stopped(root.clone(), maximum)
                        .map_err(|_| Error::Publication)?
                    {
                        Some(repository) => {
                            if repository.original_config() != &configuration {
                                return Err(Error::InvalidRequest);
                            }
                            repository
                        }
                        None => PaperCheckpointRepository::try_new(root, configuration, maximum)
                            .map_err(|_| Error::Publication)?,
                    };
                let opened_at = current_timestamp().map_err(|_| Error::Publication)?;
                let replay = repository
                    .initialize_cash_account(
                        account,
                        Money::new(prepared.initial_cash, prepared.currency),
                        opened_at,
                    )
                    .map_err(|_| Error::Publication)?;
                Ok((replay, (owner, repository)))
            })
            .await
            .map_err(|error| {
                tracing::warn!(%error, "virtual account publication unavailable");
                use crate::portfolio_application::PortfolioApplicationServiceError as Error;
                match error {
                    Error::Cancelled => ServiceError::Cancelled,
                    Error::DeadlineExceeded => ServiceError::DeadlineExceeded,
                    Error::ResourceExhausted => ServiceError::ResourceExhausted,
                    Error::InvalidRequest => ServiceError::InvalidRequest,
                    _ => ServiceError::Unavailable,
                }
            })?;
        Ok(
            json!({"accountCreated":true,"sessionAvailability":"stopped",
            "message":"Your virtual account is ready on the Portfolio page. No paper session or order was started."}),
        )
    }
}

impl ProductAuthorityTokens {
    fn insert_account(&mut self, prepared: PreparedPaperAccount) -> Result<Box<str>, ServiceError> {
        self.prune_preparations(Instant::now());
        if self.account_preparations.len() >= MAXIMUM_PENDING_PAPER_PREPARATIONS {
            return Err(ServiceError::ResourceExhausted);
        }
        self.account_preparations
            .try_reserve(1)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let token = self.unique_confirmation_token(
            b"market-squawk/product-paper-account-confirmation/v1\0",
            prepared.origin,
        )?;
        self.account_preparations.push(ProductAccountPreparation {
            token: token.clone(),
            prepared,
        });
        Ok(token)
    }
    fn consume_account(
        &mut self,
        token: &str,
        origin: RequestOrigin,
        now: Instant,
    ) -> Result<PreparedPaperAccount, ServiceError> {
        self.prune_preparations(now);
        let index = unique_preparation_index(
            self.account_preparations
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.token.as_ref() == token)
                .map(|(i, entry)| (i, entry.prepared.origin)),
            origin,
        )?;
        Ok(self.account_preparations.remove(index).prepared)
    }
}
