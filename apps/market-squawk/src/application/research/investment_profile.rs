//! Selected reference profiles over the existing canonical and official-listing authorities.
//!
//! A single backend cutoff binds token resolution and the complete reference read. This leaf
//! acquires no provider data and never turns a symbol or display name into identity authority.

use std::{sync::Arc, time::Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_domain::{AssetClass, Currency, InstrumentId, Timestamp};
use market_squawk_services::ServiceError;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use super::corporate_actions::map_research_error;
use super::instrument_context::{
    InstrumentContextMissingReason, InstrumentContextOutcome, InstrumentContextRead,
    InstrumentContextReadCapability, InstrumentContextReadError, InstrumentContextRequest,
    InstrumentContextUnavailableReason, InstrumentOfficialLifecycleEvidence,
};
use crate::ResearchService;
use crate::application::domain_support::try_boxed_product_text;
use crate::application::market_selection::product::MarketProductSelectionReadCapability;

pub(crate) const INVESTMENT_PROFILE_READ_OPERATION: &str = "Research.GetInvestmentProfile";

/// Reuses the retained-read worker and exact canonical token/reference owners.
#[derive(Clone, Debug)]
pub(crate) struct InvestmentProfileReadCapability {
    research: Arc<ResearchService>,
    selections: MarketProductSelectionReadCapability,
    references: Option<InstrumentContextReadCapability>,
}

impl InvestmentProfileReadCapability {
    pub(crate) const fn new(
        research: Arc<ResearchService>,
        selections: MarketProductSelectionReadCapability,
        references: Option<InstrumentContextReadCapability>,
    ) -> Self {
        Self {
            research,
            selections,
            references,
        }
    }

    /// Reads only the admitted selection; reference absence cannot select a substitute.
    pub(crate) async fn read(
        &self,
        selection_token: &str,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InvestmentProfileResult, ServiceError> {
        check(deadline, cancellation)?;
        let cutoff = Utc::now()
            .timestamp_nanos_opt()
            .filter(|value| *value > 0)
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::Unavailable)?;
        let instrument_id = self
            .selections
            .resolve(selection_token, cutoff, deadline, cancellation)
            .await?;
        check(deadline, cancellation)?;
        // Resolution has already applied the existing token's admitted byte bound.
        let selection_token = copy_text(selection_token)?;
        let references = self.references.clone();
        self.research
            .run_owned_research_read(deadline, cancellation, move |operation_cancellation| {
                check(deadline, &operation_cancellation)?;
                let mut result = InvestmentProfileResult {
                    selection_token,
                    knowledge_at: timestamp_text(cutoff),
                    state: InvestmentProfileState::Unavailable,
                    reason: Some(InvestmentProfileReason::ReferenceNotConfigured),
                    profile: None,
                };
                if let Some(references) = references {
                    let request = InstrumentContextRequest::try_new(instrument_id, cutoff, cutoff)
                        .map_err(map_reference_error)?;
                    let read = references
                        .read(request, deadline, &operation_cancellation)
                        .map_err(map_reference_error)?;
                    result.project(&read, instrument_id, cutoff)?;
                }
                check(deadline, &operation_cancellation)?;
                Ok(result)
            })
            .await
            .map_err(map_research_error)?
    }
}

/// Provider-neutral result; private selector receipts and canonical storage coordinates stay local.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InvestmentProfileResult {
    selection_token: Box<str>,
    knowledge_at: String,
    state: InvestmentProfileState,
    reason: Option<InvestmentProfileReason>,
    profile: Option<InvestmentReferenceProfile>,
}

impl InvestmentProfileResult {
    fn project(
        &mut self,
        read: &InstrumentContextRead,
        instrument_id: InstrumentId,
        cutoff: Timestamp,
    ) -> Result<(), ServiceError> {
        if read.request().instrument_id() != instrument_id
            || read.request().knowledge_at() != cutoff
            || read.request().effective_at() != cutoff
        {
            return Err(ServiceError::InvalidResult);
        }
        match read.outcome() {
            InstrumentContextOutcome::Exact(context) => {
                if context.instrument_id() != instrument_id || context.known_at() != cutoff {
                    return Err(ServiceError::InvalidResult);
                }
                self.profile = Some(InvestmentReferenceProfile {
                    display_name: copy_text(context.display_name())?,
                    symbol: copy_text(context.listed_symbol())?,
                    asset_class: context.asset_class(),
                    currency: context.quote_currency(),
                    listing_venue: copy_text(context.listing_venue().as_str())?,
                    exchange_traded_fund: context.exchange_traded_fund(),
                    round_lot_size: context.round_lot_size(),
                    effective_from: timestamp_text(context.validity().starts_at()),
                    effective_until: context.validity().ends_at().map(timestamp_text),
                    known_at: timestamp_text(context.known_at()),
                    reference_updated_at: timestamp_text(context.official_directory_updated_at()),
                    lifecycle: match context.official_lifecycle() {
                        InstrumentOfficialLifecycleEvidence::SuccessorAndDelistingNotEstablished => {
                            InvestmentProfileLifecycle::SuccessorAndDelistingNotEstablished
                        }
                    },
                });
                self.state = InvestmentProfileState::Available;
                self.reason = None;
            }
            InstrumentContextOutcome::Missing(reason) => {
                self.state = InvestmentProfileState::Missing;
                self.reason = Some(match reason {
                    InstrumentContextMissingReason::CanonicalDefinition => {
                        InvestmentProfileReason::CanonicalDefinition
                    }
                    InstrumentContextMissingReason::OfficialDirectory => {
                        InvestmentProfileReason::OfficialDirectory
                    }
                    InstrumentContextMissingReason::OfficialMembership => {
                        InvestmentProfileReason::OfficialMembership
                    }
                });
            }
            InstrumentContextOutcome::Ambiguous => {
                self.state = InvestmentProfileState::Ambiguous;
                self.reason = None;
            }
            InstrumentContextOutcome::Unavailable(reason) => {
                self.state = InvestmentProfileState::Unavailable;
                self.reason = Some(match reason {
                    InstrumentContextUnavailableReason::DirectoryReadBound => {
                        InvestmentProfileReason::DirectoryReadBound
                    }
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum InvestmentProfileState {
    Available,
    Missing,
    Ambiguous,
    Unavailable,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum InvestmentProfileReason {
    CanonicalDefinition,
    OfficialDirectory,
    OfficialMembership,
    DirectoryReadBound,
    ReferenceNotConfigured,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct InvestmentReferenceProfile {
    display_name: Box<str>,
    symbol: Box<str>,
    asset_class: AssetClass,
    currency: Currency,
    listing_venue: Box<str>,
    exchange_traded_fund: bool,
    round_lot_size: u32,
    effective_from: String,
    effective_until: Option<String>,
    known_at: String,
    reference_updated_at: String,
    lifecycle: InvestmentProfileLifecycle,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum InvestmentProfileLifecycle {
    SuccessorAndDelistingNotEstablished,
}

// Text has already crossed the existing bounded canonical/reference constructors.
fn copy_text(value: &str) -> Result<Box<str>, ServiceError> {
    try_boxed_product_text(value, value.len()).map_err(|_| ServiceError::ResourceExhausted)
}

fn timestamp_text(value: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(value.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn check(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_reference_error(error: InstrumentContextReadError) -> ServiceError {
    match error {
        InstrumentContextReadError::InvalidRequest => ServiceError::InvalidRequest,
        InstrumentContextReadError::Cancelled => ServiceError::Cancelled,
        InstrumentContextReadError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        InstrumentContextReadError::AuthorityUnavailable => ServiceError::Unavailable,
        InstrumentContextReadError::ResourceExhausted => ServiceError::ResourceExhausted,
        InstrumentContextReadError::EvidenceConflict
        | InstrumentContextReadError::RestartConflict => ServiceError::InvalidResult,
    }
}
