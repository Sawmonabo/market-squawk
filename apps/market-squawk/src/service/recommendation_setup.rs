//! Installed typed operations for explicit personalized-recommendation setup.

use std::{sync::Arc, time::Instant};

use market_squawk_domain::{AccountId, Currency, DigestAlgorithm, EvidenceDigest, Money};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    application::recommendation::{
        RecommendationProfileInput, RecommendationSetupAuthority, RecommendationSetupConfirmation,
        RecommendationSetupError, RecommendationSetupPreview, RecommendationSetupPreviewId,
        RecommendationSetupResolution, RecommendationSetupSnapshot, SelectedRecommendationAccount,
        UserConfirmedAllocationProfile,
    },
    portfolio_application::{
        PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
        PortfolioAccountCatalogSnapshot,
    },
};

pub(super) const GET_RECOMMENDATION_SETUP: &str = "Portfolio.GetRecommendationSetup";
pub(super) const PREVIEW_RECOMMENDATION_SETUP: &str = "Portfolio.PreviewRecommendationSetup";
pub(super) const COMMIT_RECOMMENDATION_SETUP: &str = "Portfolio.CommitRecommendationSetup";

const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;

/// One installed adapter over the exact active-workspace setup authority and portfolio catalog.
pub(super) struct InstalledRecommendationSetupOperations {
    authority: Arc<RecommendationSetupAuthority>,
    catalog: PortfolioAccountCatalogReadCapability,
    runtime: RuntimeIdentity,
}

impl InstalledRecommendationSetupOperations {
    pub(super) fn try_new(
        authority: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        if authority.owner_workspace() != runtime.workspace_id() {
            return Err(ServiceError::Unavailable);
        }
        Ok(Self {
            authority,
            catalog,
            runtime,
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_RECOMMENDATION_SETUP | PREVIEW_RECOMMENDATION_SETUP | COMMIT_RECOMMENDATION_SETUP
        )
    }

    pub(super) fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        let content = match request.name() {
            GET_RECOMMENDATION_SETUP => self.get(context)?,
            PREVIEW_RECOMMENDATION_SETUP => self.preview(request, context)?,
            COMMIT_RECOMMENDATION_SETUP => self.commit(request, context)?,
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn get(&self, context: &RequestContext) -> Result<Value, ServiceError> {
        let before = self.authority.status().map_err(map_setup)?;
        let catalog = self.snapshot_catalog(context)?;
        let as_of = super::runtime::current_timestamp().map_err(|_| ServiceError::Unavailable)?;
        let resolution = self.authority.resolve(&catalog, as_of).map_err(map_setup)?;
        let after = self.authority.status().map_err(map_setup)?;
        if before != after {
            return Err(ServiceError::Unavailable);
        }
        self.catalog
            .recheck(&catalog, context.deadline(), context.cancellation())
            .map_err(map_catalog)?;
        let (state, setup_required_reason) = match resolution {
            RecommendationSetupResolution::Ready(_) => ("ready", Value::Null),
            RecommendationSetupResolution::SetupRequired(required) => (
                "setup_required",
                Value::String(required.kind().as_str().to_owned()),
            ),
        };
        Ok(json!({
            "workspaceId": self.runtime.workspace_id().as_uuid(),
            "state": state,
            "setupRequiredReason": setup_required_reason,
            "authority": authority_value(&before),
            "accountSelection": before.selected_account().map(selected_account_value),
            "allocationProfile": before.profile().map(profile_value),
            "portfolioCatalog": catalog_value(&catalog),
        }))
    }

    fn preview(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: PreviewRequest = decode(request.arguments())?;
        let account_id =
            AccountId::try_from(input.account_id).map_err(|_| ServiceError::InvalidRequest)?;
        let profile = input.allocation_profile.try_into_profile(account_id)?;
        let catalog = self.snapshot_catalog(context)?;
        let preview = self
            .authority
            .preview_configuration(
                self.runtime.workspace_id(),
                input.expected_revision,
                &catalog,
                profile,
            )
            .map_err(map_setup)?;
        self.catalog
            .recheck(&catalog, context.deadline(), context.cancellation())
            .map_err(map_catalog)?;
        preview_value(&preview)
    }

    fn commit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: CommitRequest = decode(request.arguments())?;
        let preview_id =
            RecommendationSetupPreviewId::try_from_uuid(input.preview_id).map_err(map_setup)?;
        let preview_digest = decode_sha256(&input.preview_digest)?;
        let catalog = self.snapshot_catalog(context)?;
        let receipt = self
            .authority
            .apply(
                self.runtime.workspace_id(),
                RecommendationSetupConfirmation::new(preview_id, preview_digest),
                &catalog,
            )
            .map_err(map_setup)?;
        if !receipt.configured() {
            return Err(ServiceError::InvalidResult);
        }
        Ok(json!({
            "workspaceId": self.runtime.workspace_id().as_uuid(),
            "revision": receipt.revision(),
            "authorityDigest": hex(receipt.digest()),
            "configured": true,
            "acceptedAtUnixNanos": receipt.accepted_at().unix_nanos().to_string(),
        }))
    }

    fn snapshot_catalog(
        &self,
        context: &RequestContext,
    ) -> Result<PortfolioAccountCatalogSnapshot, ServiceError> {
        self.catalog
            .snapshot_current(context.deadline(), context.cancellation())
            .map_err(map_catalog)
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid()
            || self.authority.owner_workspace() != self.runtime.workspace_id()
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }
}

impl std::fmt::Debug for InstalledRecommendationSetupOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledRecommendationSetupOperations")
            .field("authority", &"[WORKSPACE RECOMMENDATION SETUP AUTHORITY]")
            .field("catalog", &self.catalog)
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreviewRequest {
    expected_revision: u64,
    account_id: Uuid,
    allocation_profile: AllocationProfileRequest,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AllocationProfileRequest {
    preferred_position_weight_lower_bps: u16,
    preferred_position_weight_upper_bps: u16,
    minimum_cash_reserve: MoneyRequest,
    maximum_downside_loss_bps_of_marked_equity: u16,
    available_investment_horizon_days: u16,
}

impl AllocationProfileRequest {
    fn try_into_profile(
        self,
        account_id: AccountId,
    ) -> Result<RecommendationProfileInput, ServiceError> {
        let amount = self
            .minimum_cash_reserve
            .amount
            .parse::<Decimal>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let currency = Currency::try_from(self.minimum_cash_reserve.currency.as_str())
            .map_err(|_| ServiceError::InvalidRequest)?;
        let available_investment_horizon_nanos = i64::from(self.available_investment_horizon_days)
            .checked_mul(NANOS_PER_DAY)
            .ok_or(ServiceError::InvalidRequest)?;
        RecommendationProfileInput::try_new(
            account_id,
            self.preferred_position_weight_lower_bps,
            self.preferred_position_weight_upper_bps,
            Money::new(amount, currency),
            self.maximum_downside_loss_bps_of_marked_equity,
            available_investment_horizon_nanos,
        )
        .map_err(map_setup)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MoneyRequest {
    amount: String,
    currency: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommitRequest {
    preview_id: Uuid,
    preview_digest: String,
}

fn authority_value(snapshot: &RecommendationSetupSnapshot) -> Value {
    json!({
        "revision": snapshot.revision(),
        "digest": hex(snapshot.digest()),
        "transitionAtUnixNanos": snapshot.transition_at().map(|at| at.unix_nanos().to_string()),
        "configurationDigest": snapshot.configuration_digest().map(hex),
    })
}

fn selected_account_value(selected: &SelectedRecommendationAccount) -> Value {
    json!({
        "setupRevision": selected.setup_revision(),
        "accountId": selected.account_id().as_uuid(),
        "reportingCurrency": selected.reporting_currency().as_str(),
        "confirmedPortfolioRevisionSha256": hex(selected.confirmed_portfolio_revision().bytes()),
        "confirmedCatalogDigestSha256": evidence_sha256(selected.confirmed_catalog_digest()),
        "confirmedAtUnixNanos": selected.confirmed_at().unix_nanos().to_string(),
        "digest": hex(selected.digest()),
    })
}

fn profile_value(profile: &UserConfirmedAllocationProfile) -> Value {
    let reserve = profile.minimum_cash_reserve();
    json!({
        "setupRevision": profile.setup_revision(),
        "accountId": profile.account_id().as_uuid(),
        "reportingCurrency": profile.reporting_currency().as_str(),
        "preferredPositionWeightLowerBps": profile.preferred_position_weight_lower_bps(),
        "preferredPositionWeightUpperBps": profile.preferred_position_weight_upper_bps(),
        "minimumCashReserve": {
            "amount": reserve.amount().normalize().to_string(),
            "currency": reserve.currency().as_str(),
        },
        "maximumDownsideLossBpsOfMarkedEquity": profile.maximum_downside_loss_bps_of_marked_equity(),
        "availableInvestmentHorizonNanos": profile.available_investment_horizon_nanos().to_string(),
        "acceptedAtUnixNanos": profile.accepted_at().unix_nanos().to_string(),
        "reviewDueAtUnixNanos": profile.review_due_at().unix_nanos().to_string(),
        "digest": hex(profile.digest()),
    })
}

fn catalog_value(catalog: &PortfolioAccountCatalogSnapshot) -> Value {
    let accounts = catalog
        .heads()
        .iter()
        .map(|head| {
            json!({
                "accountId": head.account_id().as_uuid(),
                "portfolioRevisionSha256": hex(head.revision().bytes()),
                "reportingCurrency": head.reporting_currency().as_str(),
                "effectiveAtUnixNanos": head.effective_at().unix_nanos().to_string(),
                "availableAtUnixNanos": head.available_at().map(|at| at.unix_nanos().to_string()),
                "sourceId": head.source_id().as_str(),
                "sourceCoverage": head.source_coverage().iter().map(|source| source.as_str()).collect::<Vec<_>>(),
                "artifactSha256": hex(head.artifact_sha256()),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "digest": evidence_sha256(catalog.digest()),
        "accountCount": catalog.account_count(),
        "accounts": accounts,
    })
}

fn preview_value(preview: &RecommendationSetupPreview) -> Result<Value, ServiceError> {
    let account_id = preview
        .selected_account_id()
        .ok_or(ServiceError::InvalidResult)?;
    let portfolio_revision = preview
        .selected_portfolio_revision()
        .ok_or(ServiceError::InvalidResult)?;
    let reporting_currency = preview
        .selected_reporting_currency()
        .ok_or(ServiceError::InvalidResult)?;
    let profile = preview.profile().ok_or(ServiceError::InvalidResult)?;
    let reserve = profile.minimum_cash_reserve();
    Ok(json!({
        "workspaceId": preview.owner_workspace().as_uuid(),
        "previewId": preview.preview_id().as_uuid(),
        "previewDigest": hex(preview.digest()),
        "currentRevision": preview.current_revision(),
        "resultingRevision": preview.resulting_revision(),
        "currentAuthorityDigest": hex(preview.current_authority_digest()),
        "catalogDigest": evidence_sha256(preview.catalog_digest()),
        "kind": preview.kind().as_str(),
        "accountSelection": {
            "accountId": account_id.as_uuid(),
            "portfolioRevisionSha256": hex(portfolio_revision.bytes()),
            "reportingCurrency": reporting_currency.as_str(),
        },
        "allocationProfile": {
            "preferredPositionWeightLowerBps": profile.preferred_position_weight_lower_bps(),
            "preferredPositionWeightUpperBps": profile.preferred_position_weight_upper_bps(),
            "minimumCashReserve": {
                "amount": reserve.amount().normalize().to_string(),
                "currency": reserve.currency().as_str(),
            },
            "maximumDownsideLossBpsOfMarkedEquity": profile.maximum_downside_loss_bps_of_marked_equity(),
            "availableInvestmentHorizonNanos": profile.available_investment_horizon_nanos().to_string(),
        },
        "issuedAtUnixNanos": preview.issued_at().unix_nanos().to_string(),
        "expiresAtUnixNanos": preview.expires_at().unix_nanos().to_string(),
    }))
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(super::business_arguments(arguments)))
        .map_err(|_| ServiceError::InvalidRequest)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).map_err(|_| ServiceError::InvalidRequest)?;
        bytes[index] = u8::from_str_radix(pair, 16).map_err(|_| ServiceError::InvalidRequest)?;
    }
    if bytes == [0; 32] {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(bytes)
}

fn evidence_sha256(value: EvidenceDigest) -> String {
    debug_assert_eq!(value.algorithm(), DigestAlgorithm::Sha256);
    hex(value.bytes())
}

fn hex(bytes: [u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn map_setup(error: RecommendationSetupError) -> ServiceError {
    match error {
        RecommendationSetupError::InvalidProfile
        | RecommendationSetupError::AccountUnavailable
        | RecommendationSetupError::CurrencyMismatch
        | RecommendationSetupError::StaleRevision
        | RecommendationSetupError::StaleCatalog
        | RecommendationSetupError::PreviewUnavailable
        | RecommendationSetupError::PreviewExpired
        | RecommendationSetupError::InvalidConfirmation
        | RecommendationSetupError::InvalidAsOf
        | RecommendationSetupError::StateChanged
        | RecommendationSetupError::InvalidBackup
        | RecommendationSetupError::RestoreTargetOccupied => ServiceError::InvalidRequest,
        RecommendationSetupError::CrossWorkspacePreview => ServiceError::Unauthorized,
        RecommendationSetupError::CapacityExceeded => ServiceError::ResourceExhausted,
        RecommendationSetupError::RevisionExhausted
        | RecommendationSetupError::CorruptState
        | RecommendationSetupError::Encoding => ServiceError::Internal,
        RecommendationSetupError::Unavailable
        | RecommendationSetupError::RecoveryRequired
        | RecommendationSetupError::TimeUnavailable
        | RecommendationSetupError::Persistence(_) => ServiceError::Unavailable,
    }
}

fn map_catalog(error: PortfolioAccountCatalogError) -> ServiceError {
    match error {
        PortfolioAccountCatalogError::Portfolio(error) => error.as_service_error(),
        PortfolioAccountCatalogError::ResourceExhausted => ServiceError::ResourceExhausted,
        PortfolioAccountCatalogError::CorruptPublication
        | PortfolioAccountCatalogError::CatalogChanged => ServiceError::Unavailable,
    }
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
