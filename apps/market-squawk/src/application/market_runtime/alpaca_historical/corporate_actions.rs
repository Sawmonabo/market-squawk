//! Finite corporate-action capture under the existing active account operation and rate owner.

use super::*;
use market_squawk_adapter_alpaca::{
    AlpacaCorporateActionsClient, AlpacaCorporateActionsRequest, AlpacaCorporateActionsSealRejoin,
};
use market_squawk_domain::{
    CoverageDelay, DeliveryEvidence, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence,
};
use market_squawk_sources::{
    ApiEndpointRule, CoverageDomain, EndpointPolicy, NetworkAccessPolicy, PathScope,
    ProviderCaptureSealRequest, QueryParameterRule, QuerySensitivity, SourceCoverage,
    SourceMetadataInput,
};

const ENDPOINT: &str = "https://data.alpaca.markets/v1/corporate-actions";
// This is a reviewed application normalization/request contract, not provider event-time proof.
// The complete declaration and original account metadata are part of the new metadata evidence.
const CONTRACT: &str = "alpaca-corporate-actions/v1;region=us;types=all16;data_quality=all;sort=asc;process-dates=inclusive;maximum-pages=16;page-limit=1000;currency=explicit-only;dates=native-civil;availability=local-first-observed;terminal-query-is-not-economic-absence;creation-and-processing-may-be-delayed";

impl AlpacaHistoricalRuntimeCapability {
    /// Derives a separate nonmarket reference profile from the exact active account authority.
    /// Historical bars keep their original endpoint allowlist, revision, and content evidence.
    pub(crate) fn corporate_action_metadata(
        &self,
    ) -> Result<SourceMetadata, AlpacaHistoricalPlanOperationError> {
        let _operation = self.inner.admit()?;
        self.validate_current_now()?;
        let metadata = metadata(self.historical_metadata(), self.historical_request_bounds())?;
        self.validate_current_now()?;
        Ok(metadata)
    }

    /// The existing reviewed owner-local, source-wide grant is unchanged. This does not issue
    /// new rights or use the bars-only metadata as an action endpoint authorization.
    pub(crate) fn corporate_action_rights(&self) -> &ResearchRightsAuthority {
        self.historical_rights()
    }

    pub(crate) async fn acquire_corporate_actions(
        &self,
        request: AlpacaCorporateActionsRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        (AlpacaCorporateActionsSealRejoin, ProviderCaptureSealRequest),
        AlpacaHistoricalPlanOperationError,
    > {
        ensure_before(deadline, cancellation)?;
        let _operation = self.inner.admit()?;
        self.require_current(deadline, cancellation).await?;
        let metadata = self.corporate_action_metadata()?;
        let client =
            AlpacaCorporateActionsClient::try_new(metadata, self.historical_request_bounds())?;
        let (credentials, budget) = self.inner.historical_authority()?;
        let acquired = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => return Err(AlpacaHistoricalCapabilityError::Revoked.into()),
            () = cancellation.cancelled() => return Err(AlpacaHistoricalCapabilityError::Cancelled.into()),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(AlpacaHistoricalCapabilityError::DeadlineExceeded.into()),
            result = client.acquire_complete(&credentials, &budget, request, deadline, cancellation) => result?,
        };
        drop(client);
        drop(credentials);
        drop(budget);
        self.require_current(deadline, cancellation).await?;
        Ok(acquired)
    }
}

fn metadata(
    parent: &SourceMetadata,
    bounds: HttpRequestBounds,
) -> Result<SourceMetadata, market_squawk_adapter_alpaca::AlpacaError> {
    use market_squawk_adapter_alpaca::AlpacaError;
    AlpacaHistoricalEquityConfig::validate_parent_metadata(parent, bounds)?;
    let public = |name: &str, maximum| {
        QueryParameterRule::try_new(
            SourceIdentifier::try_from(name)?,
            maximum,
            false,
            QuerySensitivity::Public,
        )
        .map_err(AlpacaError::from)
    };
    let exact = |name: &str, value: &str| {
        QueryParameterRule::try_new_exact_public(
            SourceIdentifier::try_from(name)?,
            SourceIdentifier::try_from(value)?,
        )
        .map_err(AlpacaError::from)
    };
    let policy = EndpointPolicy::try_from_api_rules(
        vec![ApiEndpointRule::try_new(
            ENDPOINT,
            PathScope::Exact,
            vec![
                public("symbols", 1_056)?,
                public("start", 10)?,
                public("end", 10)?,
                exact("region", "us")?,
                exact("data_quality", "all")?,
                exact("limit", "1000")?,
                exact("sort", "asc")?,
                public("page_token", 2_048)?,
            ],
            8,
            4_096,
        )?],
        bounds,
    )?;
    // Retain the complete original metadata and actual new route policy in the derivative hash.
    // Reusing a parent digest for changed coverage would misidentify the original authority.
    let declaration =
        serde_json::to_vec(&(CONTRACT, parent, &policy)).map_err(|_| AlpacaError::Protocol)?;
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/alpaca-action-account-metadata/v1\0");
    hash.update(&declaration);
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
    let mut hexadecimal = String::with_capacity(64);
    use std::fmt::Write as _;
    for byte in digest.bytes() {
        write!(&mut hexadecimal, "{byte:02x}").map_err(|_| AlpacaError::Protocol)?;
    }
    let evidence = ExactPayloadEvidence::from_content_digest(digest);
    SourceMetadata::try_new(SourceMetadataInput::new(
        parent.schema_version(),
        parent.source_id().clone(),
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(format!(
                "alpaca-actions-{hexadecimal}"
            ))?),
            evidence.clone(),
        ),
        parent.source_class(),
        parent.provider().clone(),
        parent.authorization().clone(),
        SourceCoverage::try_non_instrument(
            evidence,
            parent.authorization().effective_interval(),
            CoverageDomain::CorporateActions,
            CoverageDelay::NotApplicable,
            DeliveryEvidence::AuthorizedBroker,
        )?,
        parent.quality_ceiling(),
        NetworkAccessPolicy::Allowlisted(policy),
        parent.freshness_policy(),
        parent.budget_policy().cloned(),
        parent.capabilities(),
        parent.protocol_profile().clone(),
    ))
    .map_err(AlpacaError::from)
}
