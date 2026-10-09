//! Operation-owned original-manifest read receipts for financial pages and preparation.
use super::*;
use market_squawk_data::AuthorizedResearchRead;

/// Each receipt authorizes one use of the same original manifest for this operation only.
/// These read receipts cannot authorize durable derived publication.
#[derive(Debug)]
pub(crate) struct FinancialReadAuthorization {
    receipts: Vec<Arc<AuthorizedResearchRead>>,
}

impl FinancialReadAuthorization {
    pub(super) fn no_selected_rows() -> Self {
        Self {
            receipts: Vec::new(),
        }
    }

    pub(super) async fn recheck(
        &self,
        research: &ResearchService,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        check(deadline, cancellation)?;
        for receipt in &self.receipts {
            research
                .recheck_research_read(Arc::clone(receipt), deadline, cancellation)
                .await
                .map_err(map_research_error)?
                .map_err(|error| match error {
                    ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
                    ResearchUseCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
                    _ => ServiceError::Unavailable,
                })?;
        }
        check(deadline, cancellation)
    }
}

pub(crate) async fn authorize_financial_manifest(
    research: &ResearchService,
    manifest: &DatasetManifestRef,
    local_analysis: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<FinancialReadAuthorization>, ServiceError> {
    let roots: Vec<DatasetManifestRef> = vec![manifest.clone()];
    let uses: &[ResearchUse] = if local_analysis {
        &[ResearchUse::Display, ResearchUse::LocalAnalysis]
    } else {
        &[ResearchUse::Display]
    };
    let mut receipts = Vec::new();
    for use_kind in uses {
        check(deadline, cancellation)?;
        let duration = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(
                MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
            ));
        let request = ResearchUseRequest::try_new(
            roots.clone(),
            *use_kind,
            ResearchUseLimits::try_new(
                1,
                MAX_RESEARCH_USE_GRAPH_NODES,
                MAX_RESEARCH_USE_EDGES,
                MAX_RESEARCH_USE_SOURCES,
                MAX_RESEARCH_USE_RETAINED_BYTES,
                duration,
                Duration::from_secs(MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS),
            )
            .map_err(|_| ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        // Both display and transient ratio calculation retain snapshot-only authority.
        // The final page rechecks these exact receipts rather than authorizing again.
        let authorization = research
            .authorize_research_read(request, deadline, cancellation)
            .await
            .map_err(map_research_error)?;
        let authorization = match authorization {
            Ok(authorization) => authorization,
            Err(ResearchUseCatalogError::Cancelled) => return Err(ServiceError::Cancelled),
            Err(ResearchUseCatalogError::DeadlineExceeded) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            Err(_) => return Ok(None),
        };
        if authorization.research_use() != *use_kind
            || authorization.graph().roots() != roots.as_slice()
            || Utc::now()
                .timestamp_nanos_opt()
                .is_none_or(|now| now >= authorization.expires_at().unix_nanos())
        {
            return Err(ServiceError::InvalidResult);
        }
        receipts.push(authorization);
    }
    check(deadline, cancellation)?;
    Ok(Some(FinancialReadAuthorization { receipts }))
}
