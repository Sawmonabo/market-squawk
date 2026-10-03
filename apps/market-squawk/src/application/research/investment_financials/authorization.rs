//! Fresh original-manifest permission checks shared by financial preparation and page reads.
use super::*;

pub(crate) async fn authorize_financial_manifest(
    research: &ResearchService,
    manifest: &DatasetManifestRef,
    local_analysis: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<bool, ServiceError> {
    let roots: Vec<DatasetManifestRef> = vec![manifest.clone()];
    let uses: &[ResearchUse] = if local_analysis {
        &[ResearchUse::Display, ResearchUse::LocalAnalysis]
    } else {
        &[ResearchUse::Display]
    };
    let mut admitted = true;
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
        let authorization = research
            .authorize_research_use(request, deadline, cancellation)
            .await
            .map_err(map_research_error)?;
        let authorization = match authorization {
            Ok(authorization) => authorization,
            Err(ResearchUseCatalogError::Cancelled) => return Err(ServiceError::Cancelled),
            Err(ResearchUseCatalogError::DeadlineExceeded) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            Err(_) => {
                admitted = false;
                break;
            }
        };
        if authorization.research_use() != *use_kind
            || authorization.graph().roots() != roots.as_slice()
            || Utc::now()
                .timestamp_nanos_opt()
                .is_none_or(|now| now >= authorization.expires_at().unix_nanos())
        {
            return Err(ServiceError::InvalidResult);
        }
    }
    Ok(admitted)
}
