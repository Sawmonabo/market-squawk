//! Shared failure classification for corporate-action source acquisition and exact replay.

use market_squawk_services::{RequestContext, ServiceError};
use std::time::Instant;

pub(super) fn check(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
pub(super) fn controlled(context: &RequestContext, otherwise: ServiceError) -> ServiceError {
    check(context).err().unwrap_or(otherwise)
}

pub(crate) fn map_research_error(error: crate::ResearchServiceError) -> ServiceError {
    use crate::ResearchServiceError;
    use market_squawk_data::IngestError;
    match error {
        ResearchServiceError::Ingest(IngestError::SealedProviderCapture(error))
        | ResearchServiceError::ProviderCaptureStore(error) => map_store_error(error),
        ResearchServiceError::Ingest(IngestError::ProviderCapture(error)) => match error {
            market_squawk_sources::ProviderCaptureError::PageLimitExceeded { .. }
            | market_squawk_sources::ProviderCaptureError::ByteLimitExceeded { .. }
            | market_squawk_sources::ProviderCaptureError::AllocationFailed => {
                ServiceError::ResourceExhausted
            }
            _ => ServiceError::InvalidResult,
        },
        ResearchServiceError::Dataset(market_squawk_data::DatasetBuildError::LimitExceeded) => {
            ServiceError::ResourceExhausted
        }
        ResearchServiceError::Dataset(market_squawk_data::DatasetBuildError::PythonDataset(
            error,
        )) => crate::application::research::map_python_dataset_error(error),
        ResearchServiceError::Catalog(error) => map_ingest_error(IngestError::Catalog(error)),
        ResearchServiceError::Manifest(error) => map_ingest_error(IngestError::Manifest(error)),
        ResearchServiceError::Ingest(error) => map_ingest_error(error),
        ResearchServiceError::ProviderCaptureSealWorkerUnavailable
        | ResearchServiceError::IdentityOverflow => ServiceError::Internal,
        ResearchServiceError::Rights(_) => ServiceError::Unauthorized,
        ResearchServiceError::Path(_) => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}

pub(super) fn map_ingest_error(error: market_squawk_data::IngestError) -> ServiceError {
    use market_squawk_data::IngestError;
    use market_squawk_services::ServiceError;
    // Reuse the typed shared cases only; its fallback is intentionally not absence evidence.
    match error {
        IngestError::AuthorityLockPoisoned
        | IngestError::ProviderCaptureRecoveryWorkerUnavailable => ServiceError::Internal,
        IngestError::Manifest(market_squawk_data::ManifestCatalogError::LockPoisoned)
        | IngestError::Catalog(market_squawk_data::CatalogError::WriterRegistryUnavailable)
        | IngestError::Parquet(
            market_squawk_data::ParquetStoreError::BlockingTaskFailed
            | market_squawk_data::ParquetStoreError::RootAuthorityRegistryUnavailable,
        ) => ServiceError::Internal,
        IngestError::Parquet(
            market_squawk_data::ParquetStoreError::InvalidConfiguration
            | market_squawk_data::ParquetStoreError::InvalidPublicationLease
            | market_squawk_data::ParquetStoreError::InvalidStagedObject
            | market_squawk_data::ParquetStoreError::CatalogRestoreConflict
            | market_squawk_data::ParquetStoreError::Arrow(_)
            | market_squawk_data::ParquetStoreError::Parquet(_)
            | market_squawk_data::ParquetStoreError::ArtifactPath(_),
        ) => ServiceError::InvalidResult,
        error @ (IngestError::Cancelled
        | IngestError::DeadlineExceeded
        | IngestError::Parquet(_)
        | IngestError::Catalog(_)
        | IngestError::Manifest(_)
        | IngestError::ResearchUse(_)
        | IngestError::ListingReference(_)
        | IngestError::MarketDataInstrumentReference(_)
        | IngestError::ProviderMarketEventSelection(_)) => {
            crate::application::research::map_durable_market_ingest_error(error)
        }
        IngestError::AuthorityBusy | IngestError::PublicationAuthorityRevoked => {
            ServiceError::Unavailable
        }
        _ => ServiceError::InvalidResult,
    }
}

pub(super) fn map_store_error(
    error: market_squawk_platform::SealedResearchJournalStoreError,
) -> ServiceError {
    use market_squawk_platform::{
        ResearchObjectControlError as C, SealedResearchJournalStoreError as E,
    };
    match error {
        E::OperationLockPoisoned | E::ObjectControl(C::Unavailable) => ServiceError::Internal,
        E::ObjectControl(C::Cancelled) => ServiceError::Cancelled,
        E::ObjectControl(C::DeadlineExceeded) => ServiceError::DeadlineExceeded,
        E::FrameLimitExceeded { .. }
        | E::ByteLimitExceeded { .. }
        | E::ObjectByteLimitExceeded { .. }
        | E::ObjectChunkLimitExceeded { .. }
        | E::ObjectAllocationFailed => ServiceError::ResourceExhausted,
        E::Io { .. } | E::AlreadyOwned | E::ObjectStageActive => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}

pub(super) fn map_capability_error(
    error: crate::application::market_runtime::AlpacaHistoricalCapabilityError,
) -> ServiceError {
    use crate::application::market_runtime::AlpacaHistoricalCapabilityError;
    match error {
        AlpacaHistoricalCapabilityError::Cancelled => ServiceError::Cancelled,
        AlpacaHistoricalCapabilityError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        AlpacaHistoricalCapabilityError::Revoked | AlpacaHistoricalCapabilityError::Stale => {
            ServiceError::Unavailable
        }
    }
}

pub(super) fn map_calendar_error(
    error: crate::application::market_calendar::CompletedMarketSessionError,
) -> ServiceError {
    use crate::application::market_calendar::CompletedMarketSessionError as E;
    match error {
        E::Cancelled => ServiceError::Cancelled,
        E::DeadlineExceeded => ServiceError::DeadlineExceeded,
        E::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        E::InvalidRequest => ServiceError::InvalidRequest,
        E::InvalidEvidence => ServiceError::InvalidResult,
        E::Unavailable => ServiceError::Unavailable,
    }
}

pub(super) fn map_plan_error(
    error: super::ApplicableActionPlanError,
    context: &RequestContext,
) -> ServiceError {
    use super::ApplicableActionPlanError as E;
    match error {
        E::SourceRead(error) => error,
        E::InvalidEvidence => ServiceError::InvalidResult,
        E::UnresolvedApplicableActions | E::IncompleteOrdinaryCoverage => ServiceError::Unavailable,
        E::Interrupted => controlled(context, ServiceError::Cancelled),
    }
}

pub(super) fn map_continuity_error(
    error: super::SourceForecastUnitContinuityError,
    context: &RequestContext,
) -> ServiceError {
    use super::SourceForecastUnitContinuityError as E;
    match error {
        E::InvalidEvidence | E::OriginalSourceFrameChanged => ServiceError::InvalidResult,
        E::Interrupted => controlled(context, ServiceError::Cancelled),
        E::MissingFreshSourceAnchor
        | E::OriginalCoordinateUnavailable
        | E::FinalSessionUnavailable
        | E::SourceCaptureOutsideFreshnessBound
        | E::SourceShareRelationUnavailable
        | E::KnownShareOrLifecycleChange
        | E::UnresolvedApplicableLifecycle => ServiceError::Unavailable,
    }
}

pub(super) fn map_query_identity_error(
    error: market_squawk_data::CorporateActionQueryIdentityError,
) -> ServiceError {
    use market_squawk_data::CorporateActionQueryIdentityError as E;
    match error {
        E::MissingIdentity => ServiceError::Unavailable,
        E::Mismatch => ServiceError::InvalidResult,
        E::ResourceBound => ServiceError::ResourceExhausted,
        E::Catalog(error) => super::super::map_market_definition_read_error(error),
    }
}

pub(super) fn map_query_error(error: super::SourceActionQueryError) -> ServiceError {
    match error {
        super::SourceActionQueryError::Mismatch => ServiceError::InvalidResult,
        super::SourceActionQueryError::Identity(error) => map_query_identity_error(error),
        super::SourceActionQueryError::Source(error) => map_adapter_error(error),
    }
}

pub(super) fn map_runtime_action_error(
    error: crate::application::market_runtime::AlpacaHistoricalPlanOperationError,
) -> ServiceError {
    use crate::application::market_runtime::AlpacaHistoricalPlanOperationError as E;
    match error {
        E::Capability(error) => map_capability_error(error),
        E::Adapter(error) => map_adapter_error(error),
    }
}

pub(super) fn map_adapter_error(error: market_squawk_adapter_alpaca::AlpacaError) -> ServiceError {
    use market_squawk_adapter_alpaca::AlpacaError as E;
    match error {
        E::Cancelled => ServiceError::Cancelled,
        E::DeadlineExceeded => ServiceError::DeadlineExceeded,
        E::BodyTooLarge | E::Allocation => ServiceError::ResourceExhausted,
        E::Network => ServiceError::Unavailable,
        _ => ServiceError::InvalidResult,
    }
}

pub(super) fn map_source_read_error(
    error: market_squawk_data::CorporateActionSourceReadError,
    context: &RequestContext,
) -> ServiceError {
    use market_squawk_data::CorporateActionSourceReadError as E;
    match error {
        E::InvalidEvidence | E::Arrow(_) => ServiceError::InvalidResult,
        E::FutureEvidence => ServiceError::Unavailable,
        E::ResourceBound => ServiceError::ResourceExhausted,
        E::Interrupted => controlled(context, ServiceError::Cancelled),
        E::Manifest(error) => map_ingest_error(market_squawk_data::IngestError::Manifest(error)),
        E::Parquet(error) => map_ingest_error(market_squawk_data::IngestError::Parquet(error)),
    }
}

pub(crate) fn map_analytical_error(error: market_squawk_data::AnalyticalReadError) -> ServiceError {
    use market_squawk_data::{AnalyticalReadError as E, QueryError as Q, IngestError};
    use market_squawk_platform::ResearchObjectControlError as C;
    match error {
        E::NativeSessionControl(C::Cancelled) => ServiceError::Cancelled,
        E::NativeSessionControl(C::DeadlineExceeded) => ServiceError::DeadlineExceeded,
        E::NativeSessionControl(C::Unavailable) => ServiceError::Internal,
        E::ForecastDatasetUnavailable => ServiceError::NotFound,
        E::InvalidLimit | E::InstrumentLimitExceeded | E::InvalidMarketBarLimit
        | E::MarketBarResultRequiresInline | E::InputEpochResultRequiresInline => ServiceError::ResourceExhausted,
        E::Manifest(error) => map_ingest_error(IngestError::Manifest(error)),
        E::Parquet(error) => map_ingest_error(IngestError::Parquet(error)),
        E::PythonDataset(error) => crate::application::research::map_python_dataset_error(error),
        E::Query(error) => match error {
            Q::Cancelled => ServiceError::Cancelled,
            Q::DeadlineExceeded => ServiceError::DeadlineExceeded,
            Q::InvalidLimits | Q::AstLimitExceeded | Q::PlanLimitExceeded | Q::PartitionLimitExceeded
            | Q::RowLimitExceeded { .. } | Q::ByteLimitExceeded { .. } | Q::MemoryLimitExceeded { .. }
            | Q::SizeOverflow | Q::DependencyAllocationContract | Q::BlockingTaskLimitExceeded
            | Q::ReaderMemoryBoundExceeded | Q::ArtifactStoreRequired | Q::ArtifactAuthorityRequired => ServiceError::ResourceExhausted,
            Q::Artifact(error) => map_ingest_error(IngestError::Parquet(error)),
            Q::Catalog(error) => map_ingest_error(IngestError::Catalog(error)),
            Q::InvalidSource | Q::ManifestPinMismatch | Q::ArrowConversion(_) | Q::Arrow(_)
            | Q::InvalidMonetaryCell | Q::UnsupportedMonetaryScale => ServiceError::InvalidResult,
            _ => ServiceError::Internal,
        },
        _ => ServiceError::InvalidResult,
    }
}
