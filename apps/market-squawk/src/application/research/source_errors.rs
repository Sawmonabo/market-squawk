//! Shared conversion of normalized source-reference and publication failures for producers and reads.

use market_squawk_data::{
    CurrentPopulationError, IngestError, ListingReferenceError, MarketDataInstrumentCatalogError,
    ParquetStoreError, ProviderMarketEventSelectionError, PythonDatasetCatalogError,
    ResearchUseCatalogError, ResearchUseError,
};
use market_squawk_services::ServiceError;

use super::{map_catalog_error, map_manifest_error};

/// Shared by the existing Markets-token resolver and its durable investment consumer.
pub(crate) fn map_market_definition_read_error(
    error: MarketDataInstrumentCatalogError,
) -> ServiceError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => ServiceError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketDataInstrumentCatalogError::BatchLimitExceeded { .. }
        | MarketDataInstrumentCatalogError::RevisionLimitExceeded
        | MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            ServiceError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::InvalidInput
        | MarketDataInstrumentCatalogError::InvalidPopulationQuery
        | MarketDataInstrumentCatalogError::InvalidLimit => ServiceError::InvalidRequest,
        MarketDataInstrumentCatalogError::AuthorityUnavailable
        | MarketDataInstrumentCatalogError::Storage(_) => ServiceError::Unavailable,
        MarketDataInstrumentCatalogError::PartialBatch { .. }
        | MarketDataInstrumentCatalogError::SourceIdentityConflict
        | MarketDataInstrumentCatalogError::ReferencePositionConflict
        | MarketDataInstrumentCatalogError::DuplicateInstrumentId
        | MarketDataInstrumentCatalogError::StaleRevision
        | MarketDataInstrumentCatalogError::EqualTimeRevisionConflict
        | MarketDataInstrumentCatalogError::CorruptCatalog
        | MarketDataInstrumentCatalogError::Serialization(_) => ServiceError::InvalidResult,
        MarketDataInstrumentCatalogError::SourceAuthority(error) => map_catalog_error(error),
        MarketDataInstrumentCatalogError::ListingAuthority(error) => map_listing_error(error),
        MarketDataInstrumentCatalogError::PublicationAuthority(error) => {
            map_durable_market_ingest_error(*error)
        }
        MarketDataInstrumentCatalogError::BlockingIo(error) => map_parquet_error(error),
    }
}

pub(crate) fn map_point_in_time_read_error(
    error: ProviderMarketEventSelectionError,
) -> ServiceError {
    match error {
        ProviderMarketEventSelectionError::InvalidRequest => ServiceError::InvalidRequest,
        ProviderMarketEventSelectionError::CandidateLimitExceeded
        | ProviderMarketEventSelectionError::Allocation
        | ProviderMarketEventSelectionError::DigestOverflow => ServiceError::ResourceExhausted,
        ProviderMarketEventSelectionError::EvidenceMismatch
        | ProviderMarketEventSelectionError::RestartMismatch => ServiceError::InvalidResult,
        ProviderMarketEventSelectionError::Manifest(error) => map_manifest_error(error),
        ProviderMarketEventSelectionError::Catalog(error) => map_catalog_error(error),
    }
}

pub(crate) fn map_durable_market_ingest_error(error: IngestError) -> ServiceError {
    match error {
        IngestError::Cancelled => ServiceError::Cancelled,
        IngestError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        IngestError::ResearchUse(error) => map_research_use_error(*error),
        IngestError::PublicationAuthorityRevoked | IngestError::AuthorityTransitionRejected => {
            ServiceError::Unauthorized
        }
        IngestError::Parquet(error) => map_parquet_error(error),
        IngestError::Catalog(error) => map_catalog_error(error),
        IngestError::Manifest(error) => map_manifest_error(error),
        IngestError::ListingReference(error) => map_listing_error(error),
        IngestError::MarketDataInstrumentReference(error) => {
            map_market_definition_read_error(*error)
        }
        IngestError::ProviderMarketEventSelection(error) => map_point_in_time_read_error(error),
        _ => ServiceError::Unavailable,
    }
}

pub(super) fn map_parquet_error(error: ParquetStoreError) -> ServiceError {
    match error {
        ParquetStoreError::Cancelled => ServiceError::Cancelled,
        ParquetStoreError::ReadDeadlineExceeded | ParquetStoreError::RecoveryDeadlineExceeded => {
            ServiceError::DeadlineExceeded
        }
        ParquetStoreError::StagingLimitExceeded
        | ParquetStoreError::ReadLimitExceeded
        | ParquetStoreError::SizeOverflow
        | ParquetStoreError::BlockingTaskLimitExceeded
        | ParquetStoreError::RecoveryScanLimit => ServiceError::ResourceExhausted,
        ParquetStoreError::ContentAddressConflict
        | ParquetStoreError::ObjectMetadataMismatch
        | ParquetStoreError::RootCatalogMismatch => ServiceError::InvalidResult,
        _ => ServiceError::Unavailable,
    }
}

fn map_listing_error(error: ListingReferenceError) -> ServiceError {
    match error {
        ListingReferenceError::Cancelled => ServiceError::Cancelled,
        ListingReferenceError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ListingReferenceError::MemoryLimitExceeded => ServiceError::ResourceExhausted,
        ListingReferenceError::InvalidInput
        | ListingReferenceError::InvalidKnowledgeCutoff
        | ListingReferenceError::InvalidLimit => ServiceError::InvalidRequest,
        ListingReferenceError::InvalidSourceContract
        | ListingReferenceError::InvalidRightsCapability
        | ListingReferenceError::RightsUnavailable => ServiceError::Unauthorized,
        ListingReferenceError::SourceRevisionUnavailable
        | ListingReferenceError::SupersededGeneration
        | ListingReferenceError::AuthorityUnavailable
        | ListingReferenceError::Storage(_) => ServiceError::Unavailable,
        ListingReferenceError::PositionConflict
        | ListingReferenceError::CorruptCatalog
        | ListingReferenceError::Serialization(_) => ServiceError::InvalidResult,
    }
}

/// Preserves cancellation and current grant revocation through population replay.
pub(crate) fn map_research_use_error(error: ResearchUseCatalogError) -> ServiceError {
    match error {
        ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
        ResearchUseCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ResearchUseCatalogError::LimitExceeded
        | ResearchUseCatalogError::Contract(
            ResearchUseError::AllocationFailed | ResearchUseError::CanonicalEncodingOverflow,
        ) => ServiceError::ResourceExhausted,
        ResearchUseCatalogError::Denied { .. }
        | ResearchUseCatalogError::Expired
        | ResearchUseCatalogError::Revoked
        | ResearchUseCatalogError::InvalidPermitSession => ServiceError::Unauthorized,
        ResearchUseCatalogError::InvalidGrant
        | ResearchUseCatalogError::InvalidRevocation
        | ResearchUseCatalogError::Contract(_) => ServiceError::InvalidRequest,
        ResearchUseCatalogError::UnknownGeneration => ServiceError::NotFound,
        ResearchUseCatalogError::InvalidPublication | ResearchUseCatalogError::CorruptCatalog => {
            ServiceError::InvalidResult
        }
        ResearchUseCatalogError::Catalog(error) => map_catalog_error(error),
        ResearchUseCatalogError::Sqlite(_) => ServiceError::Unavailable,
    }
}

pub(crate) fn map_current_population_error(error: CurrentPopulationError) -> ServiceError {
    match error {
        CurrentPopulationError::ResearchUseUnavailable => ServiceError::Unauthorized,
        CurrentPopulationError::ResearchUse(error) => map_research_use_error(error),
        CurrentPopulationError::InvalidInput => ServiceError::InvalidRequest,
        CurrentPopulationError::LimitExceeded => ServiceError::ResourceExhausted,
        CurrentPopulationError::Unavailable
        | CurrentPopulationError::Superseded
        | CurrentPopulationError::AuthorityUnavailable => ServiceError::Unavailable,
        CurrentPopulationError::Canonical(error) => map_market_definition_read_error(error),
        CurrentPopulationError::Listing(error) => map_listing_error(error),
    }
}

pub(crate) fn map_python_dataset_error(error: PythonDatasetCatalogError) -> ServiceError {
    match error {
        PythonDatasetCatalogError::Cancelled => ServiceError::Cancelled,
        PythonDatasetCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        PythonDatasetCatalogError::LimitExceeded => ServiceError::ResourceExhausted,
        PythonDatasetCatalogError::PopulationResearchUse(error) => map_research_use_error(*error),
        PythonDatasetCatalogError::ResearchAuthorizationExpired => ServiceError::Unauthorized,
        PythonDatasetCatalogError::UnknownAdmission => ServiceError::NotFound,
        PythonDatasetCatalogError::Catalog(error) => map_catalog_error(error),
        PythonDatasetCatalogError::CorruptAdmission
        | PythonDatasetCatalogError::InvalidProductionEvidence
        | PythonDatasetCatalogError::ConflictingProductionAdmission
        | PythonDatasetCatalogError::ProductionReceiptEncoding
        | PythonDatasetCatalogError::Parquet(_)
        | PythonDatasetCatalogError::Arrow(_)
        | PythonDatasetCatalogError::ArrowDecode(_) => ServiceError::InvalidResult,
        PythonDatasetCatalogError::Path(_)
        | PythonDatasetCatalogError::Artifact(_)
        | PythonDatasetCatalogError::Sqlite(_)
        | PythonDatasetCatalogError::Io(_) => ServiceError::Unavailable,
    }
}
