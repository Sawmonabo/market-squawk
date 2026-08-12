//! Reproducible point-in-time feature/label dataset construction and authorized publication.

use std::fmt;
use std::sync::{Arc, Mutex};

use market_squawk_domain::Timestamp;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::analytical_backup::AnalyticalOperationGate;
use crate::{
    AnalyticalDataService, CatalogAuthority, ResearchUse, ResearchUseDecisionDigest,
    ResearchUseGraphDigest, ResearchUseRequest,
};

#[path = "dataset_builder/admission.rs"]
mod admission;
#[path = "dataset_builder/build.rs"]
mod build;
#[path = "dataset_builder/canonical.rs"]
mod canonical;
#[path = "dataset_builder/export.rs"]
mod export;
#[path = "dataset_builder/model.rs"]
mod model;

pub use admission::PythonDatasetAdmission;
pub use export::{FeatureLabelPythonExport, MAX_FEATURE_LABEL_EXPORT_BYTES};

pub use model::{
    ChronologicalSplitPolicy, ComponentAdjustmentEvidence, ComponentKind, ComponentScope,
    ComponentSelector, ComponentValue, CorporateActionSensitivity, DatasetBuildInputs,
    DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildRequest, DatasetExample,
    DatasetOutputAuthorization, DatasetSplit, DatasetSplitCounts, FEATURE_LABEL_PROBABILITY_UNIT,
    FEATURE_LABEL_RETURN_UNIT, FeatureLabelComponentInput, FeatureLabelComponentSpec,
    FeatureLabelDataset, FeatureLabelMeasurement, FeatureLabelMeasurementBinding,
    MissingValuePolicy,
};

/// Process-local authority that must remain live through derived-generation publication.
pub trait DatasetBuildPrecommitAuthority: fmt::Debug + Send + Sync {
    /// Revalidates the exact caller authority immediately before the derived-generation commit.
    fn validate_precommit(&self) -> Result<(), DatasetBuildError>;

    /// Records that the derived-generation commit succeeded before any later fallible work.
    fn commit_succeeded(&self);
}

/// Immutable identities proven by one successful pre-read research-use evaluation.
///
/// This receipt is evidence for orchestration only. It contains no catalog session, permit,
/// publication authority, or reusable capability; the opaque permit minted by the evaluation is
/// dropped before this value is returned. Every build still performs its own authorization before
/// reading inputs and again at its durable publication boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetResearchUsePreflightReceipt {
    request: ResearchUseRequest,
    decision_digest: ResearchUseDecisionDigest,
    graph_digest: ResearchUseGraphDigest,
    expires_at: Timestamp,
}

impl DatasetResearchUsePreflightReceipt {
    /// Returns exact roots, downstream use, and bounds evaluated by the authority.
    pub const fn request(&self) -> &ResearchUseRequest {
        &self.request
    }

    /// Returns the canonical durable decision identity produced by this evaluation.
    pub const fn decision_digest(&self) -> ResearchUseDecisionDigest {
        self.decision_digest
    }

    /// Returns the canonical exact transitive parent graph evaluated by the authority.
    pub const fn graph_digest(&self) -> ResearchUseGraphDigest {
        self.graph_digest
    }

    /// Returns the independently authorized downstream research use.
    pub const fn research_use(&self) -> ResearchUse {
        self.request.requested_use()
    }

    /// Returns the exclusive expiry of the ephemeral decision evaluated by this preflight.
    ///
    /// The receipt remains identity evidence after this time but never carries usable authority.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Rights-bound builder scoped to one active analytical catalog and artifact root.
pub struct DatasetBuilderService<'service> {
    service: &'service AnalyticalDataService,
    authority: Arc<Mutex<CatalogAuthority>>,
    operation_gate: AnalyticalOperationGate,
}

impl fmt::Debug for DatasetBuilderService<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatasetBuilderService")
            .field("service", &"[SEALED ANALYTICAL COMPOSITION]")
            .field("authority", &"[SEALED CATALOG AUTHORITY]")
            .finish()
    }
}

impl<'service> DatasetBuilderService<'service> {
    pub(crate) const fn new(
        service: &'service AnalyticalDataService,
        authority: Arc<Mutex<CatalogAuthority>>,
        operation_gate: AnalyticalOperationGate,
    ) -> Self {
        Self {
            service,
            authority,
            operation_gate,
        }
    }

    /// Re-resolves and returns the immutable catalog admission for one producer-owned result.
    pub fn python_admission(
        &self,
        dataset: &FeatureLabelDataset,
    ) -> Result<PythonDatasetAdmission, DatasetBuildError> {
        admission::register(self, dataset)
    }

    /// Re-resolves the exact parent graph and validates its current research-use authority.
    ///
    /// This is an admission-only preflight for guided preparation. Publication performs the same
    /// validation again before reading inputs and at its durable commit boundary.
    pub fn validate_request_authority(
        &self,
        request: &DatasetBuildRequest,
        cancellation: &CancellationToken,
    ) -> Result<(), DatasetBuildError> {
        build::validate_request_authority(self, request, cancellation)
    }

    /// Evaluates current ResearchUse rights before an orchestrator reads any parent generation.
    ///
    /// Only immutable request and graph identities cross this boundary. The authorization's
    /// single-use permit is deliberately destroyed here and cannot be supplied to `build`; build
    /// therefore re-traverses and reauthorizes the graph under its existing commit protocol. A
    /// later [`DatasetBuildRequest`] must retain these exact roots, requested use, and limits;
    /// equality with [`DatasetResearchUsePreflightReceipt::request`] is the producer's explicit
    /// composition check, not authority conferred by this receipt.
    pub fn preflight_research_use(
        &self,
        request: ResearchUseRequest,
        cancellation: &CancellationToken,
    ) -> Result<DatasetResearchUsePreflightReceipt, DatasetBuildError> {
        let authority = self
            .authority
            .lock()
            .map_err(|_| DatasetBuildError::AuthorityLockPoisoned)?;
        let authorization = authority.authorize_research_use(request.clone(), cancellation)?;
        if authorization.research_use() != request.requested_use() {
            return Err(DatasetBuildError::InvalidRequest);
        }
        let receipt = DatasetResearchUsePreflightReceipt {
            request,
            decision_digest: authorization.decision_digest(),
            graph_digest: authorization.graph().digest(),
            expires_at: authorization.expires_at(),
        };
        drop(authorization);
        Ok(receipt)
    }
}

/// Asynchronous bounded dataset-construction service.
#[allow(
    async_fn_in_trait,
    reason = "the local service contract intentionally preserves native cancellation"
)]
pub trait DatasetBuilder {
    /// Builds and atomically records one exact derived feature/label generation.
    async fn build(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<FeatureLabelDataset, DatasetBuildError>;

    /// Builds while retaining exact caller authority through derived-generation publication.
    async fn build_with_precommit_authority(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn DatasetBuildPrecommitAuthority>,
    ) -> Result<FeatureLabelDataset, DatasetBuildError>;
}

impl DatasetBuilder for DatasetBuilderService<'_> {
    async fn build(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
    ) -> Result<FeatureLabelDataset, DatasetBuildError> {
        build::build(self, request, cancellation, None).await
    }

    async fn build_with_precommit_authority(
        &self,
        request: DatasetBuildRequest,
        cancellation: CancellationToken,
        precommit_authority: Arc<dyn DatasetBuildPrecommitAuthority>,
    ) -> Result<FeatureLabelDataset, DatasetBuildError> {
        build::build(self, request, cancellation, Some(precommit_authority)).await
    }
}

/// Dataset construction, temporal admission, authority, or publication failure.
#[derive(Debug, Error)]
pub enum DatasetBuildError {
    /// A request is empty, inconsistent, duplicated, or outside its closed grammar.
    #[error("dataset build request is invalid")]
    InvalidRequest,
    /// Caller-selected work, time, or retained-memory bounds are invalid.
    #[error("dataset build limits are invalid")]
    InvalidLimits,
    /// Work exceeded one caller-selected bound.
    #[error("dataset build resource limit was exceeded")]
    LimitExceeded,
    /// An exact input generation was absent or did not retain research observations.
    #[error("dataset build input generation is invalid")]
    InvalidInputGeneration,
    /// A feature or label could not prove its requested point-in-time source family.
    #[error("dataset component selector does not match point-in-time evidence")]
    ComponentEvidenceMismatch,
    /// A value's producer evidence does not prove the requested corporate-action treatment.
    #[error("dataset component does not prove its requested corporate-action treatment")]
    ComponentAdjustmentMismatch,
    /// A missing component was forbidden by the selected missing-value policy.
    #[error("dataset build encountered a forbidden missing component")]
    MissingValueRejected,
    /// Chronological cutoffs would leak label-period data across a split boundary.
    #[error("dataset build violates chronological leakage boundaries")]
    TemporalLeakage,
    /// Historical universe evidence excluded the requested example instrument.
    #[error("dataset example instrument is absent from its historical universe")]
    InstrumentOutsideUniverse,
    /// A claimed historical membership did not resolve to one exact pinned source observation.
    #[error("dataset historical-universe evidence does not match its pinned parent")]
    UniverseEvidenceMismatch,
    /// Corporate-action policy retained unresolved economic terms.
    #[error("corporate-action treatment contains unresolved economics")]
    UnresolvedCorporateAction,
    /// All examples were removed by the explicit missing-value policy.
    #[error("dataset build produced no rows")]
    EmptyDataset,
    /// A bounded canonical Python export descriptor could not be encoded.
    #[error("feature-label export descriptor could not be encoded within its bound")]
    ExportEncoding,
    /// The caller cancelled before the durable derived-generation commit.
    #[error("dataset build was cancelled")]
    Cancelled,
    /// The caller-selected monotonic deadline elapsed before commit.
    #[error("dataset build deadline elapsed")]
    DeadlineExceeded,
    /// The caller's publication authority was revoked before the durable generation commit.
    #[error("dataset build publication authority was revoked")]
    PublicationAuthorityRevoked,
    /// The process-owned catalog writer lock is unavailable.
    #[error("dataset build catalog authority is unavailable")]
    AuthorityLockPoisoned,
    /// Point-in-time selection rejected the bounded candidate set.
    #[error("point-in-time dataset selection failed")]
    PointInTime,
    /// Historical-universe construction failed closed.
    #[error("historical-universe construction failed: {0}")]
    Universe(#[from] crate::UniverseError),
    /// Corporate-action planning failed closed.
    #[error("corporate-action planning failed: {0}")]
    CorporateAction(#[from] crate::CorporateActionError),
    /// Registered Arrow conversion or validation failed.
    #[error("feature/label Arrow construction failed: {0}")]
    Arrow(#[from] crate::ArrowConversionError),
    /// Controlled Parquet publication or verification failed.
    #[error("feature/label Parquet publication failed: {0}")]
    Parquet(#[from] crate::ParquetStoreError),
    /// Immutable manifest planning failed.
    #[error("feature/label manifest planning failed: {0}")]
    ManifestPlan(#[from] crate::ManifestPlanError),
    /// Immutable manifest lookup failed.
    #[error("feature/label manifest lookup failed: {0}")]
    ManifestCatalog(#[from] crate::ManifestCatalogError),
    /// The composed analytical service rejected a pin or composition operation.
    #[error("dataset build analytical service failed: {0}")]
    Ingest(#[from] crate::IngestError),
    /// Source or output rights admission failed.
    #[error("dataset build rights contract failed: {0}")]
    Rights(#[from] crate::RightsError),
    /// Durable catalog authority rejected an operation.
    #[error("dataset build catalog operation failed: {0}")]
    Catalog(#[from] crate::CatalogError),
    /// Research-use traversal or publication authority rejected the build.
    #[error("dataset build research-use authority failed: {0}")]
    ResearchUse(#[from] crate::ResearchUseCatalogError),
    /// Immutable Python dataset admission failed.
    #[error("dataset build Python admission failed: {0}")]
    PythonDataset(#[from] crate::PythonDatasetCatalogError),
    /// Dataset schema construction or resolution failed.
    #[error("dataset build schema is invalid: {0}")]
    Schema(#[from] crate::DatasetSchemaError),
}
