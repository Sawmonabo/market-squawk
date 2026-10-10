//! Native Treasury page failures retained until the application boundary.

use market_squawk_sources::{ExtractionSourceError, SourceError};
use thiserror::Error;

use super::TreasurySourceError;

/// Closed code-owned stage of one native Treasury page operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TreasuryPageStage {
    /// Metadata/current extraction authority was rejected before transport.
    HttpAuthority,
    /// The request authority was bound to different source metadata.
    HttpMetadata,
    /// The source metadata was not effective at the request clock.
    HttpEffectiveTime,
    /// The bounded request transport or shared provider budget failed.
    HttpTransport,
    /// Status, retry-after, or response byte admission was rejected.
    HttpResponse,
    /// The successful response supplied an unsupported content encoding.
    HttpEncoding,
    /// The successful response did not supply an accepted content type.
    HttpContentType,
    /// Configured dataset or exact query/request binding was rejected.
    Query,
    /// Strict Fiscal JSON parsing failed.
    FiscalParse,
    /// Strict daily-rate XML parsing failed.
    DailyParse,
    /// Exact received-response capture construction failed.
    Capture,
    /// Source health accounting could not be recorded.
    Health,
    /// Retained checkpoint/source binding failed before requesting a page.
    Checkpoint,
    /// The current backfill/request state did not admit another page.
    Admission,
    /// Page sequence, totals, schema, or terminal tracking failed.
    Pagination,
    /// Provider/receive/validation clocks were inconsistent.
    Chronology,
    /// Exact source-object identity could not be constructed.
    SourceObject,
    /// A canonical row or its bounded record admission failed.
    Canonical,
    /// Page/accounting bounds or exact totals were inconsistent.
    Accounting,
    /// Exact native lineage construction or row mapping failed.
    NativeLineage,
    /// The source-neutral extraction contract rejected the page.
    Extraction,
}

/// Preserves the exact native failure without retaining response bodies or relaxing admission.
#[derive(Debug, Error)]
#[error("Treasury page failed at {stage:?}: {source}")]
pub struct TreasuryPageError {
    stage: TreasuryPageStage,
    #[source]
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl TreasuryPageError {
    pub(crate) fn new(
        stage: TreasuryPageStage,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            stage,
            source: Box::new(source),
        }
    }

    /// Returns the exact code-owned failing stage.
    pub const fn stage(&self) -> TreasuryPageStage {
        self.stage
    }

    /// Recognizes a direct terminal cancellation, never an arbitrary nested source chain.
    pub fn is_terminal_cancellation(&self) -> bool {
        matches!(
            self.source.downcast_ref::<ExtractionSourceError>(),
            Some(ExtractionSourceError::Cancelled)
        ) || matches!(
            self.source.downcast_ref::<TreasurySourceError>(),
            Some(TreasurySourceError::Cancelled)
        )
    }

    /// Recognizes a direct terminal deadline, without hiding another enclosing failure.
    pub fn is_terminal_deadline(&self) -> bool {
        matches!(
            self.source.downcast_ref::<ExtractionSourceError>(),
            Some(ExtractionSourceError::DeadlineExceeded)
        ) || matches!(
            self.source.downcast_ref::<TreasurySourceError>(),
            Some(TreasurySourceError::DeadlineExceeded)
        )
    }

    pub(super) fn is_budget_wait(&self) -> bool {
        matches!(
            self.source.downcast_ref::<ExtractionSourceError>(),
            Some(
                ExtractionSourceError::Authority(
                    market_squawk_sources::ExtractionAuthorityError::BudgetWaitUntil { .. }
                ) | ExtractionSourceError::Source(SourceError::BudgetWaitUntil { .. })
            )
        )
    }

    pub(super) fn into_extraction(self) -> ExtractionSourceError {
        let source = match self.source.downcast::<ExtractionSourceError>() {
            Ok(source) => return *source,
            Err(source) => source,
        };
        match source.downcast::<TreasurySourceError>() {
            Ok(source) => super::map_adapter_error(*source),
            Err(_) => super::lineage::invalid_protocol(),
        }
    }
}

impl From<ExtractionSourceError> for TreasuryPageError {
    fn from(error: ExtractionSourceError) -> Self {
        Self::new(TreasuryPageStage::Extraction, error)
    }
}

impl From<market_squawk_sources::ExtractionError> for TreasuryPageError {
    fn from(error: market_squawk_sources::ExtractionError) -> Self {
        Self::from(ExtractionSourceError::from(error))
    }
}

impl From<TreasuryPageError> for ExtractionSourceError {
    fn from(error: TreasuryPageError) -> Self {
        error.into_extraction()
    }
}
