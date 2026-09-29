//! Default source-owned ten-complete-year historical equity-premium estimate.

mod equity_cash;
mod historical;
pub(crate) use historical::HistoricalOriginEquityPremiumRead;
mod reference;
pub(super) mod selection;
use super::macro_context::annual_yields::{
    AnnualGovernmentYieldRead, AnnualGovernmentYieldReference,
};
use super::{
    MacroContextReadCapability, RecommendationBenchmarkSelection, TiingoCompletedEodActionRead,
};
use chrono::{DateTime, Datelike, TimeZone, Utc};
pub(crate) use equity_cash::{AnnualEquityCashReturnRead, AnnualEquityCashReturnReference};
use market_squawk_data::DatasetManifestRef;
use market_squawk_domain::{CalendarDate, DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_services::ServiceError;
use market_squawk_valuation::{
    AnnualEquityPremiumArithmetic, AutomaticValuationAssumption, AutomaticValuationAssumptionKind,
    EQUITY_PREMIUM_ESTIMATOR,
};
pub(crate) use reference::MAXIMUM_EQUITY_PREMIUM_REFERENCE_BYTES;
pub(crate) use selection::required_annual_source_dates;
use sha2::{Digest as _, Sha256};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// Missing actual evidence is a distinct outcome; none of these selects a different estimator.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum EquityPremiumUnavailable {
    #[error("eleven year-end endpoints covering ten complete calendar years are required")]
    IncompleteTenYearHistory,
    #[error(
        "the fixed primary and accompanying benchmark selection must be available from the original catalog cutoff"
    )]
    BenchmarkSelectionMissing,
    #[error(
        "the required complete annual equity history has not been published at the original source cutoff"
    )]
    AnnualHistoryNotPublished,
    #[error("a source-owned nominal daily date and complete session graph are required")]
    NativeDateAuthorityMissing,
    #[error(
        "the exact original calendar publication referenced by source history must be available at the original cutoff"
    )]
    OriginalCalendarPublicationMissing,
    #[error("an actual closing price on the final source session of every year is required")]
    MissingAnnualClosingPrice,
    #[error("every source cash-distribution and split field must be explicitly reported")]
    MissingActionField,
    #[error("source-proven cash-distribution currency and per-share monetary basis are required")]
    DividendCurrencyUnproven,
    #[error("same-day dividend and split require actual source share-basis evidence")]
    DividendShareBasisUnproven,
    #[error("a non-unit split requires the existing source-backed action accounting owner")]
    SplitAccountingRequired,
    #[error(
        "an observed annual-percent ten-year yield on every exact equity closing date is required"
    )]
    GovernmentYearEndObservationMissing,
    #[error(
        "the original fixed benchmark, source generation, cutoff and replay identities must agree"
    )]
    SourceIdentityMismatch,
    #[error("the historical estimate has expired and requires a new source selection")]
    Expired,
    #[error("the estimate requires checked finite return arithmetic")]
    Arithmetic,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum EquityPremiumReadError {
    #[error(transparent)]
    Unavailable(#[from] EquityPremiumUnavailable),
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error("equity premium original history source read failed: {0}")]
    History(#[from] crate::ResearchServiceError),
    #[error("equity premium original calendar source read failed: {0}")]
    Calendar(#[from] crate::application::market_calendar::CompletedMarketSessionError),
}

impl EquityPremiumReadError {
    /// Projects an error from an actual source attempt into the existing service contract. The
    /// typed producer retains the original history failure until this final consumer boundary.
    pub(crate) fn into_service_error(self) -> ServiceError {
        use crate::ResearchServiceError;
        use market_squawk_data::IngestError;
        use market_squawk_platform::{ResearchObjectControlError, SealedResearchJournalStoreError};
        match self {
            Self::Service(error) => error,
            Self::Calendar(crate::application::market_calendar::CompletedMarketSessionError::Cancelled) => ServiceError::Cancelled,
            Self::Calendar(crate::application::market_calendar::CompletedMarketSessionError::DeadlineExceeded) => ServiceError::DeadlineExceeded,
            Self::Calendar(crate::application::market_calendar::CompletedMarketSessionError::ResourceBoundExceeded) => ServiceError::ResourceExhausted,
            Self::Calendar(crate::application::market_calendar::CompletedMarketSessionError::InvalidEvidence) => ServiceError::InvalidResult,
            Self::Calendar(_) => ServiceError::Unavailable,
            Self::History(ResearchServiceError::Ingest(IngestError::Cancelled))
            | Self::History(ResearchServiceError::ProviderCaptureStore(
                SealedResearchJournalStoreError::ObjectControl(ResearchObjectControlError::Cancelled),
            )) => ServiceError::Cancelled,
            Self::History(ResearchServiceError::Ingest(IngestError::DeadlineExceeded))
            | Self::History(ResearchServiceError::ProviderCaptureStore(
                SealedResearchJournalStoreError::ObjectControl(
                    ResearchObjectControlError::DeadlineExceeded,
                ),
            )) => ServiceError::DeadlineExceeded,
            Self::History(ResearchServiceError::IngestAuthorityMismatch)
            | Self::Unavailable(EquityPremiumUnavailable::SourceIdentityMismatch
                | EquityPremiumUnavailable::Arithmetic) => ServiceError::InvalidResult,
            Self::History(_) | Self::Unavailable(_) => ServiceError::Unavailable,
        }
    }
}

/// Inert reconstruction recipe retained with the existing canonical macro-assumption receipt.
/// The identity never substitutes for fresh source replay and final LocalAnalysis authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HistoricalEquityPremiumReference {
    equity: AnnualEquityCashReturnReference,
    government: AnnualGovernmentYieldReference,
    produced_at: Timestamp,
    expires_at: Timestamp,
    evidence_digest: EvidenceDigest,
}
impl HistoricalEquityPremiumReference {
    pub(crate) const fn equity(&self) -> &AnnualEquityCashReturnReference {
        &self.equity
    }
    pub(crate) const fn government(&self) -> &AnnualGovernmentYieldReference {
        &self.government
    }
    pub(crate) const fn produced_at(&self) -> Timestamp {
        self.produced_at
    }
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }
}

/// Actual source selections, retained numerical diagnostics and source-owned assumption producer.
#[derive(Debug)]
pub(crate) struct HistoricalEquityPremiumRead {
    equity: AnnualEquityCashReturnRead,
    government: AnnualGovernmentYieldRead,
    estimate: AnnualEquityPremiumArithmetic,
    reference: HistoricalEquityPremiumReference,
    parent_manifests: Box<[DatasetManifestRef]>,
}

impl MacroContextReadCapability {
    /// Default estimator uses the authenticated primary benchmark; source history selection must
    /// precede outcomes. All ten annual observations are mandatory, regardless of result sign.
    pub(crate) async fn read_default_equity_premium(
        &self,
        research: &crate::ResearchService,
        source: TiingoCompletedEodActionRead,
        benchmark: RecommendationBenchmarkSelection,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalEquityPremiumRead, EquityPremiumReadError> {
        let calendar_parent = self
            .read_equity_history_calendar_parent(research, &source, deadline, &cancellation)
            .await?;
        let equity = AnnualEquityCashReturnRead::from_source(source, benchmark)?;
        let government = self
            .read_annual_government_yields(research, &equity, deadline, cancellation)
            .await
            .map_err(map_government_error)?;
        HistoricalEquityPremiumRead::from_sources(equity, government, calendar_parent, None)
            .map_err(Into::into)
    }

    /// Callers must first reopen the exact equity history and benchmark using their source-owned
    /// readers. This method compares the resulting receipt and reopens the original yield recipe;
    /// it does not accept an equity vector or refresh the original estimate's lifetime.
    pub(crate) async fn rejoin_equity_premium_reference(
        &self,
        research: &crate::ResearchService,
        reference: &HistoricalEquityPremiumReference,
        source: TiingoCompletedEodActionRead,
        benchmark: RecommendationBenchmarkSelection,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalEquityPremiumRead, EquityPremiumReadError> {
        let calendar_parent = self
            .read_equity_history_calendar_parent(research, &source, deadline, &cancellation)
            .await?;
        let equity = AnnualEquityCashReturnRead::from_source(source, benchmark)?;
        if equity.reference() != reference.equity() {
            return Err(EquityPremiumUnavailable::SourceIdentityMismatch.into());
        }
        let government = self
            .read_annual_government_yield_reference(
                research,
                reference.government(),
                &equity,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_government_error)?;
        HistoricalEquityPremiumRead::from_sources(
            equity,
            government,
            calendar_parent,
            Some(reference),
        )
        .map_err(Into::into)
    }
}

impl HistoricalEquityPremiumRead {
    fn from_sources(
        equity: AnnualEquityCashReturnRead,
        government: AnnualGovernmentYieldRead,
        calendar_parent: DatasetManifestRef,
        original: Option<&HistoricalEquityPremiumReference>,
    ) -> Result<Self, EquityPremiumUnavailable> {
        let mismatch = EquityPremiumUnavailable::SourceIdentityMismatch;
        if government.reference().knowledge_cutoff() != equity.source().knowledge_cutoff()
            || government.reference().economic_origin() != equity.economic_origin()
            || government.reference().equity_sample_evidence()
                != equity.reference().evidence_digest()
            || government.reference().equity_closing_dates() != equity.closing_dates()
        {
            return Err(mismatch);
        }
        let estimate = AnnualEquityPremiumArithmetic::calculate(
            equity.annual_returns(),
            government.yields_percent(),
        )
        .map_err(|_| EquityPremiumUnavailable::Arithmetic)?;
        let now = current_timestamp()?;
        let produced_at = original.map_or(now, HistoricalEquityPremiumReference::produced_at);
        let cutoff = equity.source().knowledge_cutoff();
        // Thirty days from the original knowledge selection, additionally capped at the next
        // UTC calendar-year boundary when this fixed trailing-year sample must change.
        let age_bound = cutoff
            .unix_nanos()
            .checked_add(30_i64 * 86_400 * 1_000_000_000)
            .ok_or(mismatch)?;
        let next_year = i32::from(calendar_date(cutoff)?.year()) + 1;
        let year_bound = Utc
            .with_ymd_and_hms(next_year, 1, 1, 0, 0, 0)
            .single()
            .and_then(|value| value.timestamp_nanos_opt())
            .ok_or(mismatch)?;
        let expires_at = Timestamp::from_unix_nanos(age_bound.min(year_bound));
        if produced_at < cutoff || produced_at > now || now >= expires_at {
            return Err(EquityPremiumUnavailable::Expired);
        }
        let mut digest = Sha256::new();
        digest.update(b"market-squawk/source-owned-historical-equity-premium/v1\0");
        digest.update(EQUITY_PREMIUM_ESTIMATOR.as_bytes());
        digest.update(equity.reference().evidence_digest().bytes());
        digest.update(government.reference().evidence_digest().bytes());
        digest.update(produced_at.unix_nanos().to_be_bytes());
        digest.update(expires_at.unix_nanos().to_be_bytes());
        for value in [
            estimate.geometric_premium(),
            estimate.arithmetic_premium(),
            estimate.geometric_returns().0,
            estimate.geometric_returns().1,
        ] {
            digest.update(value.normalize().mantissa().to_be_bytes());
            digest.update(value.normalize().scale().to_be_bytes());
        }
        digest.update(
            estimate
                .arithmetic_standard_error()
                .value()
                .to_bits()
                .to_be_bytes(),
        );
        digest.update(
            estimate
                .geometric_delta_standard_error()
                .value()
                .to_bits()
                .to_be_bytes(),
        );
        let evidence_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
        if evidence_digest.bytes() == [0; 32] {
            return Err(mismatch);
        }
        let reference = HistoricalEquityPremiumReference {
            equity: equity.reference().clone(),
            government: government.reference().clone(),
            produced_at,
            expires_at,
            evidence_digest,
        };
        if original.is_some_and(|original| *original != reference) {
            return Err(mismatch);
        }
        let mut parents = vec![
            equity
                .source()
                .history()
                .selection()
                .pinned()
                .manifest()
                .clone(),
            equity
                .source()
                .history()
                .read_receipt()
                .origin_manifest()
                .clone(),
            government.reference().manifest().clone(),
            calendar_parent,
        ];
        parents.sort_by(|left, right| {
            left.dataset_id()
                .as_str()
                .cmp(right.dataset_id().as_str())
                .then_with(|| left.manifest_version().cmp(&right.manifest_version()))
        });
        if parents.windows(2).any(|pair| {
            pair[0].dataset_id() == pair[1].dataset_id()
                && pair[0].manifest_version() == pair[1].manifest_version()
                && pair[0] != pair[1]
        }) {
            return Err(mismatch);
        }
        parents.dedup();
        Ok(Self {
            equity,
            government,
            estimate,
            reference,
            parent_manifests: parents.into_boxed_slice(),
        })
    }

    /// Annual decimal fraction for either existing annual equity-rate role. The caller must retain
    /// this read's recipe/parent roots with the canonical macro receipt and authorize their whole
    /// source graph before serving. A thirty-year current reference is not this estimator's basis.
    pub(crate) fn assumption(
        &self,
        kind: AutomaticValuationAssumptionKind,
    ) -> Result<AutomaticValuationAssumption, EquityPremiumUnavailable> {
        if !matches!(
            kind,
            AutomaticValuationAssumptionKind::DiscountRate
                | AutomaticValuationAssumptionKind::CostOfEquity
        ) {
            return Err(EquityPremiumUnavailable::SourceIdentityMismatch);
        }
        if current_timestamp()? >= self.reference.expires_at {
            return Err(EquityPremiumUnavailable::Expired);
        }
        AutomaticValuationAssumption::try_new(
            kind,
            EQUITY_PREMIUM_ESTIMATOR,
            self.estimate.geometric_premium(),
            self.reference.evidence_digest,
            self.reference.produced_at,
            self.reference.expires_at,
        )
        .map_err(|_| EquityPremiumUnavailable::Arithmetic)
    }
    pub(crate) const fn reference(&self) -> &HistoricalEquityPremiumReference {
        &self.reference
    }
    pub(crate) const fn equity(&self) -> &AnnualEquityCashReturnRead {
        &self.equity
    }
    pub(crate) const fn government(&self) -> &AnnualGovernmentYieldRead {
        &self.government
    }
    pub(crate) const fn estimate(&self) -> &AnnualEquityPremiumArithmetic {
        &self.estimate
    }
    pub(crate) fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parent_manifests
    }
}

fn map_government_error(error: ServiceError) -> EquityPremiumReadError {
    match error {
        ServiceError::Unavailable => {
            EquityPremiumUnavailable::GovernmentYearEndObservationMissing.into()
        }
        other => EquityPremiumReadError::Service(other),
    }
}
fn current_timestamp() -> Result<Timestamp, EquityPremiumUnavailable> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(EquityPremiumUnavailable::SourceIdentityMismatch)
}
fn calendar_date(timestamp: Timestamp) -> Result<CalendarDate, EquityPremiumUnavailable> {
    let date = DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)?,
        date.month() as u8,
        date.day() as u8,
    )
    .map_err(|_| EquityPremiumUnavailable::SourceIdentityMismatch)
}
