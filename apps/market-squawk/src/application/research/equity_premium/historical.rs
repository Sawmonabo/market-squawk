//! Ten complete economic years preceding the original study origin, at its actual source cutoff.

use super::*;
use crate::application::market_calendar::CompletedMarketSessionReadCapability;
use market_squawk_data::{DatasetBuildPurpose, FeatureDatasetInputEpoch, Sha256Digest};
use market_squawk_domain::HistoricalStudyBasis;

/// A source-owned historical sample. It is not a current premium or a claimed historical vintage.
#[derive(Debug)]
pub(crate) struct HistoricalOriginEquityPremiumRead {
    epoch_identity: Sha256Digest,
    identity: EvidenceDigest,
    equity: AnnualEquityCashReturnRead,
    government: AnnualGovernmentYieldRead,
    estimate: AnnualEquityPremiumArithmetic,
    parents: Box<[DatasetManifestRef]>,
}

impl MacroContextReadCapability {
    pub(crate) async fn read_historical_origin_equity_premium(
        &self,
        research: &crate::ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        epoch: &FeatureDatasetInputEpoch,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<HistoricalOriginEquityPremiumRead, EquityPremiumReadError> {
        selection::check_selection_control(deadline, &cancellation)?;
        let mismatch = EquityPremiumUnavailable::SourceIdentityMismatch;
        let origin = epoch.target_origin().ok_or(mismatch)?;
        let decision = epoch.decision_at().ok_or(mismatch)?;
        let cutoff = epoch.source_selection_as_of();
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.market_bar().is_none()
            || origin > decision
            || origin > cutoff
            || (epoch.basis() == HistoricalStudyBasis::HistoricalAsKnown && cutoff > decision)
            || (epoch.basis() == HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                && cutoff != epoch.snapshot_as_of())
        {
            return Err(mismatch.into());
        }
        let benchmark_reader = super::super::RecommendationBenchmarkSelectionReadCapability::new(
            research.market_data_instruments(),
        );
        let benchmark = research
            .run_owned_research_io(deadline, &cancellation, move |token| {
                benchmark_reader.select(cutoff, origin, deadline, &token)
            })
            .await??
            .ok_or(EquityPremiumUnavailable::BenchmarkSelectionMissing)?;
        let (start, end) = selection::required_annual_source_dates(origin)?;
        let history = selection::read_annual_source_superset(
            research,
            benchmark.primary().instrument_id(),
            cutoff,
            start,
            end,
            deadline,
            &cancellation,
        )
        .await?;
        let source = selection::rejoin_source_with_original_calendar(
            research,
            calendars,
            history,
            deadline,
            &cancellation,
        )
        .await?;
        let calendar_parent = self
            .read_equity_history_calendar_parent(research, &source, deadline, &cancellation)
            .await?;
        let equity =
            AnnualEquityCashReturnRead::from_source_at_origin(source, benchmark, Some(epoch))?;
        let origin_date = calendar_date(origin)?;
        if equity
            .closing_dates()
            .iter()
            .any(|date| *date >= origin_date)
        {
            return Err(mismatch.into());
        }
        let government = self
            .read_annual_government_yields(research, &equity, deadline, cancellation.child_token())
            .await?;
        if government.reference().knowledge_cutoff() != cutoff
            || government.reference().economic_origin() != origin
            || government.reference().equity_sample_evidence()
                != equity.reference().evidence_digest()
            || government.reference().equity_closing_dates() != equity.closing_dates()
        {
            return Err(mismatch.into());
        }
        let estimate = AnnualEquityPremiumArithmetic::calculate(
            equity.annual_returns(),
            government.yields_percent(),
        )
        .map_err(|_| EquityPremiumUnavailable::Arithmetic)?;
        let epoch_identity = Sha256Digest::new(
            Sha256::digest(epoch.canonical_bytes().map_err(|_| mismatch)?).into(),
        );
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
        parents.sort_by(|a, b| {
            a.dataset_id()
                .as_str()
                .cmp(b.dataset_id().as_str())
                .then_with(|| a.manifest_version().cmp(&b.manifest_version()))
        });
        if parents.windows(2).any(|p| {
            p[0].dataset_id() == p[1].dataset_id()
                && p[0].manifest_version() == p[1].manifest_version()
                && p[0] != p[1]
        }) {
            return Err(mismatch.into());
        }
        parents.dedup();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/historical-origin-equity-premium/v1\0");
        hash.update(epoch_identity.bytes());
        hash.update(equity.reference().evidence_digest().bytes());
        hash.update(government.reference().evidence_digest().bytes());
        hash.update(EQUITY_PREMIUM_ESTIMATOR.as_bytes());
        let premium = estimate.geometric_premium().normalize();
        hash.update(premium.mantissa().to_be_bytes());
        hash.update(premium.scale().to_be_bytes());
        for parent in &parents {
            hash.update(parent.content_hash().bytes());
        }
        selection::check_selection_control(deadline, &cancellation)?;
        Ok(HistoricalOriginEquityPremiumRead {
            epoch_identity,
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            equity,
            government,
            estimate,
            parents: parents.into_boxed_slice(),
        })
    }
}

impl HistoricalOriginEquityPremiumRead {
    pub(crate) const fn epoch_identity(&self) -> Sha256Digest {
        self.epoch_identity
    }
    pub(crate) const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    pub(crate) fn annual_premium(&self) -> rust_decimal::Decimal {
        self.estimate.geometric_premium()
    }
    pub(crate) fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parents
    }
    pub(crate) const fn equity(&self) -> &AnnualEquityCashReturnRead {
        &self.equity
    }
    pub(crate) const fn government(&self) -> &AnnualGovernmentYieldRead {
        &self.government
    }
}
