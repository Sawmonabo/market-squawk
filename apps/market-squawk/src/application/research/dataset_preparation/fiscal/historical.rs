//! Fiscal preparation fenced by one genuine historical price-study origin.
//!
//! Retrospective source selection keeps its actual later snapshot and limitations. Only native
//! economic periods ending at or before the original market origin may enter preparation.

use super::*;
use crate::application::model::forecast::{HistoricalFinancialForecast, SelectedForecastRuntime};
use market_squawk_data::{
    FeatureDatasetInputEpoch, FeatureDatasetInputEpochCursor, PythonDatasetSelection,
};
use market_squawk_jobs::{JobSnapshot, JobState};

/// The two existing publisher requests and their source-minted post-publication expectation.
pub(crate) struct PreparedHistoricalFiscalDatasets {
    pair: PreparedFiscalDatasetPair,
    expectation: HistoricalFiscalDatasetExpectation,
}
impl PreparedHistoricalFiscalDatasets {
    pub(crate) fn into_parts(
        self,
    ) -> (
        PreparedFeatureDatasetBuild,
        PreparedFeatureDatasetBuild,
        String,
        HistoricalFiscalDatasetExpectation,
    ) {
        let (training, inputs, example) = self.pair.into_parts();
        (training, inputs, example, self.expectation)
    }
}

/// Reconstructed from the original price epoch and actual prepared native requests after restart.
/// It accepts no deserialized model selection or caller-authored financial amounts.
pub(crate) struct HistoricalFiscalDatasetExpectation {
    target_id: String,
    training_build: Sha256Digest,
    inputs_build: Sha256Digest,
    origin_identity: Sha256Digest,
    profile_digest: [u8; 32],
    source_cutoff: Timestamp,
}

/// Distinct completed historical fiscal role; current fiscal job receipts cannot construct it.
pub(crate) struct HistoricalFiscalTrainingAuthority {
    training_build: Sha256Digest,
    inputs_build: Sha256Digest,
    origin_identity: Sha256Digest,
    selection: Sha256Digest,
    profile_digest: [u8; 32],
}

impl DatasetPreparationAuthority {
    /// Prepares native financial inputs for the actual epoch's economic origin, using its exact
    /// original knowledge snapshot. The latest native period at that origin is reserved from all
    /// fitting/calibration/testing labels; the existing publisher validates every source row.
    pub(crate) async fn prepare_historical_financial_datasets(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        target: &crate::application::research::fiscal_projection::FiscalProjectionTarget,
        population: CurrentListedPopulation,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<PreparedHistoricalFiscalDatasets, ServiceError> {
        ensure_request(context)?;
        let origin = epoch.target_origin().ok_or(ServiceError::Unavailable)?;
        let decision = epoch.decision_at().ok_or(ServiceError::Unavailable)?;
        if epoch.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            || epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.market_bar().is_none()
            || decision < origin
            || epoch.source_selection_as_of() != epoch.snapshot_as_of()
            || origin > epoch.source_selection_as_of()
            || population.instrument_ids() != [epoch.instrument_id()]
            || !crate::application::research::fiscal_projection::fiscal_projection_targets()
                .contains(target)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let [member] = population.members() else {
            return Err(ServiceError::InvalidResult);
        };
        if !profile.admits_investment(
            member.canonical_record().definition().asset_class(),
            member.listing_record().is_etf(),
        ) {
            return Err(ServiceError::Unavailable);
        }
        let request = FiscalDatasetPreparationRequest {
            instrument_id: epoch.instrument_id(),
            knowledge_cutoff: epoch.source_selection_as_of(),
            effective_date_cutoff: origin
                .utc_calendar_date()
                .map_err(|_| ServiceError::InvalidRequest)?,
            measurement: target.measurement(),
            cadence: target.cadence,
            periods_ahead: target.periods_ahead,
            population,
        };
        let pair = self
            .prepare_financial_datasets_at_origin(request, profile, Some(epoch), context)
            .await
            .map_err(|error| {
                // This closed native role has no admitted SEC common-cash recipe. Preserve an
                // actual source rejection as missing data, without substituting entity CFO.
                if error == ServiceError::InvalidRequest
                    && target.role == market_squawk_data::FinancialAmountRole::CommonEquityCashFlow
                {
                    ServiceError::Unavailable
                } else {
                    error
                }
            })?;
        let origin_identity = Sha256Digest::new(
            Sha256::digest(epoch.canonical_bytes().map_err(map_fiscal_build_error)?).into(),
        );
        let expectation = HistoricalFiscalDatasetExpectation {
            target_id: target.target_id.clone(),
            training_build: pair.training.request.build_spec_digest().digest(),
            inputs_build: pair.study_inputs.request.build_spec_digest().digest(),
            origin_identity,
            profile_digest: decode_sha256(&profile.resolution().configuration_digest)
                .ok_or(ServiceError::InvalidRequest)?,
            source_cutoff: epoch.source_selection_as_of(),
        };
        Ok(PreparedHistoricalFiscalDatasets { pair, expectation })
    }
}

impl HistoricalFiscalDatasetExpectation {
    /// Admits only the actual completed source-owned training publication and reopened selection.
    pub(crate) fn admit_training(
        &self,
        job: &JobSnapshot,
        selection: &PythonDatasetSelection,
        profile: &ValidatedAnalyticalProfile,
    ) -> Result<HistoricalFiscalTrainingAuthority, ServiceError> {
        if job.state() != JobState::Completed
            || job.spec().input().digest().bytes() != self.training_build.bytes()
            || selection.identity().build_spec_digest().digest() != self.training_build
            || selection.product_contract()
                != FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1
            || selection.as_of() != self.source_cutoff
            || selection.study_policy().is_none_or(|study| {
                study.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
                    || study.purpose() != DatasetBuildPurpose::Training
                    || study.snapshot_as_of() != self.source_cutoff
            })
            || decode_sha256(&profile.resolution().configuration_digest)
                != Some(self.profile_digest)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(HistoricalFiscalTrainingAuthority {
            training_build: self.training_build,
            inputs_build: self.inputs_build,
            origin_identity: self.origin_identity,
            selection: selection.selection_sha256(),
            profile_digest: self.profile_digest,
        })
    }
}
impl HistoricalFiscalTrainingAuthority {
    pub(crate) fn admits(
        &self,
        selection: &PythonDatasetSelection,
        profile: &ValidatedAnalyticalProfile,
    ) -> bool {
        selection.selection_sha256() == self.selection
            && selection.identity().build_spec_digest().digest() == self.training_build
            && selection.product_contract()
                == FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1
            && decode_sha256(&profile.resolution().configuration_digest)
                == Some(self.profile_digest)
    }
    pub(crate) fn identity(&self) -> Sha256Digest {
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/historical-fiscal-training-role/v1\0");
        for digest in [
            self.training_build,
            self.inputs_build,
            self.origin_identity,
            self.selection,
        ] {
            hash.update(digest.bytes());
        }
        hash.update(self.profile_digest);
        Sha256Digest::new(hash.finalize().into())
    }
}

/// Original historical market epoch paired with actual native fiscal inference and both builds.
/// Only the expectation reconstructed from those original sources can mint this pairing.
pub(crate) struct HistoricalOriginFinancialForecast {
    reference: super::replay::HistoricalFiscalForecastReference,
    forecast: HistoricalFinancialForecast,
    inference_rights_decision: market_squawk_data::ResearchUseDecisionDigest,
    inference_rights_graph: market_squawk_data::ResearchUseGraphDigest,
    origin_identity: Sha256Digest,
    training_build: Sha256Digest,
    inputs_build: Sha256Digest,
}
impl HistoricalFiscalDatasetExpectation {
    pub(crate) fn forecast(
        &self,
        research: &crate::ResearchService,
        runtime: &SelectedForecastRuntime,
        output: &FeatureDatasetInputEpochCursor,
        price_coordinate: market_squawk_data::FeatureDatasetInputCoordinate<'_>,
        context: &RequestContext,
    ) -> Result<HistoricalOriginFinancialForecast, ServiceError> {
        ensure_request(context)?;
        let price_epoch = price_coordinate.epoch();
        if runtime.training_dataset().build_spec_digest().digest() != self.training_build
            || output
                .dataset()
                .generation()
                .build_spec_digest()
                .map(|digest| digest.digest())
                != Some(self.inputs_build)
            || output.dataset().product_contract()
                != FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
            || output.len() != 1
            || Sha256::digest(
                price_epoch
                    .canonical_bytes()
                    .map_err(map_fiscal_build_error)?,
            )
            .as_slice()
                != self.origin_identity.bytes()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let owned_coordinate = output
            .coordinate(0)
            .map_err(super::super::super::map_read_error)?
            .ok_or(ServiceError::InvalidResult)?;
        let coordinate = owned_coordinate.coordinate();
        let epoch = coordinate.epoch();
        let origin = price_epoch
            .target_origin()
            .ok_or(ServiceError::Unavailable)?;
        if epoch.instrument_id() != price_epoch.instrument_id()
            || epoch.source_selection_as_of() != self.source_cutoff
            || epoch.financial_period().is_none_or(|period| {
                origin
                    .utc_calendar_date()
                    .map_or(true, |date| period.observed_period().end() > date)
            })
        {
            return Err(ServiceError::InvalidRequest);
        }
        let distribution_roots = [
            runtime.training_dataset().manifest(),
            output.dataset().generation().manifest(),
            epoch.source_manifest(),
        ];
        let mut roots = Vec::new();
        roots
            .try_reserve_exact(3)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for root in distribution_roots {
            if !roots.contains(root) {
                roots.push(root.clone());
            }
        }
        let duration = context
            .deadline()
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(5));
        if duration.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let authorization = research
            .analytical()
            .authorize_research_use(
                market_squawk_data::ResearchUseRequest::try_new(
                    roots.clone(),
                    market_squawk_data::ResearchUse::LocalAnalysis,
                    ResearchUseLimits::try_new(
                        3,
                        4096,
                        8192,
                        4096,
                        4 * 1024 * 1024,
                        duration,
                        Duration::from_secs(300),
                    )
                    .map_err(|_| ServiceError::InvalidRequest)?,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                context.cancellation(),
            )
            .map_err(|error| match error {
                market_squawk_data::ResearchUseCatalogError::Cancelled => ServiceError::Cancelled,
                market_squawk_data::ResearchUseCatalogError::DeadlineExceeded => {
                    ServiceError::DeadlineExceeded
                }
                market_squawk_data::ResearchUseCatalogError::LimitExceeded => {
                    ServiceError::ResourceExhausted
                }
                market_squawk_data::ResearchUseCatalogError::Denied { .. }
                | market_squawk_data::ResearchUseCatalogError::Expired
                | market_squawk_data::ResearchUseCatalogError::Revoked
                | market_squawk_data::ResearchUseCatalogError::UnknownGeneration => {
                    ServiceError::Unavailable
                }
                _ => ServiceError::InvalidResult,
            })?;
        if authorization.research_use() != market_squawk_data::ResearchUse::LocalAnalysis
            || authorization.graph().roots().len() != roots.len()
            || roots
                .iter()
                .any(|root| !authorization.graph().roots().contains(root))
        {
            return Err(ServiceError::InvalidResult);
        }
        let forecast = runtime.forecast_financial_coordinate(coordinate, context)?;
        if forecast.calculated_at() >= authorization.expires_at() {
            return Err(ServiceError::Unavailable);
        }
        let inference_rights_decision = authorization.decision_digest();
        let inference_rights_graph = authorization.graph().digest();
        let _permit = authorization.into_permit();
        let reference = super::replay::HistoricalFiscalForecastReference::from_source(
            self.target_id.clone(),
            price_coordinate,
            output,
            runtime,
            forecast.native_distribution().identity(),
            self.origin_identity,
        )?;
        Ok(HistoricalOriginFinancialForecast {
            reference,
            forecast,
            inference_rights_decision,
            inference_rights_graph,
            origin_identity: self.origin_identity,
            training_build: self.training_build,
            inputs_build: self.inputs_build,
        })
    }
}
impl HistoricalOriginFinancialForecast {
    pub(crate) const fn reference(&self) -> &super::replay::HistoricalFiscalForecastReference {
        &self.reference
    }
    pub(crate) const fn inference_rights_decision(
        &self,
    ) -> market_squawk_data::ResearchUseDecisionDigest {
        self.inference_rights_decision
    }
    pub(crate) const fn inference_rights_graph(
        &self,
    ) -> market_squawk_data::ResearchUseGraphDigest {
        self.inference_rights_graph
    }
    pub(crate) const fn forecast(&self) -> &HistoricalFinancialForecast {
        &self.forecast
    }
    pub(crate) const fn origin_identity(&self) -> Sha256Digest {
        self.origin_identity
    }
    pub(crate) const fn training_build(&self) -> Sha256Digest {
        self.training_build
    }
    pub(crate) const fn inputs_build(&self) -> Sha256Digest {
        self.inputs_build
    }
}
