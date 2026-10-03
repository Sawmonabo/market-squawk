//! Forecast outcome admission and artifact-authenticated recovery.

use std::{future::Future, pin::Pin, sync::Arc};

use market_squawk_data::{
    AnalyticalMarketBarOutput, AuthorizedResearchUse, FeatureDatasetInputEpoch,
    FinancialAmountBasis, Sha256Digest,
};
use market_squawk_domain::{InstrumentId, MarketBarAdjustment, MarketBarObservation};
use market_squawk_modeling::{
    AuthenticatedForecastServingBinding, ForecastMeasurement, ForecastTerminalDistribution,
};

use super::*;

/// Native source kind and exact original economic input identity; it grants no authority alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForecastValuationOriginIdentity {
    /// Exact completed source price bar used by the original serving calculation.
    CompletedBar(EvidenceDigest),
    /// Exact data-issued fiscal input epoch and native period/measurement binding.
    FinancialEpoch(EvidenceDigest),
    /// Exact current source price epoch, including named-session and split-unit evidence.
    CurrentPriceEpoch(EvidenceDigest),
}
impl ForecastValuationOriginIdentity {
    /// Returns the canonical original input identity.
    pub const fn digest(self) -> EvidenceDigest {
        match self {
            Self::CompletedBar(value)
            | Self::FinancialEpoch(value)
            | Self::CurrentPriceEpoch(value) => value,
        }
    }
}

/// Exact immutable source coordinates needed to reopen one retained forecast distribution.
///
/// This reference carries no financial scalar and cannot create model evidence by itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastValuationReference {
    pub(crate) identity: EvidenceDigest,
    pub(crate) distribution_identity: Sha256Digest,
    pub(crate) vintage_id: Sha256Digest,
    pub(crate) forecast_artifact_hash: Sha256Digest,
    pub(crate) metadata_hash: Sha256Digest,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) training_manifest: DatasetManifestRef,
    pub(crate) serving_manifest: DatasetManifestRef,
    pub(crate) parent_manifests: Box<[DatasetManifestRef]>,
    pub(crate) serving_source: SourceId,
    pub(crate) serving_graph: EvidenceDigest,
    pub(crate) serving_query: EvidenceDigest,
    pub(crate) serving_result: EvidenceDigest,
    pub(crate) serving_feature: Sha256Digest,
    pub(crate) source_origin: ForecastValuationOriginIdentity,
    pub(crate) knowledge_at: Timestamp,
    pub(crate) selected_at: Timestamp,
}

impl ForecastValuationReference {
    /// Complete distribution, source selection, conversion, and time identity.
    pub const fn identity(&self) -> EvidenceDigest {
        self.identity
    }
    /// Exact raw model-unit distribution identity.
    pub const fn distribution_identity(&self) -> Sha256Digest {
        self.distribution_identity
    }
    /// Exact immutable forecast vintage to reopen without replacement.
    pub const fn vintage_id(&self) -> Sha256Digest {
        self.vintage_id
    }
    /// Exact controlled forecast artifact content.
    pub const fn forecast_artifact_hash(&self) -> Sha256Digest {
        self.forecast_artifact_hash
    }
    /// Exact admitted model metadata artifact content.
    pub const fn metadata_hash(&self) -> Sha256Digest {
        self.metadata_hash
    }
    /// Exact forecast instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Actual training generation retained by the admitted bundle.
    pub const fn training_manifest(&self) -> &DatasetManifestRef {
        &self.training_manifest
    }
    /// Actual market-history generation used for the causal price conversion.
    pub const fn serving_manifest(&self) -> &DatasetManifestRef {
        &self.serving_manifest
    }
    /// All exact source parents, including training, market history, and serving macro inputs.
    pub fn parent_manifests(&self) -> &[DatasetManifestRef] {
        &self.parent_manifests
    }
    /// Exact source owning the serving bars.
    pub const fn serving_source(&self) -> &SourceId {
        &self.serving_source
    }
    /// Exact immutable object graph read for serving.
    pub const fn serving_graph(&self) -> EvidenceDigest {
        self.serving_graph
    }
    /// Exact bounded query selected for serving.
    pub const fn serving_query(&self) -> EvidenceDigest {
        self.serving_query
    }
    /// Exact serving query result.
    pub const fn serving_result(&self) -> EvidenceDigest {
        self.serving_result
    }
    /// Original complete feature-vector evidence identity retained by the forecast artifact.
    pub const fn serving_feature(&self) -> Sha256Digest {
        self.serving_feature
    }
    /// Complete canonical causal-origin bar identity.
    pub const fn source_origin(&self) -> ForecastValuationOriginIdentity {
        self.source_origin
    }
    /// Original source knowledge cutoff.
    pub const fn knowledge_at(&self) -> Timestamp {
        self.knowledge_at
    }
    /// Actual forecast selection time, after publication.
    pub const fn selected_at(&self) -> Timestamp {
        self.selected_at
    }
}

/// Reopens actual controlled artifacts and their exact source selections for catalog recovery.
pub trait ForecastValuationResolver: Send + Sync {
    /// Returns genuine reconstructed evidence plus fresh local-analysis source authorization.
    ///
    /// Implementations must revalidate the exact bundle, vintage, controlled artifact, complete
    /// serving binding, and source parents. Choosing a replacement vintage is not permitted.
    fn resolve<'a>(
        &'a self,
        reference: &'a ForecastValuationReference,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        (ForecastValuationSource, AuthorizedResearchUse),
                        FairValueError,
                    >,
                > + Send
                + 'a,
        >,
    >;
}

/// Genuine model distribution and its manifest-pinned causal price origin.
#[derive(Clone, Debug, PartialEq)]
pub struct ForecastValuationSource {
    reference: ForecastValuationReference,
    distribution: Arc<ForecastTerminalDistribution>,
    origin: ForecastValuationOrigin,
    canonical_origin: Box<str>,
    retained_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
enum ForecastValuationOrigin {
    CompletedBar(MarketBarObservation),
    FinancialEpoch(Box<FeatureDatasetInputEpoch>),
    CurrentPriceEpoch(Box<FeatureDatasetInputEpoch>),
}

// Modeling only admits finite residuals and fixed-scale outcomes; its equality is reflexive.
impl Eq for ForecastValuationSource {}

impl ForecastValuationSource {
    /// Consumes a sealed native-unit model distribution and selects its exact causal source bar.
    ///
    /// The serving fence is authenticated by the exact controlled forecast artifact. Neither a
    /// caller-selected replacement price nor an interval endpoint enters this boundary.
    pub fn try_from_distribution(
        distribution: ForecastTerminalDistribution,
        binding: &AuthenticatedForecastServingBinding,
        serving: &AnalyticalMarketBarOutput,
        knowledge_at: Timestamp,
        selected_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let (Some(observed), Some(target)) =
            (distribution.observed_through(), distribution.target_at())
        else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        if binding.financial_epoch().is_some() || distribution.financial_target().is_some() {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        if binding.output_identity() != distribution.identity()
            || binding.forecast_artifact_hash() != distribution.forecast_artifact_hash()
            || binding.knowledge_cutoff() != knowledge_at
            || binding.manifest() != serving.output().manifest()
            || binding.source_id() != serving.source_id()
            || serving.output().object_graph_digest()
                != EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    binding.object_graph_sha256().bytes(),
                )
            || serving.output().query_identity()
                != EvidenceDigest::new(DigestAlgorithm::Sha256, binding.selection_sha256().bytes())
            || serving.output().result_digest()
                != EvidenceDigest::new(DigestAlgorithm::Sha256, binding.result_sha256().bytes())
            || selected_at < knowledge_at
            || distribution.available_at() > knowledge_at
            || observed > knowledge_at
            || distribution.training_dataset().selection_as_of() > knowledge_at
            || distribution.published_at() > selected_at
            || selected_at >= distribution.expires_at()
            || target <= knowledge_at
            || distribution.points().is_empty()
            || distribution.points().len() > 512
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let mut candidates = serving
            .bars()
            .iter()
            .filter(|bar| bar.completed_at() == Some(observed));
        let bar = candidates
            .next()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        if candidates.next().is_some()
            || binding.origin_bar() != Some(bar)
            || bar.context().provenance().instrument_id() != Some(distribution.instrument_id())
            || bar.context().provenance().source_id() != serving.source_id()
            || bar.adjustment() != MarketBarAdjustment::Split
            || bar.completed_at().is_none_or(|time| time > knowledge_at)
            || bar
                .context()
                .provenance()
                .availability()
                .conservative_available_at()
                .is_none_or(|available| available > knowledge_at)
            || matches!(
                bar.context().provenance().quality(),
                DataQuality::Modeled | DataQuality::Stale | DataQuality::Quarantined
            )
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        match distribution.output_binding().measurement() {
            ForecastMeasurement::Price { currency } if currency == bar.close().currency() => {}
            ForecastMeasurement::Return
                if distribution
                    .output_binding()
                    .expected_arithmetic_return_horizon_nanos()
                    .is_some() => {}
            _ => return Err(FairValueError::InvalidProducerEvidence),
        }
        Self::finish_source(
            distribution,
            binding,
            ForecastValuationOrigin::CompletedBar(bar.clone()),
            knowledge_at,
            selected_at,
        )
    }

    /// Consumes the existing artifact-authenticated data epoch; no scalar or synthetic timestamp enters.
    pub fn try_from_financial_distribution(
        distribution: ForecastTerminalDistribution,
        binding: &AuthenticatedForecastServingBinding,
        knowledge_at: Timestamp,
        selected_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let epoch = binding
            .financial_epoch()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        if binding.output_identity() != distribution.identity()
            || binding.forecast_artifact_hash() != distribution.forecast_artifact_hash()
            || binding.knowledge_cutoff() != knowledge_at
            || epoch.source_selection_as_of() != knowledge_at
            || epoch.instrument_id() != distribution.instrument_id()
            || epoch.financial_period() != distribution.financial_target()
            || epoch.financial_period().is_none()
            || distribution.observed_through().is_some()
            || distribution.target_at().is_some()
            || binding.origin_bar().is_some()
            || selected_at < knowledge_at
            || distribution.available_at() != knowledge_at
            || distribution.training_dataset().selection_as_of() > knowledge_at
            || distribution.published_at() > selected_at
            || selected_at >= distribution.expires_at()
            || distribution.points().is_empty()
            || distribution.points().len() > 512
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let Some(market_squawk_data::FeatureLabelMeasurement::FinancialAmount {
            currency,
            role,
            basis,
            share_convention,
        }) = epoch.financial_measurement()
        else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        if distribution.output_binding().measurement()
            != (ForecastMeasurement::FinancialAmount {
                currency,
                role,
                basis,
                share_convention,
            })
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        // Native per-common-share reporting is not proof of an instrument's class allocation.
        // Keep it at its model/source boundary until an authentic instrument-unit bridge exists.
        if basis == FinancialAmountBasis::PerCommonShare {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        Self::finish_source(
            distribution,
            binding,
            ForecastValuationOrigin::FinancialEpoch(Box::new(epoch.clone())),
            knowledge_at,
            selected_at,
        )
    }

    /// Consumes a current epoch already authenticated by both the complete forecast artifact and native source reread.
    pub fn try_from_current_price_distribution(
        distribution: ForecastTerminalDistribution,
        binding: &AuthenticatedForecastServingBinding,
        knowledge_at: Timestamp,
        selected_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let epoch = binding
            .current_price_epoch()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        if binding.output_identity() != distribution.identity()
            || binding.forecast_artifact_hash() != distribution.forecast_artifact_hash()
            || binding.knowledge_cutoff() != knowledge_at
            || epoch.source_selection_as_of() != knowledge_at
            || epoch.instrument_id() != distribution.instrument_id()
            || epoch.target_origin() != distribution.observed_through()
            || epoch.target_at() != distribution.target_at()
            || epoch
                .target_at()
                .is_none_or(|target| target <= knowledge_at)
            || distribution.output_binding().measurement() != ForecastMeasurement::Return
            || distribution
                .output_binding()
                .expected_arithmetic_return_horizon_nanos()
                .is_none()
            || epoch.market_bar() != binding.origin_bar()
            || epoch.financial_period().is_some()
            || selected_at < knowledge_at
            || distribution.available_at() != knowledge_at
            || distribution.training_dataset().selection_as_of() > knowledge_at
            || distribution.published_at() > selected_at
            || selected_at >= distribution.expires_at()
            || distribution.points().is_empty()
            || distribution.points().len() > 512
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        epoch
            .current_unit_price()
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
        Self::finish_source(
            distribution,
            binding,
            ForecastValuationOrigin::CurrentPriceEpoch(Box::new(epoch.clone())),
            knowledge_at,
            selected_at,
        )
    }

    fn finish_source(
        distribution: ForecastTerminalDistribution,
        binding: &AuthenticatedForecastServingBinding,
        origin: ForecastValuationOrigin,
        knowledge_at: Timestamp,
        selected_at: Timestamp,
    ) -> Result<Self, FairValueError> {
        let canonical_origin = match &origin {
            ForecastValuationOrigin::CompletedBar(bar) => encode_source_record(bar)?,
            ForecastValuationOrigin::FinancialEpoch(epoch)
            | ForecastValuationOrigin::CurrentPriceEpoch(epoch) => String::from_utf8(
                epoch
                    .canonical_bytes()
                    .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            )
            .map_err(|_| FairValueError::InvalidProducerEvidence)?
            .into_boxed_str(),
        };
        let origin_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(canonical_origin.as_bytes()).into(),
        );
        let source_origin = match &origin {
            ForecastValuationOrigin::CompletedBar(_) => {
                ForecastValuationOriginIdentity::CompletedBar(origin_digest)
            }
            ForecastValuationOrigin::FinancialEpoch(_) => {
                ForecastValuationOriginIdentity::FinancialEpoch(origin_digest)
            }
            ForecastValuationOrigin::CurrentPriceEpoch(_) => {
                ForecastValuationOriginIdentity::CurrentPriceEpoch(origin_digest)
            }
        };
        let mut parent_manifests = binding.parent_manifests().to_vec();
        if !parent_manifests.contains(distribution.training_manifest()) {
            parent_manifests.push(distribution.training_manifest().clone());
        }
        if parent_manifests.is_empty()
            || parent_manifests.len() > market_squawk_modeling::MAX_FORECAST_SERVING_PARENTS + 1
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let mut reference = ForecastValuationReference {
            identity: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            distribution_identity: distribution.identity(),
            vintage_id: Sha256Digest::new(distribution.vintage_id().bytes()),
            forecast_artifact_hash: distribution.forecast_artifact_hash(),
            metadata_hash: distribution.metadata_hash(),
            instrument_id: distribution.instrument_id(),
            training_manifest: distribution.training_manifest().clone(),
            serving_manifest: binding.manifest().clone(),
            parent_manifests: parent_manifests.into_boxed_slice(),
            serving_source: binding.source_id().clone(),
            serving_graph: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                binding.object_graph_sha256().bytes(),
            ),
            serving_query: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                binding.selection_sha256().bytes(),
            ),
            serving_result: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                binding.result_sha256().bytes(),
            ),
            serving_feature: binding.feature_sha256(),
            source_origin,
            knowledge_at,
            selected_at,
        };
        let mut hash = CanonicalHasher::new(b"market-squawk/forecast-valuation-source/v1");
        hash.fixed(reference.distribution_identity.bytes());
        hash_manifest(&mut hash, &reference.training_manifest);
        hash_manifest(&mut hash, &reference.serving_manifest);
        hash.u64(
            u64::try_from(reference.parent_manifests.len())
                .map_err(|_| FairValueError::Arithmetic)?,
        );
        for parent in &reference.parent_manifests {
            hash_manifest(&mut hash, parent);
        }
        hash.fixed(reference.serving_feature.bytes());
        hash.bytes(reference.serving_source.as_str().as_bytes());
        for digest in [
            reference.serving_graph,
            reference.serving_query,
            reference.serving_result,
            reference.source_origin.digest(),
        ] {
            hash_digest(&mut hash, digest);
        }
        hash.i64(knowledge_at.unix_nanos());
        hash.i64(selected_at.unix_nanos());
        hash.bytes(match reference.source_origin {
            ForecastValuationOriginIdentity::CompletedBar(_) => {
                b"arithmetic-return-origin-close-half-even-12-places/v1".as_slice()
            }
            ForecastValuationOriginIdentity::FinancialEpoch(_) => {
                b"native-financial-role-basis-ordinal/v1".as_slice()
            }
            ForecastValuationOriginIdentity::CurrentPriceEpoch(_) => {
                b"current-price-epoch-split-units-half-even-12-places/v1".as_slice()
            }
        });
        reference.identity = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finish());
        let distribution_bytes = distribution
            .canonical_bytes()
            .map_err(|_| FairValueError::InvalidProducerEvidence)?
            .len();
        // Charge the genuine data epoch's retained graph plus its separate canonical encoding.
        let native_bytes = match &origin {
            ForecastValuationOrigin::FinancialEpoch(epoch)
            | ForecastValuationOrigin::CurrentPriceEpoch(epoch) => epoch.retained_bytes(),
            ForecastValuationOrigin::CompletedBar(_) => canonical_origin.len(),
        };
        let mut retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(
                distribution_bytes,
                checked_add(canonical_origin.len(), native_bytes)?,
            )?,
        )?;
        retained_bytes = checked_add(
            retained_bytes,
            manifest_retained_bytes(&reference.training_manifest)?,
        )?;
        retained_bytes = checked_add(
            retained_bytes,
            manifest_retained_bytes(&reference.serving_manifest)?,
        )?;
        retained_bytes = checked_add(
            retained_bytes,
            std::mem::size_of_val(&*reference.parent_manifests),
        )?;
        for parent in &reference.parent_manifests {
            retained_bytes = checked_add(retained_bytes, manifest_retained_bytes(parent)?)?;
        }
        let value = Self {
            reference,
            distribution: Arc::new(distribution),
            origin,
            canonical_origin,
            retained_bytes,
        };
        value.central_amount()?;
        for ordinal in 0..value.distribution.points().len() {
            value.amount(ordinal)?;
        }
        Ok(value)
    }

    /// Exact artifact/source reference retained for recovery.
    pub const fn reference(&self) -> &ForecastValuationReference {
        &self.reference
    }
    /// Complete authentic distribution in native model units.
    pub fn distribution(&self) -> &ForecastTerminalDistribution {
        &self.distribution
    }
    /// Genuine causal source bar used for monetary conversion.
    pub fn origin_bar(&self) -> Option<&MarketBarObservation> {
        match &self.origin {
            ForecastValuationOrigin::CompletedBar(value) => Some(value),
            ForecastValuationOrigin::FinancialEpoch(_) => None,
            ForecastValuationOrigin::CurrentPriceEpoch(epoch) => epoch.market_bar(),
        }
    }
    /// Original data-issued fiscal coordinates and source rows, absent for price models.
    pub fn financial_epoch(&self) -> Option<&FeatureDatasetInputEpoch> {
        match &self.origin {
            ForecastValuationOrigin::FinancialEpoch(value) => Some(value),
            ForecastValuationOrigin::CompletedBar(_)
            | ForecastValuationOrigin::CurrentPriceEpoch(_) => None,
        }
    }
    /// Genuine native model center, without using an empirical support endpoint as the center.
    pub fn central_amount(&self) -> Result<ValuationAmount, FairValueError> {
        self.monetary_value(self.distribution.central())
    }
    pub(crate) fn selected_amount(
        &self,
        selection: ForecastValuationValueSelection,
    ) -> Result<ValuationAmount, FairValueError> {
        match selection {
            ForecastValuationValueSelection::Outcome(index) => self.amount(index),
            ForecastValuationValueSelection::ConditionalMean => self.central_amount(),
            ForecastValuationValueSelection::FinancialOrigin => self.financial_origin_amount(),
        }
    }
    /// Recomputes actual current book/income/cash-flow source amount from the sealed original rows.
    pub fn financial_origin_amount(&self) -> Result<ValuationAmount, FairValueError> {
        let epoch = self
            .financial_epoch()
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let amount = epoch
            .current_financial_amount()
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
        let Some(market_squawk_data::FeatureLabelMeasurement::FinancialAmount {
            currency,
            basis,
            ..
        }) = epoch.financial_measurement()
        else {
            return Err(FairValueError::InvalidProducerEvidence);
        };
        let basis = match basis {
            FinancialAmountBasis::ReportingEntityTotal => {
                ValuationAmountBasis::ReportingEntityTotal
            }
            FinancialAmountBasis::TotalCommonEquity => ValuationAmountBasis::TotalCommonEquity,
            FinancialAmountBasis::PerCommonShare => {
                return Err(FairValueError::InvalidProducerEvidence);
            }
        };
        ValuationAmount::try_new(
            Money::new(amount, currency),
            u8::try_from(amount.scale()).map_err(|_| FairValueError::InvalidAmount)?,
            basis,
        )
    }
    /// Exact monetary outcome, derived only from a genuine model outcome and source price.
    pub fn amount(&self, ordinal: usize) -> Result<ValuationAmount, FairValueError> {
        let point = self
            .distribution
            .points()
            .get(ordinal)
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        self.monetary_value(point.value())
    }
    fn monetary_value(
        &self,
        value: market_squawk_modeling::ForecastValue,
    ) -> Result<ValuationAmount, FairValueError> {
        let raw = Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
            .map_err(|_| FairValueError::InvalidAmount)?;
        let (mut amount, scale, currency, basis) =
            match self.distribution.output_binding().measurement() {
                ForecastMeasurement::Price { currency } => (
                    raw,
                    value.scale(),
                    currency,
                    ValuationAmountBasis::PerInstrumentUnit,
                ),
                ForecastMeasurement::Return => {
                    let bar = self
                        .origin_bar()
                        .ok_or(FairValueError::InvalidProducerEvidence)?;
                    let origin_price = match &self.origin {
                        ForecastValuationOrigin::CurrentPriceEpoch(epoch) => epoch
                            .current_unit_price()
                            .map_err(|_| FairValueError::InvalidProducerEvidence)?,
                        _ => bar.close(),
                    };
                    let amount = Decimal::ONE
                        .checked_add(raw)
                        .and_then(|gross| origin_price.amount().checked_mul(gross))
                        .ok_or(FairValueError::Arithmetic)?
                        .round_dp_with_strategy(
                            12,
                            rust_decimal::RoundingStrategy::MidpointNearestEven,
                        );
                    (
                        amount,
                        12,
                        bar.close().currency(),
                        ValuationAmountBasis::PerInstrumentUnit,
                    )
                }
                ForecastMeasurement::FinancialAmount {
                    currency, basis, ..
                } => (
                    raw,
                    value.scale(),
                    currency,
                    match basis {
                        FinancialAmountBasis::ReportingEntityTotal => {
                            ValuationAmountBasis::ReportingEntityTotal
                        }
                        FinancialAmountBasis::TotalCommonEquity => {
                            ValuationAmountBasis::TotalCommonEquity
                        }
                        FinancialAmountBasis::PerCommonShare => {
                            return Err(FairValueError::InvalidProducerEvidence);
                        }
                    },
                ),
                _ => return Err(FairValueError::InvalidProducerEvidence),
            };
        if basis == ValuationAmountBasis::PerInstrumentUnit && amount <= Decimal::ZERO {
            return Err(FairValueError::InvalidAmount);
        }
        amount.rescale(u32::from(scale));
        ValuationAmount::try_new(Money::new(amount, currency), scale, basis)
    }
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Exact value selected from the original artifact and source epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ForecastValuationValueSelection {
    /// An empirical residual outcome, with actual retained probability mass.
    Outcome(usize),
    /// The authentic model conditional center, independent of outcome support endpoints.
    ConditionalMean,
    /// The actual current native financial amount from original source rows; not a forecast.
    FinancialOrigin,
}
impl ForecastValuationValueSelection {
    pub(crate) const fn digest_ordinal(self) -> u64 {
        match self {
            Self::Outcome(index) => index as u64,
            Self::ConditionalMean => u64::MAX,
            Self::FinancialOrigin => u64::MAX - 1,
        }
    }
}

/// One selected monetary outcome from an artifact-authenticated model distribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForecastValuationEvidence {
    pub(crate) source: Arc<ForecastValuationSource>,
    pub(crate) selection: ForecastValuationValueSelection,
}

impl ForecastValuationEvidence {
    /// Complete authentic source shared by this distribution's outcome inputs.
    pub fn source(&self) -> &ForecastValuationSource {
        &self.source
    }
    /// Exact support ordinal, absent for the model center or original financial source amount.
    pub const fn ordinal(&self) -> Option<usize> {
        match self.selection {
            ForecastValuationValueSelection::Outcome(index) => Some(index),
            _ => None,
        }
    }
    /// Distinguishes original source amount from a model center or empirical outcome.
    pub const fn selection(&self) -> ForecastValuationValueSelection {
        self.selection
    }
}

impl ValuationInput {
    /// Selects one genuine forecast outcome, preserving actual publication and original cutoff.
    pub fn from_forecast_distribution_point(
        source: Arc<ForecastValuationSource>,
        ordinal: usize,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        Self::from_forecast_value(
            source,
            ForecastValuationValueSelection::Outcome(ordinal),
            significance,
        )
    }
    /// Selects the genuine native model center for a financial method input.
    pub fn from_forecast_distribution_central(
        source: Arc<ForecastValuationSource>,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        Self::from_forecast_value(
            source,
            ForecastValuationValueSelection::ConditionalMean,
            significance,
        )
    }
    /// Uses the actual current native source amount retained by the authenticated fiscal serving epoch.
    pub fn from_forecast_financial_origin(
        source: Arc<ForecastValuationSource>,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        Self::from_forecast_value(
            source,
            ForecastValuationValueSelection::FinancialOrigin,
            significance,
        )
    }
    fn from_forecast_value(
        source: Arc<ForecastValuationSource>,
        selection: ForecastValuationValueSelection,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let amount = source.selected_amount(selection)?;
        let from_source = selection == ForecastValuationValueSelection::FinancialOrigin;
        let published_at = source.distribution.published_at();
        let instrument_id = source.reference.instrument_id;
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: SourceId::try_from("market-squawk.forecast")
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            source_identifier: SourceIdentifier::try_from(forecast_value_identifier(selection))
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            payload_digest: source.reference.identity,
            source_timestamp: source.distribution.observed_through(),
            effective_at: source.distribution.target_at(),
            published_at: Some(published_at),
            available_at: Some(published_at),
            received_at: Some(published_at),
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: published_at,
            verification: EvidenceVerification::Verified,
            origin: EvidenceOrigin::ForecastDistribution {
                evidence: Box::new(ForecastValuationEvidence { source, selection }),
            },
        })?;
        Self::try_from_spec(crate::measurement::ValuationInputSpec {
            subject_instrument_id: instrument_id,
            reference_instrument_id: instrument_id,
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: if from_source {
                InputObservability::Observable
            } else {
                InputObservability::Unobservable
            },
            adjustment: if from_source {
                PriceAdjustment::None
            } else {
                PriceAdjustment::Unobservable
            },
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: if from_source {
                DataQuality::Aggregated
            } else {
                DataQuality::Modeled
            },
            evidence,
            use_assessment: None,
        })
    }
}

pub(super) fn forecast_value_identifier(selection: ForecastValuationValueSelection) -> String {
    match selection {
        ForecastValuationValueSelection::Outcome(index) => format!("forecast-outcome-{index}"),
        ForecastValuationValueSelection::ConditionalMean => "forecast-central".to_owned(),
        ForecastValuationValueSelection::FinancialOrigin => "financial-origin".to_owned(),
    }
}
