//! Three independent, immutable event forecasts. These values grant no execution authority.

use super::evidence::sha256_content;
use super::{InvestmentProposalError as Error, ProposalEvidenceWindow, ProposalForecastVintageId};
use crate::DecisionContentDigest;
use market_squawk_domain::{DigestAlgorithm, InstrumentId, Timestamp};
use market_squawk_modeling::{
    CalibrationWindow, FixedHorizonOriginBasis, ForecastMeasurement, ForecastTargetMeaning,
    ForecastValue, ForecastVintage, ModelMetadata, ModelOutputSemantics,
    ProbabilityCalibrationArtifacts,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use std::num::NonZeroU64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbabilityEventKind {
    PriceHigher,
    BenchmarkOutperformance,
    ProfitAfterCosts,
}
impl ProbabilityEventKind {
    pub const fn label_component_name(self) -> &'static str {
        match self {
            Self::PriceHigher => "research.fixed-horizon-price-higher",
            Self::BenchmarkOutperformance => "research.fixed-horizon-benchmark-outperformance",
            Self::ProfitAfterCosts => "research.fixed-horizon-profit-after-costs",
        }
    }
    pub const fn tag(self) -> u8 {
        match self {
            Self::PriceHigher => 1,
            Self::BenchmarkOutperformance => 2,
            Self::ProfitAfterCosts => 3,
        }
    }
    pub fn from_target(target: ForecastTargetMeaning) -> Result<Self, Error> {
        let ForecastTargetMeaning::FixedHorizonEvent {
            event,
            origin_basis,
            ..
        } = target
        else {
            return Err(Error::InvalidEvidenceMetric);
        };
        event.validate().map_err(|_| Error::InvalidEvidenceMetric)?;
        if !matches!(
            origin_basis,
            FixedHorizonOriginBasis::CompletedBarClose
                | FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar
        ) {
            return Err(Error::InvalidEvidenceMetric);
        }
        [
            Self::PriceHigher,
            Self::BenchmarkOutperformance,
            Self::ProfitAfterCosts,
        ]
        .into_iter()
        .find(|kind| kind.label_component_name() == event.label_component_name())
        .ok_or(Error::InvalidEvidenceMetric)
    }
}

/// Original job selector. Ready evidence separately proves completion; this grants no authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbabilityForecastReference {
    job_id: [u8; 16],
    generation: NonZeroU64,
    forecast_token: [u8; 16],
    request_identity: DecisionContentDigest,
    profile_identity: DecisionContentDigest,
}
impl ProbabilityForecastReference {
    pub fn try_new(
        job_id: [u8; 16],
        generation: NonZeroU64,
        forecast_token: [u8; 16],
        request_identity: DecisionContentDigest,
        profile_identity: DecisionContentDigest,
    ) -> Result<Self, Error> {
        if job_id == [0; 16] || forecast_token == [0; 16] {
            return Err(Error::ReservedIdentity);
        }
        Ok(Self {
            job_id,
            generation,
            forecast_token,
            request_identity,
            profile_identity,
        })
    }
    pub const fn job_id(self) -> [u8; 16] {
        self.job_id
    }
    pub const fn generation(self) -> NonZeroU64 {
        self.generation
    }
    pub const fn forecast_token(self) -> [u8; 16] {
        self.forecast_token
    }
    pub const fn request_identity(self) -> DecisionContentDigest {
        self.request_identity
    }
    pub const fn profile_identity(self) -> DecisionContentDigest {
        self.profile_identity
    }
}

/// Exact IEEE identities of original evaluation means, without inventing values for empty bins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProbabilityReliabilityEvidence {
    pub count: u32,
    pub mean_probability_bits: Option<u64>,
    pub observed_frequency_bits: Option<u64>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbabilityCalibrationSummary {
    pub policy_identity: DecisionContentDigest,
    pub outcomes_identity: DecisionContentDigest,
    pub train_window: CalibrationWindow,
    pub calibration_window: CalibrationWindow,
    pub evaluation_window: CalibrationWindow,
    pub slope_bits: u64,
    pub intercept_bits: u64,
    pub brier_score_bits: u64,
    pub log_loss_bits: u64,
    pub reliability_bins: [ProbabilityReliabilityEvidence; 10],
}
impl ProbabilityCalibrationSummary {
    fn from_original(value: &ProbabilityCalibrationArtifacts) -> Result<Self, Error> {
        let result = Self {
            policy_identity: sha256_content(value.policy_hash().bytes())?,
            outcomes_identity: sha256_content(value.outcomes_hash().bytes())?,
            train_window: value.train_window(),
            calibration_window: value.calibration_window(),
            evaluation_window: value.evaluation_window(),
            slope_bits: value.calibration_slope().to_bits(),
            intercept_bits: value.calibration_intercept().to_bits(),
            brier_score_bits: value.brier_score().to_bits(),
            log_loss_bits: value.log_loss().to_bits(),
            reliability_bins: std::array::from_fn(|index| {
                let bin = value.reliability_bins()[index];
                ProbabilityReliabilityEvidence {
                    count: bin.count(),
                    mean_probability_bits: bin.mean_probability().map(f64::to_bits),
                    observed_frequency_bits: bin.observed_frequency().map(f64::to_bits),
                }
            }),
        };
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<(), Error> {
        let windows = [
            self.train_window,
            self.calibration_window,
            self.evaluation_window,
        ];
        if windows.iter().any(|w| {
            w.start()
                .zip(w.end())
                .is_none_or(|(start, end)| start >= end)
        }) || self.train_window.end() > self.calibration_window.start()
            || self.calibration_window.end() > self.evaluation_window.start()
            || !f64::from_bits(self.slope_bits).is_finite()
            || !f64::from_bits(self.intercept_bits).is_finite()
            || !unit_float(self.brier_score_bits)
            || !f64::from_bits(self.log_loss_bits).is_finite()
            || f64::from_bits(self.log_loss_bits) < 0.0
        {
            return Err(Error::InvalidEvidenceMetric);
        }
        let mut count = 0_u32;
        for bin in &self.reliability_bins {
            count = count
                .checked_add(bin.count)
                .ok_or(Error::ArithmeticOverflow)?;
            match (
                bin.count,
                bin.mean_probability_bits,
                bin.observed_frequency_bits,
            ) {
                (0, None, None) => {}
                (n, Some(mean), Some(observed))
                    if n > 0 && unit_float(mean) && unit_float(observed) => {}
                _ => return Err(Error::InvalidEvidenceMetric),
            }
        }
        if count != self.evaluation_window.observations().get() {
            return Err(Error::InvalidEvidenceMetric);
        }
        Ok(())
    }
    pub fn brier_score(&self) -> f64 {
        f64::from_bits(self.brier_score_bits)
    }
    pub fn log_loss(&self) -> f64 {
        f64::from_bits(self.log_loss_bits)
    }
}
fn unit_float(bits: u64) -> bool {
    let v = f64::from_bits(bits);
    v.is_finite() && (0.0..=1.0).contains(&v)
}

/// Strict recovery fields. Reconstructing this record replays saved evidence; it does not admit a source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbabilityForecastEvidenceRecord {
    pub reference: ProbabilityForecastReference,
    pub instrument_id: InstrumentId,
    pub target: ForecastTargetMeaning,
    pub observed_at: Timestamp,
    pub target_at: Timestamp,
    pub probability: ForecastValue,
    pub vintage_id: ProposalForecastVintageId,
    pub output_binding_identity: DecisionContentDigest,
    pub metadata_identity: DecisionContentDigest,
    pub model_artifact_identity: DecisionContentDigest,
    pub training_run_identity: DecisionContentDigest,
    pub forecast_artifact_identity: DecisionContentDigest,
    pub source_feature_identity: DecisionContentDigest,
    pub calibration: ProbabilityCalibrationSummary,
    pub window: ProposalEvidenceWindow,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbabilityForecastEvidence {
    record: ProbabilityForecastEvidenceRecord,
    kind: ProbabilityEventKind,
}
impl ProbabilityForecastEvidence {
    /// Projects the re-admitted original binary vintage and model, never a residual distribution.
    pub fn try_from_forecast(
        reference: ProbabilityForecastReference,
        vintage: &ForecastVintage,
        metadata: &ModelMetadata,
        source_knowledge_cutoff: Timestamp,
        source_selection_identity: DecisionContentDigest,
        source_feature_identity: DecisionContentDigest,
    ) -> Result<Self, Error> {
        let path = vintage.path();
        let [point] = path.points() else {
            return Err(Error::InvalidEvidenceMetric);
        };
        let calibration = path
            .probability_calibration()
            .ok_or(Error::InvalidEvidenceMetric)?;
        if path.output_binding() != metadata.output_binding()
            || path.output_binding().output_semantics() != ModelOutputSemantics::BinaryProbability
            || path.output_binding().measurement() != ForecastMeasurement::Probability
            || path.probability_calibration() != metadata.probability_calibration()
            || path.metadata_hash() != metadata.metadata_hash()
            || path.artifact_hash() != metadata.artifact_hash()
            || path.training_run_hash() != metadata.training_run_hash()
            || path.model_id() != metadata.model_id()
            || path.dataset() != metadata.dataset()
            || path.calibration().is_some()
            || point.intervals().is_some()
            || path.available_at() > source_knowledge_cutoff
            || source_knowledge_cutoff > vintage.created_at()
        {
            return Err(Error::InvalidEvidenceMetric);
        }
        let observed_at = path.observed_cutoff().ok_or(Error::InvalidTimeOrder)?;
        Self::try_recover(ProbabilityForecastEvidenceRecord {
            reference,
            instrument_id: path.instrument_id(),
            target: path.output_binding().target(),
            observed_at,
            target_at: point.target_at().ok_or(Error::InvalidTimeOrder)?,
            probability: point.central(),
            vintage_id: ProposalForecastVintageId::try_from_forecast_vintage(vintage.id())?,
            output_binding_identity: sha256_content(path.output_binding().identity().bytes())?,
            metadata_identity: sha256_content(path.metadata_hash().bytes())?,
            model_artifact_identity: sha256_content(path.artifact_hash().bytes())?,
            training_run_identity: sha256_content(path.training_run_hash().bytes())?,
            forecast_artifact_identity: sha256_content(vintage.artifact_hash().bytes())?,
            source_feature_identity,
            calibration: ProbabilityCalibrationSummary::from_original(calibration)?,
            window: ProposalEvidenceWindow::try_from_derived(
                observed_at,
                source_knowledge_cutoff,
                vintage.created_at(),
                vintage.expires_at(),
                source_selection_identity,
            )?,
        })
    }
    /// Verifies strict retained shape for deterministic recovery, without granting fresh authority.
    pub fn try_recover(record: ProbabilityForecastEvidenceRecord) -> Result<Self, Error> {
        let kind = ProbabilityEventKind::from_target(record.target)?;
        let ForecastTargetMeaning::FixedHorizonEvent { horizon_nanos, .. } = record.target else {
            return Err(Error::InvalidEvidenceMetric);
        };
        let probability = Decimal::try_from_i128_with_scale(
            record.probability.mantissa(),
            u32::from(record.probability.scale()),
        )
        .map_err(|_| Error::InvalidEvidenceMetric)?;
        record.calibration.validate()?;
        if !(Decimal::ZERO..=Decimal::ONE).contains(&probability)
            || record
                .observed_at
                .checked_add_nanos(
                    i64::try_from(horizon_nanos.get()).map_err(|_| Error::InvalidTimeOrder)?,
                )
                .ok()
                != Some(record.target_at)
            || record.window.observed_at() != record.observed_at
            || record.target_at <= record.window.available_at()
            || record
                .calibration
                .evaluation_window
                .end()
                .is_none_or(|end| end > record.observed_at)
        {
            return Err(Error::InvalidEvidenceMetric);
        }
        Ok(Self { record, kind })
    }
    pub const fn record(&self) -> &ProbabilityForecastEvidenceRecord {
        &self.record
    }
    pub const fn kind(&self) -> ProbabilityEventKind {
        self.kind
    }
    pub const fn target(&self) -> ForecastTargetMeaning {
        self.record.target
    }
    pub const fn probability(&self) -> ForecastValue {
        self.record.probability
    }
    pub const fn reference(&self) -> ProbabilityForecastReference {
        self.record.reference
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbabilityUnavailableReason {
    ForecastEvidenceUnavailable,
    SourceEvidenceUnavailable,
    BenchmarkEvidenceUnavailable,
    CostEvidenceUnavailable,
    CalibrationUnavailable,
    HorizonMismatch,
    OriginMismatch,
    ForecastExpired,
    ForecastFailed,
}
impl ProbabilityUnavailableReason {
    pub const fn tag(self) -> u8 {
        match self {
            Self::ForecastEvidenceUnavailable => 1,
            Self::SourceEvidenceUnavailable => 2,
            Self::BenchmarkEvidenceUnavailable => 3,
            Self::CostEvidenceUnavailable => 4,
            Self::CalibrationUnavailable => 5,
            Self::HorizonMismatch => 6,
            Self::ForecastExpired => 7,
            Self::ForecastFailed => 8,
            Self::OriginMismatch => 9,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProbabilityEventEvidence {
    Ready(ProbabilityForecastEvidence),
    Unavailable {
        kind: ProbabilityEventKind,
        target: Option<ForecastTargetMeaning>,
        reference: Option<ProbabilityForecastReference>,
        reason: ProbabilityUnavailableReason,
    },
}
impl ProbabilityEventEvidence {
    pub fn kind(&self) -> ProbabilityEventKind {
        match self {
            Self::Ready(v) => v.kind(),
            Self::Unavailable { kind, .. } => *kind,
        }
    }
    pub fn reference(&self) -> Option<ProbabilityForecastReference> {
        match self {
            Self::Ready(v) => Some(v.reference()),
            Self::Unavailable { reference, .. } => *reference,
        }
    }
    pub fn target(&self) -> Option<ForecastTargetMeaning> {
        match self {
            Self::Ready(v) => Some(v.target()),
            Self::Unavailable { target, .. } => *target,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvestmentProbabilityEvidence {
    instrument_id: InstrumentId,
    horizon_nanos: NonZeroU64,
    events: [ProbabilityEventEvidence; 3],
    digest: DecisionContentDigest,
}
impl InvestmentProbabilityEvidence {
    pub fn try_new(
        instrument_id: InstrumentId,
        horizon_nanos: NonZeroU64,
        price_higher: ProbabilityEventEvidence,
        benchmark_outperformance: ProbabilityEventEvidence,
        profit_after_costs: ProbabilityEventEvidence,
    ) -> Result<Self, Error> {
        if horizon_nanos.get() > i64::MAX as u64 {
            return Err(Error::InvalidTimeOrder);
        }
        let events = [price_higher, benchmark_outperformance, profit_after_costs];
        let mut origin = None;
        let mut profile = None;
        for (index, event) in events.iter().enumerate() {
            if usize::from(event.kind().tag()) != index + 1 {
                return Err(Error::InvalidEvidenceMetric);
            }
            if let Some(target) = event.target() {
                if ProbabilityEventKind::from_target(target)? != event.kind() {
                    return Err(Error::InvalidEvidenceMetric);
                }
                let permits_mismatch = matches!(
                    event,
                    ProbabilityEventEvidence::Unavailable {
                        reason: ProbabilityUnavailableReason::HorizonMismatch,
                        ..
                    }
                );
                if !permits_mismatch
                    && !matches!(target,ForecastTargetMeaning::FixedHorizonEvent{horizon_nanos: h,..} if h==horizon_nanos)
                {
                    return Err(Error::InvalidEvidenceMetric);
                }
            }
            if let Some(reference) = event.reference() {
                if profile.is_some_and(|value| value != reference.profile_identity()) {
                    return Err(Error::InvalidEvidenceMetric);
                }
                profile = Some(reference.profile_identity());
            }
            if let ProbabilityEventEvidence::Ready(value) = event {
                let row = value.record();
                if row.instrument_id != instrument_id
                    || origin.is_some_and(|value| value != (row.observed_at, row.target_at))
                {
                    return Err(Error::InvalidEvidenceMetric);
                }
                origin = Some((row.observed_at, row.target_at));
            }
        }
        let digest = hash_group(instrument_id, horizon_nanos, &events)?;
        Ok(Self {
            instrument_id,
            horizon_nanos,
            events,
            digest,
        })
    }
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    pub const fn horizon_nanos(&self) -> NonZeroU64 {
        self.horizon_nanos
    }
    pub const fn events(&self) -> &[ProbabilityEventEvidence; 3] {
        &self.events
    }
    pub const fn price_higher(&self) -> &ProbabilityEventEvidence {
        &self.events[0]
    }
    pub const fn benchmark_outperformance(&self) -> &ProbabilityEventEvidence {
        &self.events[1]
    }
    pub const fn profit_after_costs(&self) -> &ProbabilityEventEvidence {
        &self.events[2]
    }
    pub const fn digest(&self) -> DecisionContentDigest {
        self.digest
    }
}

fn hash_group(
    instrument: InstrumentId,
    horizon: NonZeroU64,
    events: &[ProbabilityEventEvidence; 3],
) -> Result<DecisionContentDigest, Error> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/investment-event-probabilities/v1\0");
    hash.update(instrument.as_uuid().as_bytes());
    hash.update(horizon.get().to_be_bytes());
    for event in events {
        hash.update([event.kind().tag()]);
        match event.target() {
            Some(target) => {
                hash.update([1]);
                hash_target(&mut hash, target)?;
            }
            None => hash.update([0]),
        }
        match event.reference() {
            Some(reference) => {
                hash.update([1]);
                hash.update(reference.job_id);
                hash.update(reference.generation.get().to_be_bytes());
                hash.update(reference.forecast_token);
                hash_content(&mut hash, reference.request_identity);
                hash_content(&mut hash, reference.profile_identity);
            }
            None => hash.update([0]),
        }
        match event {
            ProbabilityEventEvidence::Unavailable { reason, .. } => hash.update([0, reason.tag()]),
            ProbabilityEventEvidence::Ready(value) => {
                hash.update([1]);
                let r = value.record();
                hash.update(r.instrument_id.as_uuid().as_bytes());
                for t in [
                    r.observed_at,
                    r.target_at,
                    r.window.observed_at(),
                    r.window.source_knowledge_cutoff(),
                    r.window.available_at(),
                    r.window.expires_at(),
                ] {
                    hash.update(t.unix_nanos().to_be_bytes());
                }
                hash.update(r.probability.mantissa().to_be_bytes());
                hash.update([r.probability.scale()]);
                hash.update(r.vintage_id.bytes());
                for identity in [
                    r.output_binding_identity,
                    r.metadata_identity,
                    r.model_artifact_identity,
                    r.training_run_identity,
                    r.forecast_artifact_identity,
                    r.source_feature_identity,
                    r.window.content_identity(),
                    r.calibration.policy_identity,
                    r.calibration.outcomes_identity,
                ] {
                    hash_content(&mut hash, identity);
                }
                for w in [
                    r.calibration.train_window,
                    r.calibration.calibration_window,
                    r.calibration.evaluation_window,
                ] {
                    hash.update(
                        w.start()
                            .ok_or(Error::InvalidTimeOrder)?
                            .unix_nanos()
                            .to_be_bytes(),
                    );
                    hash.update(
                        w.end()
                            .ok_or(Error::InvalidTimeOrder)?
                            .unix_nanos()
                            .to_be_bytes(),
                    );
                    hash.update(w.observations().get().to_be_bytes());
                }
                for bits in [
                    r.calibration.slope_bits,
                    r.calibration.intercept_bits,
                    r.calibration.brier_score_bits,
                    r.calibration.log_loss_bits,
                ] {
                    hash.update(bits.to_be_bytes());
                }
                for bin in r.calibration.reliability_bins {
                    hash.update(bin.count.to_be_bytes());
                    for value in [bin.mean_probability_bits, bin.observed_frequency_bits] {
                        match value {
                            None => hash.update([0]),
                            Some(bits) => {
                                hash.update([1]);
                                hash.update(bits.to_be_bytes());
                            }
                        }
                    }
                }
            }
        }
    }
    sha256_content(hash.finalize().into())
}
fn hash_content(hash: &mut Sha256, value: DecisionContentDigest) {
    let digest = value.evidence_digest();
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}
fn hash_target(hash: &mut Sha256, target: ForecastTargetMeaning) -> Result<(), Error> {
    let ForecastTargetMeaning::FixedHorizonEvent {
        horizon_nanos,
        origin_basis,
        event,
    } = target
    else {
        return Err(Error::InvalidEvidenceMetric);
    };
    hash.update(horizon_nanos.get().to_be_bytes());
    // Preserve the closed original origin basis independently of display spelling.
    let origin = match origin_basis {
        FixedHorizonOriginBasis::CompletedBarClose => 1,
        FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar => 2,
        _ => return Err(Error::InvalidEvidenceMetric),
    };
    hash.update([origin]);
    hash.update(event.digest().bytes());
    Ok(())
}
