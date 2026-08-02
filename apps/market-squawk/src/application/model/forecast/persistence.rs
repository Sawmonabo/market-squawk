//! Closed durable forecast-index records and restart validation.

use std::{collections::HashSet, str::FromStr};

use market_squawk_data::{Sha256Digest, UniverseId};
use market_squawk_domain::{InstrumentId, ModelId};
use market_squawk_modeling::{
    BundleId, CalibrationMethod, ForecastOutcome, ForecastPath, ForecastVintage,
};
use market_squawk_services::ArtifactReference;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ForecastApplicationError, ForecastApplicationLimits, INDEX_SCHEMA_VERSION};

const MAXIMUM_CALIBRATION_ASSUMPTION_BYTES: usize = 512;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct VintageRecord {
    pub(super) vintage_id: String,
    pub(super) request_hash: String,
    controlled_artifact: ControlledArtifactRecord,
    #[serde(flatten)]
    payload: ForecastPayloadRecord,
}

impl VintageRecord {
    pub(super) fn from_publication(
        request_hash: Sha256Digest,
        vintage: &ForecastVintage,
        payload: ForecastPayloadRecord,
        artifact: &ArtifactReference,
    ) -> Result<Self, ForecastApplicationError> {
        if artifact.sha256() != hex(vintage.artifact_hash().bytes()) {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        Ok(Self {
            vintage_id: hex(vintage.id().bytes()),
            request_hash: hex(request_hash.bytes()),
            controlled_artifact: ControlledArtifactRecord {
                artifact_id: artifact.id().to_owned(),
                sha256: artifact.sha256().to_owned(),
                byte_count: artifact.byte_count(),
                media_type: artifact.media_type().to_owned(),
            },
            payload,
        })
    }

    pub(super) fn summary(&self) -> Value {
        json!({
            "vintageId": self.vintage_id,
            "requestHash": self.request_hash,
            "instrumentId": self.payload.instrument_id,
            "modelId": self.payload.model_id,
            "bundleId": self.payload.bundle_id,
            "bundleVersion": self.payload.bundle_version,
            "observedThroughUnixNanos": self.payload.observed_through_unix_nanos,
            "createdAtUnixNanos": self.payload.created_at_unix_nanos,
            "expiresAtUnixNanos": self.payload.expires_at_unix_nanos,
            "horizonPoints": self.payload.horizon_points,
            "horizonStepNanos": self.payload.horizon_step_nanos,
            "hasCalibratedIntervals": self.payload.calibration.is_some(),
            "quality": self.payload.quality,
            "unavailableReason": self.payload.unavailable_reason,
            "controlledArtifact": self.controlled_artifact,
        })
    }

    fn validate(&self) -> bool {
        valid_digest(&self.vintage_id)
            && valid_digest(&self.request_hash)
            && self.controlled_artifact.validate()
            && self.payload.validate()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ControlledArtifactRecord {
    artifact_id: String,
    sha256: String,
    byte_count: usize,
    media_type: String,
}

impl ControlledArtifactRecord {
    fn validate(&self) -> bool {
        self.artifact_id
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            && self.artifact_id.len() <= 160
            && self
                .artifact_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            && valid_digest(&self.sha256)
            && self.byte_count > 0
            && self.media_type == "application/json"
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ForecastPayloadRecord {
    instrument_id: String,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    metadata_hash: String,
    artifact_hash: String,
    training_run_hash: String,
    dataset_export_hash: String,
    dataset_selection_hash: String,
    universe_id: String,
    training_start_unix_nanos: i64,
    training_end_unix_nanos: i64,
    feature_semantic_hashes: Vec<String>,
    observed_through_unix_nanos: i64,
    available_at_unix_nanos: i64,
    created_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
    model_age_nanos_at_publication: i64,
    data_age_nanos_at_publication: i64,
    horizon_points: u16,
    horizon_step_nanos: u64,
    quality: String,
    points: Vec<PointRecord>,
    calibration: Option<CalibrationRecord>,
    limitations: Vec<String>,
    unavailable_reason: String,
}

impl ForecastPayloadRecord {
    pub(super) fn from_path(
        path: &ForecastPath,
        created_at: market_squawk_domain::Timestamp,
        expires_at: market_squawk_domain::Timestamp,
    ) -> Result<Self, ForecastApplicationError> {
        if created_at < path.available_at()
            || path
                .points()
                .first()
                .is_none_or(|point| created_at >= point.target_at())
            || expires_at <= created_at
        {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        let model_age_nanos_at_publication = created_at
            .unix_nanos()
            .checked_sub(path.training_period().end().unix_nanos())
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        let data_age_nanos_at_publication = created_at
            .unix_nanos()
            .checked_sub(path.observed_cutoff().unix_nanos())
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        if model_age_nanos_at_publication < 0 || data_age_nanos_at_publication < 0 {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        Ok(Self {
            instrument_id: path.instrument_id().to_string(),
            model_id: path.model_id().to_string(),
            bundle_id: path.bundle_id().as_str().to_owned(),
            bundle_version: path.bundle_version().get(),
            metadata_hash: hex(path.metadata_hash().bytes()),
            artifact_hash: hex(path.artifact_hash().bytes()),
            training_run_hash: hex(path.training_run_hash().bytes()),
            dataset_export_hash: hex(path.dataset().export_digest().bytes()),
            dataset_selection_hash: hex(path.dataset().selection_digest().bytes()),
            universe_id: path.universe_id().as_str().to_owned(),
            training_start_unix_nanos: path.training_period().start().unix_nanos(),
            training_end_unix_nanos: path.training_period().end().unix_nanos(),
            feature_semantic_hashes: path
                .feature_semantic_digests()
                .iter()
                .map(|digest| hex(digest.as_bytes()))
                .collect(),
            observed_through_unix_nanos: path.observed_cutoff().unix_nanos(),
            available_at_unix_nanos: path.available_at().unix_nanos(),
            created_at_unix_nanos: created_at.unix_nanos(),
            expires_at_unix_nanos: expires_at.unix_nanos(),
            model_age_nanos_at_publication,
            data_age_nanos_at_publication,
            horizon_points: path.horizon().points().get(),
            horizon_step_nanos: path.horizon().step_nanos().get(),
            quality: "modeled".to_owned(),
            points: path
                .points()
                .iter()
                .copied()
                .map(PointRecord::from_point)
                .collect(),
            calibration: path.calibration().map(CalibrationRecord::from_evidence),
            limitations: path
                .limitations()
                .iter()
                .map(|value| value.to_string())
                .collect(),
            unavailable_reason: path.fallback_reason().to_owned(),
        })
    }

    fn validate(&self) -> bool {
        let first = match self.points.first() {
            Some(value) => value,
            None => return false,
        };
        let model_age = self
            .created_at_unix_nanos
            .checked_sub(self.training_end_unix_nanos);
        let data_age = self
            .created_at_unix_nanos
            .checked_sub(self.observed_through_unix_nanos);
        if InstrumentId::from_str(&self.instrument_id).is_err()
            || ModelId::from_str(&self.model_id).is_err()
            || BundleId::try_new(&self.bundle_id).is_err()
            || UniverseId::from_str(&self.universe_id).is_err()
            || self.bundle_version == 0
            || [
                &self.metadata_hash,
                &self.artifact_hash,
                &self.training_run_hash,
                &self.dataset_export_hash,
                &self.dataset_selection_hash,
            ]
            .iter()
            .any(|digest| !valid_digest(digest))
            || self.feature_semantic_hashes.is_empty()
            || self.feature_semantic_hashes.len() > market_squawk_modeling::MAX_MODEL_FEATURES
            || self
                .feature_semantic_hashes
                .iter()
                .any(|digest| !valid_digest(digest))
            || self.training_start_unix_nanos >= self.training_end_unix_nanos
            || self.training_end_unix_nanos > self.observed_through_unix_nanos
            || self.available_at_unix_nanos > self.observed_through_unix_nanos
            || self.created_at_unix_nanos < self.available_at_unix_nanos
            || self.created_at_unix_nanos >= first.target_at_unix_nanos
            || self.expires_at_unix_nanos <= self.created_at_unix_nanos
            || model_age != Some(self.model_age_nanos_at_publication)
            || data_age != Some(self.data_age_nanos_at_publication)
            || self.model_age_nanos_at_publication < 0
            || self.data_age_nanos_at_publication < 0
            || self.horizon_points == 0
            || usize::from(self.horizon_points) != self.points.len()
            || self.horizon_step_nanos == 0
            || self.quality != "modeled"
        {
            return false;
        }
        let calibrated = self.calibration.is_some();
        self.calibration
            .as_ref()
            .is_none_or(|value| value.validate(self.observed_through_unix_nanos))
            && self.points.iter().enumerate().all(|(index, point)| {
                point.validate(
                    self.observed_through_unix_nanos,
                    self.horizon_step_nanos,
                    index,
                    calibrated,
                )
            })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct PointRecord {
    target_at_unix_nanos: i64,
    central_mantissa: String,
    decimal_scale: u8,
    intervals: Option<IntervalRecord>,
}

impl PointRecord {
    fn from_point(point: market_squawk_modeling::ForecastPoint) -> Self {
        let central = point.central();
        Self {
            target_at_unix_nanos: point.target_at().unix_nanos(),
            central_mantissa: central.mantissa().to_string(),
            decimal_scale: central.scale(),
            intervals: point.intervals().map(IntervalRecord::from_intervals),
        }
    }

    fn validate(&self, cutoff: i64, step: u64, index: usize, calibrated: bool) -> bool {
        let ordinal = match u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
        {
            Some(value) => value,
            None => return false,
        };
        let target = step
            .checked_mul(ordinal)
            .and_then(|offset| i64::try_from(offset).ok())
            .and_then(|offset| cutoff.checked_add(offset));
        let central = match self.central_mantissa.parse::<i128>() {
            Ok(value) => value,
            Err(_error) => return false,
        };
        target == Some(self.target_at_unix_nanos)
            && self.decimal_scale <= market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
            && (self.intervals.is_some() == calibrated)
            && self
                .intervals
                .as_ref()
                .is_none_or(|value| value.validate(central))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct IntervalRecord {
    interval_50: [String; 2],
    interval_80: [String; 2],
    interval_95: [String; 2],
}

impl IntervalRecord {
    fn from_intervals(value: market_squawk_modeling::ForecastIntervals) -> Self {
        fn pair(value: market_squawk_modeling::ForecastInterval) -> [String; 2] {
            [
                value.lower().mantissa().to_string(),
                value.upper().mantissa().to_string(),
            ]
        }
        Self {
            interval_50: pair(value.interval_50()),
            interval_80: pair(value.interval_80()),
            interval_95: pair(value.interval_95()),
        }
    }

    fn validate(&self, central: i128) -> bool {
        let parsed = [&self.interval_50, &self.interval_80, &self.interval_95].map(|pair| {
            pair[0]
                .parse::<i128>()
                .ok()
                .zip(pair[1].parse::<i128>().ok())
        });
        match parsed {
            [Some(fifty), Some(eighty), Some(ninety_five)] => {
                ninety_five.0 <= eighty.0
                    && eighty.0 <= fifty.0
                    && fifty.0 <= central
                    && central <= fifty.1
                    && fifty.0 <= fifty.1
                    && fifty.1 <= eighty.1
                    && eighty.1 <= ninety_five.1
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CalibrationRecord {
    method: String,
    window_start_unix_nanos: i64,
    window_end_unix_nanos: i64,
    observations: u32,
    policy_hash: String,
    residuals_hash: String,
    target_coverage_basis_points: [u16; 3],
    lower_offsets: [f64; 3],
    upper_offsets: [f64; 3],
    realized_covered: [u64; 3],
    realized_total: [u64; 3],
    coverage_interpretation: String,
    dependence_assumptions: String,
}

impl CalibrationRecord {
    fn from_evidence(value: &market_squawk_modeling::CalibrationEvidence) -> Self {
        let bands = value.bands();
        Self {
            method: match value.method() {
                CalibrationMethod::MapieEnbpi => "mapie_enbpi",
                CalibrationMethod::MapieAci => "mapie_aci",
                CalibrationMethod::ResidualQuantile => "residual_quantile",
            }
            .to_owned(),
            window_start_unix_nanos: value.window().start().unix_nanos(),
            window_end_unix_nanos: value.window().end().unix_nanos(),
            observations: value.window().observations().get(),
            policy_hash: hex(value.policy_hash().bytes()),
            residuals_hash: hex(value.residuals_hash().bytes()),
            target_coverage_basis_points: bands.map(|band| band.coverage().basis_points()),
            lower_offsets: bands.map(|band| band.lower_offset()),
            upper_offsets: bands.map(|band| band.upper_offset()),
            realized_covered: bands.map(|band| band.realized().covered()),
            realized_total: bands.map(|band| band.realized().total().get()),
            coverage_interpretation:
                "realized marginal empirical coverage; not a per-observation guarantee".to_owned(),
            dependence_assumptions: value.dependence_assumptions().to_owned(),
        }
    }

    fn validate(&self, observed_cutoff: i64) -> bool {
        matches!(
            self.method.as_str(),
            "mapie_enbpi" | "mapie_aci" | "residual_quantile"
        ) && self.window_start_unix_nanos < self.window_end_unix_nanos
            && self.window_end_unix_nanos <= observed_cutoff
            && self.observations > 0
            && valid_digest(&self.policy_hash)
            && valid_digest(&self.residuals_hash)
            && self.target_coverage_basis_points == [5_000, 8_000, 9_500]
            && self.lower_offsets.iter().all(|value| value.is_finite())
            && self.upper_offsets.iter().all(|value| value.is_finite())
            && self.lower_offsets[2] <= self.lower_offsets[1]
            && self.lower_offsets[1] <= self.lower_offsets[0]
            && self.lower_offsets[0] <= 0.0
            && self.upper_offsets[0] >= 0.0
            && self.upper_offsets[0] <= self.upper_offsets[1]
            && self.upper_offsets[1] <= self.upper_offsets[2]
            && self
                .realized_covered
                .iter()
                .zip(self.realized_total)
                .all(|(covered, total)| total > 0 && *covered <= total)
            && self.coverage_interpretation
                == "realized marginal empirical coverage; not a per-observation guarantee"
            && !self.dependence_assumptions.is_empty()
            && self.dependence_assumptions.len() <= MAXIMUM_CALIBRATION_ASSUMPTION_BYTES
            && !self
                .dependence_assumptions
                .bytes()
                .any(|byte| byte.is_ascii_control())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct OutcomeRecord {
    outcome_id: String,
    pub(super) vintage_id: String,
    target_at_unix_nanos: i64,
    observed_at_unix_nanos: i64,
    available_at_unix_nanos: i64,
    actual_mantissa: String,
    decimal_scale: u8,
    signed_error_mantissa: String,
    absolute_error_mantissa: String,
    source_pit_hash: String,
    quality: String,
}

impl OutcomeRecord {
    pub(super) fn id(&self) -> &str {
        &self.outcome_id
    }

    pub(super) fn from_outcome(
        outcome: &ForecastOutcome,
        vintage: &VintageRecord,
    ) -> Result<Self, ForecastApplicationError> {
        let point = vintage
            .payload
            .points
            .iter()
            .find(|point| point.target_at_unix_nanos == outcome.target_at().unix_nanos())
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        let central = point
            .central_mantissa
            .parse::<i128>()
            .map_err(|_error| ForecastApplicationError::InvalidRecord)?;
        let error = outcome
            .actual()
            .mantissa()
            .checked_sub(central)
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        let absolute = error
            .checked_abs()
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        Ok(Self {
            outcome_id: hex(outcome.id().bytes()),
            vintage_id: hex(outcome.vintage_id().bytes()),
            target_at_unix_nanos: outcome.target_at().unix_nanos(),
            observed_at_unix_nanos: outcome.observed_at().unix_nanos(),
            available_at_unix_nanos: outcome.available_at().unix_nanos(),
            actual_mantissa: outcome.actual().mantissa().to_string(),
            decimal_scale: outcome.actual().scale(),
            signed_error_mantissa: error.to_string(),
            absolute_error_mantissa: absolute.to_string(),
            source_pit_hash: hex(outcome.source_pit_hash().bytes()),
            quality: serde_json::to_value(outcome.quality())
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .ok_or(ForecastApplicationError::InvalidRecord)?,
        })
    }

    fn validate(&self, vintage: &VintageRecord) -> bool {
        let point = match vintage
            .payload
            .points
            .iter()
            .find(|point| point.target_at_unix_nanos == self.target_at_unix_nanos)
        {
            Some(value) => value,
            None => return false,
        };
        let exact_errors = self
            .actual_mantissa
            .parse::<i128>()
            .ok()
            .zip(point.central_mantissa.parse::<i128>().ok())
            .and_then(|(actual, central)| actual.checked_sub(central))
            .and_then(|signed| signed.checked_abs().map(|absolute| (signed, absolute)))
            .is_some_and(|(signed, absolute)| {
                self.signed_error_mantissa == signed.to_string()
                    && self.absolute_error_mantissa == absolute.to_string()
            });
        valid_digest(&self.outcome_id)
            && valid_digest(&self.vintage_id)
            && self.vintage_id == vintage.vintage_id
            && self.observed_at_unix_nanos >= self.target_at_unix_nanos
            && self.available_at_unix_nanos >= self.observed_at_unix_nanos
            && exact_errors
            && valid_digest(&self.source_pit_hash)
            && self.decimal_scale == point.decimal_scale
            && self.decimal_scale <= market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
            && matches!(
                self.quality.as_str(),
                "direct_verified"
                    | "direct_unverified"
                    | "official_delayed"
                    | "aggregated"
                    | "indicative"
                    | "estimated"
                    | "stale"
                    | "quarantined"
            )
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(super) struct ForecastIndex {
    schema_version: u32,
    pub(super) vintages: Vec<VintageRecord>,
    pub(super) outcomes: Vec<OutcomeRecord>,
}

impl Default for ForecastIndex {
    fn default() -> Self {
        Self {
            schema_version: INDEX_SCHEMA_VERSION,
            vintages: Vec::new(),
            outcomes: Vec::new(),
        }
    }
}

impl ForecastIndex {
    pub(super) fn validate(
        &self,
        limits: ForecastApplicationLimits,
    ) -> Result<(), ForecastApplicationError> {
        if self.schema_version != INDEX_SCHEMA_VERSION
            || self.vintages.len() > limits.maximum_vintages.get()
            || self.outcomes.len() > limits.maximum_outcomes.get()
            || serde_json::to_vec(self).map_or(true, |payload| {
                payload.len() > limits.maximum_index_bytes.get()
            })
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let mut vintage_ids = HashSet::new();
        let mut request_hashes = HashSet::new();
        let mut outcome_ids = HashSet::new();
        vintage_ids
            .try_reserve(self.vintages.len())
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        request_hashes
            .try_reserve(self.vintages.len())
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        outcome_ids
            .try_reserve(self.outcomes.len())
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        for vintage in &self.vintages {
            if !vintage.validate()
                || !vintage_ids.insert(vintage.vintage_id.as_str())
                || !request_hashes.insert(vintage.request_hash.as_str())
            {
                return Err(ForecastApplicationError::CorruptIndex);
            }
        }
        for outcome in &self.outcomes {
            let vintage = self
                .vintages
                .iter()
                .find(|vintage| vintage.vintage_id == outcome.vintage_id)
                .ok_or(ForecastApplicationError::CorruptIndex)?;
            if !outcome.validate(vintage) || !outcome_ids.insert(outcome.outcome_id.as_str()) {
                return Err(ForecastApplicationError::CorruptIndex);
            }
        }
        Ok(())
    }
}

pub(super) fn validate_digest(value: &str) -> Result<(), ForecastApplicationError> {
    if valid_digest(value) {
        Ok(())
    } else {
        Err(ForecastApplicationError::InvalidRecord)
    }
}

pub(super) fn digest_from_hex(value: &str) -> Result<Sha256Digest, ForecastApplicationError> {
    validate_digest(value)?;
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(ForecastApplicationError::InvalidRecord)?;
        let low = hex_nibble(pair[1]).ok_or(ForecastApplicationError::InvalidRecord)?;
        bytes[index] = (high << 4) | low;
    }
    Ok(Sha256Digest::new(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

pub(super) fn hex<const N: usize>(bytes: [u8; N]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(N * 2);
    for byte in bytes {
        value.push(char::from(ALPHABET[usize::from(byte >> 4)]));
        value.push(char::from(ALPHABET[usize::from(byte & 0x0f)]));
    }
    value
}
