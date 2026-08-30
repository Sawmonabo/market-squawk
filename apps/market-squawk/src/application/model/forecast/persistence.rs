//! Closed durable forecast-index records and restart validation.

use std::{
    cmp::Ordering,
    collections::HashSet,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
};

use market_squawk_data::{
    ComponentKind, ComponentScope, CorporateActionSensitivity, DatasetId, DatasetManifestRef,
    DatasetSchemaRef, DatasetSchemaRegistry, FeatureLabelComponentSpec, Sha256Digest, UniverseId,
};
use market_squawk_domain::{
    Currency, DataQuality, InstrumentId, ModelId, SchemaVersion, SourceId, Timestamp,
};
use market_squawk_modeling::{
    BundleId, CalibrationBand, CalibrationEvidence, CalibrationMethod, CalibrationWindow,
    ForecastCentralStatistic, ForecastCoverage, ForecastEstimatorProfile, ForecastHorizon,
    ForecastMeasurement, ForecastObservedPoint, ForecastOutcome, ForecastOutputBinding,
    ForecastPath, ForecastTargetMeaning, ForecastTrainingObjective, ForecastTransform,
    ForecastValue, ForecastVintage, ModelBundle, ModelMetadata, ModelOutputSemantics,
    RealizedCoverage, verify_forecast_vintage_identity,
};
use market_squawk_services::{ArtifactRead, ArtifactReference};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::application::domain_support::opaque_product_token;
use crate::application::model::{
    ForecastModelCalibrationState, ForecastModelEvidenceProjection, ForecastModelEvidenceState,
    forecast_model_evidence_projection_for_horizon,
};

use super::{
    FORECAST_PAYLOAD_SCHEMA_VERSION, FORECAST_SELECTION_POLICY_REVISION, ForecastAnalysisEvidence,
    ForecastApplicationError, ForecastApplicationLimits, ForecastPriceEvidence,
    ForecastPriceUnavailableReason, ForecastProductHorizon, ForecastProductIdentity,
    ForecastProductTarget, ForecastSelectionOrder, ForecastSelectionQualification,
    ForecastSelectionReceipt, ForecastSelectionReceiptBody, ForecastServingEvidence,
    INDEX_SCHEMA_VERSION, SelectedForecastPriceUnavailable, SelectedPriceForecast,
    SelectedPriceForecastPoint, SelectedPriceInterval, SelectedPriceIntervals,
};

const MAXIMUM_CALIBRATION_ASSUMPTION_BYTES: usize = 512;
const OUTPUT_BINDING_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct VintageRecord {
    pub(super) vintage_id: String,
    pub(super) request_hash: String,
    controlled_artifact: ControlledArtifactRecord,
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

    pub(super) fn product_token(&self) -> Result<Uuid, ForecastApplicationError> {
        product_token(b"market-squawk/product-forecast/v1\0", &self.vintage_id)
    }

    pub(super) fn matches_product_token(
        &self,
        token: Uuid,
    ) -> Result<bool, ForecastApplicationError> {
        self.product_token().map(|candidate| candidate == token)
    }

    pub(super) fn product_summary(&self) -> Result<Value, ForecastApplicationError> {
        Ok(json!({
            "forecastToken": self.product_token()?,
            "investment": self.payload.product_identity.product_value(),
            "target": self.payload.output_binding.product_value()?,
            "modelEvidence": self.payload.model_evidence.product_value()?,
            "observedThroughUnixNanos": self.payload.observed_through_unix_nanos.to_string(),
            "createdAtUnixNanos": self.payload.created_at_unix_nanos.to_string(),
            "expiresAtUnixNanos": self.payload.expires_at_unix_nanos.to_string(),
            "horizon": product_horizon(
                self.payload.horizon_points,
                self.payload.horizon_step_nanos,
            )?,
            "historicalObservationCount": self.payload.observed_history.len(),
            "limitations": self.payload.limitations,
        }))
    }

    pub(super) fn product_detail(
        &self,
        drift_monitoring: Value,
    ) -> Result<Value, ForecastApplicationError> {
        let estimates = self
            .payload
            .points
            .iter()
            .map(|point| point.product_value(&self.payload.output_binding))
            .collect::<Result<Vec<_>, _>>()?;
        let observed_history = self
            .payload
            .observed_history
            .iter()
            .map(|point| point.product_value(&self.payload.output_binding))
            .collect::<Result<Vec<_>, _>>()?;
        let calibration = self
            .payload
            .calibration
            .as_ref()
            .map(CalibrationRecord::product_value)
            .transpose()?;
        Ok(json!({
            "forecastToken": self.product_token()?,
            "investment": self.payload.product_identity.product_value(),
            "target": self.payload.output_binding.product_value()?,
            "modelEvidence": self.payload.model_evidence.product_value()?,
            "observedThroughUnixNanos": self.payload.observed_through_unix_nanos.to_string(),
            "availableAtUnixNanos": self.payload.available_at_unix_nanos.to_string(),
            "createdAtUnixNanos": self.payload.created_at_unix_nanos.to_string(),
            "expiresAtUnixNanos": self.payload.expires_at_unix_nanos.to_string(),
            "horizon": product_horizon(
                self.payload.horizon_points,
                self.payload.horizon_step_nanos,
            )?,
            "observedHistory": observed_history,
            "estimates": estimates,
            "calibration": calibration,
            "limitations": self.payload.limitations,
            "unavailableBehavior": "no_action",
            "outcomeMonitoring": drift_monitoring,
            "analysisOnly": true,
        }))
    }

    pub(super) fn decimal_scale(&self) -> Option<u8> {
        self.payload.points.first().map(|point| point.decimal_scale)
    }

    pub(super) fn product_outcome(
        &self,
        outcome: &OutcomeRecord,
    ) -> Result<Value, ForecastApplicationError> {
        outcome.product_value(&self.payload.output_binding)
    }

    pub(super) fn product_amount(
        &self,
        mantissa: &str,
        scale: u8,
    ) -> Result<Value, ForecastApplicationError> {
        self.payload.output_binding.product_amount(mantissa, scale)
    }

    pub(in crate::application::model) fn artifact_reference(
        &self,
    ) -> Result<ArtifactReference, ForecastApplicationError> {
        ArtifactReference::try_new(
            self.controlled_artifact.artifact_id.clone(),
            self.controlled_artifact.sha256.clone(),
            self.controlled_artifact.byte_count,
            self.controlled_artifact.media_type.clone(),
        )
        .map_err(ForecastApplicationError::from)
    }

    pub(in crate::application::model) fn model_coordinate(&self) -> (&str, &str, u64) {
        (
            &self.payload.model_id,
            &self.payload.bundle_id,
            self.payload.bundle_version,
        )
    }

    pub(super) fn typed_model_coordinate(
        &self,
    ) -> Result<(ModelId, BundleId, NonZeroU64), ForecastApplicationError> {
        Ok((
            ModelId::from_str(&self.payload.model_id)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            BundleId::try_new(&self.payload.bundle_id)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            NonZeroU64::new(self.payload.bundle_version)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        ))
    }

    fn validate(&self) -> bool {
        valid_digest(&self.vintage_id)
            && valid_digest(&self.request_hash)
            && self.controlled_artifact.validate()
            && self.controlled_artifact.matches_payload(&self.payload)
            && self.payload.validate()
    }

    fn exact_horizon_price_terminal_target(
        &self,
        requested_horizon_nanos: NonZeroU64,
        as_of: Timestamp,
    ) -> Result<Option<i64>, ForecastApplicationError> {
        let payload = &self.payload;
        let [point] = payload.points.as_slice() else {
            return Ok(None);
        };
        let expected_target = payload
            .observed_through_unix_nanos
            .checked_add(
                i64::try_from(requested_horizon_nanos.get())
                    .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            )
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let exact_binding = matches!(
            payload.output_binding.decoded_measurement(),
            Some(ForecastMeasurement::Price { .. })
        ) && payload.output_binding.decoded_central_statistic()
            == Some(ForecastCentralStatistic::ModelEstimatedConditionalMean)
            && payload.output_binding.target.decoded()
                == Some(ForecastTargetMeaning::FixedHorizonTerminal {
                    horizon_nanos: requested_horizon_nanos,
                });
        Ok((exact_binding
            && payload.horizon_points == 1
            && payload.horizon_step_nanos == requested_horizon_nanos.get()
            && payload.calibration.is_some()
            && point.intervals.is_some()
            && point.target_at_unix_nanos == expected_target
            && point.target_at_unix_nanos > as_of.unix_nanos()
            && payload.expires_at_unix_nanos <= point.target_at_unix_nanos)
            .then_some(point.target_at_unix_nanos))
    }

    pub(super) fn verify_artifact_read(
        &self,
        artifact: &ArtifactRead,
    ) -> Result<(), ForecastApplicationError> {
        let reference = self.artifact_reference()?;
        let payload = self.canonical_payload_bytes()?;
        if artifact.reference() != &reference || artifact.content() != payload.as_slice() {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(())
    }

    fn canonical_payload_bytes(&self) -> Result<Vec<u8>, ForecastApplicationError> {
        serde_json::to_vec(&self.payload).map_err(|_error| ForecastApplicationError::CorruptIndex)
    }

    pub(super) fn revalidated_price_evidence(
        &self,
        bundle: &ModelBundle,
    ) -> Result<ForecastPriceEvidence, ForecastApplicationError> {
        let metadata = bundle.metadata();
        if !self.validate() || !self.payload.matches_model_metadata(metadata) {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let selected_horizon = ForecastHorizon::try_new(
            NonZeroU16::new(self.payload.horizon_points)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
            NonZeroU64::new(self.payload.horizon_step_nanos)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let authoritative_model_evidence =
            forecast_model_evidence_projection_for_horizon(bundle, selected_horizon)
                .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        if self.payload.model_evidence.typed()? != authoritative_model_evidence {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let instrument_id = InstrumentId::from_str(&self.payload.instrument_id)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let vintage_id = digest_from_hex(&self.vintage_id)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let stored_binding = &self.payload.output_binding;
        let admitted_binding = metadata.output_binding();
        if !stored_binding.matches(admitted_binding) {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let output_binding_identity = digest_from_hex(&stored_binding.identity_sha256)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let analysis_evidence = self.payload.analysis_evidence.typed()?;
        let serving_evidence = self.payload.serving_evidence.typed()?;
        let calibration = self.payload.revalidated_calibration(metadata)?;
        self.payload.verify_vintage_identity(
            vintage_id,
            metadata,
            instrument_id,
            calibration.as_ref(),
            digest_from_hex(&self.controlled_artifact.sha256)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
        )?;
        let currency = match admitted_binding.measurement() {
            ForecastMeasurement::Price { currency } => currency,
            ForecastMeasurement::Return => {
                return Ok(ForecastPriceEvidence::Unavailable(
                    SelectedForecastPriceUnavailable {
                        vintage_id,
                        instrument_id,
                        output_binding_identity,
                        analysis_evidence,
                        serving_evidence,
                        reason: ForecastPriceUnavailableReason::ReturnMeasurement,
                    },
                ));
            }
            ForecastMeasurement::Probability => {
                return Ok(ForecastPriceEvidence::Unavailable(
                    SelectedForecastPriceUnavailable {
                        vintage_id,
                        instrument_id,
                        output_binding_identity,
                        analysis_evidence,
                        serving_evidence,
                        reason: ForecastPriceUnavailableReason::ProbabilityMeasurement,
                    },
                ));
            }
            ForecastMeasurement::OtherRegression => {
                return Ok(ForecastPriceEvidence::Unavailable(
                    SelectedForecastPriceUnavailable {
                        vintage_id,
                        instrument_id,
                        output_binding_identity,
                        analysis_evidence,
                        serving_evidence,
                        reason: ForecastPriceUnavailableReason::OtherRegressionMeasurement,
                    },
                ));
            }
        };
        let terminal_horizon_nanos = match admitted_binding.target() {
            ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos } => horizon_nanos,
            ForecastTargetMeaning::Unsupported => {
                return Ok(ForecastPriceEvidence::Unavailable(
                    SelectedForecastPriceUnavailable {
                        vintage_id,
                        instrument_id,
                        output_binding_identity,
                        analysis_evidence,
                        serving_evidence,
                        reason: ForecastPriceUnavailableReason::TerminalHorizonUnavailable,
                    },
                ));
            }
        };
        if admitted_binding.central_statistic()
            != ForecastCentralStatistic::ModelEstimatedConditionalMean
        {
            return Ok(ForecastPriceEvidence::Unavailable(
                SelectedForecastPriceUnavailable {
                    vintage_id,
                    instrument_id,
                    output_binding_identity,
                    analysis_evidence,
                    serving_evidence,
                    reason: ForecastPriceUnavailableReason::CentralStatisticUnavailable,
                },
            ));
        }
        if admitted_binding.expected_terminal_price_horizon_nanos() != Some(terminal_horizon_nanos)
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let points = self.payload.typed_price_points()?;
        Ok(ForecastPriceEvidence::Available(Box::new(
            SelectedPriceForecast {
                vintage_id,
                instrument_id,
                currency,
                central_statistic: ForecastCentralStatistic::ModelEstimatedConditionalMean,
                terminal_horizon_nanos,
                observed_through: Timestamp::from_unix_nanos(
                    self.payload.observed_through_unix_nanos,
                ),
                available_at: Timestamp::from_unix_nanos(self.payload.available_at_unix_nanos),
                created_at: Timestamp::from_unix_nanos(self.payload.created_at_unix_nanos),
                expires_at: Timestamp::from_unix_nanos(self.payload.expires_at_unix_nanos),
                output_binding_identity,
                analysis_evidence,
                serving_evidence,
                model_metadata: metadata.clone(),
                forecast_artifact: self.artifact_reference()?,
                points,
                calibration,
            },
        )))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    fn matches_payload(&self, payload: &ForecastPayloadRecord) -> bool {
        let Ok(encoded) = serde_json::to_vec(payload) else {
            return false;
        };
        let digest: [u8; 32] = Sha256::digest(&encoded).into();
        self.byte_count == encoded.len() && self.sha256 == hex(digest)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastOutputBindingRecord {
    schema_version: u32,
    output_semantics: String,
    measurement: ForecastMeasurementRecord,
    central_statistic: String,
    target: ForecastTargetRecord,
    target_transform: String,
    output_transform: String,
    objective: String,
    estimator: ForecastEstimatorRecord,
    identity_sha256: String,
    label: ForecastOutputLabelRecord,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastProductIdentityRecord {
    display_name: String,
    canonical_symbol: Option<String>,
    description: String,
    quote_currency: String,
    knowledge_at_unix_nanos: i64,
    effective_at_unix_nanos: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastModelEvidenceRecord {
    model_token: String,
    selected_horizon_points: u16,
    selected_horizon_step_nanos: u64,
    overall: String,
    pit_inputs: String,
    out_of_sample: String,
    horizon_alignment: String,
    calibration: String,
    interpretation: String,
}

impl ForecastModelEvidenceRecord {
    fn from_projection(
        value: &ForecastModelEvidenceProjection,
    ) -> Result<Self, ForecastApplicationError> {
        let selected_horizon = value
            .selected_horizon()
            .ok_or(ForecastApplicationError::InvalidRecord)?;
        Ok(Self {
            model_token: value.model_token().to_string(),
            selected_horizon_points: selected_horizon.points().get(),
            selected_horizon_step_nanos: selected_horizon.step_nanos().get(),
            overall: value.overall().as_str().to_owned(),
            pit_inputs: value.pit_inputs().as_str().to_owned(),
            out_of_sample: value.out_of_sample().as_str().to_owned(),
            horizon_alignment: value.horizon_alignment().as_str().to_owned(),
            calibration: value.calibration().as_str().to_owned(),
            interpretation: value.interpretation().to_owned(),
        })
    }

    fn typed(&self) -> Result<ForecastModelEvidenceProjection, ForecastApplicationError> {
        let selected_horizon = ForecastHorizon::try_new(
            NonZeroU16::new(self.selected_horizon_points)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
            NonZeroU64::new(self.selected_horizon_step_nanos)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        ForecastModelEvidenceProjection::try_from_product_fields_for_horizon(
            Uuid::parse_str(&self.model_token)
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            selected_horizon,
            &self.overall,
            &self.pit_inputs,
            &self.out_of_sample,
            &self.horizon_alignment,
            &self.calibration,
            &self.interpretation,
        )
        .ok_or(ForecastApplicationError::CorruptIndex)
    }

    fn validate(&self) -> bool {
        self.typed().is_ok()
    }

    fn product_value(&self) -> Result<Value, ForecastApplicationError> {
        self.typed().map(|value| value.product_value())
    }

    fn matches_product_model(
        &self,
        model_id: &str,
        bundle_id: &str,
        bundle_version: u64,
        horizon_points: u16,
        horizon_step_nanos: u64,
        calibrated: bool,
    ) -> bool {
        let Ok(model_id) = ModelId::from_str(model_id) else {
            return false;
        };
        let Ok(bundle_id) = BundleId::try_new(bundle_id) else {
            return false;
        };
        let Some(bundle_version) = NonZeroU64::new(bundle_version) else {
            return false;
        };
        let Ok(value) = self.typed() else {
            return false;
        };
        let expected_token = opaque_product_token(
            b"market-squawk/product-model/v1\0",
            &[
                model_id.as_uuid().as_bytes(),
                bundle_id.as_str().as_bytes(),
                &bundle_version.get().to_be_bytes(),
            ],
        );
        value.model_token() == expected_token
            && value.selected_horizon().is_some_and(|selected| {
                selected.points().get() == horizon_points
                    && selected.step_nanos().get() == horizon_step_nanos
            })
            && value.overall() != ForecastModelEvidenceState::Unavailable
            && matches!(
                (value.calibration(), calibrated),
                (ForecastModelCalibrationState::Calibrated, true)
                    | (
                        ForecastModelCalibrationState::Limited
                            | ForecastModelCalibrationState::Unavailable,
                        false
                    )
            )
    }
}

impl ForecastProductIdentityRecord {
    fn from_identity(identity: &ForecastProductIdentity) -> Self {
        Self {
            display_name: identity.display_name().to_owned(),
            canonical_symbol: identity.canonical_symbol().map(str::to_owned),
            description: identity.description().to_owned(),
            quote_currency: identity.quote_currency().as_str().to_owned(),
            knowledge_at_unix_nanos: identity.knowledge_at().unix_nanos(),
            effective_at_unix_nanos: identity.effective_at().unix_nanos(),
        }
    }

    fn typed(&self) -> Result<ForecastProductIdentity, ForecastApplicationError> {
        let currency = Currency::try_from(self.quote_currency.as_str())
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        if currency.as_str() != self.quote_currency {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        ForecastProductIdentity::try_new(
            self.display_name.clone(),
            self.canonical_symbol.clone(),
            self.description.clone(),
            currency,
            Timestamp::from_unix_nanos(self.knowledge_at_unix_nanos),
            Timestamp::from_unix_nanos(self.effective_at_unix_nanos),
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)
    }

    fn validate(&self) -> bool {
        self.typed().is_ok()
    }

    fn product_value(&self) -> Value {
        json!({
            "name": self.display_name,
            "symbol": self.canonical_symbol,
            "description": self.description,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ForecastTargetRecord {
    FixedHorizonTerminal { horizon_nanos: u64 },
    Unsupported,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ForecastMeasurementRecord {
    Price { currency: String },
    Return,
    Probability,
    OtherRegression,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ForecastEstimatorRecord {
    SealedDirectLeastSquaresV1,
    SealedDirectRidgeV1 { ridge_alpha: f64 },
    SealedBinaryLogisticV1,
}

impl ForecastOutputBindingRecord {
    fn from_binding(value: &ForecastOutputBinding) -> Self {
        Self {
            schema_version: OUTPUT_BINDING_SCHEMA_VERSION,
            output_semantics: output_semantics_name(value.output_semantics()).to_owned(),
            measurement: ForecastMeasurementRecord::from_measurement(value.measurement()),
            central_statistic: central_statistic_name(value.central_statistic()).to_owned(),
            target: ForecastTargetRecord::from_target(value.target()),
            target_transform: forecast_transform_name(value.target_transform()).to_owned(),
            output_transform: forecast_transform_name(value.output_transform()).to_owned(),
            objective: forecast_objective_name(value.objective()).to_owned(),
            estimator: ForecastEstimatorRecord::from_estimator(value.estimator()),
            identity_sha256: hex(value.identity().bytes()),
            label: ForecastOutputLabelRecord::from_label(value.label()),
        }
    }

    fn validate(&self) -> bool {
        self.schema_version == OUTPUT_BINDING_SCHEMA_VERSION
            && self.decoded_semantics().is_some()
            && self.decoded_measurement().is_some()
            && self.decoded_central_statistic().is_some()
            && matches!(
                self.target.decoded(),
                Some(ForecastTargetMeaning::FixedHorizonTerminal { .. })
            )
            && self.decoded_target_transform().is_some()
            && self.decoded_output_transform().is_some()
            && self.decoded_objective().is_some()
            && self.estimator.decoded().is_some()
            && self.label.decoded().is_some()
            && valid_digest(&self.identity_sha256)
            && matches!(
                (self.decoded_semantics(), self.decoded_measurement()),
                (
                    Some(ModelOutputSemantics::Regression),
                    Some(
                        ForecastMeasurement::Price { .. }
                            | ForecastMeasurement::Return
                            | ForecastMeasurement::OtherRegression
                    )
                ) | (
                    Some(ModelOutputSemantics::BinaryProbability),
                    Some(ForecastMeasurement::Probability)
                )
            )
            && self.contract_is_coherent()
    }

    fn matches(&self, value: &ForecastOutputBinding) -> bool {
        self.validate()
            && self.decoded_semantics() == Some(value.output_semantics())
            && self.decoded_measurement() == Some(value.measurement())
            && self.decoded_central_statistic() == Some(value.central_statistic())
            && self.target.decoded() == Some(value.target())
            && self.decoded_target_transform() == Some(value.target_transform())
            && self.decoded_output_transform() == Some(value.output_transform())
            && self.decoded_objective() == Some(value.objective())
            && self.estimator.decoded() == Some(value.estimator())
            && self.label.matches(value.label())
            && self.identity_sha256 == hex(value.identity().bytes())
    }

    fn decoded_semantics(&self) -> Option<ModelOutputSemantics> {
        match self.output_semantics.as_str() {
            "regression" => Some(ModelOutputSemantics::Regression),
            "binary_probability" => Some(ModelOutputSemantics::BinaryProbability),
            _ => None,
        }
    }

    fn decoded_measurement(&self) -> Option<ForecastMeasurement> {
        self.measurement.decoded()
    }

    fn decoded_central_statistic(&self) -> Option<ForecastCentralStatistic> {
        match self.central_statistic.as_str() {
            "model_estimated_conditional_mean" => {
                Some(ForecastCentralStatistic::ModelEstimatedConditionalMean)
            }
            "unavailable" => Some(ForecastCentralStatistic::Unavailable),
            _ => None,
        }
    }

    fn decoded_target_transform(&self) -> Option<ForecastTransform> {
        decode_forecast_transform(&self.target_transform)
    }

    fn decoded_output_transform(&self) -> Option<ForecastTransform> {
        decode_forecast_transform(&self.output_transform)
    }

    fn decoded_objective(&self) -> Option<ForecastTrainingObjective> {
        match self.objective.as_str() {
            "squared_error" => Some(ForecastTrainingObjective::SquaredError),
            "binary_cross_entropy" => Some(ForecastTrainingObjective::BinaryCrossEntropy),
            _ => None,
        }
    }

    fn contract_is_coherent(&self) -> bool {
        let Some(semantics) = self.decoded_semantics() else {
            return false;
        };
        let Some(measurement) = self.decoded_measurement() else {
            return false;
        };
        let Some(statistic) = self.decoded_central_statistic() else {
            return false;
        };
        let Some(target) = self.target.decoded() else {
            return false;
        };
        let Some(target_transform) = self.decoded_target_transform() else {
            return false;
        };
        let Some(output_transform) = self.decoded_output_transform() else {
            return false;
        };
        let Some(objective) = self.decoded_objective() else {
            return false;
        };
        let Some(estimator) = self.estimator.decoded() else {
            return false;
        };
        let regression_estimator = matches!(
            estimator,
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1
                | ForecastEstimatorProfile::SealedDirectRidgeV1 { .. }
        );
        let producer_contract = match semantics {
            ModelOutputSemantics::Regression => {
                regression_estimator
                    && output_transform == ForecastTransform::Identity
                    && objective == ForecastTrainingObjective::SquaredError
            }
            ModelOutputSemantics::BinaryProbability => {
                estimator == ForecastEstimatorProfile::SealedBinaryLogisticV1
                    && output_transform == ForecastTransform::Logistic
                    && objective == ForecastTrainingObjective::BinaryCrossEntropy
            }
        };
        let expected_value_contract = matches!(measurement, ForecastMeasurement::Price { .. })
            && matches!(target, ForecastTargetMeaning::FixedHorizonTerminal { .. })
            && semantics == ModelOutputSemantics::Regression
            && target_transform == ForecastTransform::Identity
            && output_transform == ForecastTransform::Identity
            && objective == ForecastTrainingObjective::SquaredError
            && regression_estimator;
        producer_contract
            && target_transform == ForecastTransform::Identity
            && (statistic != ForecastCentralStatistic::ModelEstimatedConditionalMean
                || expected_value_contract)
    }

    fn product_value(&self) -> Result<Value, ForecastApplicationError> {
        if !self.validate() {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let measurement = self
            .decoded_measurement()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let target_meaning = self
            .target
            .decoded()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let label = self
            .label
            .decoded()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let target =
            ForecastProductTarget::try_from_admitted_parts(measurement, target_meaning, &label)?;
        Ok(json!({
            "label": target.label(),
            "meaning": target.meaning(),
            "valueKind": target.value_kind(),
            "unitLabel": target.unit_label(),
            "currencyCode": target.currency_code(),
        }))
    }

    fn product_amount(&self, mantissa: &str, scale: u8) -> Result<Value, ForecastApplicationError> {
        let exact = decimal_text(mantissa, scale)?;
        let formatted = match self
            .decoded_measurement()
            .ok_or(ForecastApplicationError::CorruptIndex)?
        {
            ForecastMeasurement::Price { currency } => format!("{exact} {}", currency.as_str()),
            ForecastMeasurement::Return => format!("{exact} return"),
            ForecastMeasurement::Probability => format!("{exact} probability"),
            ForecastMeasurement::OtherRegression => exact.clone(),
        };
        Ok(json!({
            "exact": exact,
            "formatted": formatted,
        }))
    }
}

impl ForecastMeasurementRecord {
    fn from_measurement(value: ForecastMeasurement) -> Self {
        match value {
            ForecastMeasurement::Price { currency } => Self::Price {
                currency: currency.as_str().to_owned(),
            },
            ForecastMeasurement::Return => Self::Return,
            ForecastMeasurement::Probability => Self::Probability,
            ForecastMeasurement::OtherRegression => Self::OtherRegression,
        }
    }

    fn decoded(&self) -> Option<ForecastMeasurement> {
        match self {
            Self::Price { currency } => Currency::try_from(currency.as_str())
                .ok()
                .filter(|parsed| parsed.as_str() == currency)
                .map(|currency| ForecastMeasurement::Price { currency }),
            Self::Return => Some(ForecastMeasurement::Return),
            Self::Probability => Some(ForecastMeasurement::Probability),
            Self::OtherRegression => Some(ForecastMeasurement::OtherRegression),
        }
    }
}

impl ForecastTargetRecord {
    const fn from_target(value: ForecastTargetMeaning) -> Self {
        match value {
            ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos } => {
                Self::FixedHorizonTerminal {
                    horizon_nanos: horizon_nanos.get(),
                }
            }
            ForecastTargetMeaning::Unsupported => Self::Unsupported,
        }
    }

    fn decoded(self) -> Option<ForecastTargetMeaning> {
        match self {
            Self::FixedHorizonTerminal { horizon_nanos } => NonZeroU64::new(horizon_nanos)
                .map(|horizon_nanos| ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos }),
            Self::Unsupported => Some(ForecastTargetMeaning::Unsupported),
        }
    }
}

impl ForecastEstimatorRecord {
    const fn from_estimator(value: ForecastEstimatorProfile) -> Self {
        match value {
            ForecastEstimatorProfile::SealedDirectLeastSquaresV1 => {
                Self::SealedDirectLeastSquaresV1
            }
            ForecastEstimatorProfile::SealedDirectRidgeV1 { ridge_alpha_bits } => {
                Self::SealedDirectRidgeV1 {
                    ridge_alpha: f64::from_bits(ridge_alpha_bits),
                }
            }
            ForecastEstimatorProfile::SealedBinaryLogisticV1 => Self::SealedBinaryLogisticV1,
        }
    }

    fn decoded(self) -> Option<ForecastEstimatorProfile> {
        match self {
            Self::SealedDirectLeastSquaresV1 => {
                Some(ForecastEstimatorProfile::SealedDirectLeastSquaresV1)
            }
            Self::SealedDirectRidgeV1 { ridge_alpha } => (ridge_alpha.is_finite()
                && ridge_alpha >= 0.0)
                .then_some(ForecastEstimatorProfile::SealedDirectRidgeV1 {
                    ridge_alpha_bits: ridge_alpha.to_bits(),
                }),
            Self::SealedBinaryLogisticV1 => Some(ForecastEstimatorProfile::SealedBinaryLogisticV1),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastOutputLabelRecord {
    kind: String,
    scope: String,
    corporate_actions: String,
    name: String,
    version: u32,
}

impl ForecastOutputLabelRecord {
    fn from_label(value: &FeatureLabelComponentSpec) -> Self {
        Self {
            kind: component_kind_name(value.kind()).to_owned(),
            scope: component_scope_name(value.scope()).to_owned(),
            corporate_actions: corporate_action_sensitivity_name(value.corporate_actions())
                .to_owned(),
            name: value.name().to_owned(),
            version: value.version().get(),
        }
    }

    fn decoded(&self) -> Option<FeatureLabelComponentSpec> {
        let kind = match self.kind.as_str() {
            "label" => ComponentKind::Label,
            _ => return None,
        };
        let scope = match self.scope.as_str() {
            "instrument" => ComponentScope::Instrument,
            _ => return None,
        };
        let corporate_actions = match self.corporate_actions.as_str() {
            "not_applicable" => CorporateActionSensitivity::NotApplicable,
            "requires_adjustment" => CorporateActionSensitivity::RequiresAdjustment,
            _ => return None,
        };
        FeatureLabelComponentSpec::try_new(
            kind,
            scope,
            corporate_actions,
            &self.name,
            NonZeroU32::new(self.version)?,
        )
        .ok()
    }

    fn matches(&self, value: &FeatureLabelComponentSpec) -> bool {
        self.decoded().as_ref() == Some(value)
            && self.kind == component_kind_name(value.kind())
            && self.scope == component_scope_name(value.scope())
            && self.corporate_actions
                == corporate_action_sensitivity_name(value.corporate_actions())
    }
}

const fn output_semantics_name(value: ModelOutputSemantics) -> &'static str {
    match value {
        ModelOutputSemantics::Regression => "regression",
        ModelOutputSemantics::BinaryProbability => "binary_probability",
    }
}

const fn central_statistic_name(value: ForecastCentralStatistic) -> &'static str {
    match value {
        ForecastCentralStatistic::ModelEstimatedConditionalMean => {
            "model_estimated_conditional_mean"
        }
        ForecastCentralStatistic::Unavailable => "unavailable",
    }
}

const fn forecast_transform_name(value: ForecastTransform) -> &'static str {
    match value {
        ForecastTransform::Identity => "identity",
        ForecastTransform::Logistic => "logistic",
    }
}

fn decode_forecast_transform(value: &str) -> Option<ForecastTransform> {
    match value {
        "identity" => Some(ForecastTransform::Identity),
        "logistic" => Some(ForecastTransform::Logistic),
        _ => None,
    }
}

const fn forecast_objective_name(value: ForecastTrainingObjective) -> &'static str {
    match value {
        ForecastTrainingObjective::SquaredError => "squared_error",
        ForecastTrainingObjective::BinaryCrossEntropy => "binary_cross_entropy",
    }
}

const fn component_kind_name(value: ComponentKind) -> &'static str {
    match value {
        ComponentKind::Feature => "feature",
        ComponentKind::Label => "label",
    }
}

const fn component_scope_name(value: ComponentScope) -> &'static str {
    match value {
        ComponentScope::Instrument => "instrument",
        ComponentScope::Account => "account",
        ComponentScope::Global => "global",
    }
}

const fn corporate_action_sensitivity_name(value: CorporateActionSensitivity) -> &'static str {
    match value {
        CorporateActionSensitivity::NotApplicable => "not_applicable",
        CorporateActionSensitivity::RequiresAdjustment => "requires_adjustment",
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastAnalysisEvidenceRecord {
    manifest: ForecastAnalysisManifestRecord,
    production_identity_sha256: String,
    production_receipt_sha256: String,
    pairing_sha256: String,
}

impl ForecastAnalysisEvidenceRecord {
    fn from_evidence(evidence: &ForecastAnalysisEvidence) -> Self {
        let manifest = evidence.manifest();
        Self {
            manifest: ForecastAnalysisManifestRecord {
                dataset: manifest.dataset_id().as_str().to_owned(),
                manifest_version: manifest.manifest_version(),
                schema: ForecastAnalysisSchemaRecord {
                    name: manifest.schema().name().to_owned(),
                    version: manifest.schema_version().get(),
                    fingerprint: hex(manifest.schema().fingerprint()),
                },
                content_hash: hex(manifest.content_hash().bytes()),
            },
            production_identity_sha256: hex(evidence.production_identity_sha256().bytes()),
            production_receipt_sha256: hex(evidence.production_receipt_sha256().bytes()),
            pairing_sha256: hex(evidence.pairing_sha256().bytes()),
        }
    }

    fn typed(&self) -> Result<ForecastAnalysisEvidence, ForecastApplicationError> {
        let fingerprint = digest_from_hex(&self.manifest.schema.fingerprint)?;
        let content_hash = digest_from_hex(&self.manifest.content_hash)?;
        if fingerprint.bytes() == [0; 32] || content_hash.bytes() == [0; 32] {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let schema = DatasetSchemaRef::try_new(
            &self.manifest.schema.name,
            SchemaVersion::new(self.manifest.schema.version)
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            fingerprint.bytes(),
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.manifest.dataset.as_str())
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            self.manifest.manifest_version,
            schema,
            content_hash,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        ForecastAnalysisEvidence::try_new(
            manifest,
            digest_from_hex(&self.production_identity_sha256)?,
            digest_from_hex(&self.production_receipt_sha256)?,
            digest_from_hex(&self.pairing_sha256)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)
    }

    fn validate(&self) -> bool {
        self.typed().is_ok()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastServingEvidenceRecord {
    manifest: ForecastAnalysisManifestRecord,
    source_id: String,
    object_graph_sha256: String,
    selection_sha256: String,
    result_sha256: String,
    knowledge_cutoff_unix_nanos: i64,
    prior_observed_at_unix_nanos: i64,
    observed_through_unix_nanos: i64,
    feature_sha256: String,
}

impl ForecastServingEvidenceRecord {
    fn from_evidence(evidence: &ForecastServingEvidence) -> Self {
        let manifest = evidence.manifest();
        Self {
            manifest: ForecastAnalysisManifestRecord {
                dataset: manifest.dataset_id().as_str().to_owned(),
                manifest_version: manifest.manifest_version(),
                schema: ForecastAnalysisSchemaRecord {
                    name: manifest.schema().name().to_owned(),
                    version: manifest.schema_version().get(),
                    fingerprint: hex(manifest.schema().fingerprint()),
                },
                content_hash: hex(manifest.content_hash().bytes()),
            },
            source_id: evidence.source_id().as_str().to_owned(),
            object_graph_sha256: hex(evidence.object_graph_sha256().bytes()),
            selection_sha256: hex(evidence.selection_sha256().bytes()),
            result_sha256: hex(evidence.result_sha256().bytes()),
            knowledge_cutoff_unix_nanos: evidence.knowledge_cutoff().unix_nanos(),
            prior_observed_at_unix_nanos: evidence.prior_observed_at().unix_nanos(),
            observed_through_unix_nanos: evidence.observed_through().unix_nanos(),
            feature_sha256: hex(evidence.feature_sha256().bytes()),
        }
    }

    fn typed(&self) -> Result<ForecastServingEvidence, ForecastApplicationError> {
        let fingerprint = digest_from_hex(&self.manifest.schema.fingerprint)?;
        let content_hash = digest_from_hex(&self.manifest.content_hash)?;
        if fingerprint.bytes() == [0; 32] || content_hash.bytes() == [0; 32] {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let schema = DatasetSchemaRef::try_new(
            &self.manifest.schema.name,
            SchemaVersion::new(self.manifest.schema.version)
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            fingerprint.bytes(),
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        let manifest = DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.manifest.dataset.as_str())
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            self.manifest.manifest_version,
            schema,
            content_hash,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        ForecastServingEvidence::try_new(
            manifest,
            SourceId::try_from(self.source_id.as_str())
                .map_err(|_| ForecastApplicationError::CorruptIndex)?,
            digest_from_hex(&self.object_graph_sha256)?,
            digest_from_hex(&self.selection_sha256)?,
            digest_from_hex(&self.result_sha256)?,
            Timestamp::from_unix_nanos(self.knowledge_cutoff_unix_nanos),
            Timestamp::from_unix_nanos(self.prior_observed_at_unix_nanos),
            Timestamp::from_unix_nanos(self.observed_through_unix_nanos),
            digest_from_hex(&self.feature_sha256)?,
        )
        .map_err(|_| ForecastApplicationError::CorruptIndex)
    }

    fn validate(&self) -> bool {
        self.typed().is_ok()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastAnalysisManifestRecord {
    dataset: String,
    manifest_version: u64,
    schema: ForecastAnalysisSchemaRecord,
    content_hash: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastAnalysisSchemaRecord {
    name: String,
    version: u16,
    fingerprint: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ForecastPayloadRecord {
    payload_schema_version: u32,
    instrument_id: String,
    product_identity: ForecastProductIdentityRecord,
    model_evidence: ForecastModelEvidenceRecord,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    metadata_hash: String,
    artifact_hash: String,
    training_run_hash: String,
    output_binding: ForecastOutputBindingRecord,
    analysis_evidence: ForecastAnalysisEvidenceRecord,
    serving_evidence: ForecastServingEvidenceRecord,
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
    observed_history: Vec<ObservedPointRecord>,
    quality: String,
    points: Vec<PointRecord>,
    calibration: Option<CalibrationRecord>,
    limitations: Vec<String>,
    unavailable_reason: String,
}

impl ForecastPayloadRecord {
    pub(super) fn from_path(
        path: &ForecastPath,
        product_identity: &ForecastProductIdentity,
        model_evidence: &ForecastModelEvidenceProjection,
        analysis_evidence: &ForecastAnalysisEvidence,
        serving_evidence: &ForecastServingEvidence,
        created_at: market_squawk_domain::Timestamp,
        expires_at: market_squawk_domain::Timestamp,
    ) -> Result<Self, ForecastApplicationError> {
        if created_at < path.available_at()
            || created_at < serving_evidence.knowledge_cutoff()
            || path.observed_cutoff() != serving_evidence.observed_through()
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
        let record = Self {
            payload_schema_version: FORECAST_PAYLOAD_SCHEMA_VERSION,
            instrument_id: path.instrument_id().to_string(),
            product_identity: ForecastProductIdentityRecord::from_identity(product_identity),
            model_evidence: ForecastModelEvidenceRecord::from_projection(model_evidence)?,
            model_id: path.model_id().to_string(),
            bundle_id: path.bundle_id().as_str().to_owned(),
            bundle_version: path.bundle_version().get(),
            metadata_hash: hex(path.metadata_hash().bytes()),
            artifact_hash: hex(path.artifact_hash().bytes()),
            training_run_hash: hex(path.training_run_hash().bytes()),
            output_binding: ForecastOutputBindingRecord::from_binding(path.output_binding()),
            analysis_evidence: ForecastAnalysisEvidenceRecord::from_evidence(analysis_evidence),
            serving_evidence: ForecastServingEvidenceRecord::from_evidence(serving_evidence),
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
            observed_history: path
                .observed_history()
                .iter()
                .copied()
                .map(ObservedPointRecord::from_point)
                .collect(),
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
        };
        if !record.validate() {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        Ok(record)
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
        if self.payload_schema_version != FORECAST_PAYLOAD_SCHEMA_VERSION
            || !self.output_binding.validate()
            || !self.product_identity.validate()
            || !self.model_evidence.validate()
            || !self.model_evidence.matches_product_model(
                &self.model_id,
                &self.bundle_id,
                self.bundle_version,
                self.horizon_points,
                self.horizon_step_nanos,
                self.calibration.is_some(),
            )
            || !self.analysis_evidence.validate()
            || !self.serving_evidence.validate()
            || InstrumentId::from_str(&self.instrument_id).is_err()
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
            || self.available_at_unix_nanos < self.observed_through_unix_nanos
            || self.serving_evidence.observed_through_unix_nanos != self.observed_through_unix_nanos
            || self.product_identity.knowledge_at_unix_nanos
                != self.serving_evidence.knowledge_cutoff_unix_nanos
            || self.product_identity.effective_at_unix_nanos != self.observed_through_unix_nanos
            || self.available_at_unix_nanos > self.serving_evidence.knowledge_cutoff_unix_nanos
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
            || self
                .points
                .iter()
                .any(|point| point.decimal_scale != first.decimal_scale)
            || self.observed_history.len() > market_squawk_modeling::MAX_FORECAST_OBSERVED_POINTS
            || self.quality != "modeled"
            || matches!(
                self.output_binding.decoded_measurement(),
                Some(ForecastMeasurement::Price { currency })
                    if currency.as_str() != self.product_identity.quote_currency
            )
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
            && (self.observed_history.is_empty()
                || (self
                    .observed_history
                    .windows(2)
                    .all(|pair| pair[0].observed_at_unix_nanos < pair[1].observed_at_unix_nanos)
                    && self.observed_history.last().is_some_and(|point| {
                        point.observed_at_unix_nanos == self.observed_through_unix_nanos
                    })
                    && self.observed_history.iter().all(|point| {
                        point.validate(
                            self.observed_through_unix_nanos,
                            self.available_at_unix_nanos,
                            self.points.first().map(|value| value.decimal_scale),
                        )
                    })))
    }

    fn matches_model_metadata(&self, metadata: &ModelMetadata) -> bool {
        self.model_id == metadata.model_id().to_string()
            && self.bundle_id == metadata.bundle_id().as_str()
            && self.bundle_version == metadata.bundle_version().get()
            && self.metadata_hash == hex(metadata.metadata_hash().bytes())
            && self.artifact_hash == hex(metadata.artifact_hash().bytes())
            && self.training_run_hash == hex(metadata.training_run_hash().bytes())
            && self.dataset_export_hash == hex(metadata.dataset().export_digest().bytes())
            && self.dataset_selection_hash == hex(metadata.dataset().selection_digest().bytes())
            && self.universe_id == metadata.universe_id().as_str()
            && self.training_start_unix_nanos == metadata.training_period().start().unix_nanos()
            && self.training_end_unix_nanos == metadata.training_period().end().unix_nanos()
            && self.feature_semantic_hashes.len() == metadata.feature_semantic_digests().len()
            && self
                .feature_semantic_hashes
                .iter()
                .zip(metadata.feature_semantic_digests())
                .all(|(stored, admitted)| *stored == hex(admitted.as_bytes()))
            && self
                .limitations
                .iter()
                .map(String::as_str)
                .eq(metadata.limitations().iter().map(|value| value.as_ref()))
            && self.unavailable_reason == metadata.fallback_reason()
            && self.output_binding.matches(metadata.output_binding())
    }

    fn revalidated_calibration(
        &self,
        metadata: &ModelMetadata,
    ) -> Result<Option<CalibrationEvidence>, ForecastApplicationError> {
        match (&self.calibration, metadata.forecast_calibration()) {
            (None, None) => Ok(None),
            (Some(value), Some(_admitted)) => value
                .revalidated(metadata, self.observed_through_unix_nanos)
                .map(Some),
            (None, Some(_)) | (Some(_), None) => Err(ForecastApplicationError::CorruptIndex),
        }
    }

    fn verify_vintage_identity(
        &self,
        vintage_id: Sha256Digest,
        metadata: &ModelMetadata,
        instrument_id: InstrumentId,
        calibration: Option<&CalibrationEvidence>,
        controlled_artifact_hash: Sha256Digest,
    ) -> Result<(), ForecastApplicationError> {
        let horizon = ForecastHorizon::try_new(
            NonZeroU16::new(self.horizon_points).ok_or(ForecastApplicationError::CorruptIndex)?,
            NonZeroU64::new(self.horizon_step_nanos)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let mut observed_history = Vec::new();
        observed_history
            .try_reserve_exact(self.observed_history.len())
            .map_err(|_error| ForecastApplicationError::Capacity)?;
        for point in &self.observed_history {
            observed_history.push(point.typed()?);
        }
        let mut points = Vec::new();
        points
            .try_reserve_exact(self.points.len())
            .map_err(|_error| ForecastApplicationError::Capacity)?;
        for point in &self.points {
            points.push(point.identity_point()?);
        }
        verify_forecast_vintage_identity(
            vintage_id,
            metadata,
            instrument_id,
            Timestamp::from_unix_nanos(self.observed_through_unix_nanos),
            Timestamp::from_unix_nanos(self.available_at_unix_nanos),
            horizon,
            &observed_history,
            &points,
            calibration,
            Timestamp::from_unix_nanos(self.created_at_unix_nanos),
            Timestamp::from_unix_nanos(self.expires_at_unix_nanos),
            controlled_artifact_hash,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)
    }

    fn typed_price_points(
        &self,
    ) -> Result<Box<[SelectedPriceForecastPoint]>, ForecastApplicationError> {
        let mut points = Vec::new();
        points
            .try_reserve_exact(self.points.len())
            .map_err(|_error| ForecastApplicationError::Capacity)?;
        for point in &self.points {
            points.push(point.typed_price_point()?);
        }
        Ok(points.into_boxed_slice())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ObservedPointRecord {
    observed_at_unix_nanos: i64,
    available_at_unix_nanos: i64,
    mantissa: String,
    decimal_scale: u8,
    source_pit_hash: String,
    quality: String,
}

impl ObservedPointRecord {
    fn from_point(point: market_squawk_modeling::ForecastObservedPoint) -> Self {
        Self {
            observed_at_unix_nanos: point.observed_at().unix_nanos(),
            available_at_unix_nanos: point.available_at().unix_nanos(),
            mantissa: point.value().mantissa().to_string(),
            decimal_scale: point.value().scale(),
            source_pit_hash: hex(point.source_pit_hash().bytes()),
            quality: observed_quality_name(point.quality()).to_owned(),
        }
    }

    fn validate(&self, cutoff: i64, path_available_at: i64, scale: Option<u8>) -> bool {
        self.mantissa.parse::<i128>().is_ok()
            && self.decimal_scale <= market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE
            && scale == Some(self.decimal_scale)
            && self.observed_at_unix_nanos <= cutoff
            && self.available_at_unix_nanos >= self.observed_at_unix_nanos
            && self.available_at_unix_nanos <= path_available_at
            && valid_digest(&self.source_pit_hash)
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

    fn typed(&self) -> Result<ForecastObservedPoint, ForecastApplicationError> {
        ForecastObservedPoint::try_new(
            Timestamp::from_unix_nanos(self.observed_at_unix_nanos),
            Timestamp::from_unix_nanos(self.available_at_unix_nanos),
            ForecastValue::try_new(
                self.mantissa
                    .parse::<i128>()
                    .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                self.decimal_scale,
            )
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            digest_from_hex(&self.source_pit_hash)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            decoded_observed_quality(&self.quality)
                .ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)
    }

    fn product_value(
        &self,
        binding: &ForecastOutputBindingRecord,
    ) -> Result<Value, ForecastApplicationError> {
        Ok(json!({
            "observedAtUnixNanos": self.observed_at_unix_nanos.to_string(),
            "availableAtUnixNanos": self.available_at_unix_nanos.to_string(),
            "value": binding.product_amount(&self.mantissa, self.decimal_scale)?,
        }))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    fn typed_price_point(&self) -> Result<SelectedPriceForecastPoint, ForecastApplicationError> {
        let central = ForecastValue::try_new(
            self.central_mantissa
                .parse::<i128>()
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            self.decimal_scale,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let intervals = self
            .intervals
            .as_ref()
            .map(|value| value.typed_price_intervals(self.decimal_scale))
            .transpose()?;
        Ok(SelectedPriceForecastPoint {
            target_at: Timestamp::from_unix_nanos(self.target_at_unix_nanos),
            central,
            intervals,
        })
    }

    fn product_value(
        &self,
        binding: &ForecastOutputBindingRecord,
    ) -> Result<Value, ForecastApplicationError> {
        let central = binding.product_amount(&self.central_mantissa, self.decimal_scale)?;
        let ranges = self
            .intervals
            .as_ref()
            .map(|intervals| intervals.product_value(self.decimal_scale, binding))
            .transpose()?;
        Ok(json!({
            "targetAtUnixNanos": self.target_at_unix_nanos.to_string(),
            "central": central,
            "ranges": ranges,
        }))
    }

    fn identity_point(
        &self,
    ) -> Result<(Timestamp, ForecastValue, Option<[[ForecastValue; 2]; 3]>), ForecastApplicationError>
    {
        let central = ForecastValue::try_new(
            self.central_mantissa
                .parse::<i128>()
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            self.decimal_scale,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let intervals = self
            .intervals
            .as_ref()
            .map(|value| value.identity_bounds(self.decimal_scale))
            .transpose()?;
        Ok((
            Timestamp::from_unix_nanos(self.target_at_unix_nanos),
            central,
            intervals,
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    fn typed_price_intervals(
        &self,
        scale: u8,
    ) -> Result<SelectedPriceIntervals, ForecastApplicationError> {
        fn interval(
            pair: &[String; 2],
            scale: u8,
        ) -> Result<SelectedPriceInterval, ForecastApplicationError> {
            let lower = ForecastValue::try_new(
                pair[0]
                    .parse::<i128>()
                    .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                scale,
            )
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
            let upper = ForecastValue::try_new(
                pair[1]
                    .parse::<i128>()
                    .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                scale,
            )
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
            if lower > upper {
                return Err(ForecastApplicationError::CorruptIndex);
            }
            Ok(SelectedPriceInterval { lower, upper })
        }
        let interval_50 = interval(&self.interval_50, scale)?;
        let interval_80 = interval(&self.interval_80, scale)?;
        let interval_95 = interval(&self.interval_95, scale)?;
        if interval_95.lower > interval_80.lower
            || interval_80.lower > interval_50.lower
            || interval_50.upper > interval_80.upper
            || interval_80.upper > interval_95.upper
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(SelectedPriceIntervals {
            interval_50,
            interval_80,
            interval_95,
        })
    }

    fn product_value(
        &self,
        scale: u8,
        binding: &ForecastOutputBindingRecord,
    ) -> Result<Value, ForecastApplicationError> {
        let range = |pair: &[String; 2]| -> Result<Value, ForecastApplicationError> {
            Ok(json!({
                "lower": binding.product_amount(&pair[0], scale)?,
                "upper": binding.product_amount(&pair[1], scale)?,
            }))
        };
        Ok(json!({
            "likely": range(&self.interval_50)?,
            "wider": range(&self.interval_80)?,
            "stress": range(&self.interval_95)?,
        }))
    }

    fn identity_bounds(
        &self,
        scale: u8,
    ) -> Result<[[ForecastValue; 2]; 3], ForecastApplicationError> {
        fn pair(
            values: &[String; 2],
            scale: u8,
        ) -> Result<[ForecastValue; 2], ForecastApplicationError> {
            Ok([
                ForecastValue::try_new(
                    values[0]
                        .parse::<i128>()
                        .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                    scale,
                )
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                ForecastValue::try_new(
                    values[1]
                        .parse::<i128>()
                        .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
                    scale,
                )
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            ])
        }
        Ok([
            pair(&self.interval_50, scale)?,
            pair(&self.interval_80, scale)?,
            pair(&self.interval_95, scale)?,
        ])
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CalibrationRecord {
    identity_sha256: String,
    method: String,
    window_start_unix_nanos: i64,
    window_end_unix_nanos: i64,
    observations: u32,
    policy_hash: String,
    policy_size_bytes: u64,
    residuals_hash: String,
    residuals_size_bytes: u64,
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
            identity_sha256: hex(value.identity().bytes()),
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
            policy_size_bytes: value.policy_size_bytes(),
            residuals_hash: hex(value.residuals_hash().bytes()),
            residuals_size_bytes: value.residuals_size_bytes(),
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

    fn product_value(&self) -> Result<Value, ForecastApplicationError> {
        let bands = [0_usize, 1, 2]
            .map(|index| {
                Ok(json!({
                    "targetCoveragePercent": product_percent(
                        &self.target_coverage_basis_points[index].to_string(),
                        2,
                    )?,
                    "realizedCovered": self.realized_covered[index],
                    "realizedTotal": self.realized_total[index],
                }))
            })
            .into_iter()
            .collect::<Result<Vec<_>, ForecastApplicationError>>()?;
        Ok(json!({
            "windowStartUnixNanos": self.window_start_unix_nanos.to_string(),
            "windowEndUnixNanos": self.window_end_unix_nanos.to_string(),
            "observationCount": self.observations,
            "coverage": bands,
            "interpretation": self.coverage_interpretation,
            "assumptions": self.dependence_assumptions,
        }))
    }

    fn validate(&self, observed_cutoff: i64) -> bool {
        valid_digest(&self.identity_sha256)
            && self.policy_size_bytes > 0
            && self.residuals_size_bytes > 0
            && matches!(
                self.method.as_str(),
                "mapie_enbpi" | "mapie_aci" | "residual_quantile"
            )
            && self.window_start_unix_nanos < self.window_end_unix_nanos
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

    fn revalidated(
        &self,
        metadata: &ModelMetadata,
        observed_cutoff_unix_nanos: i64,
    ) -> Result<CalibrationEvidence, ForecastApplicationError> {
        if !self.validate(observed_cutoff_unix_nanos) {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let method = match self.method.as_str() {
            "mapie_enbpi" => CalibrationMethod::MapieEnbpi,
            "mapie_aci" => CalibrationMethod::MapieAci,
            "residual_quantile" => CalibrationMethod::ResidualQuantile,
            _ => return Err(ForecastApplicationError::CorruptIndex),
        };
        let window = CalibrationWindow::try_new(
            Timestamp::from_unix_nanos(self.window_start_unix_nanos),
            Timestamp::from_unix_nanos(self.window_end_unix_nanos),
            NonZeroU32::new(self.observations).ok_or(ForecastApplicationError::CorruptIndex)?,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let coverage = [
            ForecastCoverage::Fifty,
            ForecastCoverage::Eighty,
            ForecastCoverage::NinetyFive,
        ];
        let mut bands = Vec::new();
        bands
            .try_reserve_exact(coverage.len())
            .map_err(|_error| ForecastApplicationError::Capacity)?;
        for (index, coverage) in coverage.into_iter().enumerate() {
            let total = NonZeroU64::new(self.realized_total[index])
                .ok_or(ForecastApplicationError::CorruptIndex)?;
            let realized = RealizedCoverage::try_new(self.realized_covered[index], total)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
            bands.push(
                CalibrationBand::try_new(
                    coverage,
                    self.lower_offsets[index],
                    self.upper_offsets[index],
                    realized,
                )
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            );
        }
        let bands: [CalibrationBand; 3] = bands
            .try_into()
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let evidence = CalibrationEvidence::try_new(
            metadata,
            method,
            window,
            digest_from_hex(&self.policy_hash)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            digest_from_hex(&self.residuals_hash)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            bands,
            &self.dependence_assumptions,
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let identity = digest_from_hex(&self.identity_sha256)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        if evidence.identity() != identity
            || evidence.policy_size_bytes() != self.policy_size_bytes
            || evidence.residuals_size_bytes() != self.residuals_size_bytes
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(evidence)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

    pub(super) fn absolute_error_mantissa(&self) -> Option<i128> {
        self.absolute_error_mantissa.parse().ok()
    }

    fn product_value(
        &self,
        binding: &ForecastOutputBindingRecord,
    ) -> Result<Value, ForecastApplicationError> {
        Ok(json!({
            "targetAtUnixNanos": self.target_at_unix_nanos.to_string(),
            "observedAtUnixNanos": self.observed_at_unix_nanos.to_string(),
            "availableAtUnixNanos": self.available_at_unix_nanos.to_string(),
            "actual": binding.product_amount(&self.actual_mantissa, self.decimal_scale)?,
            "signedError": binding.product_amount(&self.signed_error_mantissa, self.decimal_scale)?,
            "absoluteError": binding.product_amount(&self.absolute_error_mantissa, self.decimal_scale)?,
        }))
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
#[serde(deny_unknown_fields)]
pub(in crate::application::model) struct ForecastIndex {
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

    pub(in crate::application::model) fn canonical_bytes(
        &self,
        limits: ForecastApplicationLimits,
    ) -> Result<Vec<u8>, ForecastApplicationError> {
        self.validate(limits)?;
        let mut canonical = self.clone();
        canonical
            .vintages
            .sort_unstable_by(|left, right| left.vintage_id.cmp(&right.vintage_id));
        canonical.outcomes.sort_unstable_by(|left, right| {
            left.vintage_id
                .cmp(&right.vintage_id)
                .then_with(|| left.target_at_unix_nanos.cmp(&right.target_at_unix_nanos))
                .then_with(|| left.outcome_id.cmp(&right.outcome_id))
        });
        let bytes = serde_json::to_vec(&canonical)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        if bytes.len() > limits.maximum_index_bytes.get() {
            return Err(ForecastApplicationError::Capacity);
        }
        Ok(bytes)
    }

    pub(in crate::application::model) fn decode_canonical(
        bytes: &[u8],
        limits: ForecastApplicationLimits,
    ) -> Result<Self, ForecastApplicationError> {
        let index = serde_json::from_slice::<Self>(bytes)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        if index.canonical_bytes(limits)? != bytes {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(index)
    }

    pub(in crate::application::model) fn artifact_references(
        &self,
    ) -> Result<Vec<ArtifactReference>, ForecastApplicationError> {
        let mut references = self
            .vintages
            .iter()
            .map(VintageRecord::artifact_reference)
            .collect::<Result<Vec<_>, _>>()?;
        if references.iter().enumerate().any(|(position, reference)| {
            references[position + 1..].iter().any(|other| {
                (reference.sha256() == other.sha256() || reference.id() == other.id())
                    && reference != other
            })
        }) {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        references.sort_unstable_by(|left, right| {
            left.sha256()
                .cmp(right.sha256())
                .then_with(|| left.id().cmp(right.id()))
        });
        references.dedup();
        Ok(references)
    }

    pub(in crate::application::model) fn model_coordinates(
        &self,
    ) -> impl Iterator<Item = (&str, &str, u64)> {
        self.vintages.iter().map(VintageRecord::model_coordinate)
    }

    pub(super) fn latest_valid_for_instrument(
        &self,
        instrument_id: InstrumentId,
        as_of: market_squawk_domain::Timestamp,
        retained_vintage_hard_ceiling: NonZeroUsize,
    ) -> Result<ForecastIndexSelection, ForecastApplicationError> {
        if self.vintages.len() > retained_vintage_hard_ceiling.get() {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let mut eligible_vintage_count = 0_usize;
        let mut selected: Option<&VintageRecord> = None;
        for vintage in &self.vintages {
            let candidate_instrument = InstrumentId::from_str(&vintage.payload.instrument_id)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
            if candidate_instrument != instrument_id
                || vintage.payload.available_at_unix_nanos > as_of.unix_nanos()
                || vintage.payload.created_at_unix_nanos > as_of.unix_nanos()
                || vintage.payload.expires_at_unix_nanos <= as_of.unix_nanos()
            {
                continue;
            }
            if !vintage.validate() {
                return Err(ForecastApplicationError::CorruptIndex);
            }
            eligible_vintage_count = eligible_vintage_count
                .checked_add(1)
                .ok_or(ForecastApplicationError::CorruptIndex)?;
            if selected.is_none_or(|current| compare_selection_priority(vintage, current).is_gt()) {
                selected = Some(vintage);
            }
        }
        let selected = selected.ok_or(ForecastApplicationError::NotFound)?;
        let competing_eligible_vintage_count = eligible_vintage_count
            .checked_sub(1)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let receipt = ForecastSelectionReceipt::try_new(ForecastSelectionReceiptBody {
            policy_revision: FORECAST_SELECTION_POLICY_REVISION,
            selection_order:
                ForecastSelectionOrder::NewestCreatedAtObservedThroughAvailableAtThenLowestVintageId,
            qualification: ForecastSelectionQualification::AnyValid,
            instrument_id,
            as_of_unix_nanos: as_of.unix_nanos(),
            considered_vintage_count: self.vintages.len(),
            retained_vintage_hard_ceiling: retained_vintage_hard_ceiling.get(),
            eligible_vintage_count,
            competing_eligible_vintage_count,
            selection_complete: true,
            selected_vintage_id: selected.vintage_id.clone(),
            selected_created_at_unix_nanos: selected.payload.created_at_unix_nanos,
            selected_observed_through_unix_nanos: selected.payload.observed_through_unix_nanos,
            selected_available_at_unix_nanos: selected.payload.available_at_unix_nanos,
            selected_expires_at_unix_nanos: selected.payload.expires_at_unix_nanos,
            selected_terminal_target_at_unix_nanos: None,
            selected_analysis_pairing_sha256: selected
                .payload
                .analysis_evidence
                .pairing_sha256
                .clone(),
            selected_serving_feature_sha256: selected
                .payload
                .serving_evidence
                .feature_sha256
                .clone(),
        })?;
        Ok(ForecastIndexSelection {
            vintage: selected.clone(),
            receipt,
        })
    }

    pub(super) fn latest_valid_exact_horizon_price_for_instrument(
        &self,
        instrument_id: InstrumentId,
        requested_horizon_nanos: NonZeroU64,
        as_of: Timestamp,
        retained_vintage_hard_ceiling: NonZeroUsize,
    ) -> Result<ForecastIndexSelection, ForecastApplicationError> {
        if self.vintages.len() > retained_vintage_hard_ceiling.get() {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let mut eligible_vintage_count = 0_usize;
        let mut selected: Option<(&VintageRecord, i64)> = None;
        for vintage in &self.vintages {
            let candidate_instrument = InstrumentId::from_str(&vintage.payload.instrument_id)
                .map_err(|_| ForecastApplicationError::CorruptIndex)?;
            if candidate_instrument != instrument_id
                || vintage.payload.available_at_unix_nanos > as_of.unix_nanos()
                || vintage.payload.created_at_unix_nanos > as_of.unix_nanos()
                || vintage.payload.expires_at_unix_nanos <= as_of.unix_nanos()
            {
                continue;
            }
            if !vintage.validate() {
                return Err(ForecastApplicationError::CorruptIndex);
            }
            let Some(terminal_target) =
                vintage.exact_horizon_price_terminal_target(requested_horizon_nanos, as_of)?
            else {
                continue;
            };
            eligible_vintage_count = eligible_vintage_count
                .checked_add(1)
                .ok_or(ForecastApplicationError::CorruptIndex)?;
            if selected
                .is_none_or(|(current, _)| compare_selection_priority(vintage, current).is_gt())
            {
                selected = Some((vintage, terminal_target));
            }
        }
        let (selected, terminal_target) = selected.ok_or(ForecastApplicationError::NotFound)?;
        let competing_eligible_vintage_count = eligible_vintage_count
            .checked_sub(1)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        let receipt = ForecastSelectionReceipt::try_new(ForecastSelectionReceiptBody {
            policy_revision: FORECAST_SELECTION_POLICY_REVISION,
            selection_order:
                ForecastSelectionOrder::NewestCreatedAtObservedThroughAvailableAtThenLowestVintageId,
            qualification: ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
                horizon_nanos: requested_horizon_nanos,
            },
            instrument_id,
            as_of_unix_nanos: as_of.unix_nanos(),
            considered_vintage_count: self.vintages.len(),
            retained_vintage_hard_ceiling: retained_vintage_hard_ceiling.get(),
            eligible_vintage_count,
            competing_eligible_vintage_count,
            selection_complete: true,
            selected_vintage_id: selected.vintage_id.clone(),
            selected_created_at_unix_nanos: selected.payload.created_at_unix_nanos,
            selected_observed_through_unix_nanos: selected.payload.observed_through_unix_nanos,
            selected_available_at_unix_nanos: selected.payload.available_at_unix_nanos,
            selected_expires_at_unix_nanos: selected.payload.expires_at_unix_nanos,
            selected_terminal_target_at_unix_nanos: Some(terminal_target),
            selected_analysis_pairing_sha256: selected
                .payload
                .analysis_evidence
                .pairing_sha256
                .clone(),
            selected_serving_feature_sha256: selected
                .payload
                .serving_evidence
                .feature_sha256
                .clone(),
        })?;
        Ok(ForecastIndexSelection {
            vintage: selected.clone(),
            receipt,
        })
    }
}

pub(super) struct ForecastIndexSelection {
    pub(super) vintage: VintageRecord,
    pub(super) receipt: ForecastSelectionReceipt,
}

fn compare_selection_priority(left: &VintageRecord, right: &VintageRecord) -> Ordering {
    left.payload
        .created_at_unix_nanos
        .cmp(&right.payload.created_at_unix_nanos)
        .then_with(|| {
            left.payload
                .observed_through_unix_nanos
                .cmp(&right.payload.observed_through_unix_nanos)
        })
        .then_with(|| {
            left.payload
                .available_at_unix_nanos
                .cmp(&right.payload.available_at_unix_nanos)
        })
        // The lower content identity wins an otherwise exact tie. Reverse the comparison because
        // the caller retains the candidate that orders greater.
        .then_with(|| right.vintage_id.cmp(&left.vintage_id))
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

const fn observed_quality_name(quality: DataQuality) -> &'static str {
    match quality {
        DataQuality::DirectVerified => "direct_verified",
        DataQuality::DirectUnverified => "direct_unverified",
        DataQuality::OfficialDelayed => "official_delayed",
        DataQuality::Aggregated => "aggregated",
        DataQuality::Indicative => "indicative",
        DataQuality::Modeled => "modeled",
        DataQuality::Estimated => "estimated",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => "quarantined",
    }
}

fn product_token(domain: &[u8], identity: &str) -> Result<Uuid, ForecastApplicationError> {
    let identity = digest_from_hex(identity)?;
    Ok(opaque_product_token(domain, &[&identity.bytes()]))
}

pub(super) fn decimal_text(value: &str, scale: u8) -> Result<String, ForecastApplicationError> {
    let parsed = value
        .parse::<i128>()
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
    let negative = parsed.is_negative();
    let digits = parsed
        .checked_abs()
        .ok_or(ForecastApplicationError::CorruptIndex)?
        .to_string();
    if scale == 0 {
        return Ok(value.to_owned());
    }
    let scale = usize::from(scale);
    let padded_length = scale
        .checked_add(1)
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let mut magnitude = String::new();
    magnitude
        .try_reserve_exact(padded_length.max(digits.len()))
        .map_err(|_| ForecastApplicationError::Capacity)?;
    for _ in digits.len()..padded_length {
        magnitude.push('0');
    }
    magnitude.push_str(&digits);
    let integral_digits = magnitude.len() - scale;
    let mut rendered = String::new();
    rendered
        .try_reserve_exact(magnitude.len() + usize::from(negative) + 1)
        .map_err(|_| ForecastApplicationError::Capacity)?;
    if negative {
        rendered.push('-');
    }
    rendered.push_str(&magnitude[..integral_digits]);
    rendered.push('.');
    rendered.push_str(&magnitude[integral_digits..]);
    Ok(rendered)
}

fn product_horizon(points: u16, step_nanos: u64) -> Result<Value, ForecastApplicationError> {
    let horizon = ForecastHorizon::try_new(
        NonZeroU16::new(points).ok_or(ForecastApplicationError::CorruptIndex)?,
        NonZeroU64::new(step_nanos).ok_or(ForecastApplicationError::CorruptIndex)?,
    )
    .map_err(|_| ForecastApplicationError::CorruptIndex)?;
    let product = ForecastProductHorizon::try_from_horizon(horizon)
        .map_err(|_| ForecastApplicationError::CorruptIndex)?;
    Ok(json!({
        "label": product.label(),
        "description": product.description(),
        "points": product.points(),
    }))
}

fn product_percent(value: &str, scale: u8) -> Result<Value, ForecastApplicationError> {
    let exact = decimal_text(value, scale)?;
    let formatted = format!("{exact}%");
    Ok(json!({
        "exact": exact,
        "formatted": formatted,
    }))
}

fn decoded_observed_quality(value: &str) -> Option<DataQuality> {
    match value {
        "direct_verified" => Some(DataQuality::DirectVerified),
        "direct_unverified" => Some(DataQuality::DirectUnverified),
        "official_delayed" => Some(DataQuality::OfficialDelayed),
        "aggregated" => Some(DataQuality::Aggregated),
        "indicative" => Some(DataQuality::Indicative),
        "estimated" => Some(DataQuality::Estimated),
        "stale" => Some(DataQuality::Stale),
        "quarantined" => Some(DataQuality::Quarantined),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        str::FromStr,
    };

    use market_squawk_data::DatasetSchemaRegistry;
    use market_squawk_domain::{Currency, InstrumentId, Timestamp};
    use market_squawk_modeling::{ForecastMeasurement, ModelOutputSemantics};
    use sha2::{Digest as _, Sha256};

    use super::{
        BundleId, CalibrationRecord, ControlledArtifactRecord, ForecastAnalysisEvidenceRecord,
        ForecastAnalysisManifestRecord, ForecastAnalysisSchemaRecord, ForecastEstimatorRecord,
        ForecastIndex, ForecastMeasurementRecord, ForecastModelEvidenceRecord,
        ForecastOutputBindingRecord, ForecastOutputLabelRecord, ForecastPayloadRecord,
        ForecastProductIdentityRecord, ForecastServingEvidenceRecord, ForecastTargetRecord,
        IntervalRecord, ModelId, OUTPUT_BINDING_SCHEMA_VERSION, PointRecord, VintageRecord, hex,
        opaque_product_token,
    };
    use crate::application::model::forecast::{
        FORECAST_PAYLOAD_SCHEMA_VERSION, ForecastApplicationError, ForecastSelectionQualification,
        ForecastSelectionReceipt, INDEX_SCHEMA_VERSION,
    };

    #[test]
    fn latest_valid_selection_is_complete_deterministic_and_rejects_expiry_and_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let selected_instrument = InstrumentId::from_str("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")?;
        let other_instrument = InstrumentId::from_str("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")?;
        let ceiling = NonZeroUsize::new(16).ok_or("nonzero ceiling")?;
        let current_payload =
            serde_json::to_value(&vintage(selected_instrument, 4, 35, 10, 40).payload)?;
        assert_eq!(
            current_payload["payloadSchemaVersion"],
            FORECAST_PAYLOAD_SCHEMA_VERSION
        );
        assert!(current_payload["outputBinding"].is_object());
        let mut unknown_field = current_payload.clone();
        unknown_field
            .as_object_mut()
            .ok_or("payload object")?
            .insert("unknownField".to_owned(), serde_json::Value::Bool(true));
        assert!(serde_json::from_value::<ForecastPayloadRecord>(unknown_field).is_err());
        let mut index = ForecastIndex {
            schema_version: INDEX_SCHEMA_VERSION,
            vintages: vec![
                vintage(selected_instrument, 1, 20, 10, 100),
                vintage(selected_instrument, 3, 30, 10, 100),
                vintage(selected_instrument, 2, 30, 10, 100),
                vintage(selected_instrument, 4, 35, 10, 40),
                vintage(selected_instrument, 5, 70, 60, 100),
                vintage(other_instrument, 6, 40, 10, 1_000),
            ],
            outcomes: Vec::new(),
        };

        let (first_vintage_id, first_receipt_digest) = {
            let first = index.latest_valid_for_instrument(
                selected_instrument,
                Timestamp::from_unix_nanos(50),
                ceiling,
            )?;
            assert_eq!(first.receipt.eligible_vintage_count(), 3);
            assert_eq!(first.receipt.selected_vintage_id(), hex([2; 32]));
            assert_eq!(
                first.receipt.selected_analysis_pairing_sha256()?,
                super::digest_from_hex(&first.vintage.payload.analysis_evidence.pairing_sha256)?
            );
            assert_eq!(
                first.receipt.selected_serving_feature_sha256()?,
                super::digest_from_hex(&first.vintage.payload.serving_evidence.feature_sha256)?
            );
            assert_eq!(first.receipt.body.considered_vintage_count, 6);
            assert_eq!(first.receipt.body.retained_vintage_hard_ceiling, 16);
            assert_eq!(first.receipt.body.competing_eligible_vintage_count, 2);
            assert!(first.receipt.body.selection_complete);
            let exact_horizon = NonZeroU64::new(100).ok_or("nonzero horizon")?;
            assert_eq!(
                first.receipt.qualification(),
                ForecastSelectionQualification::AnyValid
            );
            assert_eq!(
                first.receipt.body.selected_terminal_target_at_unix_nanos,
                None
            );
            assert!(
                !first
                    .receipt
                    .is_exact_horizon_price_qualified(exact_horizon)
            );

            // Pin the temporal boundary independently of index selection so receipt validation
            // still rejects target-expiry and selection-time drift.
            let mut future_qualified_body = first.receipt.body.clone();
            future_qualified_body.qualification =
                ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
                    horizon_nanos: exact_horizon,
                };
            future_qualified_body.selected_terminal_target_at_unix_nanos = Some(110);
            future_qualified_body.selected_expires_at_unix_nanos = 100;
            let future_qualified =
                ForecastSelectionReceipt::try_new(future_qualified_body.clone())?;
            assert!(future_qualified.is_exact_horizon_price_qualified(exact_horizon));
            assert!(future_qualified.binds_live_terminal_target(Timestamp::from_unix_nanos(110)));

            let mut expires_after_target_body = future_qualified_body.clone();
            expires_after_target_body.selected_expires_at_unix_nanos = 111;
            let expires_after_target =
                ForecastSelectionReceipt::try_new(expires_after_target_body)?;
            assert!(
                !expires_after_target.binds_live_terminal_target(Timestamp::from_unix_nanos(110))
            );

            future_qualified_body.as_of_unix_nanos = 110;
            let selected_at_target = ForecastSelectionReceipt::try_new(future_qualified_body)?;
            assert!(
                !selected_at_target.binds_live_terminal_target(Timestamp::from_unix_nanos(110))
            );
            assert_eq!(
                first.vintage.payload.output_binding.decoded_semantics(),
                Some(ModelOutputSemantics::Regression)
            );
            assert_eq!(
                first.vintage.payload.output_binding.decoded_measurement(),
                Some(ForecastMeasurement::Price {
                    currency: Currency::try_from("USD")?,
                })
            );
            assert_eq!(
                first
                    .vintage
                    .payload
                    .output_binding
                    .decoded_central_statistic(),
                Some(
                    market_squawk_modeling::ForecastCentralStatistic::ModelEstimatedConditionalMean
                )
            );
            (
                first.receipt.selected_vintage_id().to_owned(),
                first.receipt.receipt_digest(),
            )
        };

        index.vintages.reverse();
        let reordered_digest = {
            let reordered = index.latest_valid_for_instrument(
                selected_instrument,
                Timestamp::from_unix_nanos(50),
                ceiling,
            )?;
            assert_eq!(reordered.receipt.selected_vintage_id(), first_vintage_id);
            reordered.receipt.receipt_digest()
        };
        assert_eq!(reordered_digest, first_receipt_digest);

        let changed_as_of = index.latest_valid_for_instrument(
            selected_instrument,
            Timestamp::from_unix_nanos(51),
            ceiling,
        )?;
        assert_eq!(
            changed_as_of.receipt.selected_vintage_id(),
            first_vintage_id
        );
        assert_ne!(changed_as_of.receipt.receipt_digest(), first_receipt_digest);

        let mut changed_identity = index.clone();
        changed_identity
            .vintages
            .iter_mut()
            .find(|vintage| vintage.vintage_id == first_vintage_id)
            .ok_or("selected fixture vintage")?
            .vintage_id = hex([7; 32]);
        let changed_identity = changed_identity.latest_valid_for_instrument(
            selected_instrument,
            Timestamp::from_unix_nanos(50),
            ceiling,
        )?;
        assert_eq!(changed_identity.receipt.selected_vintage_id(), hex([3; 32]));
        assert_ne!(
            changed_identity.receipt.receipt_digest(),
            first_receipt_digest
        );

        assert!(matches!(
            index.latest_valid_for_instrument(
                selected_instrument,
                Timestamp::from_unix_nanos(200),
                ceiling,
            ),
            Err(ForecastApplicationError::NotFound)
        ));

        let exact_horizon = NonZeroU64::new(100).ok_or("nonzero exact horizon")?;
        let mut exact_index = ForecastIndex {
            schema_version: INDEX_SCHEMA_VERSION,
            vintages: vec![
                calibrated_vintage(selected_instrument, 8, 35, 10, 100, 100, false),
                calibrated_vintage(selected_instrument, 9, 40, 10, 100, 200, false),
                calibrated_vintage(selected_instrument, 10, 45, 10, 100, 100, true),
                vintage(selected_instrument, 11, 49, 10, 100),
            ],
            outcomes: Vec::new(),
        };
        let exact = exact_index.latest_valid_exact_horizon_price_for_instrument(
            selected_instrument,
            exact_horizon,
            Timestamp::from_unix_nanos(50),
            ceiling,
        )?;
        assert_eq!(exact.receipt.selected_vintage_id(), hex([8; 32]));
        assert_eq!(exact.receipt.eligible_vintage_count(), 1);
        assert_eq!(exact.receipt.body.considered_vintage_count, 4);
        assert_eq!(
            exact.receipt.qualification(),
            ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
                horizon_nanos: exact_horizon
            }
        );
        assert_eq!(
            exact.receipt.selected_terminal_target_at_unix_nanos(),
            Some(110)
        );
        assert!(
            exact
                .receipt
                .binds_live_terminal_target(Timestamp::from_unix_nanos(110))
        );
        let exact_digest = exact.receipt.receipt_digest();
        exact_index.vintages.reverse();
        let reordered_exact = exact_index.latest_valid_exact_horizon_price_for_instrument(
            selected_instrument,
            exact_horizon,
            Timestamp::from_unix_nanos(50),
            ceiling,
        )?;
        assert_eq!(reordered_exact.receipt.receipt_digest(), exact_digest);
        let return_only = ForecastIndex {
            schema_version: INDEX_SCHEMA_VERSION,
            vintages: vec![calibrated_vintage(
                selected_instrument,
                12,
                35,
                10,
                100,
                100,
                true,
            )],
            outcomes: Vec::new(),
        };
        assert!(matches!(
            return_only.latest_valid_exact_horizon_price_for_instrument(
                selected_instrument,
                exact_horizon,
                Timestamp::from_unix_nanos(50),
                ceiling,
            ),
            Err(ForecastApplicationError::NotFound)
        ));
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the focused selector fixture keeps exact time and output coordinates explicit"
    )]
    fn calibrated_vintage(
        instrument_id: InstrumentId,
        identity: u8,
        created_at_unix_nanos: i64,
        available_at_unix_nanos: i64,
        expires_at_unix_nanos: i64,
        horizon_nanos: u64,
        return_measurement: bool,
    ) -> VintageRecord {
        let mut vintage = vintage(
            instrument_id,
            identity,
            created_at_unix_nanos,
            available_at_unix_nanos,
            expires_at_unix_nanos,
        );
        vintage.payload.horizon_step_nanos = horizon_nanos;
        vintage.payload.model_evidence.selected_horizon_step_nanos = horizon_nanos;
        vintage.payload.points[0].target_at_unix_nanos =
            available_at_unix_nanos + i64::try_from(horizon_nanos).expect("fixture horizon fits");
        vintage.payload.points[0].intervals = Some(IntervalRecord {
            interval_50: ["9900".to_owned(), "10100".to_owned()],
            interval_80: ["9800".to_owned(), "10200".to_owned()],
            interval_95: ["9700".to_owned(), "10300".to_owned()],
        });
        vintage.payload.calibration = Some(CalibrationRecord {
            identity_sha256: hex([21; 32]),
            method: "residual_quantile".to_owned(),
            window_start_unix_nanos: 0,
            window_end_unix_nanos: 1,
            observations: 3,
            policy_hash: hex([22; 32]),
            policy_size_bytes: 1,
            residuals_hash: hex([23; 32]),
            residuals_size_bytes: 24,
            target_coverage_basis_points: [5_000, 8_000, 9_500],
            lower_offsets: [-1.0, -2.0, -3.0],
            upper_offsets: [1.0, 2.0, 3.0],
            realized_covered: [1, 2, 3],
            realized_total: [3, 3, 3],
            coverage_interpretation:
                "realized marginal empirical coverage; not a per-observation guarantee".to_owned(),
            dependence_assumptions: "fixture marginal calibration".to_owned(),
        });
        vintage.payload.model_evidence.calibration = "calibrated".to_owned();
        vintage.payload.output_binding.target =
            ForecastTargetRecord::FixedHorizonTerminal { horizon_nanos };
        if return_measurement {
            vintage.payload.output_binding.measurement = ForecastMeasurementRecord::Return;
            vintage.payload.output_binding.central_statistic = "unavailable".to_owned();
        }
        reseal_payload(&mut vintage);
        vintage
    }

    fn reseal_payload(vintage: &mut VintageRecord) {
        let encoded = serde_json::to_vec(&vintage.payload).expect("fixture payload serializes");
        let payload_sha256: [u8; 32] = Sha256::digest(&encoded).into();
        vintage.controlled_artifact.sha256 = hex(payload_sha256);
        vintage.controlled_artifact.byte_count = encoded.len();
    }

    fn vintage(
        instrument_id: InstrumentId,
        identity: u8,
        created_at_unix_nanos: i64,
        available_at_unix_nanos: i64,
        expires_at_unix_nanos: i64,
    ) -> VintageRecord {
        let observed_through_unix_nanos = available_at_unix_nanos;
        let model_id =
            ModelId::from_str("cccccccc-cccc-4ccc-8ccc-cccccccccccc").expect("fixture model id");
        let bundle_id = BundleId::try_new("selection-fixture").expect("fixture bundle id");
        let bundle_version = NonZeroU64::new(1).expect("fixture bundle version");
        let model_token = opaque_product_token(
            b"market-squawk/product-model/v1\0",
            &[
                model_id.as_uuid().as_bytes(),
                bundle_id.as_str().as_bytes(),
                &bundle_version.get().to_be_bytes(),
            ],
        );
        let payload = ForecastPayloadRecord {
            payload_schema_version: FORECAST_PAYLOAD_SCHEMA_VERSION,
            instrument_id: instrument_id.to_string(),
            product_identity: ForecastProductIdentityRecord {
                display_name: "Fixture investment".to_owned(),
                canonical_symbol: Some("FIX".to_owned()),
                description: "Listed company investment with point-in-time verified identity."
                    .to_owned(),
                quote_currency: "USD".to_owned(),
                knowledge_at_unix_nanos: available_at_unix_nanos,
                effective_at_unix_nanos: observed_through_unix_nanos,
            },
            model_evidence: ForecastModelEvidenceRecord {
                model_token: model_token.to_string(),
                selected_horizon_points: 1,
                selected_horizon_step_nanos: 100,
                overall: "limited".to_owned(),
                pit_inputs: "sufficient".to_owned(),
                out_of_sample: "limited".to_owned(),
                horizon_alignment: "sufficient".to_owned(),
                calibration: "limited".to_owned(),
                interpretation: "Some required model evidence is limited or unavailable. Use this forecast only as supporting research, and take no action when required evidence is missing.".to_owned(),
            },
            model_id: model_id.to_string(),
            bundle_id: bundle_id.as_str().to_owned(),
            bundle_version: bundle_version.get(),
            metadata_hash: hex([10; 32]),
            artifact_hash: hex([11; 32]),
            training_run_hash: hex([12; 32]),
            output_binding: ForecastOutputBindingRecord {
                schema_version: OUTPUT_BINDING_SCHEMA_VERSION,
                output_semantics: "regression".to_owned(),
                measurement: ForecastMeasurementRecord::Price {
                    currency: "USD".to_owned(),
                },
                central_statistic: "model_estimated_conditional_mean".to_owned(),
                target: ForecastTargetRecord::FixedHorizonTerminal { horizon_nanos: 100 },
                target_transform: "identity".to_owned(),
                output_transform: "identity".to_owned(),
                objective: "squared_error".to_owned(),
                estimator: ForecastEstimatorRecord::SealedDirectLeastSquaresV1,
                identity_sha256: hex([16; 32]),
                label: ForecastOutputLabelRecord {
                    kind: "label".to_owned(),
                    scope: "instrument".to_owned(),
                    corporate_actions: "requires_adjustment".to_owned(),
                    name: "price-target".to_owned(),
                    version: 1,
                },
            },
            analysis_evidence: analysis_evidence(identity),
            serving_evidence: serving_evidence(
                identity,
                observed_through_unix_nanos,
                available_at_unix_nanos,
            ),
            dataset_export_hash: hex([13; 32]),
            dataset_selection_hash: hex([14; 32]),
            universe_id: "selection-fixture".to_owned(),
            training_start_unix_nanos: 0,
            training_end_unix_nanos: 1,
            feature_semantic_hashes: vec![hex([15; 32])],
            observed_through_unix_nanos,
            available_at_unix_nanos,
            created_at_unix_nanos,
            expires_at_unix_nanos,
            model_age_nanos_at_publication: created_at_unix_nanos - 1,
            data_age_nanos_at_publication: created_at_unix_nanos - observed_through_unix_nanos,
            horizon_points: 1,
            horizon_step_nanos: 100,
            observed_history: Vec::new(),
            quality: "modeled".to_owned(),
            points: vec![PointRecord {
                target_at_unix_nanos: observed_through_unix_nanos + 100,
                central_mantissa: "10000".to_owned(),
                decimal_scale: 2,
                intervals: None,
            }],
            calibration: None,
            limitations: vec!["Research forecast; realized outcomes may differ.".to_owned()],
            unavailable_reason: "No action when evidence is unavailable.".to_owned(),
        };
        let encoded = serde_json::to_vec(&payload).expect("fixture payload serializes");
        let payload_sha256: [u8; 32] = Sha256::digest(&encoded).into();
        VintageRecord {
            vintage_id: hex([identity; 32]),
            request_hash: hex([identity.saturating_add(32); 32]),
            controlled_artifact: ControlledArtifactRecord {
                artifact_id: format!("forecast-{identity}"),
                sha256: hex(payload_sha256),
                byte_count: encoded.len(),
                media_type: "application/json".to_owned(),
            },
            payload,
        }
    }

    fn analysis_evidence(identity: u8) -> ForecastAnalysisEvidenceRecord {
        let schema = DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .expect("fixture schema is registered");
        ForecastAnalysisEvidenceRecord {
            manifest: ForecastAnalysisManifestRecord {
                dataset: "selection-analysis-v1".to_owned(),
                manifest_version: u64::from(identity),
                schema: ForecastAnalysisSchemaRecord {
                    name: schema.name().to_owned(),
                    version: schema.version().get(),
                    fingerprint: hex(schema.fingerprint()),
                },
                content_hash: hex([identity.saturating_add(64); 32]),
            },
            production_identity_sha256: hex([identity.saturating_add(65); 32]),
            production_receipt_sha256: hex([identity.saturating_add(66); 32]),
            pairing_sha256: hex([identity.saturating_add(67); 32]),
        }
    }

    fn serving_evidence(
        identity: u8,
        observed_through_unix_nanos: i64,
        knowledge_cutoff_unix_nanos: i64,
    ) -> ForecastServingEvidenceRecord {
        let schema = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .expect("fixture schema is registered");
        ForecastServingEvidenceRecord {
            manifest: ForecastAnalysisManifestRecord {
                dataset: "selection-serving-v1".to_owned(),
                manifest_version: u64::from(identity),
                schema: ForecastAnalysisSchemaRecord {
                    name: schema.name().to_owned(),
                    version: schema.version().get(),
                    fingerprint: hex(schema.fingerprint()),
                },
                content_hash: hex([identity.saturating_add(68); 32]),
            },
            source_id: "alpaca-basic-iex-market-data".to_owned(),
            object_graph_sha256: hex([identity.saturating_add(69); 32]),
            selection_sha256: hex([identity.saturating_add(70); 32]),
            result_sha256: hex([identity.saturating_add(71); 32]),
            knowledge_cutoff_unix_nanos,
            prior_observed_at_unix_nanos: observed_through_unix_nanos - 1,
            observed_through_unix_nanos,
            feature_sha256: hex([identity.saturating_add(72); 32]),
        }
    }
}
