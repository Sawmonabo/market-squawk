//! Durable forecast generation, immutable vintages, and realized-outcome presentation.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use market_squawk_data::{DatasetManifestRef, Sha256Digest};
use market_squawk_domain::{
    Currency, DigestAlgorithm, EvidenceDigest, InstrumentId, SourceId, Timestamp,
};
use market_squawk_modeling::{
    CalibrationEvidence, ForecastCentralStatistic, ForecastCoverage, ForecastOutcome, ForecastPath,
    ForecastValue, ForecastVintage, ModelMetadata,
};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactReference, ArtifactRepository,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use persistence::{
    ForecastIndex, ForecastIndexSelection, ForecastPayloadRecord, OutcomeRecord, VintageRecord,
    decimal_text, digest_from_hex, hex,
};

use super::{
    ModelDomainService,
    runtime::{
        ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
        RetainedRuntimeBackup,
    },
};

mod generation;
pub(in crate::application::model) mod persistence;

/// Executes one already admitted forecast request inside the durable job runner.
pub(super) const GENERATE_FORECAST: &str = "Model.GenerateForecast";
/// Reads one exact immutable forecast vintage.
pub const GET_FORECAST: &str = "Model.GetForecast";
/// Selects and fully revalidates the newest nonexpired forecast for one exact instrument.
pub const SELECT_LATEST_VALID_FORECAST: &str = "Model.SelectLatestValidForecast";
/// Lists bounded immutable forecast-vintage summaries.
pub const LIST_FORECASTS: &str = "Model.ListForecasts";
/// Reads bounded immutable outcomes appended to one vintage.
pub const GET_FORECAST_OUTCOMES: &str = "Model.GetForecastOutcomes";

const INDEX_SCHEMA_VERSION: u32 = 5;
const FORECAST_PAYLOAD_SCHEMA_VERSION: u32 = 5;
const MAXIMUM_VINTAGES: usize = 100_000;
const MAXIMUM_OUTCOMES: usize = 1_000_000;
const MAXIMUM_DRIFT_OUTCOMES: usize = 4_096;
const FORECAST_SELECTION_POLICY_REVISION: u32 = 4;

/// Exact separately admitted AnalysisV1 product generation that authorized one forecast request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastAnalysisEvidence {
    manifest: DatasetManifestRef,
    production_identity_sha256: Sha256Digest,
    production_receipt_sha256: Sha256Digest,
    pairing_sha256: Sha256Digest,
}

impl ForecastAnalysisEvidence {
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        production_identity_sha256: Sha256Digest,
        production_receipt_sha256: Sha256Digest,
        pairing_sha256: Sha256Digest,
    ) -> Result<Self, ForecastApplicationError> {
        if [
            production_identity_sha256,
            production_receipt_sha256,
            pairing_sha256,
        ]
        .iter()
        .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        Ok(Self {
            manifest,
            production_identity_sha256,
            production_receipt_sha256,
            pairing_sha256,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn production_identity_sha256(&self) -> Sha256Digest {
        self.production_identity_sha256
    }

    pub(crate) const fn production_receipt_sha256(&self) -> Sha256Digest {
        self.production_receipt_sha256
    }

    pub(crate) const fn pairing_sha256(&self) -> Sha256Digest {
        self.pairing_sha256
    }
}

/// Exact label-free current-PIT input evidence retained independently from historical OOS
/// TrainingV1/AnalysisV1 calibration evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ForecastServingEvidence {
    manifest: DatasetManifestRef,
    source_id: SourceId,
    object_graph_sha256: Sha256Digest,
    selection_sha256: Sha256Digest,
    result_sha256: Sha256Digest,
    knowledge_cutoff: Timestamp,
    prior_observed_at: Timestamp,
    observed_through: Timestamp,
    feature_sha256: Sha256Digest,
}

impl ForecastServingEvidence {
    #[allow(
        clippy::too_many_arguments,
        reason = "manifest, query, cutoff, temporal, and feature identities remain explicit"
    )]
    pub(crate) fn try_new(
        manifest: DatasetManifestRef,
        source_id: SourceId,
        object_graph_sha256: Sha256Digest,
        selection_sha256: Sha256Digest,
        result_sha256: Sha256Digest,
        knowledge_cutoff: Timestamp,
        prior_observed_at: Timestamp,
        observed_through: Timestamp,
        feature_sha256: Sha256Digest,
    ) -> Result<Self, ForecastApplicationError> {
        if prior_observed_at >= observed_through
            || observed_through > knowledge_cutoff
            || [
                object_graph_sha256,
                selection_sha256,
                result_sha256,
                feature_sha256,
            ]
            .iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(ForecastApplicationError::InvalidRecord);
        }
        Ok(Self {
            manifest,
            source_id,
            object_graph_sha256,
            selection_sha256,
            result_sha256,
            knowledge_cutoff,
            prior_observed_at,
            observed_through,
            feature_sha256,
        })
    }

    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn object_graph_sha256(&self) -> Sha256Digest {
        self.object_graph_sha256
    }

    pub(crate) const fn selection_sha256(&self) -> Sha256Digest {
        self.selection_sha256
    }

    pub(crate) const fn result_sha256(&self) -> Sha256Digest {
        self.result_sha256
    }

    pub(crate) const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    pub(crate) const fn prior_observed_at(&self) -> Timestamp {
        self.prior_observed_at
    }

    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }

    pub(crate) const fn feature_sha256(&self) -> Sha256Digest {
        self.feature_sha256
    }
}

/// Stable newest-valid ordering used by the internal investment-workspace read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ForecastSelectionOrder {
    /// Newest publication, then freshest observation/input, then the lowest immutable identity.
    NewestCreatedAtObservedThroughAvailableAtThenLowestVintageId,
}

impl ForecastSelectionOrder {
    const fn canonical_bytes(self) -> &'static [u8] {
        match self {
            Self::NewestCreatedAtObservedThroughAvailableAtThenLowestVintageId => {
                b"newest_created_at_observed_through_available_at_then_lowest_vintage_id"
            }
        }
    }

    /// Stable operation-facing name included in the canonically identified receipt.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NewestCreatedAtObservedThroughAvailableAtThenLowestVintageId => {
                "newest_created_at_observed_through_available_at_then_lowest_vintage_id"
            }
        }
    }
}

/// Exact evidence shape applied before newest-valid ordering.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ForecastSelectionQualification {
    /// Every valid, available, published, and nonexpired vintage is eligible.
    AnyValid,
    /// Only complete calibrated conditional-mean terminal-price evidence at one exact horizon.
    ExactCalibratedConditionalMeanPrice { horizon_nanos: NonZeroU64 },
}

impl ForecastSelectionQualification {
    fn update_receipt_digest(self, digest: &mut Sha256) -> Result<(), ForecastApplicationError> {
        match self {
            Self::AnyValid => {
                update_receipt_digest_field(digest, b"qualification", b"any_valid")?;
            }
            Self::ExactCalibratedConditionalMeanPrice { horizon_nanos } => {
                update_receipt_digest_field(
                    digest,
                    b"qualification",
                    b"exact_calibrated_conditional_mean_price",
                )?;
                update_receipt_digest_field(
                    digest,
                    b"qualification_horizon_nanos",
                    &horizon_nanos.get().to_be_bytes(),
                )?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ForecastSelectionReceiptBody {
    policy_revision: u32,
    selection_order: ForecastSelectionOrder,
    qualification: ForecastSelectionQualification,
    instrument_id: InstrumentId,
    as_of_unix_nanos: i64,
    considered_vintage_count: usize,
    retained_vintage_hard_ceiling: usize,
    eligible_vintage_count: usize,
    competing_eligible_vintage_count: usize,
    selection_complete: bool,
    selected_vintage_id: String,
    selected_created_at_unix_nanos: i64,
    selected_observed_through_unix_nanos: i64,
    selected_available_at_unix_nanos: i64,
    selected_expires_at_unix_nanos: i64,
    selected_terminal_target_at_unix_nanos: Option<i64>,
    selected_analysis_pairing_sha256: String,
    selected_serving_feature_sha256: String,
}

/// Exact, canonically identified proof that the complete retained forecast set was considered.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ForecastSelectionReceipt {
    #[serde(flatten)]
    body: ForecastSelectionReceiptBody,
    receipt_digest: EvidenceDigest,
}

impl ForecastSelectionReceipt {
    fn try_new(body: ForecastSelectionReceiptBody) -> Result<Self, ForecastApplicationError> {
        let pairing = digest_from_hex(&body.selected_analysis_pairing_sha256)?;
        let serving = digest_from_hex(&body.selected_serving_feature_sha256)?;
        if pairing.bytes() == [0; 32] || serving.bytes() == [0; 32] {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let receipt_digest = forecast_selection_receipt_digest(&body)?;
        Ok(Self {
            body,
            receipt_digest,
        })
    }

    /// Code-owned selection-policy revision.
    #[must_use]
    pub(crate) const fn policy_revision(&self) -> u32 {
        self.body.policy_revision
    }

    /// Exact deterministic ordering applied to every eligible vintage.
    #[must_use]
    pub(crate) const fn selection_order(&self) -> ForecastSelectionOrder {
        self.body.selection_order
    }

    /// Exact evidence shape applied before newest-valid ordering.
    #[must_use]
    pub(crate) const fn qualification(&self) -> ForecastSelectionQualification {
        self.body.qualification
    }

    /// Exact requested instrument.
    #[must_use]
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.body.instrument_id
    }

    /// Exact point-in-time selection cutoff.
    #[must_use]
    pub(crate) const fn as_of_unix_nanos(&self) -> i64 {
        self.body.as_of_unix_nanos
    }

    /// Complete number of retained vintages examined before eligibility filtering.
    #[must_use]
    pub(crate) const fn considered_vintage_count(&self) -> usize {
        self.body.considered_vintage_count
    }

    /// Hard retained ceiling under which the complete selection was performed.
    #[must_use]
    pub(crate) const fn retained_vintage_hard_ceiling(&self) -> usize {
        self.body.retained_vintage_hard_ceiling
    }

    /// Number of exact-instrument, available, published, nonexpired vintages satisfying the
    /// recorded qualification.
    #[must_use]
    pub(crate) const fn eligible_vintage_count(&self) -> usize {
        self.body.eligible_vintage_count
    }

    /// Other qualification-eligible vintages that lost the deterministic ordering comparison.
    #[must_use]
    pub(crate) const fn competing_eligible_vintage_count(&self) -> usize {
        self.body.competing_eligible_vintage_count
    }

    /// Whether the receipt covers the complete retained set without truncation.
    #[must_use]
    pub(crate) const fn selection_complete(&self) -> bool {
        self.body.selection_complete
    }

    /// Immutable identity selected by the recorded deterministic order.
    #[must_use]
    pub(crate) fn selected_vintage_id(&self) -> &str {
        &self.body.selected_vintage_id
    }

    /// Exact selected publication time.
    #[must_use]
    pub(crate) const fn selected_created_at_unix_nanos(&self) -> i64 {
        self.body.selected_created_at_unix_nanos
    }

    /// Exact selected effective observation cutoff.
    #[must_use]
    pub(crate) const fn selected_observed_through_unix_nanos(&self) -> i64 {
        self.body.selected_observed_through_unix_nanos
    }

    /// Exact selected conservative knowledge time.
    #[must_use]
    pub(crate) const fn selected_available_at_unix_nanos(&self) -> i64 {
        self.body.selected_available_at_unix_nanos
    }

    /// Exact exclusive model-risk expiry.
    #[must_use]
    pub(crate) const fn selected_expires_at_unix_nanos(&self) -> i64 {
        self.body.selected_expires_at_unix_nanos
    }

    /// Exact terminal target bound into an exact-horizon receipt, absent for newest-any reads.
    #[must_use]
    pub(crate) const fn selected_terminal_target_at_unix_nanos(&self) -> Option<i64> {
        self.body.selected_terminal_target_at_unix_nanos
    }

    /// Exact TrainingV1→AnalysisV1 pairing selected with the immutable vintage.
    pub(crate) fn selected_analysis_pairing_sha256(
        &self,
    ) -> Result<Sha256Digest, ForecastApplicationError> {
        digest_from_hex(&self.body.selected_analysis_pairing_sha256)
    }

    /// Exact label-free current-PIT serving feature selected with the immutable vintage.
    pub(crate) fn selected_serving_feature_sha256(
        &self,
    ) -> Result<Sha256Digest, ForecastApplicationError> {
        digest_from_hex(&self.body.selected_serving_feature_sha256)
    }

    const fn is_exact_horizon_price_qualified(&self, requested_horizon_nanos: NonZeroU64) -> bool {
        matches!(
            self.body.qualification,
            ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
                horizon_nanos
            } if horizon_nanos.get() == requested_horizon_nanos.get()
        )
    }

    fn binds_live_terminal_target(&self, terminal_target: Timestamp) -> bool {
        self.body.selected_terminal_target_at_unix_nanos == Some(terminal_target.unix_nanos())
            && terminal_target.unix_nanos() > self.body.as_of_unix_nanos
            && self.body.selected_expires_at_unix_nanos <= terminal_target.unix_nanos()
    }

    /// Versioned SHA-256 identity of every canonical receipt field.
    #[must_use]
    pub(crate) const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

fn forecast_selection_receipt_digest(
    body: &ForecastSelectionReceiptBody,
) -> Result<EvidenceDigest, ForecastApplicationError> {
    let mut digest = Sha256::new();
    update_receipt_digest_field(
        &mut digest,
        b"domain",
        b"market-squawk/forecast-selection-receipt/v4",
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"policy_revision",
        &body.policy_revision.to_be_bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selection_order",
        body.selection_order.canonical_bytes(),
    )?;
    body.qualification.update_receipt_digest(&mut digest)?;
    let instrument_id = body.instrument_id.as_uuid();
    update_receipt_digest_field(&mut digest, b"instrument_id", instrument_id.as_bytes())?;
    update_receipt_digest_field(
        &mut digest,
        b"as_of_unix_nanos",
        &body.as_of_unix_nanos.to_be_bytes(),
    )?;
    update_receipt_digest_count(
        &mut digest,
        b"considered_vintage_count",
        body.considered_vintage_count,
    )?;
    update_receipt_digest_count(
        &mut digest,
        b"retained_vintage_hard_ceiling",
        body.retained_vintage_hard_ceiling,
    )?;
    update_receipt_digest_count(
        &mut digest,
        b"eligible_vintage_count",
        body.eligible_vintage_count,
    )?;
    update_receipt_digest_count(
        &mut digest,
        b"competing_eligible_vintage_count",
        body.competing_eligible_vintage_count,
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selection_complete",
        &[u8::from(body.selection_complete)],
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_vintage_id",
        body.selected_vintage_id.as_bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_created_at_unix_nanos",
        &body.selected_created_at_unix_nanos.to_be_bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_observed_through_unix_nanos",
        &body.selected_observed_through_unix_nanos.to_be_bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_available_at_unix_nanos",
        &body.selected_available_at_unix_nanos.to_be_bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_expires_at_unix_nanos",
        &body.selected_expires_at_unix_nanos.to_be_bytes(),
    )?;
    match body.selected_terminal_target_at_unix_nanos {
        Some(target_at_unix_nanos) => {
            update_receipt_digest_field(&mut digest, b"selected_terminal_target_present", &[1])?;
            update_receipt_digest_field(
                &mut digest,
                b"selected_terminal_target_at_unix_nanos",
                &target_at_unix_nanos.to_be_bytes(),
            )?;
        }
        None => {
            update_receipt_digest_field(&mut digest, b"selected_terminal_target_present", &[0])?
        }
    }
    update_receipt_digest_field(
        &mut digest,
        b"selected_analysis_pairing_sha256",
        &digest_from_hex(&body.selected_analysis_pairing_sha256)?.bytes(),
    )?;
    update_receipt_digest_field(
        &mut digest,
        b"selected_serving_feature_sha256",
        &digest_from_hex(&body.selected_serving_feature_sha256)?.bytes(),
    )?;
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn update_receipt_digest_count(
    digest: &mut Sha256,
    field: &[u8],
    value: usize,
) -> Result<(), ForecastApplicationError> {
    let value = u64::try_from(value).map_err(|_error| ForecastApplicationError::CorruptIndex)?;
    update_receipt_digest_field(digest, field, &value.to_be_bytes())
}

fn update_receipt_digest_field(
    digest: &mut Sha256,
    field: &[u8],
    value: &[u8],
) -> Result<(), ForecastApplicationError> {
    let field_length =
        u64::try_from(field.len()).map_err(|_error| ForecastApplicationError::CorruptIndex)?;
    let value_length =
        u64::try_from(value.len()).map_err(|_error| ForecastApplicationError::CorruptIndex)?;
    digest.update(field_length.to_be_bytes());
    digest.update(field);
    digest.update(value_length.to_be_bytes());
    digest.update(value);
    Ok(())
}

/// Whether the selected admitted forecast can supply governed price evidence.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ForecastPriceEvidence {
    /// Exact admitted conditional-mean terminal-price evidence.
    Available(Box<SelectedPriceForecast>),
    /// The selected vintage is valid forecast evidence, but cannot be interpreted as a price.
    Unavailable(SelectedForecastPriceUnavailable),
}

/// Closed reason that one valid selected forecast cannot supply price evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ForecastPriceUnavailableReason {
    /// The admitted model predicts a dimensionless return rather than a price.
    ReturnMeasurement,
    /// The admitted model predicts a probability rather than a price.
    ProbabilityMeasurement,
    /// The admitted regression output is explicitly not a price.
    OtherRegressionMeasurement,
    /// Exact rows do not prove one fixed positive terminal horizon.
    TerminalHorizonUnavailable,
    /// The sealed estimator output is not admitted as a conditional mean.
    CentralStatisticUnavailable,
}

/// Exact selected identity retained when price interpretation is unavailable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SelectedForecastPriceUnavailable {
    vintage_id: Sha256Digest,
    instrument_id: InstrumentId,
    output_binding_identity: Sha256Digest,
    analysis_evidence: ForecastAnalysisEvidence,
    serving_evidence: ForecastServingEvidence,
    reason: ForecastPriceUnavailableReason,
}

impl SelectedForecastPriceUnavailable {
    /// Exact selected immutable forecast identity.
    #[must_use]
    pub(crate) const fn vintage_id(&self) -> Sha256Digest {
        self.vintage_id
    }

    /// Exact instrument that was selected.
    #[must_use]
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Exact admitted binding identity retained for every current measurement.
    #[must_use]
    pub(crate) const fn output_binding_identity(&self) -> Sha256Digest {
        self.output_binding_identity
    }

    /// Exact immutable AnalysisV1 and TrainingV1→AnalysisV1 pairing evidence.
    #[must_use]
    pub(crate) const fn analysis_evidence(&self) -> &ForecastAnalysisEvidence {
        &self.analysis_evidence
    }

    /// Exact immutable label-free serving input and current-PIT selection evidence.
    #[must_use]
    pub(crate) const fn serving_evidence(&self) -> &ForecastServingEvidence {
        &self.serving_evidence
    }

    /// Closed non-price reason; callers must surface this as unavailable, never as a price.
    #[must_use]
    pub(crate) const fn reason(&self) -> ForecastPriceUnavailableReason {
        self.reason
    }
}

/// One exact fixed-scale interval in the selected forecast currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedPriceInterval {
    lower: ForecastValue,
    upper: ForecastValue,
}

impl SelectedPriceInterval {
    /// Inclusive lower value.
    #[must_use]
    pub(crate) const fn lower(self) -> ForecastValue {
        self.lower
    }

    /// Inclusive upper value.
    #[must_use]
    pub(crate) const fn upper(self) -> ForecastValue {
        self.upper
    }
}

/// Exact calibrated 50/80/95 price intervals for one future point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedPriceIntervals {
    interval_50: SelectedPriceInterval,
    interval_80: SelectedPriceInterval,
    interval_95: SelectedPriceInterval,
}

impl SelectedPriceIntervals {
    /// Realized-marginal 50-percent interval; not a per-observation guarantee.
    #[must_use]
    pub(crate) const fn interval_50(self) -> SelectedPriceInterval {
        self.interval_50
    }

    /// Realized-marginal 80-percent interval; not a per-observation guarantee.
    #[must_use]
    pub(crate) const fn interval_80(self) -> SelectedPriceInterval {
        self.interval_80
    }

    /// Realized-marginal 95-percent interval; not a per-observation guarantee.
    #[must_use]
    pub(crate) const fn interval_95(self) -> SelectedPriceInterval {
        self.interval_95
    }
}

/// One typed future price point from the selected immutable vintage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedPriceForecastPoint {
    target_at: Timestamp,
    central: ForecastValue,
    intervals: Option<SelectedPriceIntervals>,
}

impl SelectedPriceForecastPoint {
    /// Exact future effective time.
    #[must_use]
    pub(crate) const fn target_at(self) -> Timestamp {
        self.target_at
    }

    /// Exact fixed-scale modeled price in the enclosing forecast currency.
    #[must_use]
    pub(crate) const fn central(self) -> ForecastValue {
        self.central
    }

    /// Calibrated intervals when exact admitted calibration evidence exists.
    #[must_use]
    pub(crate) const fn intervals(self) -> Option<SelectedPriceIntervals> {
        self.intervals
    }
}

/// Complete typed price evidence from one selected and model-revalidated forecast vintage.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SelectedPriceForecast {
    vintage_id: Sha256Digest,
    instrument_id: InstrumentId,
    currency: Currency,
    central_statistic: ForecastCentralStatistic,
    terminal_horizon_nanos: NonZeroU64,
    observed_through: Timestamp,
    available_at: Timestamp,
    created_at: Timestamp,
    expires_at: Timestamp,
    output_binding_identity: Sha256Digest,
    analysis_evidence: ForecastAnalysisEvidence,
    serving_evidence: ForecastServingEvidence,
    model_metadata: ModelMetadata,
    forecast_artifact: ArtifactReference,
    points: Box<[SelectedPriceForecastPoint]>,
    calibration: Option<CalibrationEvidence>,
}

impl SelectedPriceForecast {
    /// Exact selected immutable forecast identity.
    #[must_use]
    pub(crate) const fn vintage_id(&self) -> Sha256Digest {
        self.vintage_id
    }

    /// Exact forecasted instrument.
    #[must_use]
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Exact model-admitted quote currency for every returned point.
    #[must_use]
    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }

    /// Exact admitted statistical meaning of every central point.
    #[must_use]
    pub(crate) const fn central_statistic(&self) -> ForecastCentralStatistic {
        self.central_statistic
    }

    /// Exact positive effective-time offset of the terminal-price label.
    #[must_use]
    pub(crate) const fn terminal_horizon_nanos(&self) -> NonZeroU64 {
        self.terminal_horizon_nanos
    }

    /// Effective cutoff of the source-qualified observed series.
    #[must_use]
    pub(crate) const fn observed_through(&self) -> Timestamp {
        self.observed_through
    }

    /// Conservative knowledge time of the complete forecast input.
    #[must_use]
    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Publication time of the immutable vintage.
    #[must_use]
    pub(crate) const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    /// Exclusive model-risk expiry time used by selection.
    #[must_use]
    pub(crate) const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Versioned model-admission identity that authorizes price interpretation.
    #[must_use]
    pub(crate) const fn output_binding_identity(&self) -> Sha256Digest {
        self.output_binding_identity
    }

    /// Exact immutable AnalysisV1 and TrainingV1→AnalysisV1 pairing evidence.
    #[must_use]
    pub(crate) const fn analysis_evidence(&self) -> &ForecastAnalysisEvidence {
        &self.analysis_evidence
    }

    /// Exact immutable label-free serving input and current-PIT selection evidence.
    #[must_use]
    pub(crate) const fn serving_evidence(&self) -> &ForecastServingEvidence {
        &self.serving_evidence
    }

    /// Exact reloaded admitted model metadata matched against the durable payload.
    #[must_use]
    pub(crate) const fn model_metadata(&self) -> &ModelMetadata {
        &self.model_metadata
    }

    /// Exact path-free controlled forecast artifact reference.
    #[must_use]
    pub(crate) const fn forecast_artifact(&self) -> &ArtifactReference {
        &self.forecast_artifact
    }

    /// Complete ordered future price path.
    #[must_use]
    pub(crate) fn points(&self) -> &[SelectedPriceForecastPoint] {
        &self.points
    }

    /// Exact reconstructed admitted calibration evidence, when intervals exist.
    #[must_use]
    pub(crate) const fn calibration(&self) -> Option<&CalibrationEvidence> {
        self.calibration.as_ref()
    }
}

/// Complete selected typed forecast evidence and the authority-owned selection receipt.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LatestValidForecast {
    price_evidence: ForecastPriceEvidence,
    selection_receipt: ForecastSelectionReceipt,
    model_metadata: ModelMetadata,
    forecast_artifact: ArtifactReference,
}

/// Exact typed reason one selected forecast cannot satisfy a requested price horizon.
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactHorizonPriceForecastUnavailableReason {
    /// The receipt came from the generic newest-valid research selector.
    SelectionNotExactHorizonQualified,
    /// The selected model binding is not admitted as conditional-mean terminal-price evidence.
    PriceEvidenceUnavailable(ForecastPriceUnavailableReason),
    /// The admitted model predicts a different exact terminal horizon.
    HorizonMismatch { selected_horizon_nanos: NonZeroU64 },
    /// The selected price forecast has no admitted interval-calibration evidence.
    CalibrationUnavailable,
}

/// Identity-bound refusal to reinterpret a selected vintage at a caller-requested horizon.
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactHorizonPriceForecastUnavailable {
    requested_horizon_nanos: NonZeroU64,
    vintage_id: Sha256Digest,
    instrument_id: InstrumentId,
    output_binding_identity: Sha256Digest,
    selection_receipt_digest: EvidenceDigest,
    reason: ExactHorizonPriceForecastUnavailableReason,
}

#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
impl ExactHorizonPriceForecastUnavailable {
    /// Exact positive horizon requested by the analysis policy.
    #[must_use]
    pub(crate) const fn requested_horizon_nanos(self) -> NonZeroU64 {
        self.requested_horizon_nanos
    }

    /// Immutable selected vintage that could not satisfy the requested interpretation.
    #[must_use]
    pub(crate) const fn vintage_id(self) -> Sha256Digest {
        self.vintage_id
    }

    /// Exact selected instrument.
    #[must_use]
    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Model-admission identity retained even when the requested projection is unavailable.
    #[must_use]
    pub(crate) const fn output_binding_identity(self) -> Sha256Digest {
        self.output_binding_identity
    }

    /// Complete newest-valid selection identity.
    #[must_use]
    pub(crate) const fn selection_receipt_digest(self) -> EvidenceDigest {
        self.selection_receipt_digest
    }

    /// Closed reason this vintage cannot be used at the requested horizon.
    #[must_use]
    pub(crate) const fn reason(self) -> ExactHorizonPriceForecastUnavailableReason {
        self.reason
    }
}

/// One exact calibrated conditional-mean terminal-price projection.
///
/// This is research evidence only. It grants no proposal, sizing, risk, order, or execution
/// authority and does not reinterpret generic regression output or an approximate horizon.
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct ExactHorizonPriceForecastProjection<'forecast> {
    price: &'forecast SelectedPriceForecast,
    selection_receipt: &'forecast ForecastSelectionReceipt,
    terminal: SelectedPriceForecastPoint,
    intervals: SelectedPriceIntervals,
    calibration: &'forecast CalibrationEvidence,
}

#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
impl ExactHorizonPriceForecastProjection<'_> {
    /// Exact immutable forecast-vintage identity.
    #[must_use]
    pub(crate) const fn vintage_id(self) -> Sha256Digest {
        self.price.vintage_id()
    }

    /// Exact forecasted instrument.
    #[must_use]
    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.price.instrument_id()
    }

    /// Quote currency of the modeled price and every interval bound.
    #[must_use]
    pub(crate) const fn currency(self) -> Currency {
        self.price.currency()
    }

    /// Exact positive terminal horizon admitted by the model and requested by the caller.
    #[must_use]
    pub(crate) const fn terminal_horizon_nanos(self) -> NonZeroU64 {
        self.price.terminal_horizon_nanos()
    }

    /// Exact future effective coordinate of the single terminal point.
    #[must_use]
    pub(crate) const fn terminal_at(self) -> Timestamp {
        self.terminal.target_at()
    }

    /// Model-estimated conditional-mean terminal price.
    #[must_use]
    pub(crate) const fn terminal_mean(self) -> ForecastValue {
        self.terminal.central()
    }

    /// Complete nested marginal-coverage intervals at 50, 80, and 95 percent.
    #[must_use]
    pub(crate) const fn intervals(self) -> SelectedPriceIntervals {
        self.intervals
    }

    /// Complete admitted interval-calibration identity.
    #[must_use]
    pub(crate) const fn calibration_identity(self) -> Sha256Digest {
        self.calibration.identity()
    }

    /// Model-output admission identity authorizing the price/mean/horizon interpretation.
    #[must_use]
    pub(crate) const fn output_binding_identity(self) -> Sha256Digest {
        self.price.output_binding_identity()
    }

    /// Exact complete-set newest-valid selection identity.
    #[must_use]
    pub(crate) const fn selection_receipt_digest(self) -> EvidenceDigest {
        self.selection_receipt.receipt_digest()
    }

    /// Exact point-in-time selection cutoff.
    #[must_use]
    pub(crate) const fn selected_as_of(self) -> Timestamp {
        Timestamp::from_unix_nanos(self.selection_receipt.as_of_unix_nanos())
    }

    /// Effective cutoff of the source-qualified observed series.
    #[must_use]
    pub(crate) const fn observed_through(self) -> Timestamp {
        self.price.observed_through()
    }

    /// Conservative knowledge time of the complete forecast input.
    #[must_use]
    pub(crate) const fn available_at(self) -> Timestamp {
        self.price.available_at()
    }

    /// Immutable publication time.
    #[must_use]
    pub(crate) const fn created_at(self) -> Timestamp {
        self.price.created_at()
    }

    /// Exclusive model-risk expiry used by selection.
    #[must_use]
    pub(crate) const fn expires_at(self) -> Timestamp {
        self.price.expires_at()
    }
}

#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
impl<'forecast> ExactHorizonPriceForecastProjection<'forecast> {
    /// Complete authority-owned newest-valid selection receipt.
    #[must_use]
    pub(crate) const fn selection_receipt(self) -> &'forecast ForecastSelectionReceipt {
        self.selection_receipt
    }

    /// Complete bundle-, artifact-, window-, residual-, band-, and coverage-bound calibration.
    #[must_use]
    pub(crate) const fn calibration(self) -> &'forecast CalibrationEvidence {
        self.calibration
    }

    /// Exact reloaded model metadata used to revalidate this vintage.
    #[must_use]
    pub(crate) const fn model_metadata(self) -> &'forecast ModelMetadata {
        self.price.model_metadata()
    }

    /// Path-free controlled forecast artifact verified before selection returned.
    #[must_use]
    pub(crate) const fn forecast_artifact(self) -> &'forecast ArtifactReference {
        self.price.forecast_artifact()
    }
}

/// Exact calibrated terminal-price evidence or an identity-bound typed refusal.
#[allow(
    dead_code,
    reason = "a generic analysis consumer uses this at the next composition seam"
)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ExactHorizonPriceForecastEvidence<'forecast> {
    Available(ExactHorizonPriceForecastProjection<'forecast>),
    Unavailable(ExactHorizonPriceForecastUnavailable),
}

impl LatestValidForecast {
    /// Typed price evidence or an explicit non-price/unavailable result.
    #[must_use]
    pub(crate) const fn price_evidence(&self) -> &ForecastPriceEvidence {
        &self.price_evidence
    }

    /// Exact non-truncated selection proof.
    #[must_use]
    pub(crate) const fn selection_receipt(&self) -> &ForecastSelectionReceipt {
        &self.selection_receipt
    }

    /// Exact reloaded model authority common to available and unavailable price evidence.
    #[must_use]
    pub(crate) const fn model_metadata(&self) -> &ModelMetadata {
        &self.model_metadata
    }

    /// Exact controlled artifact read and verified before either result variant is returned.
    #[must_use]
    pub(crate) const fn forecast_artifact(&self) -> &ArtifactReference {
        &self.forecast_artifact
    }

    /// Projects only an exact calibrated conditional-mean price at the requested horizon.
    ///
    /// A different admitted horizon, a non-price model, or absent calibration remains explicitly
    /// unavailable. Internal identity or chronology drift is treated as corrupt retained state.
    #[allow(
        dead_code,
        reason = "a generic analysis consumer uses this at the next composition seam"
    )]
    pub(crate) fn exact_horizon_price_projection(
        &self,
        requested_horizon_nanos: NonZeroU64,
    ) -> Result<ExactHorizonPriceForecastEvidence<'_>, ForecastApplicationError> {
        let (vintage_id, instrument_id, output_binding_identity) = match &self.price_evidence {
            ForecastPriceEvidence::Available(price) => (
                price.vintage_id(),
                price.instrument_id(),
                price.output_binding_identity(),
            ),
            ForecastPriceEvidence::Unavailable(unavailable) => (
                unavailable.vintage_id(),
                unavailable.instrument_id(),
                unavailable.output_binding_identity(),
            ),
        };
        let unavailable = |reason| {
            ExactHorizonPriceForecastEvidence::Unavailable(ExactHorizonPriceForecastUnavailable {
                requested_horizon_nanos,
                vintage_id,
                instrument_id,
                output_binding_identity,
                selection_receipt_digest: self.selection_receipt.receipt_digest(),
                reason,
            })
        };
        if !self
            .selection_receipt
            .is_exact_horizon_price_qualified(requested_horizon_nanos)
        {
            let reason = match self.selection_receipt.qualification() {
                ForecastSelectionQualification::AnyValid => {
                    ExactHorizonPriceForecastUnavailableReason::SelectionNotExactHorizonQualified
                }
                ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice {
                    horizon_nanos,
                } => ExactHorizonPriceForecastUnavailableReason::HorizonMismatch {
                    selected_horizon_nanos: horizon_nanos,
                },
            };
            return Ok(unavailable(reason));
        }
        let ForecastPriceEvidence::Available(price) = &self.price_evidence else {
            let ForecastPriceEvidence::Unavailable(price) = &self.price_evidence else {
                return Err(ForecastApplicationError::CorruptIndex);
            };
            return Ok(unavailable(
                ExactHorizonPriceForecastUnavailableReason::PriceEvidenceUnavailable(
                    price.reason(),
                ),
            ));
        };
        if price.terminal_horizon_nanos() != requested_horizon_nanos {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let Some(calibration) = price.calibration() else {
            return Ok(unavailable(
                ExactHorizonPriceForecastUnavailableReason::CalibrationUnavailable,
            ));
        };
        let [terminal] = price.points() else {
            return Err(ForecastApplicationError::CorruptIndex);
        };
        let expected_target = price
            .observed_through()
            .checked_add_nanos(
                i64::try_from(requested_horizon_nanos.get())
                    .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            )
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let intervals = terminal
            .intervals()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        // Persistence already admitted this object through `verify_forecast_vintage_identity`,
        // including its model/cutoff calibration match. Reconstruct it again here so this narrow
        // projection independently rejects any locally retained calibration drift.
        let revalidated_calibration = CalibrationEvidence::try_new(
            price.model_metadata(),
            calibration.method(),
            calibration.window(),
            calibration.policy_hash(),
            calibration.residuals_hash(),
            *calibration.bands(),
            calibration.dependence_assumptions(),
        )
        .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let nested = intervals.interval_95().lower() <= intervals.interval_80().lower()
            && intervals.interval_80().lower() <= intervals.interval_50().lower()
            && intervals.interval_50().lower() <= terminal.central()
            && terminal.central() <= intervals.interval_50().upper()
            && intervals.interval_50().upper() <= intervals.interval_80().upper()
            && intervals.interval_80().upper() <= intervals.interval_95().upper();
        if price.central_statistic() != ForecastCentralStatistic::ModelEstimatedConditionalMean
            || terminal.target_at() != expected_target
            || !self
                .selection_receipt
                .binds_live_terminal_target(terminal.target_at())
            || !nested
            || calibration.identity().bytes() == [0; 32]
            || &revalidated_calibration != calibration
            || calibration.window().end() > price.observed_through()
            || calibration.bands()[0].coverage() != ForecastCoverage::Fifty
            || calibration.bands()[1].coverage() != ForecastCoverage::Eighty
            || calibration.bands()[2].coverage() != ForecastCoverage::NinetyFive
            || !self.selection_receipt.selection_complete()
            || self.selection_receipt.instrument_id() != instrument_id
            || self.selection_receipt.selected_vintage_id() != hex(vintage_id.bytes())
            || self
                .selection_receipt
                .selected_observed_through_unix_nanos()
                != price.observed_through().unix_nanos()
            || self.selection_receipt.selected_available_at_unix_nanos()
                != price.available_at().unix_nanos()
            || self.selection_receipt.selected_created_at_unix_nanos()
                != price.created_at().unix_nanos()
            || self.selection_receipt.selected_expires_at_unix_nanos()
                != price.expires_at().unix_nanos()
            || price.model_metadata().metadata_hash() != self.model_metadata.metadata_hash()
            || price.forecast_artifact() != &self.forecast_artifact
        {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        Ok(ExactHorizonPriceForecastEvidence::Available(
            ExactHorizonPriceForecastProjection {
                price,
                selection_receipt: &self.selection_receipt,
                terminal: *terminal,
                intervals,
                calibration,
            },
        ))
    }
}

/// Least-authority lifecycle and complete-object ceiling for one typed forecast evidence read.
#[derive(Clone, Debug)]
pub(crate) struct ForecastEvidenceReadContext {
    artifact: ArtifactReadContext,
    maximum_artifact_bytes: NonZeroUsize,
}

impl ForecastEvidenceReadContext {
    /// Binds service-owned cancellation/deadline authority to the admitted complete-object bound.
    #[must_use]
    pub(crate) const fn new(
        artifact: ArtifactReadContext,
        maximum_artifact_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            artifact,
            maximum_artifact_bytes,
        }
    }

    fn ensure_live(&self) -> Result<(), ArtifactError> {
        self.artifact.ensure_live()
    }
}

/// Least-authority typed forecast read retained before model service trait erasure.
#[async_trait]
pub(crate) trait ForecastEvidenceReader: Send + Sync {
    /// Selects one exact nonexpired vintage and verifies its model and controlled artifact.
    async fn latest_valid_for_instrument(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        context: ForecastEvidenceReadContext,
    ) -> Result<LatestValidForecast, ForecastApplicationError>;

    /// Selects the newest exact calibrated conditional-mean terminal-price vintage.
    async fn latest_valid_exact_horizon_price_for_instrument(
        &self,
        instrument_id: InstrumentId,
        requested_horizon_nanos: NonZeroU64,
        as_of: Timestamp,
        context: ForecastEvidenceReadContext,
    ) -> Result<LatestValidForecast, ForecastApplicationError>;
}

/// Closed storage and result ceilings for one installed forecast authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ForecastApplicationLimits {
    maximum_vintages: NonZeroUsize,
    maximum_outcomes: NonZeroUsize,
    maximum_index_bytes: NonZeroUsize,
}

impl ForecastApplicationLimits {
    /// Constructs hard retained-index ceilings.
    pub fn try_new(
        maximum_vintages: NonZeroUsize,
        maximum_outcomes: NonZeroUsize,
        maximum_index_bytes: NonZeroUsize,
    ) -> Result<Self, ForecastApplicationError> {
        if maximum_vintages.get() > MAXIMUM_VINTAGES
            || maximum_outcomes.get() > MAXIMUM_OUTCOMES
            || maximum_index_bytes.get() > LocalAuthorityStateStore::maximum_payload_bytes()
        {
            return Err(ForecastApplicationError::InvalidLimits);
        }
        Ok(Self {
            maximum_vintages,
            maximum_outcomes,
            maximum_index_bytes,
        })
    }

    /// Maximum result rows a caller may request from this authority.
    #[must_use]
    pub const fn maximum_vintages(self) -> NonZeroUsize {
        self.maximum_vintages
    }
}

/// Sole append authority for durable immutable forecast records.
pub struct ForecastApplicationService {
    store: LocalAuthorityStateStore,
    index: Mutex<ForecastIndex>,
    publication: Mutex<()>,
    artifacts: Arc<dyn ArtifactRepository>,
    limits: ForecastApplicationLimits,
}

pub(super) struct RetainedForecastBackup {
    pub(super) runtime: RetainedRuntimeBackup,
    pub(super) canonical_index: Box<[u8]>,
    pub(super) artifact_references: Vec<ArtifactReference>,
}

impl std::fmt::Debug for RetainedForecastBackup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedForecastBackup")
            .field("runtime", &self.runtime)
            .field("canonical_index", &"[CANONICAL FORECAST INDEX]")
            .field("artifact_count", &self.artifact_references.len())
            .finish()
    }
}

impl ForecastApplicationService {
    /// Opens and semantically verifies the complete durable forecast index.
    pub fn try_open(
        root: impl AsRef<Path>,
        artifacts: Arc<dyn ArtifactRepository>,
        limits: ForecastApplicationLimits,
    ) -> Result<Self, ForecastApplicationError> {
        let store = LocalAuthorityStateStore::try_open(root)?;
        let index = match store.load()? {
            Some(payload) => serde_json::from_slice::<ForecastIndex>(&payload)
                .map_err(|_error| ForecastApplicationError::CorruptIndex)?,
            None => ForecastIndex::default(),
        };
        index.validate(limits)?;
        Ok(Self {
            store,
            index: Mutex::new(index),
            publication: Mutex::new(()),
            artifacts,
            limits,
        })
    }

    pub(super) fn artifact_repository(&self) -> Arc<dyn ArtifactRepository> {
        Arc::clone(&self.artifacts)
    }

    pub(super) const fn backup_limits(&self) -> ForecastApplicationLimits {
        self.limits
    }

    /// Publishes one complete path, then durably appends its immutable vintage record.
    ///
    /// The request digest makes an exact retry return the already committed vintage. Publication
    /// is serialized so concurrent copies cannot create competing wall-clock vintages. The
    /// artifact commits first; an index failure may leave an unreachable content-addressed object,
    /// but never a vintage that references missing payload bytes.
    pub async fn publish_vintage(
        &self,
        request_hash: Sha256Digest,
        path: ForecastPath,
        analysis_evidence: ForecastAnalysisEvidence,
        serving_evidence: ForecastServingEvidence,
        created_at: market_squawk_domain::Timestamp,
        expires_at: market_squawk_domain::Timestamp,
        context: ArtifactPublicationContext,
    ) -> Result<Value, ForecastApplicationError> {
        let _publication = self.publication.lock().await;
        if let Some(existing) = self.vintage_for_request(request_hash).await {
            return self.get_forecast_by_identity(&existing.vintage_id).await;
        }
        context.ensure_live()?;
        let payload = ForecastPayloadRecord::from_path(
            &path,
            &analysis_evidence,
            &serving_evidence,
            created_at,
            expires_at,
        )?;
        let publication = ArtifactPublication::try_json(
            serde_json::to_vec(&payload)
                .map_err(|_error| ForecastApplicationError::InvalidRecord)?,
        )?;
        let artifact = self.artifacts.publish(publication, context.clone()).await?;
        context.ensure_live()?;
        let artifact_hash = digest_from_hex(artifact.sha256())?;
        let vintage = ForecastVintage::try_new(path, created_at, expires_at, artifact_hash)
            .map_err(|_error| ForecastApplicationError::InvalidRecord)?;
        let record = VintageRecord::from_publication(request_hash, &vintage, payload, &artifact)?;
        self.commit(|index| {
            match index
                .vintages
                .iter()
                .find(|existing| existing.request_hash == record.request_hash)
            {
                Some(existing) if existing == &record => return Ok(false),
                Some(_) => return Err(ForecastApplicationError::Conflict),
                None => {}
            }
            if index.vintages.len() >= self.limits.maximum_vintages.get() {
                return Err(ForecastApplicationError::Capacity);
            }
            index.vintages.push(record.clone());
            Ok(true)
        })
        .await?;
        self.get_forecast_by_identity(&record.vintage_id).await
    }

    /// Appends one realized outcome without mutating the referenced vintage.
    pub async fn append_outcome(
        &self,
        outcome: &ForecastOutcome,
    ) -> Result<(), ForecastApplicationError> {
        self.commit(|index| {
            let vintage_id = hex(outcome.vintage_id().bytes());
            let vintage = index
                .vintages
                .iter()
                .find(|value| value.vintage_id == vintage_id)
                .ok_or(ForecastApplicationError::NotFound)?;
            let record = OutcomeRecord::from_outcome(outcome, vintage)?;
            match index.outcomes.iter().find(|existing| *existing == &record) {
                Some(_) => return Ok(false),
                None if index
                    .outcomes
                    .iter()
                    .any(|existing| existing.id() == record.id()) =>
                {
                    return Err(ForecastApplicationError::Conflict);
                }
                None => {}
            }
            if index.outcomes.len() >= self.limits.maximum_outcomes.get() {
                return Err(ForecastApplicationError::Capacity);
            }
            index.outcomes.push(record);
            Ok(true)
        })
        .await
    }

    /// Returns an investment-facing projection for one opaque forecast token.
    pub async fn get_forecast(&self, token: &str) -> Result<Value, ForecastApplicationError> {
        let token = Uuid::parse_str(token).map_err(|_| ForecastApplicationError::InvalidRecord)?;
        let index = self.index.lock().await;
        let vintage = product_vintage(&index, token)?;
        vintage.product_detail(drift_monitoring_value(&index, vintage)?)
    }

    /// Lists newest stored vintages first under the lower caller/storage ceiling.
    pub async fn list_forecasts(
        &self,
        maximum: NonZeroUsize,
    ) -> Result<Value, ForecastApplicationError> {
        let maximum = maximum.get().min(self.limits.maximum_vintages.get());
        let index = self.index.lock().await;
        let available = index.vintages.len();
        let records = index
            .vintages
            .iter()
            .rev()
            .take(maximum)
            .map(VintageRecord::product_summary)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "forecasts": records,
            "available": available,
            "truncated": records.len() < available,
        }))
    }

    /// Returns immutable stored outcomes for one exact vintage.
    pub async fn get_forecast_outcomes(
        &self,
        token: &str,
        maximum: NonZeroUsize,
    ) -> Result<Value, ForecastApplicationError> {
        let token = Uuid::parse_str(token).map_err(|_| ForecastApplicationError::InvalidRecord)?;
        let index = self.index.lock().await;
        let vintage = product_vintage(&index, token)?;
        let available = index
            .outcomes
            .iter()
            .filter(|value| value.vintage_id == vintage.vintage_id)
            .count();
        let outcomes = index
            .outcomes
            .iter()
            .filter(|value| value.vintage_id == vintage.vintage_id)
            .take(maximum.get().min(self.limits.maximum_outcomes.get()))
            .map(OutcomeRecord::product_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "forecastToken": token,
            "outcomes": outcomes,
            "available": available,
            "truncated": outcomes.len() < available,
        }))
    }

    async fn get_forecast_by_identity(
        &self,
        identity: &str,
    ) -> Result<Value, ForecastApplicationError> {
        let index = self.index.lock().await;
        let vintage = index
            .vintages
            .iter()
            .find(|value| value.vintage_id == identity)
            .ok_or(ForecastApplicationError::NotFound)?;
        vintage.product_detail(drift_monitoring_value(&index, vintage)?)
    }

    pub(super) async fn retain_backup_with_runtime(
        &self,
        runtime: Option<&ProductionModelRuntime>,
        runtime_limits: ProductionModelRuntimeLimits,
    ) -> Result<RetainedForecastBackup, ForecastBackupCaptureError> {
        let index = self.index.lock().await;
        let canonical_index = index.canonical_bytes(self.limits)?.into_boxed_slice();
        let artifact_references = index.artifact_references()?;
        let runtime = match runtime {
            Some(runtime) => runtime.retain_backup()?,
            None => ProductionModelRuntime::empty_backup(runtime_limits)?,
        };
        let runtime_coordinates = runtime
            .models
            .iter()
            .map(|(coordinate, _bundle)| {
                (
                    coordinate.model_id.to_string(),
                    coordinate.bundle_id.as_str().to_owned(),
                    coordinate.bundle_version.get(),
                )
            })
            .collect::<Vec<_>>();
        if index.model_coordinates().any(|coordinate| {
            !runtime_coordinates.iter().any(|candidate| {
                candidate.0 == coordinate.0
                    && candidate.1 == coordinate.1
                    && candidate.2 == coordinate.2
            })
        }) {
            return Err(ForecastBackupCaptureError::ModelCoordinateMismatch);
        }
        Ok(RetainedForecastBackup {
            runtime,
            canonical_index,
            artifact_references,
        })
    }

    pub(super) fn stage_backup_index(
        root: impl AsRef<Path>,
        canonical_index: &[u8],
        expected_artifacts: &[ArtifactReference],
        limits: ForecastApplicationLimits,
    ) -> Result<(), ForecastApplicationError> {
        let index = ForecastIndex::decode_canonical(canonical_index, limits)?;
        if index.artifact_references()? != expected_artifacts {
            return Err(ForecastApplicationError::CorruptIndex);
        }
        let store = LocalAuthorityStateStore::try_open(root)?;
        if store.load()?.is_some() {
            return Err(ForecastApplicationError::RestoreTargetNotFresh);
        }
        store.store(canonical_index)?;
        Ok(())
    }

    async fn vintage_for_request(&self, request_hash: Sha256Digest) -> Option<VintageRecord> {
        let request_hash = hex(request_hash.bytes());
        self.index
            .lock()
            .await
            .vintages
            .iter()
            .find(|value| value.request_hash == request_hash)
            .cloned()
    }

    async fn commit(
        &self,
        change: impl FnOnce(&mut ForecastIndex) -> Result<bool, ForecastApplicationError>,
    ) -> Result<(), ForecastApplicationError> {
        let mut index = self.index.lock().await;
        let mut candidate = index.clone();
        if !change(&mut candidate)? {
            return Ok(());
        }
        candidate.validate(self.limits)?;
        let payload = serde_json::to_vec(&candidate)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        if payload.len() > self.limits.maximum_index_bytes.get() {
            return Err(ForecastApplicationError::Capacity);
        }
        self.store.store(&payload)?;
        *index = candidate;
        Ok(())
    }
}

#[async_trait]
impl ForecastEvidenceReader for ModelDomainService {
    async fn latest_valid_for_instrument(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        context: ForecastEvidenceReadContext,
    ) -> Result<LatestValidForecast, ForecastApplicationError> {
        let forecasts = self
            .forecasts
            .as_ref()
            .ok_or(ForecastApplicationError::Unavailable)?;
        context.ensure_live()?;
        let selected = {
            let index = forecasts.index.lock().await;
            context.ensure_live()?;
            let selected = index.latest_valid_for_instrument(
                instrument_id,
                as_of,
                forecasts.limits.maximum_vintages,
            )?;
            context.ensure_live()?;
            selected
        };
        read_forecast_index_selection(self, forecasts, selected, context).await
    }

    async fn latest_valid_exact_horizon_price_for_instrument(
        &self,
        instrument_id: InstrumentId,
        requested_horizon_nanos: NonZeroU64,
        as_of: Timestamp,
        context: ForecastEvidenceReadContext,
    ) -> Result<LatestValidForecast, ForecastApplicationError> {
        let forecasts = self
            .forecasts
            .as_ref()
            .ok_or(ForecastApplicationError::Unavailable)?;
        context.ensure_live()?;
        let selected = {
            let index = forecasts.index.lock().await;
            context.ensure_live()?;
            let selected = index.latest_valid_exact_horizon_price_for_instrument(
                instrument_id,
                requested_horizon_nanos,
                as_of,
                forecasts.limits.maximum_vintages,
            )?;
            context.ensure_live()?;
            selected
        };
        read_forecast_index_selection(self, forecasts, selected, context).await
    }
}

async fn read_forecast_index_selection(
    service: &ModelDomainService,
    forecasts: &ForecastApplicationService,
    selected: ForecastIndexSelection,
    context: ForecastEvidenceReadContext,
) -> Result<LatestValidForecast, ForecastApplicationError> {
    let image = service.read_image.load();
    let (model_id, bundle_id, bundle_version) = selected.vintage.typed_model_coordinate()?;
    let bundle = image
        .registry
        .get(&bundle_id, bundle_version)
        .map_err(|_error| ForecastApplicationError::Unavailable)?
        .ok_or(ForecastApplicationError::Unavailable)?;
    let metadata = bundle.metadata();
    if metadata.model_id() != model_id {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let reference = selected.vintage.artifact_reference()?;
    let artifact = forecasts
        .artifacts
        .read(
            ArtifactReadRequest::try_new(reference.clone(), context.maximum_artifact_bytes)?,
            context.artifact.clone(),
        )
        .await?;
    context.ensure_live()?;
    selected.vintage.verify_artifact_read(&artifact)?;
    let price_evidence = selected.vintage.revalidated_price_evidence(metadata)?;
    let (selected_pairing, selected_serving_feature) = match &price_evidence {
        ForecastPriceEvidence::Available(evidence) => (
            evidence.analysis_evidence().pairing_sha256(),
            evidence.serving_evidence().feature_sha256(),
        ),
        ForecastPriceEvidence::Unavailable(evidence) => (
            evidence.analysis_evidence().pairing_sha256(),
            evidence.serving_evidence().feature_sha256(),
        ),
    };
    if selected.receipt.selected_analysis_pairing_sha256()? != selected_pairing
        || selected.receipt.selected_serving_feature_sha256()? != selected_serving_feature
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    let selected = LatestValidForecast {
        price_evidence,
        selection_receipt: selected.receipt,
        model_metadata: metadata.clone(),
        forecast_artifact: reference,
    };
    if let ForecastSelectionQualification::ExactCalibratedConditionalMeanPrice { horizon_nanos } =
        selected.selection_receipt().qualification()
        && !matches!(
            selected.exact_horizon_price_projection(horizon_nanos)?,
            ExactHorizonPriceForecastEvidence::Available(_)
        )
    {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    Ok(selected)
}

fn drift_monitoring_value(
    index: &ForecastIndex,
    vintage: &VintageRecord,
) -> Result<Value, ForecastApplicationError> {
    let mut observed = 0_usize;
    let mut included = 0_usize;
    let mut total_absolute_error = 0_i128;
    for outcome in index
        .outcomes
        .iter()
        .filter(|outcome| outcome.vintage_id == vintage.vintage_id)
    {
        observed = observed
            .checked_add(1)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        if included == MAXIMUM_DRIFT_OUTCOMES {
            continue;
        }
        let absolute = outcome
            .absolute_error_mantissa()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        total_absolute_error = total_absolute_error
            .checked_add(absolute)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        included = included
            .checked_add(1)
            .ok_or(ForecastApplicationError::CorruptIndex)?;
    }
    let scale = vintage
        .decimal_scale()
        .ok_or(ForecastApplicationError::CorruptIndex)?;
    let state = if observed == 0 {
        "awaiting_outcomes"
    } else {
        "outcomes_available"
    };
    let mean_absolute_error = if included == 0 {
        None
    } else {
        Some(decimal_text(
            &(total_absolute_error
                / i128::try_from(included).map_err(|_| ForecastApplicationError::CorruptIndex)?)
            .to_string(),
            scale,
        )?)
    };
    Ok(json!({
        "state": state,
        "observedCount": observed,
        "includedCount": included,
        "truncated": observed > included,
        "meanAbsoluteError": mean_absolute_error,
        "interpretation": "Observed outcome error is monitoring evidence, not a future-performance guarantee."
    }))
}

fn product_vintage(
    index: &ForecastIndex,
    token: Uuid,
) -> Result<&VintageRecord, ForecastApplicationError> {
    let mut matches =
        index
            .vintages
            .iter()
            .filter_map(|vintage| match vintage.matches_product_token(token) {
                Ok(true) => Some(Ok(vintage)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            });
    let selected = matches
        .next()
        .transpose()?
        .ok_or(ForecastApplicationError::NotFound)?;
    if matches.next().transpose()?.is_some() {
        return Err(ForecastApplicationError::CorruptIndex);
    }
    Ok(selected)
}

impl std::fmt::Debug for ForecastApplicationService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ForecastApplicationService")
            .field("index", &"[DURABLE IMMUTABLE FORECAST INDEX]")
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .field("limits", &self.limits)
            .finish()
    }
}

/// Durable forecast authority failure.
#[derive(Debug, Error)]
pub enum ForecastApplicationError {
    /// A configured hard bound is unsupported.
    #[error("forecast application limits are invalid")]
    InvalidLimits,
    /// Durable index contents are invalid.
    #[error("forecast index is corrupt")]
    CorruptIndex,
    /// A supplied vintage or outcome cannot be represented safely.
    #[error("forecast record is invalid")]
    InvalidRecord,
    /// A content identity already names different immutable content.
    #[error("forecast content identity conflicts with retained content")]
    Conflict,
    /// Retained count or bytes reached its hard ceiling.
    #[error("forecast retained capacity is exhausted")]
    Capacity,
    /// Referenced immutable content does not exist.
    #[error("forecast content was not found")]
    NotFound,
    /// The process-local writer or installed authority is unavailable.
    #[error("forecast authority is unavailable")]
    Unavailable,
    /// Durable local state is unavailable.
    #[error(transparent)]
    State(#[from] LocalAuthorityStateStoreError),
    /// Controlled forecast payload publication or verified access failed.
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    /// Restore attempted to reuse an authority outside a fresh inactive workspace.
    #[error("forecast restore target is not fresh")]
    RestoreTargetNotFresh,
}

#[derive(Debug, Error)]
pub(super) enum ForecastBackupCaptureError {
    #[error(transparent)]
    Forecast(#[from] ForecastApplicationError),
    #[error(transparent)]
    Runtime(#[from] ProductionModelRuntimeError),
    #[error("forecast refers to a model generation outside the retained runtime")]
    ModelCoordinateMismatch,
}
