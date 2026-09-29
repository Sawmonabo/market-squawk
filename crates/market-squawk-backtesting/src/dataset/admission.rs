//! Admission from an owned non-forgeable Task 11 pinned-query receipt.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use arrow::array::{
    Array as _, Decimal128Array, FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray,
    UInt8Array, UInt32Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, DatasetBuildPurpose, DatasetSchemaRegistry,
    FeatureDatasetInputEpochOutput, FeatureDatasetProductContract, PinnedInstrumentDefinitions,
    PinnedQueryOutput, QueryResult, Sha256Digest,
};
use market_squawk_domain::{
    BasisPoints, DigestAlgorithm, InstrumentExecutionTerms, InstrumentId, MarketBarAdjustment,
    PriceTicks, QuantityLots, SourceIdentifier, Timestamp,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{
    BacktestDailyBar, BacktestDailyHistory, BacktestDataset, BacktestDatasetInput, BacktestLimits,
    BacktestObservation, BacktestObservationInput, BacktestStudyQualification,
    HistoricalUniverseStatus, NominalOutcomeSource, ResearchFeatureValue,
};
use crate::engine::BacktestError;

pub const EVENT_AT_COMPONENT: &str = "market_squawk.backtest.event_at_unix_nanos";
pub const AVAILABLE_AT_COMPONENT: &str = "market_squawk.backtest.available_at_unix_nanos";
pub const STALE_AT_COMPONENT: &str = "market_squawk.backtest.stale_at_unix_nanos";
pub const MID_PRICE_COMPONENT: &str = "market_squawk.backtest.mid_price_ticks";
pub const SPREAD_COMPONENT: &str = "market_squawk.backtest.spread_basis_points";
pub const DEPTH_COMPONENT: &str = "market_squawk.backtest.executable_depth_lots";
pub const UNIVERSE_COMPONENT: &str = "market_squawk.backtest.universe_status";

const EXAMPLE_ID: &str = "example_id";
const INSTRUMENT_ID: &str = "instrument_id";
const CUTOFF_AT: &str = "decision_at";
const OBSERVED_EFFECTIVE_AT: &str = "observed_effective_at";
const LABEL_EFFECTIVE_AT: &str = "label_effective_at";
const TARGET_COORDINATE_KIND: &str = "target_coordinate_kind";
const SPLIT: &str = "split";
const COMPONENT_KIND: &str = "component_kind";
const COMPONENT_NAME: &str = "component_name";
const COMPONENT_VERSION: &str = "component_version";
const VALUE_F64: &str = "value_f64";
const VALUE_DECIMAL: &str = "value_decimal_mantissa";
const VALUE_SCALE: &str = "value_decimal_scale";
const MISSING_REASON: &str = "missing_reason";
const LINEAGE: &str = "lineage_sha256";
const EXACT_TARGET_COORDINATES: u8 = 1;
const NON_EXACT_TARGET_COORDINATES: u8 = 2;
const FEATURE_KIND: u8 = 1;
const LABEL_KIND: u8 = 2;
const RESERVED_VERSION: u32 = 1;

/// The sealed reader retains every scoring origin independently of label maturity.
pub(super) fn from_study_input_epochs(
    output: FeatureDatasetInputEpochOutput,
    definitions: &PinnedInstrumentDefinitions,
    admitted_at: Timestamp,
    limits: BacktestLimits,
) -> Result<BacktestDataset, BacktestError> {
    let product = output.dataset();
    let policy = product
        .study_policy()
        .ok_or(BacktestError::InvalidDataset)?;
    // The sealed producer policy owns the horizon. Generic study admission must not replace
    // a selected event horizon with the recommendation strategy's separate one-year contract.
    let target_horizon_nanos = policy.target_horizon().exact_elapsed()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .filter(|nanos| *nanos > 0)
        .ok_or(BacktestError::InvalidDataset)?;
    if product.product_contract()
        != FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
        || policy.purpose() != DatasetBuildPurpose::StudyInputs
        || policy.snapshot_as_of() > admitted_at
        || product.production_receipt().admitted_at() > admitted_at
        || output.epochs().is_empty()
        || output.epochs().len() > limits.max_observations
    {
        return Err(BacktestError::InvalidDataset);
    }
    let qualification = BacktestStudyQualification::try_new(
        policy.basis(),
        policy.snapshot_as_of(),
        product
            .source_snapshot_digest()
            .ok_or(BacktestError::InvalidDataset)?,
        policy
            .decision_lag()
            .map(|lag| i64::try_from(lag.as_nanos()).map_err(|_| BacktestError::InvalidDataset))
            .transpose()?,
        policy.limitations(),
    )?;
    let query = output.query_output();
    let manifest = query.manifest().clone();
    let object_graph_digest = digest(query.object_graph_digest().bytes())?;
    let point_in_time_audit = digest(query.query_identity().bytes())?;
    let point_in_time_content = digest(query.result_digest().bytes())?;
    if product.generation().manifest() != &manifest {
        return Err(BacktestError::InvalidDataset);
    }
    let QueryResult::Inline { byte_count, .. } = query.result() else {
        return Err(BacktestError::PinnedInputRequiresInlineBatches);
    };
    let mut retained_bytes =
        usize::try_from(*byte_count).map_err(|_| BacktestError::LimitExceeded)?;
    for epoch in output.epochs() {
        retained_bytes = retained_bytes
            .checked_add(epoch.retained_bytes())
            .ok_or(BacktestError::LimitExceeded)?;
    }
    for row in output.rows() {
        retained_bytes = retained_bytes
            .checked_add(row.retained_bytes())
            .ok_or(BacktestError::LimitExceeded)?;
    }
    if retained_bytes > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }
    let (_, query, coordinates) = output
        .into_coordinates()
        .map_err(|_| BacktestError::InvalidDataset)?;
    // Native rows have already passed the data decoder. Drop Arrow arrays before compacting
    // current-coordinate features; each native row and source epoch moves exactly once.
    drop(query);
    let shared_bytes = coordinates
        .first()
        .ok_or(BacktestError::InvalidDataset)?
        .shared_dataset_retained_bytes();
    let mut retained_bytes = coordinates.iter().try_fold(
        shared_bytes + std::mem::size_of::<BacktestDataset>(),
        |total, coordinate| {
            total
                .checked_add(coordinate.retained_bytes())
                .and_then(|bytes| bytes.checked_add(std::mem::size_of::<BacktestObservation>()))
                .ok_or(BacktestError::LimitExceeded)
        },
    )?;
    if retained_bytes > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(coordinates.len())
        .map_err(|_| BacktestError::LimitExceeded)?;
    for coordinate in coordinates.into_vec() {
        let epoch = coordinate.epoch();
        // This consumer admits the closed completed-bar price recipe only. Native fiscal
        // coordinates cannot be coerced into timestamps or daily execution observations.
        let market_bar = epoch.market_bar().ok_or(BacktestError::InvalidDataset)?;
        let origin = epoch.target_origin().ok_or(BacktestError::InvalidDataset)?;
        let target = epoch.target_at().ok_or(BacktestError::InvalidDataset)?;
        let decision_at = epoch.decision_at().ok_or(BacktestError::InvalidDataset)?;
        let row_coordinate_kind = match (epoch.fixed_horizon_origin_basis(), epoch.named_session_origin()) {
            (Some(market_squawk_data::FixedHorizonOriginBasis::CompletedBarClose), None)
                if market_bar.completed_at() == Some(origin) => 3,
            (Some(market_squawk_data::FixedHorizonOriginBasis::NamedSessionCloseForNominalDailyBar), Some(native))
                if native.matches_origin_bar(market_bar, epoch.source_manifest(), origin, epoch.source_selection_as_of())
                    && native.target_native_date().is_none() => 5,
            _ => return Err(BacktestError::InvalidDataset),
        };
        if epoch.financial_period().is_some()
            || origin
                .checked_add_nanos(target_horizon_nanos)
                .ok()
                != Some(target)
        {
            return Err(BacktestError::InvalidDataset);
        }
        let available_at = market_bar
            .context()
            .provenance()
            .availability()
            .conservative_available_at()
            .ok_or(BacktestError::InvalidDataset)?;
        let market_reference = epoch
            .current_unit_price()
            .map_err(|_| BacktestError::InvalidDataset)?;
        let terms = definitions
            .execution_terms_at(epoch.instrument_id(), decision_at)
            .ok_or(BacktestError::InvalidDataset)?;
        if epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.basis() != qualification.basis()
            || epoch.snapshot_as_of() != qualification.snapshot_as_of()
            || epoch.source_snapshot_digest() != qualification.source_snapshot_digest()
            || epoch.limitations() != qualification.limitations()
            || epoch.calculated_at() > admitted_at
            || !qualification.admits_clocks(
                origin,
                available_at,
                epoch.source_selection_as_of(),
                decision_at,
            )
            || market_reference.currency() != terms.quote_currency()
        {
            return Err(BacktestError::InvalidDataset);
        }
        let mut features = Vec::new();
        let mut previous_name: Option<String> = None;
        let mut lineage = Sha256::new();
        lineage.update(b"market-squawk/study-input-observation/v1\0");
        for row in coordinate.rows() {
            if row.component_kind() != FEATURE_KIND
                || row.target_coordinate_kind() != row_coordinate_kind
                || row.label_selection_as_of().is_some()
                || row.source_selection_as_of() != epoch.source_selection_as_of()
                || row.observed_effective_at() != Some(origin)
                || row.label_effective_at() != Some(target)
                || row.component_name().starts_with("market_squawk.backtest.")
                || previous_name
                    .as_ref()
                    .is_some_and(|prior| prior.as_str() >= row.component_name())
            {
                return Err(BacktestError::InvalidDataset);
            }
            previous_name = Some(row.component_name().into());
            let version =
                NonZeroU32::new(row.component_version()).ok_or(BacktestError::InvalidDataset)?;
            let value = match row.value() {
                market_squawk_data::ForecastFeatureValue::Float(value) => Some(*value),
                market_squawk_data::ForecastFeatureValue::Decimal { mantissa, scale } => Some(
                    Decimal::try_from_i128_with_scale(*mantissa, u32::from(*scale))
                        .map_err(|_| BacktestError::InvalidDataset)?
                        .to_f64()
                        .ok_or(BacktestError::InvalidDataset)?,
                ),
                market_squawk_data::ForecastFeatureValue::Missing => None,
            };
            if let Some(value) = value {
                retained_bytes = retained_bytes
                    .checked_add(
                        row.component_name().len() + std::mem::size_of::<ResearchFeatureValue>(),
                    )
                    .ok_or(BacktestError::LimitExceeded)?;
                if retained_bytes > limits.max_retained_bytes {
                    return Err(BacktestError::LimitExceeded);
                }
                features
                    .try_reserve(1)
                    .map_err(|_| BacktestError::LimitExceeded)?;
                features.push(ResearchFeatureValue::try_new(
                    SourceIdentifier::try_from(row.component_name())?,
                    version,
                    value,
                )?);
            }
            lineage.update(row.lineage_sha256().bytes());
        }
        if coordinate.rows().is_empty() {
            return Err(BacktestError::InvalidDataset);
        }
        lineage.update(epoch.source_evidence_digest().bytes());
        lineage.update(epoch.point_in_time_content().bytes());
        lineage.update(epoch.point_in_time_audit().bytes());
        lineage.update(epoch.universe_content().bytes());
        lineage.update(epoch.universe_audit().bytes());
        qualification.hash_into(&mut lineage);
        observations.push(BacktestObservation {
            execution_terms: terms,
            event_at: origin,
            available_at,
            decision_at,
            // No quote freshness, spread or depth is claimed by daily study admission.
            stale_at: decision_at,
            mid_price: None,
            spread_basis_points: BasisPoints::new(0),
            executable_depth: QuantityLots::new(0).map_err(|_| BacktestError::InvalidDataset)?,
            universe: HistoricalUniverseStatus::Eligible,
            features: features.into_boxed_slice(),
            lineage_digest: Sha256Digest::new(lineage.finalize().into()),
            financial_target: Some((origin, target)),
            source_selection_as_of: epoch.source_selection_as_of(),
            market_reference: Some(market_reference),
            input_coordinate: Some(Box::new(coordinate)),
        });
    }
    let mut dataset = BacktestDataset::try_new(BacktestDatasetInput {
        manifest,
        object_graph_digest,
        point_in_time_content,
        point_in_time_audit,
        instrument_definition_content: definitions.content_identity(),
        instrument_definition_audit: definitions.audit_identity(),
        observations,
    })?;
    dataset.retained_bytes = dataset
        .retained_bytes
        .checked_add(shared_bytes)
        .ok_or(BacktestError::LimitExceeded)?;
    if dataset.retained_bytes > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/qualified-backtest-dataset/v1\0");
    identity.update(dataset.identity.bytes());
    qualification.hash_into(&mut identity);
    dataset.identity = Sha256Digest::new(identity.finalize().into());
    dataset.study_qualification = Some(qualification);
    Ok(dataset)
}

pub(super) fn with_complete_daily_history(
    mut dataset: BacktestDataset,
    mut histories: Vec<CompleteMarketBarHistoryOutput>,
    definitions: &PinnedInstrumentDefinitions,
    admitted_at: Timestamp,
    limits: BacktestLimits,
) -> Result<BacktestDataset, BacktestError> {
    if dataset.daily_history.is_some()
        || histories.is_empty()
        || histories.len() > 4_096
        || histories.len() != definitions.instrument_count()
        || definitions.as_of() > admitted_at
    {
        return Err(BacktestError::InvalidDataset);
    }
    let total_bars = histories.iter().try_fold(0_usize, |total, history| {
        total
            .checked_add(history.bars().len())
            .ok_or(BacktestError::LimitExceeded)
    })?;
    let retained_bytes = total_bars
        .checked_mul(std::mem::size_of::<BacktestDailyBar>())
        .and_then(|bytes| bytes.checked_add(dataset.retained_bytes))
        .ok_or(BacktestError::LimitExceeded)?;
    if total_bars == 0
        || total_bars
            .checked_add(dataset.observations.len())
            .is_none_or(|count| count > limits.max_observations)
        || retained_bytes > limits.max_retained_bytes
    {
        return Err(BacktestError::LimitExceeded);
    }
    histories.sort_unstable_by_key(|history| history.selection().receipt().instrument_id());
    if histories.windows(2).any(|pair| {
        pair[0].selection().receipt().instrument_id()
            == pair[1].selection().receipt().instrument_id()
    }) {
        return Err(BacktestError::InvalidDataset);
    }
    let instruments = dataset
        .observations
        .iter()
        .map(BacktestObservation::instrument_id)
        .collect::<BTreeSet<_>>();
    let outcome_instruments = histories
        .iter()
        .map(|history| history.selection().receipt().instrument_id())
        .collect::<BTreeSet<_>>();
    if !instruments.is_subset(&outcome_instruments)
        || definitions
            .instrument_ids()
            .any(|instrument| !outcome_instruments.contains(&instrument))
    {
        return Err(BacktestError::InvalidDataset);
    }
    // Timestamp and native-date sources need not share synthetic request timestamps. Each
    // immutable complete window is checked in its own original source coordinate below.
    let mut nominal_sources = Vec::new();
    nominal_sources
        .try_reserve_exact(histories.len())
        .map_err(|_| BacktestError::LimitExceeded)?;
    let mut bars = Vec::new();
    bars.try_reserve_exact(total_bars)
        .map_err(|_| BacktestError::LimitExceeded)?;
    let mut evidence = Sha256::new();
    evidence.update(b"market-squawk/backtest-complete-raw-daily-history/v2\0");
    evidence.update(definitions.content_identity().bytes());
    evidence.update(definitions.audit_identity().bytes());
    evidence.update((histories.len() as u64).to_be_bytes());
    let mut available_at = Timestamp::from_unix_nanos(i64::MIN);
    for history in histories {
        let receipt = history.selection().receipt();
        let (available, received, ingested) = receipt.knowledge_clocks();
        let mut history_available = available
            .max(received)
            .max(ingested)
            .max(receipt.published_at())
            .max(receipt.capture_recorded_at());
        if receipt.adjustment() != MarketBarAdjustment::Raw
            || !receipt.realized_outcome_eligible()
            || history.read_receipt().knowledge_cutoff() > admitted_at
            || history_available > history.read_receipt().knowledge_cutoff()
            || history.bars().is_empty()
            || history.bars().len() != receipt.bar_count()
        {
            return Err(BacktestError::InvalidDataset);
        }
        let nominal_dates = receipt.requested_dates();
        let native = match nominal_dates {
            Some(_) => {
                let native = history
                    .native_sessions()
                    .ok_or(BacktestError::InvalidDataset)?;
                if receipt.requested_range().is_some()
                    || receipt.date_windows().is_none()
                    || native.sessions().len() != history.bars().len()
                    || native.sessions().windows(2).any(|rows| {
                        rows[0].native_date() >= rows[1].native_date()
                            || rows[0].closes_at_exclusive() >= rows[1].opens_at()
                    })
                    || native.received_at() > native.published_at()
                    || native.published_at() > history.read_receipt().knowledge_cutoff()
                {
                    return Err(BacktestError::InvalidDataset);
                }
                history_available = history_available
                    .max(native.published_at())
                    .max(native.received_at());
                nominal_sources.push(NominalOutcomeSource {
                    instrument: receipt.instrument_id(),
                    manifest: history.selection().pinned().manifest().clone(),
                    knowledge_cutoff: history.read_receipt().knowledge_cutoff(),
                });
                Some(native)
            }
            None => {
                if receipt.interval().as_str() != "1Day" || receipt.requested_range().is_none() {
                    return Err(BacktestError::InvalidDataset);
                }
                None
            }
        };
        available_at = available_at.max(history_available);
        evidence.update(receipt.instrument_id().as_uuid().as_bytes());
        evidence.update(history.selection().selection_digest().bytes());
        evidence.update(history.read_receipt().result_digest().bytes());
        evidence.update(receipt.receipt_digest().bytes());
        evidence.update(
            history
                .read_receipt()
                .knowledge_cutoff()
                .unix_nanos()
                .to_be_bytes(),
        );
        if let Some(native) = native {
            evidence.update([2]);
            evidence.update(native.mapping_digest().bytes());
            evidence.update(native.source_replay_digest().bytes());
        } else {
            evidence.update([1]);
        }
        for (index, bar) in history.bars().iter().enumerate() {
            // These are financial execution-session bounds. For a nominal bar they are not
            // provider aggregation timestamps; the original native date remains in lineage.
            let (starts_at, ends_at) = if let Some(native) = native {
                let session = native
                    .sessions()
                    .get(index)
                    .ok_or(BacktestError::InvalidDataset)?;
                let date = bar
                    .time_semantics()
                    .nominal_daily_date()
                    .ok_or(BacktestError::InvalidDataset)?;
                let dates = nominal_dates.ok_or(BacktestError::InvalidDataset)?;
                if !session.bar_present()
                    || session.provider_timestamp().is_some()
                    || session.provider_period().is_some()
                    || bar.completed_at().is_some()
                    || date.date() != session.native_date()
                    || date.date() < dates.0
                    || date.date() > dates.1
                    || bar.context().time().effective().calendar_date_value() != Some(date.date())
                {
                    return Err(BacktestError::InvalidDataset);
                }
                (session.opens_at(), session.closes_at_exclusive())
            } else {
                let time = bar
                    .time_semantics()
                    .timestamped_period()
                    .ok_or(BacktestError::InvalidDataset)?;
                let range = receipt
                    .requested_range()
                    .ok_or(BacktestError::InvalidDataset)?;
                if time.provider_timestamp() < range.0
                    || time.provider_timestamp() > range.1
                {
                    return Err(BacktestError::InvalidDataset);
                }
                (time.period_start(), time.period_end_exclusive())
            };
            let terms = definitions
                .execution_terms_at(receipt.instrument_id(), ends_at)
                .ok_or(BacktestError::InvalidDataset)?;
            if bar.adjustment() != MarketBarAdjustment::Raw
                || bar.context().provenance().instrument_id() != Some(receipt.instrument_id())
                || bar.currency() != terms.quote_currency()
                || starts_at >= ends_at
                || ends_at > history.read_receipt().knowledge_cutoff()
                || ends_at > history_available
                || bar.volume() < Decimal::ZERO
            {
                return Err(BacktestError::InvalidDataset);
            }
            let close = bar.close();
            if close.amount() <= Decimal::ZERO {
                return Err(BacktestError::InvalidDataset);
            }
            let mut lineage = Sha256::new();
            lineage.update(b"market-squawk/backtest-realized-daily-bar/v2\0");
            lineage.update(history.read_receipt().result_digest().bytes());
            match bar.time_semantics().nominal_daily_date() {
                Some(date) => {
                    lineage.update([2]);
                    lineage.update(date.date().days_since_unix_epoch().to_be_bytes());
                    lineage.update(
                        native
                            .ok_or(BacktestError::InvalidDataset)?
                            .mapping_digest()
                            .bytes(),
                    );
                }
                None => {
                    lineage.update([1]);
                }
            }
            lineage.update(starts_at.unix_nanos().to_be_bytes());
            lineage.update(ends_at.unix_nanos().to_be_bytes());
            bars.push(BacktestDailyBar {
                execution_terms: terms,
                starts_at,
                ends_at,
                available_at: history_available,
                close,
                traded_volume: bar.volume(),
                lineage_digest: Sha256Digest::new(lineage.finalize().into()),
            });
        }
    }
    bars.sort_unstable_by_key(|bar| (bar.ends_at, bar.execution_terms.instrument_id()));
    if bars.windows(2).any(|pair| {
        pair[0].ends_at == pair[1].ends_at
            && pair[0].execution_terms.instrument_id() == pair[1].execution_terms.instrument_id()
    }) {
        return Err(BacktestError::InvalidDataset);
    }
    let raw_digest = Sha256Digest::new(evidence.finalize().into());
    let mut identity = Sha256::new();
    identity.update(b"market-squawk/backtest-dataset-with-daily-execution/v1\0");
    identity.update(dataset.identity.bytes());
    identity.update(raw_digest.bytes());
    dataset.identity = Sha256Digest::new(identity.finalize().into());
    dataset.retained_bytes = retained_bytes;
    let source_bytes = nominal_sources.iter().try_fold(0_usize, |sum, source| {
        sum.checked_add(std::mem::size_of::<NominalOutcomeSource>())
            .and_then(|n| n.checked_add(source.manifest.dataset_id().as_str().len()))
            .and_then(|n| n.checked_add(source.manifest.schema().name().len()))
            .ok_or(BacktestError::LimitExceeded)
    })?;
    dataset.retained_bytes = dataset
        .retained_bytes
        .checked_add(source_bytes)
        .ok_or(BacktestError::LimitExceeded)?;
    if dataset.retained_bytes > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }
    dataset.daily_history = Some(BacktestDailyHistory {
        bars: bars.into_boxed_slice(),
        digest: raw_digest,
        available_at,
        nominal_sources: nominal_sources.into_boxed_slice(),
    });
    Ok(dataset)
}

pub(super) fn from_pinned_query(
    output: PinnedQueryOutput,
    instrument_definitions: &PinnedInstrumentDefinitions,
    limits: BacktestLimits,
    has_separate_outcome_definitions: bool,
) -> Result<BacktestDataset, BacktestError> {
    let canonical = DatasetSchemaRegistry::local().canonical_feature_labels()?;
    if output.manifest().schema() != &canonical
        || output.object_graph_digest().algorithm() != DigestAlgorithm::Sha256
        || output.query_identity().algorithm() != DigestAlgorithm::Sha256
        || output.result_digest().algorithm() != DigestAlgorithm::Sha256
    {
        return Err(BacktestError::InvalidDataset);
    }
    let object_graph_digest = digest(output.object_graph_digest().bytes())?;
    let point_in_time_audit = digest(output.query_identity().bytes())?;
    let point_in_time_content = digest(output.result_digest().bytes())?;
    let instrument_definition_content = instrument_definitions.content_identity();
    let instrument_definition_audit = instrument_definitions.audit_identity();
    let manifest = output.manifest().clone();
    let QueryResult::Inline {
        batches,
        byte_count,
    } = output.result()
    else {
        return Err(BacktestError::PinnedInputRequiresInlineBatches);
    };
    let byte_count = usize::try_from(*byte_count).map_err(|_| BacktestError::LimitExceeded)?;
    if batches.is_empty() || byte_count == 0 || byte_count > limits.max_retained_bytes {
        return Err(BacktestError::LimitExceeded);
    }

    if instrument_definitions.instrument_count() == 0 {
        return Err(BacktestError::InvalidDataset);
    }

    let expected = DatasetSchemaRegistry::local().resolve(&canonical)?;
    let mut groups = BTreeMap::<GroupKey, Group>::new();
    let mut row_count = 0_usize;
    for batch in batches {
        validate_schema(batch, &expected)?;
        row_count = row_count
            .checked_add(batch.num_rows())
            .ok_or(BacktestError::LimitExceeded)?;
        if row_count > limits.max_observations.saturating_mul(1_024) {
            return Err(BacktestError::LimitExceeded);
        }
        admit_batch(batch, instrument_definitions, &mut groups)?;
    }
    if groups.is_empty() || groups.len() > limits.max_observations {
        return Err(BacktestError::LimitExceeded);
    }

    let mut used_instruments = BTreeSet::new();
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(groups.len())
        .map_err(|_| BacktestError::LimitExceeded)?;
    for (key, group) in groups {
        used_instruments.insert(key.instrument_id);
        observations.push(group.finish(key)?);
    }
    if !has_separate_outcome_definitions
        && (used_instruments.len() != instrument_definitions.instrument_count()
            || instrument_definitions
                .instrument_ids()
                .any(|instrument| !used_instruments.contains(&instrument)))
    {
        return Err(BacktestError::InvalidDataset);
    }
    BacktestDataset::try_new(BacktestDatasetInput {
        manifest,
        object_graph_digest,
        point_in_time_content,
        point_in_time_audit,
        instrument_definition_content,
        instrument_definition_audit,
        observations,
    })
}

fn validate_schema(
    batch: &RecordBatch,
    expected: &arrow::datatypes::Schema,
) -> Result<(), BacktestError> {
    if batch.num_columns() != expected.fields().len()
        || batch
            .schema()
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(actual, expected)| {
                actual.name() != expected.name()
                    || actual.data_type() != expected.data_type()
                    || actual.is_nullable() != expected.is_nullable()
            })
    {
        return Err(BacktestError::InvalidDataset);
    }
    Ok(())
}

fn admit_batch(
    batch: &RecordBatch,
    instrument_definitions: &PinnedInstrumentDefinitions,
    groups: &mut BTreeMap<GroupKey, Group>,
) -> Result<(), BacktestError> {
    let examples = array::<FixedSizeBinaryArray>(batch, EXAMPLE_ID)?;
    let instruments = array::<FixedSizeBinaryArray>(batch, INSTRUMENT_ID)?;
    let cutoffs = array::<TimestampNanosecondArray>(batch, CUTOFF_AT)?;
    let observed_effective = array::<TimestampNanosecondArray>(batch, OBSERVED_EFFECTIVE_AT)?;
    let label_effective = array::<TimestampNanosecondArray>(batch, LABEL_EFFECTIVE_AT)?;
    let target_coordinate_kinds = array::<UInt8Array>(batch, TARGET_COORDINATE_KIND)?;
    let splits = array::<UInt8Array>(batch, SPLIT)?;
    let kinds = array::<UInt8Array>(batch, COMPONENT_KIND)?;
    let names = array::<FixedSizeBinaryArray>(batch, COMPONENT_NAME)?;
    let versions = array::<UInt32Array>(batch, COMPONENT_VERSION)?;
    let float_values = array::<Float64Array>(batch, VALUE_F64)?;
    let decimal_values = array::<Decimal128Array>(batch, VALUE_DECIMAL)?;
    let scales = array::<UInt8Array>(batch, VALUE_SCALE)?;
    let missing = array::<FixedSizeBinaryArray>(batch, MISSING_REASON)?;
    let lineages = array::<FixedSizeBinaryArray>(batch, LINEAGE)?;
    if decimal_values.data_type() != &DataType::Decimal128(38, 0) {
        return Err(BacktestError::InvalidDataset);
    }
    for row in 0..batch.num_rows() {
        let coordinates = target_coordinates(
            observed_effective,
            label_effective,
            target_coordinate_kinds,
            splits,
            row,
        )?;
        match required(kinds, row)? {
            LABEL_KIND => continue,
            FEATURE_KIND => {}
            _ => return Err(BacktestError::InvalidDataset),
        }
        let instrument_id = instrument(instruments, row)?;
        let cutoff = Timestamp::from_unix_nanos(required(cutoffs, row)?);
        let execution_terms = instrument_definitions
            .execution_terms_at(instrument_id, cutoff)
            .ok_or(BacktestError::InvalidDataset)?;
        let key = GroupKey {
            cutoff,
            instrument_id,
            example_id: fixed_text(examples, row)?.to_owned(),
            coordinates,
        };
        let version =
            NonZeroU32::new(required(versions, row)?).ok_or(BacktestError::InvalidDataset)?;
        let name = fixed_text(names, row)?;
        let lineage = fixed_digest(lineages, row)?;
        let group = groups
            .entry(key)
            .or_insert_with(|| Group::new(execution_terms));
        if group.execution_terms != execution_terms || !group.names.insert(name.to_owned()) {
            return Err(BacktestError::InvalidDataset);
        }
        group.hash_component(name, version, lineage)?;
        let value = ComponentValueReader {
            float_values,
            decimal_values,
            scales,
            missing,
            row,
        };
        group.apply(name, version, value)?;
    }
    Ok(())
}

fn target_coordinates(
    observed_effective: &TimestampNanosecondArray,
    label_effective: &TimestampNanosecondArray,
    target_coordinate_kinds: &UInt8Array,
    splits: &UInt8Array,
    row: usize,
) -> Result<TargetCoordinates, BacktestError> {
    let observed_effective_at = optional_timestamp(observed_effective, row);
    let label_effective_at = optional_timestamp(label_effective, row);
    let kind = required(target_coordinate_kinds, row)?;
    let split = required(splits, row)?;
    let valid_target = match (kind, observed_effective_at, label_effective_at) {
        (EXACT_TARGET_COORDINATES, Some(observed), Some(target)) => target > observed,
        (NON_EXACT_TARGET_COORDINATES, None, None) => true,
        _ => false,
    };
    if !valid_target || !matches!(split, 1..=3) {
        return Err(BacktestError::InvalidDataset);
    }
    Ok(TargetCoordinates {
        observed_effective_at,
        label_effective_at,
        kind,
        split,
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetCoordinates {
    observed_effective_at: Option<Timestamp>,
    label_effective_at: Option<Timestamp>,
    kind: u8,
    split: u8,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct GroupKey {
    cutoff: Timestamp,
    instrument_id: InstrumentId,
    example_id: String,
    coordinates: TargetCoordinates,
}

#[derive(Debug)]
struct Group {
    execution_terms: InstrumentExecutionTerms,
    event_at: Option<Timestamp>,
    available_at: Option<Timestamp>,
    stale_at: Option<Timestamp>,
    mid_seen: bool,
    mid_price: Option<PriceTicks>,
    spread: Option<BasisPoints>,
    depth: Option<QuantityLots>,
    universe: Option<HistoricalUniverseStatus>,
    features: Vec<ResearchFeatureValue>,
    names: BTreeSet<String>,
    lineage: Sha256,
}

impl Group {
    fn new(execution_terms: InstrumentExecutionTerms) -> Self {
        let mut lineage = Sha256::new();
        lineage.update(b"market-squawk/backtest-observation-lineage/v1");
        Self {
            execution_terms,
            event_at: None,
            available_at: None,
            stale_at: None,
            mid_seen: false,
            mid_price: None,
            spread: None,
            depth: None,
            universe: None,
            features: Vec::new(),
            names: BTreeSet::new(),
            lineage,
        }
    }

    fn hash_component(
        &mut self,
        name: &str,
        version: NonZeroU32,
        lineage: [u8; 32],
    ) -> Result<(), BacktestError> {
        self.lineage.update(
            u64::try_from(name.len())
                .map_err(|_| BacktestError::LimitExceeded)?
                .to_be_bytes(),
        );
        self.lineage.update(name.as_bytes());
        self.lineage.update(version.get().to_be_bytes());
        self.lineage.update(lineage);
        Ok(())
    }

    fn apply(
        &mut self,
        name: &str,
        version: NonZeroU32,
        value: ComponentValueReader<'_>,
    ) -> Result<(), BacktestError> {
        let reserved = matches!(
            name,
            EVENT_AT_COMPONENT
                | AVAILABLE_AT_COMPONENT
                | STALE_AT_COMPONENT
                | MID_PRICE_COMPONENT
                | SPREAD_COMPONENT
                | DEPTH_COMPONENT
                | UNIVERSE_COMPONENT
        );
        if reserved && version.get() != RESERVED_VERSION {
            return Err(BacktestError::InvalidDataset);
        }
        match name {
            EVENT_AT_COMPONENT => self.event_at = Some(value.exact_timestamp()?),
            AVAILABLE_AT_COMPONENT => self.available_at = Some(value.exact_timestamp()?),
            STALE_AT_COMPONENT => self.stale_at = Some(value.exact_timestamp()?),
            MID_PRICE_COMPONENT => {
                self.mid_seen = true;
                self.mid_price = value.optional_exact_i64()?.map(PriceTicks::new);
            }
            SPREAD_COMPONENT => {
                let raw =
                    i32::try_from(value.exact_i64()?).map_err(|_| BacktestError::InvalidDataset)?;
                self.spread = Some(BasisPoints::new(raw));
            }
            DEPTH_COMPONENT => {
                self.depth = Some(
                    QuantityLots::new(value.exact_i64()?)
                        .map_err(|_| BacktestError::InvalidDataset)?,
                );
            }
            UNIVERSE_COMPONENT => {
                self.universe = Some(match value.exact_i64()? {
                    1 => HistoricalUniverseStatus::Eligible,
                    2 => HistoricalUniverseStatus::Ineligible,
                    3 => HistoricalUniverseStatus::Delisted,
                    _ => return Err(BacktestError::InvalidDataset),
                });
            }
            _ => {
                if let Some(number) = value.optional_feature()? {
                    self.features.push(ResearchFeatureValue::try_new(
                        SourceIdentifier::try_from(name)?,
                        version,
                        number,
                    )?);
                }
            }
        }
        Ok(())
    }

    fn finish(self, key: GroupKey) -> Result<BacktestObservation, BacktestError> {
        if !self.mid_seen {
            return Err(BacktestError::InvalidDataset);
        }
        BacktestObservation::try_new(BacktestObservationInput {
            execution_terms: self.execution_terms,
            event_at: self.event_at.ok_or(BacktestError::InvalidDataset)?,
            available_at: self.available_at.ok_or(BacktestError::InvalidDataset)?,
            decision_at: key.cutoff,
            stale_at: self.stale_at.ok_or(BacktestError::InvalidDataset)?,
            mid_price: self.mid_price,
            spread_basis_points: self.spread.ok_or(BacktestError::InvalidDataset)?,
            executable_depth: self.depth.ok_or(BacktestError::InvalidDataset)?,
            universe: self.universe.ok_or(BacktestError::InvalidDataset)?,
            features: self.features,
            lineage_digest: Sha256Digest::new(self.lineage.finalize().into()),
        })
    }
}

struct ComponentValueReader<'batch> {
    float_values: &'batch Float64Array,
    decimal_values: &'batch Decimal128Array,
    scales: &'batch UInt8Array,
    missing: &'batch FixedSizeBinaryArray,
    row: usize,
}

impl ComponentValueReader<'_> {
    fn exact_timestamp(&self) -> Result<Timestamp, BacktestError> {
        Ok(Timestamp::from_unix_nanos(self.exact_i64()?))
    }

    fn exact_i64(&self) -> Result<i64, BacktestError> {
        self.optional_exact_i64()?
            .ok_or(BacktestError::InvalidDataset)
    }

    fn optional_exact_i64(&self) -> Result<Option<i64>, BacktestError> {
        if !self.missing.is_null(self.row) {
            if self.float_values.is_null(self.row)
                && self.decimal_values.is_null(self.row)
                && self.scales.is_null(self.row)
            {
                return Ok(None);
            }
            return Err(BacktestError::InvalidDataset);
        }
        if !self.float_values.is_null(self.row)
            || self.decimal_values.is_null(self.row)
            || self.scales.is_null(self.row)
            || self.scales.value(self.row) != 0
        {
            return Err(BacktestError::InvalidDataset);
        }
        i64::try_from(self.decimal_values.value(self.row))
            .map(Some)
            .map_err(|_| BacktestError::InvalidDataset)
    }

    fn optional_feature(&self) -> Result<Option<f64>, BacktestError> {
        if !self.missing.is_null(self.row) {
            if self.float_values.is_null(self.row) && self.decimal_values.is_null(self.row) {
                return Ok(None);
            }
            return Err(BacktestError::InvalidDataset);
        }
        match (
            self.float_values.is_null(self.row),
            self.decimal_values.is_null(self.row),
        ) {
            (false, true) => {
                let value = self.float_values.value(self.row);
                value
                    .is_finite()
                    .then_some(Some(value))
                    .ok_or(BacktestError::InvalidDataset)
            }
            (true, false) if !self.scales.is_null(self.row) => {
                let decimal = Decimal::try_from_i128_with_scale(
                    self.decimal_values.value(self.row),
                    u32::from(self.scales.value(self.row)),
                )
                .map_err(|_| BacktestError::InvalidDataset)?;
                decimal
                    .to_f64()
                    .filter(|value| value.is_finite())
                    .map(Some)
                    .ok_or(BacktestError::InvalidDataset)
            }
            _ => Err(BacktestError::InvalidDataset),
        }
    }
}

fn array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T, BacktestError> {
    batch
        .column_by_name(name)
        .ok_or(BacktestError::InvalidDataset)?
        .as_any()
        .downcast_ref::<T>()
        .ok_or(BacktestError::InvalidDataset)
}

fn required<T: arrow::array::ArrowPrimitiveType>(
    values: &arrow::array::PrimitiveArray<T>,
    row: usize,
) -> Result<T::Native, BacktestError> {
    if values.is_null(row) {
        Err(BacktestError::InvalidDataset)
    } else {
        Ok(values.value(row))
    }
}

fn optional_timestamp(values: &TimestampNanosecondArray, row: usize) -> Option<Timestamp> {
    (!values.is_null(row)).then(|| Timestamp::from_unix_nanos(values.value(row)))
}

fn fixed_text(values: &FixedSizeBinaryArray, row: usize) -> Result<&str, BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let bytes = values.value(row);
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    if end == 0
        || bytes
            .get(end..)
            .is_some_and(|tail| tail.iter().any(|byte| *byte != 0))
    {
        return Err(BacktestError::InvalidDataset);
    }
    std::str::from_utf8(&bytes[..end]).map_err(|_| BacktestError::InvalidDataset)
}

fn instrument(values: &FixedSizeBinaryArray, row: usize) -> Result<InstrumentId, BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let bytes: [u8; 16] = values
        .value(row)
        .try_into()
        .map_err(|_| BacktestError::InvalidDataset)?;
    InstrumentId::try_from(Uuid::from_bytes(bytes)).map_err(|_| BacktestError::InvalidDataset)
}

fn fixed_digest(values: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 32], BacktestError> {
    if values.is_null(row) {
        return Err(BacktestError::InvalidDataset);
    }
    let digest: [u8; 32] = values
        .value(row)
        .try_into()
        .map_err(|_| BacktestError::InvalidDataset)?;
    if digest == [0; 32] {
        return Err(BacktestError::InvalidDataset);
    }
    Ok(digest)
}

fn digest(bytes: [u8; 32]) -> Result<Sha256Digest, BacktestError> {
    if bytes == [0; 32] {
        Err(BacktestError::InvalidDataset)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}
