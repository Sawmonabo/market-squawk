//! Production forecast evidence derived from exact Python-admitted feature datasets.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroU16, NonZeroU64, NonZeroUsize},
    time::Instant,
};

use async_trait::async_trait;
use market_squawk_data::{
    AnalyticalReadCapability, ForecastDatasetEvidence, ForecastDatasetReadLimits,
    ForecastFeatureRow, ForecastFeatureValue, Sha256Digest,
};
use market_squawk_domain::{DataQuality, InstrumentId, Timestamp};
use market_squawk_modeling::{ForecastObservedPoint, ForecastValue, ModelMetadata};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::application::model::forecast_preparation::{
    ForecastEvidenceCatalogRequest, ForecastEvidenceCatalogSnapshot, ForecastEvidenceDataset,
    ForecastEvidenceMaterializationRequest, ForecastEvidencePolicy, ForecastEvidenceReadError,
    ForecastEvidenceReader, ForecastEvidenceRevalidation, ForecastInstrumentAvailability,
    PreparedForecastEvidence,
};

const MAX_ROWS: usize = 100_000;
const MAX_BYTES: usize = 256 * 1024 * 1024;
const MAX_HORIZON_POINTS: u16 = 365;
const MAX_VALIDITY_NANOS: u64 = 30 * 24 * 60 * 60 * 1_000_000_000;
const MINIMUM_HISTORY: usize = 3;

/// Analytical implementation of the model-owned forecast evidence contract.
#[derive(Clone)]
pub(crate) struct AnalyticalForecastEvidenceReader {
    analytical: AnalyticalReadCapability,
}

impl AnalyticalForecastEvidenceReader {
    pub(crate) const fn new(analytical: AnalyticalReadCapability) -> Self {
        Self { analytical }
    }

    async fn exact_evidence(
        &self,
        metadata: &ModelMetadata,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastDatasetEvidence, ForecastEvidenceReadError> {
        let identity = metadata.dataset();
        let limits = ForecastDatasetReadLimits::try_new(MAX_ROWS, MAX_BYTES)
            .map_err(|_| ForecastEvidenceReadError::Capacity)?;
        let evidence = self
            .analytical
            .forecast_dataset_evidence(
                identity.manifest(),
                identity.selection_as_of(),
                limits,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_read_error)?;
        let dataset = evidence.dataset();
        let generation = dataset.generation();
        let fence = evidence.fence();
        if generation.manifest() != identity.manifest()
            || generation.build_spec_digest() != Some(identity.build_spec_digest())
            || dataset.universe_digest() != identity.universe_digest()
            || dataset.policy_digest() != identity.policy_digest()
            || dataset.universe_id() != metadata.universe_id()
            || fence.catalog_identity() != identity.catalog_identity()
            || fence.export_sha256() != identity.export_digest()
            || fence.selection_sha256() != identity.selection_digest()
            || fence.as_of() != identity.selection_as_of()
            || fence.selected_rows() != identity.selected_component_rows()
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(evidence)
    }
}

impl fmt::Debug for AnalyticalForecastEvidenceReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticalForecastEvidenceReader")
            .field("analytical", &self.analytical)
            .finish()
    }
}

#[async_trait]
impl ForecastEvidenceReader for AnalyticalForecastEvidenceReader {
    async fn catalog(
        &self,
        request: ForecastEvidenceCatalogRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<ForecastEvidenceCatalogSnapshot, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        let mut datasets = Vec::new();
        let mut authority_generation = None;
        for model in request.models() {
            check_control(deadline, &cancellation)?;
            if model.runtime_generation_sha256() != request.runtime_generation_sha256() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            let metadata = model.metadata();
            let evidence = self
                .exact_evidence(metadata, deadline, cancellation.child_token())
                .await?;
            let observed_authority = authority_for_evidence(&evidence);
            if authority_generation
                .replace(observed_authority)
                .is_some_and(|expected| expected != observed_authority)
            {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            let instruments =
                instrument_inventory(metadata, evidence.rows(), evidence.fence().as_of())?;
            if instruments.is_empty() {
                continue;
            }
            let (maximum_horizon_points, step) = metadata
                .output_binding()
                .expected_terminal_price_horizon_nanos()
                .map_or_else(
                    || {
                        inferred_step(evidence.rows())
                            .map(|step| (MAX_HORIZON_POINTS, step))
                            .ok_or(ForecastEvidenceReadError::NotFound)
                    },
                    |step| Ok((1, step)),
                )?;
            let policy = ForecastEvidencePolicy::try_new(
                NonZeroU16::new(maximum_horizon_points)
                    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
                step,
                NonZeroU64::new(MAX_VALIDITY_NANOS)
                    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
                NonZeroUsize::new(MINIMUM_HISTORY)
                    .ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
            )?;
            datasets.push(ForecastEvidenceDataset::try_new(
                metadata.model_id(),
                metadata.bundle_id().clone(),
                metadata.bundle_version(),
                metadata.dataset().clone(),
                instruments,
                vec![policy],
            )?);
        }
        let authority_generation = authority_generation.unwrap_or_else(|| {
            let mut digest = Sha256::new();
            digest.update(b"market-squawk/forecast-evidence-catalog/empty/v1");
            digest.update(request.runtime_generation_sha256().bytes());
            Sha256Digest::new(digest.finalize().into())
        });
        ForecastEvidenceCatalogSnapshot::try_new(authority_generation, datasets)
    }

    async fn prepare(
        &self,
        request: ForecastEvidenceMaterializationRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
        check_control(deadline, &cancellation)?;
        let evidence = self
            .exact_evidence(
                request.model().metadata(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        let expected_authority = authority_for_evidence(&evidence);
        if expected_authority != request.authority_generation_sha256() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        materialize(request, &evidence)
    }

    async fn revalidate(
        &self,
        expected: &ForecastEvidenceRevalidation,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastEvidenceReadError> {
        let prepared = self
            .prepare(expected.request().clone(), deadline, cancellation)
            .await?;
        if prepared.evidence_sha256() != expected.evidence_sha256() {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        Ok(())
    }
}

fn authority_for_evidence(evidence: &ForecastDatasetEvidence) -> Sha256Digest {
    let mut authority = Sha256::new();
    authority.update(b"market-squawk/forecast-evidence-catalog/v1");
    authority.update(evidence.fence().catalog_identity().bytes());
    Sha256Digest::new(authority.finalize().into())
}

fn instrument_inventory(
    metadata: &ModelMetadata,
    rows: &[ForecastFeatureRow],
    available_at: Timestamp,
) -> Result<Vec<ForecastInstrumentAvailability>, ForecastEvidenceReadError> {
    let mut by_instrument: BTreeMap<InstrumentId, (BTreeSet<Timestamp>, Option<u8>)> =
        BTreeMap::new();
    for row in rows.iter().filter(|row| model_label(row, metadata)) {
        let scale = match row.value() {
            ForecastFeatureValue::Decimal { scale, .. } => *scale,
            ForecastFeatureValue::Missing => continue,
            ForecastFeatureValue::Float(_) => {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
        };
        let effective_at = exact_label_effective(row)?;
        if available_at < effective_at {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
        let (cutoffs, retained_scale) = by_instrument.entry(row.instrument_id()).or_default();
        if retained_scale
            .replace(scale)
            .is_some_and(|value| value != scale)
            || !cutoffs.insert(effective_at)
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
    }
    let admitted_horizon = metadata
        .output_binding()
        .expected_terminal_price_horizon_nanos();
    by_instrument
        .into_iter()
        .filter(|(instrument, _history)| {
            admitted_horizon.is_none_or(|horizon| {
                rows.iter().any(|row| {
                    current_expected_origin(row, metadata, *instrument, available_at, horizon)
                        .is_some()
                })
            })
        })
        .map(|(instrument, (cutoffs, scale))| {
            let first = cutoffs
                .first()
                .copied()
                .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
            let last = cutoffs
                .last()
                .copied()
                .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
            let count = NonZeroUsize::new(cutoffs.len())
                .filter(|count| count.get() >= MINIMUM_HISTORY)
                .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
            ForecastInstrumentAvailability::try_new(
                instrument,
                first,
                last,
                available_at,
                count,
                scale.ok_or(ForecastEvidenceReadError::InvalidEvidence)?,
            )
        })
        .collect()
}

fn inferred_step(rows: &[ForecastFeatureRow]) -> Option<NonZeroU64> {
    let mut by_instrument: BTreeMap<InstrumentId, BTreeSet<Timestamp>> = BTreeMap::new();
    for row in rows.iter().filter(|row| row.component_kind() == 2) {
        by_instrument
            .entry(row.instrument_id())
            .or_default()
            .insert(exact_label_effective(row).ok()?);
    }
    let mut expected = None;
    for cutoffs in by_instrument.into_values() {
        let values = cutoffs.into_iter().collect::<Vec<_>>();
        let mut instrument_step = None;
        for pair in values.windows(2) {
            let delta = pair[1].unix_nanos().checked_sub(pair[0].unix_nanos())?;
            let delta = u64::try_from(delta).ok().and_then(NonZeroU64::new)?;
            if instrument_step
                .replace(delta)
                .is_some_and(|value| value != delta)
            {
                return None;
            }
        }
        let instrument_step = instrument_step?;
        if expected
            .replace(instrument_step)
            .is_some_and(|value| value != instrument_step)
        {
            return None;
        }
    }
    expected
}

fn materialize(
    request: ForecastEvidenceMaterializationRequest,
    evidence: &ForecastDatasetEvidence,
) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
    let metadata = request.model().metadata();
    if let Some(horizon) = metadata
        .output_binding()
        .expected_terminal_price_horizon_nanos()
    {
        return materialize_expected_terminal_price(request, evidence, horizon);
    }
    let instrument = request.selection().instrument_id();
    let mut labels = evidence
        .rows()
        .iter()
        .filter(|row| {
            row.instrument_id() == instrument
                && row.component_kind() == 2
                && row.component_name() == metadata.label().name()
                && row.component_version() == metadata.label().version().get()
        })
        .collect::<Vec<_>>();
    if labels.iter().any(|row| exact_label_effective(row).is_err()) {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    labels.sort_unstable_by_key(|row| row.label_effective_at());
    if labels.len() < MINIMUM_HISTORY
        || labels
            .windows(2)
            .any(|pair| pair[0].label_effective_at() >= pair[1].label_effective_at())
    {
        return Err(ForecastEvidenceReadError::NotFound);
    }
    let decimal_scale = labels
        .iter()
        .find_map(|row| match row.value() {
            ForecastFeatureValue::Decimal { scale, .. } => Some(*scale),
            ForecastFeatureValue::Float(_) | ForecastFeatureValue::Missing => None,
        })
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let observed_history = labels
        .iter()
        .map(|row| {
            let ForecastFeatureValue::Decimal { mantissa, scale } = row.value() else {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            };
            if *scale != decimal_scale {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            let observed_at = exact_label_effective(row)?;
            let available_at = evidence.fence().as_of();
            if available_at < observed_at {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            ForecastObservedPoint::try_new(
                observed_at,
                available_at,
                ForecastValue::try_new(*mantissa, *scale)
                    .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?,
                row.lineage_sha256(),
                DataQuality::Aggregated,
            )
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let observed_cutoff = labels
        .last()
        .map(|row| exact_label_effective(row))
        .transpose()?
        .ok_or(ForecastEvidenceReadError::NotFound)?;
    let feature_cutoff = labels
        .last()
        .map(|row| row.cutoff_at())
        .ok_or(ForecastEvidenceReadError::NotFound)?;
    let row = coefficient_row(metadata, evidence.rows(), instrument, feature_cutoff)?;
    let inputs = (0..usize::from(request.selection().horizon().points().get()))
        .map(|_| row.clone().into_boxed_slice())
        .collect::<Vec<_>>();
    PreparedForecastEvidence::try_new(
        request,
        observed_cutoff,
        evidence.fence().as_of(),
        decimal_scale,
        observed_history,
        inputs,
    )
}

fn materialize_expected_terminal_price(
    request: ForecastEvidenceMaterializationRequest,
    evidence: &ForecastDatasetEvidence,
    admitted_horizon: NonZeroU64,
) -> Result<PreparedForecastEvidence, ForecastEvidenceReadError> {
    if request.selection().horizon().points().get() != 1
        || request.selection().horizon().step_nanos() != admitted_horizon
    {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }
    let metadata = request.model().metadata();
    let instrument = request.selection().instrument_id();
    let available_at = evidence.fence().as_of();
    let mut origins = evidence
        .rows()
        .iter()
        .filter(|row| {
            model_label(row, metadata) && matches!(row.value(), ForecastFeatureValue::Missing)
        })
        .filter_map(|row| {
            current_expected_origin(row, metadata, instrument, available_at, admitted_horizon)
                .map(|(observed, target)| (row, observed, target))
        })
        .collect::<Vec<_>>();
    origins.sort_unstable_by_key(|(row, observed, target)| (*observed, row.cutoff_at(), *target));
    let (origin_row, observed_cutoff, target_at) = origins
        .last()
        .copied()
        .ok_or(ForecastEvidenceReadError::NotFound)?;
    if origins.iter().rev().skip(1).any(|(row, observed, target)| {
        *observed == observed_cutoff
            && row.cutoff_at() == origin_row.cutoff_at()
            && *target == target_at
    }) {
        return Err(ForecastEvidenceReadError::InvalidEvidence);
    }

    let mut labels = evidence
        .rows()
        .iter()
        .filter(|row| {
            row.instrument_id() == instrument
                && model_label(row, metadata)
                && matches!(row.value(), ForecastFeatureValue::Decimal { .. })
        })
        .filter(|row| {
            exact_terminal_coordinates(row).is_ok_and(|(observed, target)| {
                target <= observed_cutoff
                    && target
                        .unix_nanos()
                        .checked_sub(observed.unix_nanos())
                        .and_then(|value| u64::try_from(value).ok())
                        .and_then(NonZeroU64::new)
                        == Some(admitted_horizon)
            })
        })
        .collect::<Vec<_>>();
    labels.sort_unstable_by_key(|row| row.label_effective_at());
    if labels.len() < MINIMUM_HISTORY
        || labels
            .windows(2)
            .any(|pair| pair[0].label_effective_at() >= pair[1].label_effective_at())
        || labels.last().and_then(|row| row.label_effective_at()) != Some(observed_cutoff)
    {
        return Err(ForecastEvidenceReadError::NotFound);
    }
    let decimal_scale = labels
        .first()
        .and_then(|row| match row.value() {
            ForecastFeatureValue::Decimal { scale, .. } => Some(*scale),
            ForecastFeatureValue::Float(_) | ForecastFeatureValue::Missing => None,
        })
        .ok_or(ForecastEvidenceReadError::InvalidEvidence)?;
    let observed_history = labels
        .iter()
        .map(|row| {
            let ForecastFeatureValue::Decimal { mantissa, scale } = row.value() else {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            };
            let observed_at = exact_label_effective(row)?;
            if *scale != decimal_scale
                || row.cutoff_at() < observed_at
                || row.cutoff_at() > available_at
            {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            ForecastObservedPoint::try_new(
                observed_at,
                row.cutoff_at(),
                ForecastValue::try_new(*mantissa, *scale)
                    .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)?,
                row.lineage_sha256(),
                DataQuality::Aggregated,
            )
            .map_err(|_| ForecastEvidenceReadError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let row = coefficient_row_at_origin(
        metadata,
        evidence.rows(),
        instrument,
        origin_row.cutoff_at(),
        observed_cutoff,
        target_at,
    )?;
    PreparedForecastEvidence::try_new(
        request,
        observed_cutoff,
        available_at,
        decimal_scale,
        observed_history,
        vec![row.into_boxed_slice()],
    )
}

fn model_label(row: &ForecastFeatureRow, metadata: &ModelMetadata) -> bool {
    row.component_kind() == 2
        && row.component_name() == metadata.label().name()
        && row.component_version() == metadata.label().version().get()
}

fn current_expected_origin(
    row: &ForecastFeatureRow,
    metadata: &ModelMetadata,
    instrument: InstrumentId,
    available_at: Timestamp,
    admitted_horizon: NonZeroU64,
) -> Option<(Timestamp, Timestamp)> {
    if row.instrument_id() != instrument
        || !model_label(row, metadata)
        || !matches!(row.value(), ForecastFeatureValue::Missing)
    {
        return None;
    }
    let (observed, target) = exact_terminal_coordinates(row).ok()?;
    let horizon = target
        .unix_nanos()
        .checked_sub(observed.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .and_then(NonZeroU64::new)?;
    (horizon == admitted_horizon
        && row.cutoff_at() <= available_at
        && observed <= available_at
        && available_at < target)
        .then_some((observed, target))
}

fn exact_label_effective(row: &ForecastFeatureRow) -> Result<Timestamp, ForecastEvidenceReadError> {
    exact_terminal_coordinates(row).map(|(_observed, label)| label)
}

fn exact_terminal_coordinates(
    row: &ForecastFeatureRow,
) -> Result<(Timestamp, Timestamp), ForecastEvidenceReadError> {
    match (
        row.target_coordinate_kind(),
        row.observed_effective_at(),
        row.label_effective_at(),
    ) {
        (1, Some(observed), Some(label)) if label > observed => Ok((observed, label)),
        _ => Err(ForecastEvidenceReadError::InvalidEvidence),
    }
}

fn coefficient_row_at_origin(
    metadata: &ModelMetadata,
    rows: &[ForecastFeatureRow],
    instrument: InstrumentId,
    cutoff: Timestamp,
    observed: Timestamp,
    target: Timestamp,
) -> Result<Vec<f64>, ForecastEvidenceReadError> {
    metadata
        .features()
        .iter()
        .map(|binding| {
            let mut candidates = rows.iter().filter(|row| {
                row.instrument_id() == instrument
                    && row.cutoff_at() == cutoff
                    && row.component_kind() == 1
                    && row.component_name() == binding.key().name()
                    && row.component_version() == binding.key().version().get()
                    && exact_terminal_coordinates(row) == Ok((observed, target))
            });
            let selected = candidates
                .next()
                .ok_or(ForecastEvidenceReadError::NotFound)?;
            if candidates.next().is_some() {
                return Err(ForecastEvidenceReadError::InvalidEvidence);
            }
            finite_value(selected)
        })
        .collect()
}

fn coefficient_row(
    metadata: &ModelMetadata,
    rows: &[ForecastFeatureRow],
    instrument: InstrumentId,
    cutoff: Timestamp,
) -> Result<Vec<f64>, ForecastEvidenceReadError> {
    metadata
        .features()
        .iter()
        .map(|binding| {
            rows.iter()
                .filter(|row| {
                    row.instrument_id() == instrument
                        && row.cutoff_at() <= cutoff
                        && row.component_kind() == 1
                        && row.component_name() == binding.key().name()
                        && row.component_version() == binding.key().version().get()
                })
                .max_by_key(|row| row.cutoff_at())
                .ok_or(ForecastEvidenceReadError::NotFound)
                .and_then(finite_value)
        })
        .collect()
}

fn finite_value(row: &ForecastFeatureRow) -> Result<f64, ForecastEvidenceReadError> {
    let value = match row.value() {
        ForecastFeatureValue::Float(value) => *value,
        ForecastFeatureValue::Decimal { mantissa, scale } => {
            (*mantissa as f64) / 10_f64.powi(i32::from(*scale))
        }
        ForecastFeatureValue::Missing => return Err(ForecastEvidenceReadError::InvalidEvidence),
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(ForecastEvidenceReadError::InvalidEvidence)
    }
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ForecastEvidenceReadError> {
    if cancellation.is_cancelled() {
        Err(ForecastEvidenceReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ForecastEvidenceReadError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_read_error(error: market_squawk_data::AnalyticalReadError) -> ForecastEvidenceReadError {
    match error {
        market_squawk_data::AnalyticalReadError::ForecastDatasetUnavailable => {
            ForecastEvidenceReadError::NotFound
        }
        market_squawk_data::AnalyticalReadError::InvalidLimit => {
            ForecastEvidenceReadError::Capacity
        }
        _ => ForecastEvidenceReadError::Unavailable,
    }
}
