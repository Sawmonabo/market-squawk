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
            let instruments = instrument_inventory(evidence.rows())?;
            if instruments.is_empty() {
                continue;
            }
            let step = inferred_step(evidence.rows()).ok_or(ForecastEvidenceReadError::NotFound)?;
            let policy = ForecastEvidencePolicy::try_new(
                NonZeroU16::new(MAX_HORIZON_POINTS)
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
    rows: &[ForecastFeatureRow],
) -> Result<Vec<ForecastInstrumentAvailability>, ForecastEvidenceReadError> {
    let mut by_instrument: BTreeMap<InstrumentId, (BTreeSet<Timestamp>, Option<u8>)> =
        BTreeMap::new();
    for row in rows.iter().filter(|row| row.component_kind() == 2) {
        let ForecastFeatureValue::Decimal { scale, .. } = row.value() else {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        };
        let (cutoffs, retained_scale) = by_instrument.entry(row.instrument_id()).or_default();
        if retained_scale
            .replace(*scale)
            .is_some_and(|value| value != *scale)
            || !cutoffs.insert(row.cutoff_at())
        {
            return Err(ForecastEvidenceReadError::InvalidEvidence);
        }
    }
    by_instrument
        .into_iter()
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
                last,
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
            .insert(row.cutoff_at());
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
    labels.sort_unstable_by_key(|row| row.cutoff_at());
    if labels.len() < MINIMUM_HISTORY
        || labels
            .windows(2)
            .any(|pair| pair[0].cutoff_at() >= pair[1].cutoff_at())
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
            ForecastObservedPoint::try_new(
                row.cutoff_at(),
                row.cutoff_at(),
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
        .map(|row| row.cutoff_at())
        .ok_or(ForecastEvidenceReadError::NotFound)?;
    let row = coefficient_row(metadata, evidence.rows(), instrument, observed_cutoff)?;
    let inputs = (0..usize::from(request.selection().horizon().points().get()))
        .map(|_| row.clone().into_boxed_slice())
        .collect::<Vec<_>>();
    PreparedForecastEvidence::try_new(
        request,
        observed_cutoff,
        observed_cutoff,
        decimal_scale,
        observed_history,
        inputs,
    )
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
