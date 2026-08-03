//! Durable forecast generation, immutable vintages, and realized-outcome presentation.

use std::{num::NonZeroUsize, path::Path, sync::Arc};

use market_squawk_data::Sha256Digest;
use market_squawk_modeling::{ForecastOutcome, ForecastPath, ForecastVintage};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReference,
    ArtifactRepository,
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex;

use persistence::{
    ForecastIndex, ForecastPayloadRecord, OutcomeRecord, VintageRecord, digest_from_hex, hex,
    validate_digest,
};

use super::runtime::{
    ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
    RetainedRuntimeBackup,
};

mod generation;
pub(in crate::application::model) mod persistence;

/// Starts durable forecast generation through the installed job authority.
pub const START_FORECAST: &str = "Model.StartForecast";
/// Executes one already admitted forecast request inside the durable job runner.
pub const GENERATE_FORECAST: &str = "Model.GenerateForecast";
/// Reads one exact immutable forecast vintage.
pub const GET_FORECAST: &str = "Model.GetForecast";
/// Lists bounded immutable forecast-vintage summaries.
pub const LIST_FORECASTS: &str = "Model.ListForecasts";
/// Reads bounded immutable outcomes appended to one vintage.
pub const GET_FORECAST_OUTCOMES: &str = "Model.GetForecastOutcomes";

const INDEX_SCHEMA_VERSION: u32 = 1;
const MAXIMUM_VINTAGES: usize = 100_000;
const MAXIMUM_OUTCOMES: usize = 1_000_000;
const MAXIMUM_DRIFT_OUTCOMES: usize = 4_096;

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
        created_at: market_squawk_domain::Timestamp,
        expires_at: market_squawk_domain::Timestamp,
        context: ArtifactPublicationContext,
    ) -> Result<Value, ForecastApplicationError> {
        let _publication = self.publication.lock().await;
        if let Some(existing) = self.vintage_for_request(request_hash).await {
            return self.get_forecast(&existing.vintage_id).await;
        }
        context.ensure_live()?;
        let payload = ForecastPayloadRecord::from_path(&path, created_at, expires_at)?;
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
        self.get_forecast(&record.vintage_id).await
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

    /// Returns a complete stored model-risk presentation for one exact vintage.
    pub async fn get_forecast(&self, id: &str) -> Result<Value, ForecastApplicationError> {
        validate_digest(id)?;
        let index = self.index.lock().await;
        let vintage = index
            .vintages
            .iter()
            .find(|value| value.vintage_id == id)
            .ok_or(ForecastApplicationError::NotFound)?;
        let mut value = serde_json::to_value(vintage)
            .map_err(|_error| ForecastApplicationError::CorruptIndex)?;
        let object = value
            .as_object_mut()
            .ok_or(ForecastApplicationError::CorruptIndex)?;
        object.insert(
            "driftMonitoring".to_owned(),
            drift_monitoring_value(&index, vintage)?,
        );
        Ok(value)
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
            .map(VintageRecord::summary)
            .collect::<Vec<_>>();
        Ok(json!({
            "forecasts": records,
            "available": available,
            "truncated": records.len() < available,
        }))
    }

    /// Returns immutable stored outcomes for one exact vintage.
    pub async fn get_forecast_outcomes(
        &self,
        id: &str,
        maximum: NonZeroUsize,
    ) -> Result<Value, ForecastApplicationError> {
        validate_digest(id)?;
        let index = self.index.lock().await;
        if !index.vintages.iter().any(|value| value.vintage_id == id) {
            return Err(ForecastApplicationError::NotFound);
        }
        let available = index
            .outcomes
            .iter()
            .filter(|value| value.vintage_id == id)
            .count();
        let outcomes = index
            .outcomes
            .iter()
            .filter(|value| value.vintage_id == id)
            .take(maximum.get().min(self.limits.maximum_outcomes.get()))
            .collect::<Vec<_>>();
        Ok(json!({
            "vintageId": id,
            "outcomes": outcomes,
            "available": available,
            "truncated": outcomes.len() < available,
        }))
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
        "awaiting_observed_outcomes"
    } else {
        "outcome_error_observed"
    };
    Ok(json!({
        "status": state,
        "basis": "immutable_forecast_outcomes",
        "observedOutcomeCount": observed,
        "includedOutcomeCount": included,
        "truncated": observed > included,
        "absoluteErrorMantissaTotal": total_absolute_error.to_string(),
        "meanAbsoluteErrorMantissa": if included == 0 {
            None
        } else {
            Some((total_absolute_error / i128::try_from(included).map_err(|_| ForecastApplicationError::CorruptIndex)?).to_string())
        },
        "decimalScale": scale,
        "thresholdState": "not_configured",
        "interpretation": "Observed outcome error is monitoring evidence, not a future-performance guarantee."
    }))
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
    /// Controlled forecast payload publication failed.
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
