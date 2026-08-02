//! Durable forecast generation, immutable vintages, and realized-outcome presentation.

use std::{num::NonZeroUsize, path::Path, sync::Arc};

use market_squawk_data::Sha256Digest;
use market_squawk_modeling::{ForecastOutcome, ForecastPath, ForecastVintage};
use market_squawk_platform::{LocalAuthorityStateStore, LocalAuthorityStateStoreError};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRepository,
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex;

use persistence::{
    ForecastIndex, ForecastPayloadRecord, OutcomeRecord, VintageRecord, digest_from_hex, hex,
    validate_digest,
};

mod generation;
mod persistence;

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
            return serde_json::to_value(existing)
                .map_err(|_error| ForecastApplicationError::CorruptIndex);
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
        serde_json::to_value(record).map_err(|_error| ForecastApplicationError::CorruptIndex)
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
        serde_json::to_value(vintage).map_err(|_error| ForecastApplicationError::CorruptIndex)
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
}
