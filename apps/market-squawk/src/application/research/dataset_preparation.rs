//! Authority-derived dataset preparation and one-use build-admission receipts.

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use market_squawk_data::{
    AnalyticalGeneration, AnalyticalObservationReadRequest, AnalyticalObservationTemplate,
    AnalyticalReadCapability, AnalyticalReadLimit, ChronologicalSplitPolicy,
    ComponentAdjustmentEvidence, ComponentKind, ComponentScope, ComponentSelector, ComponentValue,
    CorporateActionAdjustment, CorporateActionLimits, CorporateActionPolicy,
    CorporateActionSensitivity, DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy,
    DatasetBuildRequest, DatasetExample, DatasetId, DatasetManifestRef, DatasetOutputAuthorization,
    DatasetSchemaRegistry, FeatureLabelComponentInput, FeatureLabelComponentSpec,
    MissingValuePolicy, ObservationFamilyKey, PointInTimeCandidate, PointInTimeLimits,
    PointInTimePolicy, PointInTimeRevisionMode, PointInTimeService, QueryLimits, QueryResult,
    ResearchArrowBatch, ResearchUse, ResearchUseLimits, RightsBasis, Sha256Digest, UniverseId,
    UniverseLimits, UniverseMembership,
};
use market_squawk_domain::{
    AlternativeDataObservation, DigestAlgorithm, EvidenceDigest, InstrumentId, ResearchObservation,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
    UniverseMembershipObservation,
};
use market_squawk_services::{RequestOrigin, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{ResearchService, application::lifecycle::WorkspaceRuntimeIdentity};

const MAXIMUM_GENERATIONS: usize = 64;
const MAXIMUM_OBSERVATIONS_PER_GENERATION: usize = 4_096;
const MAXIMUM_QUERY_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_OPTIONS: usize = 256;
const MAXIMUM_EXAMPLES: usize = 2_048;
const MAXIMUM_RECEIPTS: usize = 256;
const MAXIMUM_RECEIPT_BYTES: usize = 256 * 1024 * 1024;
const RECEIPT_LIFETIME: Duration = Duration::from_secs(15 * 60);
const QUERY_DURATION: Duration = Duration::from_secs(20);
const BUILD_DURATION: Duration = Duration::from_secs(120);
const DERIVED_RIGHTS_REFERENCE: &str = "https://market-squawk.local/derived-dataset-policy/v1";

/// Closed downstream purpose offered by guided dataset preparation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DatasetPreparationUse {
    LocalAnalysis,
    Train,
}

impl DatasetPreparationUse {
    const fn domain(self) -> ResearchUse {
        match self {
            Self::LocalAnalysis => ResearchUse::LocalAnalysis,
            Self::Train => ResearchUse::Train,
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::LocalAnalysis => 1,
            Self::Train => 2,
        }
    }
}

/// One bounded data-owner-derived choice suitable for a point-in-time feature/label build.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatasetPreparationOption {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) source_dataset: String,
    pub(crate) immutable_generation: u64,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) observed_points: usize,
    pub(crate) examples: usize,
    pub(crate) observed_from: Timestamp,
    pub(crate) observed_through: Timestamp,
    pub(crate) available_uses: Vec<DatasetPreparationUse>,
}

/// Exact bounded option snapshot projected from current immutable analytical authority.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatasetPreparationOptions {
    pub(crate) catalog_generation: String,
    pub(crate) datasets: Vec<DatasetPreparationOption>,
}

/// Closed user choice over an already enumerated option and use.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DatasetPreparationSelection {
    pub(crate) catalog_generation: String,
    pub(crate) dataset: String,
    pub(crate) intended_use: DatasetPreparationUse,
}

/// Trusted application fences used to prepare one exact guided dataset build.
#[derive(Debug)]
pub(crate) struct DatasetPreparationPreviewRequest {
    pub(crate) selection: DatasetPreparationSelection,
    pub(crate) origin: RequestOrigin,
    pub(crate) workspace: WorkspaceRuntimeIdentity,
    pub(crate) now: Instant,
    pub(crate) observed_at: Timestamp,
    pub(crate) deadline: Instant,
    pub(crate) cancellation: CancellationToken,
}

/// Opaque process-local one-use capability for one exact build request.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DatasetPreparationReceipt {
    receipt_id: Uuid,
    preparation_sha256: Sha256Digest,
    expires_at: Timestamp,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetPreparationReceiptWire {
    receipt_id: Uuid,
    preparation_sha256: String,
    expires_at: Timestamp,
}

impl Serialize for DatasetPreparationReceipt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        DatasetPreparationReceiptWire {
            receipt_id: self.receipt_id,
            preparation_sha256: encode_hex(self.preparation_sha256.bytes()),
            expires_at: self.expires_at,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DatasetPreparationReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = DatasetPreparationReceiptWire::deserialize(deserializer)?;
        let bytes = decode_sha256(&wire.preparation_sha256)
            .filter(|bytes| *bytes != [0; 32])
            .ok_or_else(|| serde::de::Error::custom("invalid preparation receipt digest"))?;
        if wire.receipt_id.is_nil() {
            return Err(serde::de::Error::custom(
                "invalid preparation receipt identity",
            ));
        }
        Ok(Self {
            receipt_id: wire.receipt_id,
            preparation_sha256: Sha256Digest::new(bytes),
            expires_at: wire.expires_at,
        })
    }
}

/// Human-readable review paired with an opaque one-use build capability.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DatasetPreparationPreview {
    pub(crate) receipt: DatasetPreparationReceipt,
    pub(crate) dataset: String,
    pub(crate) source: String,
    pub(crate) instrument_id: InstrumentId,
    pub(crate) intended_use: DatasetPreparationUse,
    pub(crate) examples: usize,
    pub(crate) train_examples: usize,
    pub(crate) validation_examples: usize,
    pub(crate) test_examples: usize,
    pub(crate) observed_from: Timestamp,
    pub(crate) observed_through: Timestamp,
    pub(crate) build_spec_sha256: String,
    pub(crate) evidence: Vec<String>,
}

#[derive(Clone, Debug)]
struct PreparedVariant {
    use_case: DatasetPreparationUse,
    request: DatasetBuildRequest,
}

#[derive(Clone, Debug)]
struct PreparedOption {
    summary: DatasetPreparationOption,
    parents: Box<[DatasetManifestRef]>,
    variants: Box<[PreparedVariant]>,
    split_counts: [usize; 3],
}

impl PreparedOption {
    fn variant(&self, use_case: DatasetPreparationUse) -> Option<&PreparedVariant> {
        self.variants
            .iter()
            .find(|variant| variant.use_case == use_case)
    }
}

#[derive(Debug)]
struct PreparedCatalog {
    options: Box<[PreparedOption]>,
    digest: Sha256Digest,
}

impl PreparedCatalog {
    fn option(&self, id: &str) -> Option<&PreparedOption> {
        self.options
            .binary_search_by(|option| option.summary.id.as_str().cmp(id))
            .ok()
            .and_then(|index| self.options.get(index))
    }
}

#[derive(Debug)]
struct StoredPreparation {
    origin: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
    catalog_digest: Sha256Digest,
    option_id: Box<str>,
    use_case: DatasetPreparationUse,
    request: DatasetBuildRequest,
    parents: Box<[DatasetManifestRef]>,
    expires_at: Instant,
    expires_at_wall: Timestamp,
    retained_bytes: usize,
}

#[derive(Debug, Default)]
struct ReceiptRegistry {
    entries: BTreeMap<Uuid, (DatasetPreparationReceipt, StoredPreparation)>,
    retained_bytes: usize,
}

impl ReceiptRegistry {
    fn purge_expired(&mut self, now: Instant) {
        let expired = self
            .entries
            .iter()
            .filter_map(|(id, (_, stored))| (stored.expires_at <= now).then_some(*id))
            .collect::<Vec<_>>();
        for id in expired {
            if let Some((_, stored)) = self.entries.remove(&id) {
                self.retained_bytes = self.retained_bytes.saturating_sub(stored.retained_bytes);
            }
        }
    }

    fn insert(
        &mut self,
        receipt: DatasetPreparationReceipt,
        stored: StoredPreparation,
        now: Instant,
    ) -> Result<(), DatasetPreparationError> {
        self.purge_expired(now);
        let next_bytes = self
            .retained_bytes
            .checked_add(stored.retained_bytes)
            .ok_or(DatasetPreparationError::Capacity)?;
        if self.entries.len() >= MAXIMUM_RECEIPTS || next_bytes > MAXIMUM_RECEIPT_BYTES {
            return Err(DatasetPreparationError::Capacity);
        }
        if self.entries.contains_key(&receipt.receipt_id) {
            return Err(DatasetPreparationError::Conflict);
        }
        self.entries.insert(receipt.receipt_id, (receipt, stored));
        self.retained_bytes = next_bytes;
        Ok(())
    }

    fn consume(
        &mut self,
        receipt: DatasetPreparationReceipt,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
    ) -> Result<StoredPreparation, DatasetPreparationError> {
        let Some((expected, retained)) = self.entries.get(&receipt.receipt_id) else {
            self.purge_expired(now);
            return Err(DatasetPreparationError::NotFound);
        };
        if retained.expires_at <= now {
            if let Some((_, stored)) = self.entries.remove(&receipt.receipt_id) {
                self.retained_bytes = self.retained_bytes.saturating_sub(stored.retained_bytes);
            }
            return Err(DatasetPreparationError::Expired);
        }
        if expected != &receipt
            || retained.origin != origin
            || retained.workspace != workspace
            || retained.expires_at_wall != receipt.expires_at
        {
            return Err(DatasetPreparationError::Unauthorized);
        }
        let (_, stored) = self
            .entries
            .remove(&receipt.receipt_id)
            .ok_or(DatasetPreparationError::NotFound)?;
        self.retained_bytes = self.retained_bytes.saturating_sub(stored.retained_bytes);
        Ok(stored)
    }
}

/// Process-owned guided preparation authority. Restart invalidates every outstanding receipt.
pub(crate) struct DatasetPreparationAuthority {
    research: Arc<ResearchService>,
    reader: AnalyticalReadCapability,
    receipts: Mutex<ReceiptRegistry>,
}

impl DatasetPreparationAuthority {
    #[must_use]
    pub(crate) fn new(research: Arc<ResearchService>) -> Self {
        let reader = research.analytical_reader();
        Self {
            research,
            reader,
            receipts: Mutex::new(ReceiptRegistry::default()),
        }
    }

    /// Lists only builds derivable from complete current source evidence and current rights.
    pub(crate) async fn options(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DatasetPreparationOptions, DatasetPreparationError> {
        let catalog = self.catalog(deadline, cancellation).await?;
        Ok(DatasetPreparationOptions {
            catalog_generation: encode_hex(catalog.digest.bytes()),
            datasets: catalog
                .options
                .iter()
                .map(|option| option.summary.clone())
                .collect(),
        })
    }

    /// Re-derives one exact build, validates its research authority, and retains it for review.
    pub(crate) async fn preview(
        &self,
        request: DatasetPreparationPreviewRequest,
    ) -> Result<DatasetPreparationPreview, DatasetPreparationError> {
        let DatasetPreparationPreviewRequest {
            selection,
            origin,
            workspace,
            now,
            observed_at,
            deadline,
            cancellation,
        } = request;
        ensure_origin(origin, workspace)?;
        let catalog = self.catalog(deadline, cancellation.clone()).await?;
        if selection.catalog_generation != encode_hex(catalog.digest.bytes()) {
            return Err(DatasetPreparationError::StaleCatalog);
        }
        let option = catalog
            .option(&selection.dataset)
            .ok_or(DatasetPreparationError::InvalidSelection)?;
        let variant = option
            .variant(selection.intended_use)
            .ok_or(DatasetPreparationError::InvalidSelection)?;
        self.research
            .analytical()
            .dataset_builder()
            .validate_request_authority(&variant.request, &cancellation)
            .map_err(|_| DatasetPreparationError::Authority)?;
        let expires_at = now
            .checked_add(RECEIPT_LIFETIME)
            .ok_or(DatasetPreparationError::Capacity)?;
        let wall_delta = i64::try_from(RECEIPT_LIFETIME.as_nanos())
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let expires_at_wall = observed_at
            .checked_add_nanos(wall_delta)
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let receipt_id = Uuid::new_v4();
        let preparation_sha256 = receipt_digest(
            receipt_id,
            expires_at_wall,
            origin,
            workspace,
            catalog.digest,
            option.summary.id.as_bytes(),
            selection.intended_use,
            variant.request.build_spec_digest().digest().bytes(),
        );
        let receipt = DatasetPreparationReceipt {
            receipt_id,
            preparation_sha256,
            expires_at: expires_at_wall,
        };
        let retained_bytes = variant
            .request
            .retained_bytes()
            .checked_add(option.parents.len() * std::mem::size_of::<DatasetManifestRef>())
            .ok_or(DatasetPreparationError::Capacity)?;
        self.receipts
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?
            .insert(
                receipt,
                StoredPreparation {
                    origin,
                    workspace,
                    catalog_digest: catalog.digest,
                    option_id: option.summary.id.clone().into(),
                    use_case: selection.intended_use,
                    request: variant.request.clone(),
                    parents: option.parents.clone(),
                    expires_at,
                    expires_at_wall,
                    retained_bytes,
                },
                now,
            )?;
        Ok(DatasetPreparationPreview {
            receipt,
            dataset: option.summary.label.clone(),
            source: format!(
                "{} generation {}",
                option.summary.source_dataset, option.summary.immutable_generation
            ),
            instrument_id: option.summary.instrument_id,
            intended_use: selection.intended_use,
            examples: option.summary.examples,
            train_examples: option.split_counts[0],
            validation_examples: option.split_counts[1],
            test_examples: option.split_counts[2],
            observed_from: option.summary.observed_from,
            observed_through: option.summary.observed_through,
            build_spec_sha256: encode_hex(variant.request.build_spec_digest().digest().bytes()),
            evidence: vec![
                "Values, temporal coordinates, universe membership, and immutable parent generation were derived from canonical persisted observations.".to_owned(),
                "The build uses non-overlapping chronological examples and distinct train, validation, and test intervals.".to_owned(),
                "Current source rights and the exact parent generation are checked again when this one-use receipt is consumed.".to_owned(),
            ],
        })
    }

    /// Consumes once, revalidates every immutable parent and rights fence, and returns the exact
    /// request accepted by the existing dataset runner.
    pub(crate) fn consume(
        &self,
        receipt: DatasetPreparationReceipt,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<DatasetBuildRequest, DatasetPreparationError> {
        ensure_origin(origin, workspace)?;
        let stored = self
            .receipts
            .lock()
            .map_err(|_| DatasetPreparationError::Unavailable)?
            .consume(receipt, origin, workspace, now)?;
        let expected = receipt_digest(
            receipt.receipt_id,
            stored.expires_at_wall,
            stored.origin,
            stored.workspace,
            stored.catalog_digest,
            stored.option_id.as_bytes(),
            stored.use_case,
            stored.request.build_spec_digest().digest().bytes(),
        );
        if expected != receipt.preparation_sha256 {
            return Err(DatasetPreparationError::Unauthorized);
        }
        for parent in &stored.parents {
            let latest = self
                .reader
                .latest(parent.dataset_id(), deadline, cancellation)
                .map_err(|_| DatasetPreparationError::Unavailable)?
                .ok_or(DatasetPreparationError::StaleCatalog)?;
            if latest.manifest() != parent {
                return Err(DatasetPreparationError::StaleCatalog);
            }
        }
        self.research
            .analytical()
            .dataset_builder()
            .validate_request_authority(&stored.request, cancellation)
            .map_err(|_| DatasetPreparationError::Authority)?;
        Ok(stored.request)
    }

    async fn catalog(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedCatalog, DatasetPreparationError> {
        check_control(deadline, &cancellation)?;
        let limit = AnalyticalReadLimit::try_new(MAXIMUM_GENERATIONS)
            .map_err(|_| DatasetPreparationError::Capacity)?;
        let page = self
            .reader
            .datasets(None, limit, deadline, &cancellation)
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        if page.has_more() {
            return Err(DatasetPreparationError::Capacity);
        }
        let canonical = DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let mut options = Vec::new();
        for generation in page
            .generations()
            .iter()
            .filter(|generation| generation.manifest().schema() == &canonical)
        {
            check_control(deadline, &cancellation)?;
            let observations = self
                .observations(generation, deadline, cancellation.child_token())
                .await?;
            derive_generation_options(self, generation, observations, &mut options, &cancellation)?;
            if options.len() > MAXIMUM_OPTIONS {
                return Err(DatasetPreparationError::Capacity);
            }
        }
        options.sort_unstable_by(|left, right| left.summary.id.cmp(&right.summary.id));
        if options
            .windows(2)
            .any(|pair| pair[0].summary.id == pair[1].summary.id)
        {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let digest = catalog_digest(&options);
        Ok(PreparedCatalog {
            options: options.into_boxed_slice(),
            digest,
        })
    }

    async fn observations(
        &self,
        generation: &AnalyticalGeneration,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Vec<ResearchObservation>, DatasetPreparationError> {
        let request = AnalyticalObservationReadRequest::try_new(
            generation.manifest().clone(),
            AnalyticalObservationTemplate::All,
            Vec::new(),
            None,
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let limits = QueryLimits::try_new_with_inline_bytes(
            MAXIMUM_OBSERVATIONS_PER_GENERATION as u64,
            MAXIMUM_QUERY_BYTES as u64,
            MAXIMUM_QUERY_BYTES as u64,
            (MAXIMUM_QUERY_BYTES * 2) as u64,
            4,
            512,
            512,
            QUERY_DURATION,
        )
        .map_err(|_| DatasetPreparationError::Capacity)?;
        let output = self
            .reader
            .read_observations(request, limits, deadline, cancellation)
            .await
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        let QueryResult::Inline { batches, .. } = output.output().result() else {
            return Err(DatasetPreparationError::Capacity);
        };
        let mut observations = Vec::new();
        for batch in batches {
            let remaining = MAXIMUM_QUERY_BYTES
                .checked_sub(estimated_observation_bytes(&observations))
                .ok_or(DatasetPreparationError::Capacity)?;
            let (mut decoded, _) =
                ResearchArrowBatch::decode_record_batch_bounded(batch.clone(), remaining)
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
            observations.append(&mut decoded);
            if observations.len() > MAXIMUM_OBSERVATIONS_PER_GENERATION {
                return Err(DatasetPreparationError::Capacity);
            }
        }
        Ok(observations)
    }
}

impl fmt::Debug for DatasetPreparationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatasetPreparationAuthority")
            .field("research", &"[RESEARCH AUTHORITY]")
            .field("reader", &self.reader)
            .field("receipts", &"[PROCESS-LOCAL ONE-USE RECEIPTS]")
            .finish()
    }
}

impl From<DatasetPreparationError> for ServiceError {
    fn from(value: DatasetPreparationError) -> Self {
        match value {
            DatasetPreparationError::InvalidSelection
            | DatasetPreparationError::InvalidEvidence => Self::InvalidRequest,
            DatasetPreparationError::NotFound | DatasetPreparationError::Expired => Self::NotFound,
            DatasetPreparationError::Unauthorized | DatasetPreparationError::Authority => {
                Self::Unauthorized
            }
            DatasetPreparationError::Conflict | DatasetPreparationError::StaleCatalog => {
                Self::InvalidRequest
            }
            DatasetPreparationError::Capacity => Self::ResourceExhausted,
            DatasetPreparationError::Cancelled | DatasetPreparationError::Unavailable => {
                Self::Unavailable
            }
        }
    }
}

/// Guided preparation, evidence, authority, or receipt failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum DatasetPreparationError {
    #[error("dataset preparation selection is invalid")]
    InvalidSelection,
    #[error("dataset preparation source evidence is invalid")]
    InvalidEvidence,
    #[error("dataset preparation catalog changed")]
    StaleCatalog,
    #[error("dataset preparation receipt was not found")]
    NotFound,
    #[error("dataset preparation receipt expired")]
    Expired,
    #[error("dataset preparation receipt is not authorized")]
    Unauthorized,
    #[error("dataset preparation source rights do not authorize this build")]
    Authority,
    #[error("dataset preparation receipt conflicts with retained authority")]
    Conflict,
    #[error("dataset preparation capacity was exceeded")]
    Capacity,
    #[error("dataset preparation was cancelled or exceeded its deadline")]
    Cancelled,
    #[error("dataset preparation authority is unavailable")]
    Unavailable,
}

#[derive(Clone)]
struct SeriesPoint {
    observation: AlternativeDataObservation,
    effective: ResearchTemporalCoordinate,
    available_at: Timestamp,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct SeriesKey {
    instrument_id: InstrumentId,
    source_id: SourceId,
    dataset: SourceIdentifier,
    field: SourceIdentifier,
    unit: Option<SourceIdentifier>,
}

fn derive_generation_options(
    authority: &DatasetPreparationAuthority,
    generation: &AnalyticalGeneration,
    observations: Vec<ResearchObservation>,
    output: &mut Vec<PreparedOption>,
    cancellation: &CancellationToken,
) -> Result<(), DatasetPreparationError> {
    let memberships = observations
        .iter()
        .filter_map(|observation| match observation {
            ResearchObservation::UniverseMembership(value) => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let mut series: BTreeMap<SeriesKey, Vec<SeriesPoint>> = BTreeMap::new();
    for observation in observations {
        let ResearchObservation::AlternativeData(value) = observation else {
            continue;
        };
        let context = value.context();
        let Some(instrument_id) = context.provenance().instrument_id() else {
            continue;
        };
        let Some(available_at) = context
            .provenance()
            .availability()
            .conservative_available_at()
        else {
            continue;
        };
        let key = SeriesKey {
            instrument_id,
            source_id: context.provenance().source_id().clone(),
            dataset: value.dataset().clone(),
            field: value.field().clone(),
            unit: value.unit().cloned(),
        };
        series.entry(key).or_default().push(SeriesPoint {
            effective: context.time().effective().clone(),
            available_at,
            observation: value,
        });
    }
    for (key, mut points) in series {
        points.sort_by(|left, right| {
            left.effective
                .partial_cmp(&right.effective)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.available_at.cmp(&right.available_at))
                .then_with(|| {
                    left.observation
                        .context()
                        .time()
                        .revision()
                        .get()
                        .cmp(&right.observation.context().time().revision().get())
                })
        });
        let mut canonical = Vec::new();
        for point in points {
            if canonical
                .last()
                .is_some_and(|previous: &SeriesPoint| previous.effective == point.effective)
            {
                continue;
            }
            canonical.push(point);
        }
        if canonical.len() < 6
            || canonical.windows(2).any(|pair| {
                !matches!(
                    pair[1].effective.partial_cmp(&pair[0].effective),
                    Some(Ordering::Greater)
                ) || pair[1].available_at <= pair[0].available_at
            })
        {
            continue;
        }
        canonical.truncate(MAXIMUM_EXAMPLES.saturating_mul(2));
        if let Some(option) = build_option(
            authority,
            generation,
            &memberships,
            key,
            &canonical,
            cancellation,
        )? {
            output.push(option);
        }
    }
    Ok(())
}

fn build_option(
    authority: &DatasetPreparationAuthority,
    generation: &AnalyticalGeneration,
    memberships: &[UniverseMembershipObservation],
    key: SeriesKey,
    points: &[SeriesPoint],
    cancellation: &CancellationToken,
) -> Result<Option<PreparedOption>, DatasetPreparationError> {
    let example_count = points.len() / 2;
    if example_count < 3 {
        return Ok(None);
    }
    let train_count = example_count / 3;
    let validation_count = example_count / 3;
    let test_count = example_count
        .checked_sub(train_count + validation_count)
        .ok_or(DatasetPreparationError::Capacity)?;
    if train_count == 0 || validation_count == 0 || test_count == 0 {
        return Ok(None);
    }
    let option_identity = series_identity(generation.manifest(), &key);
    let feature_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        component_name("feature", &key.field, option_identity),
        NonZeroU32::MIN,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let label_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        component_name("label", &key.field, option_identity),
        NonZeroU32::MIN,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let mut examples = Vec::with_capacity(example_count);
    for (index, pair) in points.chunks_exact(2).enumerate() {
        let feature = &pair[0];
        let label = &pair[1];
        examples.push(
            DatasetExample::try_new_with_temporal_cutoffs(
                format!("guided-{}-{index:05}", short_hex(option_identity)),
                key.instrument_id,
                feature.available_at,
                label.available_at,
                feature.effective.clone(),
                label.effective.clone(),
                vec![
                    component_input(feature_spec.clone(), feature)?,
                    component_input(label_spec.clone(), label)?,
                ],
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        );
    }
    let train_end = examples[train_count - 1].label_cutoff_at();
    let validation_end = examples[train_count + validation_count - 1].label_cutoff_at();
    let test_end = examples
        .last()
        .map(DatasetExample::label_cutoff_at)
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let first_cutoff = examples
        .first()
        .map(DatasetExample::cutoff_at)
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let membership = membership_evidence(
        generation.manifest(),
        memberships,
        key.instrument_id,
        first_cutoff,
        test_end,
        cancellation,
    )?;
    let Some((universe_id, membership)) = membership else {
        return Ok(None);
    };
    let inputs = DatasetBuildInputs::try_new(
        vec![generation.manifest().clone()],
        universe_id,
        vec![membership],
        vec![feature_spec, label_spec],
        examples,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let policy = DatasetBuildPolicy::new(
        ChronologicalSplitPolicy::try_new(train_end, validation_end, test_end)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        CorporateActionPolicy::new(CorporateActionAdjustment::Raw, NonZeroU32::MIN),
        MissingValuePolicy::Reject,
        SourceIdentifier::try_from("guided-dataset-preparation-v1")
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
    );
    let mut variants = Vec::new();
    for use_case in [
        DatasetPreparationUse::LocalAnalysis,
        DatasetPreparationUse::Train,
    ] {
        let request = dataset_request(
            option_identity,
            use_case,
            inputs.clone(),
            policy.clone(),
            example_count,
        )?;
        if authority
            .research
            .analytical()
            .dataset_builder()
            .validate_request_authority(&request, cancellation)
            .is_ok()
        {
            variants.push(PreparedVariant { use_case, request });
        }
    }
    if variants.is_empty() {
        return Ok(None);
    }
    let option_id = format!("dataset-{}", short_hex(option_identity));
    let observed_from = points
        .first()
        .map(|point| point.available_at)
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let observed_through = points
        .get(example_count * 2 - 1)
        .map(|point| point.available_at)
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    Ok(Some(PreparedOption {
        summary: DatasetPreparationOption {
            id: option_id,
            label: format!("{} · {}", key.dataset.as_str(), key.field.as_str()),
            source_dataset: generation.manifest().dataset_id().as_str().to_owned(),
            immutable_generation: generation.manifest().manifest_version(),
            instrument_id: key.instrument_id,
            observed_points: example_count * 2,
            examples: example_count,
            observed_from,
            observed_through,
            available_uses: variants.iter().map(|variant| variant.use_case).collect(),
        },
        parents: vec![generation.manifest().clone()].into_boxed_slice(),
        variants: variants.into_boxed_slice(),
        split_counts: [train_count, validation_count, test_count],
    }))
}

fn component_input(
    spec: FeatureLabelComponentSpec,
    point: &SeriesPoint,
) -> Result<FeatureLabelComponentInput, DatasetPreparationError> {
    let context = point.observation.context();
    let family = ObservationFamilyKey::AlternativeData {
        source_id: context.provenance().source_id().clone(),
        instrument_id: context.provenance().instrument_id(),
        source_record: context.provenance().source_identifier().clone(),
        dataset: point.observation.dataset().clone(),
        field: point.observation.field().clone(),
        effective: point.effective.clone(),
    };
    FeatureLabelComponentInput::try_new(
        spec,
        ComponentValue::decimal(
            point.observation.value(),
            point.observation.unit().cloned(),
            None,
        )
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        vec![ComponentSelector::new(family)],
        ComponentAdjustmentEvidence::Raw,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn membership_evidence(
    manifest: &DatasetManifestRef,
    memberships: &[UniverseMembershipObservation],
    instrument_id: InstrumentId,
    first_cutoff: Timestamp,
    final_cutoff: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Option<(UniverseId, UniverseMembership)>, DatasetPreparationError> {
    for observed in memberships {
        let context = observed.context();
        let provenance = context.provenance();
        if provenance.instrument_id() != Some(instrument_id)
            || provenance
                .availability()
                .conservative_available_at()
                .is_none_or(|available| available > first_cutoff)
            || observed.effective_interval().starts_at() > first_cutoff
            || observed
                .effective_interval()
                .ends_at()
                .is_some_and(|end| end <= final_cutoff)
        {
            continue;
        }
        let Ok(universe) = UniverseId::try_from(observed.universe().as_str()) else {
            continue;
        };
        let identity_deadline = Instant::now()
            .checked_add(Duration::from_secs(1))
            .ok_or(DatasetPreparationError::Capacity)?;
        let candidate = PointInTimeCandidate::new(
            ResearchObservation::UniverseMembership(observed.clone()),
            manifest.clone(),
        );
        let identity = PointInTimeService::new()
            .payload_identity(&candidate, cancellation, identity_deadline)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        return Ok(Some((
            universe,
            UniverseMembership::new(
                instrument_id,
                observed.effective_interval(),
                provenance.availability().clone(),
                manifest.clone(),
                EvidenceDigest::new(DigestAlgorithm::Sha256, identity.bytes()),
            ),
        )));
    }
    Ok(None)
}

fn dataset_request(
    identity: Sha256Digest,
    use_case: DatasetPreparationUse,
    inputs: DatasetBuildInputs,
    policy: DatasetBuildPolicy,
    examples: usize,
) -> Result<DatasetBuildRequest, DatasetPreparationError> {
    let output_id = format!(
        "prepared.{}.{}",
        short_hex(identity),
        match use_case {
            DatasetPreparationUse::LocalAnalysis => "analysis",
            DatasetPreparationUse::Train => "train",
        }
    );
    let authorization = derived_authorization(identity, use_case)?;
    let output_rows = examples
        .checked_mul(2)
        .ok_or(DatasetPreparationError::Capacity)?;
    DatasetBuildRequest::try_new(
        DatasetId::try_from(output_id.as_str())
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        inputs,
        policy,
        use_case.domain(),
        ResearchUseLimits::try_new(
            4,
            16_384,
            65_536,
            4_096,
            64 * 1024 * 1024,
            Duration::from_secs(30),
            Duration::from_secs(5 * 60),
        )
        .map_err(|_| DatasetPreparationError::Capacity)?,
        authorization,
        DatasetBuildLimits::try_new(
            MAXIMUM_OBSERVATIONS_PER_GENERATION,
            examples,
            2,
            output_rows,
            128 * 1024 * 1024,
            BUILD_DURATION,
            PointInTimeLimits::try_new(
                MAXIMUM_OBSERVATIONS_PER_GENERATION,
                MAXIMUM_OBSERVATIONS_PER_GENERATION,
                256,
                MAXIMUM_OBSERVATIONS_PER_GENERATION,
                64 * 1024 * 1024,
            )
            .map_err(|_| DatasetPreparationError::Capacity)?,
            UniverseLimits::try_new(64, 4 * 1024 * 1024)
                .map_err(|_| DatasetPreparationError::Capacity)?,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(64).ok_or(DatasetPreparationError::Capacity)?,
                NonZeroUsize::new(4 * 1024 * 1024).ok_or(DatasetPreparationError::Capacity)?,
            )
            .map_err(|_| DatasetPreparationError::Capacity)?,
        )
        .map_err(|_| DatasetPreparationError::Capacity)?,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn derived_authorization(
    identity: Sha256Digest,
    use_case: DatasetPreparationUse,
) -> Result<DatasetOutputAuthorization, DatasetPreparationError> {
    let terms = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(b"market-squawk/derived-dataset-policy/v1").into(),
    );
    let mut authorization = Sha256::new();
    authorization.update(b"market-squawk/guided-dataset-output-authorization/v1");
    authorization.update(identity.bytes());
    authorization.update([use_case.tag()]);
    DatasetOutputAuthorization::try_new(
        SourceId::try_from("market-squawk.derived")
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        RightsBasis::reviewed_terms(DERIVED_RIGHTS_REFERENCE, terms)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        EvidenceDigest::new(DigestAlgorithm::Sha256, authorization.finalize().into()),
        None,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn series_identity(manifest: &DatasetManifestRef, key: &SeriesKey) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/guided-dataset-series/v1");
    digest.update(manifest.content_hash().bytes());
    digest.update(key.instrument_id.as_uuid().as_bytes());
    update_text(&mut digest, key.source_id.as_str());
    update_text(&mut digest, key.dataset.as_str());
    update_text(&mut digest, key.field.as_str());
    update_text(
        &mut digest,
        key.unit.as_ref().map_or("", SourceIdentifier::as_str),
    );
    Sha256Digest::new(digest.finalize().into())
}

fn component_name(prefix: &str, field: &SourceIdentifier, identity: Sha256Digest) -> String {
    let mut name = String::with_capacity(prefix.len() + field.as_str().len() + 18);
    name.push_str(prefix);
    name.push('-');
    for byte in field.as_str().bytes().take(192) {
        name.push(
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
                char::from(byte)
            } else {
                '_'
            },
        );
    }
    name.push('-');
    name.push_str(&short_hex(identity));
    name
}

fn catalog_digest(options: &[PreparedOption]) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/guided-dataset-catalog/v1");
    digest.update((options.len() as u64).to_be_bytes());
    for option in options {
        update_text(&mut digest, &option.summary.id);
        for variant in &option.variants {
            digest.update([variant.use_case.tag()]);
            digest.update(variant.request.build_spec_digest().digest().bytes());
        }
    }
    Sha256Digest::new(digest.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "every receipt authority fence remains explicit"
)]
fn receipt_digest(
    receipt_id: Uuid,
    expires_at: Timestamp,
    origin: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
    catalog_digest: Sha256Digest,
    option_id: &[u8],
    use_case: DatasetPreparationUse,
    build_spec: [u8; 32],
) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/guided-dataset-receipt/v1");
    digest.update(receipt_id.as_bytes());
    digest.update(expires_at.unix_nanos().to_be_bytes());
    digest.update(origin.workspace_id().as_bytes());
    digest.update(origin.client_id().as_bytes());
    digest.update(workspace.workspace_id().as_uuid().as_bytes());
    digest.update(workspace.generation().get().to_be_bytes());
    digest.update(catalog_digest.bytes());
    digest.update((option_id.len() as u64).to_be_bytes());
    digest.update(option_id);
    digest.update([use_case.tag()]);
    digest.update(build_spec);
    Sha256Digest::new(digest.finalize().into())
}

fn ensure_origin(
    origin: RequestOrigin,
    workspace: WorkspaceRuntimeIdentity,
) -> Result<(), DatasetPreparationError> {
    if origin.workspace_id() == workspace.workspace_id().as_uuid() {
        Ok(())
    } else {
        Err(DatasetPreparationError::Unauthorized)
    }
}

fn check_control(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), DatasetPreparationError> {
    if cancellation.is_cancelled() || Instant::now() >= deadline {
        Err(DatasetPreparationError::Cancelled)
    } else {
        Ok(())
    }
}

fn estimated_observation_bytes(observations: &[ResearchObservation]) -> usize {
    observations
        .len()
        .saturating_mul(std::mem::size_of::<ResearchObservation>().saturating_add(512))
}

fn update_text(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn short_hex(digest: Sha256Digest) -> String {
    let bytes = digest.bytes();
    let mut short = [0_u8; 8];
    short.copy_from_slice(&bytes[..8]);
    encode_hex(short)
}

fn encode_hex<const N: usize>(bytes: [u8; N]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(N * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn decode_sha256(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        output[index] = (high << 4) | low;
    }
    Some(output)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}
