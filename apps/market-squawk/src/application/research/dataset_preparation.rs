//! Authority-derived dataset preparation and one-use build-admission receipts.

use std::{
    collections::BTreeMap,
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::{DateTime, Datelike, Utc};
use market_squawk_data::{
    AdjustmentStep, AnalyticalGeneration, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadCapability, AnalyticalReadLimit,
    ChronologicalSplitPolicy, ComponentAdjustmentEvidence, ComponentKind, ComponentScope,
    ComponentSelector, ComponentValue, CorporateActionAdjustment, CorporateActionLimits,
    CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord, CorporateActionSensitivity,
    DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildRequest,
    DatasetExample, DatasetId, DatasetManifestRef, DatasetOutputAuthorization,
    DatasetSchemaRegistry, FEATURE_LABEL_RETURN_UNIT, FeatureDatasetProductContract,
    FeatureDatasetProductionError, FeatureDatasetProductionProofV1,
    FeatureDatasetProductionPublication, FeatureDatasetProductionPublisher,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, FeatureLabelDataset, MissingValuePolicy,
    ObservationFamilyKey, PointInTimeCandidate, PointInTimeLimits, PointInTimePolicy,
    PointInTimeRequest, PointInTimeRevisionMode, PointInTimeService, QueryLimits, QueryResult,
    ResearchArrowBatch, ResearchUse, ResearchUseLimits, RightsBasis, Sha256Digest, UniverseId,
    UniverseLimits, UniverseMembership,
};
use market_squawk_domain::{
    BarTimestampBasis, CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId,
    MarketBarAdjustment, MarketBarObservation, MarketBarSessionEvidence, ProviderInstrumentId,
    ResearchObservation, ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
    UniverseMembershipObservation, VenueId,
};
use market_squawk_services::{RequestOrigin, ServiceError};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    macro_context::MacroContextReadCapability,
    macro_features::{MacroFeatureVector, read_macro_feature_vector},
};
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
const SPLIT_RETURN_KERNEL_REVISION: &str = "market-squawk/split-adjusted-price-return-kernel/v1";

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
    production: PreparedProductionEvidence,
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
    production: PreparedProductionEvidence,
    expires_at: Instant,
    expires_at_wall: Timestamp,
    retained_bytes: usize,
}

#[derive(Clone, Debug)]
struct PreparedProductionEvidence {
    universe_membership_content: EvidenceDigest,
    universe_membership_audit: EvidenceDigest,
    instrument_population_query: EvidenceDigest,
    instrument_population_receipt: EvidenceDigest,
    completed_session_request: EvidenceDigest,
    completed_session_receipt: EvidenceDigest,
    feature_point_in_time_content: EvidenceDigest,
    feature_point_in_time_audit: EvidenceDigest,
    macro_context_evidence: EvidenceDigest,
    macro_parent_manifests: Box<[DatasetManifestRef]>,
    label_point_in_time_content: EvidenceDigest,
    label_point_in_time_audit: EvidenceDigest,
    return_kernel_output: EvidenceDigest,
}

/// Exact phase-one request paired with a one-use product-finalization handoff.
pub(crate) struct PreparedFeatureDatasetBuild {
    request: DatasetBuildRequest,
    finalizer: FeatureDatasetProductionFinalizer,
}

impl PreparedFeatureDatasetBuild {
    /// Separates the existing phase-one build input from the sole post-build finalizer.
    pub(crate) fn into_parts(self) -> (DatasetBuildRequest, FeatureDatasetProductionFinalizer) {
        (self.request, self.finalizer)
    }
}

/// One-use evidence handoff to the composition-owned sole production publisher.
pub(crate) struct FeatureDatasetProductionFinalizer {
    contract: FeatureDatasetProductContract,
    build_spec: Sha256Digest,
    evidence: PreparedProductionEvidence,
}

impl FeatureDatasetProductionFinalizer {
    /// Finalizes the unchanged phase-one result without creating or retaining publisher authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "publication authority and currentness coordinates remain explicit"
    )]
    pub(crate) fn publish(
        self,
        research: &ResearchService,
        publisher: &FeatureDatasetProductionPublisher,
        request: &DatasetBuildRequest,
        dataset: &FeatureLabelDataset,
        attested_at: Timestamp,
        currentness_expires_at: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<FeatureDatasetProductionPublication, FeatureDatasetProductionError> {
        if request.build_spec_digest().digest() != self.build_spec {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        let currentness = evidence_digest(
            b"market-squawk/completed-session-currentness/v1",
            &[
                EvidencePart::Timestamp(attested_at),
                EvidencePart::Timestamp(currentness_expires_at),
                EvidencePart::Digest(self.evidence.completed_session_receipt),
            ],
        );
        let proof = FeatureDatasetProductionProofV1::try_from_request_evidence(
            request,
            self.evidence.universe_membership_content,
            self.evidence.universe_membership_audit,
            self.evidence.instrument_population_query,
            self.evidence.instrument_population_receipt,
            self.evidence.completed_session_request,
            self.evidence.completed_session_receipt,
            currentness,
            self.evidence.feature_point_in_time_content,
            self.evidence.feature_point_in_time_audit,
            self.evidence.macro_context_evidence,
            self.evidence.macro_parent_manifests.into_vec(),
            self.evidence.label_point_in_time_content,
            self.evidence.label_point_in_time_audit,
            self.evidence.return_kernel_output,
            attested_at,
            currentness_expires_at,
        )?;
        publisher.publish(
            research.analytical(),
            self.contract,
            request,
            dataset,
            proof,
            cancellation,
        )
    }
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
    macro_context: MacroContextReadCapability,
    receipts: Mutex<ReceiptRegistry>,
}

impl DatasetPreparationAuthority {
    #[must_use]
    pub(crate) fn new(
        research: Arc<ResearchService>,
        macro_context: MacroContextReadCapability,
    ) -> Self {
        let reader = research.analytical_reader();
        Self {
            research,
            reader,
            macro_context,
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
                    production: option.production.clone(),
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
    /// request and one-use finalization handoff accepted by the existing dataset runner.
    pub(crate) fn consume(
        &self,
        receipt: DatasetPreparationReceipt,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
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
        let contract = match stored.use_case {
            DatasetPreparationUse::LocalAnalysis => {
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1
            }
            DatasetPreparationUse::Train => {
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
            }
        };
        let build_spec = stored.request.build_spec_digest().digest();
        Ok(PreparedFeatureDatasetBuild {
            request: stored.request,
            finalizer: FeatureDatasetProductionFinalizer {
                contract,
                build_spec,
                evidence: stored.production,
            },
        })
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
        let mut generations = Vec::new();
        for generation in page
            .generations()
            .iter()
            .filter(|generation| generation.manifest().schema() == &canonical)
        {
            check_control(deadline, &cancellation)?;
            let observations = self
                .observations(generation, deadline, cancellation.child_token())
                .await?;
            generations.push((generation.clone(), observations));
        }
        let support = CanonicalSupport::from_generations(&generations)?;
        let mut macro_cache = BTreeMap::new();
        let mut options = Vec::new();
        for (generation, observations) in &generations {
            derive_generation_options(
                self,
                generation,
                observations,
                &support,
                &mut macro_cache,
                &mut options,
                deadline,
                &cancellation,
            )
            .await?;
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
            let (mut decoded, _) = ResearchArrowBatch::decode_query_projection_bounded(
                batch.clone(),
                remaining,
            )
            .map_err(|error| {
                tracing::warn!(
                    dataset = generation.manifest().dataset_id().as_str(),
                    generation = generation.manifest().manifest_version(),
                    error = ?error,
                    "canonical research generation could not be decoded for guided dataset preparation"
                );
                DatasetPreparationError::InvalidEvidence
            })?;
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
            .field("macro_context", &"[NEUTRAL MACRO READ CAPABILITY]")
            .field("receipts", &"[PROCESS-LOCAL ONE-USE RECEIPTS]")
            .finish()
    }
}

impl From<DatasetPreparationError> for ServiceError {
    fn from(value: DatasetPreparationError) -> Self {
        match value {
            DatasetPreparationError::InvalidSelection => Self::InvalidRequest,
            DatasetPreparationError::InvalidEvidence => Self::InvalidResult,
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
struct CanonicalMembership {
    observation: UniverseMembershipObservation,
    manifest: DatasetManifestRef,
}

struct CanonicalSupport {
    memberships: Box<[CanonicalMembership]>,
    actions: Box<[PointInTimeCandidate]>,
}

impl CanonicalSupport {
    fn from_generations(
        generations: &[(AnalyticalGeneration, Vec<ResearchObservation>)],
    ) -> Result<Self, DatasetPreparationError> {
        let mut memberships = Vec::new();
        let mut actions = Vec::new();
        for (generation, observations) in generations {
            for observation in observations {
                match observation {
                    ResearchObservation::UniverseMembership(observation) => {
                        memberships.push(CanonicalMembership {
                            observation: observation.clone(),
                            manifest: generation.manifest().clone(),
                        });
                    }
                    ResearchObservation::CorporateAction(observation) => {
                        actions.push(PointInTimeCandidate::new(
                            ResearchObservation::CorporateAction(observation.clone()),
                            generation.manifest().clone(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        if memberships.len() > MAXIMUM_GENERATIONS * MAXIMUM_OBSERVATIONS_PER_GENERATION
            || actions.len() > MAXIMUM_GENERATIONS * MAXIMUM_OBSERVATIONS_PER_GENERATION
        {
            return Err(DatasetPreparationError::Capacity);
        }
        Ok(Self {
            memberships: memberships.into_boxed_slice(),
            actions: actions.into_boxed_slice(),
        })
    }

    fn actions_for(&self, instrument_id: InstrumentId) -> Vec<PointInTimeCandidate> {
        self.actions
            .iter()
            .filter(|candidate| {
                let ResearchObservation::CorporateAction(observation) = candidate.observation()
                else {
                    return false;
                };
                observation.context().provenance().instrument_id() == Some(instrument_id)
            })
            .cloned()
            .collect()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct MarketSeriesKey {
    instrument_id: InstrumentId,
    source_id: SourceId,
    venue_id: VenueId,
    provider_instrument_id: ProviderInstrumentId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    timestamp_basis: BarTimestampBasis,
    session: MarketBarSessionEvidence,
    currency: market_squawk_domain::Currency,
}

#[derive(Clone)]
struct MarketSeriesPoint {
    observation: MarketBarObservation,
    effective: Timestamp,
    available_at: Timestamp,
}

struct MarketSeries {
    key: MarketSeriesKey,
    identity: Sha256Digest,
    points: Vec<MarketSeriesPoint>,
}

async fn derive_generation_options(
    authority: &DatasetPreparationAuthority,
    generation: &AnalyticalGeneration,
    observations: &[ResearchObservation],
    support: &CanonicalSupport,
    macro_cache: &mut BTreeMap<(Timestamp, CalendarDate), MacroFeatureVector>,
    output: &mut Vec<PreparedOption>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), DatasetPreparationError> {
    let mut series: BTreeMap<Sha256Digest, MarketSeries> = BTreeMap::new();
    for observation in observations {
        let ResearchObservation::MarketBar(value) = observation else {
            continue;
        };
        if value.adjustment() != MarketBarAdjustment::Raw {
            continue;
        }
        let context = value.context();
        let Some(instrument_id) = context.provenance().instrument_id() else {
            continue;
        };
        let Some(venue_id) = context.provenance().venue_id().cloned() else {
            continue;
        };
        let Some(effective) = context.time().effective().exact_timestamp() else {
            continue;
        };
        let Some(available_at) = context
            .provenance()
            .availability()
            .conservative_available_at()
        else {
            continue;
        };
        if available_at < value.completed_at() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        let key = MarketSeriesKey {
            instrument_id,
            source_id: context.provenance().source_id().clone(),
            venue_id,
            provider_instrument_id: value.provider_instrument_id().clone(),
            feed: value.feed().clone(),
            interval: value.interval().clone(),
            timestamp_basis: value.time_semantics().timestamp_basis(),
            session: value.time_semantics().session().clone(),
            currency: value.currency(),
        };
        let identity = market_series_identity(generation.manifest(), &key);
        let retained = series.entry(identity).or_insert_with(|| MarketSeries {
            key: key.clone(),
            identity,
            points: Vec::new(),
        });
        if retained.key != key {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        retained.points.push(MarketSeriesPoint {
            effective,
            available_at,
            observation: value.clone(),
        });
    }
    for (_, mut series) in series {
        check_control(deadline, cancellation)?;
        let points = &mut series.points;
        points.sort_by(|left, right| {
            left.effective
                .cmp(&right.effective)
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
        for point in points.drain(..) {
            if canonical
                .last()
                .is_some_and(|previous: &MarketSeriesPoint| previous.effective == point.effective)
            {
                let _ = canonical.pop();
            }
            canonical.push(point);
        }
        if canonical.len() < 9
            || canonical.windows(2).any(|pair| {
                pair[1].effective <= pair[0].effective
                    || pair[1].available_at <= pair[0].available_at
            })
        {
            continue;
        }
        canonical.truncate(MAXIMUM_EXAMPLES.saturating_mul(3));
        if let Some(option) = build_option(
            authority,
            generation,
            support,
            macro_cache,
            series.key,
            series.identity,
            &canonical,
            deadline,
            cancellation,
        )
        .await?
        {
            output.push(option);
        }
    }
    Ok(())
}

async fn build_option(
    authority: &DatasetPreparationAuthority,
    generation: &AnalyticalGeneration,
    support: &CanonicalSupport,
    macro_cache: &mut BTreeMap<(Timestamp, CalendarDate), MacroFeatureVector>,
    key: MarketSeriesKey,
    option_identity: Sha256Digest,
    points: &[MarketSeriesPoint],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<PreparedOption>, DatasetPreparationError> {
    let mut horizons: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for (chunk_index, triple) in points.chunks_exact(3).enumerate() {
        let horizon = triple[2]
            .effective
            .unix_nanos()
            .checked_sub(triple[1].effective.unix_nanos())
            .and_then(|value| u64::try_from(value).ok());
        let Some(horizon) = horizon.filter(|value| *value > 0) else {
            continue;
        };
        horizons.entry(horizon).or_default().push(chunk_index * 3);
    }
    let selected = horizons
        .into_iter()
        .fold(None, |selected, candidate| match selected {
            None => Some(candidate),
            Some(current) if candidate.1.len() > current.1.len() => Some(candidate),
            Some(current) if candidate.1.len() == current.1.len() && candidate.0 < current.0 => {
                Some(candidate)
            }
            Some(current) => Some(current),
        });
    let Some((_fixed_horizon_nanos, starts)) = selected else {
        return Ok(None);
    };
    let example_count = starts.len();
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
    let contract =
        FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnAnalysisV1;
    let feature_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        contract.feature_component_name(),
        NonZeroU32::MIN,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let label_spec = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Instrument,
        CorporateActionSensitivity::RequiresAdjustment,
        contract.label_component_name(),
        NonZeroU32::MIN,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let point_in_time_policy =
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let corporate_action_policy =
        CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN);
    let action_candidates = support.actions_for(key.instrument_id);
    let mut macro_parents = Vec::new();
    let mut all_parents = vec![generation.manifest().clone()];
    for candidate in &action_candidates {
        push_parent(&mut all_parents, candidate.source_manifest())?;
    }
    let mut examples = Vec::with_capacity(example_count);
    let mut macro_evidence = Vec::with_capacity(example_count);
    let mut feature_content = Vec::with_capacity(example_count);
    let mut feature_audit = Vec::with_capacity(example_count);
    let mut label_content = Vec::with_capacity(example_count);
    let mut label_audit = Vec::with_capacity(example_count);
    let mut session_evidence = Vec::with_capacity(example_count);
    let mut return_evidence = Vec::with_capacity(example_count);
    for (index, start) in starts.into_iter().enumerate() {
        check_control(deadline, cancellation)?;
        let triple = points
            .get(start..start + 3)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let prior = &triple[0];
        let current = &triple[1];
        let terminal = &triple[2];
        let effective_date = timestamp_calendar_date(current.effective)?;
        let cache_key = (current.available_at, effective_date);
        if !macro_cache.contains_key(&cache_key) {
            let vector = read_macro_feature_vector(
                &authority.macro_context,
                current.available_at,
                effective_date,
                deadline,
                cancellation.child_token(),
            )
            .await
            .map_err(|_| DatasetPreparationError::Unavailable)?;
            macro_cache.insert(cache_key, vector);
        }
        let macro_vector = macro_cache
            .get(&cache_key)
            .cloned()
            .ok_or(DatasetPreparationError::Unavailable)?;
        for parent in macro_vector.parent_manifests() {
            push_parent(&mut macro_parents, parent)?;
            push_parent(&mut all_parents, parent)?;
        }

        let feature_plan = action_plan(
            &action_candidates,
            point_in_time_policy,
            corporate_action_policy,
            current.available_at,
            ResearchTemporalCoordinate::exact(current.effective),
            None,
            deadline,
            cancellation,
        )
        .await?;
        let label_plan = action_plan(
            &action_candidates,
            point_in_time_policy,
            corporate_action_policy,
            terminal.available_at,
            ResearchTemporalCoordinate::exact(current.effective),
            Some(ResearchTemporalCoordinate::exact(terminal.effective)),
            deadline,
            cancellation,
        )
        .await?;
        let feature_return = split_adjusted_return(prior, current, &feature_plan)?;
        let label_return = split_adjusted_return(current, terminal, &label_plan)?;
        let feature_adjustment = adjustment_evidence(&feature_plan)?;
        let label_adjustment = adjustment_evidence(&label_plan)?;
        let feature_input = return_component(
            feature_spec.clone(),
            feature_return,
            vec![market_bar_family(prior)?, market_bar_family(current)?],
            ResearchTemporalCoordinate::exact(current.effective),
            None,
            feature_adjustment,
        )?;
        let label_input = return_component(
            label_spec.clone(),
            label_return,
            vec![market_bar_family(terminal)?],
            ResearchTemporalCoordinate::exact(current.effective),
            Some(ResearchTemporalCoordinate::exact(terminal.effective)),
            label_adjustment,
        )?;
        let mut components = Vec::with_capacity(contract.macro_components().len() + 2);
        components.push(feature_input.clone());
        components.extend(macro_vector.components().iter().cloned());
        components.push(label_input.clone());
        examples.push(
            DatasetExample::try_new_with_temporal_cutoffs(
                format!("product-{}-{index:05}", short_hex(option_identity)),
                key.instrument_id,
                current.available_at,
                terminal.available_at,
                ResearchTemporalCoordinate::exact(current.effective),
                ResearchTemporalCoordinate::exact(terminal.effective),
                components,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        );
        macro_evidence.push(macro_vector.evidence_digest());
        feature_content.push(component_content_evidence(&feature_input));
        feature_audit.push(plan_audit_evidence(&feature_plan));
        label_content.push(component_content_evidence(&label_input));
        label_audit.push(plan_audit_evidence(&label_plan));
        session_evidence.push(completed_session_evidence(current, terminal));
        return_evidence.push(return_kernel_evidence(
            feature_return,
            label_return,
            &feature_plan,
            &label_plan,
        ));
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
        &support.memberships,
        key.instrument_id,
        first_cutoff,
        test_end,
        cancellation,
    )?;
    let Some(membership) = membership else {
        return Ok(None);
    };
    push_parent(&mut all_parents, &membership.manifest)?;
    let universe_id = membership.universe_id.clone();
    let membership_value = membership.value.clone();
    let mut component_specs = Vec::with_capacity(contract.macro_components().len() + 2);
    component_specs.push(feature_spec);
    for descriptor in contract.macro_components() {
        component_specs.push(
            FeatureLabelComponentSpec::try_new(
                ComponentKind::Feature,
                ComponentScope::Global,
                CorporateActionSensitivity::NotApplicable,
                descriptor.component_name(),
                NonZeroU32::MIN,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        );
    }
    component_specs.push(label_spec);
    let inputs = DatasetBuildInputs::try_new(
        all_parents.clone(),
        universe_id,
        vec![membership_value],
        component_specs,
        examples,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let policy = DatasetBuildPolicy::new(
        ChronologicalSplitPolicy::try_new(train_end, validation_end, test_end)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        point_in_time_policy,
        corporate_action_policy,
        MissingValuePolicy::Reject,
        SourceIdentifier::try_from(contract.implementation_revision())
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
    let option_id = format!("market-research-{}", short_hex(option_identity));
    let observed_from = first_cutoff;
    let observed_through = test_end;
    Ok(Some(PreparedOption {
        summary: DatasetPreparationOption {
            id: option_id,
            label: "Price returns with economic context".to_owned(),
            source_dataset: "Canonical market history".to_owned(),
            immutable_generation: generation.manifest().manifest_version(),
            instrument_id: key.instrument_id,
            observed_points: example_count * 3,
            examples: example_count,
            observed_from,
            observed_through,
            available_uses: variants.iter().map(|variant| variant.use_case).collect(),
        },
        parents: all_parents.into_boxed_slice(),
        variants: variants.into_boxed_slice(),
        production: PreparedProductionEvidence {
            universe_membership_content: membership.content,
            universe_membership_audit: membership.audit,
            instrument_population_query: instrument_population_query_evidence(
                key.instrument_id,
                first_cutoff,
                test_end,
            ),
            instrument_population_receipt: membership.receipt,
            completed_session_request: aggregate_evidence(
                b"market-squawk/completed-session-request-set/v1",
                &session_evidence,
            ),
            completed_session_receipt: aggregate_evidence(
                b"market-squawk/completed-session-receipt-set/v1",
                &session_evidence,
            ),
            feature_point_in_time_content: aggregate_evidence(
                b"market-squawk/feature-pit-content-set/v1",
                &feature_content,
            ),
            feature_point_in_time_audit: aggregate_evidence(
                b"market-squawk/feature-pit-audit-set/v1",
                &feature_audit,
            ),
            macro_context_evidence: aggregate_evidence(
                b"market-squawk/macro-context-evidence-set/v1",
                &macro_evidence,
            ),
            macro_parent_manifests: macro_parents.into_boxed_slice(),
            label_point_in_time_content: aggregate_evidence(
                b"market-squawk/label-pit-content-set/v1",
                &label_content,
            ),
            label_point_in_time_audit: aggregate_evidence(
                b"market-squawk/label-pit-audit-set/v1",
                &label_audit,
            ),
            return_kernel_output: aggregate_evidence(
                b"market-squawk/return-kernel-output-set/v1",
                &return_evidence,
            ),
        },
        split_counts: [train_count, validation_count, test_count],
    }))
}

fn return_component(
    spec: FeatureLabelComponentSpec,
    value: Decimal,
    families: Vec<ObservationFamilyKey>,
    selection_effective_cutoff: ResearchTemporalCoordinate,
    label_selection_effective_cutoff: Option<ResearchTemporalCoordinate>,
    adjustment: ComponentAdjustmentEvidence,
) -> Result<FeatureLabelComponentInput, DatasetPreparationError> {
    let unit = SourceIdentifier::try_from(FEATURE_LABEL_RETURN_UNIT)
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    FeatureLabelComponentInput::try_new(
        spec,
        ComponentValue::decimal(value.normalize(), Some(unit), None)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        families.into_iter().map(ComponentSelector::new).collect(),
        selection_effective_cutoff,
        label_selection_effective_cutoff,
        adjustment,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn market_bar_family(
    point: &MarketSeriesPoint,
) -> Result<ObservationFamilyKey, DatasetPreparationError> {
    let bar = &point.observation;
    let provenance = bar.context().provenance();
    Ok(ObservationFamilyKey::MarketBar {
        source_id: provenance.source_id().clone(),
        instrument_id: provenance
            .instrument_id()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        venue_id: provenance
            .venue_id()
            .cloned()
            .ok_or(DatasetPreparationError::InvalidEvidence)?,
        provider_instrument_id: bar.provider_instrument_id().clone(),
        feed: bar.feed().clone(),
        interval: bar.interval().clone(),
        adjustment: bar.adjustment(),
        timestamp_basis: bar.time_semantics().timestamp_basis(),
        session: bar.time_semantics().session().clone(),
        effective: ResearchTemporalCoordinate::exact(point.effective),
    })
}

async fn action_plan(
    candidates: &[PointInTimeCandidate],
    point_in_time_policy: PointInTimePolicy,
    corporate_action_policy: CorporateActionPolicy,
    knowledge_cutoff: Timestamp,
    effective_cutoff: ResearchTemporalCoordinate,
    label_cutoff: Option<ResearchTemporalCoordinate>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CorporateActionPlan, DatasetPreparationError> {
    let candidate_limit = candidates.len().max(1);
    if candidate_limit > MAXIMUM_OBSERVATIONS_PER_GENERATION {
        return Err(DatasetPreparationError::Capacity);
    }
    let point_in_time_limits = PointInTimeLimits::try_new(
        candidate_limit,
        candidate_limit,
        candidate_limit.min(256),
        candidate_limit,
        64 * 1024 * 1024,
    )
    .map_err(|_| DatasetPreparationError::Capacity)?;
    let request = PointInTimeRequest::try_new(
        point_in_time_policy,
        knowledge_cutoff,
        None,
        effective_cutoff,
        label_cutoff,
        point_in_time_limits,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let selection = PointInTimeService::new()
        .select(&request, candidates, cancellation, deadline)
        .await
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let mut records = Vec::new();
    for record in selection.records() {
        let ResearchObservation::CorporateAction(observation) = record.candidate().observation()
        else {
            return Err(DatasetPreparationError::InvalidEvidence);
        };
        records.push(CorporateActionRecord::new(
            observation.clone(),
            record.candidate().source_manifest().clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, record.evidence_identity().bytes()),
        ));
    }
    let action_limit =
        NonZeroUsize::new(records.len().max(1)).ok_or(DatasetPreparationError::Capacity)?;
    let limits = CorporateActionLimits::try_new(
        action_limit,
        NonZeroUsize::new(4 * 1024 * 1024).ok_or(DatasetPreparationError::Capacity)?,
    )
    .map_err(|_| DatasetPreparationError::Capacity)?;
    CorporateActionPlan::try_build(
        corporate_action_policy,
        knowledge_cutoff,
        knowledge_cutoff,
        records,
        limits,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn adjustment_evidence(
    plan: &CorporateActionPlan,
) -> Result<ComponentAdjustmentEvidence, DatasetPreparationError> {
    if !plan.conflicts().is_empty()
        || plan
            .steps()
            .iter()
            .any(|step| !matches!(step, AdjustmentStep::Split { .. }))
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    let implementation = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(SPLIT_RETURN_KERNEL_REVISION.as_bytes()).into(),
    );
    ComponentAdjustmentEvidence::try_applied(
        plan.policy(),
        plan.content_hash(),
        plan.audit_hash(),
        implementation,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn split_adjusted_return(
    left: &MarketSeriesPoint,
    right: &MarketSeriesPoint,
    plan: &CorporateActionPlan,
) -> Result<Decimal, DatasetPreparationError> {
    let left = split_adjusted_close(left, plan)?;
    let right = split_adjusted_close(right, plan)?;
    right
        .checked_div(left)
        .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
        .map(|value| value.normalize())
        .ok_or(DatasetPreparationError::InvalidEvidence)
}

fn split_adjusted_close(
    point: &MarketSeriesPoint,
    plan: &CorporateActionPlan,
) -> Result<Decimal, DatasetPreparationError> {
    let mut adjusted = point.observation.close().amount();
    for step in plan.steps() {
        let AdjustmentStep::Split {
            admitted_index,
            price_factor,
            ..
        } = step
        else {
            return Err(DatasetPreparationError::InvalidEvidence);
        };
        let action = plan
            .admitted()
            .get(*admitted_index)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let effective = action
            .observation()
            .context()
            .time()
            .effective()
            .exact_timestamp()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        if point.effective < effective {
            adjusted = adjusted
                .checked_mul(Decimal::from(price_factor.numerator().get()))
                .and_then(|value| {
                    value.checked_div(Decimal::from(price_factor.denominator().get()))
                })
                .ok_or(DatasetPreparationError::InvalidEvidence)?;
        }
    }
    Ok(adjusted.normalize())
}

fn component_content_evidence(component: &FeatureLabelComponentInput) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/product-component-content/v1");
    update_text(&mut hash, component.spec().name());
    hash_coordinate(&mut hash, component.selection_effective_cutoff());
    if let Some(label) = component.label_selection_effective_cutoff() {
        hash.update([1]);
        hash_coordinate(&mut hash, label);
    } else {
        hash.update([0]);
    }
    hash.update((component.selectors().len() as u64).to_be_bytes());
    for selector in component.selectors() {
        hash.update(selector.identity().bytes());
    }
    match component.value() {
        ComponentValue::Decimal {
            value,
            unit,
            currency,
        } => {
            hash.update([1]);
            update_text(&mut hash, &value.normalize().to_string());
            update_text(
                &mut hash,
                unit.as_ref().map_or("", SourceIdentifier::as_str),
            );
            update_text(
                &mut hash,
                currency
                    .as_ref()
                    .map_or("", market_squawk_domain::Currency::as_str),
            );
        }
        ComponentValue::Float { value, .. } => {
            hash.update([2]);
            hash.update(value.to_bits().to_be_bytes());
        }
        ComponentValue::Missing { reason } => {
            hash.update([3]);
            update_text(&mut hash, reason.as_str());
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn plan_audit_evidence(plan: &CorporateActionPlan) -> EvidenceDigest {
    evidence_digest(
        b"market-squawk/split-plan-pit-audit/v1",
        &[
            EvidencePart::Sha256(plan.content_hash()),
            EvidencePart::Sha256(plan.audit_hash()),
            EvidencePart::Timestamp(plan.knowledge_cutoff()),
            EvidencePart::Timestamp(plan.valuation_cutoff()),
        ],
    )
}

fn completed_session_evidence(
    current: &MarketSeriesPoint,
    terminal: &MarketSeriesPoint,
) -> EvidenceDigest {
    let session = current.observation.time_semantics().session();
    evidence_digest(
        b"market-squawk/completed-market-session/v1",
        &[
            EvidencePart::Timestamp(current.observation.completed_at()),
            EvidencePart::Timestamp(terminal.observation.completed_at()),
            EvidencePart::Text(session.ruleset().as_str()),
            EvidencePart::Digest(session.evidence()),
        ],
    )
}

fn return_kernel_evidence(
    feature_return: Decimal,
    label_return: Decimal,
    feature_plan: &CorporateActionPlan,
    label_plan: &CorporateActionPlan,
) -> EvidenceDigest {
    let feature_return = feature_return.normalize().to_string();
    let label_return = label_return.normalize().to_string();
    evidence_digest(
        b"market-squawk/split-adjusted-return-output/v1",
        &[
            EvidencePart::Text(SPLIT_RETURN_KERNEL_REVISION),
            EvidencePart::Text(&feature_return),
            EvidencePart::Text(&label_return),
            EvidencePart::Sha256(feature_plan.content_hash()),
            EvidencePart::Sha256(feature_plan.audit_hash()),
            EvidencePart::Sha256(label_plan.content_hash()),
            EvidencePart::Sha256(label_plan.audit_hash()),
        ],
    )
}

struct MembershipSelection {
    universe_id: UniverseId,
    value: UniverseMembership,
    manifest: DatasetManifestRef,
    content: EvidenceDigest,
    audit: EvidenceDigest,
    receipt: EvidenceDigest,
}

fn membership_evidence(
    memberships: &[CanonicalMembership],
    instrument_id: InstrumentId,
    first_cutoff: Timestamp,
    final_cutoff: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Option<MembershipSelection>, DatasetPreparationError> {
    for retained in memberships {
        let observed = &retained.observation;
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
            retained.manifest.clone(),
        );
        let identity = PointInTimeService::new()
            .payload_identity(&candidate, cancellation, identity_deadline)
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
        let identity = EvidenceDigest::new(DigestAlgorithm::Sha256, identity.bytes());
        let value = UniverseMembership::new(
            instrument_id,
            observed.effective_interval(),
            provenance.availability().clone(),
            retained.manifest.clone(),
            identity,
        );
        let content = evidence_digest(
            b"market-squawk/universe-membership-content/v1",
            &[
                EvidencePart::Manifest(&retained.manifest),
                EvidencePart::Digest(identity),
                EvidencePart::Timestamp(observed.effective_interval().starts_at()),
            ],
        );
        let audit = evidence_digest(
            b"market-squawk/universe-membership-audit/v1",
            &[
                EvidencePart::Digest(content),
                EvidencePart::Timestamp(first_cutoff),
                EvidencePart::Timestamp(final_cutoff),
            ],
        );
        let receipt = evidence_digest(
            b"market-squawk/instrument-population-receipt/v1",
            &[
                EvidencePart::Digest(content),
                EvidencePart::Digest(audit),
                EvidencePart::Bytes(instrument_id.as_uuid().as_bytes()),
            ],
        );
        return Ok(Some(MembershipSelection {
            universe_id: universe,
            value,
            manifest: retained.manifest.clone(),
            content,
            audit,
            receipt,
        }));
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
    let parent_count = inputs.parents().len();
    let component_count = inputs.component_specs().len();
    let max_input_rows = parent_count
        .checked_mul(MAXIMUM_OBSERVATIONS_PER_GENERATION)
        .filter(|rows| *rows <= 1_000_000)
        .ok_or(DatasetPreparationError::Capacity)?;
    let output_rows = examples
        .checked_mul(component_count)
        .ok_or(DatasetPreparationError::Capacity)?;
    DatasetBuildRequest::try_new(
        DatasetId::try_from(output_id.as_str())
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        inputs,
        policy,
        use_case.domain(),
        ResearchUseLimits::try_new(
            parent_count,
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
            max_input_rows,
            examples,
            component_count,
            output_rows,
            128 * 1024 * 1024,
            BUILD_DURATION,
            PointInTimeLimits::try_new(
                max_input_rows,
                max_input_rows,
                256,
                max_input_rows,
                64 * 1024 * 1024,
            )
            .map_err(|_| DatasetPreparationError::Capacity)?,
            UniverseLimits::try_new(64, 4 * 1024 * 1024)
                .map_err(|_| DatasetPreparationError::Capacity)?,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(MAXIMUM_OBSERVATIONS_PER_GENERATION)
                    .ok_or(DatasetPreparationError::Capacity)?,
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

fn market_series_identity(manifest: &DatasetManifestRef, key: &MarketSeriesKey) -> Sha256Digest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/product-market-bar-series/v1");
    digest.update(manifest.content_hash().bytes());
    digest.update(key.instrument_id.as_uuid().as_bytes());
    update_text(&mut digest, key.source_id.as_str());
    update_text(&mut digest, key.venue_id.as_str());
    update_text(&mut digest, key.provider_instrument_id.as_str());
    update_text(&mut digest, key.feed.as_str());
    update_text(&mut digest, key.interval.as_str());
    digest.update([match key.timestamp_basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }]);
    digest.update([match key.session.kind() {
        market_squawk_domain::MarketBarSessionKind::Regular => 1,
        market_squawk_domain::MarketBarSessionKind::Extended => 2,
        market_squawk_domain::MarketBarSessionKind::Continuous => 3,
        market_squawk_domain::MarketBarSessionKind::ProviderDefined => 4,
    }]);
    update_text(&mut digest, key.session.ruleset().as_str());
    digest.update([digest_algorithm_tag(key.session.evidence().algorithm())]);
    digest.update(key.session.evidence().bytes());
    update_text(&mut digest, key.currency.as_str());
    Sha256Digest::new(digest.finalize().into())
}

fn push_parent(
    parents: &mut Vec<DatasetManifestRef>,
    candidate: &DatasetManifestRef,
) -> Result<(), DatasetPreparationError> {
    for retained in parents.iter() {
        if retained.dataset_id() == candidate.dataset_id()
            && retained.manifest_version() == candidate.manifest_version()
        {
            return if retained == candidate {
                Ok(())
            } else {
                Err(DatasetPreparationError::InvalidEvidence)
            };
        }
    }
    parents.push(candidate.clone());
    Ok(())
}

fn timestamp_calendar_date(timestamp: Timestamp) -> Result<CalendarDate, DatasetPreparationError> {
    let date = DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos()).date_naive();
    CalendarDate::new(
        u16::try_from(date.year()).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        u8::try_from(date.month()).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        u8::try_from(date.day()).map_err(|_| DatasetPreparationError::InvalidEvidence)?,
    )
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

fn instrument_population_query_evidence(
    instrument_id: InstrumentId,
    first_cutoff: Timestamp,
    final_cutoff: Timestamp,
) -> EvidenceDigest {
    evidence_digest(
        b"market-squawk/instrument-population-query/v1",
        &[
            EvidencePart::Bytes(instrument_id.as_uuid().as_bytes()),
            EvidencePart::Timestamp(first_cutoff),
            EvidencePart::Timestamp(final_cutoff),
        ],
    )
}

fn aggregate_evidence(domain: &'static [u8], evidence: &[EvidenceDigest]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain);
    hash.update((evidence.len() as u64).to_be_bytes());
    for retained in evidence {
        hash.update([digest_algorithm_tag(retained.algorithm())]);
        hash.update(retained.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

enum EvidencePart<'value> {
    Bytes(&'value [u8]),
    Text(&'value str),
    Timestamp(Timestamp),
    Digest(EvidenceDigest),
    Sha256(Sha256Digest),
    Manifest(&'value DatasetManifestRef),
}

fn evidence_digest(domain: &'static [u8], parts: &[EvidencePart<'_>]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update((domain.len() as u64).to_be_bytes());
    hash.update(domain);
    hash.update((parts.len() as u64).to_be_bytes());
    for part in parts {
        match part {
            EvidencePart::Bytes(value) => {
                hash.update([1]);
                hash.update((value.len() as u64).to_be_bytes());
                hash.update(value);
            }
            EvidencePart::Text(value) => {
                hash.update([2]);
                update_text(&mut hash, value);
            }
            EvidencePart::Timestamp(value) => {
                hash.update([3]);
                hash.update(value.unix_nanos().to_be_bytes());
            }
            EvidencePart::Digest(value) => {
                hash.update([4, digest_algorithm_tag(value.algorithm())]);
                hash.update(value.bytes());
            }
            EvidencePart::Sha256(value) => {
                hash.update([5]);
                hash.update(value.bytes());
            }
            EvidencePart::Manifest(value) => {
                hash.update([6]);
                hash_manifest(&mut hash, value);
            }
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_manifest(hash: &mut Sha256, manifest: &DatasetManifestRef) {
    update_text(hash, manifest.dataset_id().as_str());
    hash.update(manifest.manifest_version().to_be_bytes());
    update_text(hash, manifest.schema().name());
    hash.update(manifest.schema().version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
}

fn hash_coordinate(hash: &mut Sha256, coordinate: &ResearchTemporalCoordinate) {
    if let Some(timestamp) = coordinate.exact_timestamp() {
        hash.update([1]);
        hash.update(timestamp.unix_nanos().to_be_bytes());
    } else if let Some(date) = coordinate.calendar_date_value() {
        hash.update([2]);
        update_text(hash, &date.to_string());
    } else if let Some(period) = coordinate.source_period_value() {
        hash.update([3]);
        update_text(hash, period.scheme().as_str());
        update_text(hash, period.code().as_str());
    }
}

const fn digest_algorithm_tag(algorithm: DigestAlgorithm) -> u8 {
    match algorithm {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }
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
