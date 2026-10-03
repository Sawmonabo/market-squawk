//! Authority-derived dataset preparation and one-use build-admission receipts.

mod catalog;
mod current;
mod fiscal;
mod history;
mod probability;
pub(crate) use current::{
    CurrentFindPartitionEvidenceReference, CurrentFindPartitionPreparationEvidence,
    CurrentFindScreenPartition, PreparedCurrentFindFeaturePartition, PreparedCurrentFindFeatures,
};
pub(crate) use fiscal::{
    FiscalDatasetPreparationRequest, HISTORICAL_FISCAL_MAXIMUM_ORIGINS,
    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES, HISTORICAL_FISCAL_MAXIMUM_PAGES,
    HISTORICAL_FISCAL_PAGE_SIZE, HistoricalFiscalCompletedJobs, HistoricalFiscalDatasetExpectation,
    HistoricalFiscalForecastReadCapability, HistoricalFiscalForecastReference,
    HistoricalFiscalJobReference, HistoricalFiscalPageDescriptor, HistoricalFiscalPageReference,
    HistoricalFiscalRecipeReference, HistoricalFiscalSourceSelection, HistoricalFiscalStudyBinding,
    HistoricalFiscalTrainingAuthority, HistoricalFiscalUnavailableReference,
    HistoricalOriginFinancialForecast, PreparedFiscalDatasetPair, PreparedHistoricalFiscalDatasets,
};
pub(crate) use probability::{
    PreparedProbabilityDatasetPair, ProbabilityBenchmarkSource, ProbabilityCohortCoverage,
    ProbabilityCohortPreparationRequest, ProbabilitySubjectInputRequest,
};

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
    DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildPurpose,
    DatasetBuildRequest, DatasetExample, DatasetId, DatasetManifestRef, DatasetOutputAuthorization,
    DatasetSchemaRegistry, DatasetStudyPolicy, FEATURE_LABEL_RETURN_UNIT,
    FeatureDatasetProductContract, FeatureDatasetProductionError, FeatureDatasetProductionProofV1,
    FeatureDatasetProductionPublication, FeatureDatasetProductionPublisher,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, FeatureLabelDataset, MissingValuePolicy,
    ObservationFamilyKey, PointInTimeCandidate, PointInTimeLimits, PointInTimePolicy,
    PointInTimeRequest, PointInTimeRevisionMode, PointInTimeService, QueryLimits, QueryResult,
    ResearchArrowBatch, ResearchUse, ResearchUseLimits, RightsBasis, Sha256Digest, UniverseId,
    UniverseLimits, UniverseMembership,
};
use market_squawk_domain::{
    BarTimestampBasis, CalendarDate, DigestAlgorithm, EvidenceDigest, HistoricalStudyBasis,
    InstrumentId, MarketBarAdjustment, MarketBarObservation, MarketBarSessionEvidence,
    ProviderInstrumentId, ResearchObservation, ResearchTemporalCoordinate, SourceId,
    SourceIdentifier, Timestamp, UniverseMembershipObservation, VenueId,
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
use crate::{
    ResearchService,
    application::{
        lifecycle::WorkspaceRuntimeIdentity,
        market_calendar::{
            CompletedMarketSessionError, CompletedMarketSessionRead,
            CompletedMarketSessionReadCapability, CompletedMarketSessionReference,
        },
    },
};

struct DatasetRecipeCoordinates {
    identity: Sha256Digest,
    label: &'static str,
    coordinates: Vec<[usize; 3]>,
    split_counts: [usize; 3],
    observed_points: usize,
}
pub(crate) use history::RecommendationCohortPreparationRequest;

const MAXIMUM_GENERATIONS: usize = 64;
pub(crate) const MAXIMUM_OBSERVATIONS_PER_GENERATION: usize = 4_096;
const MAXIMUM_QUERY_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_EXAMPLES: usize = 2_048;
const MAXIMUM_RECEIPTS: usize = 256;
const MAXIMUM_RECEIPT_BYTES: usize = 256 * 1024 * 1024;
const RECEIPT_LIFETIME: Duration = Duration::from_secs(15 * 60);
const QUERY_DURATION: Duration = Duration::from_secs(20);
const BUILD_DURATION: Duration = Duration::from_secs(120);
const DERIVED_RIGHTS_REFERENCE: &str = "https://market-squawk.local/derived-dataset-policy/v1";
const SPLIT_RETURN_KERNEL_REVISION: &str =
    "market-squawk/completed-bar-close-split-adjusted-price-return-kernel/v1";

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

/// Source/recipe choice; preview separately validates complete macro and financial evidence.
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

/// Exact source/recipe snapshot, not a promise of complete build-ready financial inputs.
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
    label_point_in_time_content: Option<EvidenceDigest>,
    label_point_in_time_audit: Option<EvidenceDigest>,
    return_kernel_output: EvidenceDigest,
}

/// Exact phase-one request paired with a one-use product-finalization handoff.
pub(crate) struct PreparedFeatureDatasetBuild {
    request: DatasetBuildRequest,
    finalizer: FeatureDatasetProductionFinalizer,
}

impl PreparedFeatureDatasetBuild {
    /// Original native request commitment, for durable idempotent job reservation.
    pub(crate) fn build_spec_digest(&self) -> market_squawk_data::DatasetBuildSpecDigest {
        self.request.build_spec_digest()
    }
    /// Separates the existing phase-one build input from the sole post-build finalizer.
    pub(crate) fn into_parts(self) -> (DatasetBuildRequest, FeatureDatasetProductionFinalizer) {
        (self.request, self.finalizer)
    }
}

/// One-use evidence handoff to the composition-owned sole production publisher.
pub(crate) struct FeatureDatasetProductionFinalizer {
    contract: FeatureDatasetProductContract,
    build_spec: Sha256Digest,
    evidence: Option<PreparedProductionEvidence>,
    maximum_currentness_expires_at: Option<Timestamp>,
}

impl FeatureDatasetProductionFinalizer {
    /// Exact source-owned contract to be published by this one-use finalizer.
    pub(crate) const fn contract(&self) -> FeatureDatasetProductContract {
        self.contract
    }

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
        let currentness_expires_at = self
            .maximum_currentness_expires_at
            .map_or(currentness_expires_at, |expiry| {
                expiry.min(currentness_expires_at)
            });
        if attested_at >= currentness_expires_at
            || request.build_spec_digest().digest() != self.build_spec
        {
            return Err(FeatureDatasetProductionError::InvalidProof);
        }
        if self.contract.is_financial() {
            if self.evidence.is_some() {
                return Err(FeatureDatasetProductionError::InvalidProof);
            }
            let proof = FeatureDatasetProductionProofV1::try_from_financial_request(
                request,
                attested_at,
                currentness_expires_at,
            )?;
            return publisher.publish(
                research.analytical(),
                self.contract,
                request,
                dataset,
                proof,
                cancellation,
            );
        }
        let evidence = self
            .evidence
            .ok_or(FeatureDatasetProductionError::InvalidProof)?;
        let currentness = evidence_digest(
            b"market-squawk/completed-session-currentness/v1",
            &[
                EvidencePart::Timestamp(attested_at),
                EvidencePart::Timestamp(currentness_expires_at),
                EvidencePart::Digest(evidence.completed_session_receipt),
            ],
        );
        let proof = FeatureDatasetProductionProofV1::try_from_request_evidence(
            request,
            evidence.universe_membership_content,
            evidence.universe_membership_audit,
            evidence.instrument_population_query,
            evidence.instrument_population_receipt,
            evidence.completed_session_request,
            evidence.completed_session_receipt,
            currentness,
            evidence.feature_point_in_time_content,
            evidence.feature_point_in_time_audit,
            evidence.macro_context_evidence,
            evidence.macro_parent_manifests.into_vec(),
            evidence.label_point_in_time_content,
            evidence.label_point_in_time_audit,
            evidence.return_kernel_output,
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
    calendar: CompletedMarketSessionReadCapability,
    source_actions:
        crate::application::research::corporate_actions::SourceAppliedCorporateActionReadCapability,
    receipts: Mutex<ReceiptRegistry>,
}

impl DatasetPreparationAuthority {
    #[must_use]
    pub(crate) fn new(
        research: Arc<ResearchService>,
        macro_context: MacroContextReadCapability,
        calendar: CompletedMarketSessionReadCapability,
        artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
    ) -> Self {
        let reader = research.analytical_reader();
        let source_actions = crate::application::research::corporate_actions::SourceAppliedCorporateActionReadCapability::new(
            Arc::clone(&research), calendar.clone(),
        ).with_artifact_repository(artifacts);
        Self {
            research,
            reader,
            macro_context,
            calendar,
            source_actions,
            receipts: Mutex::new(ReceiptRegistry::default()),
        }
    }

    /// Reopens the original calendar named by the source publication and performs the existing
    /// controlled history association. A newer current calendar cannot replace that evidence.
    pub(crate) async fn rejoin_nominal_history<
        H: crate::application::research::market_history::NativeSessionHistory,
    >(
        &self,
        history: H,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(H, CompletedMarketSessionRead), DatasetPreparationError> {
        check_control(deadline, cancellation)?;
        let graph = history
            .selection()
            .receipt()
            .date_windows()
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let original = graph.calendar();
        let reference = CompletedMarketSessionReference::try_from_retained_digests(
            original.origin_content_digest,
            original.capture_binding_digest,
        )
        .map_err(map_calendar_preparation_error)?;
        let cutoff = history.read_receipt().knowledge_cutoff();
        let calendar = self
            .calendar
            .read_reference(&reference, cutoff, deadline, cancellation.child_token())
            .await
            .map_err(map_calendar_preparation_error)?
            .ok_or(DatasetPreparationError::Unavailable)?;
        let history = self
            .research
            .rejoin_market_history_native_sessions_with_calendar(
                history,
                &calendar,
                deadline,
                cancellation,
            )
            .await
            .map_err(|_| {
                check_control(deadline, cancellation)
                    .err()
                    .unwrap_or(DatasetPreparationError::InvalidEvidence)
            })?;
        if history.native_sessions().is_none() {
            return Err(DatasetPreparationError::InvalidEvidence);
        }
        check_control(deadline, cancellation)?;
        Ok((history, calendar))
    }
    /// Lists source/recipe choices with current source rights, without preparing every build.
    /// Complete macro, point-in-time and financial evidence is checked by selected-only preview.
    pub(crate) async fn options(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DatasetPreparationOptions, DatasetPreparationError> {
        let catalog = catalog::read(self, deadline, cancellation).await?;
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
        let catalog = catalog::read(self, deadline, cancellation.clone()).await?;
        if selection.catalog_generation != encode_hex(catalog.digest.bytes()) {
            return Err(DatasetPreparationError::StaleCatalog);
        }
        let option = catalog
            .prepare(self, &selection.dataset, deadline, &cancellation)
            .await?;
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
                "Labels that cross chronological train, validation, and test boundaries are excluded.".to_owned(),
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
                evidence: Some(stored.production),
                maximum_currentness_expires_at: None,
            },
        })
    }

    /// Builds the selected stock's annual cohort from its retained native source authority.
    /// LocalAnalysis retains the same labeled evaluation mirror required by forecast pairing;
    /// those historical labels are never used as current serving features.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, profile, population and request authority remain explicit"
    )]
    pub(crate) async fn prepare_investment_dataset(
        &self,
        instrument: InstrumentId,
        source_cutoff: Timestamp,
        source_action_reference: super::corporate_actions::SourceAppliedCorporateActionPlanReference,
        profile: &crate::application::analytical_profile::ValidatedAnalyticalProfile,
        population: market_squawk_data::CurrentListedPopulation,
        intended_use: DatasetPreparationUse,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        observed_at: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        ensure_origin(origin, workspace)?;
        if source_cutoff > observed_at {
            return Err(DatasetPreparationError::InvalidSelection);
        }
        history::prepare_investment_dataset(
            self,
            instrument,
            source_cutoff,
            source_action_reference,
            profile,
            population,
            intended_use,
            deadline,
            &cancellation,
        )
        .await
    }

    async fn observation_selection(
        &self,
        generation: &AnalyticalGeneration,
        template: AnalyticalObservationTemplate,
        instruments: Vec<InstrumentId>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(Vec<ResearchObservation>, usize), DatasetPreparationError> {
        let request = AnalyticalObservationReadRequest::try_new(
            generation.manifest().clone(),
            template,
            instruments,
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
        let reader = self.reader.clone();
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| DatasetPreparationError::Unavailable)?;
        // Keep the exact-manifest read and its query in the existing I/O lane. The worker
        // releases its permit before decoding or deriving options that may read macro sources.
        let output = self
            .research
            .run_owned_research_io(deadline, &cancellation, move |worker_cancellation| {
                runtime.block_on(reader.read_observations(
                    request,
                    limits,
                    deadline,
                    worker_cancellation,
                ))
            })
            .await
            .map_err(|error| preparation_worker_error("observation_worker", error))?
            .map_err(|error| preparation_read_error("observation_read", error))?;
        let QueryResult::Inline { batches, .. } = output.output().result() else {
            return Err(DatasetPreparationError::Capacity);
        };
        let mut observations = Vec::new();
        let mut retained_bytes = 0_usize;
        for batch in batches {
            let remaining = MAXIMUM_QUERY_BYTES
                .checked_sub(retained_bytes)
                .ok_or(DatasetPreparationError::Capacity)?;
            let (mut decoded, decoded_bytes) = ResearchArrowBatch::decode_query_projection_bounded(
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
            retained_bytes = retained_bytes
                .checked_add(decoded_bytes)
                .filter(|bytes| *bytes <= MAXIMUM_QUERY_BYTES)
                .ok_or(DatasetPreparationError::Capacity)?;
            observations.append(&mut decoded);
            if observations.len() > MAXIMUM_OBSERVATIONS_PER_GENERATION {
                return Err(DatasetPreparationError::Capacity);
            }
        }
        Ok((observations, retained_bytes))
    }
}

fn preparation_worker_error(
    stage: &'static str,
    error: crate::ResearchServiceError,
) -> DatasetPreparationError {
    let error_class = match error {
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::Cancelled) => {
            "cancelled"
        }
        crate::ResearchServiceError::Ingest(market_squawk_data::IngestError::DeadlineExceeded) => {
            "deadline_exceeded"
        }
        crate::ResearchServiceError::ProviderCaptureSealWorkerUnavailable => "worker_unavailable",
        _ => "worker_other",
    };
    tracing::warn!(stage, error_class, "guided dataset preparation I/O failed");
    DatasetPreparationError::Unavailable
}

fn preparation_read_error(
    stage: &'static str,
    error: market_squawk_data::AnalyticalReadError,
) -> DatasetPreparationError {
    use market_squawk_data::{
        AnalyticalReadError, ManifestCatalogError, ParquetStoreError, QueryError,
    };

    // Never format the source error: nested SQL, paths and provider payloads are not diagnostics.
    let error_class = match error {
        AnalyticalReadError::Manifest(ManifestCatalogError::LockPoisoned) => "catalog_lock",
        AnalyticalReadError::Manifest(ManifestCatalogError::Cancelled) => "catalog_cancelled",
        AnalyticalReadError::Manifest(ManifestCatalogError::DeadlineExceeded) => "catalog_deadline",
        AnalyticalReadError::Manifest(_) => "catalog_other",
        AnalyticalReadError::Query(QueryError::Cancelled) => "query_cancelled",
        AnalyticalReadError::Query(QueryError::DeadlineExceeded) => "query_deadline",
        AnalyticalReadError::Query(QueryError::MemoryLimitExceeded { .. }) => "query_memory",
        AnalyticalReadError::Query(QueryError::ReaderMemoryBoundExceeded) => "query_reader_memory",
        AnalyticalReadError::Query(QueryError::RowLimitExceeded { .. }) => "query_rows",
        AnalyticalReadError::Query(QueryError::ByteLimitExceeded { .. }) => "query_bytes",
        AnalyticalReadError::Query(QueryError::BlockingTaskLimitExceeded) => "query_workers",
        AnalyticalReadError::Query(QueryError::UnsupportedSourceSchema) => "query_schema",
        AnalyticalReadError::Query(QueryError::DependencyAllocationContract) => "query_allocation",
        AnalyticalReadError::Query(QueryError::DataFusion(_)) => "query_datafusion",
        AnalyticalReadError::Query(_) => "query_other",
        AnalyticalReadError::Parquet(ParquetStoreError::ReadLimitExceeded) => "cursor_memory",
        AnalyticalReadError::Parquet(ParquetStoreError::Cancelled) => "cursor_cancelled",
        AnalyticalReadError::Parquet(ParquetStoreError::ReadDeadlineExceeded) => "cursor_deadline",
        AnalyticalReadError::Parquet(_) => "cursor_other",
        _ => "read_other",
    };
    tracing::warn!(stage, error_class, "guided dataset preparation I/O failed");
    DatasetPreparationError::Unavailable
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
            DatasetPreparationError::SourceRead(error) => error,
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
    #[error("dataset preparation source read failed")]
    SourceRead(ServiceError),
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
    snapshot_as_of: Option<Timestamp>,
    memberships: Box<[CanonicalMembership]>,
    actions: Box<[PointInTimeCandidate]>,
}

impl CanonicalSupport {
    fn from_generations(
        generations: &[(AnalyticalGeneration, Vec<ResearchObservation>)],
    ) -> Result<Self, DatasetPreparationError> {
        let mut memberships = Vec::new();
        let mut actions = Vec::new();
        let mut snapshot_as_of = None;
        for (generation, observations) in generations {
            for observation in observations {
                let provenance = observation_context(observation).provenance();
                let retained_at = provenance.ingested_at().max(provenance.received_at()).max(
                    provenance
                        .availability()
                        .conservative_available_at()
                        .unwrap_or(provenance.ingested_at()),
                );
                snapshot_as_of = Some(
                    snapshot_as_of
                        .map_or(retained_at, |current: Timestamp| current.max(retained_at)),
                );
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
            snapshot_as_of,
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
    manifest: DatasetManifestRef,
    session_evidence: EvidenceDigest,
    /// Financial origin of the completed close; the provider timestamp stays in the observation.
    effective: Timestamp,
    available_at: Timestamp,
}

async fn build_option(
    authority: &DatasetPreparationAuthority,
    generation: &AnalyticalGeneration,
    support: &CanonicalSupport,
    macro_cache: &mut BTreeMap<(Timestamp, CalendarDate), MacroFeatureVector>,
    key: MarketSeriesKey,
    recipe: DatasetRecipeCoordinates,
    points: &[MarketSeriesPoint],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<PreparedOption>, DatasetPreparationError> {
    let option_identity = recipe.identity;
    let example_count = recipe.coordinates.len();
    let [train_count, validation_count, test_count] = recipe.split_counts;
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
    for (index, [prior_index, current_index, terminal_index]) in
        recipe.coordinates.iter().copied().enumerate()
    {
        check_control(deadline, cancellation)?;
        let prior = points
            .get(prior_index)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let current = points
            .get(current_index)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
        let terminal = points
            .get(terminal_index)
            .ok_or(DatasetPreparationError::InvalidEvidence)?;
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
            current.available_at,
            ResearchTemporalCoordinate::exact(current.available_at),
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
            terminal.available_at,
            ResearchTemporalCoordinate::exact(terminal.effective),
            None,
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
                Some(terminal.available_at),
                current.available_at,
                ResearchTemporalCoordinate::exact(current.effective),
                ResearchTemporalCoordinate::exact(terminal.effective),
                components,
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        );
        macro_evidence.push(macro_vector.downstream_evidence_digest());
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
    let train_end = examples[train_count - 1]
        .label_selection_as_of()
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let validation_end = examples[train_count + validation_count - 1]
        .label_selection_as_of()
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let test_end = examples
        .last()
        .and_then(DatasetExample::label_selection_as_of)
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    let first_cutoff = examples
        .first()
        .map(DatasetExample::source_selection_as_of)
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
        Some(
            DatasetStudyPolicy::try_new(
                HistoricalStudyBasis::HistoricalAsKnown,
                DatasetBuildPurpose::Training,
                support
                    .snapshot_as_of
                    .ok_or(DatasetPreparationError::InvalidEvidence)?,
                None,
                market_squawk_data::DatasetTargetHorizon::ExactElapsed(Duration::from_nanos(
                    u64::try_from(
                        points[recipe.coordinates[0][2]].effective.unix_nanos()
                            - points[recipe.coordinates[0][1]].effective.unix_nanos(),
                    )
                    .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
                )),
            )
            .map_err(|_| DatasetPreparationError::InvalidEvidence)?,
        ),
    );
    // The builder streams complete parents into its disk index. Admit their actual immutable
    // row counts, not a per-generation history ceiling unrelated to the selected recipe.
    let reader = authority.reader.clone();
    let input_parents = all_parents.clone();
    let max_input_rows = authority
        .research
        .run_owned_research_io(deadline, cancellation, move |worker_cancellation| {
            input_parents.iter().try_fold(0_usize, |total, parent| {
                check_control(deadline, &worker_cancellation)?;
                let generation = reader
                    .exact(parent, deadline, &worker_cancellation)
                    .map_err(|error| preparation_read_error("preview_parent", error))?;
                let rows = usize::try_from(generation.row_count())
                    .map_err(|_| DatasetPreparationError::Capacity)?;
                total
                    .checked_add(rows)
                    .ok_or(DatasetPreparationError::Capacity)
            })
        })
        .await
        .map_err(|error| preparation_worker_error("preview_parent_worker", error))??;
    let mut variants = Vec::new();
    for use_case in [
        DatasetPreparationUse::LocalAnalysis,
        DatasetPreparationUse::Train,
    ] {
        let request = dataset_request_with_input_rows(
            option_identity,
            use_case,
            inputs.clone(),
            policy.clone(),
            example_count,
            Some(max_input_rows),
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
            label: recipe.label.to_owned(),
            source_dataset: "Canonical market history".to_owned(),
            immutable_generation: generation.manifest().manifest_version(),
            instrument_id: key.instrument_id,
            observed_points: recipe.observed_points,
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
            label_point_in_time_content: Some(aggregate_evidence(
                b"market-squawk/label-pit-content-set/v1",
                &label_content,
            )),
            label_point_in_time_audit: Some(aggregate_evidence(
                b"market-squawk/label-pit-audit-set/v1",
                &label_audit,
            )),
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
    PointInTimeCandidate::new(
        ResearchObservation::MarketBar(point.observation.clone()),
        point.manifest.clone(),
    )
    .family_key()
    .map_err(|_| DatasetPreparationError::InvalidEvidence)
}

async fn action_plan(
    candidates: &[PointInTimeCandidate],
    point_in_time_policy: PointInTimePolicy,
    corporate_action_policy: CorporateActionPolicy,
    valuation_cutoff: Timestamp,
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
        valuation_cutoff,
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
        Sha256::digest(if plan.source_split_admission().is_some() {
            b"market-squawk/source-applied-native-close-split-price-return-kernel/v1".as_slice()
        } else {
            SPLIT_RETURN_KERNEL_REVISION.as_bytes()
        })
        .into(),
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
    if plan.source_split_admission().is_some() {
        return super::corporate_actions::source_split_adjusted_close(
            plan,
            &point.observation,
            point.effective,
        )
        .map_err(map_source_action_error);
    }
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
        let effective = if let Some(application) = action.application() {
            // Application is issued by the original native calendar; the source date is unchanged.
            application.application_at()
        } else {
            if point
                .observation
                .time_semantics()
                .nominal_daily_date()
                .is_some()
            {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
            action
                .observation()
                .context()
                .time()
                .effective()
                .exact_timestamp()
                .ok_or(DatasetPreparationError::InvalidEvidence)?
        };
        // The bar's aggregation interval is half-open. A split at its exclusive end changes
        // subsequent units, so this earlier completed close still needs the price adjustment.
        if point.effective <= effective {
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
    evidence_digest(
        b"market-squawk/completed-market-session/v1",
        &[
            EvidencePart::Timestamp(current.effective),
            EvidencePart::Timestamp(terminal.effective),
            EvidencePart::Digest(current.session_evidence),
            EvidencePart::Digest(terminal.session_evidence),
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
    dataset_request_with_input_rows(identity, use_case, inputs, policy, examples, None)
}

fn dataset_request_with_input_rows(
    identity: Sha256Digest,
    use_case: DatasetPreparationUse,
    inputs: DatasetBuildInputs,
    policy: DatasetBuildPolicy,
    examples: usize,
    actual_input_rows: Option<usize>,
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
    let population_count = inputs.population_member_count();
    let population_bytes = inputs
        .population_retained_bytes()
        .map_err(|_| DatasetPreparationError::Capacity)?;
    if population_count == 0
        || population_count > market_squawk_data::MAX_CURRENT_LISTED_POPULATION_MEMBERS
        || population_bytes > 64 * 1024 * 1024
    {
        return Err(DatasetPreparationError::Capacity);
    }
    let parent_count = inputs.parents().len();
    let component_count = inputs.component_specs().len();
    let max_input_rows = match actual_input_rows {
        Some(rows) => rows,
        None => parent_count
            .checked_mul(MAXIMUM_OBSERVATIONS_PER_GENERATION)
            .filter(|rows| *rows <= 1_000_000)
            .ok_or(DatasetPreparationError::Capacity)?,
    };
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
            UniverseLimits::try_new(
                population_count.max(64),
                population_bytes.max(4 * 1024 * 1024),
            )
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
    digest.update(b"CompletedBarClose\0");
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

fn observation_context(
    observation: &ResearchObservation,
) -> &market_squawk_domain::ResearchContext {
    match observation {
        ResearchObservation::Filing(value) => value.context(),
        ResearchObservation::Fundamental(value) => value.context(),
        ResearchObservation::Macro(value) => value.context(),
        ResearchObservation::MarketBar(value) => value.context(),
        ResearchObservation::MarketCalendar(value) => value.context(),
        ResearchObservation::FundNav(value) => value.context(),
        ResearchObservation::PortfolioPosition(value) => value.context(),
        ResearchObservation::Transaction(value) => value.context(),
        ResearchObservation::CorporateAction(value) => value.context(),
        ResearchObservation::CorporateActionSource(value) => value.context(),
        ResearchObservation::UniverseMembership(value) => value.context(),
        ResearchObservation::AlternativeData(value) => value.context(),
    }
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

fn map_calendar_preparation_error(error: CompletedMarketSessionError) -> DatasetPreparationError {
    match error {
        CompletedMarketSessionError::Cancelled | CompletedMarketSessionError::DeadlineExceeded => {
            DatasetPreparationError::Cancelled
        }
        CompletedMarketSessionError::ResourceBoundExceeded => DatasetPreparationError::Capacity,
        CompletedMarketSessionError::Unavailable => DatasetPreparationError::Unavailable,
        CompletedMarketSessionError::InvalidRequest
        | CompletedMarketSessionError::InvalidEvidence => DatasetPreparationError::InvalidEvidence,
    }
}

/// Projects only an originally covered price-action pool across the actual native source span.
/// Snapshot knowledge is fixed; a financial date never becomes a fabricated effective instant.
fn project_source_price_plan(
    source: &CorporateActionPlan,
    instrument: InstrumentId,
    knowledge: Timestamp,
    valuation: Timestamp,
    left: &MarketSeriesPoint,
    right: &MarketSeriesPoint,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CorporateActionPlan, DatasetPreparationError> {
    check_control(deadline, cancellation)?;
    let coverage = source
        .source_split_admission()
        .ok_or(DatasetPreparationError::InvalidEvidence)?;
    if knowledge != coverage.knowledge_cutoff()
        || left.effective >= right.effective
        || valuation < right.effective
        || valuation > knowledge
        || !coverage.history_input_manifests().contains(&left.manifest)
        || !coverage.history_input_manifests().contains(&right.manifest)
        || coverage
            .application_starts_at(instrument)
            .is_none_or(|start| start > left.effective)
    {
        return Err(DatasetPreparationError::InvalidEvidence);
    }
    match (
        left.observation.time_semantics().nominal_daily_date(),
        right.observation.time_semantics().nominal_daily_date(),
    ) {
        (Some(left_date), Some(right_date)) => {
            if coverage.interval().0 > left_date.date() || coverage.interval().1 < right_date.date()
            {
                return Err(DatasetPreparationError::InvalidEvidence);
            }
        }
        (None, None)
            if left.manifest == right.manifest
                && left.observation.completed_at() == Some(left.effective)
                && right.observation.completed_at() == Some(right.effective)
                && coverage.covers_timestamp_history_span(
                    instrument,
                    &left.manifest,
                    knowledge,
                    left.effective,
                    right.effective,
                ) => {}
        _ => return Err(DatasetPreparationError::InvalidEvidence),
    }
    let policy =
        CorporateActionPolicy::new(CorporateActionAdjustment::SplitAdjusted, NonZeroU32::MIN);
    let limits = source
        .source_split_projection_limits(policy, instrument, knowledge, valuation)
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    let plan = source
        .try_project_source_split_plan(policy, instrument, knowledge, valuation, limits)
        .map_err(|_| DatasetPreparationError::InvalidEvidence)?;
    // Ordinary dividends do not change the split-adjusted PRICE return. Other unit/lifecycle
    // effects inside this span require a different modeled target; never silently ignore them.
    for record in plan.admitted() {
        let at = record
            .application()
            .ok_or(DatasetPreparationError::InvalidEvidence)?
            .application_at();
        if at >= left.effective
            && at <= right.effective
            && !matches!(
                record.observation().action(),
                market_squawk_domain::CorporateActionKind::Split { .. }
                    | market_squawk_domain::CorporateActionKind::CashDividend { .. }
            )
        {
            return Err(DatasetPreparationError::Unavailable);
        }
    }
    check_control(deadline, cancellation)?;
    Ok(plan)
}

fn map_source_action_error(
    error: crate::application::research::corporate_actions::ApplicableActionPlanError,
) -> DatasetPreparationError {
    use crate::application::research::corporate_actions::ApplicableActionPlanError as Error;
    match error {
        Error::SourceRead(error) => DatasetPreparationError::SourceRead(error),
        Error::Interrupted => DatasetPreparationError::Cancelled,
        Error::InvalidEvidence => DatasetPreparationError::InvalidEvidence,
        Error::IncompleteOrdinaryCoverage | Error::UnresolvedApplicableActions => {
            DatasetPreparationError::Unavailable
        }
    }
}
