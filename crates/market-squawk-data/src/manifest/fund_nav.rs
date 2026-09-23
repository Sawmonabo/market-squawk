//! Provider-neutral durable Fund NAV publication and immutable-generation selection.

use std::time::Instant;

use market_squawk_domain::{
    AssetClass, CalendarDate, Currency, DigestAlgorithm, FundNavCorrectionState, FundNavFinality,
    FundNavValuationBasis, InstrumentId, MarketDataInstrumentDefinition, ProviderChannel,
    ProviderInstrumentId, ProviderProduct, ResearchObservation, SourceId, SourceIdentifier,
    Timestamp,
};
use market_squawk_sources::{CanonicalObservationPayload, ExtractionBatch};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::catalog::load_pinned;
use super::{
    DatasetId, DatasetManifestRef, ManifestCatalogError, ManifestPlan, PinnedDataset, Sha256Digest,
};
use crate::catalog::{PreparedProviderCaptureBinding, load_provider_capture_for_run};
use crate::schema::{DatasetSchemaRef, DatasetSchemaRegistry};
use crate::{
    AnalyticalFundNavReadLimit, AnalyticalFundNavReadRequest, ArtifactRecord,
    DatasetManifestRecord, FundNavDateRange, IngestRunRecord, PointInTimeRevisionMode,
};

const FUND_NAV_RECEIPT_VERSION: u16 = 1;
const FUND_NAV_SELECTION_POLICY_VERSION: u16 = 1;
const MAX_GENERATION_FUND_NAV_INPUTS: usize = 4_096;
const RECEIPT_DOMAIN: &[u8] = b"market-squawk/fund-nav-publication/v1";
const FAMILY_DOMAIN: &[u8] = b"market-squawk/fund-nav-source-family/v1";
const ROW_SET_DOMAIN: &[u8] = b"market-squawk/fund-nav-normalized-row-set/v1";
const POLICY_DOMAIN: &[u8] = b"market-squawk/fund-nav-selection-policy/v1";
const SELECTION_DOMAIN: &[u8] = b"market-squawk/fund-nav-selection/v1";

/// Opaque provider-neutral policy for selecting one canonical Fund NAV publication family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FundNavSelectionPolicy {
    version: u16,
}

impl FundNavSelectionPolicy {
    /// The sole supported V1 policy: exact canonical instrument, schema and Fund NAV family.
    pub const CANONICAL_V1: Self = Self {
        version: FUND_NAV_SELECTION_POLICY_VERSION,
    };

    /// Returns the stable policy version included in the selection receipt.
    pub const fn version(self) -> u16 {
        self.version
    }

    const fn is_supported(self) -> bool {
        self.version == FUND_NAV_SELECTION_POLICY_VERSION
    }
}

/// Calendar selection for a provider-neutral NAV read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FundNavDateSelection {
    /// Reads the bounded history request, optionally restricted to a calendar range.
    History(Option<FundNavDateRange>),
    /// Reads only the greatest eligible NAV date, without scanning older history.
    Latest,
}

/// Provider-neutral latest or exact immutable Fund NAV read request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalFundNavReadRequest {
    instrument_id: InstrumentId,
    knowledge_cutoff: Timestamp,
    date_selection: FundNavDateSelection,
    revision_mode: PointInTimeRevisionMode,
    limit: AnalyticalFundNavReadLimit,
    policy: FundNavSelectionPolicy,
    exact_manifest: Option<DatasetManifestRef>,
}

impl CanonicalFundNavReadRequest {
    /// Selects the latest immutable generation known at `knowledge_cutoff`.
    pub fn try_latest(
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        date_selection: FundNavDateSelection,
        revision_mode: PointInTimeRevisionMode,
        limit: AnalyticalFundNavReadLimit,
        policy: FundNavSelectionPolicy,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            knowledge_cutoff,
            date_selection,
            revision_mode,
            limit,
            policy,
            None,
        )
    }

    /// Selects only from the supplied exact immutable generation, with no latest fallback.
    pub fn try_exact(
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        date_selection: FundNavDateSelection,
        revision_mode: PointInTimeRevisionMode,
        limit: AnalyticalFundNavReadLimit,
        policy: FundNavSelectionPolicy,
        manifest: DatasetManifestRef,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            knowledge_cutoff,
            date_selection,
            revision_mode,
            limit,
            policy,
            Some(manifest),
        )
    }

    fn try_new(
        instrument_id: InstrumentId,
        knowledge_cutoff: Timestamp,
        date_selection: FundNavDateSelection,
        revision_mode: PointInTimeRevisionMode,
        limit: AnalyticalFundNavReadLimit,
        policy: FundNavSelectionPolicy,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ManifestCatalogError> {
        if !policy.is_supported() {
            return Err(ManifestCatalogError::FundNavPublicationMismatch);
        }
        Ok(Self {
            instrument_id,
            knowledge_cutoff,
            date_selection,
            revision_mode,
            limit,
            policy,
            exact_manifest,
        })
    }

    /// Returns the sole caller-supplied lookup coordinate.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the inclusive trusted internal knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the requested calendar selection, resolved before the typed history read.
    pub const fn date_selection(&self) -> FundNavDateSelection {
        self.date_selection
    }

    /// Returns the requested point-in-time revision policy.
    pub const fn revision_mode(&self) -> PointInTimeRevisionMode {
        self.revision_mode
    }

    /// Returns the fixed typed-reader response ceiling.
    pub const fn limit(&self) -> AnalyticalFundNavReadLimit {
        self.limit
    }

    /// Returns the versioned provider-neutral selection policy.
    pub const fn policy(&self) -> FundNavSelectionPolicy {
        self.policy
    }

    /// Returns the optional exact immutable generation pin.
    pub const fn exact_manifest(&self) -> Option<&DatasetManifestRef> {
        self.exact_manifest.as_ref()
    }
}

/// Precommit evidence reconstructed from typed NAV rows and one exact provider binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FundNavPublicationCandidate {
    binding_digest: Sha256Digest,
    capture_receipt_digest: Sha256Digest,
    capture_content_digest: Sha256Digest,
    capture_observation_digest: Sha256Digest,
    source_id: SourceId,
    provider_dataset: SourceIdentifier,
    instrument_id: InstrumentId,
    instrument_reference_revision: SourceIdentifier,
    provider_instrument_id: ProviderInstrumentId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    valuation_basis: FundNavValuationBasis,
    currency: Currency,
    source_family_digest: Sha256Digest,
    row_set_digest: Sha256Digest,
    row_count: usize,
    first_nav_date: CalendarDate,
    last_nav_date: CalendarDate,
    max_available_at: Timestamp,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
    max_canonical_published_at: Timestamp,
    has_preliminary: bool,
    has_final: bool,
    has_correction: bool,
}

impl FundNavPublicationCandidate {
    pub(crate) fn try_from_batch(
        batch: &ExtractionBatch,
        observations: &[ResearchObservation],
        prepared: Option<&PreparedProviderCaptureBinding>,
    ) -> Result<Option<Self>, ManifestCatalogError> {
        let nav_count = observations
            .iter()
            .filter(|observation| matches!(observation, ResearchObservation::FundNav(_)))
            .count();
        if nav_count == 0 {
            return Ok(None);
        }
        let prepared = prepared.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let evidence = &prepared.evidence;
        let capture = evidence.capture();
        if nav_count != observations.len()
            || observations.len() != batch.records().len()
            || observations.len() != evidence.record_count()
            || observations.len() != evidence.rows().len()
            || observations.is_empty()
            || capture.source_id() != batch.request().object().source_id()
            || capture.dataset() != batch.request().object().dataset()
        {
            return Err(ManifestCatalogError::FundNavPublicationMismatch);
        }

        let mut instrument_id = None;
        let mut instrument_reference_revision = None;
        let mut provider_instrument_id = None;
        let mut provider_product = None;
        let mut provider_channel = None;
        let mut valuation_basis = None;
        let mut currency = None;
        let mut first_nav_date = None;
        let mut last_nav_date = None;
        let mut max_available_at = None;
        let mut max_received_at = None;
        let mut max_ingested_at = None;
        let mut max_canonical_published_at = None;
        let mut has_preliminary = false;
        let mut has_final = false;
        let mut has_correction = false;
        let mut row_set = Sha256::new();
        row_set.update(ROW_SET_DOMAIN);
        row_set.update(
            u64::try_from(observations.len())
                .map_err(|_| ManifestCatalogError::CountOverflow)?
                .to_be_bytes(),
        );

        for (ordinal, ((record, observation), binding_row)) in batch
            .records()
            .iter()
            .zip(observations)
            .zip(evidence.rows())
            .enumerate()
        {
            let ResearchObservation::FundNav(nav) = observation else {
                return Err(ManifestCatalogError::FundNavPublicationMismatch);
            };
            let context = nav.context();
            let provenance = context.provenance();
            let current_instrument = provenance
                .instrument_id()
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
            let available_at = provenance
                .availability()
                .conservative_available_at()
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
            let semantic = CanonicalObservationPayload::try_from_observation(observation)
                .map_err(|_| ManifestCatalogError::FundNavPublicationMismatch)?;
            if binding_row.canonical_row_ordinal()
                != u32::try_from(ordinal).map_err(|_| ManifestCatalogError::CountOverflow)?
                || binding_row.canonical_row_digest() != record.evidence().content_digest()
                || nav.lineage().raw_object().content_digest() != binding_row.page_body_digest()
                || nav.lineage().raw_row().content_digest() != binding_row.native_semantic_digest()
                || binding_row.received_at() != provenance.received_at()
                || provenance.source_id() != capture.source_id()
                || context.time().superseded().is_some()
                || available_at > provenance.ingested_at()
                || provenance.received_at() > provenance.ingested_at()
                || nav.canonical_published_at() < provenance.ingested_at()
            {
                return Err(ManifestCatalogError::FundNavPublicationMismatch);
            }
            require_same(&mut instrument_id, current_instrument)?;
            require_same(
                &mut instrument_reference_revision,
                nav.instrument_reference_revision()
                    .as_source_identifier()
                    .clone(),
            )?;
            require_same(
                &mut provider_instrument_id,
                nav.provider_instrument_id().clone(),
            )?;
            require_same(&mut provider_product, nav.provider_product().clone())?;
            require_same(&mut provider_channel, nav.provider_channel().clone())?;
            require_same(&mut valuation_basis, nav.valuation_basis())?;
            require_same(&mut currency, nav.currency())?;
            first_nav_date = Some(
                first_nav_date.map_or(nav.nav_date(), |value: CalendarDate| {
                    value.min(nav.nav_date())
                }),
            );
            last_nav_date = Some(last_nav_date.map_or(nav.nav_date(), |value: CalendarDate| {
                value.max(nav.nav_date())
            }));
            max_available_at = Some(
                max_available_at.map_or(available_at, |value: Timestamp| value.max(available_at)),
            );
            max_received_at = Some(
                max_received_at.map_or(provenance.received_at(), |value: Timestamp| {
                    value.max(provenance.received_at())
                }),
            );
            max_ingested_at = Some(
                max_ingested_at.map_or(provenance.ingested_at(), |value: Timestamp| {
                    value.max(provenance.ingested_at())
                }),
            );
            max_canonical_published_at = Some(
                max_canonical_published_at
                    .map_or(nav.canonical_published_at(), |value: Timestamp| {
                        value.max(nav.canonical_published_at())
                    }),
            );
            has_preliminary |= nav.revision_evidence().finality() == FundNavFinality::Preliminary;
            has_final |= nav.revision_evidence().finality() == FundNavFinality::Final;
            has_correction |=
                nav.revision_evidence().correction() == FundNavCorrectionState::Corrected;
            row_set.update((ordinal as u64).to_be_bytes());
            row_set.update(semantic.identity().bytes());
            row_set.update(binding_row.native_semantic_digest().bytes());
        }

        let source_id = capture.source_id().clone();
        let provider_dataset = capture.dataset().clone();
        let instrument_id =
            instrument_id.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let instrument_reference_revision = instrument_reference_revision
            .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let provider_instrument_id =
            provider_instrument_id.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let provider_product =
            provider_product.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let provider_channel =
            provider_channel.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let valuation_basis =
            valuation_basis.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let currency = currency.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        let source_family_digest = fund_nav_family_digest(
            &source_id,
            instrument_id,
            &provider_instrument_id,
            &provider_product,
            &provider_channel,
            valuation_basis,
            currency,
        )?;
        Ok(Some(Self {
            binding_digest: sha256_evidence(evidence.binding_digest())?,
            capture_receipt_digest: sha256_evidence(evidence.sealed_capture_receipt_digest())?,
            capture_content_digest: sha256_evidence(capture.content_digest())?,
            capture_observation_digest: sha256_evidence(capture.observation_digest())?,
            source_id,
            provider_dataset,
            instrument_id,
            instrument_reference_revision,
            provider_instrument_id,
            provider_product,
            provider_channel,
            valuation_basis,
            currency,
            source_family_digest,
            row_set_digest: nonzero_sha256(row_set.finalize().into())?,
            row_count: observations.len(),
            first_nav_date: first_nav_date
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            last_nav_date: last_nav_date.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            max_available_at: max_available_at
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            max_received_at: max_received_at
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            max_ingested_at: max_ingested_at
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            max_canonical_published_at: max_canonical_published_at
                .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?,
            has_preliminary,
            has_final,
            has_correction,
        }))
    }
}

fn require_same<T: Eq + Clone>(slot: &mut Option<T>, value: T) -> Result<(), ManifestCatalogError> {
    match slot {
        Some(existing) if existing != &value => {
            Err(ManifestCatalogError::FundNavPublicationMismatch)
        }
        Some(_) => Ok(()),
        None => {
            *slot = Some(value);
            Ok(())
        }
    }
}

/// Exact immutable receipt for one provider-bound Fund NAV publication object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FundNavPublicationReceipt {
    receipt_digest: Sha256Digest,
    origin_manifest: DatasetManifestRef,
    origin_run_id: Uuid,
    origin_artifact_id: Uuid,
    origin_object_ordinal: u16,
    source_id: SourceId,
    binding_digest: Sha256Digest,
    capture_receipt_digest: Sha256Digest,
    instrument_id: InstrumentId,
    instrument_revision_digest: Sha256Digest,
    source_family_digest: Sha256Digest,
    row_set_digest: Sha256Digest,
    row_count: usize,
    first_nav_date: CalendarDate,
    last_nav_date: CalendarDate,
    max_available_at: Timestamp,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
    max_canonical_published_at: Timestamp,
    published_at: Timestamp,
}

impl FundNavPublicationReceipt {
    /// Returns the SHA-256 identity of the canonical versioned receipt JSON.
    pub const fn receipt_digest(&self) -> Sha256Digest {
        self.receipt_digest
    }
    /// Returns the generation that originally published this exact NAV object.
    pub const fn origin_manifest(&self) -> &DatasetManifestRef {
        &self.origin_manifest
    }
    /// Returns the origin ingest run.
    pub const fn origin_run_id(&self) -> Uuid {
        self.origin_run_id
    }
    /// Returns the exact origin artifact.
    pub const fn origin_artifact_id(&self) -> Uuid {
        self.origin_artifact_id
    }
    /// Returns the exact object ordinal within the origin generation.
    pub const fn origin_object_ordinal(&self) -> u16 {
        self.origin_object_ordinal
    }
    /// Returns the provider source authority retained as evidence, never as a lookup coordinate.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    /// Returns the exact provider capture binding identity.
    pub const fn binding_digest(&self) -> Sha256Digest {
        self.binding_digest
    }
    /// Returns the exact sealed capture receipt identity.
    pub const fn capture_receipt_digest(&self) -> Sha256Digest {
        self.capture_receipt_digest
    }
    /// Returns the canonical fund/share-class identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
    /// Returns the exact canonical instrument revision admitted at publication.
    pub const fn instrument_revision_digest(&self) -> Sha256Digest {
        self.instrument_revision_digest
    }
    /// Returns the opaque source-family identity used only to detect ambiguity.
    pub const fn source_family_digest(&self) -> Sha256Digest {
        self.source_family_digest
    }
    /// Returns the normalized row-set identity.
    pub const fn row_set_digest(&self) -> Sha256Digest {
        self.row_set_digest
    }
    /// Returns the exact number of rows published by the origin object.
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    /// Returns the inclusive calendar-date coverage.
    pub const fn nav_date_range(&self) -> (CalendarDate, CalendarDate) {
        (self.first_nav_date, self.last_nav_date)
    }
    /// Returns the greatest availability, receive, ingest and canonical-publication clocks.
    pub const fn knowledge_clocks(&self) -> (Timestamp, Timestamp, Timestamp, Timestamp) {
        (
            self.max_available_at,
            self.max_received_at,
            self.max_ingested_at,
            self.max_canonical_published_at,
        )
    }
    /// Returns when the immutable origin generation entered the catalog.
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }
}

/// Restart-safe provider-neutral selection plus the exact request for the existing NAV reader.
#[derive(Debug)]
pub struct CanonicalFundNavSelection {
    pinned: PinnedDataset,
    receipt: FundNavPublicationReceipt,
    analytical_request: AnalyticalFundNavReadRequest,
    policy_digest: Sha256Digest,
    selection_digest: Sha256Digest,
}

impl CanonicalFundNavSelection {
    /// Returns the selected descendant generation.
    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }
    /// Returns the exact origin publication receipt.
    pub const fn receipt(&self) -> &FundNavPublicationReceipt {
        &self.receipt
    }
    /// Returns the already validated request delegated to the current typed NAV reader.
    pub const fn analytical_request(&self) -> &AnalyticalFundNavReadRequest {
        &self.analytical_request
    }
    /// Returns the code-owned policy digest.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }
    /// Returns the digest binding request, cutoff, receipt and selected generation.
    pub const fn selection_digest(&self) -> Sha256Digest {
        self.selection_digest
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct FundNavReceiptWire {
    receipt_version: u16,
    origin_dataset_id: String,
    origin_manifest_version: u64,
    origin_schema_name: String,
    origin_schema_version: u16,
    origin_schema_fingerprint: [u8; 32],
    origin_manifest_content_hash: [u8; 32],
    origin_run_id: Uuid,
    origin_anchor_manifest_id: Uuid,
    origin_artifact_id: Uuid,
    origin_object_ordinal: u16,
    source_id: SourceId,
    binding_digest: [u8; 32],
    capture_receipt_digest: [u8; 32],
    capture_content_digest: [u8; 32],
    capture_observation_digest: [u8; 32],
    capture_recorded_at_ns: i64,
    provider_dataset: SourceIdentifier,
    instrument_id: InstrumentId,
    instrument_revision_digest: [u8; 32],
    provider_instrument_id: ProviderInstrumentId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    valuation_basis: FundNavValuationBasis,
    currency: Currency,
    source_family_digest: [u8; 32],
    row_set_digest: [u8; 32],
    row_count: u32,
    first_nav_date: CalendarDate,
    last_nav_date: CalendarDate,
    max_available_at_ns: i64,
    max_received_at_ns: i64,
    max_ingested_at_ns: i64,
    max_canonical_published_at_ns: i64,
    published_at_ns: i64,
    has_preliminary: bool,
    has_final: bool,
    has_correction: bool,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn insert_generation_fund_nav_inputs(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
    source_input: Option<&IngestRunRecord>,
    candidate: Option<&FundNavPublicationCandidate>,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    if let Some(candidate) = candidate {
        let source_input = source_input.ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
        if candidate.source_id != *source_input.source_id()
            || candidate.max_ingested_at > anchor.created_at()
        {
            return Err(ManifestCatalogError::FundNavPublicationMismatch);
        }
        let instrument_revision_digest =
            validate_exact_fund_revision(transaction, candidate, source_input.requested_at())?;
        insert_fund_nav_publication(
            transaction,
            generation_sequence,
            plan,
            artifact,
            anchor,
            schema,
            source_input,
            candidate,
            instrument_revision_digest,
        )?;
    }
    propagate_generation_fund_nav_inputs(transaction, generation_sequence)
}

pub(crate) fn propagate_generation_fund_nav_inputs(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
) -> Result<(), ManifestCatalogError> {
    let inserted = transaction.execute(
        "INSERT INTO analytical_generation_fund_nav_inputs
         (generation_sequence, input_ordinal, publication_receipt_digest)
         WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_fund_nav_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
             UNION
             SELECT publication_receipt_digest FROM fund_nav_publications
             WHERE origin_generation_sequence=?1
         )
         SELECT ?1, ROW_NUMBER() OVER (ORDER BY publication_receipt_digest)-1,
                publication_receipt_digest
         FROM candidates ORDER BY publication_receipt_digest LIMIT ?2",
        params![
            generation_sequence,
            i64::try_from(MAX_GENERATION_FUND_NAV_INPUTS)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ],
    )?;
    let expected: i64 = transaction.query_row(
        "WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_fund_nav_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
             UNION
             SELECT publication_receipt_digest FROM fund_nav_publications
             WHERE origin_generation_sequence=?1
         ) SELECT COUNT(*) FROM candidates",
        [generation_sequence],
        |row| row.get(0),
    )?;
    if expected < 0
        || usize::try_from(expected)
            .ok()
            .is_none_or(|count| count > MAX_GENERATION_FUND_NAV_INPUTS || count != inserted)
    {
        return Err(ManifestCatalogError::FundNavInputLimitExceeded {
            max: MAX_GENERATION_FUND_NAV_INPUTS,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_fund_nav_publication(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
    source_input: &IngestRunRecord,
    candidate: &FundNavPublicationCandidate,
    instrument_revision_digest: Sha256Digest,
) -> Result<(), ManifestCatalogError> {
    require_canonical_schema(schema)?;
    let origin_object_ordinal = plan
        .objects()
        .len()
        .checked_sub(1)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
    let manifest_version: i64 = transaction.query_row(
        "SELECT manifest_version FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let manifest_version = u64::try_from(manifest_version)
        .ok()
        .filter(|value| *value > 0)
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let capture_recorded_at_ns: i64 = transaction
        .query_row(
            "SELECT capture.recorded_at_ns
             FROM provider_capture_bindings AS binding
             JOIN provider_raw_observations AS capture
               ON capture.capture_observation_digest=binding.capture_observation_digest
             WHERE binding.binding_digest=?1
               AND capture.capture_content_digest=?2
               AND capture.capture_observation_digest=?3
               AND capture.source_id=?4 AND capture.provider_dataset=?5",
            params![
                candidate.binding_digest.bytes(),
                candidate.capture_content_digest.bytes(),
                candidate.capture_observation_digest.bytes(),
                candidate.source_id.as_str(),
                candidate.provider_dataset.as_str(),
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::FundNavPublicationMismatch)?;
    if capture_recorded_at_ns > anchor.created_at().unix_nanos() {
        return Err(ManifestCatalogError::FundNavPublicationMismatch);
    }
    let row_count = u32::try_from(candidate.row_count)
        .map_err(|_| ManifestCatalogError::FundNavPublicationMismatch)?;
    let wire = FundNavReceiptWire {
        receipt_version: FUND_NAV_RECEIPT_VERSION,
        origin_dataset_id: plan.dataset_id().as_str().to_owned(),
        origin_manifest_version: manifest_version,
        origin_schema_name: schema.name().to_owned(),
        origin_schema_version: schema.version().get(),
        origin_schema_fingerprint: schema.fingerprint(),
        origin_manifest_content_hash: plan.content_hash().bytes(),
        origin_run_id: source_input.run_id(),
        origin_anchor_manifest_id: anchor.manifest_id(),
        origin_artifact_id: artifact.artifact_id(),
        origin_object_ordinal,
        source_id: candidate.source_id.clone(),
        binding_digest: candidate.binding_digest.bytes(),
        capture_receipt_digest: candidate.capture_receipt_digest.bytes(),
        capture_content_digest: candidate.capture_content_digest.bytes(),
        capture_observation_digest: candidate.capture_observation_digest.bytes(),
        capture_recorded_at_ns,
        provider_dataset: candidate.provider_dataset.clone(),
        instrument_id: candidate.instrument_id,
        instrument_revision_digest: instrument_revision_digest.bytes(),
        provider_instrument_id: candidate.provider_instrument_id.clone(),
        provider_product: candidate.provider_product.clone(),
        provider_channel: candidate.provider_channel.clone(),
        valuation_basis: candidate.valuation_basis,
        currency: candidate.currency,
        source_family_digest: candidate.source_family_digest.bytes(),
        row_set_digest: candidate.row_set_digest.bytes(),
        row_count,
        first_nav_date: candidate.first_nav_date,
        last_nav_date: candidate.last_nav_date,
        max_available_at_ns: candidate.max_available_at.unix_nanos(),
        max_received_at_ns: candidate.max_received_at.unix_nanos(),
        max_ingested_at_ns: candidate.max_ingested_at.unix_nanos(),
        max_canonical_published_at_ns: candidate.max_canonical_published_at.unix_nanos(),
        published_at_ns: anchor.created_at().unix_nanos(),
        has_preliminary: candidate.has_preliminary,
        has_final: candidate.has_final,
        has_correction: candidate.has_correction,
    };
    let receipt_json = serde_json::to_string(&wire)
        .map_err(|_| ManifestCatalogError::FundNavPublicationMismatch)?;
    let receipt_digest = receipt_digest(receipt_json.as_bytes())?;
    transaction.execute(
        "INSERT INTO fund_nav_publications
         (publication_receipt_digest, receipt_version, origin_generation_sequence,
          origin_run_id, origin_anchor_manifest_id, origin_artifact_id, origin_object_ordinal,
          source_id, binding_digest, capture_receipt_digest, capture_content_digest,
          capture_observation_digest, capture_recorded_at_ns, provider_dataset, instrument_id,
          instrument_revision_digest, provider_instrument_id, provider_product, provider_channel,
          valuation_basis, currency, source_family_digest, row_set_digest, row_count,
          first_nav_date, last_nav_date, max_available_at_ns, max_received_at_ns,
          max_ingested_at_ns, max_canonical_published_at_ns, published_at_ns,
          has_preliminary, has_final, has_correction, receipt_json)
         VALUES (?1,1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,
                 ?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34)",
        params![
            receipt_digest.bytes(),
            generation_sequence,
            source_input.run_id().to_string(),
            anchor.manifest_id().to_string(),
            artifact.artifact_id().to_string(),
            i64::from(origin_object_ordinal),
            candidate.source_id.as_str(),
            candidate.binding_digest.bytes(),
            candidate.capture_receipt_digest.bytes(),
            candidate.capture_content_digest.bytes(),
            candidate.capture_observation_digest.bytes(),
            capture_recorded_at_ns,
            candidate.provider_dataset.as_str(),
            candidate.instrument_id.to_string(),
            instrument_revision_digest.bytes(),
            candidate.provider_instrument_id.as_str(),
            candidate.provider_product.as_source_identifier().as_str(),
            candidate.provider_channel.as_source_identifier().as_str(),
            valuation_basis_name(candidate.valuation_basis),
            candidate.currency.as_str(),
            candidate.source_family_digest.bytes(),
            candidate.row_set_digest.bytes(),
            i64::from(row_count),
            candidate.first_nav_date.to_string(),
            candidate.last_nav_date.to_string(),
            candidate.max_available_at.unix_nanos(),
            candidate.max_received_at.unix_nanos(),
            candidate.max_ingested_at.unix_nanos(),
            candidate.max_canonical_published_at.unix_nanos(),
            anchor.created_at().unix_nanos(),
            if candidate.has_preliminary {
                1_i64
            } else {
                0_i64
            },
            if candidate.has_final { 1_i64 } else { 0_i64 },
            if candidate.has_correction {
                1_i64
            } else {
                0_i64
            },
            receipt_json,
        ],
    )?;
    Ok(())
}

fn validate_exact_fund_revision(
    connection: &Connection,
    candidate: &FundNavPublicationCandidate,
    admitted_at: Timestamp,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut statement = connection.prepare(
        "SELECT revision_digest, definition_json, published_at_ns
         FROM market_data_instrument_revisions
         WHERE instrument_id=?1 AND reference_revision=?2 AND published_at_ns<=?3
         ORDER BY revision_sequence LIMIT 2",
    )?;
    let rows = statement.query_map(
        params![
            candidate.instrument_id.to_string(),
            candidate.instrument_reference_revision.as_str(),
            admitted_at.unix_nanos(),
        ],
        |row| {
            Ok((
                row.get::<_, Vec<u8>>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        },
    )?;
    let retained: Vec<_> = rows.collect::<Result<_, _>>()?;
    if retained.len() != 1 {
        return Err(ManifestCatalogError::FundNavPublicationMismatch);
    }
    let (digest, json, published_at_ns) = &retained[0];
    let digest = parse_sha256(digest)?;
    let definition: MarketDataInstrumentDefinition =
        serde_json::from_str(json).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let provider_identity = definition.provider_identity_at(
        &candidate.source_id,
        &candidate.provider_instrument_id,
        candidate.max_canonical_published_at,
    );
    let computed_digest: [u8; 32] = Sha256::digest(json.as_bytes()).into();
    if computed_digest != digest.bytes()
        || definition.instrument_id() != candidate.instrument_id
        || definition.asset_class() != AssetClass::Fund
        || definition.quote_currency() != candidate.currency
        || definition.reference_revision().as_source_identifier()
            != &candidate.instrument_reference_revision
        || definition.effective_interval().starts_at() > candidate.max_canonical_published_at
        || definition
            .effective_interval()
            .ends_at()
            .is_some_and(|end| candidate.max_canonical_published_at >= end)
        || provider_identity.is_none()
        || *published_at_ns > admitted_at.unix_nanos()
    {
        return Err(ManifestCatalogError::FundNavPublicationMismatch);
    }
    Ok(digest)
}

pub(super) fn generation_fund_nav_candidate_matches(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    candidate: Option<&FundNavPublicationCandidate>,
) -> Result<bool, ManifestCatalogError> {
    let generation_sequence = generation_sequence(connection, manifest)?;
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM fund_nav_publications WHERE origin_generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let Some(candidate) = candidate else {
        return Ok(count == 0);
    };
    if count != 1 {
        return Ok(false);
    }
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM fund_nav_publications
             WHERE origin_generation_sequence=?1 AND binding_digest=?2
               AND source_id=?3 AND instrument_id=?4 AND source_family_digest=?5
               AND row_set_digest=?6 AND row_count=?7 AND first_nav_date=?8
               AND last_nav_date=?9 AND max_available_at_ns=?10
               AND max_received_at_ns=?11 AND max_ingested_at_ns=?12
               AND max_canonical_published_at_ns=?13)",
            params![
                generation_sequence,
                candidate.binding_digest.bytes(),
                candidate.source_id.as_str(),
                candidate.instrument_id.to_string(),
                candidate.source_family_digest.bytes(),
                candidate.row_set_digest.bytes(),
                i64::try_from(candidate.row_count)
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
                candidate.first_nav_date.to_string(),
                candidate.last_nav_date.to_string(),
                candidate.max_available_at.unix_nanos(),
                candidate.max_received_at.unix_nanos(),
                candidate.max_ingested_at.unix_nanos(),
                candidate.max_canonical_published_at.unix_nanos(),
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

pub(super) fn generation_fund_nav_inputs_match_manifest(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<bool, ManifestCatalogError> {
    let generation_sequence = generation_sequence(connection, manifest)?;
    let actual: i64 = connection.query_row(
        "SELECT COUNT(*) FROM analytical_generation_fund_nav_inputs
         WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let expected: i64 = connection.query_row(
        "WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_fund_nav_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
             UNION
             SELECT publication_receipt_digest FROM fund_nav_publications
             WHERE origin_generation_sequence=?1
         ) SELECT COUNT(*) FROM candidates",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let invalid: i64 = connection.query_row(
        "SELECT COUNT(*) FROM analytical_generation_fund_nav_inputs AS input
         LEFT JOIN fund_nav_publications AS publication USING (publication_receipt_digest)
         LEFT JOIN analytical_generation_provider_capture_bindings AS capture
           ON capture.generation_sequence=input.generation_sequence
          AND capture.binding_digest=publication.binding_digest
         WHERE input.generation_sequence=?1
           AND (publication.publication_receipt_digest IS NULL OR capture.binding_digest IS NULL)",
        [generation_sequence],
        |row| row.get(0),
    )?;
    Ok(actual == expected
        && invalid == 0
        && actual >= 0
        && usize::try_from(actual)
            .ok()
            .is_some_and(|count| count <= MAX_GENERATION_FUND_NAV_INPUTS))
}

pub(super) fn select_canonical_fund_nav(
    connection: &Connection,
    max_objects: usize,
    request: &CanonicalFundNavReadRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CanonicalFundNavSelection>, ManifestCatalogError> {
    check_operation(deadline, cancellation)?;
    let schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    if request
        .exact_manifest()
        .is_some_and(|manifest| manifest.schema() != &schema)
    {
        return Err(ManifestCatalogError::FundNavPublicationMismatch);
    }
    ensure_unambiguous_family(connection, request, &schema)?;
    let exact = request.exact_manifest();
    let selected = connection
        .query_row(
            "SELECT selected_generation.dataset_id, selected_generation.manifest_version,
                    selected_generation.schema_name, selected_generation.schema_version,
                    selected_generation.schema_fingerprint, selected_generation.content_hash,
                    publication.publication_receipt_digest
             FROM analytical_generations AS selected_generation
             JOIN dataset_manifests AS selected_manifest
               ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
             JOIN artifacts AS selected_artifact
               ON selected_artifact.artifact_id=selected_manifest.artifact_id
             JOIN ingest_runs AS selected_run ON selected_run.run_id=selected_artifact.run_id
             JOIN analytical_generation_fund_nav_inputs AS input
               ON input.generation_sequence=selected_generation.generation_sequence
             JOIN fund_nav_publications AS publication USING (publication_receipt_digest)
             JOIN ingest_runs AS origin_run ON origin_run.run_id=publication.origin_run_id
             JOIN provider_capture_bindings AS binding
               ON binding.binding_digest=publication.binding_digest
             JOIN provider_raw_observations AS capture
               ON capture.capture_observation_digest=binding.capture_observation_digest
             JOIN analytical_generation_provider_capture_bindings AS selected_capture
               ON selected_capture.generation_sequence=selected_generation.generation_sequence
              AND selected_capture.binding_digest=publication.binding_digest
             WHERE publication.instrument_id=?1
               AND selected_generation.schema_name=?2
               AND selected_generation.schema_version=?3
               AND selected_generation.schema_fingerprint=?4
               AND selected_generation.generation_kind IN ('ingest','compaction')
               AND selected_generation.created_at_ns<=?5
               AND selected_manifest.created_at_ns<=?5
               AND selected_artifact.created_at_ns<=?5
               AND selected_run.state='succeeded' AND selected_run.operation='persist'
               AND origin_run.state='succeeded' AND origin_run.operation='persist'
               AND selected_run.requested_at_ns<=?5 AND selected_run.completed_at_ns<=?5
               AND origin_run.requested_at_ns<=?5 AND origin_run.completed_at_ns<=?5
               AND capture.recorded_at_ns<=?5 AND publication.capture_recorded_at_ns<=?5
               AND publication.max_available_at_ns<=?5
               AND publication.max_received_at_ns<=?5
               AND publication.max_ingested_at_ns<=?5
               AND publication.max_canonical_published_at_ns<=?5
               AND publication.published_at_ns<=?5
               AND (?6 IS NULL OR selected_generation.dataset_id=?6)
               AND (?7 IS NULL OR selected_generation.manifest_version=?7)
               AND (?8 IS NULL OR selected_generation.content_hash=?8)
             ORDER BY CASE WHEN ?9 THEN publication.last_nav_date END DESC,
                      publication.published_at_ns DESC,
                      publication.origin_generation_sequence DESC,
                      publication.publication_receipt_digest DESC,
                      selected_generation.created_at_ns DESC,
                      selected_generation.generation_sequence DESC
             LIMIT 1",
            params![
                request.instrument_id().to_string(),
                schema.name(),
                i64::from(schema.version().get()),
                schema.fingerprint().as_slice(),
                request.knowledge_cutoff().unix_nanos(),
                exact.map(|value| value.dataset_id().as_str()),
                exact
                    .map(|value| i64::try_from(value.manifest_version()))
                    .transpose()
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
                exact.map(|value| value.content_hash().bytes()),
                matches!(request.date_selection(), FundNavDateSelection::Latest),
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, Vec<u8>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((dataset, version, schema_name, schema_version, fingerprint, content, receipt)) =
        selected
    else {
        return Ok(None);
    };
    let selected_schema = DatasetSchemaRef::try_new(
        schema_name,
        market_squawk_domain::SchemaVersion::new(
            u16::try_from(schema_version).map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        )
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        parse_digest_array(&fingerprint)?,
    )?;
    if selected_schema != schema {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(dataset.as_str())?,
        u64::try_from(version)
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ManifestCatalogError::CorruptCatalog)?,
        selected_schema,
        parse_sha256(&content)?,
    )?;
    let pinned = load_pinned(connection, &manifest, max_objects)?;
    let receipt = load_fund_nav_receipt(
        connection,
        parse_sha256(&receipt)?,
        request.instrument_id(),
        request.knowledge_cutoff(),
    )?;
    let date_range = match request.date_selection() {
        FundNavDateSelection::History(range) => range,
        FundNavDateSelection::Latest => Some(
            FundNavDateRange::try_new(receipt.last_nav_date, receipt.last_nav_date)
                .map_err(|_| ManifestCatalogError::FundNavPublicationMismatch)?,
        ),
    };
    let analytical_request = AnalyticalFundNavReadRequest::try_new(
        manifest.clone(),
        request.instrument_id(),
        request.knowledge_cutoff(),
        date_range,
        request.revision_mode(),
        request.limit(),
    )
    .map_err(|_| ManifestCatalogError::FundNavPublicationMismatch)?;
    let policy_digest = policy_digest(request.policy())?;
    let selection_digest =
        selection_digest(policy_digest, request, &manifest, receipt.receipt_digest())?;
    check_operation(deadline, cancellation)?;
    Ok(Some(CanonicalFundNavSelection {
        pinned,
        receipt,
        analytical_request,
        policy_digest,
        selection_digest,
    }))
}

fn ensure_unambiguous_family(
    connection: &Connection,
    request: &CanonicalFundNavReadRequest,
    schema: &DatasetSchemaRef,
) -> Result<(), ManifestCatalogError> {
    let exact = request.exact_manifest();
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (
             SELECT DISTINCT publication.source_family_digest
             FROM analytical_generations AS generation
             JOIN analytical_generation_fund_nav_inputs AS input
               ON input.generation_sequence=generation.generation_sequence
             JOIN fund_nav_publications AS publication USING (publication_receipt_digest)
             WHERE publication.instrument_id=?1
               AND generation.schema_name=?2 AND generation.schema_version=?3
               AND generation.schema_fingerprint=?4
               AND generation.generation_kind IN ('ingest','compaction')
               AND generation.created_at_ns<=?5
               AND publication.max_available_at_ns<=?5
               AND publication.max_received_at_ns<=?5
               AND publication.max_ingested_at_ns<=?5
               AND publication.max_canonical_published_at_ns<=?5
               AND publication.published_at_ns<=?5
               AND (?6 IS NULL OR generation.dataset_id=?6)
               AND (?7 IS NULL OR generation.manifest_version=?7)
               AND (?8 IS NULL OR generation.content_hash=?8)
             LIMIT 2
         )",
        params![
            request.instrument_id().to_string(),
            schema.name(),
            i64::from(schema.version().get()),
            schema.fingerprint().as_slice(),
            request.knowledge_cutoff().unix_nanos(),
            exact.map(|value| value.dataset_id().as_str()),
            exact
                .map(|value| i64::try_from(value.manifest_version()))
                .transpose()
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
            exact.map(|value| value.content_hash().bytes()),
        ],
        |row| row.get(0),
    )?;
    if count > 1 {
        Err(ManifestCatalogError::FundNavPublicationMismatch)
    } else {
        Ok(())
    }
}

fn load_fund_nav_receipt(
    connection: &Connection,
    digest: Sha256Digest,
    expected_instrument: InstrumentId,
    cutoff: Timestamp,
) -> Result<FundNavPublicationReceipt, ManifestCatalogError> {
    let receipt_json: String = connection
        .query_row(
            "SELECT publication.receipt_json
             FROM fund_nav_publications AS publication
             JOIN analytical_generations AS generation
               ON generation.generation_sequence=publication.origin_generation_sequence
             JOIN analytical_generation_source_inputs AS source_input
               ON source_input.generation_sequence=generation.generation_sequence
              AND source_input.run_id=publication.origin_run_id
             JOIN analytical_generation_provider_capture_bindings AS capture_input
               ON capture_input.generation_sequence=generation.generation_sequence
              AND capture_input.binding_digest=publication.binding_digest
             JOIN market_data_instrument_revisions AS revision
               ON revision.revision_digest=publication.instrument_revision_digest
              AND revision.instrument_id=publication.instrument_id
             WHERE publication.publication_receipt_digest=?1",
            [digest.bytes()],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if receipt_digest(receipt_json.as_bytes())? != digest {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let wire: FundNavReceiptWire =
        serde_json::from_str(&receipt_json).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let persisted = load_provider_capture_for_run(connection, wire.origin_run_id)?
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if wire.receipt_version != FUND_NAV_RECEIPT_VERSION
        || wire.instrument_id != expected_instrument
        || wire.max_available_at_ns > cutoff.unix_nanos()
        || wire.max_received_at_ns > cutoff.unix_nanos()
        || wire.max_ingested_at_ns > cutoff.unix_nanos()
        || wire.max_canonical_published_at_ns > cutoff.unix_nanos()
        || wire.published_at_ns > cutoff.unix_nanos()
        || sha256_evidence(persisted.binding_digest())?.bytes() != wire.binding_digest
        || sha256_evidence(persisted.sealed_capture_receipt_digest())?.bytes()
            != wire.capture_receipt_digest
        || persisted.record_count() != usize::try_from(wire.row_count).unwrap_or(usize::MAX)
        || !fund_nav_wire_matches_row(connection, digest, &wire, &receipt_json)?
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let schema = DatasetSchemaRef::try_new(
        wire.origin_schema_name,
        market_squawk_domain::SchemaVersion::new(wire.origin_schema_version)
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        wire.origin_schema_fingerprint,
    )?;
    require_canonical_schema(&schema)?;
    let origin_manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(wire.origin_dataset_id.as_str())?,
        wire.origin_manifest_version,
        schema,
        nonzero_sha256(wire.origin_manifest_content_hash)?,
    )?;
    Ok(FundNavPublicationReceipt {
        receipt_digest: digest,
        origin_manifest,
        origin_run_id: wire.origin_run_id,
        origin_artifact_id: wire.origin_artifact_id,
        origin_object_ordinal: wire.origin_object_ordinal,
        source_id: wire.source_id,
        binding_digest: nonzero_sha256(wire.binding_digest)?,
        capture_receipt_digest: nonzero_sha256(wire.capture_receipt_digest)?,
        instrument_id: wire.instrument_id,
        instrument_revision_digest: nonzero_sha256(wire.instrument_revision_digest)?,
        source_family_digest: nonzero_sha256(wire.source_family_digest)?,
        row_set_digest: nonzero_sha256(wire.row_set_digest)?,
        row_count: usize::try_from(wire.row_count)
            .map_err(|_| ManifestCatalogError::CountOverflow)?,
        first_nav_date: wire.first_nav_date,
        last_nav_date: wire.last_nav_date,
        max_available_at: Timestamp::from_unix_nanos(wire.max_available_at_ns),
        max_received_at: Timestamp::from_unix_nanos(wire.max_received_at_ns),
        max_ingested_at: Timestamp::from_unix_nanos(wire.max_ingested_at_ns),
        max_canonical_published_at: Timestamp::from_unix_nanos(wire.max_canonical_published_at_ns),
        published_at: Timestamp::from_unix_nanos(wire.published_at_ns),
    })
}

fn fund_nav_wire_matches_row(
    connection: &Connection,
    digest: Sha256Digest,
    wire: &FundNavReceiptWire,
    receipt_json: &str,
) -> Result<bool, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM fund_nav_publications
             WHERE publication_receipt_digest=?1 AND receipt_version=?2
               AND origin_run_id=?3 AND origin_anchor_manifest_id=?4
               AND origin_artifact_id=?5 AND origin_object_ordinal=?6
               AND source_id=?7 AND binding_digest=?8 AND capture_receipt_digest=?9
               AND capture_content_digest=?10 AND capture_observation_digest=?11
               AND capture_recorded_at_ns=?12 AND provider_dataset=?13
               AND instrument_id=?14 AND instrument_revision_digest=?15
               AND provider_instrument_id=?16 AND provider_product=?17
               AND provider_channel=?18 AND valuation_basis=?19 AND currency=?20
               AND source_family_digest=?21 AND row_set_digest=?22 AND row_count=?23
               AND first_nav_date=?24 AND last_nav_date=?25
               AND max_available_at_ns=?26 AND max_received_at_ns=?27
               AND max_ingested_at_ns=?28 AND max_canonical_published_at_ns=?29
               AND published_at_ns=?30 AND has_preliminary=?31 AND has_final=?32
               AND has_correction=?33 AND receipt_json=?34)",
            params![
                digest.bytes(),
                i64::from(wire.receipt_version),
                wire.origin_run_id.to_string(),
                wire.origin_anchor_manifest_id.to_string(),
                wire.origin_artifact_id.to_string(),
                i64::from(wire.origin_object_ordinal),
                wire.source_id.as_str(),
                wire.binding_digest,
                wire.capture_receipt_digest,
                wire.capture_content_digest,
                wire.capture_observation_digest,
                wire.capture_recorded_at_ns,
                wire.provider_dataset.as_str(),
                wire.instrument_id.to_string(),
                wire.instrument_revision_digest,
                wire.provider_instrument_id.as_str(),
                wire.provider_product.as_source_identifier().as_str(),
                wire.provider_channel.as_source_identifier().as_str(),
                valuation_basis_name(wire.valuation_basis),
                wire.currency.as_str(),
                wire.source_family_digest,
                wire.row_set_digest,
                i64::from(wire.row_count),
                wire.first_nav_date.to_string(),
                wire.last_nav_date.to_string(),
                wire.max_available_at_ns,
                wire.max_received_at_ns,
                wire.max_ingested_at_ns,
                wire.max_canonical_published_at_ns,
                wire.published_at_ns,
                if wire.has_preliminary { 1_i64 } else { 0_i64 },
                if wire.has_final { 1_i64 } else { 0_i64 },
                if wire.has_correction { 1_i64 } else { 0_i64 },
                receipt_json,
            ],
            |row| row.get(0),
        )
        .map_err(Into::into)
}

fn generation_sequence(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<i64, ManifestCatalogError> {
    connection
        .query_row(
            "SELECT generation_sequence FROM analytical_generations
             WHERE dataset_id=?1 AND manifest_version=?2 AND schema_name=?3
               AND schema_version=?4 AND schema_fingerprint=?5 AND content_hash=?6",
            params![
                manifest.dataset_id().as_str(),
                i64::try_from(manifest.manifest_version())
                    .map_err(|_| ManifestCatalogError::CountOverflow)?,
                manifest.schema().name(),
                i64::from(manifest.schema_version().get()),
                manifest.schema().fingerprint().as_slice(),
                manifest.content_hash().bytes(),
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::GenerationConflict)
}

fn require_canonical_schema(schema: &DatasetSchemaRef) -> Result<(), ManifestCatalogError> {
    if schema == &DatasetSchemaRegistry::local().canonical_research_observations()? {
        Ok(())
    } else {
        Err(ManifestCatalogError::FundNavPublicationMismatch)
    }
}

fn fund_nav_family_digest(
    source_id: &SourceId,
    instrument_id: InstrumentId,
    provider_instrument_id: &ProviderInstrumentId,
    provider_product: &ProviderProduct,
    provider_channel: &ProviderChannel,
    basis: FundNavValuationBasis,
    currency: Currency,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(FAMILY_DOMAIN);
    for value in [
        source_id.as_str(),
        &instrument_id.to_string(),
        provider_instrument_id.as_str(),
        provider_product.as_source_identifier().as_str(),
        provider_channel.as_source_identifier().as_str(),
        valuation_basis_name(basis),
        currency.as_str(),
    ] {
        hash_text(&mut hash, value)?;
    }
    nonzero_sha256(hash.finalize().into())
}

fn policy_digest(policy: FundNavSelectionPolicy) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(POLICY_DOMAIN);
    hash.update(policy.version().to_be_bytes());
    hash_text(&mut hash, "canonical-research-observations")?;
    hash_text(&mut hash, "fund_nav")?;
    hash_text(&mut hash, "fail-closed-source-family")?;
    nonzero_sha256(hash.finalize().into())
}

fn selection_digest(
    policy_digest: Sha256Digest,
    request: &CanonicalFundNavReadRequest,
    manifest: &DatasetManifestRef,
    receipt: Sha256Digest,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(SELECTION_DOMAIN);
    hash.update(policy_digest.bytes());
    hash_text(&mut hash, &request.instrument_id().to_string())?;
    hash.update(request.knowledge_cutoff().unix_nanos().to_be_bytes());
    match request.date_selection() {
        FundNavDateSelection::History(Some(range)) => {
            hash.update([1]);
            hash_text(&mut hash, &range.start().to_string())?;
            hash_text(&mut hash, &range.end().to_string())?;
        }
        FundNavDateSelection::History(None) => hash.update([0]),
        FundNavDateSelection::Latest => hash.update([2]),
    }
    hash.update([match request.revision_mode() {
        PointInTimeRevisionMode::LatestKnown => 0,
        PointInTimeRevisionMode::AllKnown => 1,
    }]);
    hash.update(request.limit().get().to_be_bytes());
    hash.update([u8::from(request.exact_manifest().is_some())]);
    hash_text(&mut hash, manifest.dataset_id().as_str())?;
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_text(&mut hash, manifest.schema().name())?;
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    hash.update(receipt.bytes());
    nonzero_sha256(hash.finalize().into())
}

fn receipt_digest(json: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(RECEIPT_DOMAIN);
    hash.update(
        u64::try_from(json.len())
            .map_err(|_| ManifestCatalogError::CountOverflow)?
            .to_be_bytes(),
    );
    hash.update(json);
    nonzero_sha256(hash.finalize().into())
}

fn valuation_basis_name(value: FundNavValuationBasis) -> &'static str {
    match value {
        FundNavValuationBasis::PerShare => "per_share",
    }
}

fn sha256_evidence(
    evidence: market_squawk_domain::EvidenceDigest,
) -> Result<Sha256Digest, ManifestCatalogError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ManifestCatalogError::FundNavPublicationMismatch);
    }
    nonzero_sha256(evidence.bytes())
}

fn nonzero_sha256(bytes: [u8; 32]) -> Result<Sha256Digest, ManifestCatalogError> {
    if bytes == [0; 32] {
        Err(ManifestCatalogError::FundNavPublicationMismatch)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}

fn parse_digest_array(value: &[u8]) -> Result<[u8; 32], ManifestCatalogError> {
    value
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn parse_sha256(value: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    nonzero_sha256(parse_digest_array(value)?)
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), ManifestCatalogError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| ManifestCatalogError::CountOverflow)?
            .to_be_bytes(),
    );
    hash.update(value.as_bytes());
    Ok(())
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ManifestCatalogError> {
    if cancellation.is_cancelled() {
        Err(ManifestCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ManifestCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
