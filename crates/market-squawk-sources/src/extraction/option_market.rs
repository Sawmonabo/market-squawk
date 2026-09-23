//! Sealed bounded option-market batches for whole HTTP-response captures.
//!
//! This is the closed typed boundary for expiration catalogs and option snapshots. It consumes
//! the common one-use whole-capture token, binds every canonical row to its exact HTTP page and
//! physical journal frame, and preserves provider-native row semantics without routing options
//! through research observations or live market events.

use std::collections::BTreeSet;
use std::io::{self, Write};
use std::num::NonZeroU16;

use bytes::Bytes;
use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, InstrumentId, MetadataRevision, Money,
    OptionExpirationObservation, OptionKind, OptionSnapshotObservation, ProviderChannel,
    ProviderInstrumentId, ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use super::capture::{
    ProviderCaptureError, ProviderCaptureTerminalDisposition, ProviderWholeCaptureToken,
    SealedProviderCaptureSetReceipt,
};
use super::native_lineage::{
    MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
    MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES, ProviderNativeLineageImplementation,
    ProviderNativeLineageSchema,
};

/// Current code-owned provider option-market batch schema.
pub const PROVIDER_OPTION_MARKET_SCHEMA_VERSION: u16 = 1;
/// Maximum canonical rows retained by one bounded option HTTP response.
pub const MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS: usize = 100_000;
/// Maximum canonical bytes retained by one bounded option HTTP response.
pub const MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES: usize = 64 * 1024 * 1024;
/// Maximum serialized bytes admitted for one canonical option row.
pub const MAX_PROVIDER_OPTION_MARKET_ROW_BYTES: usize = 128 * 1024;
/// Maximum exact provider contract IDs retained in one normalized request scope.
pub const MAX_OPTION_REQUEST_CONTRACTS: usize = 4_096;

const OPTION_MARKET_SCHEMA_DOMAIN: &[u8] = b"market-squawk/provider-option-market/schema/v1";
const OPTION_MARKET_CONTENT_DOMAIN: &[u8] = b"market-squawk/provider-option-market/content/v1";
const OPTION_MARKET_NATIVE_DOMAIN: &[u8] =
    b"market-squawk/provider-option-market/native-lineage/v1";
const OPTION_MARKET_BINDING_DOMAIN: &[u8] =
    b"market-squawk/provider-option-market/sealed-binding/v1";

/// The sole canonical row kind retained by one option-market batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionMarketBatchKind {
    /// Provider-neutral expiration observations.
    Expirations,
    /// Provider-neutral contract snapshot observations.
    Snapshots,
}

impl OptionMarketBatchKind {
    const fn tag(self) -> &'static [u8] {
        match self {
            Self::Expirations => b"expirations",
            Self::Snapshots => b"snapshots",
        }
    }
}

/// Inclusive requested expiration range, without inventing a time zone or time of day.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OptionExpirationRange {
    start: CalendarDate,
    end: CalendarDate,
}

impl OptionExpirationRange {
    /// Constructs a non-reversed inclusive date range.
    pub fn try_new(start: CalendarDate, end: CalendarDate) -> Result<Self, ProviderCaptureError> {
        if start > end {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(Self { start, end })
    }

    /// Returns the inclusive first expiration date.
    pub const fn start(self) -> CalendarDate {
        self.start
    }

    /// Returns the inclusive final expiration date.
    pub const fn end(self) -> CalendarDate {
        self.end
    }
}

/// Inclusive exact strike range retained with one currency.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OptionStrikeRange {
    minimum: Money,
    maximum: Money,
}

impl OptionStrikeRange {
    /// Constructs a nonnegative, ordered, single-currency strike range.
    pub fn try_new(minimum: Money, maximum: Money) -> Result<Self, ProviderCaptureError> {
        if minimum.currency() != maximum.currency()
            || minimum.amount().is_sign_negative()
            || minimum.amount() > maximum.amount()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(Self { minimum, maximum })
    }

    /// Returns the inclusive minimum strike.
    pub const fn minimum(self) -> Money {
        self.minimum
    }

    /// Returns the inclusive maximum strike.
    pub const fn maximum(self) -> Money {
        self.maximum
    }
}

/// Provider-neutral normalized filter for one option-chain request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionMarketRequestFilter {
    expiration_range: Option<OptionExpirationRange>,
    strike_range: Option<OptionStrikeRange>,
    kind: Option<OptionKind>,
    contracts: Box<[ProviderInstrumentId]>,
}

impl OptionMarketRequestFilter {
    /// Constructs a bounded exact filter; an empty filter means every admitted contract.
    pub fn try_new(
        expiration_range: Option<OptionExpirationRange>,
        strike_range: Option<OptionStrikeRange>,
        kind: Option<OptionKind>,
        contracts: Vec<ProviderInstrumentId>,
    ) -> Result<Self, ProviderCaptureError> {
        if contracts.len() > MAX_OPTION_REQUEST_CONTRACTS {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let mut unique = BTreeSet::new();
        for contract in &contracts {
            if !unique.insert(contract.as_str()) {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
        Ok(Self {
            expiration_range,
            strike_range,
            kind,
            contracts: contracts.into_boxed_slice(),
        })
    }

    /// Returns the requested inclusive expiration range.
    pub const fn expiration_range(&self) -> Option<OptionExpirationRange> {
        self.expiration_range
    }

    /// Returns the requested inclusive strike range.
    pub const fn strike_range(&self) -> Option<OptionStrikeRange> {
        self.strike_range
    }

    /// Returns an optional requested call/put restriction.
    pub const fn kind(&self) -> Option<OptionKind> {
        self.kind
    }

    /// Returns exact source-qualified contract IDs requested explicitly.
    pub const fn contracts(&self) -> &[ProviderInstrumentId] {
        &self.contracts
    }

    fn is_empty(&self) -> bool {
        self.expiration_range.is_none()
            && self.strike_range.is_none()
            && self.kind.is_none()
            && self.contracts.is_empty()
    }
}

/// Complete input for one provider-neutral option request and observation scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OptionMarketRequestScopeInput {
    /// Registered provider source.
    pub source_id: SourceId,
    /// Exact metadata interpretation revision.
    pub metadata_revision: MetadataRevision,
    /// Canonical provider dataset identity.
    pub dataset: SourceIdentifier,
    /// Exact provider product.
    pub provider_product: ProviderProduct,
    /// Exact provider channel or REST surface.
    pub provider_channel: ProviderChannel,
    /// Venue when the source truthfully establishes one.
    pub venue_id: Option<VenueId>,
    /// Resolved canonical underlying identity.
    pub underlying_instrument_id: InstrumentId,
    /// Exact underlying definition revision.
    pub underlying_definition_revision: EvidenceDigest,
    /// Exact provider-native underlying identity.
    pub provider_instrument_id: ProviderInstrumentId,
    /// Exact credential-free authorized request identity.
    pub request_identity: EvidenceDigest,
    /// Exact complete-response observation identity, including receipt clocks.
    pub observation_identity: EvidenceDigest,
    /// Exact evidence for current option-market entitlement.
    pub entitlement_evidence: EvidenceDigest,
    /// Exact evidence for the currently admitted provider capability.
    pub capability_evidence: EvidenceDigest,
    /// Provider/source availability coordinate, conservatively established.
    pub available_at: Timestamp,
    /// Socket-boundary time at which the complete response was received.
    pub received_at: Timestamp,
    /// Local time at which canonical mapping completed.
    pub ingested_at: Timestamp,
    /// Exact normalized request filter.
    pub filter: OptionMarketRequestFilter,
}

/// Exact source, identity, entitlement, request, and clock scope for one option batch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OptionMarketRequestScope {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    venue_id: Option<VenueId>,
    underlying_instrument_id: InstrumentId,
    underlying_definition_revision: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    request_identity: EvidenceDigest,
    observation_identity: EvidenceDigest,
    entitlement_evidence: EvidenceDigest,
    capability_evidence: EvidenceDigest,
    available_at: Timestamp,
    received_at: Timestamp,
    ingested_at: Timestamp,
    filter: OptionMarketRequestFilter,
}

impl OptionMarketRequestScope {
    /// Validates nonzero identities and conservative response chronology.
    pub fn try_new(input: OptionMarketRequestScopeInput) -> Result<Self, ProviderCaptureError> {
        for evidence in [
            input.underlying_definition_revision,
            input.request_identity,
            input.observation_identity,
            input.entitlement_evidence,
            input.capability_evidence,
        ] {
            require_sha256(evidence)?;
        }
        if input.received_at > input.ingested_at || input.available_at > input.ingested_at {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(Self {
            source_id: input.source_id,
            metadata_revision: input.metadata_revision,
            dataset: input.dataset,
            provider_product: input.provider_product,
            provider_channel: input.provider_channel,
            venue_id: input.venue_id,
            underlying_instrument_id: input.underlying_instrument_id,
            underlying_definition_revision: input.underlying_definition_revision,
            provider_instrument_id: input.provider_instrument_id,
            request_identity: input.request_identity,
            observation_identity: input.observation_identity,
            entitlement_evidence: input.entitlement_evidence,
            capability_evidence: input.capability_evidence,
            available_at: input.available_at,
            received_at: input.received_at,
            ingested_at: input.ingested_at,
            filter: input.filter,
        })
    }

    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact metadata interpretation revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns canonical provider dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns exact provider product.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    /// Returns exact provider channel or REST surface.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    /// Returns exact venue when established.
    pub const fn venue_id(&self) -> Option<&VenueId> {
        self.venue_id.as_ref()
    }

    /// Returns resolved canonical underlying identity.
    pub const fn underlying_instrument_id(&self) -> InstrumentId {
        self.underlying_instrument_id
    }

    /// Returns exact underlying definition revision.
    pub const fn underlying_definition_revision(&self) -> EvidenceDigest {
        self.underlying_definition_revision
    }

    /// Returns exact provider-native underlying identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns exact authorized request identity.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns complete-response observation identity.
    pub const fn observation_identity(&self) -> EvidenceDigest {
        self.observation_identity
    }

    /// Returns current entitlement evidence.
    pub const fn entitlement_evidence(&self) -> EvidenceDigest {
        self.entitlement_evidence
    }

    /// Returns current capability evidence.
    pub const fn capability_evidence(&self) -> EvidenceDigest {
        self.capability_evidence
    }

    /// Returns conservative source/provider availability.
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns complete-response socket receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns local canonical ingest time.
    pub const fn ingested_at(&self) -> Timestamp {
        self.ingested_at
    }

    /// Returns exact normalized request filter.
    pub const fn filter(&self) -> &OptionMarketRequestFilter {
        &self.filter
    }
}

/// How a bounded option response terminated relative to its requested scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionMarketCursorState {
    /// The endpoint contract has no continuation cursor.
    NotApplicable,
    /// Pagination ended with no next cursor.
    Exhausted,
    /// A continuation remained or terminal state could not be established.
    Incomplete,
}

/// Whether an option batch may be treated as its exact requested set.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionMarketBatchDisposition {
    /// Every expected row and page reconciled.
    Complete,
    /// The sealed response remains evidence but cannot represent the requested complete set.
    Unavailable,
}

/// Complete input for checked option request/response accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OptionMarketCompletenessInput {
    /// Expected canonical records when knowable before or from the provider response.
    pub expected_records: Option<u64>,
    /// Canonical records retained in this batch.
    pub returned_records: u64,
    /// Expected records known to be missing.
    pub missing_records: u64,
    /// Returned records outside the admitted request scope.
    pub unexpected_records: u64,
    /// Provider-reported record count when supplied.
    pub provider_reported_records: Option<u64>,
    /// Exact nonzero raw response page count.
    pub page_count: NonZeroU16,
    /// Exact cursor terminal state.
    pub cursor: OptionMarketCursorState,
    /// Complete or unavailable disposition.
    pub disposition: OptionMarketBatchDisposition,
}

/// Exact count, pagination, and terminal-completeness evidence for one option batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OptionMarketCompleteness {
    expected_records: Option<u64>,
    returned_records: u64,
    missing_records: u64,
    unexpected_records: u64,
    provider_reported_records: Option<u64>,
    page_count: NonZeroU16,
    cursor: OptionMarketCursorState,
    disposition: OptionMarketBatchDisposition,
}

impl OptionMarketCompleteness {
    /// Constructs checked response accounting without promoting partial data to complete.
    pub fn try_new(input: OptionMarketCompletenessInput) -> Result<Self, ProviderCaptureError> {
        if usize::from(input.page_count.get()) > super::capture::MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        if let Some(expected) = input.expected_records {
            if input
                .returned_records
                .checked_add(input.missing_records)
                .is_none_or(|observed| observed != expected)
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
        if input.disposition == OptionMarketBatchDisposition::Complete
            && (input.missing_records != 0
                || input.unexpected_records != 0
                || input.cursor == OptionMarketCursorState::Incomplete
                || input
                    .provider_reported_records
                    .is_some_and(|reported| reported != input.returned_records))
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(Self {
            expected_records: input.expected_records,
            returned_records: input.returned_records,
            missing_records: input.missing_records,
            unexpected_records: input.unexpected_records,
            provider_reported_records: input.provider_reported_records,
            page_count: input.page_count,
            cursor: input.cursor,
            disposition: input.disposition,
        })
    }

    /// Returns expected records when knowable.
    pub const fn expected_records(self) -> Option<u64> {
        self.expected_records
    }

    /// Returns canonical records retained in the batch.
    pub const fn returned_records(self) -> u64 {
        self.returned_records
    }

    /// Returns known missing records.
    pub const fn missing_records(self) -> u64 {
        self.missing_records
    }

    /// Returns records outside the admitted request scope.
    pub const fn unexpected_records(self) -> u64 {
        self.unexpected_records
    }

    /// Returns provider-reported records when supplied.
    pub const fn provider_reported_records(self) -> Option<u64> {
        self.provider_reported_records
    }

    /// Returns exact raw-response page count.
    pub const fn page_count(self) -> NonZeroU16 {
        self.page_count
    }

    /// Returns exact cursor terminal state.
    pub const fn cursor(self) -> OptionMarketCursorState {
        self.cursor
    }

    /// Returns complete or unavailable disposition.
    pub const fn disposition(self) -> OptionMarketBatchDisposition {
        self.disposition
    }

    fn validate_row_count(self, row_count: usize) -> Result<(), ProviderCaptureError> {
        if u64::try_from(row_count).ok() != Some(self.returned_records) {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Deterministic identity of one ordered canonical option-market batch.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderOptionMarketContentIdentity {
    schema_fingerprint: EvidenceDigest,
    content_digest: EvidenceDigest,
    kind: OptionMarketBatchKind,
    row_count: usize,
}

impl ProviderOptionMarketContentIdentity {
    /// Returns the code-owned schema fingerprint.
    pub const fn schema_fingerprint(self) -> EvidenceDigest {
        self.schema_fingerprint
    }

    /// Returns deterministic identity over scope, completeness, and ordered canonical rows.
    pub const fn content_digest(self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the sole row kind in the batch.
    pub const fn kind(self) -> OptionMarketBatchKind {
        self.kind
    }

    /// Returns exact canonical row count.
    pub const fn row_count(self) -> usize {
        self.row_count
    }
}

#[derive(Debug)]
enum OptionMarketPayload {
    Expirations(Box<[OptionExpirationObservation]>),
    Snapshots(Box<[OptionSnapshotObservation]>),
}

/// One noncloneable, closed, bounded option expiration or snapshot batch.
#[derive(Debug)]
pub struct ProviderOptionMarketBatch {
    scope: OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
    payload: OptionMarketPayload,
    row_digests: Box<[EvidenceDigest]>,
    content_identity: ProviderOptionMarketContentIdentity,
}

impl ProviderOptionMarketBatch {
    /// Constructs a bounded expiration response. Option-chain filters are not valid here.
    pub fn try_expirations(
        scope: OptionMarketRequestScope,
        completeness: OptionMarketCompleteness,
        rows: Vec<OptionExpirationObservation>,
    ) -> Result<Self, ProviderCaptureError> {
        if !scope.filter.is_empty() {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        validate_expiration_rows(&scope, &rows)?;
        Self::finish(
            scope,
            completeness,
            OptionMarketPayload::Expirations(rows.into_boxed_slice()),
        )
    }

    /// Constructs a bounded option snapshot response.
    pub fn try_snapshots(
        scope: OptionMarketRequestScope,
        completeness: OptionMarketCompleteness,
        rows: Vec<OptionSnapshotObservation>,
    ) -> Result<Self, ProviderCaptureError> {
        validate_snapshot_rows(&scope, &rows)?;
        Self::finish(
            scope,
            completeness,
            OptionMarketPayload::Snapshots(rows.into_boxed_slice()),
        )
    }

    fn finish(
        scope: OptionMarketRequestScope,
        completeness: OptionMarketCompleteness,
        payload: OptionMarketPayload,
    ) -> Result<Self, ProviderCaptureError> {
        let kind = payload.kind();
        let row_count = payload.row_count();
        if row_count > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        completeness.validate_row_count(row_count)?;
        let (row_digests, content_identity) =
            option_market_content_identity(&scope, completeness, &payload, kind, row_count)?;
        Ok(Self {
            scope,
            completeness,
            payload,
            row_digests,
            content_identity,
        })
    }

    /// Returns exact source/request/identity/clock scope.
    pub const fn scope(&self) -> &OptionMarketRequestScope {
        &self.scope
    }

    /// Returns count, pagination, and terminal-completeness evidence.
    pub const fn completeness(&self) -> OptionMarketCompleteness {
        self.completeness
    }

    /// Returns the sole canonical row kind.
    pub const fn kind(&self) -> OptionMarketBatchKind {
        self.content_identity.kind
    }

    /// Returns expiration rows only for an expiration batch.
    pub const fn expirations(&self) -> Option<&[OptionExpirationObservation]> {
        match &self.payload {
            OptionMarketPayload::Expirations(rows) => Some(rows),
            OptionMarketPayload::Snapshots(_) => None,
        }
    }

    /// Returns snapshot rows only for a snapshot batch.
    pub const fn snapshots(&self) -> Option<&[OptionSnapshotObservation]> {
        match &self.payload {
            OptionMarketPayload::Expirations(_) => None,
            OptionMarketPayload::Snapshots(rows) => Some(rows),
        }
    }

    /// Returns exact canonical row count.
    pub const fn row_count(&self) -> usize {
        self.content_identity.row_count
    }

    /// Returns deterministic canonical batch identity.
    pub const fn content_identity(&self) -> ProviderOptionMarketContentIdentity {
        self.content_identity
    }

    /// Returns SHA-256 of one exact canonical row.
    pub fn canonical_row_digest(&self, ordinal: usize) -> Option<EvidenceDigest> {
        self.row_digests.get(ordinal).copied()
    }

    fn validate(&self) -> Result<(), ProviderCaptureError> {
        match &self.payload {
            OptionMarketPayload::Expirations(rows) => validate_expiration_rows(&self.scope, rows)?,
            OptionMarketPayload::Snapshots(rows) => validate_snapshot_rows(&self.scope, rows)?,
        }
        self.completeness
            .validate_row_count(self.payload.row_count())?;
        let (row_digests, identity) = option_market_content_identity(
            &self.scope,
            self.completeness,
            &self.payload,
            self.payload.kind(),
            self.payload.row_count(),
        )?;
        if row_digests != self.row_digests || identity != self.content_identity {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

impl OptionMarketPayload {
    const fn kind(&self) -> OptionMarketBatchKind {
        match self {
            Self::Expirations(_) => OptionMarketBatchKind::Expirations,
            Self::Snapshots(_) => OptionMarketBatchKind::Snapshots,
        }
    }

    const fn row_count(&self) -> usize {
        match self {
            Self::Expirations(rows) => rows.len(),
            Self::Snapshots(rows) => rows.len(),
        }
    }
}

/// Exact provider-native semantics aligned one-for-one to a canonical option batch.
#[derive(Debug)]
pub struct ProviderOptionMarketNativeLineageBatch {
    schema: ProviderNativeLineageSchema,
    content_identity: ProviderOptionMarketContentIdentity,
    rows: Box<[Bytes]>,
    row_digests: Box<[EvidenceDigest]>,
    batch_sidecar: Bytes,
    batch_sidecar_digest: EvidenceDigest,
    batch_digest: EvidenceDigest,
}

impl ProviderOptionMarketNativeLineageBatch {
    /// Binds bounded native row semantics and required batch-level response semantics.
    pub fn try_new(
        implementation: ProviderNativeLineageImplementation,
        batch: &ProviderOptionMarketBatch,
        rows: Vec<Bytes>,
        batch_sidecar: Bytes,
    ) -> Result<Self, ProviderCaptureError> {
        batch.validate()?;
        if rows.len() != batch.row_count() || rows.len() > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let mut retained = 0_usize;
        let mut row_digests = Vec::new();
        row_digests
            .try_reserve_exact(rows.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        for row in &rows {
            if row.is_empty() || row.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            retained = retained
                .checked_add(row.len())
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            row_digests.push(sha256(row));
        }
        if batch_sidecar.is_empty()
            || batch_sidecar.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        retained = retained
            .checked_add(batch_sidecar.len())
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if retained > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let schema = ProviderNativeLineageSchema::for_implementation(implementation);
        let content_identity = batch.content_identity();
        let batch_sidecar_digest = sha256(&batch_sidecar);
        let batch_digest = option_market_native_digest(
            schema,
            batch,
            &rows,
            &row_digests,
            batch_sidecar_digest,
            batch_sidecar.len(),
        )?;
        Ok(Self {
            schema,
            content_identity,
            rows: rows.into_boxed_slice(),
            row_digests: row_digests.into_boxed_slice(),
            batch_sidecar,
            batch_sidecar_digest,
            batch_digest,
        })
    }

    /// Returns the closed native-lineage schema.
    pub const fn schema(&self) -> ProviderNativeLineageSchema {
        self.schema
    }

    /// Returns the exact canonical batch identity this evidence describes.
    pub const fn content_identity(&self) -> ProviderOptionMarketContentIdentity {
        self.content_identity
    }

    /// Returns exact row-aligned provider-native semantics.
    pub const fn rows(&self) -> &[Bytes] {
        &self.rows
    }

    /// Returns SHA-256 of one exact native semantic row.
    pub fn row_digest(&self, ordinal: usize) -> Option<EvidenceDigest> {
        self.row_digests.get(ordinal).copied()
    }

    /// Returns required batch-level provider-native response semantics.
    pub const fn batch_sidecar(&self) -> &Bytes {
        &self.batch_sidecar
    }

    /// Returns SHA-256 of the batch-level provider-native semantics.
    pub const fn batch_sidecar_digest(&self) -> EvidenceDigest {
        self.batch_sidecar_digest
    }

    /// Returns deterministic identity over schema, canonical alignment, rows, and sidecar.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    fn validate(&self, batch: &ProviderOptionMarketBatch) -> Result<(), ProviderCaptureError> {
        let retained = self
            .rows
            .iter()
            .try_fold(0_usize, |bytes, row| bytes.checked_add(row.len()))
            .and_then(|bytes| bytes.checked_add(self.batch_sidecar.len()));
        if self.schema
            != ProviderNativeLineageSchema::for_implementation(self.schema.implementation())
            || self.content_identity != batch.content_identity()
            || self.rows.len() != batch.row_count()
            || self.row_digests.len() != self.rows.len()
            || self
                .rows
                .iter()
                .zip(&self.row_digests)
                .any(|(row, digest)| {
                    row.is_empty()
                        || row.len() > MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES
                        || sha256(row) != *digest
                })
            || self.batch_sidecar.is_empty()
            || self.batch_sidecar.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES
            || retained.is_none_or(|bytes| bytes > MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES)
            || sha256(&self.batch_sidecar) != self.batch_sidecar_digest
            || option_market_native_digest(
                self.schema,
                batch,
                &self.rows,
                &self.row_digests,
                self.batch_sidecar_digest,
                self.batch_sidecar.len(),
            )? != self.batch_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Exact canonical-row to HTTP-page and sealed physical-frame coordinate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderOptionMarketRowFrame {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderOptionMarketRowFrame {
    /// Returns zero-based canonical row ordinal.
    pub const fn canonical_row_ordinal(self) -> u32 {
        self.canonical_row_ordinal
    }

    /// Returns zero-based logical HTTP page ordinal.
    pub const fn capture_page_ordinal(self) -> u16 {
        self.capture_page_ordinal
    }

    /// Returns zero-based physical journal frame ordinal.
    pub const fn physical_frame_ordinal(self) -> u32 {
        self.physical_frame_ordinal
    }

    /// Returns SHA-256 of the exact provider response body.
    pub const fn page_body_digest(self) -> EvidenceDigest {
        self.page_body_digest
    }

    /// Returns exact socket-boundary receipt time.
    pub const fn received_at(self) -> Timestamp {
        self.received_at
    }

    /// Returns source sequence when one exists for the captured frame.
    pub const fn source_sequence(self) -> Option<u64> {
        self.source_sequence
    }
}

/// Copyable evidence identity of one complete sealed option-market binding.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderOptionMarketBindingDigest(EvidenceDigest);

impl ProviderOptionMarketBindingDigest {
    /// Returns algorithm-qualified immutable evidence.
    pub const fn evidence(self) -> EvidenceDigest {
        self.0
    }
}

/// Non-reusable option-market publication authority derived from one sealed HTTP response set.
#[derive(Debug)]
pub struct SealedProviderOptionMarketBinding {
    authority: ProviderWholeCaptureToken,
    batch: ProviderOptionMarketBatch,
    native_lineage: ProviderOptionMarketNativeLineageBatch,
    row_frames: Box<[ProviderOptionMarketRowFrame]>,
    evidence_digest: ProviderOptionMarketBindingDigest,
}

impl SealedProviderOptionMarketBinding {
    /// Consumes a complete whole-capture token and binds every canonical row to exact raw evidence.
    pub fn try_new(
        authority: ProviderWholeCaptureToken,
        batch: ProviderOptionMarketBatch,
        native_lineage: ProviderOptionMarketNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        batch.validate()?;
        validate_capture_scope(&authority, &batch)?;
        native_lineage.validate(&batch)?;
        let row_frames = option_market_row_frames(
            authority.persisted_receipt(),
            &batch,
            &row_capture_page_ordinals,
        )?;
        let evidence_digest = option_market_binding_digest(
            &authority,
            batch.content_identity(),
            &native_lineage,
            &row_frames,
        )?;
        Ok(Self {
            authority,
            batch,
            native_lineage,
            row_frames,
            evidence_digest,
        })
    }

    /// Revalidates canonical/native alignment and exact page/physical-frame coordinates.
    pub fn validate(&self) -> Result<(), ProviderCaptureError> {
        self.batch.validate()?;
        validate_capture_scope(&self.authority, &self.batch)?;
        self.native_lineage.validate(&self.batch)?;
        let ordinals = self
            .row_frames
            .iter()
            .map(|row| row.capture_page_ordinal)
            .collect::<Vec<_>>();
        let expected =
            option_market_row_frames(self.authority.persisted_receipt(), &self.batch, &ordinals)?;
        if expected.as_ref() != self.row_frames.as_ref()
            || option_market_binding_digest(
                &self.authority,
                self.batch.content_identity(),
                &self.native_lineage,
                &self.row_frames,
            )? != self.evidence_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }

    /// Returns the exact canonical option batch.
    pub const fn batch(&self) -> &ProviderOptionMarketBatch {
        &self.batch
    }

    /// Returns exact row-aligned provider-native evidence.
    pub const fn native_lineage(&self) -> &ProviderOptionMarketNativeLineageBatch {
        &self.native_lineage
    }

    /// Returns exact row-to-page/physical-frame coordinates.
    pub const fn row_frames(&self) -> &[ProviderOptionMarketRowFrame] {
        &self.row_frames
    }

    /// Returns deterministic complete-binding evidence identity.
    pub const fn evidence_digest(&self) -> ProviderOptionMarketBindingDigest {
        self.evidence_digest
    }

    /// Returns cloneable persisted capture evidence without reminting live authority.
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        self.authority.persisted_receipt()
    }
}

fn validate_expiration_rows(
    scope: &OptionMarketRequestScope,
    rows: &[OptionExpirationObservation],
) -> Result<(), ProviderCaptureError> {
    if rows.len() > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut dates = BTreeSet::new();
    for row in rows {
        if row.underlying_instrument_id() != scope.underlying_instrument_id
            || row.underlying_definition_revision() != scope.underlying_definition_revision
            || row.provider_instrument_id() != &scope.provider_instrument_id
            || !dates.insert(row.expiration())
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

fn validate_snapshot_rows(
    scope: &OptionMarketRequestScope,
    rows: &[OptionSnapshotObservation],
) -> Result<(), ProviderCaptureError> {
    if rows.len() > MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut instruments = BTreeSet::new();
    for row in rows {
        let terms = row.terms();
        if terms.underlying_instrument_id() != scope.underlying_instrument_id
            || terms.underlying_definition_revision() != scope.underlying_definition_revision
            || !instruments.insert(terms.option_instrument_id())
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        if let Some(range) = scope.filter.expiration_range {
            if terms.expiration() < range.start || terms.expiration() > range.end {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
        if let Some(range) = scope.filter.strike_range {
            if terms.strike().currency() != range.minimum.currency()
                || terms.strike().amount() < range.minimum.amount()
                || terms.strike().amount() > range.maximum.amount()
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
        if scope.filter.kind.is_some_and(|kind| kind != terms.kind())
            || (!scope.filter.contracts.is_empty()
                && !scope
                    .filter
                    .contracts
                    .iter()
                    .any(|contract| contract == terms.provider_instrument_id()))
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

fn option_market_content_identity(
    scope: &OptionMarketRequestScope,
    completeness: OptionMarketCompleteness,
    payload: &OptionMarketPayload,
    kind: OptionMarketBatchKind,
    row_count: usize,
) -> Result<(Box<[EvidenceDigest]>, ProviderOptionMarketContentIdentity), ProviderCaptureError> {
    let schema_fingerprint = option_market_schema_fingerprint();
    let mut content = Sha256::new();
    hash_field(&mut content, OPTION_MARKET_CONTENT_DOMAIN)?;
    hash_digest(&mut content, schema_fingerprint);
    hash_field(&mut content, kind.tag())?;
    let header = serialize_bounded(
        &(scope, completeness),
        MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES,
        MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES,
    )?;
    hash_field(&mut content, &header)?;
    hash_length(&mut content, row_count)?;
    let mut retained_bytes = header.len();
    let mut row_digests = Vec::new();
    row_digests
        .try_reserve_exact(row_count)
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    match payload {
        OptionMarketPayload::Expirations(rows) => {
            for row in rows {
                hash_option_row(&mut content, &mut retained_bytes, &mut row_digests, row)?;
            }
        }
        OptionMarketPayload::Snapshots(rows) => {
            for row in rows {
                hash_option_row(&mut content, &mut retained_bytes, &mut row_digests, row)?;
            }
        }
    }
    Ok((
        row_digests.into_boxed_slice(),
        ProviderOptionMarketContentIdentity {
            schema_fingerprint,
            content_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, content.finalize().into()),
            kind,
            row_count,
        },
    ))
}

fn hash_option_row<T: Serialize>(
    content: &mut Sha256,
    retained_bytes: &mut usize,
    row_digests: &mut Vec<EvidenceDigest>,
    row: &T,
) -> Result<(), ProviderCaptureError> {
    let payload = serialize_bounded(
        row,
        MAX_PROVIDER_OPTION_MARKET_ROW_BYTES,
        MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES,
    )?;
    *retained_bytes = retained_bytes.checked_add(payload.len()).ok_or(
        ProviderCaptureError::ByteLimitExceeded {
            max: MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES as u64,
        },
    )?;
    if *retained_bytes > MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES {
        return Err(ProviderCaptureError::ByteLimitExceeded {
            max: MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES as u64,
        });
    }
    let digest = sha256(&payload);
    hash_field(content, &payload)?;
    row_digests.push(digest);
    Ok(())
}

fn option_market_native_digest(
    schema: ProviderNativeLineageSchema,
    batch: &ProviderOptionMarketBatch,
    rows: &[Bytes],
    row_digests: &[EvidenceDigest],
    sidecar_digest: EvidenceDigest,
    sidecar_bytes: usize,
) -> Result<EvidenceDigest, ProviderCaptureError> {
    if rows.len() != batch.row_count() || rows.len() != row_digests.len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_MARKET_NATIVE_DOMAIN)?;
    digest.update(schema.version().to_be_bytes());
    hash_digest(&mut digest, schema.fingerprint());
    hash_digest(&mut digest, batch.content_identity().content_digest());
    hash_length(&mut digest, rows.len())?;
    for (ordinal, ((row, native_digest), canonical_digest)) in rows
        .iter()
        .zip(row_digests)
        .zip(&batch.row_digests)
        .enumerate()
    {
        digest.update(
            u32::try_from(ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
                .to_be_bytes(),
        );
        hash_digest(&mut digest, *canonical_digest);
        hash_length(&mut digest, row.len())?;
        hash_digest(&mut digest, *native_digest);
    }
    hash_length(&mut digest, sidecar_bytes)?;
    hash_digest(&mut digest, sidecar_digest);
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn validate_capture_scope(
    authority: &ProviderWholeCaptureToken,
    batch: &ProviderOptionMarketBatch,
) -> Result<(), ProviderCaptureError> {
    let capture = authority.persisted_receipt().capture();
    let scope = batch.scope();
    if capture.source_id() != scope.source_id()
        || capture.metadata_revision() != scope.metadata_revision()
        || capture.dataset() != scope.dataset()
        || capture.request_set_identity() != scope.request_identity()
        || capture.observation_digest() != scope.observation_identity()
        || capture.pages().len() != usize::from(batch.completeness().page_count().get())
        || capture
            .pages()
            .last()
            .is_none_or(|page| page.received_at() != scope.received_at())
        || capture
            .pages()
            .iter()
            .any(|page| page.received_at() > scope.ingested_at())
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let expected_cursor = match capture.terminal() {
        ProviderCaptureTerminalDisposition::StandaloneResponse => {
            OptionMarketCursorState::NotApplicable
        }
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage => {
            OptionMarketCursorState::Exhausted
        }
        ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    };
    if batch.completeness().cursor() != expected_cursor {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok(())
}

fn option_market_row_frames(
    receipt: &SealedProviderCaptureSetReceipt,
    batch: &ProviderOptionMarketBatch,
    page_ordinals: &[u16],
) -> Result<Box<[ProviderOptionMarketRowFrame]>, ProviderCaptureError> {
    if page_ordinals.len() != batch.row_count() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let pages = receipt.capture().pages();
    let physical_frames = receipt.segment().frames();
    if pages.len() != physical_frames.len() {
        return Err(ProviderCaptureError::PhysicalReceiptMismatch);
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(page_ordinals.len())
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    for (ordinal, page_ordinal) in page_ordinals.iter().copied().enumerate() {
        let page = pages
            .get(usize::from(page_ordinal))
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let frame = physical_frames
            .get(usize::from(page_ordinal))
            .ok_or(ProviderCaptureError::PhysicalReceiptMismatch)?;
        if page.ordinal() != page_ordinal
            || frame.provider_payload_digest() != page.body_digest()
            || frame.provider_payload_bytes() != page.body_bytes()
            || frame.received_at() != page.received_at()
        {
            return Err(ProviderCaptureError::PhysicalReceiptMismatch);
        }
        rows.push(ProviderOptionMarketRowFrame {
            canonical_row_ordinal: u32::try_from(ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            capture_page_ordinal: page_ordinal,
            physical_frame_ordinal: frame.ordinal(),
            page_body_digest: page.body_digest(),
            received_at: page.received_at(),
            source_sequence: frame.source_sequence(),
        });
    }
    Ok(rows.into_boxed_slice())
}

fn option_market_binding_digest(
    authority: &ProviderWholeCaptureToken,
    content_identity: ProviderOptionMarketContentIdentity,
    native_lineage: &ProviderOptionMarketNativeLineageBatch,
    row_frames: &[ProviderOptionMarketRowFrame],
) -> Result<ProviderOptionMarketBindingDigest, ProviderCaptureError> {
    let receipt = authority.persisted_receipt();
    let mut digest = Sha256::new();
    hash_field(&mut digest, OPTION_MARKET_BINDING_DOMAIN)?;
    hash_digest(&mut digest, receipt.receipt_digest());
    hash_digest(&mut digest, receipt.capture().content_digest());
    hash_digest(&mut digest, receipt.capture().observation_digest());
    hash_digest(&mut digest, receipt.segment().content_digest());
    hash_digest(&mut digest, receipt.segment().physical_receipt_digest());
    hash_digest(&mut digest, content_identity.schema_fingerprint());
    hash_digest(&mut digest, content_identity.content_digest());
    hash_field(&mut digest, content_identity.kind().tag())?;
    hash_length(&mut digest, content_identity.row_count())?;
    hash_digest(&mut digest, native_lineage.schema().fingerprint());
    hash_digest(&mut digest, native_lineage.batch_digest());
    hash_length(&mut digest, row_frames.len())?;
    for row in row_frames {
        digest.update(row.canonical_row_ordinal.to_be_bytes());
        digest.update(row.capture_page_ordinal.to_be_bytes());
        digest.update(row.physical_frame_ordinal.to_be_bytes());
        hash_digest(&mut digest, row.page_body_digest);
        digest.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                digest.update([1]);
                digest.update(sequence.to_be_bytes());
            }
            None => digest.update([0]),
        }
    }
    Ok(ProviderOptionMarketBindingDigest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    )))
}

fn option_market_schema_fingerprint() -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(OPTION_MARKET_SCHEMA_DOMAIN);
    digest.update(PROVIDER_OPTION_MARKET_SCHEMA_VERSION.to_be_bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn require_sha256(evidence: EvidenceDigest) -> Result<(), ProviderCaptureError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256
        || evidence.bytes().iter().all(|byte| *byte == 0)
    {
        return Err(ProviderCaptureError::InvalidDigest);
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_digest(digest: &mut Sha256, evidence: EvidenceDigest) {
    digest.update(match evidence.algorithm() {
        DigestAlgorithm::Sha256 => [1],
        DigestAlgorithm::Blake3 => [2],
    });
    digest.update(evidence.bytes());
}

fn hash_field(digest: &mut Sha256, value: &[u8]) -> Result<(), ProviderCaptureError> {
    hash_length(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn hash_length(digest: &mut Sha256, length: usize) -> Result<(), ProviderCaptureError> {
    digest.update(
        u64::try_from(length)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            .to_be_bytes(),
    );
    Ok(())
}

fn serialize_bounded<T: Serialize + ?Sized>(
    value: &T,
    item_limit: usize,
    aggregate_limit: usize,
) -> Result<Vec<u8>, ProviderCaptureError> {
    let mut writer = OptionMarketWriter {
        bytes: Vec::new(),
        item_limit,
        aggregate_limit,
        limit_exceeded: false,
        allocation_failed: false,
    };
    if serde_json::to_writer(&mut writer, value).is_err() {
        return Err(if writer.limit_exceeded {
            ProviderCaptureError::ByteLimitExceeded {
                max: u64::try_from(item_limit).unwrap_or(u64::MAX),
            }
        } else if writer.allocation_failed {
            ProviderCaptureError::AllocationFailed
        } else {
            ProviderCaptureError::SealedBindingMismatch
        });
    }
    Ok(writer.bytes)
}

struct OptionMarketWriter {
    bytes: Vec<u8>,
    item_limit: usize,
    aggregate_limit: usize,
    limit_exceeded: bool,
    allocation_failed: bool,
}

impl Write for OptionMarketWriter {
    fn write(&mut self, value: &[u8]) -> io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(value.len()) else {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "option-market serialization length overflow",
            ));
        };
        if next_len > self.item_limit || next_len > self.aggregate_limit {
            self.limit_exceeded = true;
            return Err(io::Error::other(
                "option-market serialization exceeds bound",
            ));
        }
        if self.bytes.try_reserve_exact(value.len()).is_err() {
            self.allocation_failed = true;
            return Err(io::Error::other("option-market bounded allocation failed"));
        }
        self.bytes.extend_from_slice(value);
        Ok(value.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
