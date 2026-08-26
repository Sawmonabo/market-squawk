//! Fail-closed provider-local doctor and activation evidence for OCC/Cboe references.
//!
//! This module performs no provider network work and grants no canonical-instrument authority. It
//! reconciles a capability-store durability probe, one restart recovery, exact core raw receipts,
//! and two provider-native typed reads. OCC memo discovery and raw documents remain explicitly
//! separate from operative lifecycle interpretation.

use std::num::NonZeroU32;

use market_squawk_domain::{
    CalendarDate, DigestAlgorithm, EvidenceDigest, SourceIdentifier, Timestamp,
};
use serde::Serialize;
use thiserror::Error;

use crate::store::{
    ReferenceGenerationObjectEvidence, ReferenceRecoveryOutcome, ReferenceStoreActivationReceipt,
};
use crate::{
    CanonicalReferenceIdentityState, CboeSeriesReference, CboeVenue, HttpLastModifiedEvidence,
    ObjectClockEvidence, OccDlpProductReference, PublicationCompleteness, ReferenceGeneration,
    ReferenceStoreError, ReferenceSurface, ReferenceTransportError,
};

const CORE_OBJECT_COUNT: usize = 5;
const MAX_ACTIVATION_REASONS: usize = 32;

/// The four independently published Cboe `All Series` files required for core activation.
///
/// The array is deterministic reporting order only; it does not assert a shared atomic source
/// clock.
pub const REQUIRED_CBOE_VENUES: [CboeVenue; 4] = [
    CboeVenue::C1,
    CboeVenue::Bzx,
    CboeVenue::C2,
    CboeVenue::Edgx,
];

/// Provider-local currentness disposition for selected OCC and Cboe reference objects.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceCurrentnessDisposition {
    /// Integrity and reachability passed; root application freshness policy must classify age.
    RequiresApplicationFreshnessClassification,
}

/// One exact parsed Cboe row and one exact parsed OCC DLP row used as bounded query witnesses.
#[derive(Clone, Copy, Debug)]
pub struct OptionsReferenceQueryProbe<'a> {
    cboe: &'a CboeSeriesReference,
    occ_dlp: &'a OccDlpProductReference,
}

impl<'a> OptionsReferenceQueryProbe<'a> {
    /// Selects exactly one witness for each provider-native typed query family.
    pub const fn new(cboe: &'a CboeSeriesReference, occ_dlp: &'a OccDlpProductReference) -> Self {
        Self { cboe, occ_dlp }
    }
}

/// Inputs to one local doctor evaluation.
///
/// Recovery always starts from the store's durable activation history, preventing a caller from
/// supplying a stale receipt subset or an old already-open generation. Exact core raw evidence is
/// reconstructed only from the content-digested generation and reverified raw-object closure.
#[derive(Debug)]
pub struct OptionsReferenceDoctorInput<'a> {
    recovery: &'a ReferenceRecoveryOutcome,
    storage_activation: Option<&'a ReferenceStoreActivationReceipt>,
    query_probe: Option<OptionsReferenceQueryProbe<'a>>,
    occ_memo: &'a OccMemoAcquisitionState,
}

impl<'a> OptionsReferenceDoctorInput<'a> {
    /// Binds an explicit prior activation/recovery snapshot, typed-query witnesses, and optional
    /// memo evidence. Evaluation itself performs no create, repair, quarantine, or exclusive lock.
    pub const fn new(
        recovery: &'a ReferenceRecoveryOutcome,
        storage_activation: Option<&'a ReferenceStoreActivationReceipt>,
        query_probe: Option<OptionsReferenceQueryProbe<'a>>,
        occ_memo: &'a OccMemoAcquisitionState,
    ) -> Self {
        Self {
            recovery,
            storage_activation,
            query_probe,
            occ_memo,
        }
    }
}

/// Redacted local failure class suitable for Desktop, CLI, and MCP output.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceLocalFailure {
    /// A capability root or child entry had an unsafe type or symlink state.
    UnsafeStoreEntry,
    /// A bounded local filesystem operation failed.
    StoreIo,
    /// Publication was explicitly cancelled before atomic activation.
    PublicationCancelled,
    /// Publication deadline elapsed before atomic activation.
    PublicationDeadlineExceeded,
    /// Trusted publication-control evidence was unavailable.
    PublicationControlUnavailable,
    /// Raw/generation evidence was malformed, missing, corrupt, or divergent.
    EvidenceInvalid,
    /// Another process owns the bounded activation transaction.
    ActivationBusy,
    /// Another process owns the bounded provider publication spool.
    PublicationBusy,
    /// A bounded allocation, recovery, or query ceiling was exceeded.
    ResourceLimitExceeded,
    /// The immutable generation schema or database was incompatible.
    GenerationIncompatible,
    /// An exact query or provider-row reconstruction was rejected.
    QueryRejected,
    /// Descriptor-backed immutable SQLite reopening is unavailable on this platform.
    CapabilityDatabaseUnavailable,
}

impl From<ReferenceStoreError> for OptionsReferenceLocalFailure {
    fn from(error: ReferenceStoreError) -> Self {
        match error {
            ReferenceStoreError::PublicationCancelled => Self::PublicationCancelled,
            ReferenceStoreError::PublicationDeadlineExceeded => Self::PublicationDeadlineExceeded,
            ReferenceStoreError::PublicationControlUnavailable => {
                Self::PublicationControlUnavailable
            }
            ReferenceStoreError::UnsafeStoreEntry => Self::UnsafeStoreEntry,
            ReferenceStoreError::StoreIo => Self::StoreIo,
            ReferenceStoreError::RawEvidenceMismatch
            | ReferenceStoreError::InvalidReceipt
            | ReferenceStoreError::ObjectMissing
            | ReferenceStoreError::ObjectCorrupt
            | ReferenceStoreError::RawGenerationDivergence
            | ReferenceStoreError::InvalidActivationManifest => Self::EvidenceInvalid,
            ReferenceStoreError::CapacityUnavailable
            | ReferenceStoreError::InsufficientDisk { .. }
            | ReferenceStoreError::QueryLimitExceeded
            | ReferenceStoreError::RecoveryLimitExceeded => Self::ResourceLimitExceeded,
            ReferenceStoreError::InvalidGeneration | ReferenceStoreError::Sqlite => {
                Self::GenerationIncompatible
            }
            ReferenceStoreError::InvalidQuery
            | ReferenceStoreError::Cboe
            | ReferenceStoreError::Occ => Self::QueryRejected,
            ReferenceStoreError::CapabilityDatabaseUnavailable => {
                Self::CapabilityDatabaseUnavailable
            }
            ReferenceStoreError::ActivationBusy => Self::ActivationBusy,
            ReferenceStoreError::StagingBusy => Self::PublicationBusy,
        }
    }
}

/// Restart health for the last independently verified complete generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum OptionsReferenceGenerationHealth {
    /// Recovery completed without a verified complete generation.
    NoCompleteGeneration {
        /// Newer receipted candidates rejected during recovery.
        rejected_candidates: u32,
        /// Interrupted staging objects moved out of publication namespaces.
        quarantined_staging: u32,
    },
    /// Recovery failed before selection.
    Failed {
        /// Redacted fail-closed recovery cause.
        failure: OptionsReferenceLocalFailure,
    },
    /// One immutable generation and its complete raw-object closure reopened successfully.
    Recovered {
        /// Exact immutable generation identity.
        generation_id: SourceIdentifier,
        /// Exact sealed SQLite content identity.
        database_digest: EvidenceDigest,
        /// Exact sealed SQLite byte count.
        database_bytes: u64,
        /// Newer candidates rejected before selecting this last-complete fallback.
        rejected_newer_candidates: u32,
        /// Interrupted staging objects quarantined during recovery.
        quarantined_staging: u32,
    },
}

/// Independent source and local clocks for one exact raw reference object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionsReferenceObjectClockEvidence {
    object_id: SourceIdentifier,
    payload_digest: EvidenceDigest,
    payload_bytes: u64,
    source_filename: Option<SourceIdentifier>,
    source_publication_date: Option<CalendarDate>,
    http_last_modified: Option<HttpLastModifiedEvidence>,
    clocks: ObjectClockEvidence,
}

impl OptionsReferenceObjectClockEvidence {
    fn from_generation_object(raw: &ReferenceGenerationObjectEvidence) -> Self {
        Self {
            object_id: raw.object_id().clone(),
            payload_digest: raw.payload_digest(),
            payload_bytes: raw.payload_bytes(),
            source_filename: raw.source_filename().cloned(),
            source_publication_date: raw.source_publication_date(),
            http_last_modified: raw.http_last_modified().cloned(),
            clocks: raw.clocks().clone(),
        }
    }

    /// Returns the exact content-derived object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns SHA-256 of the exact independently retained provider object.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the exact retained provider-object byte count.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the exact provider filename when supplied.
    pub const fn source_filename(&self) -> Option<&SourceIdentifier> {
        self.source_filename.as_ref()
    }

    /// Returns the provider filename/report date without inventing an instant.
    pub const fn source_publication_date(&self) -> Option<CalendarDate> {
        self.source_publication_date
    }

    /// Returns the independent exact HTTP `Last-Modified` field.
    pub const fn http_last_modified(&self) -> Option<&HttpLastModifiedEvidence> {
        self.http_last_modified.as_ref()
    }

    /// Returns provider-native coordinates, conservative availability, and local receipt time.
    pub const fn clocks(&self) -> &ObjectClockEvidence {
        &self.clocks
    }

    /// Returns monotonic HTTP send through terminal response-body latency for this object.
    pub const fn transport_elapsed_nanos(&self) -> u64 {
        self.clocks.transport_elapsed_nanos()
    }
}

/// One independently published and independently clocked Cboe object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CboeReferenceObjectDoctorEvidence {
    venue: CboeVenue,
    object: OptionsReferenceObjectClockEvidence,
}

impl CboeReferenceObjectDoctorEvidence {
    /// Returns the independently publishing venue.
    pub const fn venue(&self) -> CboeVenue {
        self.venue
    }

    /// Returns object clocks without any cross-venue atomicity assertion.
    pub const fn object(&self) -> &OptionsReferenceObjectClockEvidence {
        &self.object
    }
}

/// Selected OCC DLP representation for one complete generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccDlpRepresentation {
    /// Selected-field text directory without a provider report date.
    SelectedText,
    /// Dated daily headerless text publication.
    DailyText,
    /// Dated daily XML publication.
    DailyXml,
}

impl OccDlpRepresentation {
    fn from_surface(surface: &ReferenceSurface) -> Option<Self> {
        match surface {
            ReferenceSurface::OccDlpSelectedText => Some(Self::SelectedText),
            ReferenceSurface::OccDlpDailyText => Some(Self::DailyText),
            ReferenceSurface::OccDlpDailyXml => Some(Self::DailyXml),
            _ => None,
        }
    }
}

/// OCC DLP completeness and exact object evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum OccDlpDoctorState {
    /// No single complete generation-bound representation was proven.
    NotVerified,
    /// One exact representation completed with nonempty product/root rows.
    Complete {
        /// Selected wire representation.
        representation: OccDlpRepresentation,
        /// Valid provider rows admitted by the generation.
        product_records: u64,
        /// Exact source object and independent clocks.
        object: OptionsReferenceObjectClockEvidence,
    },
}

/// Exact nonempty official OCC memo RSS discovery evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccMemoRssDiscoveryEvidence {
    payload_digest: EvidenceDigest,
    discovered_memos: NonZeroU32,
    received_at: Timestamp,
}

impl OccMemoRssDiscoveryEvidence {
    /// Constructs RSS discovery evidence from one exact official feed object.
    ///
    /// # Errors
    ///
    /// Rejects a non-SHA-256/all-zero identity or an empty discovery count.
    pub fn try_new(
        payload_digest: EvidenceDigest,
        discovered_memos: u32,
        received_at: Timestamp,
    ) -> Result<Self, OptionsReferenceDoctorError> {
        if payload_digest.algorithm() != DigestAlgorithm::Sha256
            || payload_digest.bytes().iter().all(|byte| *byte == 0)
        {
            return Err(OptionsReferenceDoctorError::InvalidMemoEvidence);
        }
        Ok(Self {
            payload_digest,
            discovered_memos: NonZeroU32::new(discovered_memos)
                .ok_or(OptionsReferenceDoctorError::InvalidMemoEvidence)?,
            received_at,
        })
    }

    /// Returns the exact RSS payload identity.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns valid RSS discoveries, never requested memo identifiers.
    pub const fn discovered_memos(&self) -> NonZeroU32 {
        self.discovered_memos
    }

    /// Returns the trusted local receipt instant.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
}

/// Exact complete memo/document raw-object closure that remains uninterpreted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OccMemoDocumentClosureEvidence {
    closure_digest: EvidenceDigest,
    complete_documents: NonZeroU32,
    complete_attachments: u32,
    received_at: Timestamp,
}

impl OccMemoDocumentClosureEvidence {
    /// Constructs a complete raw-document closure without granting lifecycle economics.
    ///
    /// # Errors
    ///
    /// Rejects a non-SHA-256/all-zero closure identity or zero complete documents.
    pub fn try_new(
        closure_digest: EvidenceDigest,
        complete_documents: u32,
        complete_attachments: u32,
        received_at: Timestamp,
    ) -> Result<Self, OptionsReferenceDoctorError> {
        if closure_digest.algorithm() != DigestAlgorithm::Sha256
            || closure_digest.bytes().iter().all(|byte| *byte == 0)
        {
            return Err(OptionsReferenceDoctorError::InvalidMemoEvidence);
        }
        Ok(Self {
            closure_digest,
            complete_documents: NonZeroU32::new(complete_documents)
                .ok_or(OptionsReferenceDoctorError::InvalidMemoEvidence)?,
            complete_attachments,
            received_at,
        })
    }

    /// Returns the digest binding all complete memo and required attachment objects.
    pub const fn closure_digest(&self) -> EvidenceDigest {
        self.closure_digest
    }

    /// Returns complete memo body objects.
    pub const fn complete_documents(&self) -> NonZeroU32 {
        self.complete_documents
    }

    /// Returns complete required attachments, which may be zero.
    pub const fn complete_attachments(&self) -> u32 {
        self.complete_attachments
    }

    /// Returns when the complete closure was observed locally.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
}

/// Public disposition of OCC memo acquisition without exposing a forgeable report constructor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OccMemoAcquisitionDisposition {
    /// Memo acquisition was not selected for core-reference activation.
    NotSelected,
    /// Memo acquisition was selected but lacks a current probe.
    ProbeRequired,
    /// The selected acquisition path failed without an admitted anti-bot challenge.
    Unavailable,
    /// Official RSS supplied bounded discovery metadata only.
    RssDiscoveryAvailable,
    /// Operative-document acquisition hit the exact transport challenge classifier.
    ProviderAntiBotChallenge,
    /// Complete memo/attachment bytes exist, but lifecycle economics remain uninterpreted.
    CompleteOperativeDocumentsUninterpreted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
enum OccMemoAcquisitionKind {
    NotSelected,
    ProbeRequired,
    Unavailable,
    RssDiscoveryAvailable {
        evidence: OccMemoRssDiscoveryEvidence,
    },
    ProviderAntiBotChallenge {
        rss_discovery: Option<OccMemoRssDiscoveryEvidence>,
    },
    CompleteOperativeDocumentsUninterpreted {
        evidence: OccMemoDocumentClosureEvidence,
    },
}

/// Plain OCC memo acquisition state; discovery text never becomes operative economics.
///
/// Its internal variant is private so challenge and complete-document states can be minted only by
/// their validating constructors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OccMemoAcquisitionState(OccMemoAcquisitionKind);

impl OccMemoAcquisitionState {
    /// Marks memo acquisition outside this core-reference activation.
    pub const fn not_selected() -> Self {
        Self(OccMemoAcquisitionKind::NotSelected)
    }

    /// Marks a selected memo capability that still requires a current probe.
    pub const fn probe_required() -> Self {
        Self(OccMemoAcquisitionKind::ProbeRequired)
    }

    /// Marks a selected memo acquisition path unavailable without a challenge classification.
    pub const fn unavailable() -> Self {
        Self(OccMemoAcquisitionKind::Unavailable)
    }

    /// Retains exact RSS discovery-only evidence.
    pub fn rss_discovery_available(evidence: OccMemoRssDiscoveryEvidence) -> Self {
        Self(OccMemoAcquisitionKind::RssDiscoveryAvailable { evidence })
    }

    /// Creates the challenge state only from the transport's exact classifier.
    ///
    /// # Errors
    ///
    /// Rejects every other transport failure so HTTP status alone cannot mint this state.
    pub fn try_provider_anti_bot_challenge(
        error: &ReferenceTransportError,
        rss_discovery: Option<OccMemoRssDiscoveryEvidence>,
    ) -> Result<Self, OptionsReferenceDoctorError> {
        if !matches!(error, ReferenceTransportError::ProviderAntiBotChallenge) {
            return Err(OptionsReferenceDoctorError::InvalidMemoEvidence);
        }
        Ok(Self(OccMemoAcquisitionKind::ProviderAntiBotChallenge {
            rss_discovery,
        }))
    }

    /// Retains a complete raw document closure without claiming interpreted lifecycle economics.
    pub fn complete_documents_uninterpreted(evidence: OccMemoDocumentClosureEvidence) -> Self {
        Self(OccMemoAcquisitionKind::CompleteOperativeDocumentsUninterpreted { evidence })
    }

    /// Returns the closed acquisition disposition.
    pub const fn disposition(&self) -> OccMemoAcquisitionDisposition {
        match &self.0 {
            OccMemoAcquisitionKind::NotSelected => OccMemoAcquisitionDisposition::NotSelected,
            OccMemoAcquisitionKind::ProbeRequired => OccMemoAcquisitionDisposition::ProbeRequired,
            OccMemoAcquisitionKind::Unavailable => OccMemoAcquisitionDisposition::Unavailable,
            OccMemoAcquisitionKind::RssDiscoveryAvailable { .. } => {
                OccMemoAcquisitionDisposition::RssDiscoveryAvailable
            }
            OccMemoAcquisitionKind::ProviderAntiBotChallenge { .. } => {
                OccMemoAcquisitionDisposition::ProviderAntiBotChallenge
            }
            OccMemoAcquisitionKind::CompleteOperativeDocumentsUninterpreted { .. } => {
                OccMemoAcquisitionDisposition::CompleteOperativeDocumentsUninterpreted
            }
        }
    }

    /// Returns bounded RSS discovery evidence when the selected state retained it.
    pub const fn rss_discovery(&self) -> Option<&OccMemoRssDiscoveryEvidence> {
        match &self.0 {
            OccMemoAcquisitionKind::RssDiscoveryAvailable { evidence } => Some(evidence),
            OccMemoAcquisitionKind::ProviderAntiBotChallenge { rss_discovery } => {
                rss_discovery.as_ref()
            }
            OccMemoAcquisitionKind::NotSelected
            | OccMemoAcquisitionKind::ProbeRequired
            | OccMemoAcquisitionKind::Unavailable
            | OccMemoAcquisitionKind::CompleteOperativeDocumentsUninterpreted { .. } => None,
        }
    }

    /// Returns the complete raw-document closure when one was retained.
    pub const fn document_closure(&self) -> Option<&OccMemoDocumentClosureEvidence> {
        match &self.0 {
            OccMemoAcquisitionKind::CompleteOperativeDocumentsUninterpreted { evidence } => {
                Some(evidence)
            }
            OccMemoAcquisitionKind::NotSelected
            | OccMemoAcquisitionKind::ProbeRequired
            | OccMemoAcquisitionKind::Unavailable
            | OccMemoAcquisitionKind::RssDiscoveryAvailable { .. }
            | OccMemoAcquisitionKind::ProviderAntiBotChallenge { .. } => None,
        }
    }
}

/// Query family used by a typed activation reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceQueryFamily {
    /// Exact Cboe Symbol ID read.
    CboeSymbol,
    /// Exact OCC product/root plus product-type read.
    OccDlpProduct,
}

/// Fail-closed disposition of one typed query witness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum OptionsReferenceQueryFailure {
    /// The exact key returned no row.
    Missing,
    /// Returned fields or exact row evidence differed from the parsed witness.
    EvidenceMismatch,
    /// The bounded store query rejected the operation.
    Rejected {
        /// Redacted query cause.
        failure: OptionsReferenceLocalFailure,
    },
}

/// Activation state. Only `Available` and `Degraded` permit exact core reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionsReferenceActivationState {
    /// Store configuration exists, but durable recovery found no complete core generation.
    Configured,
    /// Some non-failing evidence exists, but a mandatory core proof is absent.
    ProbeRequired,
    /// A substantiated store, generation, surface, raw, or query defect exists.
    Unavailable,
    /// Core references work while fallback recovery or optional memo capability is reduced.
    Degraded,
    /// Every required core proof passed and no selected optional capability is degraded.
    Available,
}

impl OptionsReferenceActivationState {
    /// Returns whether exact provider-native core queries may be served.
    pub const fn core_query_eligible(self) -> bool {
        matches!(self, Self::Available | Self::Degraded)
    }
}

/// Closed, path-free explanation for an activation disposition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum OptionsReferenceActivationReason {
    /// The capability create/write/fsync/read/unlink/directory-fsync probe failed.
    StoreProbeFailed {
        /// Redacted fail-closed local cause.
        failure: OptionsReferenceLocalFailure,
    },
    /// The explicit mutating storage-activation probe has not supplied a valid receipt.
    StoreProbeRequired,
    /// The bounded restart scan failed.
    RecoveryFailed {
        /// Redacted fail-closed recovery cause.
        failure: OptionsReferenceLocalFailure,
    },
    /// Recovery found no independently verified complete generation.
    NoCompleteGeneration,
    /// A last-complete fallback or interrupted staging quarantine reduced health.
    RecoveryDegraded {
        /// Newer candidates rejected before fallback.
        rejected_newer_candidates: u32,
        /// Interrupted staging objects quarantined.
        quarantined_staging: u32,
    },
    /// The recovered catalog was partial, conflicted, empty, or structurally incompatible.
    CatalogIneligible,
    /// One required Cboe file was absent.
    MissingCboeSurface {
        /// Missing venue.
        venue: CboeVenue,
    },
    /// The generation did not contain exactly one OCC DLP representation.
    InvalidOccDlpClosure,
    /// An OCC memo surface entered the immutable five-object core generation.
    UnexpectedGenerationSurface,
    /// The exact five raw core receipts are required.
    CoreRawEvidenceRequired,
    /// The raw core slice was nonempty but did not contain exactly five objects.
    CoreRawClosureMismatch {
        /// Supplied receipt count, saturated only beyond `u32` representation.
        observed: u32,
    },
    /// The recovered generation did not bind exactly the same five core raw objects.
    GenerationRawClosureMismatch {
        /// Generation-bound raw object count, saturated only beyond `u32` representation.
        observed: u32,
    },
    /// Current store verification rejected one raw core receipt.
    RawObjectVerificationFailed {
        /// Exact affected surface.
        surface: ReferenceSurface,
        /// Redacted store cause.
        failure: OptionsReferenceLocalFailure,
    },
    /// Raw transport/context evidence diverged or was absent from the generation closure.
    RawObjectEvidenceMismatch {
        /// Exact affected surface.
        surface: ReferenceSurface,
    },
    /// A Cboe object omitted required independent filename/date/HTTP clock evidence.
    CboeClockEvidenceIncomplete {
        /// Affected venue.
        venue: CboeVenue,
    },
    /// OCC DLP representation, filename, date, HTTP clock, or generation binding was incomplete.
    OccDlpEvidenceIncomplete,
    /// One exact parsed Cboe and OCC row is required to prove typed reads.
    QueryProbeRequired,
    /// One exact bounded typed read failed.
    TypedQueryFailed {
        /// Query family.
        family: OptionsReferenceQueryFamily,
        /// Fail-closed disposition.
        failure: OptionsReferenceQueryFailure,
    },
    /// Selected memo capability still requires a current probe.
    OccMemoProbeRequired,
    /// Selected memo acquisition is unavailable.
    OccMemoUnavailable,
    /// RSS works, but supplies discovery metadata rather than operative documents.
    OccMemoRssDiscoveryOnly,
    /// Operative memo acquisition hit the exact provider anti-bot challenge classifier.
    OccMemoProviderAntiBotChallenge,
    /// Complete memo bytes exist, but lifecycle economics remain uninterpreted.
    OccMemoDocumentsUninterpreted,
}

/// Bounded provider-local doctor report; private fields prevent caller-minted activation reports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OptionsReferenceDoctorReport {
    store_write_and_sync_verified: bool,
    generation: OptionsReferenceGenerationHealth,
    core_reference_verified: bool,
    cboe_objects: Vec<CboeReferenceObjectDoctorEvidence>,
    occ_dlp: OccDlpDoctorState,
    typed_queries_verified: bool,
    occ_memo: OccMemoAcquisitionState,
    currentness: OptionsReferenceCurrentnessDisposition,
    activation: OptionsReferenceActivationState,
    reasons: Vec<OptionsReferenceActivationReason>,
}

impl OptionsReferenceDoctorReport {
    /// Evaluates a previously activated and recovered local snapshot without provider network or
    /// local mutation.
    ///
    /// The explicit activation workflow supplies the durability receipt and recovered generation.
    /// This query-only doctor creates no files, takes no exclusive lock, performs no repair or
    /// quarantine, and reconstructs provider evidence only from that verified snapshot. It never
    /// compares the four Cboe clocks for equality or promotes memo discovery/documents to lifecycle
    /// economics.
    /// Because neither selected provider publishes an exact refresh interval for these surfaces,
    /// provider-local success always retains an explicit root-owned freshness classification.
    ///
    /// # Errors
    ///
    /// Returns only when a fixed small diagnostic allocation cannot be reserved.
    pub fn evaluate(
        input: OptionsReferenceDoctorInput<'_>,
    ) -> Result<Self, OptionsReferenceDoctorError> {
        let mut reasons = Vec::new();
        reasons
            .try_reserve_exact(MAX_ACTIVATION_REASONS)
            .map_err(|_| OptionsReferenceDoctorError::CapacityUnavailable)?;
        let mut core_failure = false;
        let mut proof_required = false;
        let mut degraded = false;

        let store_write_and_sync_verified = input.storage_activation.is_some_and(|receipt| {
            receipt.layout_version() == 3 && receipt.verified_namespaces() == 6
        });
        if !store_write_and_sync_verified {
            push_reason(
                &mut reasons,
                OptionsReferenceActivationReason::StoreProbeRequired,
            );
            proof_required = true;
        }

        let outcome = input.recovery;
        let rejected = u32::try_from(outcome.rejected().len())
            .map_err(|_| OptionsReferenceDoctorError::CapacityUnavailable)?;
        let quarantined = outcome.quarantined_staging();
        if rejected > 0 || quarantined > 0 {
            push_reason(
                &mut reasons,
                OptionsReferenceActivationReason::RecoveryDegraded {
                    rejected_newer_candidates: rejected,
                    quarantined_staging: quarantined,
                },
            );
            degraded = true;
        }
        let (generation, generation_health) = match outcome.generation() {
            Some(generation) => {
                let receipt = generation.receipt();
                (
                    Some(generation),
                    OptionsReferenceGenerationHealth::Recovered {
                        generation_id: receipt.generation_id().clone(),
                        database_digest: receipt.database_digest(),
                        database_bytes: receipt.database_bytes(),
                        rejected_newer_candidates: rejected,
                        quarantined_staging: quarantined,
                    },
                )
            }
            None => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::NoCompleteGeneration,
                );
                if rejected == 0 {
                    proof_required = true;
                } else {
                    core_failure = true;
                }
                (
                    None,
                    OptionsReferenceGenerationHealth::NoCompleteGeneration {
                        rejected_candidates: rejected,
                        quarantined_staging: quarantined,
                    },
                )
            }
        };

        let catalog = audit_catalog(generation, &mut reasons);
        core_failure |= catalog.failed;

        let raw = audit_raw_objects(generation, catalog.dlp, &mut reasons)?;
        if generation.is_some() {
            core_failure |= raw.failed;
        } else {
            proof_required = true;
        }

        let typed_queries_verified =
            audit_queries(generation, &raw.objects, input.query_probe, &mut reasons);
        if generation.is_some() && !typed_queries_verified {
            core_failure = true;
        }

        match &input.occ_memo.0 {
            OccMemoAcquisitionKind::NotSelected => {}
            OccMemoAcquisitionKind::ProbeRequired => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::OccMemoProbeRequired,
                );
                degraded = true;
            }
            OccMemoAcquisitionKind::Unavailable => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::OccMemoUnavailable,
                );
                degraded = true;
            }
            OccMemoAcquisitionKind::RssDiscoveryAvailable { .. } => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::OccMemoRssDiscoveryOnly,
                );
                degraded = true;
            }
            OccMemoAcquisitionKind::ProviderAntiBotChallenge { .. } => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::OccMemoProviderAntiBotChallenge,
                );
                degraded = true;
            }
            OccMemoAcquisitionKind::CompleteOperativeDocumentsUninterpreted { .. } => {
                push_reason(
                    &mut reasons,
                    OptionsReferenceActivationReason::OccMemoDocumentsUninterpreted,
                );
                degraded = true;
            }
        }

        let core_reference_verified = !core_failure
            && !proof_required
            && catalog.verified
            && raw.verified
            && typed_queries_verified;
        let configured_only = generation.is_none() && input.query_probe.is_none();
        let activation = if core_failure {
            OptionsReferenceActivationState::Unavailable
        } else if configured_only {
            OptionsReferenceActivationState::Configured
        } else if proof_required {
            OptionsReferenceActivationState::ProbeRequired
        } else if degraded {
            OptionsReferenceActivationState::Degraded
        } else {
            OptionsReferenceActivationState::Available
        };

        let occ_dlp = match (raw.dlp, generation) {
            (Some((representation, object)), Some(generation))
                if raw.verified
                    && catalog.dlp == Some(representation)
                    && generation.receipt().catalog().counts().occ_dlp_products() > 0 =>
            {
                OccDlpDoctorState::Complete {
                    representation,
                    product_records: generation.receipt().catalog().counts().occ_dlp_products(),
                    object,
                }
            }
            _ => OccDlpDoctorState::NotVerified,
        };

        Ok(Self {
            store_write_and_sync_verified,
            generation: generation_health,
            core_reference_verified,
            cboe_objects: raw.cboe,
            occ_dlp,
            typed_queries_verified,
            occ_memo: input.occ_memo.clone(),
            currentness:
                OptionsReferenceCurrentnessDisposition::RequiresApplicationFreshnessClassification,
            activation,
            reasons,
        })
    }

    /// Returns whether every capability namespace passed write/fsync/read/cleanup proof.
    pub const fn store_write_and_sync_verified(&self) -> bool {
        self.store_write_and_sync_verified
    }

    /// Returns last-complete restart health.
    pub const fn generation(&self) -> &OptionsReferenceGenerationHealth {
        &self.generation
    }

    /// Returns whether every mandatory core surface, raw object, and typed read passed.
    pub const fn core_reference_verified(&self) -> bool {
        self.core_reference_verified
    }

    /// Returns four independently clocked Cboe objects in deterministic venue order.
    pub fn cboe_objects(&self) -> &[CboeReferenceObjectDoctorEvidence] {
        &self.cboe_objects
    }

    /// Returns OCC DLP completeness.
    pub const fn occ_dlp(&self) -> &OccDlpDoctorState {
        &self.occ_dlp
    }

    /// Returns whether both exact query witnesses reconciled.
    pub const fn typed_queries_verified(&self) -> bool {
        self.typed_queries_verified
    }

    /// Returns optional, non-core OCC memo acquisition state.
    pub const fn occ_memo(&self) -> &OccMemoAcquisitionState {
        &self.occ_memo
    }

    /// Returns the explicit root-owned application freshness-classification requirement.
    pub const fn currentness(&self) -> OptionsReferenceCurrentnessDisposition {
        self.currentness
    }

    /// Returns the fail-closed activation disposition.
    pub const fn activation(&self) -> OptionsReferenceActivationState {
        self.activation
    }

    /// Returns bounded typed reasons for the disposition.
    pub fn reasons(&self) -> &[OptionsReferenceActivationReason] {
        &self.reasons
    }
}

struct CatalogAudit {
    dlp: Option<OccDlpRepresentation>,
    verified: bool,
    failed: bool,
}

fn audit_catalog(
    generation: Option<&ReferenceGeneration>,
    reasons: &mut Vec<OptionsReferenceActivationReason>,
) -> CatalogAudit {
    let Some(generation) = generation else {
        return CatalogAudit {
            dlp: None,
            verified: false,
            failed: false,
        };
    };
    let receipt = generation.receipt();
    let catalog = receipt.catalog();
    let mut failed = !catalog.publication_eligible()
        || !matches!(catalog.completeness(), PublicationCompleteness::Complete)
        || catalog.request().surfaces().len() != CORE_OBJECT_COUNT
        || catalog.counts().cboe_series() == 0
        || catalog.counts().occ_dlp_products() == 0;
    if failed {
        push_reason(reasons, OptionsReferenceActivationReason::CatalogIneligible);
    }

    let mut cboe = [false; 4];
    let mut dlp = None;
    for surface in catalog.request().surfaces() {
        match surface {
            ReferenceSurface::CboeAllSeries { venue } => {
                let index = cboe_venue_index(*venue);
                if cboe[index] {
                    failed = true;
                }
                cboe[index] = true;
            }
            ReferenceSurface::OccDlpSelectedText
            | ReferenceSurface::OccDlpDailyText
            | ReferenceSurface::OccDlpDailyXml => {
                let Some(observed) = OccDlpRepresentation::from_surface(surface) else {
                    failed = true;
                    continue;
                };
                if dlp.replace(observed).is_some() {
                    failed = true;
                    push_reason(
                        reasons,
                        OptionsReferenceActivationReason::InvalidOccDlpClosure,
                    );
                }
            }
            ReferenceSurface::OccMemoIndexCsv
            | ReferenceSurface::OccMemoIndexJson
            | ReferenceSurface::OccMemoDocument { .. }
            | ReferenceSurface::OccMemoAttachment { .. } => {
                failed = true;
                push_reason(
                    reasons,
                    OptionsReferenceActivationReason::UnexpectedGenerationSurface,
                );
            }
        }
    }
    for venue in REQUIRED_CBOE_VENUES {
        if !cboe[cboe_venue_index(venue)] {
            failed = true;
            push_reason(
                reasons,
                OptionsReferenceActivationReason::MissingCboeSurface { venue },
            );
        }
    }
    if dlp.is_none() {
        failed = true;
        push_reason(
            reasons,
            OptionsReferenceActivationReason::InvalidOccDlpClosure,
        );
    }
    CatalogAudit {
        dlp,
        verified: !failed,
        failed,
    }
}

struct RawAudit {
    cboe: Vec<CboeReferenceObjectDoctorEvidence>,
    dlp: Option<(OccDlpRepresentation, OptionsReferenceObjectClockEvidence)>,
    objects: Vec<ReferenceGenerationObjectEvidence>,
    verified: bool,
    failed: bool,
}

fn audit_raw_objects(
    generation: Option<&ReferenceGeneration>,
    catalog_dlp: Option<OccDlpRepresentation>,
    reasons: &mut Vec<OptionsReferenceActivationReason>,
) -> Result<RawAudit, OptionsReferenceDoctorError> {
    let mut cboe = Vec::new();
    cboe.try_reserve_exact(REQUIRED_CBOE_VENUES.len())
        .map_err(|_| OptionsReferenceDoctorError::CapacityUnavailable)?;
    let Some(generation) = generation else {
        push_reason(
            reasons,
            OptionsReferenceActivationReason::CoreRawEvidenceRequired,
        );
        return Ok(RawAudit {
            cboe,
            dlp: None,
            objects: Vec::new(),
            verified: false,
            failed: false,
        });
    };
    let mut failed = generation.receipt().raw_object_ids().len() != CORE_OBJECT_COUNT;
    let objects = match generation.object_evidence() {
        Ok(objects) => objects,
        Err(_) => {
            failed = true;
            Vec::new()
        }
    };
    if objects.len() != CORE_OBJECT_COUNT {
        failed = true;
        push_reason(
            reasons,
            OptionsReferenceActivationReason::GenerationRawClosureMismatch {
                observed: u32::try_from(objects.len()).unwrap_or(u32::MAX),
            },
        );
    }
    let mut cboe_counts = [0_u8; 4];
    let mut dlp = None;
    for raw in objects.iter().take(CORE_OBJECT_COUNT) {
        let surface = raw.surface().clone();
        let bound_to_generation = generation
            .receipt()
            .raw_object_ids()
            .binary_search(raw.object_id())
            .is_ok();
        if raw.payload_digest().algorithm() != DigestAlgorithm::Sha256
            || raw.payload_digest().bytes().iter().all(|byte| *byte == 0)
            || raw.payload_bytes() == 0
            || !bound_to_generation
        {
            failed = true;
            push_reason(
                reasons,
                OptionsReferenceActivationReason::RawObjectEvidenceMismatch {
                    surface: surface.clone(),
                },
            );
        }
        match surface {
            ReferenceSurface::CboeAllSeries { venue } => {
                let index = cboe_venue_index(venue);
                cboe_counts[index] = cboe_counts[index].saturating_add(1);
                if raw.source_filename().is_none()
                    || raw.source_publication_date().is_none()
                    || raw.http_last_modified().is_none()
                {
                    failed = true;
                    push_reason(
                        reasons,
                        OptionsReferenceActivationReason::CboeClockEvidenceIncomplete { venue },
                    );
                }
                cboe.push(CboeReferenceObjectDoctorEvidence {
                    venue,
                    object: OptionsReferenceObjectClockEvidence::from_generation_object(raw),
                });
            }
            ReferenceSurface::OccDlpSelectedText
            | ReferenceSurface::OccDlpDailyText
            | ReferenceSurface::OccDlpDailyXml => {
                let Some(representation) = OccDlpRepresentation::from_surface(&surface) else {
                    failed = true;
                    continue;
                };
                let dated = !matches!(representation, OccDlpRepresentation::SelectedText);
                if raw.source_filename().is_none()
                    || raw.http_last_modified().is_none()
                    || (dated != raw.source_publication_date().is_some())
                    || dlp.is_some()
                {
                    failed = true;
                    push_reason(
                        reasons,
                        OptionsReferenceActivationReason::OccDlpEvidenceIncomplete,
                    );
                }
                if dlp.is_none() {
                    dlp = Some((
                        representation,
                        OptionsReferenceObjectClockEvidence::from_generation_object(raw),
                    ));
                }
            }
            ReferenceSurface::OccMemoIndexCsv
            | ReferenceSurface::OccMemoIndexJson
            | ReferenceSurface::OccMemoDocument { .. }
            | ReferenceSurface::OccMemoAttachment { .. } => {
                failed = true;
                push_reason(
                    reasons,
                    OptionsReferenceActivationReason::RawObjectEvidenceMismatch { surface },
                );
            }
        }
    }
    for venue in REQUIRED_CBOE_VENUES {
        if cboe_counts[cboe_venue_index(venue)] != 1 {
            failed = true;
            push_reason(
                reasons,
                OptionsReferenceActivationReason::CboeClockEvidenceIncomplete { venue },
            );
        }
    }
    cboe.sort_by_key(CboeReferenceObjectDoctorEvidence::venue);
    if dlp.as_ref().map(|value| value.0) != catalog_dlp {
        failed = true;
        push_reason(
            reasons,
            OptionsReferenceActivationReason::OccDlpEvidenceIncomplete,
        );
    }
    Ok(RawAudit {
        cboe,
        dlp,
        objects,
        verified: !failed,
        failed,
    })
}

fn audit_queries(
    generation: Option<&ReferenceGeneration>,
    core_raw_objects: &[ReferenceGenerationObjectEvidence],
    probe: Option<OptionsReferenceQueryProbe<'_>>,
    reasons: &mut Vec<OptionsReferenceActivationReason>,
) -> bool {
    let Some(generation) = generation else {
        push_reason(
            reasons,
            OptionsReferenceActivationReason::QueryProbeRequired,
        );
        return false;
    };
    let Some(probe) = probe else {
        return audit_durable_queries(generation, core_raw_objects, reasons);
    };

    let cboe_bound = core_raw_objects.iter().any(|raw| {
        raw.object_id() == probe.cboe.object_context().object_id()
            && raw.surface() == probe.cboe.object_context().surface()
    });
    let occ_bound = core_raw_objects.iter().any(|raw| {
        raw.object_id() == probe.occ_dlp.object_context().object_id()
            && raw.surface() == probe.occ_dlp.object_context().surface()
    });
    let cboe_ok = if !cboe_bound {
        query_reason(
            reasons,
            OptionsReferenceQueryFamily::CboeSymbol,
            OptionsReferenceQueryFailure::EvidenceMismatch,
        );
        false
    } else {
        match generation.cboe_by_symbol(probe.cboe.cboe_symbol_id()) {
            Ok(result)
                if query_evidence_reconciles(result.evidence(), generation)
                    && result.value().is_some_and(|view| {
                        cboe_query_reconciles(view, probe.cboe, generation)
                    }) =>
            {
                true
            }
            Ok(result) if !query_evidence_reconciles(result.evidence(), generation) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::CboeSymbol,
                    OptionsReferenceQueryFailure::EvidenceMismatch,
                );
                false
            }
            Ok(result) if result.value().is_none() => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::CboeSymbol,
                    OptionsReferenceQueryFailure::Missing,
                );
                false
            }
            Ok(_) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::CboeSymbol,
                    OptionsReferenceQueryFailure::EvidenceMismatch,
                );
                false
            }
            Err(error) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::CboeSymbol,
                    OptionsReferenceQueryFailure::Rejected {
                        failure: error.into(),
                    },
                );
                false
            }
        }
    };
    let occ_ok = if !occ_bound {
        query_reason(
            reasons,
            OptionsReferenceQueryFamily::OccDlpProduct,
            OptionsReferenceQueryFailure::EvidenceMismatch,
        );
        false
    } else {
        match generation.occ_product(probe.occ_dlp.options_symbol(), probe.occ_dlp.product_type()) {
            Ok(result)
                if query_evidence_reconciles(result.evidence(), generation)
                    && result.value().is_some_and(|view| {
                        occ_query_reconciles(view, probe.occ_dlp, generation)
                    }) =>
            {
                true
            }
            Ok(result) if !query_evidence_reconciles(result.evidence(), generation) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::OccDlpProduct,
                    OptionsReferenceQueryFailure::EvidenceMismatch,
                );
                false
            }
            Ok(result) if result.value().is_none() => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::OccDlpProduct,
                    OptionsReferenceQueryFailure::Missing,
                );
                false
            }
            Ok(_) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::OccDlpProduct,
                    OptionsReferenceQueryFailure::EvidenceMismatch,
                );
                false
            }
            Err(error) => {
                query_reason(
                    reasons,
                    OptionsReferenceQueryFamily::OccDlpProduct,
                    OptionsReferenceQueryFailure::Rejected {
                        failure: error.into(),
                    },
                );
                false
            }
        }
    };
    cboe_ok && occ_ok
}

fn audit_durable_queries(
    generation: &ReferenceGeneration,
    objects: &[ReferenceGenerationObjectEvidence],
    reasons: &mut Vec<OptionsReferenceActivationReason>,
) -> bool {
    let (cboe_symbol, occ_symbol, product_type) = match generation.durable_query_keys() {
        Ok(keys) => keys,
        Err(error) => {
            let failure = OptionsReferenceLocalFailure::from(error);
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::CboeSymbol,
                OptionsReferenceQueryFailure::Rejected { failure },
            );
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::OccDlpProduct,
                OptionsReferenceQueryFailure::Rejected { failure },
            );
            return false;
        }
    };
    let cboe_ok = match generation.cboe_by_symbol(&cboe_symbol) {
        Ok(result)
            if query_evidence_reconciles(result.evidence(), generation)
                && result.value().is_some_and(|view| {
                    view.canonical_identity() == CanonicalReferenceIdentityState::Unresolved
                        && !view.venues().is_empty()
                        && view.venues().iter().all(|presence| {
                            objects
                                .iter()
                                .any(|object| object.object_id() == presence.object_id())
                        })
                }) =>
        {
            true
        }
        Ok(result) if result.value().is_none() => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::CboeSymbol,
                OptionsReferenceQueryFailure::Missing,
            );
            false
        }
        Ok(_) => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::CboeSymbol,
                OptionsReferenceQueryFailure::EvidenceMismatch,
            );
            false
        }
        Err(error) => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::CboeSymbol,
                OptionsReferenceQueryFailure::Rejected {
                    failure: error.into(),
                },
            );
            false
        }
    };
    let occ_ok = match generation.occ_product(&occ_symbol, product_type) {
        Ok(result)
            if query_evidence_reconciles(result.evidence(), generation)
                && result.value().is_some_and(|view| {
                    view.canonical_identity() == CanonicalReferenceIdentityState::Unresolved
                        && objects
                            .iter()
                            .any(|object| object.object_id() == view.object_id())
                }) =>
        {
            true
        }
        Ok(result) if result.value().is_none() => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::OccDlpProduct,
                OptionsReferenceQueryFailure::Missing,
            );
            false
        }
        Ok(_) => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::OccDlpProduct,
                OptionsReferenceQueryFailure::EvidenceMismatch,
            );
            false
        }
        Err(error) => {
            query_reason(
                reasons,
                OptionsReferenceQueryFamily::OccDlpProduct,
                OptionsReferenceQueryFailure::Rejected {
                    failure: error.into(),
                },
            );
            false
        }
    };
    cboe_ok && occ_ok
}

fn query_evidence_reconciles(
    evidence: &crate::store::ReferenceQueryEvidence,
    generation: &ReferenceGeneration,
) -> bool {
    let receipt = generation.receipt();
    evidence.generation_id() == receipt.generation_id()
        && evidence.database_digest() == receipt.database_digest()
}

fn cboe_query_reconciles(
    view: &crate::CboeContractReferenceView,
    expected: &CboeSeriesReference,
    generation: &ReferenceGeneration,
) -> bool {
    view.cboe_symbol_id() == expected.cboe_symbol_id()
        && view.contract() == expected.contract()
        && view.underlying() == expected.underlying()
        && view.canonical_identity() == CanonicalReferenceIdentityState::Unresolved
        && view.venues().iter().all(|presence| {
            generation
                .receipt()
                .raw_object_ids()
                .binary_search(presence.object_id())
                .is_ok()
        })
        && view.venues().iter().any(|presence| {
            presence.venue() == expected.venue()
                && presence.matching_unit() == expected.unit().get()
                && presence.status() == expected.status()
                && presence.object_id() == expected.object_context().object_id()
                && presence.provider_row_number() == expected.provider_row_number()
                && presence.evidence() == expected.record_id()
        })
}

fn occ_query_reconciles(
    view: &crate::OccProductReferenceView,
    expected: &OccDlpProductReference,
    generation: &ReferenceGeneration,
) -> bool {
    view.options_symbol() == expected.options_symbol()
        && view.product_type() == expected.product_type()
        && view.underlying_symbol() == expected.underlying_symbol()
        && view.symbol_name() == expected.symbol_name()
        && view.trading_exchanges() == expected.trading_exchanges()
        && view.exchange_listing_evidence() == expected.exchange_listing_evidence()
        && view.position_limit() == expected.position_limit()
        && view.canonical_identity() == CanonicalReferenceIdentityState::Unresolved
        && view.object_id() == expected.object_context().object_id()
        && view.provider_row_number() == expected.provider_row_number()
        && view.evidence() == expected.record_id()
        && generation
            .receipt()
            .raw_object_ids()
            .binary_search(view.object_id())
            .is_ok()
}

fn query_reason(
    reasons: &mut Vec<OptionsReferenceActivationReason>,
    family: OptionsReferenceQueryFamily,
    failure: OptionsReferenceQueryFailure,
) {
    push_reason(
        reasons,
        OptionsReferenceActivationReason::TypedQueryFailed { family, failure },
    );
}

fn cboe_venue_index(venue: CboeVenue) -> usize {
    match venue {
        CboeVenue::C1 => 0,
        CboeVenue::Bzx => 1,
        CboeVenue::C2 => 2,
        CboeVenue::Edgx => 3,
    }
}

fn push_reason(
    reasons: &mut Vec<OptionsReferenceActivationReason>,
    reason: OptionsReferenceActivationReason,
) {
    if reasons.len() < MAX_ACTIVATION_REASONS && !reasons.contains(&reason) {
        reasons.push(reason);
    }
}

/// Doctor input or bounded diagnostic construction failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum OptionsReferenceDoctorError {
    /// A fixed, small diagnostic allocation could not be reserved.
    #[error("option-reference doctor capacity is unavailable")]
    CapacityUnavailable,
    /// Memo evidence was empty, used the wrong digest, or lacked exact challenge classification.
    #[error("invalid OCC memo doctor evidence")]
    InvalidMemoEvidence,
}
