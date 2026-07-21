//! Discovery, request, object, and normalized-record lineage contracts.

use std::mem::size_of;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use bytes::Bytes;
use market_squawk_domain::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp, VersionPinnedSourceLocator,
};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::bounded::{BoundedBytes, BoundedVec};

/// Global ceiling for one discovery request and response.
pub const MAX_DISCOVERY_OBJECTS: usize = 4_096;
/// Global record count ceiling for one extraction batch.
pub const MAX_EXTRACTION_RECORDS: usize = 100_000;
/// Maximum normalized payload bytes in one extraction record.
pub const MAX_EXTRACTION_RECORD_BYTES: usize = 1024 * 1024;
/// Maximum requested bytes across a paged extraction operation.
pub const MAX_EXTRACTION_BATCH_BYTES: u64 = 1024 * 1024 * 1024;
/// Conservative deep-retained ceiling for one in-memory extraction batch.
pub const MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES: u64 = 64 * 1024 * 1024;

/// Deterministic identity of all discovery request semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct DiscoveryRequestId(EvidenceDigest);

/// Bounded request to discover source objects for one dataset namespace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryRequest {
    dataset: SourceIdentifier,
    effective_at: Option<Timestamp>,
    max_results: NonZeroU16,
    deadline: Timestamp,
    request_id: DiscoveryRequestId,
}

impl DiscoveryRequest {
    /// Constructs a bounded request and computes its deterministic identity.
    ///
    /// # Errors
    ///
    /// Rejects a result cap above [`MAX_DISCOVERY_OBJECTS`].
    pub fn try_new(
        dataset: SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        deadline: Timestamp,
    ) -> Result<Self, ExtractionError> {
        if usize::from(max_results.get()) > MAX_DISCOVERY_OBJECTS {
            return Err(ExtractionError::LimitTooLarge {
                field: "max_results",
                max: MAX_DISCOVERY_OBJECTS as u64,
            });
        }
        let dataset = normalize_identifier(&dataset)?;
        let request_id = discovery_request_id(&dataset, effective_at, max_results, deadline);
        Ok(Self {
            dataset,
            effective_at,
            max_results,
            deadline,
            request_id,
        })
    }

    /// Returns deterministic request identity.
    pub const fn request_id(&self) -> DiscoveryRequestId {
        self.request_id
    }

    /// Returns maximum requested results.
    pub const fn max_results(&self) -> u16 {
        self.max_results.get()
    }

    /// Returns dataset namespace.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the optional point-in-time discovery instant.
    pub const fn effective_at(&self) -> Option<Timestamp> {
        self.effective_at
    }

    /// Returns operation deadline.
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryRequestWire {
    dataset: SourceIdentifier,
    effective_at: Option<Timestamp>,
    max_results: NonZeroU16,
    deadline: Timestamp,
    request_id: DiscoveryRequestId,
}

impl<'de> Deserialize<'de> for DiscoveryRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DiscoveryRequestWire::deserialize(deserializer)?;
        let rebuilt = Self::try_new(
            wire.dataset,
            wire.effective_at,
            wire.max_results,
            wire.deadline,
        )
        .map_err(serde::de::Error::custom)?;
        if rebuilt.request_id != wire.request_id {
            return Err(serde::de::Error::custom(
                ExtractionError::RequestBindingMismatch,
            ));
        }
        Ok(rebuilt)
    }
}

/// One discovered, source/revision/dataset/request-bound exact source object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObject {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    object_id: SourceIdentifier,
    media_type: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    published_at: Option<Timestamp>,
    expected_bytes: Option<u64>,
}

impl SourceObject {
    /// Constructs an exact object bound to its discovery request and source revision.
    ///
    /// # Errors
    ///
    /// Rejects a declared object above the operation byte ceiling.
    #[allow(clippy::too_many_arguments, reason = "lineage fields remain explicit")]
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        request: &DiscoveryRequest,
        object_id: SourceIdentifier,
        media_type: SourceIdentifier,
        evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
        published_at: Option<Timestamp>,
        expected_bytes: Option<u64>,
    ) -> Result<Self, ExtractionError> {
        if expected_bytes.is_some_and(|size| size > MAX_EXTRACTION_BATCH_BYTES) {
            return Err(ExtractionError::LimitTooLarge {
                field: "expected_bytes",
                max: MAX_EXTRACTION_BATCH_BYTES,
            });
        }
        let source_id = normalize_source_id(&source_id)?;
        let metadata_revision = normalize_metadata_revision(&metadata_revision)?;
        let object_id = normalize_identifier(&object_id)?;
        let media_type = normalize_identifier(&media_type)?;
        let evidence = normalize_evidence(&evidence)?;
        Ok(Self {
            source_id,
            metadata_revision,
            dataset: request.dataset.clone(),
            discovery_request_id: request.request_id,
            object_id,
            media_type,
            evidence,
            effective,
            published_at,
            expected_bytes,
        })
    }

    /// Returns exact source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns exact dataset namespace.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns discovery request identity.
    pub const fn discovery_request_id(&self) -> DiscoveryRequestId {
        self.discovery_request_id
    }

    /// Returns source object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns declared media type.
    pub const fn media_type(&self) -> &SourceIdentifier {
        &self.media_type
    }

    /// Returns exact source-object evidence.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns source-object effective interval.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective
    }

    /// Returns known publication time.
    pub const fn published_at(&self) -> Option<Timestamp> {
        self.published_at
    }

    /// Returns declared source-object bytes.
    pub const fn expected_bytes(&self) -> Option<u64> {
        self.expected_bytes
    }

    fn matches_discovery(&self, request: &DiscoveryRequest) -> bool {
        self.dataset == request.dataset && self.discovery_request_id == request.request_id
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Result<u64, ExtractionError> {
        usize_to_u64(checked_usize_sum([
            self.source_id.as_str().len(),
            self.metadata_revision.as_source_identifier().as_str().len(),
            self.dataset.as_str().len(),
            self.object_id.as_str().len(),
            self.media_type.as_str().len(),
            evidence_dynamic_bytes(&self.evidence)?,
        ])?)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceObjectWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    object_id: SourceIdentifier,
    media_type: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
    published_at: Option<Timestamp>,
    expected_bytes: Option<u64>,
}

impl<'de> Deserialize<'de> for SourceObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SourceObjectWire::deserialize(deserializer)?;
        if wire
            .expected_bytes
            .is_some_and(|size| size > MAX_EXTRACTION_BATCH_BYTES)
        {
            return Err(serde::de::Error::custom(ExtractionError::LimitTooLarge {
                field: "expected_bytes",
                max: MAX_EXTRACTION_BATCH_BYTES,
            }));
        }
        Ok(Self {
            source_id: normalize_source_id(&wire.source_id).map_err(serde::de::Error::custom)?,
            metadata_revision: normalize_metadata_revision(&wire.metadata_revision)
                .map_err(serde::de::Error::custom)?,
            dataset: normalize_identifier(&wire.dataset).map_err(serde::de::Error::custom)?,
            discovery_request_id: wire.discovery_request_id,
            object_id: normalize_identifier(&wire.object_id).map_err(serde::de::Error::custom)?,
            media_type: normalize_identifier(&wire.media_type).map_err(serde::de::Error::custom)?,
            evidence: normalize_evidence(&wire.evidence).map_err(serde::de::Error::custom)?,
            effective: wire.effective,
            published_at: wire.published_at,
            expected_bytes: wire.expected_bytes,
        })
    }
}

/// Intrinsically bounded discovery result preserving the exact request.
#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryBatch {
    request: DiscoveryRequest,
    objects: BoundedVec<SourceObject, MAX_DISCOVERY_OBJECTS>,
}

impl DiscoveryBatch {
    /// Constructs a response bound to the exact request.
    ///
    /// # Errors
    ///
    /// Rejects request-limit or lineage transplants.
    pub fn try_new(
        request: &DiscoveryRequest,
        objects: Vec<SourceObject>,
    ) -> Result<Self, ExtractionError> {
        if objects.len() > usize::from(request.max_results()) {
            return Err(ExtractionError::DiscoveryLimitExceeded {
                requested: request.max_results(),
            });
        }
        if objects
            .iter()
            .any(|object| !object.matches_discovery(request))
        {
            return Err(ExtractionError::RequestBindingMismatch);
        }
        if let Some(first) = objects.first()
            && objects.iter().skip(1).any(|object| {
                object.source_id != first.source_id
                    || object.metadata_revision != first.metadata_revision
            })
        {
            return Err(ExtractionError::SourceBindingMismatch);
        }
        Ok(Self {
            request: request.clone(),
            objects: BoundedVec::try_new(objects).map_err(|error| {
                ExtractionError::LimitTooLarge {
                    field: "discovery_objects",
                    max: error.max as u64,
                }
            })?,
        })
    }

    /// Returns exact request.
    pub const fn request(&self) -> &DiscoveryRequest {
        &self.request
    }

    /// Returns discovered objects.
    pub fn objects(&self) -> &[SourceObject] {
        self.objects.as_slice()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryBatchWire {
    request: DiscoveryRequest,
    objects: BoundedVec<SourceObject, MAX_DISCOVERY_OBJECTS>,
}

impl<'de> Deserialize<'de> for DiscoveryBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = DiscoveryBatchWire::deserialize(deserializer)?;
        Self::try_new(&wire.request, wire.objects.as_slice().to_vec())
            .map_err(serde::de::Error::custom)
    }
}

/// Deterministic identity of all extraction request semantics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ExtractionRequestId(EvidenceDigest);

/// Bounded extraction request for one exact discovered source object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractionRequest {
    object: SourceObject,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
    deadline: Timestamp,
    request_id: ExtractionRequestId,
}

impl ExtractionRequest {
    /// Constructs explicit record, deep-byte, deadline, and object evidence bounds.
    ///
    /// # Errors
    ///
    /// Rejects limits above global extraction ceilings.
    pub fn try_new(
        object: SourceObject,
        max_records: NonZeroU32,
        max_bytes: NonZeroU64,
        deadline: Timestamp,
    ) -> Result<Self, ExtractionError> {
        if usize::try_from(max_records.get()).map_or(true, |max| max > MAX_EXTRACTION_RECORDS) {
            return Err(ExtractionError::LimitTooLarge {
                field: "max_records",
                max: MAX_EXTRACTION_RECORDS as u64,
            });
        }
        if max_bytes.get() > MAX_EXTRACTION_BATCH_BYTES {
            return Err(ExtractionError::LimitTooLarge {
                field: "max_bytes",
                max: MAX_EXTRACTION_BATCH_BYTES,
            });
        }
        let request_id = extraction_request_id(&object, max_records, max_bytes, deadline);
        Ok(Self {
            object,
            max_records,
            max_bytes,
            deadline,
            request_id,
        })
    }

    /// Returns requested object.
    pub const fn object(&self) -> &SourceObject {
        &self.object
    }

    /// Returns requested record ceiling.
    pub const fn max_records(&self) -> u32 {
        self.max_records.get()
    }

    /// Returns requested aggregate deep-retained byte ceiling.
    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes.get()
    }

    /// Returns operation deadline.
    pub const fn deadline(&self) -> Timestamp {
        self.deadline
    }

    /// Returns deterministic request identity.
    pub const fn request_id(&self) -> ExtractionRequestId {
        self.request_id
    }

    pub(crate) fn dynamic_retained_bytes(&self) -> Result<u64, ExtractionError> {
        self.object.dynamic_retained_bytes()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExtractionRequestWire {
    object: SourceObject,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
    deadline: Timestamp,
    request_id: ExtractionRequestId,
}

impl<'de> Deserialize<'de> for ExtractionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExtractionRequestWire::deserialize(deserializer)?;
        let rebuilt = Self::try_new(wire.object, wire.max_records, wire.max_bytes, wire.deadline)
            .map_err(serde::de::Error::custom)?;
        if rebuilt.request_id != wire.request_id {
            return Err(serde::de::Error::custom(
                ExtractionError::RequestBindingMismatch,
            ));
        }
        Ok(rebuilt)
    }
}

include!("contracts/record.rs");
