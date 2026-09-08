const EXTRACTION_RECORD_TEMPORAL_SCHEMA_VERSION: u16 = 2;

/// Explicit source availability basis without promoting inference to point-in-time evidence.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum AvailabilityEvidence {
    /// Source/audit evidence directly establishes availability.
    Observed {
        available_at: Timestamp,
        evidence: SourceIdentifier,
    },
    /// This process first observed the source object at the retained time.
    LocalFirstObserved { observed_at: Timestamp },
    /// A non-authoritative time is retained with its exact versioned inference method.
    Inferred {
        inferred_at: Timestamp,
        method: SourceIdentifier,
    },
    /// Historical availability could not be established.
    #[default]
    Unknown,
}

fn availability_is_unknown(availability: &AvailabilityEvidence) -> bool {
    matches!(availability, AvailabilityEvidence::Unknown)
}

impl AvailabilityEvidence {
    /// Returns only conservative point-in-time availability.
    pub const fn conservative_available_at(&self) -> Option<Timestamp> {
        match self {
            Self::Observed { available_at, .. } => Some(*available_at),
            Self::LocalFirstObserved { observed_at } => Some(*observed_at),
            Self::Inferred { .. } | Self::Unknown => None,
        }
    }

    /// Returns a source-reported or inferred time for analysis, never for default admission.
    pub const fn reported_at(&self) -> Option<Timestamp> {
        match self {
            Self::Observed { available_at, .. } => Some(*available_at),
            Self::LocalFirstObserved { observed_at } => Some(*observed_at),
            Self::Inferred { inferred_at, .. } => Some(*inferred_at),
            Self::Unknown => None,
        }
    }
}

/// One bounded normalized record carrying full discovery and extraction lineage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractionRecord {
    temporal_schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    extraction_request_id: ExtractionRequestId,
    object_id: SourceIdentifier,
    object_evidence: ExactPayloadEvidence,
    schema: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    effective_time: ResearchTemporalCoordinate,
    published_time: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    revision: SourceIdentifier,
    superseded_time: Option<ResearchTemporalCoordinate>,
    payload: BoundedBytes<MAX_EXTRACTION_RECORD_BYTES>,
}

impl ExtractionRecord {
    /// Constructs one request-bound, point-in-time normalized record.
    ///
    /// # Errors
    ///
    /// Rejects oversized payloads and invalid publication/availability/supersession ordering.
    #[allow(
        clippy::too_many_arguments,
        reason = "point-in-time evidence remains explicit"
    )]
    pub fn try_new(
        request: &ExtractionRequest,
        schema: SourceIdentifier,
        evidence: ExactPayloadEvidence,
        effective_at: Timestamp,
        published_at: Option<Timestamp>,
        availability: AvailabilityEvidence,
        revision: SourceIdentifier,
        superseded_at: Option<Timestamp>,
        payload: Bytes,
    ) -> Result<Self, ExtractionError> {
        Self::try_new_with_time(
            request,
            schema,
            evidence,
            ResearchTemporalCoordinate::exact(effective_at),
            published_at.map(ResearchTemporalCoordinate::exact),
            availability,
            revision,
            superseded_at.map(ResearchTemporalCoordinate::exact),
            payload,
        )
    }

    /// Constructs one request-bound record without reducing temporal precision.
    ///
    /// # Errors
    ///
    /// Rejects oversized payloads and invalid comparable temporal ordering.
    #[allow(
        clippy::too_many_arguments,
        reason = "point-in-time evidence remains explicit"
    )]
    pub fn try_new_with_time(
        request: &ExtractionRequest,
        schema: SourceIdentifier,
        evidence: ExactPayloadEvidence,
        effective_time: ResearchTemporalCoordinate,
        published_time: Option<ResearchTemporalCoordinate>,
        availability: AvailabilityEvidence,
        revision: SourceIdentifier,
        superseded_time: Option<ResearchTemporalCoordinate>,
        payload: Bytes,
    ) -> Result<Self, ExtractionError> {
        Self::try_from_parts(
            request.object.source_id.clone(),
            request.object.metadata_revision.clone(),
            request.object.dataset.clone(),
            request.object.discovery_request_id,
            request.request_id,
            request.object.object_id.clone(),
            request.object.evidence.clone(),
            schema,
            evidence,
            effective_time,
            published_time,
            availability,
            revision,
            superseded_time,
            payload,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "wire validation preserves exact lineage"
    )]
    fn try_from_parts(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        discovery_request_id: DiscoveryRequestId,
        extraction_request_id: ExtractionRequestId,
        object_id: SourceIdentifier,
        object_evidence: ExactPayloadEvidence,
        schema: SourceIdentifier,
        evidence: ExactPayloadEvidence,
        effective_time: ResearchTemporalCoordinate,
        published_time: Option<ResearchTemporalCoordinate>,
        availability: AvailabilityEvidence,
        revision: SourceIdentifier,
        superseded_time: Option<ResearchTemporalCoordinate>,
        payload: Bytes,
    ) -> Result<Self, ExtractionError> {
        let reported_at = availability.reported_at();
        if published_time
            .as_ref()
            .and_then(ResearchTemporalCoordinate::exact_timestamp)
            .is_some_and(|published| reported_at.is_some_and(|value| value < published))
            || superseded_time.as_ref().is_some_and(|superseded| {
                published_time
                    .as_ref()
                    .is_some_and(|published| temporal_not_after(superseded, published))
            })
        {
            return Err(ExtractionError::InvalidPointInTimeOrdering);
        }
        let evidence = normalize_evidence(&evidence)?;
        let payload = BoundedBytes::try_from_bytes(payload)
            .map_err(|error| ExtractionError::RecordTooLarge { max: error.max })?;
        if !payload_matches_exact_evidence(payload.as_bytes(), &evidence) {
            return Err(ExtractionError::PayloadEvidenceMismatch);
        }
        Ok(Self {
            temporal_schema_version: EXTRACTION_RECORD_TEMPORAL_SCHEMA_VERSION,
            source_id: normalize_source_id(&source_id)?,
            metadata_revision: normalize_metadata_revision(&metadata_revision)?,
            dataset: normalize_identifier(&dataset)?,
            discovery_request_id,
            extraction_request_id,
            object_id: normalize_identifier(&object_id)?,
            object_evidence: normalize_evidence(&object_evidence)?,
            schema: normalize_identifier(&schema)?,
            evidence,
            effective_time,
            published_time,
            availability,
            revision: normalize_identifier(&revision)?,
            superseded_time,
            payload,
        })
    }

    /// Returns the exact source namespace carried by the extraction request.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision carried by the extraction request.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the request-bound dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact discovery request identity.
    pub const fn discovery_request_id(&self) -> DiscoveryRequestId {
        self.discovery_request_id
    }

    /// Returns the exact extraction request identity.
    pub const fn extraction_request_id(&self) -> ExtractionRequestId {
        self.extraction_request_id
    }

    /// Returns the exact discovered source-object identity.
    pub const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    /// Returns the source object's exact payload evidence.
    pub const fn object_evidence(&self) -> &ExactPayloadEvidence {
        &self.object_evidence
    }

    /// Returns the normalized record schema identity.
    pub const fn schema(&self) -> &SourceIdentifier {
        &self.schema
    }

    /// Returns evidence verified against the exact normalized payload bytes.
    pub const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }

    /// Returns the record's effective coordinate without precision loss.
    pub const fn effective_time(&self) -> &ResearchTemporalCoordinate {
        &self.effective_time
    }

    /// Returns the source publication coordinate, when supplied.
    pub fn published_time(&self) -> Option<&ResearchTemporalCoordinate> {
        self.published_time.as_ref()
    }

    /// Returns exact normalized payload bytes.
    pub fn payload(&self) -> &Bytes {
        self.payload.as_bytes()
    }

    /// Returns only conservative evidenced availability.
    pub const fn available_at(&self) -> Option<Timestamp> {
        self.availability.conservative_available_at()
    }

    /// Returns availability evidence basis.
    pub const fn availability(&self) -> &AvailabilityEvidence {
        &self.availability
    }

    /// Returns source revision/vintage identity.
    pub const fn revision(&self) -> &SourceIdentifier {
        &self.revision
    }

    /// Returns the exclusive supersession coordinate when known.
    pub fn superseded_time(&self) -> Option<&ResearchTemporalCoordinate> {
        self.superseded_time.as_ref()
    }

    pub(super) fn try_rebind_request(
        self,
        request: &ExtractionRequest,
    ) -> Result<Self, ExtractionError> {
        Self::try_new_with_time(
            request,
            self.schema,
            self.evidence,
            self.effective_time,
            self.published_time,
            self.availability,
            self.revision,
            self.superseded_time,
            self.payload.as_bytes().clone(),
        )
    }

    pub(crate) fn matches_request(&self, request: &ExtractionRequest) -> bool {
        self.source_id == request.object.source_id
            && self.metadata_revision == request.object.metadata_revision
            && self.dataset == request.object.dataset
            && self.discovery_request_id == request.object.discovery_request_id
            && self.extraction_request_id == request.request_id
            && self.object_id == request.object.object_id
            && self.object_evidence == request.object.evidence
    }

    pub(crate) fn retained_bytes(&self) -> Result<u64, ExtractionError> {
        let dynamic = checked_usize_sum([
            self.source_id.as_str().len(),
            self.metadata_revision.as_source_identifier().as_str().len(),
            self.dataset.as_str().len(),
            self.object_id.as_str().len(),
            evidence_dynamic_bytes(&self.object_evidence)?,
            self.schema.as_str().len(),
            evidence_dynamic_bytes(&self.evidence)?,
            self.revision.as_str().len(),
            self.payload.retained_bytes(),
        ])?;
        u64::try_from(
            size_of::<Self>()
                .checked_add(dynamic)
                .ok_or(ExtractionError::ByteCountOverflow)?,
        )
        .map_err(|_| ExtractionError::ByteCountOverflow)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentExtractionRecordWire {
    temporal_schema_version: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    extraction_request_id: ExtractionRequestId,
    object_id: SourceIdentifier,
    object_evidence: ExactPayloadEvidence,
    schema: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    effective_time: ResearchTemporalCoordinate,
    published_time: Option<ResearchTemporalCoordinate>,
    availability: AvailabilityEvidence,
    revision: SourceIdentifier,
    superseded_time: Option<ResearchTemporalCoordinate>,
    payload: BoundedBytes<MAX_EXTRACTION_RECORD_BYTES>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyExtractionRecordWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    discovery_request_id: DiscoveryRequestId,
    extraction_request_id: ExtractionRequestId,
    object_id: SourceIdentifier,
    object_evidence: ExactPayloadEvidence,
    schema: SourceIdentifier,
    evidence: ExactPayloadEvidence,
    effective_at: Timestamp,
    published_at: Option<Timestamp>,
    availability: AvailabilityEvidence,
    revision: SourceIdentifier,
    superseded_at: Option<Timestamp>,
    payload: BoundedBytes<MAX_EXTRACTION_RECORD_BYTES>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExtractionRecordWire {
    Current(CurrentExtractionRecordWire),
    Legacy(LegacyExtractionRecordWire),
}

impl<'de> Deserialize<'de> for ExtractionRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match ExtractionRecordWire::deserialize(deserializer)? {
            ExtractionRecordWire::Current(wire) => {
                if wire.temporal_schema_version != EXTRACTION_RECORD_TEMPORAL_SCHEMA_VERSION {
                    return Err(serde::de::Error::custom(
                        ExtractionError::UnsupportedTemporalSchema {
                            found: wire.temporal_schema_version,
                        },
                    ));
                }
                Self::try_from_parts(
                    wire.source_id,
                    wire.metadata_revision,
                    wire.dataset,
                    wire.discovery_request_id,
                    wire.extraction_request_id,
                    wire.object_id,
                    wire.object_evidence,
                    wire.schema,
                    wire.evidence,
                    wire.effective_time,
                    wire.published_time,
                    wire.availability,
                    wire.revision,
                    wire.superseded_time,
                    wire.payload.as_bytes().clone(),
                )
                .map_err(serde::de::Error::custom)
            }
            ExtractionRecordWire::Legacy(wire) => Self::try_from_parts(
                wire.source_id,
                wire.metadata_revision,
                wire.dataset,
                wire.discovery_request_id,
                wire.extraction_request_id,
                wire.object_id,
                wire.object_evidence,
                wire.schema,
                wire.evidence,
                ResearchTemporalCoordinate::exact(wire.effective_at),
                wire.published_at.map(ResearchTemporalCoordinate::exact),
                wire.availability,
                wire.revision,
                wire.superseded_at.map(ResearchTemporalCoordinate::exact),
                wire.payload.as_bytes().clone(),
            )
            .map_err(serde::de::Error::custom),
        }
    }
}

/// Discovery or normalized extraction invariant failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExtractionError {
    #[error("{field} exceeds maximum {max}")]
    LimitTooLarge { field: &'static str, max: u64 },
    #[error("discovery result exceeds requested maximum {requested}")]
    DiscoveryLimitExceeded { requested: u16 },
    #[error("extraction record exceeds maximum bytes {max}")]
    RecordTooLarge { max: usize },
    #[error("extraction result exceeds requested record maximum {requested}")]
    RecordLimitExceeded { requested: u32 },
    #[error("extraction result exceeds requested byte maximum {requested}")]
    ByteLimitExceeded { requested: u64 },
    #[error("extraction byte count overflow")]
    ByteCountOverflow,
    #[error("extraction batch allocation failed")]
    AllocationFailed,
    #[error("record availability/publication/supersession ordering is invalid")]
    InvalidPointInTimeOrdering,
    #[error("extraction temporal schema version {found} is unsupported")]
    UnsupportedTemporalSchema { found: u16 },
    #[error("discovery or extraction request identity does not match")]
    RequestBindingMismatch,
    #[error("source identity, metadata revision, or dataset does not match")]
    SourceBindingMismatch,
    #[error("extraction record object evidence does not match its exact request")]
    ObjectBindingMismatch,
    #[error("extraction record payload does not match its exact content evidence")]
    PayloadEvidenceMismatch,
}

/// Verifies algorithm-qualified evidence against exact payload bytes.
pub fn payload_matches_exact_evidence(payload: &[u8], evidence: &ExactPayloadEvidence) -> bool {
    let expected = evidence.content_digest();
    let actual = match expected.algorithm() {
        DigestAlgorithm::Sha256 => Sha256::digest(payload).into(),
        DigestAlgorithm::Blake3 => *blake3::hash(payload).as_bytes(),
    };
    actual == expected.bytes()
}

fn discovery_request_id(
    dataset: &SourceIdentifier,
    effective_at: Option<Timestamp>,
    max_results: NonZeroU16,
    deadline: Timestamp,
) -> DiscoveryRequestId {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/discovery-request/v2");
    hash_field(&mut hash, b"dataset", dataset.as_str().as_bytes());
    hash_optional_timestamp(&mut hash, b"effective_at", effective_at);
    hash_field(&mut hash, b"max_results", &max_results.get().to_be_bytes());
    hash_field(&mut hash, b"deadline", &deadline.unix_nanos().to_be_bytes());
    DiscoveryRequestId(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn extraction_request_id(
    object: &SourceObject,
    max_records: NonZeroU32,
    max_bytes: NonZeroU64,
    deadline: Timestamp,
) -> ExtractionRequestId {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/extraction-request/v4");
    hash_field(
        &mut hash,
        b"source_id",
        object.source_id.as_str().as_bytes(),
    );
    hash_field(
        &mut hash,
        b"metadata_revision",
        object
            .metadata_revision
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    );
    hash_field(&mut hash, b"dataset", object.dataset.as_str().as_bytes());
    hash_field(
        &mut hash,
        b"discovery_request_id",
        object.discovery_request_id.0.bytes().as_ref(),
    );
    hash_field(
        &mut hash,
        b"object_id",
        object.object_id.as_str().as_bytes(),
    );
    hash_field(
        &mut hash,
        b"media_type",
        object.media_type.as_str().as_bytes(),
    );
    hash_evidence(&mut hash, b"object_evidence", &object.evidence);
    object.capture_identity.hash_into(&mut hash);
    hash_field(
        &mut hash,
        b"effective_starts_at",
        &object.effective.starts_at().unix_nanos().to_be_bytes(),
    );
    hash_optional_timestamp(&mut hash, b"effective_ends_at", object.effective.ends_at());
    hash_optional_timestamp(&mut hash, b"published_at", object.published_at);
    hash_field(
        &mut hash,
        b"object_availability_presence",
        &[u8::from(!matches!(
            object.availability,
            AvailabilityEvidence::Unknown
        ))],
    );
    hash_availability(&mut hash, &object.availability);
    hash_optional_u64(&mut hash, b"expected_bytes", object.expected_bytes);
    hash_field(&mut hash, b"max_records", &max_records.get().to_be_bytes());
    hash_field(&mut hash, b"max_bytes", &max_bytes.get().to_be_bytes());
    hash_field(&mut hash, b"deadline", &deadline.unix_nanos().to_be_bytes());
    ExtractionRequestId(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_evidence(hash: &mut Sha256, tag: &[u8], evidence: &ExactPayloadEvidence) {
    let digest = evidence.content_digest();
    let algorithm = match digest.algorithm() {
        DigestAlgorithm::Sha256 => [1_u8],
        DigestAlgorithm::Blake3 => [2_u8],
    };
    hash_field(hash, tag, b"exact-payload-evidence/v1");
    hash_field(hash, b"digest_algorithm", &algorithm);
    hash_field(hash, b"digest_bytes", &digest.bytes());
    if let Some(locator) = evidence.version_pinned_locator() {
        hash_field(hash, b"locator_presence", &[1]);
        hash_field(
            hash,
            b"locator_reference",
            locator.reference().as_str().as_bytes(),
        );
        hash_field(
            hash,
            b"locator_version",
            locator.version().as_str().as_bytes(),
        );
    } else {
        hash_field(hash, b"locator_presence", &[0]);
    }
}

fn hash_availability(hash: &mut Sha256, availability: &AvailabilityEvidence) {
    match availability {
        AvailabilityEvidence::Observed {
            available_at,
            evidence,
        } => {
            hash_field(hash, b"object_availability_kind", b"observed");
            hash_field(
                hash,
                b"object_available_at",
                &available_at.unix_nanos().to_be_bytes(),
            );
            hash_field(
                hash,
                b"object_availability_evidence",
                evidence.as_str().as_bytes(),
            );
        }
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            hash_field(hash, b"object_availability_kind", b"local_first_observed");
            hash_field(
                hash,
                b"object_observed_at",
                &observed_at.unix_nanos().to_be_bytes(),
            );
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => {
            hash_field(hash, b"object_availability_kind", b"inferred");
            hash_field(
                hash,
                b"object_inferred_at",
                &inferred_at.unix_nanos().to_be_bytes(),
            );
            hash_field(
                hash,
                b"object_availability_method",
                method.as_str().as_bytes(),
            );
        }
        AvailabilityEvidence::Unknown => {}
    }
}

fn hash_optional_timestamp(hash: &mut Sha256, tag: &[u8], value: Option<Timestamp>) {
    let mut encoded = [0_u8; 9];
    if let Some(value) = value {
        encoded[0] = 1;
        encoded[1..].copy_from_slice(&value.unix_nanos().to_be_bytes());
    }
    hash_field(hash, tag, &encoded);
}

fn temporal_not_after(
    left: &ResearchTemporalCoordinate,
    right: &ResearchTemporalCoordinate,
) -> bool {
    !matches!(left.partial_cmp(right), Some(std::cmp::Ordering::Greater))
}

fn hash_optional_u64(hash: &mut Sha256, tag: &[u8], value: Option<u64>) {
    let mut encoded = [0_u8; 9];
    if let Some(value) = value {
        encoded[0] = 1;
        encoded[1..].copy_from_slice(&value.to_be_bytes());
    }
    hash_field(hash, tag, &encoded);
}

fn hash_field(hash: &mut Sha256, tag: &[u8], value: &[u8]) {
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn normalize_identifier(value: &SourceIdentifier) -> Result<SourceIdentifier, ExtractionError> {
    SourceIdentifier::try_from(value.as_str()).map_err(|_| ExtractionError::SourceBindingMismatch)
}

fn normalize_source_id(value: &SourceId) -> Result<SourceId, ExtractionError> {
    SourceId::try_from(value.as_str()).map_err(|_| ExtractionError::SourceBindingMismatch)
}

fn normalize_metadata_revision(
    value: &MetadataRevision,
) -> Result<MetadataRevision, ExtractionError> {
    normalize_identifier(value.as_source_identifier()).map(MetadataRevision::new)
}

fn normalize_evidence(
    evidence: &ExactPayloadEvidence,
) -> Result<ExactPayloadEvidence, ExtractionError> {
    match evidence.version_pinned_locator() {
        Some(locator) => Ok(ExactPayloadEvidence::with_version_pinned_locator(
            evidence.content_digest(),
            VersionPinnedSourceLocator::new(
                normalize_identifier(locator.reference())?,
                normalize_identifier(locator.version())?,
            ),
        )),
        None => Ok(ExactPayloadEvidence::from_content_digest(
            evidence.content_digest(),
        )),
    }
}

fn normalize_availability(
    availability: &AvailabilityEvidence,
) -> Result<AvailabilityEvidence, ExtractionError> {
    match availability {
        AvailabilityEvidence::Observed {
            available_at,
            evidence,
        } => Ok(AvailabilityEvidence::Observed {
            available_at: *available_at,
            evidence: normalize_identifier(evidence)?,
        }),
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            Ok(AvailabilityEvidence::LocalFirstObserved {
                observed_at: *observed_at,
            })
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => Ok(AvailabilityEvidence::Inferred {
            inferred_at: *inferred_at,
            method: normalize_identifier(method)?,
        }),
        AvailabilityEvidence::Unknown => Ok(AvailabilityEvidence::Unknown),
    }
}

fn validate_source_object_availability(
    published_at: Option<Timestamp>,
    availability: &AvailabilityEvidence,
) -> Result<(), ExtractionError> {
    if published_at.is_some_and(|published| {
        availability
            .reported_at()
            .is_some_and(|available| available < published)
    }) {
        Err(ExtractionError::InvalidPointInTimeOrdering)
    } else {
        Ok(())
    }
}

fn availability_dynamic_bytes(
    availability: &AvailabilityEvidence,
) -> Result<usize, ExtractionError> {
    match availability {
        AvailabilityEvidence::Observed { evidence, .. } => Ok(evidence.as_str().len()),
        AvailabilityEvidence::Inferred { method, .. } => Ok(method.as_str().len()),
        AvailabilityEvidence::LocalFirstObserved { .. } | AvailabilityEvidence::Unknown => Ok(0),
    }
}

fn evidence_dynamic_bytes(evidence: &ExactPayloadEvidence) -> Result<usize, ExtractionError> {
    evidence.version_pinned_locator().map_or(Ok(0), |locator| {
        checked_usize_sum([
            locator.reference().as_str().len(),
            locator.version().as_str().len(),
        ])
    })
}

fn usize_to_u64(value: usize) -> Result<u64, ExtractionError> {
    u64::try_from(value).map_err(|_| ExtractionError::ByteCountOverflow)
}

fn checked_usize_sum(values: impl IntoIterator<Item = usize>) -> Result<usize, ExtractionError> {
    values.into_iter().try_fold(0_usize, |total, value| {
        total
            .checked_add(value)
            .ok_or(ExtractionError::ByteCountOverflow)
    })
}
