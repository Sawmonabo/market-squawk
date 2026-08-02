//! Production dataset-builder and Python-admission release fixture.

use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::str::FromStr as _;
use std::time::{Duration, Instant};

use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    MacroObservation, MetadataRevision, PayloadReference, ResearchContext, ResearchObservation,
    ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime,
    RevisionBoundPayloadEvidence, RevisionNumber, SchemaVersion, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp, UniverseMembershipObservation,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, AvailabilityEvidence as SourceAvailabilityEvidence,
    CanonicalObservationPayload, CoverageDomain, DiscoveryRequest, ExtractionBatch,
    ExtractionRecord, ExtractionRequest, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceObject, SourceProtocolProfile,
};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    AnalyticalDataService, AnalyticalManifestCatalog, CatalogAuthority, CatalogConfig,
    CatalogLimit, CatalogResultLimits, ChronologicalSplitPolicy, ComponentAdjustmentEvidence,
    ComponentKind, ComponentScope, ComponentSelector, ComponentValue, CorporateActionAdjustment,
    CorporateActionLimits, CorporateActionPolicy, CorporateActionSensitivity, DatasetBuildInputs,
    DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildRequest, DatasetBuilder, DatasetExample,
    DatasetId, DatasetOutputAuthorization, FeatureLabelComponentInput, FeatureLabelComponentSpec,
    IngestIdentity, MissingValuePolicy, ObjectStoreConfig, ObservationFamilyKey, PointInTimeLimits,
    PointInTimePolicy, PointInTimeRevisionMode, PythonDatasetVerificationLimits,
    ResearchIngestService, ResearchUse, ResearchUseGrantInput, ResearchUseLimits, ResearchUseSet,
    RightsBasis, RightsDecisionInput, SourceOperation, UniverseId, UniverseLimits,
    UniverseMembership, extraction_provider_payload_digest, verify_python_dataset,
};

type FixtureResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const MAX_SELECTED_ROWS: usize = 48_000;
const AS_OF: Timestamp = Timestamp::from_unix_nanos(100);

pub(super) struct PythonAdmissionMeasurement {
    pub(super) requested_rows: u64,
    pub(super) measured_rows: u64,
    pub(super) selected_rows_per_verification: u64,
    pub(super) export_sha256: [u8; 32],
    pub(super) catalog_identity: [u8; 32],
    pub(super) selection_sha256: [u8; 32],
    pub(super) samples: Vec<u64>,
    pub(super) elapsed_nanos: u64,
}

pub(super) async fn measure(
    root: &Path,
    requested_rows: u64,
) -> FixtureResult<PythonAdmissionMeasurement> {
    let selected_target = usize::try_from(requested_rows.min(MAX_SELECTED_ROWS as u64))?;
    let example_count = selected_target.div_ceil(2).max(1);
    let paths = LocalPaths::prepare(root)?;
    let location = paths.catalog()?.clone();
    let authority = CatalogAuthority::open(CatalogConfig::try_new(
        location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(64)?,
        CatalogResultLimits::try_new(1024 * 1024, 16 * 1024 * 1024)?,
    )?)?;
    let source = local_source("release-python-source")?;
    authority.register_source(&source, Timestamp::from_unix_nanos(10))?;
    authority.register_source(
        &local_source("market-squawk.derived")?,
        Timestamp::from_unix_nanos(10),
    )?;
    let membership = universe_membership_observation()?;
    let membership_identity =
        CanonicalObservationPayload::try_from_observation(&membership)?.identity();
    let batch = extraction_batch(membership)?;
    let payload_digest = extraction_provider_payload_digest(&batch);
    let rights = authority.admit_source_rights(RightsDecisionInput {
        source_id: source.source_id().clone(),
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms(
            "https://market-squawk.local/release-evidence/source-rights/v1",
            digest(31),
        )?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![SourceOperation::Persist],
    })?;
    authority.admit_research_use_grant(ResearchUseGrantInput::try_new(
        rights.rights_id(),
        ResearchUseSet::try_new(vec![ResearchUse::LocalAnalysis])?,
        digest(33),
        Some(Timestamp::from_unix_nanos(i64::MAX)),
    )?)?;
    let reservation = authority.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            payload_digest,
            SourceOperation::Persist,
            "release-python-source:gdp-and-universe:v1",
        )?,
        &rights,
    )?;
    let service = AnalyticalDataService::initialize(
        authority,
        AnalyticalManifestCatalog::open(&location, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(
            512 * 1024 * 1024,
            MAX_SELECTED_ROWS,
            Duration::from_secs(240),
        )?,
    )?;
    let analytical_dataset = DatasetId::try_from(batch.request().object().dataset().as_str())?;
    let parent = service
        .ingest(
            reservation,
            analytical_dataset,
            batch,
            CancellationToken::new(),
        )
        .await?;
    let request = build_request(
        parent.manifest().clone(),
        membership_identity,
        example_count,
    )?;
    let builder = service.dataset_builder();
    let built = builder.build(request, CancellationToken::new()).await?;
    let admission = builder.python_admission(&built)?;
    let export_sha256 = admission.export().content_hash();
    let selected_rows = example_count
        .checked_mul(2)
        .ok_or("selected Python row count overflowed")?;
    let limits = PythonDatasetVerificationLimits::try_new(selected_rows, 256 * 1024 * 1024)?;
    let cancellation = CancellationToken::new();
    let baseline = verify_python_dataset(
        paths.root(),
        export_sha256,
        AS_OF,
        limits,
        Instant::now() + Duration::from_secs(240),
        &cancellation,
    )?;
    if baseline.selected_rows() != selected_rows
        || baseline.identity().manifest() != built.manifest()
        || baseline.identity().build_spec_digest() != built.build_spec_digest()
        || baseline.identity().policy_digest() != built.policy_digest()
        || baseline.identity().universe_digest() != built.universe_digest()
        || baseline.catalog_identity() != admission.catalog_identity()
        || baseline.export_sha256() != export_sha256
    {
        return Err("production Python dataset admission did not reconcile".into());
    }
    let selected_rows_u64 = u64::try_from(selected_rows)?;
    let repetitions = requested_rows
        .checked_add(selected_rows_u64.saturating_sub(1))
        .and_then(|value| value.checked_div(selected_rows_u64))
        .ok_or("Python verification repetition count overflowed")?;
    let measured_rows = repetitions
        .checked_mul(selected_rows_u64)
        .ok_or("Python verified row count overflowed")?;
    let mut samples = Vec::new();
    samples.try_reserve_exact(usize::try_from(repetitions)?)?;
    let started_all = Instant::now();
    for _ in 0..repetitions {
        let started = Instant::now();
        let selection = verify_python_dataset(
            paths.root(),
            export_sha256,
            AS_OF,
            limits,
            Instant::now() + Duration::from_secs(240),
            &cancellation,
        )?;
        if selection.identity() != baseline.identity()
            || selection.catalog_identity() != baseline.catalog_identity()
            || selection.export_sha256() != baseline.export_sha256()
            || selection.selection_sha256() != baseline.selection_sha256()
            || selection.selected_rows() != baseline.selected_rows()
            || selection.as_of() != baseline.as_of()
        {
            return Err("revalidated Python dataset identity changed".into());
        }
        samples.push(nanos(started.elapsed()));
    }
    Ok(PythonAdmissionMeasurement {
        requested_rows,
        measured_rows,
        selected_rows_per_verification: selected_rows_u64,
        export_sha256: export_sha256.bytes(),
        catalog_identity: admission.catalog_identity().bytes(),
        selection_sha256: baseline.selection_sha256().bytes(),
        samples,
        elapsed_nanos: nanos(started_all.elapsed()),
    })
}

fn build_request(
    parent: crate::DatasetManifestRef,
    membership_identity: EvidenceDigest,
    example_count: usize,
) -> FixtureResult<DatasetBuildRequest> {
    let instrument = instrument()?;
    let feature = FeatureLabelComponentSpec::try_new(
        ComponentKind::Feature,
        ComponentScope::Global,
        CorporateActionSensitivity::NotApplicable,
        "release-feature",
        NonZeroU32::MIN,
    )?;
    let label = FeatureLabelComponentSpec::try_new(
        ComponentKind::Label,
        ComponentScope::Global,
        CorporateActionSensitivity::NotApplicable,
        "release-label",
        NonZeroU32::MIN,
    )?;
    let feature_input = FeatureLabelComponentInput::try_new(
        feature.clone(),
        ComponentValue::missing(SourceIdentifier::try_from("not-observed")?),
        vec![ComponentSelector::new(ObservationFamilyKey::Macro {
            source_id: SourceId::try_from("release-python-source")?,
            series: SourceIdentifier::try_from("CPI")?,
            effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        })],
        ComponentAdjustmentEvidence::NotApplicable,
    )?;
    let label_input = FeatureLabelComponentInput::try_new(
        label.clone(),
        ComponentValue::decimal(
            Decimal::new(123_456, 2),
            Some(SourceIdentifier::try_from("USD")?),
            None,
        )?,
        vec![ComponentSelector::new(ObservationFamilyKey::Macro {
            source_id: SourceId::try_from("release-python-source")?,
            series: SourceIdentifier::try_from("GDP")?,
            effective: ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(90)),
        })],
        ComponentAdjustmentEvidence::NotApplicable,
    )?;
    let mut examples = Vec::new();
    examples.try_reserve_exact(example_count)?;
    for index in 0..example_count {
        examples.push(DatasetExample::try_new(
            format!("release-example-{index:05}"),
            instrument,
            Timestamp::from_unix_nanos(80),
            Timestamp::from_unix_nanos(100),
            vec![feature_input.clone(), label_input.clone()],
        )?);
    }
    let inputs = DatasetBuildInputs::try_new(
        vec![parent.clone()],
        UniverseId::try_from("release-universe")?,
        vec![UniverseMembership::new(
            instrument,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            market_squawk_domain::AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(1),
                SourceIdentifier::try_from("release-universe-publication")?,
            ),
            parent,
            membership_identity,
        )],
        vec![feature, label],
        examples,
    )?;
    let policy = DatasetBuildPolicy::new(
        ChronologicalSplitPolicy::try_new(
            Timestamp::from_unix_nanos(100),
            Timestamp::from_unix_nanos(200),
            Timestamp::from_unix_nanos(300),
        )?,
        PointInTimePolicy::try_new(NonZeroU32::MIN, PointInTimeRevisionMode::LatestKnown)?,
        CorporateActionPolicy::new(CorporateActionAdjustment::Raw, NonZeroU32::MIN),
        MissingValuePolicy::Preserve,
        SourceIdentifier::try_from("release-dataset-builder-v1")?,
    );
    let output_rows = example_count
        .checked_mul(2)
        .ok_or("release dataset output row count overflowed")?;
    DatasetBuildRequest::try_new(
        DatasetId::try_from("release.feature-labels")?,
        inputs,
        policy,
        ResearchUse::LocalAnalysis,
        ResearchUseLimits::try_new(
            8,
            32,
            32,
            8,
            64 * 1024 * 1024,
            Duration::from_secs(30),
            Duration::from_secs(300),
        )?,
        DatasetOutputAuthorization::try_new(
            SourceId::try_from("market-squawk.derived")?,
            RightsBasis::reviewed_terms(
                "https://market-squawk.local/release-evidence/derived-rights/v1",
                digest(62),
            )?,
            digest(63),
            None,
        )?,
        DatasetBuildLimits::try_new(
            16,
            example_count,
            2,
            output_rows,
            512 * 1024 * 1024,
            Duration::from_secs(240),
            PointInTimeLimits::try_new(16, 16, 4, 16, 4 * 1024 * 1024)?,
            UniverseLimits::try_new(4, 1024 * 1024)?,
            CorporateActionLimits::try_new(
                NonZeroUsize::new(4).ok_or("invalid corporate-action count")?,
                NonZeroUsize::new(1024 * 1024).ok_or("invalid corporate-action byte limit")?,
            )?,
        )?,
    )
    .map_err(Into::into)
}

fn extraction_batch(membership: ResearchObservation) -> FixtureResult<ExtractionBatch> {
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("release-gdp")?,
        Some(Timestamp::from_unix_nanos(90)),
        NonZeroU16::MIN,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("release-python-source")?,
        MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
        &discovery,
        SourceIdentifier::try_from("release-gdp-and-universe")?,
        SourceIdentifier::try_from("application-json")?,
        ExactPayloadEvidence::from_content_digest(digest(4)),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(100)),
        Some(4096),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::new(2).ok_or("invalid extraction record limit")?,
        NonZeroU64::new(1024 * 1024).ok_or("invalid extraction byte limit")?,
        Timestamp::from_unix_nanos(1_000),
    )?;
    let macro_payload = serde_json::to_vec(&macro_observation()?)?;
    let membership_payload = serde_json::to_vec(&membership)?;
    let records = vec![
        extraction_record(
            &request,
            macro_payload,
            Timestamp::from_unix_nanos(90),
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("release-macro-publication")?,
        )?,
        extraction_record(
            &request,
            membership_payload,
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(1),
            SourceIdentifier::try_from("release-universe-publication")?,
        )?,
    ];
    Ok(ExtractionBatch::try_new(&request, records)?)
}

fn extraction_record(
    request: &ExtractionRequest,
    payload: Vec<u8>,
    effective_at: Timestamp,
    published_at: Timestamp,
    availability_evidence: SourceIdentifier,
) -> FixtureResult<ExtractionRecord> {
    let payload_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&payload).into());
    Ok(ExtractionRecord::try_new(
        request,
        SourceIdentifier::try_from("market-squawk-research-v3")?,
        ExactPayloadEvidence::from_content_digest(payload_digest),
        effective_at,
        Some(published_at),
        SourceAvailabilityEvidence::Observed {
            available_at: published_at,
            evidence: availability_evidence,
        },
        SourceIdentifier::try_from("revision-1")?,
        None,
        payload.into(),
    )?)
}

fn macro_observation() -> FixtureResult<ResearchObservation> {
    let context = research_context(
        None,
        "GDP:release:v1",
        "release:gdp",
        Timestamp::from_unix_nanos(90),
        Timestamp::from_unix_nanos(100),
        "release-macro-publication",
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from("GDP")?,
        Decimal::new(123_456, 2),
        SourceIdentifier::try_from("USD")?,
    )))
}

fn universe_membership_observation() -> FixtureResult<ResearchObservation> {
    let context = research_context(
        Some(instrument()?),
        "release-universe:member",
        "release:universe:member",
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(1),
        "release-universe-publication",
    )?;
    Ok(ResearchObservation::UniverseMembership(
        UniverseMembershipObservation::new(
            context,
            SourceIdentifier::try_from("release-universe")?,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        )?,
    ))
}

fn research_context(
    instrument_id: Option<InstrumentId>,
    source_identifier: &str,
    payload_reference: &str,
    effective_at: Timestamp,
    published_at: Timestamp,
    availability_evidence: &str,
) -> FixtureResult<ResearchContext> {
    Ok(ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("release-python-source")?,
            instrument_id,
            venue_id: None,
            source_identifier: SourceIdentifier::try_from(source_identifier)?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                payload_reference,
            )?),
            availability: market_squawk_domain::AvailabilityEvidence::evidenced(
                published_at,
                SourceIdentifier::try_from(availability_evidence)?,
            ),
        })?,
        ResearchTime::new(
            effective_at,
            Some(published_at),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?)
}

fn local_source(source_id: &str) -> FixtureResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source_id)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("revision-1")?),
            ExactPayloadEvidence::from_content_digest(digest(1)),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("local")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            ExactPayloadEvidence::from_content_digest(digest(2)),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            ExactPayloadEvidence::from_content_digest(digest(3)),
            effective,
            CoverageDomain::Macroeconomic,
            CoverageDelay::Delayed(1),
            DeliveryEvidence::Unknown,
        )?,
        DataQuality::OfficialDelayed,
        NetworkAccessPolicy::Denied,
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        None,
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::RevisionPreserving,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn instrument() -> FixtureResult<InstrumentId> {
    Ok(InstrumentId::from_str(
        "018f0000-0000-7000-8000-000000000091",
    )?)
}

const fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
