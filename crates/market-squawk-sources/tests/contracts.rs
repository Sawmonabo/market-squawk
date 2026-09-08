use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, CalendarDate, CaptureIntegrityState, ChecksumCapability, CoverageDelay,
    DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, MetadataRevision, ProviderChannel, ProviderProduct, RawCaptureFrameView,
    ResearchPeriod, ResearchTemporalCoordinate, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, StreamIntegrityState, Timestamp, VersionPinnedSourceLocator,
    checked_arc_bytes_allocation_bytes,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationHealth, AuthorizationMode,
    AvailabilityEvidence, BudgetHealth, ConnectionLiveness, CoverageDomain, CoverageHealth,
    DecodedProviderBatch, DiscoveryRequest, ExtractionBatch, ExtractionError, ExtractionRecord,
    ExtractionRequest, ExtractionSource, FreshnessPolicy, HistoricalCapability, LiveMarketSource,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, MAX_RAW_FRAME_BYTES, MarketDecoder, MarketFreshness,
    NetworkAccessPolicy, SessionId, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata, SourceMetadataInput, SourceObject, SourceProtocolProfile, TransportFrameKind,
};

use crate::common::{TestResult, direct_metadata, exact_evidence, source_identifier};
use sha2::{Digest as _, Sha256};

static_assertions::assert_not_impl_any!(
    DecodedProviderBatch: serde::Serialize,
    serde::de::DeserializeOwned
);

#[test]
fn macro_and_local_sources_need_no_fake_asset_or_network_endpoint() -> TestResult {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let coverage = SourceCoverage::try_non_instrument(
        exact_evidence(7),
        effective,
        CoverageDomain::Macroeconomic,
        CoverageDelay::Delayed(1),
        DeliveryEvidence::Unknown,
    )?;
    let revision = MetadataRevision::new(source_identifier("local-revision-1")?);
    let metadata = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("local-macro")?,
        RevisionBoundPayloadEvidence::new(revision, exact_evidence(8)),
        SourceClass::LocalFile,
        source_identifier("local")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(source_identifier("user-owned-file")?),
            exact_evidence(9),
            effective,
        ),
        coverage,
        DataQuality::Aggregated,
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
    ))?;
    assert_eq!(metadata.coverage().domain(), CoverageDomain::Macroeconomic);
    assert!(metadata.coverage().asset_classes().is_empty());
    assert!(
        metadata
            .network_policy()
            .authorize("https://example.invalid")
            .is_err()
    );
    let wire = serde_json::to_string(&metadata)?;
    let restored: SourceMetadata = serde_json::from_str(&wire)?;
    assert!(matches!(
        restored.network_policy(),
        NetworkAccessPolicy::Denied
    ));
    assert_eq!(
        serde_json::from_str::<SourceClass>("\"standards_publisher\"")?,
        SourceClass::StandardsPublisher
    );
    let mut standards_without_network_authority = serde_json::to_value(&metadata)?;
    standards_without_network_authority["source_class"] = serde_json::json!("standards_publisher");
    assert!(serde_json::from_value::<SourceMetadata>(standards_without_network_authority).is_err());
    Ok(())
}

#[test]
fn metadata_deserialization_rejects_budget_scope_ambiguous_for_authorization() -> TestResult {
    let metadata = direct_metadata("source-a", "revision-a", 0, None)?;
    let wire = serde_json::to_string(&metadata)?;
    let user_authorized = wire.replacen(
        "\"mode\":\"public_interface\"",
        "\"mode\":\"user_authorized\"",
        1,
    );
    assert!(serde_json::from_str::<SourceMetadata>(&user_authorized).is_err());

    let account_qualified_public = wire.replacen(
        "\"authorization_account\":null",
        "\"authorization_account\":\"unexpected-account\"",
        1,
    );
    assert!(serde_json::from_str::<SourceMetadata>(&account_qualified_public).is_err());
    Ok(())
}

#[test]
fn raw_frames_share_session_identity_and_bound_exact_bytes() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        direct_metadata("source-a", "revision-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        market_squawk_domain::ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let mut factory = registry.take_raw_frame_factory(&session)?;
    let first = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"one"))?;
    let second = factory.try_frame(TransportFrameKind::Binary, Bytes::from_static(b"two"))?;
    assert!(first.binding().shares_allocation_with(second.binding()));
    assert!(
        first
            .capture_payload()
            .shares_allocation_with(first.capture_payload())
    );
    assert!(
        !first
            .capture_payload()
            .shares_allocation_with(second.capture_payload())
    );
    let first_footprint = first.checked_retained_footprint()?;
    let first_source_pointer = first.source_id().as_str().as_ptr();
    let first_payload_pointer = first.payload().as_ptr();
    for _iteration in 0..3 {
        assert_eq!(first.source_id().as_str().as_ptr(), first_source_pointer);
        assert_eq!(first.payload().as_ptr(), first_payload_pointer);
        assert_eq!(
            first.capture_payload().as_bytes().as_ptr(),
            first_payload_pointer
        );
        assert_eq!(first.checked_retained_footprint()?, first_footprint);
    }
    let first_clone = first.clone();
    assert!(
        first_clone
            .binding()
            .shares_allocation_with(first.binding())
    );
    assert!(
        first_clone
            .capture_payload()
            .shares_allocation_with(first.capture_payload())
    );
    assert_eq!(first_clone.checked_retained_footprint()?, first_footprint);
    assert_eq!(
        first_footprint.inline_slot_funded_bytes(),
        std::mem::size_of_val(&first)
    );
    assert!(first_footprint.resident_shared_bytes() > 0);
    assert_eq!(
        first_footprint.unique_frame_dynamic_bytes(),
        checked_arc_bytes_allocation_bytes(first.payload().len())?
    );
    let oversized_backing = Bytes::from(vec![7_u8; MAX_RAW_FRAME_BYTES + 1]);
    let tiny_slice = oversized_backing.slice(0..1);
    let original_pointer = tiny_slice.as_ptr();
    let normalized = factory.try_frame(TransportFrameKind::Binary, tiny_slice)?;
    assert_eq!(normalized.retained_payload_bytes(), 1);
    assert_ne!(normalized.payload().as_ptr(), original_pointer);
    assert!(
        factory
            .try_frame(
                TransportFrameKind::Binary,
                Bytes::from(vec![0; MAX_RAW_FRAME_BYTES + 1]),
            )
            .is_err()
    );
    let exact = factory.try_frame(
        TransportFrameKind::Binary,
        Bytes::from(vec![0; MAX_RAW_FRAME_BYTES]),
    )?;
    assert_eq!(
        exact.capture_payload().as_bytes().len(),
        MAX_RAW_FRAME_BYTES
    );
    Ok(())
}

#[test]
fn connection_liveness_and_market_freshness_use_separate_clocks() -> TestResult {
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(
        direct_metadata("source-a", "revision-a", 0, None)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(source_identifier("session-a")?),
        market_squawk_domain::ConnectionGeneration::new(1)?,
        Timestamp::from_unix_nanos(1),
    )?;
    let policy = FreshnessPolicy::try_new(10, 10, 10, 10, 0)?;
    let stale_connection = market_squawk_sources::SourceHealthSnapshot::try_new(
        &session,
        Timestamp::from_unix_nanos(20),
        ConnectionLiveness::Live {
            last_activity_at: Timestamp::from_unix_nanos(0),
        },
        Some(Timestamp::from_unix_nanos(20)),
        Some(Timestamp::from_unix_nanos(15)),
        Some(Timestamp::from_unix_nanos(15)),
        policy,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
        AuthorizationHealth::Valid {
            evidence: exact_evidence(11),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    assert!(matches!(
        stale_connection.connection(),
        ConnectionLiveness::Stale { .. }
    ));
    assert!(matches!(
        stale_connection.market_freshness(),
        MarketFreshness::Fresh { .. }
    ));
    let stale_market = market_squawk_sources::SourceHealthSnapshot::try_new(
        &session,
        Timestamp::from_unix_nanos(20),
        ConnectionLiveness::Live {
            last_activity_at: Timestamp::from_unix_nanos(20),
        },
        Some(Timestamp::from_unix_nanos(20)),
        Some(Timestamp::from_unix_nanos(0)),
        Some(Timestamp::from_unix_nanos(20)),
        policy,
        StreamIntegrityState::Stale,
        CaptureIntegrityState::Incomplete,
        AuthorizationHealth::Valid {
            evidence: exact_evidence(11),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(12),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until: Timestamp::from_unix_nanos(100),
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?;
    assert!(matches!(
        stale_market.connection(),
        ConnectionLiveness::Live { .. }
    ));
    assert!(matches!(
        stale_market.market_freshness(),
        MarketFreshness::Stale { .. }
    ));
    Ok(())
}

#[test]
fn extraction_enforces_lineage_point_in_time_and_request_limits() -> TestResult {
    let discovery = DiscoveryRequest::try_new(
        source_identifier("macro-series")?,
        Some(Timestamp::from_unix_nanos(5)),
        NonZeroU16::try_from(5_u16)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("source-a")?,
        MetadataRevision::new(source_identifier("metadata-a")?),
        &discovery,
        source_identifier("object-a")?,
        source_identifier("application-json")?,
        exact_evidence(4),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(2)),
        Some(100),
    )?;
    let request = ExtractionRequest::try_new(
        object.clone(),
        NonZeroU32::try_from(2_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let other_object = SourceObject::try_new(
        SourceId::try_from("source-b")?,
        MetadataRevision::new(source_identifier("metadata-b")?),
        &discovery,
        source_identifier("other-object")?,
        source_identifier("application-json")?,
        exact_evidence(14),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(2)),
        Some(100),
    )?;
    let other_request = ExtractionRequest::try_new(
        other_object,
        NonZeroU32::try_from(2_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let sha_payload = Bytes::from_static(b"record");
    let record = ExtractionRecord::try_new(
        &other_request,
        source_identifier("schema-v1")?,
        payload_evidence(DigestAlgorithm::Sha256, &sha_payload),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(3),
            evidence: source_identifier("release-record")?,
        },
        source_identifier("revision-1")?,
        Some(Timestamp::from_unix_nanos(10)),
        sha_payload,
    )?;
    assert_eq!(record.source_id(), other_request.object().source_id());
    assert_eq!(record.schema().as_str(), "schema-v1");
    let blake_payload = Bytes::from_static(b"blake3-record");
    ExtractionRecord::try_new(
        &other_request,
        source_identifier("schema-v1")?,
        payload_evidence(DigestAlgorithm::Blake3, &blake_payload),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(3),
            evidence: source_identifier("release-record")?,
        },
        source_identifier("revision-1")?,
        Some(Timestamp::from_unix_nanos(10)),
        blake_payload,
    )?;
    assert!(matches!(
        ExtractionRecord::try_new(
            &other_request,
            source_identifier("schema-v1")?,
            exact_evidence(5),
            Timestamp::from_unix_nanos(1),
            Some(Timestamp::from_unix_nanos(2)),
            AvailabilityEvidence::Observed {
                available_at: Timestamp::from_unix_nanos(3),
                evidence: source_identifier("release-record")?,
            },
            source_identifier("revision-1")?,
            Some(Timestamp::from_unix_nanos(10)),
            Bytes::from_static(b"mismatch"),
        ),
        Err(ExtractionError::PayloadEvidenceMismatch)
    ));
    let mut hostile = serde_json::to_value(&record)?;
    hostile["payload"] = serde_json::json!([116, 97, 109, 112, 101, 114, 101, 100]);
    assert!(serde_json::from_value::<ExtractionRecord>(hostile).is_err());
    assert!(matches!(
        ExtractionBatch::try_new(&request, vec![record]),
        Err(ExtractionError::ObjectBindingMismatch)
    ));
    assert_eq!(discovery.max_results(), 5);
    assert_eq!(discovery.deadline(), Timestamp::from_unix_nanos(100));
    Ok(())
}

#[test]
fn request_identities_frame_fields_and_bind_the_full_source_object() -> TestResult {
    let first_discovery = DiscoveryRequest::try_new(
        source_identifier("ab")?,
        None,
        NonZeroU16::try_from(5_u16)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let second_discovery = DiscoveryRequest::try_new(
        source_identifier("a")?,
        None,
        NonZeroU16::try_from(5_u16)?,
        Timestamp::from_unix_nanos(100),
    )?;
    assert_ne!(first_discovery.request_id(), second_discovery.request_id());
    let build_object = |media_type: &str| {
        SourceObject::try_new(
            SourceId::try_from("source-a")?,
            MetadataRevision::new(source_identifier("metadata-a")?),
            &first_discovery,
            source_identifier("c")?,
            source_identifier(media_type)?,
            exact_evidence(4),
            EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            Some(Timestamp::from_unix_nanos(2)),
            Some(100),
        )
        .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
    };
    let base = ExtractionRequest::try_new(
        build_object("application-json")?,
        NonZeroU32::try_from(5_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let mutated = ExtractionRequest::try_new(
        build_object("application-ndjson")?,
        NonZeroU32::try_from(5_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    assert_ne!(base.request_id(), mutated.request_id());
    let observed_object = SourceObject::try_new_with_availability(
        SourceId::try_from("source-a")?,
        MetadataRevision::new(source_identifier("metadata-a")?),
        &first_discovery,
        source_identifier("c")?,
        source_identifier("application-json")?,
        exact_evidence(4),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: Timestamp::from_unix_nanos(3),
        },
        Some(100),
    )?;
    let observed = ExtractionRequest::try_new(
        observed_object,
        NonZeroU32::try_from(5_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    assert_ne!(base.request_id(), observed.request_id());
    assert_eq!(
        observed.object().availability().conservative_available_at(),
        Some(Timestamp::from_unix_nanos(3))
    );
    assert!(serde_json::to_value(observed.object())?["availability"].is_object());
    let mut legacy_object = serde_json::to_value(base.object())?;
    assert!(legacy_object.get("availability").is_none());
    legacy_object
        .as_object_mut()
        .ok_or("source object wire is not an object")?
        .remove("availability");
    let legacy_object: SourceObject = serde_json::from_value(legacy_object)?;
    let legacy = ExtractionRequest::try_new(
        legacy_object,
        NonZeroU32::try_from(5_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    assert_eq!(legacy.request_id(), base.request_id());
    Ok(())
}

#[test]
fn extraction_deep_cap_counts_maximum_version_pinned_locators() -> TestResult {
    let maximum = "x".repeat(market_squawk_domain::SourceIdentifier::MAX_LENGTH);
    let locator = || -> TestResult<VersionPinnedSourceLocator> {
        Ok(VersionPinnedSourceLocator::new(
            source_identifier(&maximum)?,
            source_identifier(&maximum)?,
        ))
    };
    let discovery = DiscoveryRequest::try_new(
        source_identifier(&maximum)?,
        None,
        NonZeroU16::try_from(100_u16)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("source-a")?,
        MetadataRevision::new(source_identifier(&maximum)?),
        &discovery,
        source_identifier(&maximum)?,
        source_identifier(&maximum)?,
        ExactPayloadEvidence::with_version_pinned_locator(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [4; 32]),
            locator()?,
        ),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        Some(Timestamp::from_unix_nanos(2)),
        Some(1024 * 1024),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::try_from(100_u32)?,
        NonZeroU64::try_from(1024 * 1024 * 1024_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let payload = Bytes::from(vec![7_u8; 1024 * 1024]);
    let record = ExtractionRecord::try_new(
        &request,
        source_identifier(&maximum)?,
        ExactPayloadEvidence::with_version_pinned_locator(
            payload_evidence(DigestAlgorithm::Sha256, &payload).content_digest(),
            locator()?,
        ),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(3),
            evidence: source_identifier("release-record")?,
        },
        source_identifier(&maximum)?,
        None,
        payload,
    )?;
    let mut records_with_spare_capacity = Vec::with_capacity(8);
    records_with_spare_capacity.push(record.clone());
    let batch = ExtractionBatch::try_new(&request, records_with_spare_capacity)?;
    let encoded = serde_json::to_vec(&batch)?;
    let encoded_text = std::str::from_utf8(&encoded)?;
    assert!(encoded_text.contains("\"total_retained_bytes\""));
    assert!(!encoded_text.contains("\"logical_retained_bytes\""));
    let decoded: ExtractionBatch = serde_json::from_slice(&encoded)?;
    assert_eq!(decoded.records().len(), 1);
    assert_eq!(serde_json::to_vec(&decoded)?, encoded);
    assert!(matches!(
        ExtractionBatch::try_new(&request, vec![record; 65]),
        Err(ExtractionError::ByteLimitExceeded {
            requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
        })
    ));
    Ok(())
}

#[test]
fn extraction_availability_retains_conservative_basis_and_inference_method() -> TestResult {
    let observed = AvailabilityEvidence::Observed {
        available_at: Timestamp::from_unix_nanos(3),
        evidence: source_identifier("release-record")?,
    };
    let local = AvailabilityEvidence::LocalFirstObserved {
        observed_at: Timestamp::from_unix_nanos(4),
    };
    let inferred = AvailabilityEvidence::Inferred {
        inferred_at: Timestamp::from_unix_nanos(5),
        method: source_identifier("calendar-v2")?,
    };
    let unknown = AvailabilityEvidence::Unknown;

    assert_eq!(
        observed.conservative_available_at(),
        Some(Timestamp::from_unix_nanos(3))
    );
    assert_eq!(
        local.conservative_available_at(),
        Some(Timestamp::from_unix_nanos(4))
    );
    assert_eq!(inferred.conservative_available_at(), None);
    assert_eq!(inferred.reported_at(), Some(Timestamp::from_unix_nanos(5)));
    assert_eq!(unknown.reported_at(), None);
    for basis in [observed, local, inferred, unknown] {
        assert_eq!(
            serde_json::from_slice::<AvailabilityEvidence>(&serde_json::to_vec(&basis)?)?,
            basis
        );
    }
    Ok(())
}

#[test]
fn extraction_identity_and_wire_preserve_calendar_date_precision() -> TestResult {
    let effective_date = CalendarDate::new(2026, 7, 1)?;
    let published_date = CalendarDate::new(2026, 7, 15)?;
    let discovery = DiscoveryRequest::try_new(
        source_identifier("macro-series")?,
        None,
        NonZeroU16::try_from(1_u16)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let object = SourceObject::try_new(
        SourceId::try_from("source-a")?,
        MetadataRevision::new(source_identifier("metadata-a")?),
        &discovery,
        source_identifier("object-a")?,
        source_identifier("application-json")?,
        exact_evidence(4),
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        None,
        Some(100),
    )?;
    let request = ExtractionRequest::try_new(
        object,
        NonZeroU32::try_from(1_u32)?,
        NonZeroU64::try_from(10_000_u64)?,
        Timestamp::from_unix_nanos(100),
    )?;
    let payload = Bytes::from_static(b"record");
    let date_record = ExtractionRecord::try_new_with_time(
        &request,
        source_identifier("schema-v2")?,
        payload_evidence(DigestAlgorithm::Sha256, &payload),
        ResearchTemporalCoordinate::calendar_date(effective_date),
        Some(ResearchTemporalCoordinate::calendar_date(published_date)),
        AvailabilityEvidence::Unknown,
        source_identifier("revision-1")?,
        None,
        payload.clone(),
    )?;
    let exact_record = ExtractionRecord::try_new_with_time(
        &request,
        source_identifier("schema-v2")?,
        payload_evidence(DigestAlgorithm::Sha256, &payload),
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(i64::from(
            effective_date.days_since_unix_epoch(),
        ))),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(i64::from(published_date.days_since_unix_epoch())),
        )),
        AvailabilityEvidence::Unknown,
        source_identifier("revision-1")?,
        None,
        payload,
    )?;
    let mut legacy_wire = serde_json::to_value(&exact_record)?;
    let legacy_object = legacy_wire
        .as_object_mut()
        .ok_or("extraction record must serialize as an object")?;
    legacy_object.remove("temporal_schema_version");
    legacy_object.remove("effective_time");
    legacy_object.remove("published_time");
    legacy_object.remove("superseded_time");
    legacy_object.insert(
        "effective_at".to_owned(),
        serde_json::json!(i64::from(effective_date.days_since_unix_epoch())),
    );
    legacy_object.insert(
        "published_at".to_owned(),
        serde_json::json!(i64::from(published_date.days_since_unix_epoch())),
    );
    legacy_object.insert("superseded_at".to_owned(), serde_json::Value::Null);
    let restored_legacy: ExtractionRecord = serde_json::from_value(legacy_wire)?;
    assert_eq!(
        restored_legacy.effective_time().exact_timestamp(),
        Some(Timestamp::from_unix_nanos(i64::from(
            effective_date.days_since_unix_epoch()
        )))
    );
    let date_batch = ExtractionBatch::try_new(&request, vec![date_record])?;
    let exact_batch = ExtractionBatch::try_new(&request, vec![exact_record])?;

    let period = ResearchPeriod::try_new(
        source_identifier("bls-monthly")?,
        2026,
        NonZeroU16::try_from(13_u16)?,
        source_identifier("M13")?,
    )?;
    let period_record = ExtractionRecord::try_new_with_time(
        &request,
        source_identifier("schema-v2")?,
        payload_evidence(DigestAlgorithm::Sha256, &Bytes::from_static(b"record")),
        ResearchTemporalCoordinate::source_period(period.clone()),
        None,
        AvailabilityEvidence::Unknown,
        source_identifier("revision-1")?,
        None,
        Bytes::from_static(b"record"),
    )?;
    let period_batch = ExtractionBatch::try_new(&request, vec![period_record])?;

    let date_bytes = serde_json::to_vec(&date_batch)?;
    let exact_bytes = serde_json::to_vec(&exact_batch)?;
    assert_ne!(date_bytes, exact_bytes);
    let restored: ExtractionBatch = serde_json::from_slice(&date_bytes)?;
    assert_eq!(
        restored.records()[0].effective_time().exact_timestamp(),
        None
    );
    assert_eq!(
        restored.records()[0].effective_time().calendar_date_value(),
        Some(effective_date)
    );
    assert_eq!(restored.records()[0].available_at(), None);
    let restored_period: ExtractionBatch =
        serde_json::from_slice(&serde_json::to_vec(&period_batch)?)?;
    assert_eq!(
        restored_period.records()[0]
            .effective_time()
            .source_period_value(),
        Some(&period)
    );
    assert_eq!(
        restored_period.records()[0]
            .effective_time()
            .calendar_date_value(),
        None
    );
    Ok(())
}

fn payload_evidence(algorithm: DigestAlgorithm, payload: &Bytes) -> ExactPayloadEvidence {
    let bytes = match algorithm {
        DigestAlgorithm::Sha256 => Sha256::digest(payload).into(),
        DigestAlgorithm::Blake3 => *blake3::hash(payload).as_bytes(),
    };
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(algorithm, bytes))
}

#[test]
fn source_traits_are_object_safe() {
    fn accepts_live(_: &mut dyn LiveMarketSource) {}
    fn accepts_decoder(_: &mut dyn MarketDecoder) {}
    fn accepts_extraction(_: &dyn ExtractionSource) {}
    let _live: fn(&mut dyn LiveMarketSource) = accepts_live;
    let _decoder: fn(&mut dyn MarketDecoder) = accepts_decoder;
    let _extraction: fn(&dyn ExtractionSource) = accepts_extraction;
}

#[test]
fn direct_metadata_binds_timing_and_protocol_validation_profiles() -> TestResult {
    let metadata = direct_metadata("source-a", "revision-a", 0, None)?;
    let freshness = metadata.freshness_policy();
    assert_eq!(freshness.max_transport_age_nanos(), 1_000_000_000);
    assert_eq!(freshness.max_source_age_nanos(), 2_000_000_000);
    assert!(matches!(
        metadata.protocol_profile(),
        SourceProtocolProfile::Live(_)
    ));
    Ok(())
}
