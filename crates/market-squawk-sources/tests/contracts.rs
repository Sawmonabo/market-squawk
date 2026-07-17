mod common;

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, CaptureIntegrityState, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    MetadataRevision, ProviderChannel, ProviderProduct, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, StreamIntegrityState, Timestamp,
    VersionPinnedSourceLocator,
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

use common::{TestResult, direct_metadata, exact_evidence, source_identifier};

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
    let first = factory.try_frame(
        Timestamp::from_unix_nanos(2),
        TransportFrameKind::Binary,
        Bytes::from_static(b"one"),
    )?;
    let second = factory.try_frame(
        Timestamp::from_unix_nanos(3),
        TransportFrameKind::Binary,
        Bytes::from_static(b"two"),
    )?;
    assert!(first.binding().shares_allocation_with(second.binding()));
    let oversized_backing = Bytes::from(vec![7_u8; MAX_RAW_FRAME_BYTES + 1]);
    let tiny_slice = oversized_backing.slice(0..1);
    let original_pointer = tiny_slice.as_ptr();
    let normalized = factory.try_frame(
        Timestamp::from_unix_nanos(4),
        TransportFrameKind::Binary,
        tiny_slice,
    )?;
    assert_eq!(normalized.retained_payload_bytes(), 1);
    assert_ne!(normalized.payload().as_ptr(), original_pointer);
    assert!(
        factory
            .try_frame(
                Timestamp::from_unix_nanos(4),
                TransportFrameKind::Binary,
                Bytes::from(vec![0; MAX_RAW_FRAME_BYTES + 1]),
            )
            .is_err()
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
    let record = ExtractionRecord::try_new(
        &other_request,
        source_identifier("schema-v1")?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [5; 32],
        )),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(3),
        },
        source_identifier("revision-1")?,
        Some(Timestamp::from_unix_nanos(10)),
        Bytes::from_static(b"record"),
    )?;
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
    let record = ExtractionRecord::try_new(
        &request,
        source_identifier(&maximum)?,
        ExactPayloadEvidence::with_version_pinned_locator(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [5; 32]),
            locator()?,
        ),
        Timestamp::from_unix_nanos(1),
        Some(Timestamp::from_unix_nanos(2)),
        AvailabilityEvidence::Observed {
            available_at: Timestamp::from_unix_nanos(3),
        },
        source_identifier(&maximum)?,
        None,
        Bytes::from(vec![7_u8; 1024 * 1024]),
    )?;
    assert!(matches!(
        ExtractionBatch::try_new(&request, vec![record; 65]),
        Err(ExtractionError::ByteLimitExceeded {
            requested: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES
        })
    ));
    Ok(())
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
