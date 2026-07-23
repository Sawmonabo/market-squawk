use std::error::Error;
use std::time::Duration;

use market_squawk_data::{
    ArtifactRecord, BackupReceipt, Catalog, CatalogAuthority, CatalogConfig, CatalogError,
    CatalogLimit, CatalogResultLimits, ContractCompletion, DatasetManifestRecord, IngestIdentity,
    IngestRunState, RightsBasis, RightsDecisionInput, RightsError, SourceCursor, SourceOperation,
};
use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, ContractRollMapping, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentDefinition, LifecycleTransition, LifecycleTransitionKind, MetadataRevision,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    SymbolIdentityRecord, Timestamp, VenueId, VenueSymbol,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, CoverageDomain, FreshnessPolicy, HistoricalCapability,
    NetworkAccessPolicy, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn catalog_enforces_rights_and_recovers_the_complete_control_record() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("live"))?;
    let backup_paths = LocalPaths::prepare(directory.path().join("backup"))?;
    let location = paths.catalog()?.clone();
    let database = location.path().to_path_buf();
    let backup_location = backup_paths.catalog()?.clone();
    let config = CatalogConfig::try_new(
        location,
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let source_v1 = local_source("revision-1", 1)?;
    let source = local_source("revision-2", 4)?;
    let instrument_v1 = test_instrument("e93cb0b3-749f-4efe-a58c-22a788764bc0", "active")?;
    let instrument = test_instrument("e93cb0b3-749f-4efe-a58c-22a788764bc0", "inactive")?;
    let successor = test_instrument("e7c627d2-147c-45ef-b882-10aab0639db0", "active")?;
    let payload = digest(11);
    let rights_input = test_rights_input(source.source_id().clone(), payload, i64::MAX)?;
    assert!(matches!(
        RightsBasis::reviewed_terms("https://user@example.test/terms#fragment", digest(31)),
        Err(RightsError::InvalidTermsReference)
    ));

    let catalog = CatalogAuthority::open(config.clone())?;
    let health = catalog.health()?;
    assert_eq!(health.journal_mode(), "wal");
    assert!(health.foreign_keys());
    assert!(!health.trusted_schema());
    assert_eq!(health.synchronous(), 2);
    assert_eq!(health.busy_timeout(), Duration::from_millis(750));
    assert_eq!(health.applied_migrations(), 11);
    assert!(matches!(
        CatalogAuthority::open(config.clone()),
        Err(CatalogError::WriterAlreadyOpen)
    ));

    let alias_paths = LocalPaths::prepare(directory.path().join("alias"))?;
    let alias_location = alias_paths.catalog()?.clone();
    std::fs::hard_link(&database, alias_location.path())?;
    let alias_config = CatalogConfig::try_new(
        alias_location.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    assert!(matches!(
        CatalogAuthority::open(alias_config),
        Err(CatalogError::UnsafePath)
    ));
    assert_eq!(catalog.health()?.applied_migrations(), 11);
    drop(catalog);
    std::fs::remove_file(alias_location.path())?;
    let catalog = CatalogAuthority::open(config.clone())?;

    catalog.register_source(&source_v1, Timestamp::from_unix_nanos(9))?;
    catalog.register_source(&source, Timestamp::from_unix_nanos(10))?;
    assert!(matches!(
        catalog.admit_source_rights(test_rights_input(source.source_id().clone(), payload, 100,)?),
        Err(CatalogError::RightsDenied(
            RightsError::AuthorizationExpired
        ))
    ));
    let rights = catalog.admit_source_rights(rights_input)?;
    assert!(matches!(
        catalog.register_source(&source_v1, Timestamp::from_unix_nanos(8)),
        Err(CatalogError::StaleSourceRevision)
    ));
    catalog.put_instrument(&instrument_v1, Timestamp::from_unix_nanos(11))?;
    catalog.put_instrument(&instrument, Timestamp::from_unix_nanos(12))?;
    assert!(matches!(
        catalog.put_instrument(&instrument_v1, Timestamp::from_unix_nanos(10)),
        Err(CatalogError::StaleInstrumentRevision)
    ));
    catalog.put_instrument(&successor, Timestamp::from_unix_nanos(11))?;
    catalog.put_symbol(&SymbolIdentityRecord::new(
        instrument.instrument_id(),
        VenueId::try_from("nasdaq")?,
        VenueSymbol::try_from("MSQ")?,
        EffectiveInterval::new(
            Timestamp::from_unix_nanos(12),
            Some(Timestamp::from_unix_nanos(80)),
        )?,
    ))?;
    catalog.put_lifecycle(&LifecycleTransition::new(
        instrument.instrument_id(),
        Timestamp::from_unix_nanos(80),
        LifecycleTransitionKind::Merger {
            successor: successor.instrument_id(),
        },
    )?)?;
    catalog.put_contract_roll(&ContractRollMapping::new(
        instrument.instrument_id(),
        successor.instrument_id(),
        Timestamp::from_unix_nanos(70),
    )?)?;

    let identity = IngestIdentity::try_new(
        source.source_id().clone(),
        payload,
        SourceOperation::Persist,
        "fred:gdp:2026-07-18",
    )?;
    let reservation = catalog.reserve_ingest(&identity, &rights)?;
    let repeated = catalog.reserve_ingest(&identity, &rights)?;
    assert_eq!(reservation.run_id(), repeated.run_id());
    let unpublished = catalog.reserve_ingest(
        &IngestIdentity::try_new(
            source.source_id().clone(),
            payload,
            SourceOperation::Persist,
            "fred:gdp:unpublished",
        )?,
        &rights,
    )?;
    assert!(matches!(
        catalog.complete_ingest(&unpublished, ContractCompletion::Succeeded,),
        Err(CatalogError::RunStateConflict)
    ));

    let denied = IngestIdentity::try_new(
        source.source_id().clone(),
        payload,
        SourceOperation::Train,
        "fred:gdp:train",
    )?;
    assert!(matches!(
        catalog.reserve_ingest(&denied, &rights),
        Err(CatalogError::RightsDenied(_))
    ));
    let conflicting_payload = digest(12);
    let conflicting_identity = IngestIdentity::try_new(
        source.source_id().clone(),
        conflicting_payload,
        SourceOperation::Persist,
        identity.idempotency_key(),
    )?;
    let conflicting_rights = catalog.admit_source_rights(test_rights_input(
        source.source_id().clone(),
        conflicting_payload,
        i64::MAX,
    )?)?;
    assert!(matches!(
        catalog.reserve_ingest(&conflicting_identity, &conflicting_rights),
        Err(CatalogError::IdempotencyConflict)
    ));

    let cursor = SourceCursor::try_new(
        source.source_id().clone(),
        "observations",
        "cursor-7",
        Timestamp::from_unix_nanos(30),
    )?;
    catalog.set_cursor(&cursor)?;
    assert!(matches!(
        catalog.set_cursor(&SourceCursor::try_new(
            source.source_id().clone(),
            "observations",
            "different-cursor",
            Timestamp::from_unix_nanos(30),
        )?),
        Err(CatalogError::CursorConflict)
    ));
    let artifact = ArtifactRecord::try_new(
        "macro/fred/gdp/part-0001.parquet",
        digest(21),
        4_096,
        shift_timestamp(reservation.requested_at(), 1)?,
    )?;
    let premature_artifact = ArtifactRecord::try_new(
        "macro/fred/gdp/premature.parquet",
        digest(20),
        128,
        shift_timestamp(reservation.requested_at(), -1)?,
    )?;
    let premature_manifest = DatasetManifestRecord::try_new(
        SourceIdentifier::try_from("fred-gdp-premature")?,
        SchemaVersion::CURRENT,
        premature_artifact.artifact_id(),
        digest(20),
        reservation.requested_at(),
    );
    assert!(matches!(
        catalog.publish_artifact_manifest(&reservation, &premature_artifact, &premature_manifest),
        Err(CatalogError::PublicationTimeConflict)
    ));
    let manifest = DatasetManifestRecord::try_new(
        SourceIdentifier::try_from("fred-gdp")?,
        SchemaVersion::CURRENT,
        artifact.artifact_id(),
        digest(22),
        shift_timestamp(reservation.requested_at(), 2)?,
    );
    let published = catalog.publish_artifact_manifest(&reservation, &artifact, &manifest)?;
    assert_eq!(published.artifact(), &artifact);
    drop(catalog);

    let reopened = CatalogAuthority::open(config.clone())?;
    let resumed = reopened.resume_ingest(reservation.run_id())?;
    assert_eq!(resumed.publication(), Some(&published));
    let reconstructed_artifact = ArtifactRecord::try_new(
        artifact.relative_reference(),
        artifact.content_digest(),
        artifact.size_bytes(),
        shift_timestamp(reservation.requested_at(), 4)?,
    )?;
    let reconstructed_manifest = DatasetManifestRecord::try_new(
        manifest.dataset_name().clone(),
        manifest.schema_version(),
        reconstructed_artifact.artifact_id(),
        manifest.content_digest(),
        shift_timestamp(reservation.requested_at(), 5)?,
    );
    assert_eq!(
        reopened.publish_artifact_manifest(
            resumed.reservation(),
            &reconstructed_artifact,
            &reconstructed_manifest,
        )?,
        published
    );
    reopened.complete_ingest(resumed.reservation(), ContractCompletion::Succeeded)?;
    let backup_receipt = reopened.backup_to(&backup_location)?;
    let receipt_bytes = serde_json::to_vec(&backup_receipt)?;
    let backup_receipt = serde_json::from_slice::<BackupReceipt>(&receipt_bytes)?;
    Catalog::verify_backup(&backup_location, &backup_receipt)?;
    assert!(matches!(
        reopened.backup_to(&backup_location),
        Err(CatalogError::BackupAlreadyExists)
    ));
    reopened.integrity_check()?;
    assert_eq!(reopened.source(source.source_id())?, Some(source.clone()));
    assert_eq!(
        reopened.source_history(source_v1.source_id(), CatalogLimit::new(4)?)?,
        vec![source.clone(), source_v1.clone()]
    );
    assert_eq!(
        reopened.cursor(cursor.source_id(), cursor.name())?,
        Some(cursor.clone())
    );
    assert_eq!(
        reopened.manifest(manifest.manifest_id())?,
        Some(manifest.clone())
    );
    assert_eq!(
        reopened.artifact(artifact.artifact_id())?,
        Some(artifact.clone())
    );
    assert_eq!(
        reopened
            .ingest_run(reservation.run_id())?
            .map(|run| run.state()),
        Some(IngestRunState::Succeeded)
    );
    let active_runs = reopened.active_ingest_runs(CatalogLimit::new(4)?)?;
    assert_eq!(active_runs.len(), 1);
    assert_eq!(active_runs[0].run_id(), unpublished.run_id());
    let references =
        reopened.reference_bundle(instrument.instrument_id(), CatalogLimit::new(8)?)?;
    let identity_only =
        reopened.reference_bundle(instrument.instrument_id(), CatalogLimit::new(1)?)?;
    assert_eq!(references.instrument(), Some(&instrument));
    assert!(
        identity_only.instrument().is_some()
            && identity_only.symbols().is_empty()
            && identity_only.lifecycle().is_empty()
            && identity_only.contract_rolls().is_empty()
            && identity_only.corporate_actions().is_empty()
    );
    assert_eq!(
        reopened.instrument_history(instrument.instrument_id(), CatalogLimit::new(4)?)?,
        vec![instrument.clone(), instrument_v1]
    );
    assert_eq!(references.symbols().len(), 1);
    assert_eq!(references.lifecycle().len(), 1);
    assert_eq!(references.contract_rolls().len(), 1);
    assert!(reopened.audit_events(CatalogLimit::new(32)?)?.len() >= 8);
    drop(reopened);

    let one_source_bytes = serde_json::to_vec(&source)?
        .len()
        .max(serde_json::to_vec(&source_v1)?.len())
        .checked_add(32)
        .ok_or(CatalogError::InvalidConfiguration)?;
    let bounded = CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, one_source_bytes)?,
    )?)?;
    assert!(matches!(
        bounded.source_history(source_v1.source_id(), CatalogLimit::new(4)?),
        Err(CatalogError::ResultByteLimitExceeded)
    ));
    drop(bounded);

    let restored = CatalogAuthority::open(CatalogConfig::try_new(
        backup_location,
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?)?;
    assert_eq!(restored.manifest(manifest.manifest_id())?, Some(manifest));
    drop(restored);

    let connection = rusqlite::Connection::open(database)?;
    connection.execute(
        "UPDATE catalog_authority_clock SET last_timestamp_ns=?1 WHERE singleton=1",
        [i64::MAX],
    )?;
    drop(connection);
    let rolled_back = CatalogAuthority::open(config)?;
    assert!(matches!(
        rolled_back.admit_source_rights(test_rights_input(
            source.source_id().clone(),
            payload,
            i64::MAX,
        )?),
        Err(CatalogError::AuthorityClockRollback)
    ));
    assert!(matches!(
        rolled_back.set_cursor(&cursor),
        Err(CatalogError::AuthorityClockRollback)
    ));
    Ok(())
}

#[test]
fn pinned_instrument_definitions_resolve_at_catalog_observation_boundaries() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("definitions"))?;
    let catalog = CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(250),
        CatalogLimit::new(8)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?)?;
    let instrument_id = "00000000-0000-0000-0000-000000000020";
    let definition_v1 = test_instrument_revision(instrument_id, "active", 1, "0.01")?;
    let definition_v2 = test_instrument_revision(instrument_id, "active", 2, "0.05")?;
    catalog.put_instrument(&definition_v1, Timestamp::from_unix_nanos(10))?;
    catalog.put_instrument(&definition_v2, Timestamp::from_unix_nanos(20))?;

    let pinned = catalog.pin_instrument_definitions(
        &[definition_v1.instrument_id()],
        Timestamp::from_unix_nanos(30),
        CatalogLimit::new(2)?,
    )?;

    assert_eq!(pinned.as_of(), Timestamp::from_unix_nanos(30));
    for (decision_at, expected) in [
        (19, definition_v1.execution_terms()),
        (20, definition_v2.execution_terms()),
        (30, definition_v2.execution_terms()),
    ] {
        assert_eq!(
            pinned.execution_terms_at(
                definition_v1.instrument_id(),
                Timestamp::from_unix_nanos(decision_at)
            ),
            Some(expected)
        );
    }
    assert_eq!(
        pinned.execution_terms_at(definition_v1.instrument_id(), Timestamp::from_unix_nanos(9)),
        None
    );
    Ok(())
}

#[test]
fn catalog_rejects_tampered_migration_identity() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("tamper"))?;
    let location = paths.catalog()?.clone();
    let database = location.path().to_path_buf();
    let config = CatalogConfig::try_new(
        location,
        Duration::from_millis(250),
        CatalogLimit::new(4)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    drop(CatalogAuthority::open(config.clone())?);
    let connection = rusqlite::Connection::open(database)?;
    connection.execute(
        "UPDATE schema_migrations SET sha256 = zeroblob(32) WHERE version = 1",
        [],
    )?;
    drop(connection);
    assert!(matches!(
        CatalogAuthority::open(config),
        Err(CatalogError::MigrationDigestMismatch { version: 1 })
    ));
    Ok(())
}

fn digest(byte: u8) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
}

fn test_rights_input(
    source_id: SourceId,
    payload_digest: EvidenceDigest,
    expires_at: i64,
) -> TestResult<RightsDecisionInput> {
    Ok(RightsDecisionInput {
        source_id,
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms("https://example.test/terms/v1", digest(31))?,
        authorization_evidence: digest(32),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(expires_at)),
        permitted_operations: vec![SourceOperation::Retrieve, SourceOperation::Persist],
    })
}

fn shift_timestamp(timestamp: Timestamp, nanoseconds: i64) -> Result<Timestamp, CatalogError> {
    timestamp
        .unix_nanos()
        .checked_add(nanoseconds)
        .map(Timestamp::from_unix_nanos)
        .ok_or(CatalogError::InvalidRecord)
}

fn local_source(revision: &str, revision_evidence_byte: u8) -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let evidence = |byte| ExactPayloadEvidence::from_content_digest(digest(byte));
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("fred-local-fixture")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from(revision)?),
            evidence(revision_evidence_byte),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("local")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("user-owned-file")?),
            evidence(2),
            effective,
        ),
        SourceCoverage::try_non_instrument(
            evidence(3),
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

fn test_instrument(id: &str, status: &str) -> TestResult<InstrumentDefinition> {
    test_instrument_revision(id, status, 1, "0.01")
}

fn test_instrument_revision(
    id: &str,
    status: &str,
    revision: u64,
    tick_size: &str,
) -> TestResult<InstrumentDefinition> {
    let _: market_squawk_domain::InstrumentId = id.parse()?;
    Ok(serde_json::from_value(serde_json::json!({
        "instrument_id": id,
        "definition_revision": revision,
        "asset_class": "equity",
        "primary_denomination": { "kind": "currency", "value": "USD" },
        "quote_currency": "USD",
        "tick_size": tick_size,
        "lot_size": "1",
        "contract_multiplier": "1",
        "venue_mappings": [],
        "identifiers": [],
        "trading_status": status
    }))?)
}
