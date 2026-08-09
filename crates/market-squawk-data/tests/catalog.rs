use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use market_squawk_data::{
    ArtifactRecord, BackupReceipt, Catalog, CatalogAuthority, CatalogConfig, CatalogError,
    CatalogLimit, CatalogResultLimits, ContractCompletion, DatasetManifestRecord, IngestIdentity,
    IngestRunState, ListingReferenceError, ListingReferenceExchangeCode, ListingReferenceFileKind,
    ListingReferenceFinancialStatus, ListingReferenceGenerationInput,
    ListingReferenceMarketCategory, ListingReferencePublicationCapability,
    ListingReferencePublicationDisposition, ListingReferenceReadCapability,
    ListingReferenceRecordInput, ListingReferenceSourceFileInput, MarketDataInstrumentCatalogError,
    MarketDataInstrumentMatchKind, MarketDataInstrumentReadCapability,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
    OnboardingAppendOutcome, OnboardingReservationRequest, RightsBasis, RightsDecisionInput,
    RightsError, SourceCursor, SourceOperation, market_data_instrument_id,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, ChecksumCapability,
    ContractRollMapping, CoverageDelay, Currency, DataQuality, DeliveryEvidence, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier,
    ExternalIdentifierRecord, ExternalIdentifierRecordInput, Figi, IdentifierEntitlement,
    IdentifierRightsPolicyReference, InstrumentDefinition, InstrumentId, LifecycleTransition,
    LifecycleTransitionKind, MarketDataDisplayName, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SchemaVersion, SequenceCapability, SourceId, SourceIdentifier,
    SymbolIdentityRecord, Timestamp, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_platform::LocalPaths;
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{
    AuthorityBindings, AuthoritySet, AuthorityVerification, AuthorityVerificationInput,
    AuthorizationGrant, AuthorizationMode, CapabilityRegistrationOutcome, CoverageDomain,
    CoverageTopology, CredentialKind, EvidenceBinding, FreshnessPolicy, HistoricalCapability,
    HumanBoundary, InstrumentCoverage, LifecycleSupport, NetworkAccessPolicy, OnboardingEvent,
    OnboardingState, ProviderCapability, ProviderCapabilityInput, ProviderCapabilityRevision,
    ProviderPublicConfiguration, RatePolicyDescriptor, RightsAdmissionState, SetupMode,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use tokio_util::sync::CancellationToken;

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
    let instrument = test_instrument_revision(
        "e93cb0b3-749f-4efe-a58c-22a788764bc0",
        "inactive",
        2,
        "0.01",
    )?;
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
    assert_eq!(health.applied_migrations(), 21);
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
    assert_eq!(catalog.health()?.applied_migrations(), 21);
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
    assert_eq!(
        catalog.synchronize_instruments(
            std::slice::from_ref(&instrument_v1),
            Timestamp::from_unix_nanos(11),
            CatalogLimit::new(2)?,
        )?,
        1
    );
    assert_eq!(
        catalog.synchronize_instruments(
            std::slice::from_ref(&instrument_v1),
            Timestamp::from_unix_nanos(12),
            CatalogLimit::new(2)?,
        )?,
        0
    );
    assert_eq!(
        catalog.synchronize_instruments(
            std::slice::from_ref(&instrument),
            Timestamp::from_unix_nanos(12),
            CatalogLimit::new(2)?,
        )?,
        1
    );
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
    let search = catalog.search_instruments(
        "msq",
        CatalogLimit::new(8)?,
        Instant::now() + Duration::from_secs(1),
        &CancellationToken::new(),
    )?;
    assert!(!search.has_more());
    assert_eq!(search.matches().len(), 1);
    assert_eq!(
        search.matches()[0].definition().instrument_id(),
        instrument.instrument_id()
    );
    assert_eq!(search.matches()[0].matching_symbols().len(), 1);
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
    let mut retry_rights_input = test_rights_input(source.source_id().clone(), payload, i64::MAX)?;
    retry_rights_input.retrieved_at = Timestamp::from_unix_nanos(16);
    let retry_rights = catalog.admit_source_rights(retry_rights_input)?;
    let retried = catalog.reserve_ingest(&identity, &retry_rights)?;
    assert_eq!(reservation.run_id(), retried.run_id());
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

#[test]
fn onboarding_catalog_replays_exact_non_secret_generation_authority() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("onboarding"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(250),
        CatalogLimit::new(16)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let capability = onboarding_capability()?;
    let requested = AuthoritySet::try_new(vec![SourceIdentifier::try_from("account.read")?])?;
    let catalog = CatalogAuthority::open(config.clone())?;
    assert_eq!(
        catalog.register_provider_capability(&capability)?,
        CapabilityRegistrationOutcome::Inserted
    );
    assert_eq!(
        catalog.register_provider_capability(&capability)?,
        CapabilityRegistrationOutcome::Replay
    );
    let request = OnboardingReservationRequest::try_new(
        &capability,
        ProviderPublicConfiguration::default(),
        requested.clone(),
        SourceIdentifier::try_from("local-user")?,
        SourceIdentifier::try_from("portal-session")?,
        Timestamp::from_unix_nanos(i64::MAX),
        1,
    )?;
    let reservation = catalog.reserve_provider_onboarding(&request)?;
    assert_eq!(
        reservation.initial_state(),
        OnboardingState::UserActionRequired
    );

    let generation = SecretGeneration::new(1)?;
    let reference: SecretRef = serde_json::from_value(serde_json::json!({
        "version": 1,
        "backend": "encrypted_file",
        "locator": "a".repeat(64),
        "generation": generation.get(),
    }))?;
    let events = [
        OnboardingEvent::CredentialStored {
            reference: reference.clone(),
        },
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(AuthorityVerification::try_new(
                &capability,
                AuthorityVerificationInput {
                    requested: requested.clone(),
                    observed: requested,
                    restrictions_digest: digest(70),
                    bindings: AuthorityBindings::new(None, None, None, Some(digest(71))),
                    verified_at: Timestamp::from_unix_nanos(10),
                    expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
                    verifier_revision: SourceIdentifier::try_from("provider-key-info-v1")?,
                    assurance_limitation: SourceIdentifier::try_from(
                        "provider-reported-authority",
                    )?,
                    evidence_digest: digest(72),
                },
            )?),
        },
        OnboardingEvent::RightsAdmitted {
            generation: Some(generation),
            decision_digest: digest(73),
        },
        OnboardingEvent::RatePolicyAdmitted {
            generation: Some(generation),
            policy_digest: capability.rate_policy().evidence_digest(),
        },
        OnboardingEvent::RuntimeVerified {
            generation: Some(generation),
            evidence_digest: digest(74),
        },
        OnboardingEvent::Activate {
            generation: Some(generation),
        },
    ];
    for (offset, event) in events.into_iter().enumerate() {
        let sequence = u64::try_from(offset)?
            .checked_add(1)
            .ok_or(CatalogError::InvalidRecord)?;
        assert_eq!(
            catalog.append_provider_onboarding_event(&reservation, sequence, event.clone())?,
            OnboardingAppendOutcome::Inserted
        );
        assert_eq!(
            catalog.append_provider_onboarding_event(&reservation, sequence, event)?,
            OnboardingAppendOutcome::Replay
        );
    }
    drop(catalog);

    let reopened = CatalogAuthority::open(config)?;
    let resumed = reopened.resume_provider_onboarding(reservation.session_id())?;
    assert_eq!(resumed.lifecycle().state(), OnboardingState::ActiveScoped);
    assert!(resumed.lifecycle().generation_is_active_scoped(generation));
    assert_eq!(
        resumed.lifecycle().generation_reference(generation),
        Some(&reference)
    );
    assert_eq!(resumed.next_sequence(), 7);
    Ok(())
}

#[test]
fn listing_reference_catalog_replays_and_reopens_one_complete_generation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("listing-reference"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let source = listing_reference_source()?;
    let dataset = SourceIdentifier::try_from("nasdaq.symbol-directory.us-listed.v1")?;
    let source_id = source.source_id().clone();
    let initial_generation = listing_reference_generation(source.clone(), None, 20, 91)?;
    let source_payload_set_digest = initial_generation.source_payload_set_digest();

    let catalog = CatalogAuthority::open(config.clone())?;
    catalog.register_source(&source, Timestamp::from_unix_nanos(10))?;
    let mismatched =
        catalog.admit_source_rights(listing_reference_rights(source_id.clone(), digest(74))?)?;
    let authority = Arc::new(Mutex::new(catalog));
    let mismatched_publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        dataset.clone(),
        source_id.clone(),
        mismatched,
    )?;
    assert!(matches!(
        mismatched_publisher.publish(
            initial_generation.clone(),
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        ),
        Err(ListingReferenceError::RightsUnavailable)
    ));
    let catalog = authority
        .try_lock()
        .map_err(|_| CatalogError::AuthorityLockPoisoned)?;
    let rights = catalog.admit_source_rights(listing_reference_rights(
        source_id.clone(),
        source_payload_set_digest,
    )?)?;
    drop(catalog);
    let publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        dataset.clone(),
        source_id.clone(),
        rights,
    )?;
    let reader = ListingReferenceReadCapability::new(
        Arc::clone(&authority),
        dataset.clone(),
        source_id.clone(),
    );
    let cancellation = CancellationToken::new();
    let deadline = || Instant::now() + Duration::from_secs(2);

    let inserted = publisher.publish(initial_generation, deadline(), &cancellation)?;
    assert_eq!(
        inserted.disposition(),
        ListingReferencePublicationDisposition::Inserted
    );
    assert_eq!(inserted.generation().generation_sequence(), 1);
    assert_eq!(inserted.generation().record_count(), 2);

    let page = reader.search("p", 1, deadline(), &cancellation)?;
    assert_eq!(page.matches().len(), 1);
    assert!(page.has_more());
    let exact = reader.search("SPY", 1, deadline(), &cancellation)?;
    assert_eq!(exact.matches().len(), 1);
    assert_eq!(exact.matches()[0].record().provider_symbol(), "SPY");
    assert!(exact.matches()[0].record().is_etf());

    let replay = publisher.publish(
        listing_reference_generation(source.clone(), None, 30, 101)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        replay.disposition(),
        ListingReferencePublicationDisposition::Replay
    );
    assert_eq!(
        replay.generation().generation_digest(),
        inserted.generation().generation_digest()
    );
    drop(reader);
    drop(publisher);
    drop(authority);

    let reopened = CatalogAuthority::open(config)?;
    let rights = reopened.admit_source_rights(listing_reference_rights(
        source_id.clone(),
        source_payload_set_digest,
    )?)?;
    let authority = Arc::new(Mutex::new(reopened));
    let reader = ListingReferenceReadCapability::new(
        Arc::clone(&authority),
        dataset.clone(),
        source_id.clone(),
    );
    let current = reader
        .current(deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(
        current.generation_digest(),
        inserted.generation().generation_digest()
    );
    assert_eq!(current.generation_sequence(), 1);
    assert_eq!(current.record_count(), 2);
    let exact = reader.search("AAPL", 2, deadline(), &cancellation)?;
    assert_eq!(exact.matches().len(), 1);
    let retained = exact.matches()[0].record();
    assert_eq!(retained.provider_symbol(), "AAPL");
    assert_eq!(
        retained.source_file().received_at(),
        Timestamp::from_unix_nanos(20)
    );
    assert_eq!(
        retained.record_payload_evidence().content_digest(),
        digest(91)
    );

    let publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        dataset,
        source_id,
        rights,
    )?;
    let replay = publisher.publish(
        listing_reference_generation(source, None, 40, 111)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        replay.disposition(),
        ListingReferencePublicationDisposition::Replay
    );
    assert_eq!(replay.generation().generation_sequence(), 1);
    Ok(())
}

#[test]
fn figi_market_data_definitions_publish_atomically_and_reopen_current_search() -> TestResult {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-data-instruments"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let figi = Figi::try_from("BBG000B9XVV8")?;
    let derived_id = market_data_instrument_id(&figi)?;
    assert_eq!(derived_id, market_data_instrument_id(&figi)?);

    let initial = market_data_definition(
        figi.clone(),
        derived_id,
        10,
        "Apple Incorporated",
        "AAPL.US",
        31,
    )?;
    let wrong_id = market_data_instrument_id(&Figi::try_from("BBG000BLNNH6")?)?;
    let mismatched =
        market_data_definition(figi.clone(), wrong_id, 11, "Wrong Identity", "WRONG.US", 32)?;
    assert!(matches!(
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone()], 2),
        Err(MarketDataInstrumentCatalogError::PartialBatch {
            expected: 2,
            actual: 1
        })
    ));

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(Arc::clone(&authority));
    let cancellation = CancellationToken::new();
    let deadline = || Instant::now() + Duration::from_secs(2);
    let invalid_batch =
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone(), mismatched], 2)?;
    assert!(matches!(
        publisher.synchronize(invalid_batch, deadline(), &cancellation),
        Err(MarketDataInstrumentCatalogError::MismatchedInstrumentId)
    ));
    assert!(
        reader
            .latest(derived_id, deadline(), &cancellation)?
            .is_none()
    );

    let inserted = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone()], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((inserted.inserted(), inserted.replayed()), (1, 0));
    let retained = reader
        .latest_by_figi(&figi, deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(retained.revision_sequence(), 1);
    let replay = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![initial], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((replay.inserted(), replay.replayed()), (0, 1));
    assert_eq!(
        reader
            .latest(derived_id, deadline(), &cancellation)?
            .ok_or(CatalogError::InvalidRecord)?,
        retained
    );

    let successor =
        market_data_definition(figi.clone(), derived_id, 20, "Apple Inc.", "AAPL.US", 33)?;
    let advanced = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![successor], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((advanced.inserted(), advanced.replayed()), (1, 0));
    let provider_match = reader.search("AAPL.US", 4, deadline(), &cancellation)?;
    assert_eq!(provider_match.matches().len(), 1);
    assert_eq!(
        provider_match.matches()[0].match_kind(),
        MarketDataInstrumentMatchKind::ProviderSymbol
    );
    assert!(
        !serde_json::to_string(provider_match.matches()[0].record().definition())?
            .contains("execution")
    );
    drop(reader);
    drop(publisher);
    drop(authority);

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config)?));
    let reader = MarketDataInstrumentReadCapability::new(authority);
    let reopened = reader
        .latest_by_figi(&figi, deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(reopened.revision_sequence(), 2);
    assert_eq!(
        reopened
            .definition()
            .display_name()
            .ok_or(CatalogError::InvalidRecord)?
            .as_str(),
        "Apple Inc."
    );
    Ok(())
}

fn market_data_definition(
    figi: Figi,
    instrument_id: InstrumentId,
    effective_start: i64,
    display_name: &str,
    provider_symbol: &str,
    evidence_byte: u8,
) -> TestResult<MarketDataInstrumentDefinition> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(effective_start), None)?;
    let exact = |byte| ExactPayloadEvidence::from_content_digest(digest(byte));
    let rights = || -> TestResult<IdentifierRightsPolicyReference> {
        Ok(IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("figi-public-domain-v1")?,
            IdentifierEntitlement::PublicDomain,
            SourceIdentifier::try_from("https://www.openfigi.com/about/figi")?,
        ))
    };
    Ok(MarketDataInstrumentDefinition::try_new(
        MarketDataInstrumentDefinitionInput {
            instrument_id,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(format!(
                    "market-data-definition-{evidence_byte}"
                ))?),
                exact(evidence_byte),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Equity,
            display_name: Some(MarketDataDisplayName::try_new(
                display_name,
                SourceId::try_from("admitted-listing-reference")?,
                exact(evidence_byte.saturating_add(1)),
                rights()?,
            )?),
            quote_currency: Currency::try_from("USD")?,
            quote_currency_evidence: exact(evidence_byte.saturating_add(2)),
            venue_mappings: vec![VenueMapping::new(
                VenueId::try_from("XNAS")?,
                VenueSymbol::try_from("AAPL")?,
            )],
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id,
                source_id: SourceId::try_from("nasdaq-symbol-directory")?,
                provider_instrument_id: ProviderInstrumentId::try_from(provider_symbol)?,
                evidence: ProviderIdentityEvidence::from_content_digest(digest(
                    evidence_byte.saturating_add(3),
                )),
                source_timestamp: Some(Timestamp::from_unix_nanos(effective_start)),
                observed_at: Timestamp::from_unix_nanos(effective_start + 1),
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(format!(
                    "provider-{evidence_byte}"
                ))?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: vec![ExternalIdentifierRecord::new(
                ExternalIdentifierRecordInput {
                    identifier: ExternalIdentifier::Figi(figi),
                    assignment_verification: AssignmentVerification::VerifiedAssigned,
                    source_id: SourceId::try_from("openfigi-v3")?,
                    source_evidence: exact(evidence_byte.saturating_add(4)),
                    source_timestamp: Some(Timestamp::from_unix_nanos(effective_start)),
                    observed_at: Timestamp::from_unix_nanos(effective_start + 1),
                    validity: effective,
                    rights_policy: rights()?,
                },
            )],
        },
    )?)
}

fn listing_reference_generation(
    source: SourceMetadata,
    expected_previous: Option<EvidenceDigest>,
    observed_at: i64,
    record_evidence: u8,
) -> TestResult<ListingReferenceGenerationInput> {
    let creation = "0808202621:31";
    let last_modified = Timestamp::from_unix_nanos(19);
    let observed_at = Timestamp::from_unix_nanos(observed_at);
    let nasdaq_payload = ExactPayloadEvidence::from_content_digest(digest(81));
    let other_payload = ExactPayloadEvidence::from_content_digest(digest(82));
    let files = vec![
        ListingReferenceSourceFileInput::try_new(
            ListingReferenceFileKind::NasdaqListed,
            SourceIdentifier::try_from("nasdaq-symbols:nasdaq-listed:fixture")?,
            SourceIdentifier::try_from(
                "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
            )?,
            creation,
            nasdaq_payload.clone(),
            last_modified,
            observed_at,
            observed_at,
        )?,
        ListingReferenceSourceFileInput::try_new(
            ListingReferenceFileKind::OtherListed,
            SourceIdentifier::try_from("nasdaq-symbols:other-listed:fixture")?,
            SourceIdentifier::try_from(
                "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
            )?,
            creation,
            other_payload.clone(),
            last_modified,
            observed_at,
            observed_at,
        )?,
    ];
    let records = vec![
        (
            ListingReferenceFileKind::NasdaqListed,
            ListingReferenceRecordInput::try_nasdaq_listed(
                2,
                "AAPL",
                "Apple Inc. - Common Stock",
                VenueId::try_from("XNAS")?,
                ListingReferenceMarketCategory::GlobalSelect,
                ListingReferenceFinancialStatus::Normal,
                false,
                false,
                100,
                false,
                SourceIdentifier::try_from("nasdaq-symbols:nasdaq-listed:row-2:fixture")?,
                ExactPayloadEvidence::from_content_digest(digest(record_evidence)),
                creation,
                last_modified,
                observed_at,
                nasdaq_payload,
            )?,
        ),
        (
            ListingReferenceFileKind::OtherListed,
            ListingReferenceRecordInput::try_other_listed(
                2,
                "SPY",
                "SPDR S&P 500 ETF Trust",
                VenueId::try_from("ARCX")?,
                ListingReferenceExchangeCode::NyseArca,
                "SPY",
                "SPY",
                true,
                false,
                100,
                SourceIdentifier::try_from("nasdaq-symbols:other-listed:row-2:fixture")?,
                ExactPayloadEvidence::from_content_digest(digest(record_evidence + 1)),
                creation,
                last_modified,
                observed_at,
                other_payload,
            )?,
        ),
    ];
    Ok(ListingReferenceGenerationInput::try_new(
        source,
        expected_previous,
        files,
        records,
    )?)
}

fn listing_reference_source() -> TestResult<SourceMetadata> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let evidence = |byte| ExactPayloadEvidence::from_content_digest(digest(byte));
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("nasdaq-symbol-directory-fixture")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("nasdaq-symbols-catalog-v1")?),
            evidence(71),
        ),
        SourceClass::LocalFile,
        SourceIdentifier::try_from("local-nasdaq-symbol-fixture")?,
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            AuthorizationBasis::new(SourceIdentifier::try_from("test-owned-fixture")?),
            evidence(72),
            effective,
        ),
        SourceCoverage::try_instrument(
            evidence(73),
            effective,
            vec![AssetClass::Equity, AssetClass::Fund],
            CoverageTopology::partial_venues(vec![
                VenueId::try_from("XNAS")?,
                VenueId::try_from("XNYS")?,
                VenueId::try_from("ARCX")?,
            ])?,
            InstrumentCoverage::all_declared(),
            None,
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
            HistoricalCapability::None,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?)
}

fn listing_reference_rights(
    source_id: SourceId,
    payload_digest: EvidenceDigest,
) -> TestResult<RightsDecisionInput> {
    Ok(RightsDecisionInput {
        source_id,
        payload_digest,
        retrieved_at: Timestamp::from_unix_nanos(15),
        basis: RightsBasis::reviewed_terms(
            "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
            digest(75),
        )?,
        authorization_evidence: digest(76),
        authorization_expires_at: Some(Timestamp::from_unix_nanos(i64::MAX)),
        permitted_operations: vec![
            SourceOperation::Retrieve,
            SourceOperation::Display,
            SourceOperation::Persist,
        ],
    })
}

fn onboarding_capability() -> TestResult<ProviderCapability> {
    Ok(ProviderCapability::try_new(ProviderCapabilityInput {
        surface_id: SourceIdentifier::try_from("provider.private-account")?,
        revision: ProviderCapabilityRevision::new(1)?,
        setup_mode: SetupMode::ManualApiKeyImport,
        official_entry_uri: "https://provider.example.test/settings/api".to_owned(),
        human_boundary: HumanBoundary::ProviderControlled,
        credential_kind: CredentialKind::ApiKey,
        minimum_authority: AuthoritySet::try_new(vec![SourceIdentifier::try_from(
            "account.read",
        )?])?,
        maximum_authority: AuthoritySet::try_new(vec![SourceIdentifier::try_from(
            "account.read",
        )?])?,
        verifier_revision: SourceIdentifier::try_from("provider-key-info-v1")?,
        rate_policy: RatePolicyDescriptor::try_new(
            SourceIdentifier::try_from("provider/private/rest/key-info/v1")?,
            digest(75),
            true,
        )?,
        rights_state: RightsAdmissionState::Pending,
        lifecycle_support: LifecycleSupport::new(true, false, true),
        evidence: vec![EvidenceBinding::new(
            SourceIdentifier::try_from("DOC-TEST-001")?,
            digest(76),
        )],
        refresh_trigger: SourceIdentifier::try_from("provider-private")?,
    })?)
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
