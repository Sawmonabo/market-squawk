use std::collections::BTreeSet;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    AnalyticalDataService, AnalyticalManifestCatalog, ArtifactRecord, BackupReceipt, Catalog,
    CatalogAuthority, CatalogConfig, CatalogError, CatalogLimit, CatalogResultLimits,
    CompanySecurityIdentityDisposition, CompanySecurityIdentityExclusionReason,
    CompanySecurityIdentityQuery, CompanySecurityIdentityReadCapability,
    CompanySecurityLinkPublicationCapability, ContractCompletion, DatasetManifestRecord,
    IngestIdentity, IngestRunState, ListingReferenceError, ListingReferenceExchangeCode,
    ListingReferenceFileKind, ListingReferenceFinancialStatus, ListingReferenceGenerationInput,
    ListingReferenceGenerationSelection, ListingReferenceMarketCategory,
    ListingReferenceMembershipPageState, ListingReferencePublicationCapability,
    ListingReferencePublicationDisposition, ListingReferenceReadCapability,
    ListingReferenceRecordInput, ListingReferenceSourceFileInput,
    MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS, MarketDataInstrumentCatalogError,
    MarketDataInstrumentMatchKind, MarketDataInstrumentPopulationDisposition,
    MarketDataInstrumentPopulationExclusionReason, MarketDataInstrumentPopulationQuery,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
    MarketDataInstrumentSynchronizationCapability, MarketDataProviderIdentityQuery,
    MarketDataProviderIdentityResolutionOutcome, ObjectStoreConfig, OnboardingAppendOutcome,
    OnboardingReservationRequest, RightsBasis, RightsDecisionInput, RightsError,
    SecFundamentalIdentityAvailability, SecFundamentalIdentityQuery, SourceCursor, SourceOperation,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, AvailabilityEvidence,
    ChecksumCapability, CommonEquitySuitability, CompanyIdentityObservation,
    CompanyIdentityObservationInput, CompanyIdentitySurface, CompanySecurityIdentityLink,
    CompanySecurityIdentityLinkInput, CompanySecurityKind, CompanySecurityLinkTransition,
    CompanySecurityRelationshipKind, CompanySecurityResolutionBasis, ContractRollMapping,
    CoverageDelay, Currency, Cusip, DataQuality, DeliveryEvidence, DigestAlgorithm,
    EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExternalIdentifier,
    ExternalIdentifierRecord, ExternalIdentifierRecordInput, IdentifierEntitlement,
    IdentifierRightsPolicyReference, InstrumentDefinition, InstrumentId, LifecycleTransition,
    LifecycleTransitionKind, MarketDataDisplayName, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId,
    ProviderReportedSecurityAssociation, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, SymbolIdentityRecord, Timestamp, VenueId,
    VenueMapping, VenueSymbol, VersionPinnedSourceLocator,
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
use rusqlite::{Connection, params};
use sha2::{Digest as _, Sha256};
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
    assert_eq!(health.applied_migrations(), 22);
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
    assert_eq!(catalog.health()?.applied_migrations(), 22);
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
        catalog.publish_artifact_manifest(
            &reservation,
            std::slice::from_ref(&premature_artifact),
            &premature_manifest,
        ),
        Err(CatalogError::PublicationTimeConflict)
    ));
    let manifest = DatasetManifestRecord::try_new(
        SourceIdentifier::try_from("fred-gdp")?,
        SchemaVersion::CURRENT,
        artifact.artifact_id(),
        digest(22),
        shift_timestamp(reservation.requested_at(), 2)?,
    );
    let published = catalog.publish_artifact_manifest(
        &reservation,
        std::slice::from_ref(&artifact),
        &manifest,
    )?;
    assert_eq!(published.artifacts(), std::slice::from_ref(&artifact));
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
    assert!(matches!(
        reopened.publish_artifact_manifest(
            resumed.reservation(),
            std::slice::from_ref(&reconstructed_artifact),
            &reconstructed_manifest,
        ),
        Err(CatalogError::EvidenceConflict)
    ));
    assert_eq!(
        reopened.publish_artifact_manifest(
            resumed.reservation(),
            std::slice::from_ref(&artifact),
            &manifest,
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
    let database = paths.catalog()?.path().to_path_buf();
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(250),
        CatalogLimit::new(16)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let capability = onboarding_capability()?;
    let requested = AuthoritySet::try_new(vec![SourceIdentifier::try_from("account.read")?])?;
    let object_config = ObjectStoreConfig::try_new(8 * 1024 * 1024, 1024, Duration::from_secs(60))?;
    let (composition, catalog) = AnalyticalDataService::initialize_with_provider_onboarding(
        CatalogAuthority::open(config.clone())?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (analytical, _publisher) = composition.into_parts();
    let catalog = Arc::new(catalog);
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
    let ordinary_events = [
        OnboardingEvent::CredentialStored {
            reference: reference.clone(),
        },
        OnboardingEvent::AuthorityVerified {
            verification: Box::new(AuthorityVerification::try_new(
                &capability,
                AuthorityVerificationInput {
                    requested: requested.clone(),
                    observed: requested.clone(),
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
    ];
    for (offset, event) in ordinary_events.iter().cloned().enumerate() {
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
    assert_eq!(
        catalog.append_digest_runtime_verification(
            &reservation,
            5,
            Some(generation),
            digest(74),
        )?,
        OnboardingAppendOutcome::Inserted
    );
    assert_eq!(
        catalog.append_digest_runtime_verification(
            &reservation,
            5,
            Some(generation),
            digest(74),
        )?,
        OnboardingAppendOutcome::Replay
    );
    let activate = OnboardingEvent::Activate {
        generation: Some(generation),
    };
    let before_activation =
        catalog.resume_provider_onboarding_with_snapshot(reservation.session_id(), None)?;
    let unchanged = catalog.resume_provider_onboarding_with_snapshot(
        reservation.session_id(),
        Some(&before_activation),
    )?;
    assert!(Arc::ptr_eq(&before_activation, &unchanged));
    let other_handle = Arc::clone(&catalog);
    assert_eq!(
        other_handle.append_provider_onboarding_event(&reservation, 6, activate.clone())?,
        OnboardingAppendOutcome::Inserted
    );
    drop(other_handle);
    let active_snapshot = catalog.resume_provider_onboarding_with_snapshot(
        reservation.session_id(),
        Some(&before_activation),
    )?;
    assert!(!Arc::ptr_eq(&before_activation, &active_snapshot));
    assert_eq!(
        active_snapshot.lifecycle().state(),
        OnboardingState::ActiveScoped
    );
    assert_eq!(active_snapshot.next_sequence(), 7);
    assert_eq!(
        catalog.append_provider_onboarding_event(&reservation, 6, activate)?,
        OnboardingAppendOutcome::Replay
    );
    let wall_now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let late_deadline = wall_now
        .checked_add(Duration::from_secs(1))
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(CatalogError::InvalidRecord)?;
    let zero_event_request = OnboardingReservationRequest::try_new(
        &capability,
        ProviderPublicConfiguration::default(),
        requested.clone(),
        SourceIdentifier::try_from("local-user")?,
        SourceIdentifier::try_from("reserved-without-events")?,
        late_deadline,
        1,
    )?;
    let zero_event_reservation = catalog.reserve_provider_onboarding(&zero_event_request)?;
    let zero_event_session_id = zero_event_reservation.session_id();
    assert!(matches!(
        catalog.resume_provider_onboarding_with_snapshot(
            zero_event_session_id,
            Some(&active_snapshot),
        ),
        Err(CatalogError::InvalidOnboardingReservationCapability)
    ));
    assert_eq!(
        zero_event_reservation.initial_state(),
        OnboardingState::UserActionRequired
    );
    let late_request = OnboardingReservationRequest::try_new(
        &capability,
        ProviderPublicConfiguration::default(),
        requested,
        SourceIdentifier::try_from("local-user")?,
        SourceIdentifier::try_from("late-replay-session")?,
        late_deadline,
        1,
    )?;
    let late_reservation = catalog.reserve_provider_onboarding(&late_request)?;
    let late_event = OnboardingEvent::CredentialStored {
        reference: reference.clone(),
    };
    assert_eq!(
        catalog.append_provider_onboarding_event(&late_reservation, 1, late_event.clone())?,
        OnboardingAppendOutcome::Inserted
    );
    drop(catalog);
    drop(analytical);

    let (composition, first_reopened) = AnalyticalDataService::open_with_provider_onboarding(
        CatalogAuthority::open(config.clone())?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (firstreopened_analytical, _publisher) = composition.into_parts();
    assert!(matches!(
        first_reopened.resume_provider_onboarding_with_snapshot(
            reservation.session_id(),
            Some(&active_snapshot),
        ),
        Err(CatalogError::InvalidOnboardingReservationCapability)
    ));
    assert_eq!(first_reopened.health()?.applied_migrations(), 22);
    let resumed = first_reopened.resume_provider_onboarding(reservation.session_id())?;
    assert_eq!(resumed.lifecycle().state(), OnboardingState::ActiveScoped);
    assert!(resumed.lifecycle().generation_is_active_scoped(generation));
    assert_eq!(
        resumed.lifecycle().generation_reference(generation),
        Some(&reference)
    );
    assert_eq!(resumed.next_sequence(), 7);
    let late_resumed = first_reopened.resume_provider_onboarding(late_reservation.session_id())?;
    assert_eq!(late_resumed.next_sequence(), 2);
    let zero_event_resumed = first_reopened.resume_provider_onboarding(zero_event_session_id)?;
    assert_eq!(
        zero_event_resumed.lifecycle().state(),
        OnboardingState::UserActionRequired
    );
    assert_eq!(zero_event_resumed.next_sequence(), 1);

    let head_reader = Connection::open(&database)?;
    let head_count: i64 = head_reader.query_row(
        "SELECT COUNT(*) FROM provider_onboarding_stream_heads",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(head_count, 3);
    let successful_head: (i64, i64, Option<i64>, Option<i64>, Vec<u8>) = head_reader.query_row(
        "SELECT stream_version, event_count, last_event_sequence, last_audit_sequence,
                cumulative_sha256
         FROM provider_onboarding_stream_heads WHERE session_id=?1",
        [reservation.session_id().to_string()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    assert_eq!(successful_head.0, 1);
    assert_eq!(successful_head.1, 6);
    assert_eq!(successful_head.2, Some(6));
    assert!(successful_head.3.is_some());
    assert_eq!(successful_head.4.len(), 32);
    assert_ne!(successful_head.4, vec![0_u8; 32]);
    let zero_event_head: (i64, i64, Option<i64>, Option<i64>, Vec<u8>) = head_reader.query_row(
        "SELECT stream_version, event_count, last_event_sequence, last_audit_sequence,
                cumulative_sha256
         FROM provider_onboarding_stream_heads WHERE session_id=?1",
        [zero_event_session_id.to_string()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    assert_eq!(zero_event_head.0, 1);
    assert_eq!(zero_event_head.1, 0);
    assert_eq!(zero_event_head.2, None);
    assert_eq!(zero_event_head.3, None);
    assert_eq!(zero_event_head.4.len(), 32);
    assert_ne!(zero_event_head.4, vec![0_u8; 32]);
    drop(head_reader);
    drop(first_reopened);
    drop(firstreopened_analytical);

    let (composition, reopened) = AnalyticalDataService::open_with_provider_onboarding(
        CatalogAuthority::open(config.clone())?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (reopened_analytical, _publisher) = composition.into_parts();
    assert_eq!(reopened.health()?.applied_migrations(), 22);
    assert_eq!(
        reopened
            .resume_provider_onboarding(reservation.session_id())?
            .next_sequence(),
        7
    );
    let reopened_zero_event = reopened.resume_provider_onboarding(zero_event_session_id)?;
    assert_eq!(
        reopened_zero_event.lifecycle().state(),
        OnboardingState::UserActionRequired
    );
    assert_eq!(reopened_zero_event.next_sequence(), 1);
    let late_resumed = reopened.resume_provider_onboarding(late_reservation.session_id())?;
    let wall_now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let wall_now = i64::try_from(wall_now.as_nanos())?;
    if let Some(remaining) = late_deadline.unix_nanos().checked_sub(wall_now)
        && remaining >= 0
    {
        let wait_nanos = u64::try_from(remaining)?
            .checked_add(1_000_000)
            .ok_or(CatalogError::InvalidRecord)?;
        std::thread::sleep(Duration::from_nanos(wait_nanos));
    }
    assert_eq!(
        reopened.append_provider_onboarding_event(
            late_resumed.reservation(),
            1,
            late_event.clone()
        )?,
        OnboardingAppendOutcome::Replay
    );
    for (offset, event) in ordinary_events.into_iter().enumerate().skip(1) {
        let sequence = u64::try_from(offset)? + 1;
        assert_eq!(
            reopened.append_provider_onboarding_event(
                late_resumed.reservation(),
                sequence,
                event
            )?,
            OnboardingAppendOutcome::Inserted
        );
    }
    reopened.append_digest_runtime_verification(
        late_resumed.reservation(),
        5,
        Some(generation),
        digest(74),
    )?;
    let activate = OnboardingEvent::Activate {
        generation: Some(generation),
    };
    reopened.append_provider_onboarding_event(late_resumed.reservation(), 6, activate.clone())?;
    assert_eq!(
        reopened.append_provider_onboarding_event(late_resumed.reservation(), 6, activate)?,
        OnboardingAppendOutcome::Replay
    );
    assert!(matches!(
        reopened.append_provider_onboarding_event(reopened_zero_event.reservation(), 1, late_event),
        Err(CatalogError::OnboardingDeadlineExceeded)
    ));
    drop(reopened);
    drop(reopened_analytical);
    let (composition, final_reopened) = AnalyticalDataService::open_with_provider_onboarding(
        CatalogAuthority::open(config)?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (_analytical, _publisher) = composition.into_parts();
    let durable = final_reopened.resume_provider_onboarding(late_reservation.session_id())?;
    assert_eq!(durable.lifecycle().state(), OnboardingState::ActiveScoped);
    assert_eq!(
        durable.lifecycle().generation_reference(generation),
        Some(&reference)
    );
    assert_eq!(durable.next_sequence(), 7);
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
    let reader = ListingReferenceReadCapability::new(&catalog, dataset.clone(), source_id.clone());
    drop(catalog);
    let publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        dataset.clone(),
        source_id.clone(),
        rights,
    )?;
    let cancellation = CancellationToken::new();
    let deadline = || Instant::now() + Duration::from_secs(2);

    let inserted = publisher.publish(initial_generation, deadline(), &cancellation)?;
    assert_eq!(
        inserted.disposition(),
        ListingReferencePublicationDisposition::Inserted
    );
    assert_eq!(inserted.generation().generation_sequence(), 1);
    assert_eq!(inserted.generation().record_count(), 2);

    // Ordinary reads must remain available while the publication owner holds the writer.
    let writer_guard = authority
        .try_lock()
        .map_err(|_| CatalogError::AuthorityLockPoisoned)?;
    assert_eq!(
        reader.current(deadline(), &cancellation)?.as_ref(),
        Some(inserted.generation())
    );
    let page = reader.search("p", 1, deadline(), &cancellation)?;
    assert_eq!(page.matches().len(), 1);
    assert!(page.has_more());
    let exact = reader.search("SPY", 1, deadline(), &cancellation)?;
    assert_eq!(exact.matches().len(), 1);
    assert_eq!(exact.matches()[0].record().provider_symbol(), "SPY");
    assert!(exact.matches()[0].record().is_etf());
    assert_eq!(
        reader
            .exact_current(
                "SPY",
                &VenueId::try_from("ARCX")?,
                deadline(),
                &cancellation
            )?
            .as_ref(),
        Some(exact.matches()[0].record())
    );

    let first_membership_page = reader.memberships(
        ListingReferenceGenerationSelection::Current,
        None,
        1,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        first_membership_page.state(),
        ListingReferenceMembershipPageState::Truncated
    );
    assert_eq!(first_membership_page.records().len(), 1);
    assert_eq!(first_membership_page.records()[0].provider_symbol(), "AAPL");
    assert_eq!(
        first_membership_page.receipt().selected_generation_digest(),
        Some(inserted.generation().generation_digest())
    );
    assert_eq!(
        first_membership_page
            .receipt()
            .selected_generation_published_at(),
        Some(inserted.generation().published_at())
    );
    assert_eq!(
        first_membership_page.receipt().rights_id(),
        Some(inserted.generation().rights_id())
    );
    assert_eq!(
        first_membership_page.receipt().source_revision_digest(),
        Some(inserted.generation().source_revision_digest())
    );
    assert!(
        first_membership_page.receipt().authorization_checked_at()
            >= inserted.generation().published_at()
    );
    let first_cursor = first_membership_page
        .next_cursor()
        .ok_or(CatalogError::InvalidRecord)?;
    let second_membership_page = reader.memberships(
        ListingReferenceGenerationSelection::Current,
        Some(first_cursor),
        1,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        second_membership_page.state(),
        ListingReferenceMembershipPageState::Complete
    );
    assert_eq!(second_membership_page.records().len(), 1);
    assert_eq!(second_membership_page.records()[0].provider_symbol(), "SPY");
    assert!(second_membership_page.next_cursor().is_none());
    assert_ne!(
        second_membership_page.receipt().ordered_rows_digest(),
        first_membership_page.receipt().ordered_rows_digest()
    );
    assert_ne!(
        second_membership_page.receipt().receipt_digest(),
        first_membership_page.receipt().receipt_digest()
    );

    let before_first_publication = Timestamp::from_unix_nanos(
        inserted
            .generation()
            .published_at()
            .unix_nanos()
            .checked_sub(1)
            .ok_or(CatalogError::InvalidRecord)?,
    );
    let empty_as_of = reader.memberships(
        ListingReferenceGenerationSelection::AsOf(before_first_publication),
        None,
        2,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        empty_as_of.state(),
        ListingReferenceMembershipPageState::Complete
    );
    assert!(empty_as_of.generation().is_none());
    assert!(empty_as_of.records().is_empty());
    assert_eq!(empty_as_of.receipt().selected_generation_digest(), None);
    assert_ne!(
        empty_as_of.receipt().receipt_digest(),
        first_membership_page.receipt().receipt_digest()
    );

    let exact_as_of = reader.memberships(
        ListingReferenceGenerationSelection::AsOf(inserted.generation().published_at()),
        None,
        2,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        exact_as_of.state(),
        ListingReferenceMembershipPageState::Complete
    );
    assert_eq!(exact_as_of.records().len(), 2);
    assert_eq!(
        exact_as_of.receipt().requested_knowledge_at(),
        inserted.generation().published_at()
    );
    assert_eq!(
        exact_as_of.receipt().selected_generation_published_at(),
        Some(inserted.generation().published_at())
    );
    assert!(matches!(
        reader.memberships(
            ListingReferenceGenerationSelection::Current,
            None,
            MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS + 1,
            deadline(),
            &cancellation,
        ),
        Err(ListingReferenceError::InvalidLimit)
    ));
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        reader.memberships(
            ListingReferenceGenerationSelection::Current,
            None,
            2,
            deadline(),
            &cancelled,
        ),
        Err(ListingReferenceError::Cancelled)
    ));
    assert!(matches!(
        reader.memberships(
            ListingReferenceGenerationSelection::Current,
            None,
            2,
            Instant::now(),
            &cancellation,
        ),
        Err(ListingReferenceError::DeadlineExceeded)
    ));
    drop(writer_guard);

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
    drop(mismatched_publisher);
    drop(authority);

    let reopened = CatalogAuthority::open(config)?;
    let rights = reopened.admit_source_rights(listing_reference_rights(
        source_id.clone(),
        source_payload_set_digest,
    )?)?;
    let reader = ListingReferenceReadCapability::new(&reopened, dataset.clone(), source_id.clone());
    let authority = Arc::new(Mutex::new(reopened));
    let current = reader
        .current(deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(
        current.generation_digest(),
        inserted.generation().generation_digest()
    );
    assert_eq!(current.generation_sequence(), 1);
    assert_eq!(current.record_count(), 2);
    let reopened_memberships = reader.memberships(
        ListingReferenceGenerationSelection::AsOf(inserted.generation().published_at()),
        None,
        2,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(reopened_memberships.records(), exact_as_of.records());
    // A new authorization check must not invalidate an unchanged point-in-time selection.
    assert_ne!(
        reopened_memberships.receipt().authorization_checked_at(),
        exact_as_of.receipt().authorization_checked_at()
    );
    assert_ne!(
        reopened_memberships.receipt().receipt_digest(),
        exact_as_of.receipt().receipt_digest()
    );
    assert!(
        reopened_memberships
            .receipt()
            .same_selection(exact_as_of.receipt())
    );
    assert!(
        !reopened_memberships
            .receipt()
            .same_selection(empty_as_of.receipt())
    );
    let partial_replay = reader.memberships(
        ListingReferenceGenerationSelection::AsOf(inserted.generation().published_at()),
        None,
        1,
        deadline(),
        &cancellation,
    )?;
    assert!(
        !reopened_memberships
            .receipt()
            .same_selection(partial_replay.receipt())
    );
    assert_eq!(
        reopened_memberships.receipt().ordered_rows_digest(),
        exact_as_of.receipt().ordered_rows_digest()
    );
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
fn repository_instrument_company_security_identity_is_point_in_time_and_parent_bound() -> TestResult
{
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("market-data-instruments"))?;
    let database = paths.catalog()?.path().to_path_buf();
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let instrument_id: InstrumentId = "00000000-0000-0000-0000-000000000101".parse()?;
    let other_id: InstrumentId = "00000000-0000-0000-0000-000000000102".parse()?;

    let initial =
        market_data_definition(instrument_id, 10, None, "Apple Incorporated", "AAPL.US", 31)?;
    assert!(matches!(
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone()], 2),
        Err(MarketDataInstrumentCatalogError::PartialBatch {
            expected: 2,
            actual: 1
        })
    ));

    let company_source = local_source("company-security-source-v1", 40)?;
    let company_payload = digest(41);
    let company = company_identity_observation(
        company_source.source_id().clone(),
        company_payload,
        "Apple Incorporated",
        "AAPL",
        100,
    )?;
    let mut company_value = serde_json::to_value(&company)?;
    company_value["associations"] = serde_json::json!([
        {"ticker":"AAPL","exchange":"XNAS"},
        {"ticker":"AAPLB","exchange":"XNAS"},
        {"ticker":"AAPLP","exchange":"XNAS"}
    ]);
    let company: CompanyIdentityObservation = serde_json::from_value(company_value)?;
    let company_json = serde_json::to_string(&company)?;
    let company_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(company_json.as_bytes()).into(),
    );
    let catalog = CatalogAuthority::open(config.clone())?;
    catalog.register_source(&company_source, Timestamp::from_unix_nanos(10))?;
    let company_rights = catalog.admit_source_rights(test_rights_input(
        company_source.source_id().clone(),
        company_payload,
        i64::MAX,
    )?)?;
    let company_reservation = catalog.reserve_ingest(
        &IngestIdentity::try_new(
            company_source.source_id().clone(),
            company_payload,
            SourceOperation::Persist,
            "sec:company:0000320193:v1",
        )?,
        &company_rights,
    )?;
    let company_artifact = ArtifactRecord::try_new(
        "company/apple/part-0001.parquet",
        digest(42),
        128,
        shift_timestamp(company_reservation.requested_at(), 1)?,
    )?;
    let company_manifest = DatasetManifestRecord::try_new(
        SourceIdentifier::try_from("sec-apple-company-identity")?,
        SchemaVersion::CURRENT,
        company_artifact.artifact_id(),
        digest(43),
        shift_timestamp(company_reservation.requested_at(), 2)?,
    );
    catalog.publish_artifact_manifest(
        &company_reservation,
        std::slice::from_ref(&company_artifact),
        &company_manifest,
    )?;
    catalog.complete_ingest(&company_reservation, ContractCompletion::Succeeded)?;
    drop(catalog);
    seed_company_identity_observation(
        &database,
        &company,
        company_reservation.run_id(),
        company_manifest.manifest_id(),
    )?;

    // The same issuer retains independent company-facts and filing-acquisition parents.
    // A newer filing companion must not replace the actual submissions corroboration.
    let mut financial_parents = Vec::new();
    for (surface, byte) in [
        (CompanyIdentitySurface::SecCompanyFacts, 101),
        (CompanyIdentitySurface::SecFilingXbrl, 102),
    ] {
        let catalog = CatalogAuthority::open(config.clone())?;
        let payload = digest(byte);
        let rights = catalog.admit_source_rights(test_rights_input(
            company_source.source_id().clone(),
            payload,
            i64::MAX,
        )?)?;
        let reservation = catalog.reserve_ingest(
            &IngestIdentity::try_new(
                company_source.source_id().clone(),
                payload,
                SourceOperation::Persist,
                surface.database_name(),
            )?,
            &rights,
        )?;
        let artifact = ArtifactRecord::try_new(
            format!(
                "company/apple/{}/part-0001.parquet",
                surface.database_name()
            ),
            digest(byte + 10),
            128,
            shift_timestamp(reservation.requested_at(), 1)?,
        )?;
        let manifest = DatasetManifestRecord::try_new(
            SourceIdentifier::try_from(format!("sec-apple-{}", surface.database_name()))?,
            SchemaVersion::CURRENT,
            artifact.artifact_id(),
            digest(byte + 20),
            shift_timestamp(reservation.requested_at(), 2)?,
        );
        catalog.publish_artifact_manifest(
            &reservation,
            std::slice::from_ref(&artifact),
            &manifest,
        )?;
        catalog.complete_ingest(&reservation, ContractCompletion::Succeeded)?;
        drop(catalog);
        let mut value = serde_json::to_value(&company)?;
        value["surface"] = serde_json::to_value(surface)?;
        value["parent_ingest_payload_evidence"] =
            serde_json::to_value(ExactPayloadEvidence::from_content_digest(payload))?;
        let parent: CompanyIdentityObservation = serde_json::from_value(value)?;
        let parent_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(serde_json::to_vec(&parent)?).into(),
        );
        seed_company_identity_observation(
            &database,
            &parent,
            reservation.run_id(),
            manifest.manifest_id(),
        )?;
        financial_parents.push((parent, parent_digest));
    }

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(
        Arc::clone(&authority),
        Instant::now() + Duration::from_secs(2),
        &CancellationToken::new(),
    )?;
    let relationship_publisher =
        CompanySecurityLinkPublicationCapability::new(Arc::clone(&authority));
    let relationship_reader = CompanySecurityIdentityReadCapability::new(
        &*authority
            .try_lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)?,
    );
    let cancellation = CancellationToken::new();
    let deadline = || Instant::now() + Duration::from_secs(2);
    assert!(
        reader
            .latest(instrument_id, deadline(), &cancellation)?
            .is_none()
    );

    let inserted = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone()], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((inserted.inserted(), inserted.replayed()), (1, 0));
    let retained = reader
        .latest(instrument_id, deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(retained.revision_sequence(), 1);
    assert!(retained.matches_search_query_at("aapl", Timestamp::from_unix_nanos(10))?);
    assert!(retained.matches_search_query_at("apple", Timestamp::from_unix_nanos(10))?);
    assert_eq!(
        retained.display_symbol_at(Timestamp::from_unix_nanos(10)),
        Some("AAPL")
    );
    assert!(!retained.matches_search_query_at("aapl", Timestamp::from_unix_nanos(9))?);
    let unique_before_competitor = reader.resolve_exact_as_of(
        "AAPL",
        retained.published_at(),
        Timestamp::from_unix_nanos(10),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(unique_before_competitor.matches().len(), 1);
    assert!(!unique_before_competitor.has_more());
    assert_eq!(
        unique_before_competitor.matches()[0]
            .record()
            .definition()
            .instrument_id(),
        instrument_id
    );
    assert_eq!(
        unique_before_competitor.knowledge_at(),
        Some(retained.published_at())
    );
    assert_eq!(
        unique_before_competitor.effective_at(),
        Some(Timestamp::from_unix_nanos(10))
    );
    // Unrelated prefix matches can fill discovery without making an exact stock symbol
    // ambiguous. This is the boundary exercised after a full option chain enters the catalog.
    let prefix_count = market_squawk_data::MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS;
    let mut prefixed = Vec::new();
    for index in 0..prefix_count {
        let id = InstrumentId::try_from(uuid::Uuid::from_u128(4096 + u128::try_from(index)?))?;
        let symbol = format!("AAPL-PREFIX-{index}");
        let mut definition = serde_json::to_value(market_data_definition(
            id,
            10,
            None,
            "Unrelated prefix fixture",
            &symbol,
            91,
        )?)?;
        definition["venue_mappings"][0]["venue_symbol"] = serde_json::json!(symbol);
        definition["identifiers"] = serde_json::json!([]);
        prefixed.push(serde_json::from_value(definition)?);
    }
    publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(prefixed, prefix_count)?,
        deadline(),
        &cancellation,
    )?;
    let prefix_knowledge_at = reader
        .latest(
            InstrumentId::try_from(uuid::Uuid::from_u128(4096))?,
            deadline(),
            &cancellation,
        )?
        .ok_or(CatalogError::InvalidRecord)?
        .published_at();
    assert!(
        reader
            .search("AAPL", prefix_count, deadline(), &cancellation)?
            .has_more()
    );
    let unique_with_prefixes = reader.resolve_exact_as_of(
        "AAPL",
        prefix_knowledge_at,
        Timestamp::from_unix_nanos(10),
        deadline(),
        &cancellation,
    )?;
    assert!(!unique_with_prefixes.has_more());
    assert_eq!(unique_with_prefixes.matches().len(), 1);
    assert_eq!(unique_with_prefixes.matches()[0].record(), &retained);
    let provider_identity_query = MarketDataProviderIdentityQuery::try_new(
        SourceId::try_from("nasdaq-symbol-directory")?,
        ProviderInstrumentId::try_from("AAPL.US")?,
        retained.published_at(),
        Timestamp::from_unix_nanos(10),
    )?;
    let exact_provider_identity = reader.resolve_provider_identity_as_of(
        provider_identity_query.clone(),
        deadline(),
        &cancellation,
    )?;
    let MarketDataProviderIdentityResolutionOutcome::Exact(exact_provider_receipt) =
        exact_provider_identity.outcome()
    else {
        return Err("expected exact provider identity".into());
    };
    assert_eq!(exact_provider_receipt.instrument_id(), instrument_id);
    assert_eq!(
        exact_provider_receipt.definition_revision_digest(),
        retained.revision_digest()
    );
    assert_eq!(
        exact_provider_receipt.definition_published_at(),
        retained.published_at()
    );
    assert_eq!(
        exact_provider_receipt.provider_identity_payload_digest(),
        digest(34)
    );
    assert!(exact_provider_receipt.matching_venues().is_empty());
    assert_ne!(exact_provider_identity.receipt_digest().bytes(), [0; 32]);
    let exact_provider_selection = reader
        .select_provider_identity_as_of(provider_identity_query.clone(), deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert_eq!(
        exact_provider_selection.query(),
        exact_provider_identity.query()
    );
    assert_eq!(
        exact_provider_selection.exact_receipt()?,
        exact_provider_receipt
    );
    assert_eq!(
        exact_provider_selection.resolution_receipt_digest(),
        exact_provider_identity.receipt_digest()
    );
    assert_ne!(exact_provider_selection.selection_digest().bytes(), [0; 32]);
    let missing_provider_identity = reader.resolve_provider_identity_as_of(
        MarketDataProviderIdentityQuery::try_new(
            SourceId::try_from("nasdaq-symbol-directory")?,
            ProviderInstrumentId::try_from("MISSING")?,
            retained.published_at(),
            Timestamp::from_unix_nanos(10),
        )?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        missing_provider_identity.outcome(),
        &MarketDataProviderIdentityResolutionOutcome::Missing
    );

    let competing_definition = market_data_definition(
        other_id,
        10,
        Some(20),
        "Apple Depositary Interest",
        "AAPL.US",
        51,
    )?;
    let competing_publication = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![competing_definition], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        (
            competing_publication.inserted(),
            competing_publication.replayed()
        ),
        (1, 0)
    );
    let competing_record = reader
        .latest(other_id, deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    let ambiguous_after_competitor = reader.resolve_exact_as_of(
        "AAPL",
        competing_record.published_at(),
        Timestamp::from_unix_nanos(10),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(ambiguous_after_competitor.matches().len(), 2);
    assert!(!ambiguous_after_competitor.has_more());
    assert_eq!(
        ambiguous_after_competitor
            .matches()
            .iter()
            .map(|candidate| candidate.record().definition().instrument_id())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([instrument_id, other_id])
    );
    let ambiguous_provider_identity_query = MarketDataProviderIdentityQuery::try_new(
        SourceId::try_from("nasdaq-symbol-directory")?,
        ProviderInstrumentId::try_from("AAPL.US")?,
        competing_record.published_at(),
        Timestamp::from_unix_nanos(10),
    )?;
    let ambiguous_provider_identity = reader.resolve_provider_identity_as_of(
        ambiguous_provider_identity_query.clone(),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        ambiguous_provider_identity.outcome(),
        &MarketDataProviderIdentityResolutionOutcome::Ambiguous
    );
    assert!(
        reader
            .select_provider_identity_as_of(
                ambiguous_provider_identity_query,
                deadline(),
                &cancellation,
            )?
            .is_none()
    );
    let canonical_population = MarketDataInstrumentPopulationQuery::try_new(
        vec![other_id, instrument_id],
        retained.published_at(),
        Timestamp::from_unix_nanos(10),
    )?;
    assert_eq!(
        canonical_population,
        MarketDataInstrumentPopulationQuery::try_new(
            vec![instrument_id, other_id],
            retained.published_at(),
            Timestamp::from_unix_nanos(10),
        )?
    );
    assert!(matches!(
        MarketDataInstrumentPopulationQuery::try_new(
            vec![instrument_id, instrument_id],
            retained.published_at(),
            Timestamp::from_unix_nanos(10),
        ),
        Err(MarketDataInstrumentCatalogError::InvalidPopulationQuery)
    ));
    let link = CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
        schema_version: SchemaVersion::CURRENT,
        company_source_id: company.source_id().clone(),
        provider_company_id: company.provider_company_id().clone(),
        company_surface: company.surface(),
        company_observation_digest: company_digest,
        instrument_id,
        market_instrument_revision_digest: retained.revision_digest(),
        security_kind: CompanySecurityKind::CommonEquity,
        relationship_kind: CompanySecurityRelationshipKind::Issuer,
        common_equity_suitability: CommonEquitySuitability::SuitableIssuerCommonEquity,
        resolution_basis: CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk {
            authority_source_id: SourceId::try_from("sec-authoritative-security-reference")?,
            authority_revision: SourceIdentifier::try_from("sec-security-reference-31")?,
            evidence: ExactPayloadEvidence::with_version_pinned_locator(
                digest(35),
                VersionPinnedSourceLocator::new(
                    SourceIdentifier::try_from("sec-filing-security-record-31")?,
                    SourceIdentifier::try_from("sec-security-reference-31")?,
                ),
            ),
        },
        relationship_evidence_rights: IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("sec-reference-personal-use-v1")?,
            IdentifierEntitlement::LicensedInternalUse,
            SourceIdentifier::try_from("https://www.sec.gov/files/company_tickers_exchange.json")?,
        ),
        effective_interval: EffectiveInterval::new(Timestamp::from_unix_nanos(100), None)?,
        available_at: Timestamp::from_unix_nanos(100),
        ingested_at: Timestamp::from_unix_nanos(101),
        transition: CompanySecurityLinkTransition::Initial,
    })?;
    // Contention must honor the request deadline, not masquerade as lost authority.
    {
        let _writer = authority.lock().map_err(|_| "catalog writer poisoned")?;
        assert!(matches!(
            relationship_publisher.publish(
                link.clone(),
                Instant::now() + Duration::from_millis(2),
                &cancellation,
            ),
            Err(market_squawk_data::CompanySecurityIdentityCatalogError::DeadlineExceeded)
        ));
    }
    let relationship = relationship_publisher.publish(link, deadline(), &cancellation)?;
    let query = CompanySecurityIdentityQuery::new(
        company.source_id().clone(),
        company.provider_company_id().clone(),
        company.surface(),
        Some(instrument_id),
        true,
    );
    let before = relationship_reader.as_of(
        &query,
        Timestamp::from_unix_nanos(99),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        before.disposition(),
        CompanySecurityIdentityDisposition::Unavailable
    );
    assert!(before.candidates().is_empty());
    let selected = relationship_reader.as_of(
        &query,
        relationship.record().published_at(),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        selected.disposition(),
        CompanySecurityIdentityDisposition::Complete
    );
    assert_eq!(
        selected.candidates()[0].link_digest(),
        relationship.record().link_digest()
    );
    assert_eq!(
        selected.receipt().ordered_candidates()[0].linked_company_observation_digest(),
        company_digest
    );
    assert_eq!(
        selected.receipt().effective_at(),
        relationship.record().published_at()
    );
    let instrument_selected = relationship_reader.instrument_company_as_of(
        instrument_id,
        company.source_id(),
        company.surface(),
        relationship.record().published_at(),
        CommonEquitySuitability::SuitableIssuerCommonEquity,
        deadline(),
        &cancellation,
    )?;
    for receipt in [
        before.receipt(),
        selected.receipt(),
        instrument_selected.receipt(),
    ] {
        let bytes = receipt.canonical_bytes()?;
        assert_eq!(
            market_squawk_data::CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(
                &bytes
            )?,
            *receipt
        );
        let encoded_query = serde_json::to_vec(&receipt.query_digest())?;
        let query_start = bytes
            .windows(encoded_query.len())
            .position(|value| value == encoded_query.as_slice())
            .ok_or(CatalogError::InvalidRecord)?;
        let mut corrupted = bytes.clone();
        drop(corrupted.splice(
            query_start..query_start + encoded_query.len(),
            serde_json::to_vec(&digest(5))?,
        ));
        assert!(
            market_squawk_data::CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(
                &corrupted
            )
            .is_err()
        );
    }
    let sec_identity_query = SecFundamentalIdentityQuery::try_new(
        company.source_id().clone(),
        company.provider_company_id().clone(),
        company.surface(),
        company_digest,
        Timestamp::from_unix_nanos(100),
        relationship.record().published_at(),
    )?;
    let sec_identity = relationship_reader.sec_fundamental_identity_as_of(
        &sec_identity_query,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        sec_identity.availability(),
        SecFundamentalIdentityAvailability::Available
    );
    assert_eq!(sec_identity.instrument_id(), Some(instrument_id));
    assert_eq!(
        sec_identity.market_instrument_revision_digest(),
        Some(retained.revision_digest())
    );
    assert_eq!(sec_identity.company_observation_digest(), company_digest);
    let replay = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![initial], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((replay.inserted(), replay.replayed()), (0, 1));
    assert_eq!(
        reader
            .latest(instrument_id, deadline(), &cancellation)?
            .ok_or(CatalogError::InvalidRecord)?,
        retained
    );

    let successor_effective_start =
        shift_timestamp(relationship.record().published_at(), 86_400_000_000_000)?;
    let successor_effective_end = shift_timestamp(successor_effective_start, 86_400_000_000_000)?;
    let expired_alias_end = shift_timestamp(successor_effective_start, 1)?;
    std::thread::sleep(Duration::from_millis(1));
    let successor = market_data_definition_with_provider_identities(
        instrument_id,
        successor_effective_start.unix_nanos(),
        Some(successor_effective_end.unix_nanos()),
        "Apple Inc.",
        33,
        &[
            (
                "AAPL.AAA",
                EffectiveInterval::new(successor_effective_start, Some(expired_alias_end))?,
            ),
            (
                "AAPL.NEW",
                EffectiveInterval::new(successor_effective_start, Some(successor_effective_end))?,
            ),
        ],
    )?;
    let advanced = publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![successor], 1)?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!((advanced.inserted(), advanced.replayed()), (1, 0));
    let future_parent = reader
        .latest(instrument_id, deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    assert!(future_parent.published_at() > retained.published_at());
    assert!(future_parent.published_at() < successor_effective_start);
    // Candidate aliases identify instruments, not the revision to select. Rank every
    // known revision for that instrument so a replaced alias cannot revive its old row.
    let prior_alias = reader
        .select_provider_identity_as_of(
            MarketDataProviderIdentityQuery::try_new(
                SourceId::try_from("nasdaq-symbol-directory")?,
                ProviderInstrumentId::try_from("AAPL.US")?,
                retained.published_at(),
                retained.published_at(),
            )?,
            deadline(),
            &cancellation,
        )?
        .ok_or("missing prior provider alias before successor publication")?;
    assert_eq!(
        prior_alias.exact_receipt()?.definition_revision_digest(),
        retained.revision_digest()
    );
    assert!(
        reader
            .select_provider_identity_as_of(
                MarketDataProviderIdentityQuery::try_new(
                    SourceId::try_from("nasdaq-symbol-directory")?,
                    ProviderInstrumentId::try_from("AAPL.US")?,
                    successor_effective_start,
                    successor_effective_start,
                )?,
                deadline(),
                &cancellation,
            )?
            .is_none()
    );
    let successor_alias = reader
        .select_provider_identity_as_of(
            MarketDataProviderIdentityQuery::try_new(
                SourceId::try_from("nasdaq-symbol-directory")?,
                ProviderInstrumentId::try_from("AAPL.NEW")?,
                successor_effective_start,
                successor_effective_start,
            )?,
            deadline(),
            &cancellation,
        )?
        .ok_or("missing exact successor provider alias")?;
    assert_eq!(
        successor_alias
            .exact_receipt()?
            .definition_revision_digest(),
        future_parent.revision_digest()
    );
    let valid_lower_rank_at = shift_timestamp(expired_alias_end, 1)?;
    assert!(!future_parent.matches_search_query_at("AAPL.AAA", valid_lower_rank_at)?);
    assert!(future_parent.matches_search_query_at("aapl.new", valid_lower_rank_at)?);
    let valid_lower_rank = reader.search_as_of(
        "AAPL.",
        future_parent.published_at(),
        valid_lower_rank_at,
        4,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(valid_lower_rank.matches().len(), 1);
    assert_eq!(valid_lower_rank.matches()[0].matched_value(), "AAPL.NEW");
    let before_successor_query = MarketDataInstrumentPopulationQuery::try_new(
        vec![instrument_id],
        retained.published_at(),
        successor_effective_start,
    )?;
    let before_successor =
        reader.pin_population_as_of(before_successor_query.clone(), deadline(), &cancellation)?;
    assert_eq!(
        before_successor.disposition(),
        MarketDataInstrumentPopulationDisposition::Complete
    );
    assert_eq!(before_successor.records(), std::slice::from_ref(&retained));
    let after_successor_query = MarketDataInstrumentPopulationQuery::try_new(
        vec![instrument_id],
        future_parent.published_at(),
        successor_effective_start,
    )?;
    let after_successor =
        reader.pin_population_as_of(after_successor_query.clone(), deadline(), &cancellation)?;
    assert_eq!(
        after_successor.disposition(),
        MarketDataInstrumentPopulationDisposition::Complete
    );
    assert_eq!(
        after_successor.records(),
        std::slice::from_ref(&future_parent)
    );
    let ended_query = MarketDataInstrumentPopulationQuery::try_new(
        vec![instrument_id],
        future_parent.published_at(),
        successor_effective_end,
    )?;
    let ended = reader.pin_population_as_of(ended_query.clone(), deadline(), &cancellation)?;
    assert_eq!(
        ended.disposition(),
        MarketDataInstrumentPopulationDisposition::Unavailable
    );
    assert!(ended.records().is_empty());
    assert_eq!(ended.exclusions().len(), 1);
    assert_eq!(
        ended.exclusions()[0].reason(),
        MarketDataInstrumentPopulationExclusionReason::NoEffectiveRevision
    );
    let still_current = relationship_reader.current(&query, deadline(), &cancellation)?;
    assert_eq!(
        still_current.disposition(),
        CompanySecurityIdentityDisposition::Complete
    );
    assert_eq!(
        still_current.candidates()[0].link_digest(),
        relationship.record().link_digest()
    );
    assert_eq!(
        still_current.receipt().ordered_candidates()[0].current_market_revision_digest(),
        Some(retained.revision_digest())
    );
    let stale =
        relationship_reader.as_of(&query, successor_effective_start, deadline(), &cancellation)?;
    assert_eq!(
        stale.disposition(),
        CompanySecurityIdentityDisposition::Stale
    );
    assert!(stale.candidates().is_empty());
    assert_eq!(stale.exclusions().len(), 1);
    assert_eq!(
        stale.exclusions()[0].reason(),
        CompanySecurityIdentityExclusionReason::StaleMarketInstrumentParent
    );
    assert_eq!(
        stale.exclusions()[0].record().link_digest(),
        relationship.record().link_digest()
    );
    assert_eq!(
        stale.receipt().ordered_exclusions()[0]
            .0
            .current_market_revision_digest(),
        Some(future_parent.revision_digest())
    );
    let pending_identity_query = SecFundamentalIdentityQuery::try_new(
        company.source_id().clone(),
        company.provider_company_id().clone(),
        company.surface(),
        company_digest,
        successor_effective_start,
        future_parent.published_at(),
    )?;
    assert_eq!(
        relationship_reader
            .sec_fundamental_identity_as_of(&pending_identity_query, deadline(), &cancellation)?
            .availability(),
        SecFundamentalIdentityAvailability::IdentityPending
    );
    let future_revocation =
        CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
            schema_version: SchemaVersion::CURRENT,
            company_source_id: company.source_id().clone(),
            provider_company_id: company.provider_company_id().clone(),
            company_surface: company.surface(),
            company_observation_digest: company_digest,
            instrument_id,
            market_instrument_revision_digest: future_parent.revision_digest(),
            security_kind: CompanySecurityKind::CommonEquity,
            relationship_kind: CompanySecurityRelationshipKind::Issuer,
            common_equity_suitability: CommonEquitySuitability::SuitableIssuerCommonEquity,
            resolution_basis: CompanySecurityResolutionBasis::DirectAuthoritativeCrosswalk {
                authority_source_id: SourceId::try_from("sec-authoritative-security-reference")?,
                authority_revision: SourceIdentifier::try_from("sec-security-reference-33")?,
                evidence: ExactPayloadEvidence::with_version_pinned_locator(
                    digest(37),
                    VersionPinnedSourceLocator::new(
                        SourceIdentifier::try_from("sec-filing-security-record-33")?,
                        SourceIdentifier::try_from("sec-security-reference-33")?,
                    ),
                ),
            },
            relationship_evidence_rights: IdentifierRightsPolicyReference::new(
                SourceIdentifier::try_from("sec-reference-personal-use-v1")?,
                IdentifierEntitlement::LicensedInternalUse,
                SourceIdentifier::try_from(
                    "https://www.sec.gov/files/company_tickers_exchange.json",
                )?,
            ),
            effective_interval: EffectiveInterval::new(
                successor_effective_start,
                Some(successor_effective_end),
            )?,
            available_at: future_parent.published_at(),
            ingested_at: future_parent.published_at(),
            transition: CompanySecurityLinkTransition::Revokes {
                previous_link_digest: relationship.record().link_digest(),
                reason: SourceIdentifier::try_from("future-delisting")?,
            },
        })?;
    let revocation =
        relationship_publisher.publish(future_revocation, deadline(), &cancellation)?;
    let historical_after_future_revocation = relationship_reader.sec_fundamental_identity_as_of(
        &SecFundamentalIdentityQuery::try_new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            company_digest,
            Timestamp::from_unix_nanos(100),
            revocation.record().published_at(),
        )?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        historical_after_future_revocation.availability(),
        SecFundamentalIdentityAvailability::Available
    );
    assert_eq!(
        historical_after_future_revocation
            .relationship()
            .ok_or(CatalogError::InvalidRecord)?
            .link_digest(),
        relationship.record().link_digest()
    );
    let revoked = relationship_reader.sec_fundamental_identity_as_of(
        &SecFundamentalIdentityQuery::try_new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            company_digest,
            successor_effective_start,
            revocation.record().published_at(),
        )?,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        revoked.availability(),
        SecFundamentalIdentityAvailability::Unavailable
    );
    assert_eq!(
        revoked
            .relationship_selection()
            .receipt()
            .ordered_exclusions()[0]
            .0
            .previous_link_digest(),
        Some(relationship.record().link_digest())
    );
    assert_eq!(
        relationship_reader
            .exact(
                relationship.record().link_digest(),
                deadline(),
                &cancellation
            )?
            .ok_or(CatalogError::InvalidRecord)?
            .link_digest(),
        relationship.record().link_digest()
    );
    let provider_match = reader.search("AAPL.NEW", 4, deadline(), &cancellation)?;
    assert_eq!(provider_match.matches().len(), 1);
    assert_eq!(
        provider_match.matches()[0].match_kind(),
        MarketDataInstrumentMatchKind::ProviderSymbol
    );
    assert!(
        !serde_json::to_string(provider_match.matches()[0].record().definition())?
            .contains("execution")
    );
    let historical_provider_alias = reader.resolve_exact_as_of(
        "AAPL.US",
        future_parent.published_at(),
        successor_effective_start,
        deadline(),
        &cancellation,
    )?;
    assert!(historical_provider_alias.matches().is_empty());
    assert!(!historical_provider_alias.has_more());
    let current_provider_alias = reader.resolve_exact_as_of(
        "AAPL.NEW",
        future_parent.published_at(),
        successor_effective_start,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(current_provider_alias.matches().len(), 1);
    assert_eq!(
        current_provider_alias.matches()[0]
            .record()
            .definition()
            .instrument_id(),
        instrument_id
    );
    // Automatic issuer authority retains exact submissions/listing/class evidence. Two
    // common share classes may share an issuer; preferred stock must never be promoted.
    let listing_source = listing_reference_source()?;
    let listing_dataset = SourceIdentifier::try_from("company-security-listing-fixture")?;
    let seed = listing_reference_generation(listing_source.clone(), None, 200, 91)?;
    let mut listing_rows = seed.records().to_vec();
    for (row_number, symbol, name, byte) in [
        (3, "AAPLB", "Apple Inc. - Class B Common Stock", 94),
        (4, "AAPLP", "Apple Inc. - Preferred Stock", 95),
    ] {
        listing_rows.push((
            ListingReferenceFileKind::NasdaqListed,
            ListingReferenceRecordInput::try_nasdaq_listed(
                row_number,
                symbol,
                name,
                VenueId::try_from("XNAS")?,
                ListingReferenceMarketCategory::GlobalSelect,
                ListingReferenceFinancialStatus::Normal,
                false,
                false,
                100,
                false,
                SourceIdentifier::try_from(format!("row-{row_number}"))?,
                ExactPayloadEvidence::from_content_digest(digest(byte)),
                "0808202621:31",
                Timestamp::from_unix_nanos(19),
                Timestamp::from_unix_nanos(200),
                seed.files()[0].payload_evidence().clone(),
            )?,
        ));
    }
    let listing_input = ListingReferenceGenerationInput::try_new(
        listing_source.clone(),
        None,
        seed.files().to_vec(),
        listing_rows,
    )?;
    let guard = authority
        .try_lock()
        .map_err(|_| CatalogError::AuthorityLockPoisoned)?;
    guard.register_source(&listing_source, Timestamp::from_unix_nanos(10))?;
    let listing_rights = guard.admit_source_rights(listing_reference_rights(
        listing_source.source_id().clone(),
        listing_input.source_payload_set_digest(),
    )?)?;
    let listing_reader = ListingReferenceReadCapability::new(
        &guard,
        listing_dataset.clone(),
        listing_source.source_id().clone(),
    );
    drop(guard);
    let listing_publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        listing_dataset.clone(),
        listing_source.source_id().clone(),
        listing_rights,
    )?;
    listing_publisher.publish(listing_input, deadline(), &cancellation)?;
    let mut automatic_queries = Vec::new();
    let mut automatic_selections = Vec::new();
    let mut financial_queries = Vec::new();
    let mut financial_selections = Vec::new();
    for (id, symbol, is_preferred, byte) in [
        ("00000000-0000-0000-0000-000000000103", "AAPL", false, 61),
        ("00000000-0000-0000-0000-000000000104", "AAPLB", false, 71),
        ("00000000-0000-0000-0000-000000000105", "AAPLP", true, 81),
    ] {
        let id: InstrumentId = id.parse()?;
        let mut definition =
            serde_json::to_value(market_data_definition(id, 10, None, "Apple", symbol, byte)?)?;
        definition["venue_mappings"][0]["venue_symbol"] = serde_json::json!(symbol);
        publisher.synchronize(
            MarketDataInstrumentSynchronization::try_new(
                vec![serde_json::from_value(definition)?],
                1,
            )?,
            deadline(),
            &cancellation,
        )?;
        let market = reader
            .latest(id, deadline(), &cancellation)?
            .ok_or(CatalogError::InvalidRecord)?;
        let listing = listing_reader
            .exact_current(
                symbol,
                &VenueId::try_from("XNAS")?,
                deadline(),
                &cancellation,
            )?
            .ok_or(CatalogError::InvalidRecord)?;
        let observed_at = market
            .published_at()
            .max(listing.generation().published_at());
        let automatic = CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
            schema_version: SchemaVersion::CURRENT,
            company_source_id: company.source_id().clone(),
            provider_company_id: company.provider_company_id().clone(),
            company_surface: company.surface(),
            company_observation_digest: company_digest,
            instrument_id: id,
            market_instrument_revision_digest: market.revision_digest(),
            security_kind: CompanySecurityKind::CommonEquity,
            relationship_kind: CompanySecurityRelationshipKind::Issuer,
            common_equity_suitability: CommonEquitySuitability::SuitableIssuerCommonEquity,
            resolution_basis: CompanySecurityResolutionBasis::SourceQualifiedListing {
                submissions_observation_digest: company_digest,
                listing_source_id: listing_source.source_id().clone(),
                listing_dataset_id: listing_dataset.clone(),
                listing_generation_digest: listing.generation().generation_digest(),
                listing_file_kind: SourceIdentifier::try_from("nasdaq_listed")?,
                listing_row_number: listing.provider_row_number(),
                listing_record_digest: listing.record_digest(),
                listing_venue: listing.listing_venue().clone(),
                listing_symbol: SourceIdentifier::try_from(symbol)?,
                sec_ticker: SourceIdentifier::try_from(symbol)?,
                sec_exchange: SourceIdentifier::try_from("XNAS")?,
                classification_evidence: listing.record_payload_evidence().clone(),
                ruleset: SourceIdentifier::try_from("sec-submissions-official-common-stock-v1")?,
            },
            relationship_evidence_rights: IdentifierRightsPolicyReference::new(
                SourceIdentifier::try_from("source-qualified-personal-use")?,
                IdentifierEntitlement::LicensedInternalUse,
                SourceIdentifier::try_from(
                    "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
                )?,
            ),
            effective_interval: EffectiveInterval::new(observed_at, None)?,
            available_at: observed_at,
            ingested_at: observed_at,
            transition: CompanySecurityLinkTransition::Initial,
        })?;
        if is_preferred {
            assert!(matches!(relationship_publisher.publish(automatic, deadline(), &cancellation),
                Err(market_squawk_data::CompanySecurityIdentityCatalogError::UnverifiedIdentityAuthority)));
            continue;
        }
        // An exact listing row from a different class cannot substitute for the selected one.
        let mut wrong_class = serde_json::to_value(&automatic)?;
        wrong_class["resolution_basis"]["sec_ticker"] = serde_json::json!("WRONG");
        assert!(
            relationship_publisher
                .publish(
                    serde_json::from_value(wrong_class)?,
                    deadline(),
                    &cancellation
                )
                .is_err()
        );
        let automatic_template = serde_json::to_value(&automatic)?;
        let published = relationship_publisher.publish(automatic, deadline(), &cancellation)?;
        let selected_query = SecFundamentalIdentityQuery::try_new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            company_digest,
            published.record().published_at(),
            published.record().published_at(),
        )?
        .for_instrument(id);
        let guard = authority
            .try_lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)?;
        let selected = relationship_reader.sec_fundamental_identity_as_of(
            &selected_query,
            deadline(),
            &cancellation,
        )?;
        assert_eq!(
            selected.availability(),
            SecFundamentalIdentityAvailability::Available
        );
        assert_eq!(selected.instrument_id(), Some(id));
        assert_eq!(
            relationship_reader
                .exact(published.record().link_digest(), deadline(), &cancellation)?
                .as_ref(),
            Some(published.record())
        );
        assert_eq!(
            relationship_reader
                .exact_company_identity_by_digest(company_digest, deadline(), &cancellation)?
                .ok_or(CatalogError::InvalidRecord)?
                .observation(),
            &company
        );
        let historical = SecFundamentalIdentityQuery::try_new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            company_digest,
            Timestamp::from_unix_nanos(100),
            published.record().published_at(),
        )?
        .for_instrument(id);
        assert_ne!(
            relationship_reader
                .sec_fundamental_identity_as_of(&historical, deadline(), &cancellation)?
                .availability(),
            SecFundamentalIdentityAvailability::Available
        );
        drop(guard);
        automatic_queries.push(selected_query);
        automatic_selections.push(selected);
        if symbol == "AAPL" {
            for (parent, parent_digest) in &financial_parents {
                let mut value = automatic_template.clone();
                value["company_surface"] = serde_json::to_value(parent.surface())?;
                value["company_observation_digest"] = serde_json::to_value(parent_digest)?;
                let published = relationship_publisher.publish(
                    serde_json::from_value(value)?,
                    deadline(),
                    &cancellation,
                )?;
                let query = SecFundamentalIdentityQuery::try_new(
                    company.source_id().clone(),
                    company.provider_company_id().clone(),
                    parent.surface(),
                    *parent_digest,
                    published.record().published_at(),
                    published.record().published_at(),
                )?
                .for_instrument(id);
                let selected = relationship_reader.sec_fundamental_identity_as_of(
                    &query,
                    deadline(),
                    &cancellation,
                )?;
                assert_eq!(
                    selected.availability(),
                    SecFundamentalIdentityAvailability::Available
                );
                assert_eq!(selected.instrument_id(), Some(id));
                assert_eq!(selected.company_observation_digest(), *parent_digest);
                assert!(matches!(
                    published.record().link().resolution_basis(),
                    CompanySecurityResolutionBasis::SourceQualifiedListing {
                        submissions_observation_digest, ..
                    } if *submissions_observation_digest == company_digest
                ));
                financial_queries.push(query);
                financial_selections.push(selected);
            }
            // All three family parents remain available together at the newest cutoff.
            for (surface, expected) in std::iter::once((company.surface(), company_digest)).chain(
                financial_parents
                    .iter()
                    .map(|(parent, digest)| (parent.surface(), *digest)),
            ) {
                let selected = relationship_reader.instrument_company_as_of(
                    id,
                    company.source_id(),
                    surface,
                    financial_queries[1].knowledge_at(),
                    CommonEquitySuitability::SuitableIssuerCommonEquity,
                    deadline(),
                    &cancellation,
                )?;
                assert_eq!(
                    selected.disposition(),
                    CompanySecurityIdentityDisposition::Complete
                );
                assert_eq!(
                    selected.candidates()[0].link().company_observation_digest(),
                    expected
                );
                assert_eq!(
                    market_squawk_data::CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(
                        &selected.receipt().canonical_bytes()?
                    )?,
                    *selected.receipt()
                );
            }
        }
    }
    let issuer_only = SecFundamentalIdentityQuery::try_new(
        company.source_id().clone(),
        company.provider_company_id().clone(),
        company.surface(),
        company_digest,
        automatic_queries[1].knowledge_at(),
        automatic_queries[1].knowledge_at(),
    )?;
    assert_eq!(
        relationship_reader
            .sec_fundamental_identity_as_of(&issuer_only, deadline(), &cancellation)?
            .availability(),
        SecFundamentalIdentityAvailability::IdentityPending
    );
    // A refreshed directory invalidates current automatic authority, but never rewrites
    // an earlier receipt or silently backdates the replacement relationship.
    let previous_listing = listing_reader
        .current(deadline(), &cancellation)?
        .ok_or(CatalogError::InvalidRecord)?;
    listing_publisher.publish(
        listing_reference_generation(
            listing_source.clone(),
            Some(previous_listing.generation_digest()),
            201,
            96,
        )?,
        deadline(),
        &cancellation,
    )?;
    let stale_automatic = relationship_reader.current(
        &CompanySecurityIdentityQuery::new(
            company.source_id().clone(),
            company.provider_company_id().clone(),
            company.surface(),
            automatic_queries[0].instrument_id(),
            true,
        ),
        deadline(),
        &cancellation,
    )?;
    assert_eq!(
        stale_automatic.disposition(),
        CompanySecurityIdentityDisposition::Stale
    );
    assert_eq!(
        stale_automatic.exclusions()[0].reason(),
        CompanySecurityIdentityExclusionReason::StaleResolutionParent
    );
    let stale_bytes = stale_automatic.receipt().canonical_bytes()?;
    assert_eq!(
        market_squawk_data::CompanySecurityIdentitySelectionReceipt::from_canonical_bytes(
            &stale_bytes
        )?,
        *stale_automatic.receipt()
    );
    drop(listing_reader);
    drop(listing_publisher);
    drop(reader);
    drop(publisher);
    drop(relationship_reader);
    drop(relationship_publisher);
    drop(authority);

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config)?));
    let reader = MarketDataInstrumentReadCapability::new(
        Arc::clone(&authority),
        Instant::now() + Duration::from_secs(2),
        &CancellationToken::new(),
    )?;
    let relationship_reader = CompanySecurityIdentityReadCapability::new(
        &*authority
            .try_lock()
            .map_err(|_| CatalogError::AuthorityLockPoisoned)?,
    );
    for (query, expected) in automatic_queries.iter().zip(&automatic_selections) {
        assert_eq!(
            &relationship_reader.sec_fundamental_identity_as_of(
                query,
                deadline(),
                &cancellation
            )?,
            expected
        );
    }
    for (query, expected) in financial_queries.iter().zip(&financial_selections) {
        assert_eq!(
            &relationship_reader.sec_fundamental_identity_as_of(
                query,
                deadline(),
                &cancellation
            )?,
            expected
        );
    }
    let reopened = reader
        .latest(instrument_id, deadline(), &cancellation)?
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
    assert_eq!(
        reader.pin_population_as_of(before_successor_query, deadline(), &cancellation)?,
        before_successor
    );
    assert_eq!(
        reader.pin_population_as_of(after_successor_query, deadline(), &cancellation)?,
        after_successor
    );
    assert_eq!(
        reader.pin_population_as_of(ended_query, deadline(), &cancellation)?,
        ended
    );
    assert_eq!(
        relationship_reader.sec_fundamental_identity_as_of(
            &sec_identity_query,
            deadline(),
            &cancellation
        )?,
        sec_identity
    );
    assert_eq!(
        reader.resolve_exact_as_of(
            "AAPL",
            retained.published_at(),
            Timestamp::from_unix_nanos(10),
            deadline(),
            &cancellation,
        )?,
        unique_before_competitor
    );
    assert_eq!(
        reader.resolve_exact_as_of(
            "AAPL",
            competing_record.published_at(),
            Timestamp::from_unix_nanos(10),
            deadline(),
            &cancellation,
        )?,
        ambiguous_after_competitor
    );
    assert_eq!(
        reader.resolve_exact_as_of(
            "AAPL.US",
            future_parent.published_at(),
            successor_effective_start,
            deadline(),
            &cancellation,
        )?,
        historical_provider_alias
    );
    assert_eq!(
        reader.resolve_exact_as_of(
            "AAPL.NEW",
            future_parent.published_at(),
            successor_effective_start,
            deadline(),
            &cancellation,
        )?,
        current_provider_alias
    );
    assert_eq!(
        reader.verify_provider_identity_restart(
            &exact_provider_identity,
            deadline(),
            &cancellation,
        )?,
        exact_provider_identity
    );
    assert_eq!(
        reader.verify_provider_identity_restart(
            &missing_provider_identity,
            deadline(),
            &cancellation,
        )?,
        missing_provider_identity
    );
    assert_eq!(
        reader.verify_provider_identity_restart(
            &ambiguous_provider_identity,
            deadline(),
            &cancellation,
        )?,
        ambiguous_provider_identity
    );
    assert_eq!(
        reader.verify_provider_identity_selection_restart(
            &exact_provider_selection,
            deadline(),
            &cancellation,
        )?,
        exact_provider_selection
    );
    // Reopening an old selection must return its original revision despite later successors
    // and provider-symbol collisions already retained above.
    assert_eq!(
        reader.read_selected_provider_definition(
            &exact_provider_selection,
            deadline(),
            &cancellation,
        )?,
        retained
    );
    Ok(())
}

#[tokio::test]
async fn alpaca_asset_reference_creates_equity_and_replays_sealed_native_identity() -> TestResult {
    let _tls = market_squawk_platform::install_ring_tls_provider()?;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use market_squawk_adapter_alpaca::{
        ALPACA_ASSET_REFERENCE_ENDPOINT, AlpacaAssetReferenceClient, AlpacaPendingAssetReference,
    };
    use market_squawk_data::{
        AlpacaAssetReferenceAdmission, IngestError, IngestPrecommitAuthority,
    };
    use market_squawk_platform::RawCaptureRecord;
    use market_squawk_sources::{
        ApiEndpointRule, BackoffPolicy, BudgetScope, EndpointPolicy, HttpRequestBounds, PathScope,
        ProviderBudgetPolicy, ProviderCaptureMaterial, ProviderCapturePageReceipt,
        ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    };
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Precommit {
        checks: AtomicUsize,
        revoke_on: usize,
    }
    impl IngestPrecommitAuthority for Precommit {
        fn validate_precommit(&self) -> Result<(), IngestError> {
            if self.checks.fetch_add(1, Ordering::SeqCst) + 1 >= self.revoke_on {
                Err(IngestError::PublicationAuthorityRevoked)
            } else {
                Ok(())
            }
        }
    }
    let allowed = Precommit {
        checks: AtomicUsize::new(0),
        revoke_on: usize::MAX,
    };
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("alpaca-native-equity"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    // Establish the catalog/root authority while empty, before any reference or raw publication.
    drop(AnalyticalDataService::initialize(
        CatalogAuthority::open(config.clone())?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        ObjectStoreConfig::try_new(8 * 1024 * 1024, 32, Duration::from_secs(10))?,
    )?);
    let catalog = CatalogAuthority::open(config.clone())?;
    let listing_source = listing_reference_source()?;
    catalog.register_source(&listing_source, Timestamp::from_unix_nanos(10))?;
    let generation = listing_reference_generation(listing_source.clone(), None, 20, 91)?;
    let rights = catalog.admit_source_rights(listing_reference_rights(
        listing_source.source_id().clone(),
        generation.source_payload_set_digest(),
    )?)?;
    let listing_reader = ListingReferenceReadCapability::new(
        &catalog,
        SourceIdentifier::try_from("nasdaq.symbol-directory.us-listed.v1")?,
        listing_source.source_id().clone(),
    );
    let authority = Arc::new(Mutex::new(catalog));
    let deadline = || Instant::now() + Duration::from_secs(10);
    let cancellation = CancellationToken::new();
    let listing_publisher = ListingReferencePublicationCapability::try_new(
        Arc::clone(&authority),
        SourceIdentifier::try_from("nasdaq.symbol-directory.us-listed.v1")?,
        listing_source.source_id().clone(),
        rights,
    )?;
    listing_publisher.publish(generation, deadline(), &cancellation)?;
    let listing = listing_reader
        .exact_current(
            "AAPL",
            &VenueId::try_from("XNAS")?,
            deadline(),
            &cancellation,
        )?
        .ok_or("missing AAPL listing")?;

    // The reference profile has no canonical IDs or live authority; its only remote resource is
    // authenticated asset metadata. The same profile is accepted by the production client.
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let bounds = HttpRequestBounds::default();
    let provider = SourceIdentifier::try_from("alpaca-market-data")?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(SourceIdentifier::try_from("test-owner-authorized-account")?),
        ExactPayloadEvidence::from_content_digest(digest(151)),
        effective,
    );
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider.clone(), &authorization)?,
        NonZeroU32::new(200).ok_or("nonzero reference request limit")?,
        NonZeroU64::new(60_000_000_000).ok_or("nonzero reference window")?,
        NonZeroU16::new(2).ok_or("nonzero reference concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000_000).ok_or("nonzero initial reference backoff")?,
            NonZeroU64::new(60_000_000_000).ok_or("nonzero maximum reference backoff")?,
            1_000,
        )?,
    )?;
    let source = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("alpaca-basic-asset-reference-v1")?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(SourceIdentifier::try_from("alpaca-assets-test-v1")?),
            ExactPayloadEvidence::from_content_digest(digest(150)),
        ),
        SourceClass::Broker,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            ExactPayloadEvidence::from_content_digest(digest(152)),
            effective,
            vec![AssetClass::Equity, AssetClass::Fund],
            CoverageTopology::partial_venues(vec![VenueId::try_from("iex")?])?,
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::NotApplicable,
            DeliveryEvidence::AuthorizedBroker,
        )?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
            vec![ApiEndpointRule::try_new(
                ALPACA_ASSET_REFERENCE_ENDPOINT,
                PathScope::Descendants,
                vec![],
                1,
                128,
            )?],
            bounds,
        )?),
        FreshnessPolicy::try_new(1, 1, 1, 1, 0)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))?;
    AlpacaAssetReferenceClient::try_new(source.clone(), bounds)?;
    let raw_store = paths.sealed_research_journal_store()?;
    let received_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let native_id = uuid::Uuid::from_u128(701);
    let material = |symbol: &str,
                    exchange: &str,
                    native_id: uuid::Uuid,
                    extra: &str,
                    received_at: Timestamp|
     -> TestResult<ProviderCaptureMaterial> {
        let body = Bytes::from(format!(
            r#"{{"id":"{native_id}","symbol":"{symbol}","exchange":"{exchange}","class":"us_equity","status":"active"{extra}}}"#
        ));
        let body_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
        let request_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(
                format!("https://paper-api.alpaca.markets/v2/assets/{symbol}").as_bytes(),
            )
            .into(),
        );
        let receipt = ProviderCaptureSetReceipt::try_new(
            source.source_id().clone(),
            source.revision().clone(),
            SourceIdentifier::try_from(format!("alpaca:asset-reference:{symbol}"))?,
            request_digest,
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![ProviderCapturePageReceipt::try_new(
                0,
                request_digest,
                None,
                None,
                200,
                u64::try_from(body.len())?,
                body_digest,
                received_at,
            )?],
        )?;
        let connection = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            &receipt.observation_digest().bytes(),
        );
        let record = RawCaptureRecord::try_new_live(
            uuid::Uuid::new_v5(&connection, &body_digest.bytes()),
            Arc::from(source.source_id().as_str()),
            connection,
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
            body,
        )?;
        Ok(ProviderCaptureMaterial::try_new(receipt, vec![record])?)
    };
    let original = material("AAPL", "NASDAQ", native_id, "", received_at)?;
    let original_receipt = original.receipt().clone();
    let original_records = original.records().to_vec();
    let admission = |material: ProviderCaptureMaterial,
                     listing: &market_squawk_data::ListingReferenceRecord|
     -> TestResult<AlpacaAssetReferenceAdmission> {
        let pending =
            AlpacaPendingAssetReference::restore_original(material.receipt(), material.records())?;
        let (rejoin, seal) = pending.into_seal_parts()?;
        let (asset, capture) = rejoin.try_rejoin(seal.seal(&raw_store)?)?;
        let mut rights = test_rights_input(
            source.source_id().clone(),
            capture.persisted_receipt().capture().observation_digest(),
            i64::MAX,
        )?;
        rights.retrieved_at = asset.received_at();
        rights.permitted_operations.push(SourceOperation::Display);
        Ok(AlpacaAssetReferenceAdmission {
            source: source.clone(),
            rights,
            capture,
            asset,
            official_listing: listing.clone(),
            expected_current: None,
        })
    };
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(
        Arc::clone(&authority),
        Instant::now() + Duration::from_secs(2),
        &CancellationToken::new(),
    )?;
    let created = publisher.publish_alpaca_asset_reference(
        admission(original, &listing)?,
        &allowed,
        deadline(),
        &cancellation,
    )?;
    let definition = created.definition();
    let canonical = definition.instrument_id();
    assert_ne!(canonical.as_uuid(), native_id);
    assert_eq!(definition.asset_class(), AssetClass::Equity);
    assert_eq!(definition.quote_currency(), Currency::try_from("USD")?);
    assert_eq!(
        definition
            .quote_currency_evidence()
            .version_pinned_locator()
            .ok_or("missing currency locator")?
            .reference()
            .as_str(),
        "https://docs.alpaca.markets/us/reference/stocksnapshots-1"
    );
    assert_eq!(
        definition
            .display_name()
            .ok_or("missing listing name")?
            .as_str(),
        "Apple Inc. - Common Stock"
    );
    assert_eq!(definition.identifiers().len(), 1);
    assert!(
        matches!(definition.identifiers()[0].identifier(), ExternalIdentifier::Ticker(ticker) if ticker.as_str() == "AAPL")
    );
    assert_eq!(
        definition.provider_identities()[0].source_id(),
        source.source_id()
    );
    assert_eq!(
        definition.provider_identities()[0]
            .provider_instrument_id()
            .as_str(),
        native_id.to_string()
    );
    assert_eq!(definition.venue_mappings().len(), 2);
    assert!(
        definition
            .venue_mappings()
            .iter()
            .any(|mapping| mapping.venue_id().as_str() == "iex"
                && mapping.venue_symbol().as_str() == "AAPL")
    );

    // A different native security cannot take the same official listing or consume a new ID.
    assert!(matches!(
        publisher.publish_alpaca_asset_reference(
            admission(
                material(
                    "AAPL",
                    "NASDAQ",
                    uuid::Uuid::from_u128(702),
                    "",
                    received_at
                )?,
                &listing
            )?,
            &allowed,
            deadline(),
            &cancellation
        ),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));
    let revoked = Precommit {
        checks: AtomicUsize::new(0),
        revoke_on: 3,
    };
    let changed_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    assert!(matches!(
        publisher.publish_alpaca_asset_reference(
            admission(
                material(
                    "AAPL",
                    "NASDAQ",
                    native_id,
                    r#", "name":"changed source body""#,
                    changed_at
                )?,
                &listing
            )?,
            &revoked,
            deadline(),
            &cancellation
        ),
        Err(MarketDataInstrumentCatalogError::PublicationAuthority(_))
    ));
    assert_eq!(revoked.checks.load(Ordering::SeqCst), 3);
    assert_eq!(
        reader.latest(canonical, deadline(), &cancellation)?,
        Some(created.clone())
    );
    // The restoration boundary rejects an original body substituted under another receipt.
    let other = material(
        "AAPL",
        "NASDAQ",
        uuid::Uuid::from_u128(703),
        "",
        received_at,
    )?;
    assert!(
        AlpacaPendingAssetReference::restore_original(&original_receipt, other.records()).is_err()
    );

    // A current ETF listing and sealed native asset establish a non-execution Fund without
    // inventing a CUSIP or issuer link. Reuse the fixture's existing SPY directory membership.
    let fund_listing = listing_reader
        .exact_current(
            "SPY",
            &VenueId::try_from("ARCX")?,
            deadline(),
            &cancellation,
        )?
        .ok_or("missing SPY listing")?;
    let fund_native_id = uuid::Uuid::from_u128(704);
    let fund_original = material("SPY", "ARCA", fund_native_id, "", received_at)?;
    let fund_receipt = fund_original.receipt().clone();
    let fund_records = fund_original.records().to_vec();
    let fund = publisher.publish_alpaca_asset_reference(
        admission(fund_original, &fund_listing)?,
        &allowed,
        deadline(),
        &cancellation,
    )?;
    let fund_definition = fund.definition();
    assert_eq!(fund_definition.asset_class(), AssetClass::Fund);
    assert_ne!(fund_definition.instrument_id().as_uuid(), fund_native_id);
    assert_eq!(
        fund_definition.quote_currency(),
        definition.quote_currency()
    );
    assert_eq!(
        fund_definition.quote_currency_evidence(),
        definition.quote_currency_evidence()
    );
    assert_eq!(fund_definition.identifiers().len(), 1);
    assert!(
        matches!(fund_definition.identifiers()[0].identifier(), ExternalIdentifier::Ticker(ticker) if ticker.as_str() == "SPY")
    );
    assert_eq!(
        fund_definition.provider_identities()[0]
            .provider_instrument_id()
            .as_str(),
        fund_native_id.to_string()
    );
    assert_eq!(fund_definition.venue_mappings().len(), 2);
    for venue in ["ARCX", "iex"] {
        assert!(
            fund_definition
                .venue_mappings()
                .iter()
                .any(|mapping| mapping.venue_id().as_str() == venue
                    && mapping.venue_symbol().as_str() == "SPY")
        );
    }
    assert!(matches!(
        publisher.publish_alpaca_asset_reference(
            admission(
                material("SPY", "ARCA", uuid::Uuid::from_u128(705), "", received_at)?,
                &fund_listing
            )?,
            &allowed,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));

    drop(reader);
    drop(publisher);
    drop(listing_reader);
    drop(listing_publisher);
    drop(authority);
    let reopened = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&reopened));
    let reader = MarketDataInstrumentReadCapability::new(
        Arc::clone(&reopened),
        Instant::now() + Duration::from_secs(2),
        &CancellationToken::new(),
    )?;
    // Replay the original received timestamp, which necessarily preceded catalog publication.
    let retained = ProviderCaptureMaterial::try_new(original_receipt, original_records)?;
    let replay = publisher.publish_alpaca_asset_reference(
        admission(retained, &listing)?,
        &allowed,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(replay, created);
    assert_eq!(
        reader.latest(canonical, deadline(), &cancellation)?,
        Some(created.clone())
    );
    let fund_replay = publisher.publish_alpaca_asset_reference(
        admission(
            ProviderCaptureMaterial::try_new(fund_receipt, fund_records)?,
            &fund_listing,
        )?,
        &allowed,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(fund_replay, fund);
    assert_eq!(
        reader.latest(fund.definition().instrument_id(), deadline(), &cancellation)?,
        Some(fund.clone())
    );
    drop(reader);
    drop(publisher);
    drop(reopened);

    // The option consumer must accept the authenticated extraction namespace that actually
    // owns the underlying UUID. A live quote source cannot substitute for that identity.
    use market_squawk_adapter_alpaca::{
        ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT, AlpacaOptionContractReferenceRequest,
        AlpacaOptionContractReferenceSet, AlpacaPendingOptionContractReferencePage,
    };
    use market_squawk_data::{
        AlpacaOptionReferenceAdmission, DatasetId, OnboardingCatalogCapability,
        ResumedProviderOnboarding,
    };
    use market_squawk_domain::CalendarDate;
    let reopen = || -> TestResult<(AnalyticalDataService, OnboardingCatalogCapability)> {
        let (composition, onboarding) = AnalyticalDataService::open_with_provider_onboarding(
            CatalogAuthority::open(config.clone())?,
            AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
            paths.artifacts()?.clone(),
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 32, Duration::from_secs(10))?,
        )?;
        let (service, _publisher) = composition.into_parts();
        Ok((service, onboarding))
    };
    let (service, onboarding) = reopen()?;
    let capability = onboarding_capability()?;
    onboarding.register_provider_capability(&capability)?;
    let reservation =
        onboarding.reserve_provider_onboarding(&OnboardingReservationRequest::try_new(
            &capability,
            ProviderPublicConfiguration::default(),
            AuthoritySet::try_new(vec![SourceIdentifier::try_from("account.read")?])?,
            SourceIdentifier::try_from("local-user")?,
            SourceIdentifier::try_from("option-publication-replay")?,
            Timestamp::from_unix_nanos(i64::MAX),
            1,
        )?)?;
    let expected_session = onboarding.resume_provider_onboarding(reservation.session_id())?;
    assert_eq!(
        expected_session.lifecycle().state(),
        OnboardingState::UserActionRequired
    );
    assert_eq!(expected_session.next_sequence(), 1);
    let option_source_for = |revision: &str, effective| -> TestResult<SourceMetadata> {
        Ok(SourceMetadata::try_new(SourceMetadataInput::new(
            SchemaVersion::CURRENT,
            SourceId::try_from("alpaca-basic-indicative-option-chain-v1")?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from(revision)?),
                ExactPayloadEvidence::from_content_digest(digest(153)),
            ),
            SourceClass::Broker,
            source.provider().clone(),
            AuthorizationGrant::new(
                source.authorization().mode(),
                source.authorization().basis().clone(),
                source.authorization().evidence().clone(),
                effective,
            ),
            SourceCoverage::try_instrument(
                ExactPayloadEvidence::from_content_digest(digest(154)),
                effective,
                vec![AssetClass::Option],
                CoverageTopology::single_venue(VenueId::try_from("alpaca-indicative-options")?),
                InstrumentCoverage::partial(),
                None,
                CoverageDelay::Delayed(900_000_000_000),
                DeliveryEvidence::Indirect,
            )?,
            DataQuality::DirectUnverified,
            NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_from_api_rules(
                vec![ApiEndpointRule::try_new(
                    ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT,
                    PathScope::Exact,
                    vec![],
                    1,
                    128,
                )?],
                bounds,
            )?),
            source.freshness_policy(),
            source.budget_policy().cloned(),
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
    };
    let option_source = option_source_for("alpaca-option-reference-test-v1", effective)?;
    let option_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let expiration = CalendarDate::new(2027, 1, 15)?;
    let option_request =
        AlpacaOptionContractReferenceRequest::try_new("AAPL".to_owned(), expiration, expiration)?;
    let body = Bytes::from(serde_json::to_vec(&serde_json::json!({
        "option_contracts": [{
            "id": uuid::Uuid::from_u128(706), "symbol": "AAPL270115C00200000",
            "name": "AAPL January 2027 200 Call", "status": "active", "tradable": true,
            "expiration_date": "2027-01-15", "root_symbol": "AAPL",
            "underlying_symbol": "AAPL", "underlying_asset_id": native_id,
            "type": "call", "style": "american", "strike_price": "200",
            "multiplier": "100", "size": "100",
            "deliverables": [{"type": "equity", "symbol": "AAPL", "asset_id": native_id,
                "amount": "100", "allocation_percentage": "100", "settlement_type": "T+1",
                "settlement_method": "CCC", "delayed_settlement": false}]
        }], "next_page_token": null
    }))?);
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let request_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(
        format!("{ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT}?underlying_symbols=AAPL&expiration_date_gte=2027-01-15&expiration_date_lte=2027-01-15&status=active&show_deliverables=true&limit=1000").as_bytes(),
    ).into());
    let capture = ProviderCaptureSetReceipt::try_new(
        option_source.source_id().clone(),
        option_source.revision().clone(),
        SourceIdentifier::try_from("alpaca:option-contract-reference:AAPL:2027-01-15:2027-01-15")?,
        request_digest,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            request_digest,
            None,
            None,
            200,
            u64::try_from(body.len())?,
            body_digest,
            option_at,
        )?],
    )?;
    let connection = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        &capture.observation_digest().bytes(),
    );
    let record = RawCaptureRecord::try_new_live(
        uuid::Uuid::new_v5(&connection, &body_digest.bytes()),
        Arc::from(option_source.source_id().as_str()),
        connection,
        Some(0),
        None,
        DateTime::<Utc>::from_timestamp_nanos(option_at.unix_nanos()),
        body,
    )?;
    let material = ProviderCaptureMaterial::try_new(capture, vec![record])?;
    let mut option_rights = test_rights_input(
        option_source.source_id().clone(),
        material.receipt().observation_digest(),
        i64::MAX,
    )?;
    option_rights.retrieved_at = option_at;
    option_rights
        .permitted_operations
        .push(SourceOperation::Display);
    let context = serde_json::to_vec(&serde_json::json!({
        "version": 1, "source": option_source.source_id(), "revision": option_source.revision(),
        "request": option_request, "request_token": null, "received_at": option_at,
    }))?;
    let lease = service
        .acquire_provider_capture_original_lease(deadline(), &cancellation)
        .await?;
    let (expectation, seal) = material.into_whole_seal_parts();
    let token = expectation
        .try_rejoin(seal.seal(&raw_store)?)?
        .try_into_whole()?;
    let originals = service.retain_option_contract_reference_originals(
        &option_source,
        digest(155),
        &DatasetId::try_from("market_squawk.option_snapshots")?,
        &context,
        vec![(option_at, token, option_rights.clone())],
        &raw_store,
        deadline(),
        &cancellation,
    )?;
    let original = service.reopen_provider_capture_original(
        &originals[0],
        &raw_store,
        deadline(),
        &cancellation,
    )?;
    let pending = AlpacaPendingOptionContractReferencePage::restore_original(
        &context,
        original.original().capture(),
        original.records(),
    )?;
    let (rejoin, seal) = pending.into_seal_parts()?;
    let contracts = Arc::new(AlpacaOptionContractReferenceSet::try_from_pages(vec![
        rejoin.try_rejoin(seal.seal(&raw_store)?)?,
    ])?);
    drop(lease);
    let option_admission = |underlying, namespace| AlpacaOptionReferenceAdmission {
        source: option_source.clone(),
        origin: None,
        rights: vec![option_rights.clone()],
        originals: originals.clone(),
        contracts: Arc::clone(&contracts),
        underlying,
        underlying_asset_namespace: namespace,
    };
    let publisher = service.market_data_instrument_synchronization();
    // Expired current permission must fail before it can replace the retained source.
    let expired_source = option_source_for(
        "alpaca-option-reference-expired-v1",
        EffectiveInterval::new(Timestamp::from_unix_nanos(0), Some(option_at))?,
    )?;
    let mut expired = option_admission(created.clone(), source.source_id().clone());
    expired.source = expired_source.clone();
    assert!(matches!(
        publisher.publish_alpaca_option_references(expired, &allowed, deadline(), &cancellation,),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));
    assert_eq!(
        service.retained_source_metadata(
            expired_source.source_id(),
            expired_source.revision(),
            Timestamp::from_unix_nanos(i64::MAX),
            deadline(),
            &cancellation,
        )?,
        None,
    );
    assert!(matches!(
        publisher.publish_alpaca_option_references(
            option_admission(created.clone(), listing_source.source_id().clone()),
            &allowed,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));
    assert!(matches!(
        publisher.publish_alpaca_option_references(
            option_admission(fund, source.source_id().clone()),
            &allowed,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));
    // Exercise the real service reader while canonical publication holds its writer, both
    // before and during the publication transaction. The committed original remains visible.
    #[derive(Debug)]
    struct ReadOriginalDuringPublication<'a> {
        service: &'a AnalyticalDataService,
        session: &'a ResumedProviderOnboarding,
        original: &'a market_squawk_data::ProviderCaptureOriginalReceipt,
        store: &'a market_squawk_platform::SealedResearchJournalStore,
        checks: AtomicUsize,
        reject_on_check: usize,
    }
    impl IngestPrecommitAuthority for ReadOriginalDuringPublication<'_> {
        fn validate_precommit(&self) -> Result<(), IngestError> {
            Ok(())
        }

        fn validate_catalog_precommit(
            &self,
            catalog: &CatalogAuthority,
        ) -> Result<(), IngestError> {
            // The same canonical replay must work before and inside the option transaction.
            let resumed =
                catalog.resume_provider_onboarding(self.session.reservation().session_id())?;
            assert_eq!(
                resumed.lifecycle().state(),
                self.session.lifecycle().state()
            );
            assert_eq!(resumed.next_sequence(), self.session.next_sequence());
            assert_eq!(
                resumed.public_configuration(),
                self.session.public_configuration()
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            let cancellation = CancellationToken::new();
            assert_eq!(
                self.service
                    .pending_provider_capture_original(
                        self.original.capture().source_id(),
                        deadline,
                        &cancellation,
                    )?
                    .as_ref(),
                Some(self.original),
            );
            assert_eq!(
                self.service
                    .provider_capture_original(
                        self.original.session(),
                        self.original.ordinal(),
                        deadline,
                        &cancellation,
                    )?
                    .as_ref(),
                Some(self.original),
            );
            self.service.require_provider_capture_original_session(
                self.original.capture().source_id(),
                self.original.session(),
                deadline,
                &cancellation,
            )?;
            assert!(matches!(
                self.service.require_provider_capture_original_session(
                    self.original.capture().source_id(),
                    digest(156),
                    deadline,
                    &cancellation,
                ),
                Err(IngestError::ReplayConflict),
            ));
            let reopened = self.service.reopen_provider_capture_original(
                self.original,
                self.store,
                deadline,
                &cancellation,
            )?;
            assert_eq!(reopened.original(), self.original);
            assert_eq!(reopened.records().len(), 1);
            // The read split grants no authority to use an unpublished original as published.
            assert!(matches!(
                self.service.reopen_option_contract_reference_original(
                    self.original,
                    digest(157),
                    self.store,
                    deadline,
                    &cancellation,
                ),
                Err(IngestError::ReplayConflict),
            ));
            let cancelled = CancellationToken::new();
            cancelled.cancel();
            assert!(matches!(
                self.service.pending_provider_capture_original(
                    self.original.capture().source_id(),
                    deadline,
                    &cancelled,
                ),
                Err(IngestError::Cancelled),
            ));
            assert!(matches!(
                self.service.provider_capture_original(
                    self.original.session(),
                    self.original.ordinal(),
                    Instant::now(),
                    &cancellation,
                ),
                Err(IngestError::DeadlineExceeded),
            ));
            if self.checks.fetch_add(1, Ordering::SeqCst) + 1 >= self.reject_on_check {
                Err(IngestError::PublicationAuthorityRevoked)
            } else {
                Ok(())
            }
        }
    }
    let read_during_publication = ReadOriginalDuringPublication {
        service: &service,
        session: &expected_session,
        original: &originals[0],
        store: &raw_store,
        checks: AtomicUsize::new(0),
        reject_on_check: usize::MAX,
    };
    let revoked = ReadOriginalDuringPublication {
        service: &service,
        session: &expected_session,
        original: &originals[0],
        store: &raw_store,
        checks: AtomicUsize::new(0),
        // Reject only after replay succeeds with the newly inserted option still uncommitted.
        reject_on_check: 2,
    };
    assert!(matches!(
        publisher.publish_alpaca_option_references(
            option_admission(created.clone(), source.source_id().clone()),
            &revoked,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::PublicationAuthority(_))
    ));
    assert_eq!(revoked.checks.load(Ordering::SeqCst), 2);
    assert!(
        service
            .market_data_instruments()
            .search("AAPL270115C00200000", 2, deadline(), &cancellation)?
            .matches()
            .is_empty()
    );
    let published = publisher.publish_alpaca_option_references(
        option_admission(created.clone(), source.source_id().clone()),
        &read_during_publication,
        deadline(),
        &cancellation,
    )?;
    assert_eq!(read_during_publication.checks.load(Ordering::SeqCst), 3);
    assert_eq!(published.receipt().inserted(), 1);
    let reader = service.market_data_instruments();
    let matched = reader.search("AAPL270115C00200000", 2, deadline(), &cancellation)?;
    assert_eq!(matched.matches().len(), 1);
    let option = matched.matches()[0].record().clone();
    assert_eq!(published.records(), std::slice::from_ref(&option));
    assert_eq!(option.definition().asset_class(), AssetClass::Option);
    drop(reader);
    drop(publisher);
    drop(onboarding);
    drop(service);

    // A different catalog owner may finish between preflight and writer admission. Reuse the
    // exact retained option graph; contention must wait, then revalidate before replay commits.
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    for revoke_after_wait in [false, true] {
        let precommit = Precommit {
            checks: AtomicUsize::new(0),
            revoke_on: if revoke_after_wait { 2 } else { usize::MAX },
        };
        std::thread::scope(|scope| -> TestResult {
            let guard = authority.lock().map_err(|_| "catalog writer poisoned")?;
            let (started, started_rx) = std::sync::mpsc::sync_channel(1);
            let (finished, finished_rx) = std::sync::mpsc::sync_channel(1);
            let input = option_admission(created.clone(), source.source_id().clone());
            let publisher = &publisher;
            let precommit = &precommit;
            let cancellation = &cancellation;
            let publication = scope.spawn(move || {
                let _ = started.send(());
                let result = publisher.publish_alpaca_option_references(
                    input,
                    precommit,
                    deadline(),
                    cancellation,
                );
                let _ = finished.send(());
                result
            });
            started_rx.recv_timeout(Duration::from_secs(2))?;
            let held = finished_rx.recv_timeout(Duration::from_millis(20));
            drop(guard);
            assert!(matches!(
                held,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ));
            let result = publication
                .join()
                .map_err(|_| "option publication worker panicked")?;
            if revoke_after_wait {
                assert!(matches!(
                    result,
                    Err(MarketDataInstrumentCatalogError::PublicationAuthority(_))
                ));
                assert_eq!(precommit.checks.load(Ordering::SeqCst), 2);
            } else {
                let replayed = result?;
                assert_eq!(replayed.receipt().inserted(), 0);
                assert_eq!(replayed.receipt().replayed(), 1);
            }
            Ok(())
        })?;
    }
    drop(publisher);
    drop(authority);

    let (service, onboarding) = reopen()?;
    let resumed = onboarding.resume_provider_onboarding(reservation.session_id())?;
    assert_eq!(
        resumed.lifecycle().state(),
        expected_session.lifecycle().state()
    );
    assert_eq!(resumed.next_sequence(), expected_session.next_sequence());
    assert_eq!(
        service.market_data_instruments().latest(
            option.definition().instrument_id(),
            deadline(),
            &cancellation,
        )?,
        Some(option.clone())
    );
    let read_after_restart = ReadOriginalDuringPublication {
        service: &service,
        session: &expected_session,
        original: &originals[0],
        store: &raw_store,
        checks: AtomicUsize::new(0),
        reject_on_check: usize::MAX,
    };
    let replayed = service
        .market_data_instrument_synchronization()
        .publish_alpaca_option_references(
            option_admission(created.clone(), source.source_id().clone()),
            &read_after_restart,
            deadline(),
            &cancellation,
        )?;
    assert_eq!(replayed.receipt().inserted(), 0);
    assert_eq!(replayed.receipt().replayed(), 1);
    assert_eq!(read_after_restart.checks.load(Ordering::SeqCst), 3);
    assert_eq!(replayed.into_records(), vec![option.clone()]);

    // Publish a real sealed offline snapshot against the same retained acquisition revision.
    // Doctor renewal is exercised by the source fixture; this proves catalog dependency custody.
    use market_squawk_data::provider_option_market_publication_digest;
    use market_squawk_domain::{
        Money, OptionComponent, OptionComponentState, OptionContractTerms,
        OptionContractTermsInput, OptionSnapshotObservation, OptionSnapshotObservationInput,
        OptionUnderlyingObservation, ProviderChannel, ProviderProduct,
    };
    use market_squawk_sources::{
        OptionMarketBatchDisposition, OptionMarketCompleteness, OptionMarketCompletenessInput,
        OptionMarketCursorState, OptionMarketRequestFilter, OptionMarketRequestScope,
        OptionMarketRequestScopeInput, ProviderNativeLineageImplementation,
        ProviderOptionMarketBatch, ProviderOptionMarketNativeLineageBatch,
        SealedProviderOptionMarketBinding,
    };
    let snapshot_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let snapshot_body =
        Bytes::from_static(br#"{"snapshots":{"AAPL270115C00200000":{}},"next_page_token":null}"#);
    let snapshot_body_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(&snapshot_body).into(),
    );
    let snapshot_request = digest(156);
    let snapshot_capture = ProviderCaptureSetReceipt::try_new(
        option_source.source_id().clone(),
        option_source.revision().clone(),
        SourceIdentifier::try_from("alpaca:option-snapshots:AAPL")?,
        snapshot_request,
        ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
        vec![ProviderCapturePageReceipt::try_new(
            0,
            snapshot_request,
            None,
            None,
            200,
            u64::try_from(snapshot_body.len())?,
            snapshot_body_digest,
            snapshot_at,
        )?],
    )?;
    let snapshot_connection = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        &snapshot_capture.observation_digest().bytes(),
    );
    let snapshot_record = RawCaptureRecord::try_new_live(
        uuid::Uuid::new_v5(&snapshot_connection, &snapshot_body_digest.bytes()),
        Arc::from(option_source.source_id().as_str()),
        snapshot_connection,
        Some(0),
        None,
        DateTime::<Utc>::from_timestamp_nanos(snapshot_at.unix_nanos()),
        snapshot_body.clone(),
    )?;
    let material =
        ProviderCaptureMaterial::try_new(snapshot_capture.clone(), vec![snapshot_record])?;
    let (expectation, seal) = material.into_whole_seal_parts();
    let token = expectation
        .try_rejoin(seal.seal(&raw_store)?)?
        .try_into_whole()?;
    let original_contract = contracts
        .contracts()
        .next()
        .ok_or("missing original contract")?;
    let terms = OptionContractTerms::try_new(OptionContractTermsInput {
        option_instrument_id: option.definition().instrument_id(),
        underlying_instrument_id: created.definition().instrument_id(),
        option_definition_revision: option.revision_digest(),
        underlying_definition_revision: created.revision_digest(),
        provider_instrument_id: ProviderInstrumentId::try_from(original_contract.symbol())?,
        occ_identity: Some(original_contract.occ_identity().clone()),
        expiration: original_contract.expiration(),
        strike: Money::new(
            original_contract.strike(),
            original_contract.quote_currency()?,
        ),
        kind: original_contract.kind(),
        multiplier: original_contract.multiplier(),
        exercise_style: OptionComponent::observed(original_contract.exercise_style().clone(), None),
        settlement: OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None),
    })?;
    fn absent<T>() -> OptionComponent<T> {
        OptionComponent::unavailable(OptionComponentState::ProviderAbsent, None)
    }
    let snapshot = OptionSnapshotObservation::try_new(OptionSnapshotObservationInput {
        terms,
        bid_price: absent(),
        bid_size: absent(),
        ask_price: absent(),
        ask_size: absent(),
        last_price: absent(),
        last_size: absent(),
        mark_price: absent(),
        trade_conditions: absent(),
        volume: absent(),
        open_interest: absent(),
        implied_volatility: absent(),
        delta: absent(),
        gamma: absent(),
        theta: absent(),
        vega: absent(),
        rho: absent(),
        underlying: OptionUnderlyingObservation::try_new(absent(), snapshot_body_digest)?,
    })?;
    let scope = OptionMarketRequestScope::try_new(OptionMarketRequestScopeInput {
        source_id: option_source.source_id().clone(),
        metadata_revision: option_source.revision().clone(),
        dataset: snapshot_capture.dataset().clone(),
        provider_product: ProviderProduct::new(SourceIdentifier::try_from(
            "alpaca-indicative-options",
        )?),
        provider_channel: ProviderChannel::new(SourceIdentifier::try_from("rest")?),
        venue_id: Some(VenueId::try_from("alpaca-indicative-options")?),
        underlying_instrument_id: created.definition().instrument_id(),
        underlying_definition_revision: created.revision_digest(),
        provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
        request_identity: snapshot_request,
        observation_identity: snapshot_capture.observation_digest(),
        entitlement_evidence: digest(157),
        capability_evidence: digest(158),
        available_at: snapshot_at,
        received_at: snapshot_at,
        ingested_at: snapshot_at,
        filter: OptionMarketRequestFilter::try_new(
            Some(market_squawk_sources::OptionExpirationRange::try_new(
                expiration, expiration,
            )?),
            None,
            None,
            vec![],
        )?,
    })?;
    let batch = ProviderOptionMarketBatch::try_snapshots(
        scope,
        OptionMarketCompleteness::try_new(OptionMarketCompletenessInput {
            expected_records: Some(1),
            returned_records: 1,
            missing_records: 0,
            unexpected_records: 0,
            provider_reported_records: None,
            page_count: NonZeroU16::new(1).ok_or("page count")?,
            cursor: OptionMarketCursorState::Exhausted,
            disposition: OptionMarketBatchDisposition::Complete,
        })?,
        vec![snapshot],
    )?;
    let dependencies = contracts.dependencies_for(&batch)?;
    let native = ProviderOptionMarketNativeLineageBatch::try_new(
        ProviderNativeLineageImplementation::AlpacaIndicativeOptionsV1,
        &batch,
        vec![Bytes::from_static(
            br#"{"symbol":"AAPL270115C00200000","snapshot":{}}"#,
        )],
        snapshot_body,
    )?;
    let binding =
        SealedProviderOptionMarketBinding::try_new(token, batch, native, vec![0], dependencies)?;
    let publication_digest = provider_option_market_publication_digest(&binding)?;
    let identity = IngestIdentity::try_new(
        option_source.source_id().clone(),
        publication_digest,
        SourceOperation::Persist,
        "alpaca:option-snapshots:retained-original:v1",
    )?;
    let mut snapshot_rights = option_rights.clone();
    snapshot_rights.payload_digest = publication_digest;
    snapshot_rights.retrieved_at = snapshot_at;
    let snapshot_reservation = service
        .reserve_source_ingest(
            &option_source,
            snapshot_at,
            snapshot_rights,
            &identity,
            &cancellation,
        )
        .await?;
    let snapshot_run = snapshot_reservation.run_id();
    let committed = service
        .ingest_provider_option_market(
            snapshot_reservation,
            DatasetId::try_from("market_squawk.option_snapshots")?,
            binding,
            cancellation.clone(),
            Arc::new(Precommit {
                checks: AtomicUsize::new(0),
                revoke_on: usize::MAX,
            }),
        )
        .await?;
    let published_original = service
        .provider_capture_original(
            originals[0].session(),
            originals[0].ordinal(),
            deadline(),
            &cancellation,
        )?
        .ok_or("published original missing")?;
    assert_eq!(
        published_original.published_binding(),
        Some(publication_digest)
    );
    assert_eq!(published_original.digest(), originals[0].digest());
    assert_eq!(published_original.capture(), originals[0].capture());
    assert_eq!(published_original.physical(), originals[0].physical());
    assert!(
        service
            .pending_provider_capture_original(
                option_source.source_id(),
                deadline(),
                &cancellation,
            )?
            .is_none()
    );
    drop(onboarding);
    drop(service);
    let catalog = CatalogAuthority::open(config.clone())?;
    let retained_binding = catalog
        .catalog()
        .provider_option_market_binding_evidence(publication_digest)?
        .ok_or("option binding missing after restart")?;
    assert_eq!(retained_binding.capture(), &snapshot_capture);
    assert_eq!(retained_binding.canonical_row_count(), 1);
    let [dependency] = retained_binding.reference_dependencies() else {
        return Err("expected one retained original dependency".into());
    };
    assert_eq!(dependency.capture(), originals[0].capture());
    assert_eq!(dependency.physical(), originals[0].physical());
    assert_eq!(dependency.rows().len(), 1);
    assert!(dependency.origin().is_none());
    assert_eq!(
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?
            .for_run(snapshot_run)?
            .ok_or("snapshot manifest missing after restart")?
            .manifest(),
        committed.manifest(),
    );
    drop(catalog);
    let (service, _onboarding) = reopen()?;
    assert_eq!(
        service.provider_capture_original(
            originals[0].session(),
            originals[0].ordinal(),
            deadline(),
            &cancellation,
        )?,
        Some(published_original.clone())
    );

    // This synthetic source cannot manufacture a doctor-renewal origin. Current metadata is
    // admitted at current knowledge time, but unchanged originals still need that typed proof.
    let current_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    let current_source = option_source_for(
        "alpaca-option-reference-current-v1",
        EffectiveInterval::new(current_at, None)?,
    )?;
    let current_admission = || {
        let mut input = option_admission(created.clone(), source.source_id().clone());
        input.source = current_source.clone();
        input
    };
    let revoked = Precommit {
        checks: AtomicUsize::new(0),
        revoke_on: 2,
    };
    let publisher = service.market_data_instrument_synchronization();
    assert!(matches!(
        publisher.publish_alpaca_option_references(
            current_admission(),
            &revoked,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::PublicationAuthority(_))
    ));
    assert_eq!(
        service.retained_source_metadata(
            current_source.source_id(),
            current_source.revision(),
            Timestamp::from_unix_nanos(i64::MAX),
            deadline(),
            &cancellation,
        )?,
        None,
    );
    assert!(matches!(
        publisher.publish_alpaca_option_references(
            current_admission(),
            &allowed,
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::SourceIdentityConflict)
    ));
    assert_eq!(
        service.retained_source_metadata(
            current_source.source_id(),
            current_source.revision(),
            option_at,
            deadline(),
            &cancellation,
        )?,
        None,
    );
    let checked_at = Timestamp::from_unix_nanos(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    )?);
    assert_eq!(
        service.retained_source_metadata(
            current_source.source_id(),
            current_source.revision(),
            checked_at,
            deadline(),
            &cancellation,
        )?,
        Some(current_source.clone()),
    );
    assert_eq!(
        service.retained_source_metadata(
            option_source.source_id(),
            option_source.revision(),
            option_at,
            deadline(),
            &cancellation,
        )?,
        Some(option_source.clone()),
    );
    assert_eq!(
        service
            .reopen_option_contract_reference_original(
                &published_original,
                publication_digest,
                &raw_store,
                deadline(),
                &cancellation,
            )?
            .original(),
        &published_original,
    );
    assert_eq!(
        service.market_data_instruments().latest(
            option.definition().instrument_id(),
            deadline(),
            &cancellation,
        )?,
        Some(option),
    );
    Ok(())
}

#[tokio::test]
async fn native_reference_custody_preserves_prior_identity_and_recovers_original() -> TestResult {
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use market_squawk_data::{
        AcceptedNativeReferenceCapture, MarketDataInstrumentCurrentExpectation,
    };
    use market_squawk_platform::{RawCaptureRecord, SealedResearchRawClaim};
    use market_squawk_sources::{
        CatalogProviderIdentityAuthority, ProviderCaptureMaterial, ProviderCapturePageReceipt,
        ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
        ProviderNativeIdentityRequest, RegistryError,
    };

    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("native-reference-custody"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let initialize = || -> TestResult<AnalyticalDataService> {
        Ok(AnalyticalDataService::initialize(
            CatalogAuthority::open(config.clone())?,
            AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
            paths.artifacts()?.clone(),
            ObjectStoreConfig::try_new(8 * 1024 * 1024, 32, Duration::from_secs(10))?,
        )?)
    };
    let deadline = || Instant::now() + Duration::from_secs(10);
    let now = || -> TestResult<Timestamp> {
        Ok(Timestamp::from_unix_nanos(i64::try_from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        )?))
    };
    let cancellation = CancellationToken::new();
    let service = initialize()?;
    let publisher = service.market_data_instrument_synchronization();
    let reader = service.market_data_instruments();
    let raw_store = Arc::new(paths.sealed_research_journal_store()?);
    let instrument: InstrumentId = "00000000-0000-0000-0000-000000000501".parse()?;
    let initial = market_data_definition(instrument, 10, None, "Apple", "AAPL.OLD", 31)?;
    let unrelated_identity = initial.provider_identities()[0].clone();
    publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![initial.clone()], 1)?,
        deadline(),
        &cancellation,
    )?;
    let prior = reader
        .latest(instrument, deadline(), &cancellation)?
        .ok_or("missing prior")?;

    // Use the same consuming seal/rejoin path as a real standalone HTTP extraction. No value
    // claim or physical receipt is fabricated; the original body is written and verified.
    let source = SourceId::try_from("native-reference-fixture")?;
    let revision = MetadataRevision::new(SourceIdentifier::try_from("native-reference-v1")?);
    let namespace = SourceId::try_from("native-reference-namespace")?;
    let received_at = Timestamp::from_unix_nanos(20);
    let body = Bytes::from_static(br#"{"id":"AAPL.NATIVE","venue":"XNAS","quote_currency":"USD"}"#);
    let source_reference: serde_json::Value = serde_json::from_slice(&body)?;
    let native_symbol = source_reference["id"]
        .as_str()
        .ok_or("missing source native ID")?;
    let native_id = ProviderInstrumentId::try_from(native_symbol)?;
    let native_mapping = VenueMapping::new(
        VenueId::try_from(
            source_reference["venue"]
                .as_str()
                .ok_or("missing source venue")?,
        )?,
        VenueSymbol::try_from(native_symbol)?,
    );
    let body_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
    let material = ProviderCaptureMaterial::try_new(
        ProviderCaptureSetReceipt::try_new(
            source.clone(),
            revision.clone(),
            SourceIdentifier::try_from("native-reference")?,
            digest(141),
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![ProviderCapturePageReceipt::try_new(
                0,
                digest(142),
                None,
                None,
                200,
                u64::try_from(body.len())?,
                body_digest,
                received_at,
            )?],
        )?,
        vec![RawCaptureRecord::try_new_live(
            uuid::Uuid::from_u128(501),
            Arc::from(source.as_str()),
            uuid::Uuid::from_u128(502),
            Some(0),
            None,
            DateTime::<Utc>::from_timestamp_nanos(received_at.unix_nanos()),
            body.clone(),
        )?],
    )?;
    let (expectation, seal) = material.into_whole_seal_parts();
    let capture = expectation
        .try_rejoin(seal.seal(&raw_store)?)?
        .try_into_whole()?;
    let original_claim = capture.persisted_receipt().segment().claim().clone();
    let accepted = AcceptedNativeReferenceCapture::from_extraction_http(
        instrument,
        namespace.clone(),
        native_id.clone(),
        &capture,
    )?;
    let original_coordinate = accepted.coordinate().clone();
    drop(capture);
    // The accepted reference alone must retain physical custody before its catalog commit.
    // Maintenance has no membership edge yet and must not quarantine that live original.
    let recovery = Arc::new(Mutex::new(
        service.create_provider_capture_recovery(Arc::clone(&raw_store))?,
    ));
    loop {
        let turn = service
            .recover_provider_capture_store_turn(Arc::clone(&recovery), deadline(), &cancellation)
            .await?;
        assert!(turn.report().quarantined_objects().is_empty());
        if turn.complete() {
            break;
        }
    }
    drop(recovery);
    let native = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: instrument,
        source_id: namespace.clone(),
        provider_instrument_id: native_id.clone(),
        evidence: ProviderIdentityEvidence::from_content_digest(body_digest),
        source_timestamp: None,
        observed_at: received_at,
        metadata_revision: revision,
        validity: EffectiveInterval::new(received_at, None)?,
        supersedes: None,
    });
    let revised = |base: &MarketDataInstrumentDefinition,
                   start: i64,
                   identities: Vec<ProviderIdentityRecord>|
     -> TestResult<MarketDataInstrumentDefinition> {
        // One current symbol is allowed per venue. The sealed reference supplies this native
        // mapping; subsequent revisions copy and preserve it without adding a duplicate venue.
        let mut mappings = base.venue_mappings().to_vec();
        mappings.retain(|mapping| mapping.venue_id() != native_mapping.venue_id());
        mappings.push(native_mapping.clone());
        Ok(MarketDataInstrumentDefinition::try_new(
            MarketDataInstrumentDefinitionInput {
                instrument_id: base.instrument_id(),
                reference_evidence: base.reference_evidence().clone(),
                effective_interval: EffectiveInterval::new(
                    Timestamp::from_unix_nanos(start),
                    None,
                )?,
                asset_class: base.asset_class(),
                display_name: base.display_name().cloned(),
                quote_currency: base.quote_currency(),
                quote_currency_evidence: base.quote_currency_evidence().clone(),
                venue_mappings: mappings,
                provider_identities: identities,
                identifiers: base.identifiers().to_vec(),
            },
        )?)
    };
    let admitted = revised(&initial, 20, vec![unrelated_identity.clone(), native])?;
    publisher.synchronize_native_references_if_current(
        MarketDataInstrumentSynchronization::try_new(vec![admitted], 1)?,
        vec![MarketDataInstrumentCurrentExpectation::from_record(&prior)],
        vec![accepted],
        deadline(),
        &cancellation,
    )?;
    let admitted = reader
        .latest(instrument, deadline(), &cancellation)?
        .ok_or("missing admitted")?;
    assert!(
        admitted
            .definition()
            .provider_identities()
            .contains(&unrelated_identity)
    );
    assert!(
        admitted
            .definition()
            .venue_mappings()
            .contains(&native_mapping)
    );
    let live_cutoff = now()?;
    let request = ProviderNativeIdentityRequest {
        namespace: namespace.clone(),
        provider_instrument_id: native_id.clone(),
        instrument,
        venue: native_mapping.venue_id().clone(),
        venue_symbol: native_mapping.venue_symbol().clone(),
        knowledge_at: live_cutoff,
        effective_at: live_cutoff,
    };
    let selected = reader.select_current(&request, deadline(), &cancellation)?;
    selected.validate_at(now()?)?;
    publisher.synchronize(
        MarketDataInstrumentSynchronization::try_new(
            vec![market_data_definition(
                "00000000-0000-0000-0000-000000000502".parse()?,
                10,
                None,
                "Other",
                "OTHER",
                51,
            )?],
            1,
        )?,
        deadline(),
        &cancellation,
    )?;
    selected.validate_at(now()?)?;

    // An ordinary successor keeps the exact original identity/custody, while revoking the
    // earlier live token for this canonical instrument only.
    publisher.synchronize_native_references_if_current(
        MarketDataInstrumentSynchronization::try_new(
            vec![revised(
                admitted.definition(),
                30,
                admitted.definition().provider_identities().to_vec(),
            )?],
            1,
        )?,
        vec![MarketDataInstrumentCurrentExpectation::from_record(
            &admitted,
        )],
        Vec::new(),
        deadline(),
        &cancellation,
    )?;
    assert!(matches!(
        selected.validate_at(now()?),
        Err(RegistryError::ProviderIdentitySelectionStale)
    ));
    let successor = reader
        .latest(instrument, deadline(), &cancellation)?
        .ok_or("missing successor")?;
    assert!(
        successor
            .definition()
            .venue_mappings()
            .contains(&native_mapping)
    );
    let unclaimed = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: instrument,
        source_id: namespace.clone(),
        provider_instrument_id: ProviderInstrumentId::try_from("UNCLAIMED")?,
        evidence: ProviderIdentityEvidence::from_content_digest(digest(151)),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(40),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from("unclaimed-v1")?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(40), None)?,
        supersedes: None,
    });
    let mut identities = successor.definition().provider_identities().to_vec();
    identities.push(unclaimed.clone());
    assert!(matches!(
        publisher.synchronize_native_references_if_current(
            MarketDataInstrumentSynchronization::try_new(
                vec![revised(successor.definition(), 40, identities)?],
                1
            )?,
            vec![MarketDataInstrumentCurrentExpectation::from_record(
                &successor
            )],
            Vec::new(),
            deadline(),
            &cancellation,
        ),
        Err(MarketDataInstrumentCatalogError::InvalidInput)
    ));
    assert_eq!(
        reader.latest(instrument, deadline(), &cancellation)?,
        Some(successor.clone())
    );
    assert!(
        reader
            .select_provider_identity_as_of(
                MarketDataProviderIdentityQuery::try_new(
                    namespace.clone(),
                    unclaimed.provider_instrument_id().clone(),
                    now()?,
                    Timestamp::from_unix_nanos(40),
                )?,
                deadline(),
                &cancellation
            )?
            .is_none()
    );
    let query = MarketDataProviderIdentityQuery::try_new(
        namespace,
        native_id,
        successor.published_at(),
        Timestamp::from_unix_nanos(30),
    )?;
    let selection = reader
        .select_provider_identity_as_of(query.clone(), deadline(), &cancellation)?
        .ok_or("missing selection")?;
    let retained = reader
        .native_reference(&selection, deadline(), &cancellation)?
        .ok_or("missing custody")?;
    assert_eq!(retained.coordinate(), &original_coordinate);
    assert_eq!(
        retained.raw_claim(),
        &SealedResearchRawClaim::JournalSegment(original_claim.clone())
    );
    assert_eq!(
        publisher
            .synchronize_native_references_if_current(
                MarketDataInstrumentSynchronization::try_new(
                    vec![successor.definition().clone()],
                    1
                )?,
                vec![MarketDataInstrumentCurrentExpectation::from_record(
                    &successor
                )],
                Vec::new(),
                deadline(),
                &cancellation,
            )?
            .replayed(),
        1
    );
    drop(selected);
    drop(reader);
    drop(publisher);
    drop(service);
    drop(raw_store);

    let service = initialize()?;
    let raw_store = Arc::new(paths.sealed_research_journal_store()?);
    let recovery = Arc::new(Mutex::new(
        service.create_provider_capture_recovery(Arc::clone(&raw_store))?,
    ));
    let mut retained_segments = 0;
    loop {
        let turn = service
            .recover_provider_capture_store_turn(Arc::clone(&recovery), deadline(), &cancellation)
            .await?;
        retained_segments += turn.report().retained_journal_segments();
        assert!(turn.report().quarantined_objects().is_empty());
        if turn.complete() {
            break;
        }
    }
    assert_eq!(retained_segments, 1);
    drop(recovery);
    let reader = service.market_data_instruments();
    let selection = reader
        .select_provider_identity_as_of(query, deadline(), &cancellation)?
        .ok_or("missing restarted selection")?;
    let reopened = reader
        .native_reference(&selection, deadline(), &cancellation)?
        .ok_or("missing restarted custody")?;
    assert_eq!(reopened, retained);
    let SealedResearchRawClaim::JournalSegment(claim) = reopened.raw_claim() else {
        return Err("native extraction lost its original journal claim".into());
    };
    let verified = raw_store.open_verified_claim(claim)?;
    assert_eq!(verified.receipt().claim(), &original_claim);
    assert_eq!(verified.records()[0].payload(), body.as_ref());
    assert_eq!(reopened.received_at(), received_at);
    drop(reader);
    drop(service);

    // The same real reopened custody distinguishes transient contention from terminal authority.
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let reader = MarketDataInstrumentReadCapability::new(
        Arc::clone(&authority),
        Instant::now() + Duration::from_secs(2),
        &CancellationToken::new(),
    )?;
    let cutoff = now()?;
    let request = ProviderNativeIdentityRequest {
        knowledge_at: cutoff,
        effective_at: cutoff,
        ..request
    };
    // Immutable definition/custody reads use the same retained catalog while a publisher
    // owns its writer. Compare exact clocks, digests and exclusions with the pre-write view.
    let immutable_reads = || -> TestResult<_> {
        let selection = reader
            .select_provider_identity_as_of(
                MarketDataProviderIdentityQuery::try_new(
                    request.namespace.clone(),
                    request.provider_instrument_id.clone(),
                    cutoff,
                    cutoff,
                )?,
                deadline(),
                &cancellation,
            )?
            .ok_or("missing immutable provider identity")?;
        Ok((
            reader.latest(instrument, deadline(), &cancellation)?,
            reader.pin_population_as_of(
                MarketDataInstrumentPopulationQuery::try_new(vec![instrument], cutoff, cutoff)?,
                deadline(),
                &cancellation,
            )?,
            reader.enumerate_as_of(cutoff, cutoff, None, 256, deadline(), &cancellation)?,
            reader.search("Apple", 10, deadline(), &cancellation)?,
            reader.search_as_of("Apple", cutoff, cutoff, 10, deadline(), &cancellation)?,
            reader.resolve_exact_as_of("AAPL.NATIVE", cutoff, cutoff, deadline(), &cancellation)?,
            reader.read_selected_provider_definition(&selection, deadline(), &cancellation)?,
            reader.native_reference(&selection, deadline(), &cancellation)?,
        ))
    };
    let before_write = immutable_reads()?;
    assert_eq!(before_write.0.as_ref(), Some(&before_write.6));
    assert_eq!(before_write.7.as_ref(), Some(&retained));
    let guard = authority.lock().map_err(|_| "catalog lock poisoned")?;
    let during_write = immutable_reads();
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_read = reader.latest(instrument, deadline(), &cancelled);
    let expired_read = reader.pin_population_as_of(
        MarketDataInstrumentPopulationQuery::try_new(vec![instrument], cutoff, cutoff)?,
        Instant::now(),
        &cancellation,
    );
    drop(guard);
    assert_eq!(during_write?, before_write);
    assert!(matches!(
        cancelled_read,
        Err(MarketDataInstrumentCatalogError::Cancelled)
    ));
    assert!(matches!(
        expired_read,
        Err(MarketDataInstrumentCatalogError::DeadlineExceeded)
    ));
    // A concurrent publisher owns the real writer mutex. Selection must wait for the short
    // clock/watch fence, then resolve the sealed identity rather than fail on transient Busy.
    std::thread::scope(|scope| -> TestResult {
        let guard = authority.lock().map_err(|_| "catalog lock poisoned")?;
        let (started, started_rx) = std::sync::mpsc::sync_channel(1);
        let (finished, finished_rx) = std::sync::mpsc::sync_channel(1);
        let reader = &reader;
        let request = &request;
        let cancellation = &cancellation;
        let selection = scope.spawn(move || {
            let _ = started.send(());
            let result = reader.select_current(request, deadline(), cancellation);
            let _ = finished.send(());
            result
        });
        started_rx.recv_timeout(Duration::from_secs(2))?;
        let held = finished_rx.recv_timeout(Duration::from_millis(20));
        drop(guard);
        assert!(matches!(
            held,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        selection
            .join()
            .map_err(|_| "identity selection worker panicked")??
            .validate_at(now()?)?;
        Ok(())
    })?;
    // Cancellation during writer admission does not need that writer to release its guard.
    std::thread::scope(|scope| -> TestResult {
        let guard = authority.lock().map_err(|_| "catalog lock poisoned")?;
        let cancelled = CancellationToken::new();
        let work_cancelled = cancelled.clone();
        let (started, started_rx) = std::sync::mpsc::sync_channel(1);
        let (finished, finished_rx) = std::sync::mpsc::sync_channel(1);
        let reader = &reader;
        let request = &request;
        let selection = scope.spawn(move || {
            let _ = started.send(());
            let result = reader.select_current(request, deadline(), &work_cancelled);
            let _ = finished.send(());
            result
        });
        started_rx.recv_timeout(Duration::from_secs(2))?;
        let held = finished_rx.recv_timeout(Duration::from_millis(20));
        cancelled.cancel();
        let result = selection
            .join()
            .map_err(|_| "identity selection worker panicked")?;
        drop(guard);
        assert!(matches!(
            held,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(matches!(
            result,
            Err(RegistryError::ProviderIdentitySelectionCancelled)
        ));
        Ok(())
    })?;
    let guard = authority.lock().map_err(|_| "catalog lock poisoned")?;
    assert!(matches!(
        reader.select_current(
            &request,
            Instant::now() + Duration::from_millis(20),
            &cancellation,
        ),
        Err(RegistryError::ProviderIdentitySelectionDeadlineExceeded)
    ));
    drop(guard);
    reader
        .select_current(&request, deadline(), &cancellation)?
        .validate_at(now()?)?;

    let external = Connection::open(paths.catalog()?.path())?;
    external.execute_batch("BEGIN IMMEDIATE")?;
    let busy = reader.select_current(
        &request,
        Instant::now() + Duration::from_millis(200),
        &cancellation,
    );
    external.execute_batch("ROLLBACK")?;
    assert!(matches!(
        busy,
        Err(RegistryError::ProviderIdentityAuthorityBusy)
    ));
    reader
        .select_current(&request, deadline(), &cancellation)?
        .validate_at(now()?)?;

    #[expect(
        clippy::panic,
        reason = "the regression must distinguish a poisoned mutex from transient contention"
    )]
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = authority
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        panic!("poison the test catalog authority");
    }));
    assert!(poisoned.is_err());
    assert!(matches!(
        reader.select_current(&request, deadline(), &cancellation),
        Err(RegistryError::InvalidAuthorityState)
    ));
    Ok(())
}

fn market_data_definition(
    instrument_id: InstrumentId,
    effective_start: i64,
    effective_end: Option<i64>,
    display_name: &str,
    provider_symbol: &str,
    evidence_byte: u8,
) -> TestResult<MarketDataInstrumentDefinition> {
    let effective = EffectiveInterval::new(
        Timestamp::from_unix_nanos(effective_start),
        effective_end.map(Timestamp::from_unix_nanos),
    )?;
    market_data_definition_with_provider_identities(
        instrument_id,
        effective_start,
        effective_end,
        display_name,
        evidence_byte,
        &[(provider_symbol, effective)],
    )
}

fn market_data_definition_with_provider_identities(
    instrument_id: InstrumentId,
    effective_start: i64,
    effective_end: Option<i64>,
    display_name: &str,
    evidence_byte: u8,
    provider_identities: &[(&str, EffectiveInterval)],
) -> TestResult<MarketDataInstrumentDefinition> {
    let effective = EffectiveInterval::new(
        Timestamp::from_unix_nanos(effective_start),
        effective_end.map(Timestamp::from_unix_nanos),
    )?;
    let exact = |byte| ExactPayloadEvidence::from_content_digest(digest(byte));
    let rights = || -> TestResult<IdentifierRightsPolicyReference> {
        Ok(IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("nasdaq-reference-personal-use-v1")?,
            IdentifierEntitlement::LicensedInternalUse,
            SourceIdentifier::try_from(
                "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
            )?,
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
            provider_identities: provider_identities
                .iter()
                .enumerate()
                .map(|(index, (provider_symbol, validity))| {
                    let index_byte = u8::try_from(index)?;
                    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                        instrument_id,
                        source_id: SourceId::try_from("nasdaq-symbol-directory")?,
                        provider_instrument_id: ProviderInstrumentId::try_from(*provider_symbol)?,
                        evidence: ProviderIdentityEvidence::from_content_digest(digest(
                            evidence_byte.saturating_add(3).saturating_add(index_byte),
                        )),
                        source_timestamp: Some(validity.starts_at()),
                        observed_at: shift_timestamp(validity.starts_at(), 1)?,
                        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                            format!("provider-{evidence_byte}-{index}"),
                        )?),
                        validity: *validity,
                        supersedes: None,
                    }))
                })
                .collect::<TestResult<Vec<_>>>()?,
            identifiers: vec![ExternalIdentifierRecord::new(
                ExternalIdentifierRecordInput {
                    identifier: ExternalIdentifier::Cusip(Cusip::try_from("037833100")?),
                    assignment_verification: AssignmentVerification::VerifiedAssigned,
                    source_id: SourceId::try_from("sec-authoritative-security-reference")?,
                    source_evidence: ExactPayloadEvidence::with_version_pinned_locator(
                        digest(evidence_byte.saturating_add(4)),
                        VersionPinnedSourceLocator::new(
                            SourceIdentifier::try_from(format!(
                                "sec-filing-security-record-{evidence_byte}"
                            ))?,
                            SourceIdentifier::try_from(format!(
                                "sec-security-reference-{evidence_byte}"
                            ))?,
                        ),
                    ),
                    source_timestamp: Some(Timestamp::from_unix_nanos(effective_start)),
                    observed_at: Timestamp::from_unix_nanos(effective_start + 1),
                    validity: effective,
                    rights_policy: IdentifierRightsPolicyReference::new(
                        SourceIdentifier::try_from("sec-reference-personal-use-v1")?,
                        IdentifierEntitlement::LicensedInternalUse,
                        SourceIdentifier::try_from("https://www.sec.gov/Archives/edgar/data")?,
                    ),
                },
            )],
        },
    )?)
}

fn company_identity_observation(
    source_id: SourceId,
    parent_digest: EvidenceDigest,
    name: &str,
    ticker: &str,
    ingested_at: i64,
) -> TestResult<CompanyIdentityObservation> {
    Ok(CompanyIdentityObservation::try_new(
        CompanyIdentityObservationInput {
            schema_version: SchemaVersion::CURRENT,
            source_id,
            provider_company_id: SourceIdentifier::try_from("0000320193")?,
            surface: CompanyIdentitySurface::SecSubmissions,
            conformed_name: name.to_owned(),
            former_names: Vec::new(),
            entity_type: Some("operating".to_owned()),
            sic: Some("3571".to_owned()),
            sic_description: Some("Electronic Computers".to_owned()),
            associations: vec![ProviderReportedSecurityAssociation::try_new(
                ticker, "XNAS",
            )?],
            parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
                parent_digest,
            ),
            identity_payload_evidence: ExactPayloadEvidence::from_content_digest(digest(45)),
            received_at: Timestamp::from_unix_nanos(100),
            availability: AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(
                100,
            )),
            ingested_at: Timestamp::from_unix_nanos(ingested_at),
            quality: DataQuality::OfficialDelayed,
        },
    )?)
}

fn seed_company_identity_observation(
    database: &std::path::Path,
    observation: &CompanyIdentityObservation,
    run_id: uuid::Uuid,
    manifest_id: uuid::Uuid,
) -> TestResult {
    let connection = Connection::open(database)?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let json = serde_json::to_string(observation)?;
    let record_digest: [u8; 32] = Sha256::digest(json.as_bytes()).into();
    connection.execute(
        "INSERT INTO company_identity_observations
         (record_digest, run_id, manifest_id, source_id, source_surface,
          provider_company_id, record_json, received_at_ns, available_at_ns, ingested_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            record_digest,
            run_id.to_string(),
            manifest_id.to_string(),
            observation.source_id().as_str(),
            observation.surface().database_name(),
            observation.provider_company_id().as_str(),
            json,
            observation.received_at().unix_nanos(),
            observation
                .availability()
                .conservative_available_at()
                .map(Timestamp::unix_nanos),
            observation.ingested_at().unix_nanos(),
        ],
    )?;
    let mut ordinal = 0_i64;
    for (kind, value, association_ordinal) in [
        (
            "provider_company_id",
            observation.provider_company_id().as_str(),
            None,
        ),
        ("current_name", observation.conformed_name(), None),
        (
            "ticker",
            observation.associations()[0].ticker(),
            Some(0_i64),
        ),
        (
            "exchange",
            observation.associations()[0].exchange(),
            Some(0_i64),
        ),
    ] {
        connection.execute(
            "INSERT INTO company_identity_search_terms
             (record_digest, ordinal, term_kind, display_value, normalized_value,
              association_ordinal) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                record_digest,
                ordinal,
                kind,
                value,
                value.to_lowercase(),
                association_ordinal
            ],
        )?;
        ordinal += 1;
    }
    Ok(())
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
