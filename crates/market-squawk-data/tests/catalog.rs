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
    for (offset, event) in ordinary_events.into_iter().enumerate() {
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
    assert_eq!(
        catalog.append_provider_onboarding_event(&reservation, 6, activate.clone())?,
        OnboardingAppendOutcome::Inserted
    );
    assert_eq!(
        catalog.append_provider_onboarding_event(&reservation, 6, activate)?,
        OnboardingAppendOutcome::Replay
    );
    let zero_event_request = OnboardingReservationRequest::try_new(
        &capability,
        ProviderPublicConfiguration::default(),
        requested.clone(),
        SourceIdentifier::try_from("local-user")?,
        SourceIdentifier::try_from("reserved-without-events")?,
        Timestamp::from_unix_nanos(i64::MAX),
        1,
    )?;
    let zero_event_reservation = catalog.reserve_provider_onboarding(&zero_event_request)?;
    let zero_event_session_id = zero_event_reservation.session_id();
    assert_eq!(
        zero_event_reservation.initial_state(),
        OnboardingState::UserActionRequired
    );
    let wall_now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let late_deadline = wall_now
        .checked_add(Duration::from_secs(1))
        .and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(CatalogError::InvalidRecord)?;
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

    let legacy = Connection::open(&database)?;
    legacy.execute_batch(
        "BEGIN IMMEDIATE;
         DROP TRIGGER provider_onboarding_stream_heads_checked_update;
         DROP TRIGGER provider_onboarding_stream_heads_immutable_delete;
         DROP TABLE provider_onboarding_stream_heads;
         DELETE FROM schema_migrations WHERE version=22;
         COMMIT;",
    )?;
    let legacy_migration_count: i64 =
        legacy.query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
            row.get(0)
        })?;
    let legacy_head_table: bool = legacy.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM sqlite_schema
             WHERE type='table' AND name='provider_onboarding_stream_heads'
         )",
        [],
        |row| row.get(0),
    )?;
    let legacy_sessions: i64 = legacy.query_row(
        "SELECT COUNT(*) FROM provider_onboarding_sessions",
        [],
        |row| row.get(0),
    )?;
    let legacy_events: i64 = legacy.query_row(
        "SELECT COUNT(*) FROM provider_onboarding_events",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(legacy_migration_count, 21);
    assert!(!legacy_head_table);
    assert_eq!((legacy_sessions, legacy_events), (3, 7));
    drop(legacy);

    let (composition, migrated) = AnalyticalDataService::open_with_provider_onboarding(
        CatalogAuthority::open(config.clone())?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (migrated_analytical, _publisher) = composition.into_parts();
    assert_eq!(migrated.health()?.applied_migrations(), 22);
    let resumed = migrated.resume_provider_onboarding(reservation.session_id())?;
    assert_eq!(resumed.lifecycle().state(), OnboardingState::ActiveScoped);
    assert!(resumed.lifecycle().generation_is_active_scoped(generation));
    assert_eq!(
        resumed.lifecycle().generation_reference(generation),
        Some(&reference)
    );
    assert_eq!(resumed.next_sequence(), 7);
    let late_resumed = migrated.resume_provider_onboarding(late_reservation.session_id())?;
    assert_eq!(late_resumed.next_sequence(), 2);
    let zero_event_resumed = migrated.resume_provider_onboarding(zero_event_session_id)?;
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
    drop(migrated);
    drop(migrated_analytical);

    let (composition, reopened) = AnalyticalDataService::open_with_provider_onboarding(
        CatalogAuthority::open(config)?,
        AnalyticalManifestCatalog::open(paths.catalog()?, 8)?,
        paths.artifacts()?.clone(),
        object_config,
    )?;
    let (_reopened_analytical, _publisher) = composition.into_parts();
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
    assert!(matches!(
        reopened.append_provider_onboarding_event(late_resumed.reservation(), 1, late_event,),
        Err(CatalogError::OnboardingDeadlineExceeded)
    ));
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

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config.clone())?));
    let publisher = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(Arc::clone(&authority));
    let relationship_publisher =
        CompanySecurityLinkPublicationCapability::new(Arc::clone(&authority));
    let relationship_reader = CompanySecurityIdentityReadCapability::new(Arc::clone(&authority));
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
    let valid_lower_rank_at = shift_timestamp(expired_alias_end, 1)?;
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
    drop(reader);
    drop(publisher);
    drop(relationship_reader);
    drop(relationship_publisher);
    drop(authority);

    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config)?));
    let reader = MarketDataInstrumentReadCapability::new(Arc::clone(&authority));
    let relationship_reader = CompanySecurityIdentityReadCapability::new(authority);
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
