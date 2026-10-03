use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use market_squawk_adapter_coinbase::CoinbasePublicProductReference;
use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
    MarketDataInstrumentSynchronizationCapability,
};
use market_squawk_domain::{
    ConnectionGeneration, Currency, ExactPayloadEvidence, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_platform::{
    CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureWriterPolicy,
    LocalAuthorityStateStore, LocalPaths, MemoryCaptureSink,
    initialize_capture_process_infrastructure, raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ProviderNativeIdentityRequest, RegistryError, SessionId,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::super::{
    composition::{ProductionCoinbaseProfile, SupervisorDropCancellation, system_timestamp},
    provider::ProductionSourceProfile,
    supervisor::{
        ProductionSupervisorError, activate_owned_capture, retry_catalog_selection,
        select_catalog_routes,
    },
};
use super::budget_free_metadata;
use super::sink::app_config;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dropping_runtime_owner_cancels_and_reaps_a_blocked_provider_task() -> TestResult {
    let cancellation = CancellationToken::new();
    let provider_cancellation = cancellation.clone();
    let owner = SupervisorDropCancellation::new(cancellation.clone());
    let (admitted, admission_observed) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let mut admitted = Some(admitted);
        retry_catalog_selection(
            Instant::now() + Duration::from_secs(1),
            &provider_cancellation,
            None,
            || {
                if let Some(admitted) = admitted.take() {
                    let _sent = admitted.send(());
                }
                Err(RegistryError::ProviderIdentityAuthorityBusy.into())
            },
        )
        .await
    });
    admission_observed.await?;

    drop(owner);

    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), provider).await??,
        Err(ProductionSupervisorError::Registry(
            RegistryError::ProviderIdentitySelectionCancelled
        ))
    ));
    assert!(cancellation.is_cancelled());

    let cancellation = CancellationToken::new();
    let mut attempts = 0;
    retry_catalog_selection(
        Instant::now() + Duration::from_secs(1),
        &cancellation,
        None,
        || {
            attempts += 1;
            if attempts == 1 {
                Err(RegistryError::ProviderIdentityAuthorityBusy.into())
            } else {
                Ok(())
            }
        },
    )
    .await?;
    assert_eq!(attempts, 2);

    let startup_cancellation = CancellationToken::new();
    let mut attempts = 0;
    assert!(matches!(
        retry_catalog_selection(
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            Some(&startup_cancellation),
            || {
                attempts += 1;
                startup_cancellation.cancel();
                Err(RegistryError::ProviderIdentityAuthorityBusy.into())
            }
        )
        .await,
        Err(ProductionSupervisorError::Registry(
            RegistryError::ProviderIdentitySelectionCancelled
        ))
    ));
    assert_eq!(attempts, 1);

    assert!(matches!(
        retry_catalog_selection(
            Instant::now() + Duration::from_millis(10),
            &cancellation,
            None,
            || { Err(RegistryError::ProviderIdentityAuthorityBusy.into()) }
        )
        .await,
        Err(ProductionSupervisorError::Registry(
            RegistryError::ProviderIdentitySelectionDeadlineExceeded
        ))
    ));
    for terminal in [
        RegistryError::ProviderIdentityAuthorityUnavailable,
        RegistryError::InvalidAuthorityState,
        RegistryError::ProviderIdentityAuthorizationRejected,
        RegistryError::ProviderIdentitySelectionStale,
    ] {
        let mut attempts = 0;
        let failure = retry_catalog_selection(
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            None,
            || {
                attempts += 1;
                Err(terminal.into())
            },
        )
        .await;
        assert_eq!(attempts, 1);
        assert!(
            matches!(failure, Err(ProductionSupervisorError::Registry(error)) if error == terminal)
        );
    }

    // Exercise the production blocking boundary on the sole async worker. The real catalog
    // writer releases only after a peer task runs; an inline selector would consume its deadline.
    let config = app_config()?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let profile = ProductionCoinbaseProfile::try_from(source)?;
    let profile = ProductionSourceProfile::coinbase(profile, source, 64, 32 * 1024 * 1024)?;
    let root = TempDir::new()?;
    let (reader, request, authority) = identity_catalog(root.path(), source)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?
        .with_provider_identity_authority(Arc::new(reader))?;
    let registered = registry.register(
        budget_free_metadata(profile.metadata())?,
        system_timestamp()?,
    )?;
    let (held, writer_held) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || -> Result<(), &'static str> {
        let _guard = authority.lock().map_err(|_| "catalog lock poisoned")?;
        held.send(())
            .map_err(|_| "writer admission receiver closed")?;
        released
            .recv_timeout(Duration::from_secs(3))
            .map_err(|_| "peer did not release catalog writer")?;
        Ok(())
    });
    writer_held.await?;
    let (entered, selection_entered) = tokio::sync::oneshot::channel();
    let selection = tokio::spawn(async move {
        let _sent = entered.send(());
        let result = select_catalog_routes(
            &mut registry,
            &registered,
            &mut [request],
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        );
        (registry, registered, result)
    });
    let peer = tokio::spawn(async move {
        selection_entered
            .await
            .map_err(|_| "selector did not enter")?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        release.send(()).map_err(|_| "catalog writer exited")
    });
    let selected = selection.await;
    let peer_result = peer.await;
    let writer_result = writer.join();
    peer_result??;
    writer_result.map_err(|_| "catalog writer panicked")??;
    let (mut registry, registered, selected) = selected?;
    selected?;
    let session = registry.begin_next_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("selection-after-peer-release")?),
        system_timestamp()?,
    )?;
    registry.end_session(&session, system_timestamp()?)?;
    drop(session);
    drop(registered);
    registry.shutdown()?;
    Ok(())
}

#[test]
fn clean_restart_resumes_exact_metadata_and_advances_registry_generation() -> TestResult {
    let root = TempDir::new()?;
    let first = run_registry_generation(root.path())?;
    let second = run_registry_generation(root.path())?;

    assert_eq!(first, ConnectionGeneration::new(1)?);
    assert_eq!(second, ConnectionGeneration::new(2)?);
    Ok(())
}

#[tokio::test]
async fn activation_failure_keeps_capture_control_and_writer_under_cleanup_ownership() -> TestResult
{
    let config = app_config()?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let profile = ProductionCoinbaseProfile::try_from(source)?;
    let profile = ProductionSourceProfile::coinbase(profile, source, 64, 32 * 1024 * 1024)?;
    let root = TempDir::new()?;
    let (reader, request, _) = identity_catalog(root.path(), source)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?
        .with_provider_identity_authority(Arc::new(reader))?;
    let registered = registry.register(
        budget_free_metadata(profile.metadata())?,
        system_timestamp()?,
    )?;
    registry.record_provider_identities(
        &registered,
        &[request],
        Instant::now() + Duration::from_secs(5),
        &CancellationToken::new(),
    )?;
    let session = registry.begin_next_session(
        &registered,
        SessionId::new(market_squawk_domain::SourceIdentifier::try_from(
            "capture-activation-owner",
        )?),
        system_timestamp()?,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            config.capture_destination_registry_memory_ceiling_bytes(),
        ))?;
    let (_publisher, control, writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(
            config.capture_queue_capacity(),
            config.capture_memory_ceiling_bytes(),
        ),
        capabilities,
    )?;
    let writer = spawn_capture_writer(
        writer,
        MemoryCaptureSink::try_new(
            NonZeroUsize::new(64).ok_or("record bound must be nonzero")?,
            NonZeroUsize::new(16 * 1024 * 1024).ok_or("memory bound must be nonzero")?,
        )?,
        CaptureWriterPolicy::default(),
    )?;
    let mut owned_control = Some(control);
    let mut owned_writer = Some(writer);
    owned_control
        .as_mut()
        .ok_or("capture control missing")?
        .invalidate_current();

    assert!(activate_owned_capture(&mut owned_control, &owned_writer).is_err());
    assert!(owned_control.is_some());
    assert!(owned_writer.is_some());

    drop(owned_control.take());
    let writer = owned_writer.take().ok_or("capture writer missing")?;
    let mut pending = writer.shutdown(config.capture_shutdown());
    let _status = pending.wait_until_deadline().await;
    if !pending.is_worker_terminated() {
        pending.wait_until_terminated().await;
    }
    let _termination = pending.try_reap()?;
    registry.end_session(&session, system_timestamp()?)?;
    Ok(())
}

fn run_registry_generation(root: &std::path::Path) -> TestResult<ConnectionGeneration> {
    let config = app_config()?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let profile = ProductionCoinbaseProfile::try_from(source)?;
    let profile = ProductionSourceProfile::coinbase(profile, source, 64, 32 * 1024 * 1024)?;
    let (reader, request, _) = identity_catalog(root, source)?;
    let paths = LocalPaths::prepare(root)?;
    let provider_rate =
        crate::provider_rate::open_provider_rate_authority(paths.control_root()?.root())?;
    let store = LocalAuthorityStateStore::try_open(
        paths.root().join("authority").join(profile.source_key()),
    )?;
    let mut registry =
        AuthoritativeSourceRegistry::try_new_durable_with_provider_rate(store, provider_rate)?
            .with_provider_identity_authority(Arc::new(reader))?;
    let registered =
        registry.register_or_resume_exact(profile.metadata().clone(), system_timestamp()?)?;
    assert_eq!(registered.revision(), profile.metadata().revision());
    registry.record_provider_identities(
        &registered,
        &[request],
        Instant::now() + Duration::from_secs(5),
        &CancellationToken::new(),
    )?;
    let session = registry.begin_next_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("restart-generation")?),
        system_timestamp()?,
    )?;
    let generation = session.generation();
    registry.end_session(&session, system_timestamp()?)?;
    drop(session);
    drop(registered);
    registry.shutdown()?;
    Ok(generation)
}

// The fixture supplies a product response independently of the configured route. As in the
// benchmark catalog fixture, the real catalog publisher and reader mint session authority.
fn identity_catalog(
    root: &std::path::Path,
    source: &market_squawk_platform::CoinbaseSourceConfig,
) -> TestResult<(
    MarketDataInstrumentReadCapability,
    ProviderNativeIdentityRequest,
    Arc<Mutex<CatalogAuthority>>,
)> {
    let [mapping] = source.instruments() else {
        return Err("fixture requires one Coinbase instrument".into());
    };
    let product = CoinbasePublicProductReference::from_response(
        br#"{"product_id":"BTC-USD","product_type":"SPOT","base_currency_id":"BTC","quote_currency_id":"USD","is_disabled":false,"trading_disabled":false}"#,
        mapping.product(),
    )?;
    let paths = LocalPaths::prepare(root.join("identity-catalog"))?;
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(1)?,
        CatalogResultLimits::try_new(64 * 1024, 1024 * 1024)?,
    )?)?));
    let definition = mapping.definition();
    let namespace = SourceId::try_from(super::super::crypto_reference::COINBASE_NAMESPACE)?;
    let native_id = ProviderInstrumentId::try_from(product.product_id())?;
    let venue = VenueId::try_from("coinbase-exchange")?;
    let symbol = VenueSymbol::try_from(product.product_id())?;
    let validity = source.reference_authorization().effective_interval();
    let revision = MetadataRevision::new(SourceIdentifier::try_from("supervisor-product-fixture")?);
    let evidence = ExactPayloadEvidence::from_content_digest(product.body_digest());
    let identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: definition.instrument_id(),
        source_id: namespace.clone(),
        provider_instrument_id: native_id.clone(),
        evidence: ProviderIdentityEvidence::from_content_digest(product.body_digest()),
        source_timestamp: None,
        observed_at: validity.starts_at(),
        metadata_revision: revision.clone(),
        validity,
        supersedes: None,
    });
    let catalog_definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: definition.instrument_id(),
            reference_evidence: RevisionBoundPayloadEvidence::new(revision, evidence.clone()),
            effective_interval: validity,
            asset_class: definition.asset_class(),
            display_name: None,
            quote_currency: Currency::try_from(product.quote_currency())?,
            quote_currency_evidence: evidence,
            venue_mappings: vec![VenueMapping::new(venue.clone(), symbol.clone())],
            provider_identities: vec![identity],
            identifiers: Vec::new(),
        })?;
    MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority)).synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![catalog_definition], 1)?,
        Instant::now() + Duration::from_secs(5),
        &CancellationToken::new(),
    )?;
    let at = system_timestamp()?;
    Ok((
        MarketDataInstrumentReadCapability::new(
            Arc::clone(&authority),
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )?,
        ProviderNativeIdentityRequest {
            namespace,
            provider_instrument_id: native_id,
            instrument: definition.instrument_id(),
            venue,
            venue_symbol: symbol,
            knowledge_at: at,
            effective_at: at,
        },
        authority,
    ))
}
