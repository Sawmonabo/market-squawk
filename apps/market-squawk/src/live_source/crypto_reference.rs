//! Source-backed public crypto identity admission before live-source registration.
//!
//! Configuration supplies the intended local route. Only a current official public product or
//! instrument snapshot supplies a provider identity. The existing market-data catalog owns the
//! resulting immutable definition and the source registry selects it before network market data.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_coinbase::CoinbasePublicProductReference;
use market_squawk_adapter_kraken::KrakenSpotPairReference;
use market_squawk_data::{
    AcceptedNativeReferenceCapture, MarketDataInstrumentCurrentExpectation,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
    MarketDataInstrumentSynchronizationCapability, MarketDataProviderIdentityQuery,
};
use market_squawk_domain::{
    AssetClass, Currency, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentDefinition, InstrumentId, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp, VenueId, VenueMapping,
    VenueSymbol,
};
use market_squawk_platform::{
    CoinbaseSourceConfig, KrakenSourceConfig, LocalPaths, RECOMMENDED_PUBLIC_BTC_USD_INSTRUMENT_ID,
    SealedResearchJournalStore,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ProviderBudgetPolicy, ProviderNativeIdentityRequest,
    ProviderRateAuthority,
};
use sha2::{Digest as _, Sha256};

use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub(super) const COINBASE_NAMESPACE: &str = "coinbase-advanced-trade";
pub(super) const KRAKEN_NAMESPACE: &str = "kraken-spot-v2";
const COINBASE_VENUE: &str = "coinbase-exchange";
const KRAKEN_VENUE: &str = "kraken";

/// Catalog-backed native requests for the public provider selected by production configuration.
#[derive(Debug)]
pub(super) struct CryptoReferenceSelections {
    pub(super) reader: MarketDataInstrumentReadCapability,
    pub(super) coinbase_requests: Vec<ProviderNativeIdentityRequest>,
    pub(super) kraken_requests: Vec<ProviderNativeIdentityRequest>,
}

pub(crate) struct AcceptedCatalogReference {
    pub(crate) expected: InstrumentDefinition,
    pub(crate) namespace: SourceId,
    pub(crate) native_id: ProviderInstrumentId,
    pub(crate) canonical: InstrumentId,
    pub(crate) venue: VenueId,
    pub(crate) symbol: VenueSymbol,
    pub(crate) quote: Currency,
    pub(crate) evidence: ExactPayloadEvidence,
    pub(crate) validity: EffectiveInterval,
    pub(crate) observed_at: Timestamp,
}

struct SourceReference {
    namespace: SourceId,
    native_id: ProviderInstrumentId,
    canonical: InstrumentId,
    venue: VenueId,
    symbol: VenueSymbol,
    quote: Currency,
    evidence: ExactPayloadEvidence,
    validity: EffectiveInterval,
    observed_at: Timestamp,
}

/// Uses the same REST declaration as public onboarding on `api.coinbase.com`.
/// The live WebSocket profile has a different network authority and its own policy.
pub(super) fn coinbase_reference_budget() -> Result<ProviderBudgetPolicy, CryptoReferenceError> {
    let profiles = market_squawk_sources::built_in_provider_profiles()
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    profiles
        .get("coinbase.public-market-data")
        .and_then(|profile| profile.rate_policy().enforcement_policy())
        .cloned()
        .ok_or(CryptoReferenceError::InvalidEvidence)
}

/// Acquires original governed reference captures, commits accepted catalog identities,
/// and returns the exact requests A1 must select before live market sessions start.
#[allow(
    clippy::too_many_arguments,
    reason = "catalog, source, and rate authorities remain explicit"
)]
pub(super) async fn synchronize_public_crypto_reference(
    reader: MarketDataInstrumentReadCapability,
    synchronizer: MarketDataInstrumentSynchronizationCapability,
    paths: &LocalPaths,
    provider_rate: ProviderRateAuthority,
    reference_budget: &ProviderBudgetPolicy,
    capture_store: Arc<SealedResearchJournalStore>,
    coinbase: Option<&CoinbaseSourceConfig>,
    kraken: Option<&KrakenSourceConfig>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CryptoReferenceSelections, CryptoReferenceError> {
    check_operation(deadline, cancellation)?;
    let mut candidates = Vec::new();
    let mut native_captures = Vec::new();
    let mut reused_requests = Vec::new();
    if let Some(config) = coinbase {
        let attestation = config.reference_authorization();
        let endpoints: Vec<String> = config
            .instruments()
            .iter()
            .map(|mapping| {
                format!(
                    "{}/{}",
                    market_squawk_adapter_coinbase::COINBASE_PUBLIC_PRODUCT_ENDPOINT,
                    mapping.product()
                )
            })
            .collect();
        let metadata = super::crypto_reference_transport::metadata(
            super::crypto_reference_transport::COINBASE_REFERENCE_SOURCE,
            COINBASE_VENUE,
            &endpoints,
            AuthorizationGrant::new(
                AuthorizationMode::PublicInterface,
                attestation.basis().clone(),
                attestation.evidence().clone(),
                attestation.effective_interval(),
            ),
            reference_budget,
        )?;
        for mapping in config.instruments() {
            if let Some(reused) = try_reuse_coinbase(
                &reader,
                Arc::clone(&capture_store),
                config,
                mapping.definition(),
                mapping.product(),
                deadline,
                cancellation,
            )
            .await?
            {
                reused_requests.push(reused);
                continue;
            }
            let capture = super::crypto_reference_transport::coinbase_product(
                paths,
                Arc::clone(&capture_store),
                mapping.definition().instrument_id(),
                provider_rate.clone(),
                metadata.clone(),
                mapping.product(),
                deadline,
                cancellation,
            )
            .await?;
            let parsed =
                CoinbasePublicProductReference::from_response(&capture.body, mapping.product())
                    .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
            let observed_at = capture.observed_at;
            let expected = mapping.definition();
            if !config
                .reference_authorization()
                .is_effective_at(observed_at)
                || !config.authorization().is_effective_at(observed_at)
            {
                return Err(CryptoReferenceError::AuthorizationExpired);
            }
            if parsed.quote_currency() != expected.quote_currency().as_str()
                || parsed.product_id() != mapping.product()
                || parsed.product_id().split_once('-')
                    != Some((parsed.base_currency(), parsed.quote_currency()))
                || !expected.venue_mappings().iter().any(|venue| {
                    venue.venue_id().as_str() == COINBASE_VENUE
                        && venue.venue_symbol().as_str() == parsed.product_id()
                })
            {
                return Err(CryptoReferenceError::RouteMismatch);
            }
            if parsed.body_digest() != capture.body_digest {
                return Err(CryptoReferenceError::InvalidEvidence);
            }
            candidates.push(AcceptedCatalogReference {
                expected: expected.clone(),
                namespace: source_id(COINBASE_NAMESPACE)?,
                native_id: provider_id(parsed.product_id())?,
                canonical: expected.instrument_id(),
                venue: venue_id(COINBASE_VENUE)?,
                symbol: venue_symbol(parsed.product_id())?,
                quote: expected.quote_currency(),
                evidence: ExactPayloadEvidence::from_content_digest(capture.body_digest),
                validity: bounded_validity(observed_at, attestation.effective_interval())?,
                observed_at,
            });
            native_captures.push(capture.accepted);
        }
    }
    if let Some(config) = kraken {
        if let Some(reused) = try_reuse_kraken(
            &reader,
            Arc::clone(&capture_store),
            config,
            deadline,
            cancellation,
        )
        .await?
        {
            reused_requests.push(reused);
        } else {
            let attestation = config.reference_authorization();
            let metadata = super::crypto_reference_transport::metadata(
                super::crypto_reference_transport::KRAKEN_REFERENCE_SOURCE,
                KRAKEN_VENUE,
                &[config.endpoint().to_owned()],
                AuthorizationGrant::new(
                    AuthorizationMode::PublicInterface,
                    attestation.basis().clone(),
                    attestation.evidence().clone(),
                    attestation.effective_interval(),
                ),
                reference_budget,
            )?;
            let capture = super::crypto_reference_transport::kraken_instrument(
                paths,
                Arc::clone(&capture_store),
                config.definition().instrument_id(),
                config.symbol(),
                provider_rate,
                metadata,
                config.endpoint(),
                deadline,
                cancellation,
            )
            .await?;
            let parsed = KrakenSpotPairReference::from_snapshot(&capture.body, config.symbol())
                .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
            let observed_at = capture.observed_at;
            let expected = config.definition();
            if !config
                .reference_authorization()
                .is_effective_at(observed_at)
                || !config.authorization().is_effective_at(observed_at)
            {
                return Err(CryptoReferenceError::AuthorizationExpired);
            }
            if parsed.quote() != expected.quote_currency().as_str()
                || parsed.symbol() != config.symbol()
                || parsed.symbol().split_once('/') != Some((parsed.base(), parsed.quote()))
                || !expected.venue_mappings().iter().any(|venue| {
                    venue.venue_id().as_str() == KRAKEN_VENUE
                        && venue.venue_symbol().as_str() == parsed.symbol()
                })
            {
                return Err(CryptoReferenceError::RouteMismatch);
            }
            candidates.push(AcceptedCatalogReference {
                expected: expected.clone(),
                namespace: source_id(KRAKEN_NAMESPACE)?,
                native_id: provider_id(parsed.symbol())?,
                canonical: expected.instrument_id(),
                venue: venue_id(KRAKEN_VENUE)?,
                symbol: venue_symbol(parsed.symbol())?,
                quote: expected.quote_currency(),
                evidence: ExactPayloadEvidence::from_content_digest(capture.body_digest),
                validity: bounded_validity(observed_at, attestation.effective_interval())?,
                observed_at,
            });
            native_captures.push(capture.accepted);
        }
    }
    let mut requests = if candidates.is_empty() {
        Vec::new()
    } else {
        synchronize_accepted_catalog_references(
            reader.clone(),
            synchronizer,
            candidates,
            native_captures,
            deadline,
            cancellation,
        )
        .await?
    };
    requests.extend(reused_requests);
    let mut coinbase_requests = Vec::new();
    let mut kraken_requests = Vec::new();
    for request in requests {
        match request.namespace.as_str() {
            COINBASE_NAMESPACE => coinbase_requests.push(request),
            KRAKEN_NAMESPACE => kraken_requests.push(request),
            _ => return Err(CryptoReferenceError::InvalidEvidence),
        }
    }
    Ok(CryptoReferenceSelections {
        reader,
        coinbase_requests,
        kraken_requests,
    })
}

async fn try_reuse_coinbase(
    reader: &MarketDataInstrumentReadCapability,
    store: Arc<SealedResearchJournalStore>,
    config: &CoinbaseSourceConfig,
    expected: &InstrumentDefinition,
    product: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<ProviderNativeIdentityRequest>, CryptoReferenceError> {
    let now = trusted_now()?;
    if !config.reference_authorization().is_effective_at(now)
        || !config.authorization().is_effective_at(now)
    {
        return Err(CryptoReferenceError::AuthorizationExpired);
    }
    let namespace = source_id(COINBASE_NAMESPACE)?;
    let native_id = provider_id(product)?;
    let query =
        MarketDataProviderIdentityQuery::try_new(namespace.clone(), native_id.clone(), now, now)
            .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    let Some(selection) = reader
        .select_provider_identity_as_of(query, deadline, cancellation)
        .map_err(|error| {
            tracing::warn!(stage = "coinbase_identity_selection", %error, "public crypto catalog operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?
    else {
        return Ok(None);
    };
    let exact = selection
        .exact_receipt()
        .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    if exact.instrument_id() != expected.instrument_id()
        || !exact
            .matching_venues()
            .iter()
            .any(|venue| venue.as_str() == COINBASE_VENUE)
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    let definition = reader
        .read_selected_provider_definition(&selection, deadline, cancellation)
        .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    if definition.definition().asset_class() != AssetClass::Crypto
        || definition.definition().quote_currency() != expected.quote_currency()
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    let retained = reader
        .native_reference(&selection, deadline, cancellation)
        .map_err(|_| CryptoReferenceError::CaptureUnavailable)?
        .ok_or(CryptoReferenceError::CaptureUnavailable)?;
    let original = super::crypto_reference_transport::reopen_original(
        store,
        retained
            .claim()
            .ok_or(CryptoReferenceError::CaptureUnavailable)?
            .clone(),
        market_squawk_adapter_coinbase::MAX_COINBASE_PUBLIC_PRODUCT_BYTES,
        deadline,
        cancellation,
    )
    .await?;
    let parsed = CoinbasePublicProductReference::from_response(&original, product)
        .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
    if parsed.body_digest() != exact.provider_identity_payload_digest()
        || parsed.quote_currency() != expected.quote_currency().as_str()
        || parsed.product_id().split_once('-')
            != Some((parsed.base_currency(), parsed.quote_currency()))
        || !definition
            .definition()
            .venue_mappings()
            .iter()
            .any(|venue| {
                venue.venue_id().as_str() == COINBASE_VENUE
                    && venue.venue_symbol().as_str() == parsed.product_id()
            })
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    Ok(Some(ProviderNativeIdentityRequest {
        namespace,
        provider_instrument_id: native_id,
        instrument: expected.instrument_id(),
        venue: venue_id(COINBASE_VENUE)?,
        venue_symbol: venue_symbol(parsed.product_id())?,
        knowledge_at: now,
        effective_at: now,
    }))
}

async fn try_reuse_kraken(
    reader: &MarketDataInstrumentReadCapability,
    store: Arc<SealedResearchJournalStore>,
    config: &KrakenSourceConfig,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<ProviderNativeIdentityRequest>, CryptoReferenceError> {
    let now = trusted_now()?;
    if !config.reference_authorization().is_effective_at(now)
        || !config.authorization().is_effective_at(now)
    {
        return Err(CryptoReferenceError::AuthorizationExpired);
    }
    let expected = config.definition();
    let namespace = source_id(KRAKEN_NAMESPACE)?;
    let native_id = provider_id(config.symbol())?;
    let query =
        MarketDataProviderIdentityQuery::try_new(namespace.clone(), native_id.clone(), now, now)
            .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    let Some(selection) = reader
        .select_provider_identity_as_of(query, deadline, cancellation)
        .map_err(|error| {
            tracing::warn!(stage = "kraken_identity_selection", %error, "public crypto catalog operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?
    else {
        return Ok(None);
    };
    let exact = selection
        .exact_receipt()
        .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    if exact.instrument_id() != expected.instrument_id()
        || !exact
            .matching_venues()
            .iter()
            .any(|venue| venue.as_str() == KRAKEN_VENUE)
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    let definition = reader
        .read_selected_provider_definition(&selection, deadline, cancellation)
        .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    if definition.definition().asset_class() != AssetClass::Crypto
        || definition.definition().quote_currency() != expected.quote_currency()
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    let retained = reader
        .native_reference(&selection, deadline, cancellation)
        .map_err(|_| CryptoReferenceError::CaptureUnavailable)?
        .ok_or(CryptoReferenceError::CaptureUnavailable)?;
    let original = super::crypto_reference_transport::reopen_original(
        store,
        retained
            .claim()
            .ok_or(CryptoReferenceError::CaptureUnavailable)?
            .clone(),
        market_squawk_adapter_kraken::MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES,
        deadline,
        cancellation,
    )
    .await?;
    let parsed = KrakenSpotPairReference::from_snapshot(&original, config.symbol())
        .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
    let digest = EvidenceDigest::new(
        market_squawk_domain::DigestAlgorithm::Sha256,
        Sha256::digest(&original).into(),
    );
    if digest != exact.provider_identity_payload_digest()
        || parsed.quote() != expected.quote_currency().as_str()
        || parsed.symbol().split_once('/') != Some((parsed.base(), parsed.quote()))
        || !definition
            .definition()
            .venue_mappings()
            .iter()
            .any(|venue| {
                venue.venue_id().as_str() == KRAKEN_VENUE
                    && venue.venue_symbol().as_str() == parsed.symbol()
            })
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    Ok(Some(ProviderNativeIdentityRequest {
        namespace,
        provider_instrument_id: native_id,
        instrument: expected.instrument_id(),
        venue: venue_id(KRAKEN_VENUE)?,
        venue_symbol: venue_symbol(parsed.symbol())?,
        knowledge_at: now,
        effective_at: now,
    }))
}

/// Merges already accepted original provider assertions in one compare-and-set catalog update.
/// Direct Exchange preflight uses the same path after its own original receipt is sealed.
pub(crate) async fn synchronize_accepted_catalog_references(
    reader: MarketDataInstrumentReadCapability,
    synchronizer: MarketDataInstrumentSynchronizationCapability,
    accepted: Vec<AcceptedCatalogReference>,
    native_captures: Vec<AcceptedNativeReferenceCapture>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<ProviderNativeIdentityRequest>, CryptoReferenceError> {
    check_operation(deadline, cancellation)?;
    let candidates: Vec<_> = accepted
        .into_iter()
        .map(|item| {
            (
                item.expected,
                SourceReference {
                    namespace: item.namespace,
                    native_id: item.native_id,
                    canonical: item.canonical,
                    venue: item.venue,
                    symbol: item.symbol,
                    quote: item.quote,
                    evidence: item.evidence,
                    validity: item.validity,
                    observed_at: item.observed_at,
                },
            )
        })
        .collect();
    if candidates.is_empty() {
        return Err(CryptoReferenceError::NoConfiguredSource);
    }
    // One definition per canonical identity keeps a two-venue BTC/USD startup atomic. Existing
    // definitions provide their own canonical identity and unrelated historical assertions.
    let mut groups: BTreeMap<InstrumentId, Vec<SourceReference>> = BTreeMap::new();
    let mut expected_by_id: BTreeMap<InstrumentId, InstrumentDefinition> = BTreeMap::new();
    for (expected, candidate) in candidates {
        let id = expected.instrument_id();
        if expected_by_id.get(&id).is_some_and(|previous| {
            previous.instrument_id() != expected.instrument_id()
                || previous.asset_class() != expected.asset_class()
                || previous.quote_currency() != expected.quote_currency()
        }) {
            return Err(CryptoReferenceError::RouteMismatch);
        }
        expected_by_id.insert(id, expected);
        groups.entry(id).or_default().push(candidate);
    }
    let mut definitions = Vec::with_capacity(groups.len());
    let mut expected_current = Vec::with_capacity(groups.len());
    let mut requests = Vec::new();
    for (id, references) in groups {
        let expected = &expected_by_id[&id];
        let existing = reader
            .latest(id, deadline, cancellation)
            .map_err(|error| {
                tracing::warn!(stage = "latest_definition", %error, "public crypto catalog operation failed");
                CryptoReferenceError::CatalogUnavailable
            })?;
        let definition = prepare_definition(
            existing.as_ref().map(|record| record.definition()),
            expected,
            &references,
        )?;
        expected_current.push(existing.as_ref().map_or_else(
            || MarketDataInstrumentCurrentExpectation::absent(id),
            MarketDataInstrumentCurrentExpectation::from_record,
        ));
        definitions.push(definition);
        for reference in references {
            let request = ProviderNativeIdentityRequest {
                namespace: reference.namespace.clone(),
                provider_instrument_id: reference.native_id,
                instrument: reference.canonical,
                venue: reference.venue,
                venue_symbol: reference.symbol,
                knowledge_at: reference.observed_at,
                effective_at: reference.observed_at,
            };
            requests.push(request);
        }
    }
    let count = definitions.len();
    let batch = MarketDataInstrumentSynchronization::try_new(definitions, count)
        .map_err(|_| CryptoReferenceError::CatalogUnavailable)?;
    let cancel = cancellation.clone();
    tokio::task::spawn_blocking(move || {
        synchronizer.synchronize_native_references_if_current(
            batch,
            expected_current,
            native_captures,
            deadline,
            &cancel,
        )
    })
    .await
    .map_err(|error| {
        tracing::warn!(
            stage = "synchronization_join",
            panicked = error.is_panic(),
            cancelled = error.is_cancelled(),
            "public crypto catalog worker failed"
        );
        CryptoReferenceError::CatalogUnavailable
    })?
    .map_err(|error| {
        tracing::warn!(stage = "synchronization", %error, "public crypto catalog operation failed");
        CryptoReferenceError::CatalogUnavailable
    })?;
    // The selected query cutoff must include the durable definition publication, whose catalog
    // timestamp can be later than the initial source receipt.
    let knowledge_at = trusted_now()?;
    for request in &mut requests {
        request.knowledge_at = knowledge_at;
        request.effective_at = knowledge_at;
    }
    Ok(requests)
}

fn prepare_definition(
    existing: Option<&MarketDataInstrumentDefinition>,
    expected: &InstrumentDefinition,
    references: &[SourceReference],
) -> Result<MarketDataInstrumentDefinition, CryptoReferenceError> {
    let first = references
        .first()
        .ok_or(CryptoReferenceError::NoConfiguredSource)?;
    let is_recommended =
        expected.instrument_id().to_string() == RECOMMENDED_PUBLIC_BTC_USD_INSTRUMENT_ID;
    if (!is_recommended && existing.is_none())
        || (is_recommended
            && (expected.quote_currency().as_str() != "USD"
                || references
                    .iter()
                    .any(|reference| match reference.venue.as_str() {
                        COINBASE_VENUE => reference.symbol.as_str() != "BTC-USD",
                        KRAKEN_VENUE => reference.symbol.as_str() != "BTC/USD",
                        _ => true,
                    })))
    {
        return Err(CryptoReferenceError::CanonicalIdentityUnapproved);
    }
    if references
        .iter()
        .any(|item| item.quote != first.quote || item.canonical != first.canonical)
    {
        return Err(CryptoReferenceError::RouteMismatch);
    }
    if existing.is_some()
        && expected.instrument_id().to_string() != RECOMMENDED_PUBLIC_BTC_USD_INSTRUMENT_ID
        && references.iter().any(|reference| {
            existing.is_some_and(|record| {
                !record.provider_identities().iter().any(|identity| {
                    identity.source_id() == &reference.namespace
                        && identity.provider_instrument_id() == &reference.native_id
                })
            })
        })
    {
        return Err(CryptoReferenceError::CanonicalIdentityUnapproved);
    }
    let mut venues = existing.map_or_else(Vec::new, |record| record.venue_mappings().to_vec());
    let mut identities =
        existing.map_or_else(Vec::new, |record| record.provider_identities().to_vec());
    let mut added = existing.is_none();
    for reference in references {
        if let Some(old) = venues
            .iter()
            .find(|mapping| mapping.venue_id() == &reference.venue)
        {
            if old.venue_symbol() != &reference.symbol {
                return Err(CryptoReferenceError::RouteMismatch);
            }
        } else {
            venues.push(VenueMapping::new(
                reference.venue.clone(),
                reference.symbol.clone(),
            ));
            added = true;
        }
        if let Some(old) = identities
            .iter()
            .find(|identity| identity.source_id() == &reference.namespace)
        {
            if old.provider_instrument_id() != &reference.native_id
                || old.instrument_id() != reference.canonical
            {
                return Err(CryptoReferenceError::RouteMismatch);
            }
            if old.evidence().content_digest() != reference.evidence.content_digest()
                || old.observed_at() != reference.observed_at
            {
                return Err(CryptoReferenceError::CanonicalIdentityUnapproved);
            }
        } else {
            let revision =
                content_revision(&reference.namespace, reference.evidence.content_digest())?;
            identities.push(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: reference.canonical,
                source_id: reference.namespace.clone(),
                provider_instrument_id: reference.native_id.clone(),
                evidence: ProviderIdentityEvidence::from_content_digest(
                    reference.evidence.content_digest(),
                ),
                source_timestamp: None,
                observed_at: reference.observed_at,
                metadata_revision: revision,
                validity: reference.validity,
                supersedes: None,
            }));
            added = true;
        }
    }
    if let Some(old) = existing {
        if old.instrument_id() != expected.instrument_id()
            || old.asset_class() != AssetClass::Crypto
            || old.quote_currency() != expected.quote_currency()
        {
            return Err(CryptoReferenceError::RouteMismatch);
        }
        if !added {
            return Ok(old.clone());
        }
    }
    let start = references
        .iter()
        .map(|item| item.observed_at)
        .max()
        .ok_or(CryptoReferenceError::NoConfiguredSource)?;
    let interval_end = references
        .iter()
        .filter_map(|item| item.validity.ends_at())
        .min();
    let reference_evidence = RevisionBoundPayloadEvidence::new(
        content_revision(&first.namespace, first.evidence.content_digest())?,
        first.evidence.clone(),
    );
    MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
        instrument_id: expected.instrument_id(),
        reference_evidence,
        effective_interval: EffectiveInterval::new(start, interval_end)
            .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
        asset_class: AssetClass::Crypto,
        display_name: existing.and_then(|old| old.display_name().cloned()),
        quote_currency: first.quote,
        quote_currency_evidence: existing.map_or_else(
            || first.evidence.clone(),
            |old| old.quote_currency_evidence().clone(),
        ),
        venue_mappings: venues,
        provider_identities: identities,
        identifiers: existing.map_or_else(Vec::new, |old| old.identifiers().to_vec()),
    })
    .map_err(|_| CryptoReferenceError::InvalidEvidence)
}

fn bounded_validity(
    observed_at: Timestamp,
    authorization: EffectiveInterval,
) -> Result<EffectiveInterval, CryptoReferenceError> {
    if authorization.starts_at() > observed_at
        || authorization
            .ends_at()
            .is_some_and(|end| observed_at >= end)
    {
        return Err(CryptoReferenceError::AuthorizationExpired);
    }
    EffectiveInterval::new(observed_at, authorization.ends_at())
        .map_err(|_| CryptoReferenceError::AuthorizationExpired)
}

pub(super) async fn within<T>(
    deadline: Instant,
    cancellation: &CancellationToken,
    future: impl std::future::Future<Output = Result<T, CryptoReferenceError>>,
) -> Result<T, CryptoReferenceError> {
    tokio::select! {
        _ = cancellation.cancelled() => Err(CryptoReferenceError::Cancelled),
        result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future) =>
            result.map_err(|_| CryptoReferenceError::DeadlineElapsed)?,
    }
}

pub(super) fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CryptoReferenceError> {
    if cancellation.is_cancelled() {
        return Err(CryptoReferenceError::Cancelled);
    }
    if Instant::now() >= deadline {
        return Err(CryptoReferenceError::DeadlineElapsed);
    }
    Ok(())
}
pub(super) fn trusted_now() -> Result<Timestamp, CryptoReferenceError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CryptoReferenceError::ClockUnavailable)?;
    let nanos =
        i64::try_from(duration.as_nanos()).map_err(|_| CryptoReferenceError::ClockUnavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
fn source_id(value: &str) -> Result<SourceId, CryptoReferenceError> {
    SourceId::try_from(value).map_err(|_| CryptoReferenceError::InvalidEvidence)
}
fn provider_id(value: &str) -> Result<ProviderInstrumentId, CryptoReferenceError> {
    ProviderInstrumentId::try_from(value).map_err(|_| CryptoReferenceError::InvalidEvidence)
}
fn venue_id(value: &str) -> Result<VenueId, CryptoReferenceError> {
    VenueId::try_from(value).map_err(|_| CryptoReferenceError::InvalidEvidence)
}
fn venue_symbol(value: &str) -> Result<VenueSymbol, CryptoReferenceError> {
    VenueSymbol::try_from(value).map_err(|_| CryptoReferenceError::InvalidEvidence)
}
fn content_revision(
    namespace: &SourceId,
    digest: EvidenceDigest,
) -> Result<MetadataRevision, CryptoReferenceError> {
    let mut name = format!("{}-", namespace.as_str());
    for byte in digest.bytes() {
        name.push_str(&format!("{byte:02x}"));
    }
    let source =
        SourceIdentifier::try_from(name).map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    Ok(MetadataRevision::new(source))
}

/// Startup fails closed on any absent, ambiguous, unsupported, or stale provider reference.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CryptoReferenceError {
    #[error("no public crypto source is configured")]
    NoConfiguredSource,
    #[error("public crypto reference transport is unavailable")]
    TransportUnavailable,
    #[error("public crypto original reference capture is unavailable")]
    CaptureUnavailable,
    #[error("public crypto reference is unavailable")]
    ProviderReferenceUnavailable,
    #[error("public crypto response disagrees with the configured canonical route")]
    RouteMismatch,
    #[error("public crypto reference evidence is invalid")]
    InvalidEvidence,
    #[error("public crypto authorization has expired")]
    AuthorizationExpired,
    #[error("market-data identity catalog is unavailable or rejected the reference")]
    CatalogUnavailable,
    #[error("canonical crypto identity has no approved catalog or code-owned first-run anchor")]
    CanonicalIdentityUnapproved,
    #[error("public crypto reference awaits provider rate admission")]
    RateDeferred { not_before: Instant },
    #[error("public crypto reference deadline elapsed")]
    DeadlineElapsed,
    #[error("public crypto reference was cancelled")]
    Cancelled,
    #[error("trusted clock is unavailable")]
    ClockUnavailable,
}
