//! Governed, receipt-bearing public product and pair reference acquisition.

use std::{
    io::Read as _,
    num::NonZeroU64,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use market_squawk_adapter_coinbase::{
    COINBASE_PUBLIC_PRODUCT_ENDPOINT, CoinbasePublicProductReference,
    MAX_COINBASE_PUBLIC_PRODUCT_BYTES,
};
use market_squawk_adapter_kraken::{KrakenSpotPairReference, MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES};
use market_squawk_data::AcceptedNativeReferenceCapture;
use market_squawk_domain::{
    AssetClass, ChecksumCapability, ConnectionGeneration, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    MetadataRevision, ProviderInstrumentId, RevisionBoundPayloadEvidence, SchemaVersion,
    SequenceCapability, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    LocalAuthorityStateStore, LocalPaths, ResearchObjectAdmission, ResearchObjectClaim,
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    ResearchObjectReceipt, SealedResearchJournalStore, SealedResearchJournalStoreError,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, BudgetDispatchDecision, BudgetPermit,
    BudgetReservationDecision, BudgetUnavailableReason, CoverageTopology, CurrentSourceSession,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, HttpCaptureMethod, HttpRequestBounds,
    InstrumentCoverage, NetworkAccessPolicy, ProviderBudgetPolicy, ProviderRateAuthority,
    SessionId, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile, TransportFrameKind,
};
use reqwest::{Client, header, redirect::Policy};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::crypto_reference::CryptoReferenceError;

pub(super) const COINBASE_REFERENCE_SOURCE: &str = "coinbase-public-product-reference";
pub(super) const KRAKEN_REFERENCE_SOURCE: &str = "kraken-public-instrument-reference";
const KRAKEN_SUBSCRIBE: &str = r#"{"method":"subscribe","params":{"channel":"instrument","snapshot":true,"include_tokenized_assets":false}}"#;
const MAX_CONTROL_MESSAGES: usize = 8;

pub(super) struct CapturedReference {
    pub(super) body: Vec<u8>,
    pub(super) body_digest: EvidenceDigest,
    pub(super) observed_at: Timestamp,
    pub(super) accepted: AcceptedNativeReferenceCapture,
}

struct SealControl {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl ResearchObjectControl for SealControl {
    fn checkpoint(&self, _: ResearchObjectControlPoint) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            Err(ResearchObjectControlError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ResearchObjectControlError::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
}

// Public diagnostics retain the failed operation and closed cause, never filesystem paths or bytes.
fn capture_store_failure(
    stage: &'static str,
    error: SealedResearchJournalStoreError,
) -> CryptoReferenceError {
    match error {
        SealedResearchJournalStoreError::Io { context, source } => {
            tracing::warn!(stage, context, kind = ?source.kind(), "public reference capture failed");
        }
        SealedResearchJournalStoreError::Journal(_) => {
            tracing::warn!(stage, cause = "journal", "public reference capture failed");
        }
        error => {
            // Remaining variants contain only static descriptions, closed controls or numeric bounds.
            tracing::warn!(stage, %error, "public reference capture failed");
        }
    }
    CryptoReferenceError::CaptureUnavailable
}

/// Physically seals the complete original response/frame and re-verifies its digest before any
/// provider assertion is decoded or sent to the catalog.
pub(crate) async fn seal_original(
    store: Arc<SealedResearchJournalStore>,
    original: &[u8],
    maximum_bytes: usize,
    expected_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ResearchObjectReceipt, CryptoReferenceError> {
    super::crypto_reference::check_operation(deadline, cancellation)?;
    let bytes = original.to_vec();
    let control = SealControl {
        deadline,
        cancellation: cancellation.clone(),
    };
    let task = tokio::task::spawn_blocking(move || {
        control
            .checkpoint(ResearchObjectControlPoint::BeforeVerification)
            .map_err(|_| CryptoReferenceError::CaptureUnavailable)?;
        let admission = ResearchObjectAdmission::try_new(
            u64::try_from(maximum_bytes).map_err(|_| CryptoReferenceError::CaptureUnavailable)?,
            1,
        )
        .map_err(|_| CryptoReferenceError::CaptureUnavailable)?;
        let mut pending = store
            .begin_logical_object(admission)
            .map_err(|error| capture_store_failure("begin", error))?;
        pending
            .write_admitted(&bytes)
            .map_err(|error| capture_store_failure("write", error))?;
        let verified = store
            .finish_logical_object(pending, &control)
            .map_err(|error| capture_store_failure("seal", error))?;
        if verified.content_digest() != expected_digest
            || verified.size_bytes() != bytes.len() as u64
        {
            return Err(CryptoReferenceError::CaptureUnavailable);
        }
        verified
            .reverify_for_commit(&control)
            .map_err(|error| capture_store_failure("reverify", error))
    });
    // Retain the blocking owner until it has a definite commit/refusal outcome; dropping its
    // JoinHandle on timeout would leave an unaccounted late raw object behind.
    let result = task
        .await
        .map_err(|_| CryptoReferenceError::CaptureUnavailable)??;
    super::crypto_reference::check_operation(deadline, cancellation)?;
    Ok(result)
}

/// Reopens one catalog-retained physical claim through the sole research store and verifies its
/// complete original bytes before a restart may reuse a selected provider identity.
pub(super) async fn reopen_original(
    store: Arc<SealedResearchJournalStore>,
    claim: ResearchObjectClaim,
    maximum_bytes: usize,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>, CryptoReferenceError> {
    super::crypto_reference::check_operation(deadline, cancellation)?;
    let control = SealControl {
        deadline,
        cancellation: cancellation.clone(),
    };
    let task = tokio::task::spawn_blocking(move || {
        let mut object = store
            .open_verified_logical_object_claim(&claim, &control)
            .map_err(|error| capture_store_failure("reopen", error))?;
        let size = usize::try_from(object.size_bytes())
            .map_err(|_| CryptoReferenceError::CaptureUnavailable)?;
        if size == 0 || size > maximum_bytes {
            return Err(CryptoReferenceError::CaptureUnavailable);
        }
        let mut body = Vec::new();
        body.try_reserve_exact(size)
            .map_err(|_| CryptoReferenceError::CaptureUnavailable)?;
        object
            .read_to_end(&mut body)
            .map_err(|_| CryptoReferenceError::CaptureUnavailable)?;
        if body.len() != size {
            return Err(CryptoReferenceError::CaptureUnavailable);
        }
        Ok(body)
    });
    let result = task
        .await
        .map_err(|_| CryptoReferenceError::CaptureUnavailable)??;
    super::crypto_reference::check_operation(deadline, cancellation)?;
    Ok(result)
}

/// The endpoint's canonical policy is supplied by composition, so reference acquisition and
/// onboarding share the same provider allocation on their common network authority.
pub(super) fn metadata(
    source: &'static str,
    venue: &'static str,
    endpoints: &[String],
    authorization: AuthorizationGrant,
    budget: &ProviderBudgetPolicy,
) -> Result<SourceMetadata, CryptoReferenceError> {
    let effective = authorization.effective_interval();
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/public-crypto-reference-source/v1\0");
    hasher.update(source.as_bytes());
    for endpoint in endpoints {
        hasher.update((endpoint.len() as u64).to_be_bytes());
        hasher.update(endpoint.as_bytes());
    }
    hasher.update(authorization.evidence().content_digest().bytes());
    hasher.update(serde_json::to_vec(budget).map_err(|_| CryptoReferenceError::InvalidEvidence)?);
    let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
    let evidence = ExactPayloadEvidence::from_content_digest(digest);
    let mut revision = String::from("crypto-reference-");
    for byte in digest.bytes() {
        use std::fmt::Write as _;
        write!(&mut revision, "{byte:02x}").map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    }
    let bounds = HttpRequestBounds::try_new(
        NonZeroU64::new(5_000_000_000).ok_or(CryptoReferenceError::InvalidEvidence)?,
        NonZeroU64::new(15_000_000_000).ok_or(CryptoReferenceError::InvalidEvidence)?,
        NonZeroU64::new(20_000_000_000).ok_or(CryptoReferenceError::InvalidEvidence)?,
        0,
        NonZeroU64::new(8 * 1024 * 1024).ok_or(CryptoReferenceError::InvalidEvidence)?,
    )
    .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    let policy = EndpointPolicy::try_new_with_bounds(endpoints, bounds)
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(source).map_err(|_| CryptoReferenceError::InvalidEvidence)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(
                SourceIdentifier::try_from(revision)
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
            ),
            evidence.clone(),
        ),
        SourceClass::Exchange,
        budget.scope().as_source_identifier().clone(),
        authorization,
        SourceCoverage::try_instrument(
            evidence,
            effective,
            vec![AssetClass::Crypto],
            CoverageTopology::single_venue(
                VenueId::try_from(venue).map_err(|_| CryptoReferenceError::InvalidEvidence)?,
            ),
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::NotApplicable,
            DeliveryEvidence::DirectVenue,
        )
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Allowlisted(policy),
        FreshnessPolicy::try_new(
            86_400_000_000_000,
            86_400_000_000_000,
            86_400_000_000_000,
            86_400_000_000_000,
            1_000_000_000,
        )
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
        Some(budget.clone()),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| CryptoReferenceError::InvalidEvidence)
}

fn open_registry(
    paths: &LocalPaths,
    provider_rate: ProviderRateAuthority,
    source: &'static str,
) -> Result<AuthoritativeSourceRegistry, CryptoReferenceError> {
    let store = LocalAuthorityStateStore::try_open(paths.root().join("authority").join(source))
        .map_err(|error| {
            tracing::warn!(stage = "authority_store_open", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
    AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
        store, Arc::new(provider_rate.clone()), provider_rate,
    ).map_err(|error| {
            tracing::warn!(stage = "registry_open", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })
}

pub(super) async fn coinbase_product(
    paths: &LocalPaths,
    capture_store: Arc<SealedResearchJournalStore>,
    canonical: InstrumentId,
    provider_rate: ProviderRateAuthority,
    metadata: SourceMetadata,
    product: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CapturedReference, CryptoReferenceError> {
    let mut registry = open_registry(paths, provider_rate, COINBASE_REFERENCE_SOURCE)?;
    let result = coinbase_product_inner(
        &mut registry,
        capture_store,
        canonical,
        metadata,
        product,
        deadline,
        cancellation,
    )
    .await;
    let closed = registry
        .shutdown()
        .map_err(|error| {
            tracing::warn!(stage = "coinbase_registry_shutdown", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        });
    match (result, closed) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

async fn coinbase_product_inner(
    registry: &mut AuthoritativeSourceRegistry,
    capture_store: Arc<SealedResearchJournalStore>,
    canonical: InstrumentId,
    metadata: SourceMetadata,
    product: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CapturedReference, CryptoReferenceError> {
    let url = format!("{COINBASE_PUBLIC_PRODUCT_ENDPOINT}/{product}");
    let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
        return Err(CryptoReferenceError::InvalidEvidence);
    };
    policy
        .authorize_request(&url)
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    let registered = registry
        .register_or_resume_exact(metadata, super::crypto_reference::trusted_now()?)
        .map_err(|error| {
            tracing::warn!(stage = "coinbase_register", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
    let session = registry
        .begin_session(
            &registered,
            SessionId::new(
                SourceIdentifier::try_from(format!("crypto-product-{}", uuid::Uuid::new_v4()))
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
            ),
            next_generation()?,
            super::crypto_reference::trusted_now()?,
        )
        .map_err(|error| {
            tracing::warn!(stage = "coinbase_begin_session", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
    let result = async {
        let mut frames = registry
            .take_raw_frame_factory(&session)
            .map_err(|error| {
            tracing::warn!(stage = "coinbase_frame_factory", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
        let budget = session
            .budget()
            .ok_or(CryptoReferenceError::TransportUnavailable)?;
        let permit = commit_reference_dispatch(&session, deadline, cancellation).await?;
        let operation = async {
            let client = Client::builder()
                .redirect(Policy::none())
                .https_only(true)
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(20))
                .build()
                .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
            let response = super::crypto_reference::within(deadline, cancellation, async {
                client
                    .get(&url)
                    .header(header::ACCEPT_ENCODING, "identity")
                    .send()
                    .await
                    .map_err(|_| CryptoReferenceError::TransportUnavailable)
            })
            .await?;
            if response.url().as_str() != url || response.status().as_u16() != 200 {
                return Err(CryptoReferenceError::ProviderReferenceUnavailable);
            }
            if response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|encoding| !encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
            {
                return Err(CryptoReferenceError::ProviderReferenceUnavailable);
            }
            let declared = response.content_length();
            if declared.is_some_and(|length| length > MAX_COINBASE_PUBLIC_PRODUCT_BYTES as u64) {
                return Err(CryptoReferenceError::ProviderReferenceUnavailable);
            }
            let mut body = Vec::new();
            let mut stream = response.bytes_stream();
            loop {
                let next = super::crypto_reference::within(deadline, cancellation, async {
                    stream
                        .next()
                        .await
                        .transpose()
                        .map_err(|_| CryptoReferenceError::TransportUnavailable)
                })
                .await?;
                let Some(bytes) = next else { break };
                if body
                    .len()
                    .checked_add(bytes.len())
                    .is_none_or(|size| size > MAX_COINBASE_PUBLIC_PRODUCT_BYTES)
                {
                    return Err(CryptoReferenceError::ProviderReferenceUnavailable);
                }
                body.extend_from_slice(&bytes);
            }
            let mut builder = frames
                .try_http_response_builder(
                    HttpCaptureMethod::Get,
                    &url,
                    200,
                    declared,
                    MAX_COINBASE_PUBLIC_PRODUCT_BYTES as u64,
                    1,
                )
                .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
            builder
                .try_push_segment(Bytes::from(body.clone()))
                .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
            let capture = builder
                .finish()
                .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
            if capture
                .receipt()
                .currentness_lease()
                .validate_current()
                .is_err()
                || capture.receipt().source_id().as_str() != COINBASE_REFERENCE_SOURCE
            {
                return Err(CryptoReferenceError::InvalidEvidence);
            }
            let sealed = seal_original(
                capture_store,
                &body,
                MAX_COINBASE_PUBLIC_PRODUCT_BYTES,
                capture.receipt().body_digest(),
                deadline,
                cancellation,
            )
            .await?;
            let parsed = CoinbasePublicProductReference::from_response(&body, product)
                .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
            let accepted = AcceptedNativeReferenceCapture::from_http(
                canonical,
                SourceId::try_from(super::crypto_reference::COINBASE_NAMESPACE)
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
                ProviderInstrumentId::try_from(parsed.product_id())
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
                capture.receipt(),
                sealed,
            )
            .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
            Ok(CapturedReference {
                body,
                body_digest: capture.receipt().body_digest(),
                observed_at: capture.receipt().received_at(),
                accepted,
            })
        };
        let result = operation.await;
        permit.release();
        if result.is_ok() {
            budget
                .record_success()
                .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
        }
        result
    }
    .await;
    let ended = registry
        .end_session(&session, session.started_at())
        .map_err(|error| {
            tracing::warn!(stage = "coinbase_end_session", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        });
    match (result, ended) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

pub(super) async fn kraken_instrument(
    paths: &LocalPaths,
    capture_store: Arc<SealedResearchJournalStore>,
    canonical: InstrumentId,
    requested_symbol: &str,
    provider_rate: ProviderRateAuthority,
    metadata: SourceMetadata,
    endpoint: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CapturedReference, CryptoReferenceError> {
    let mut registry = open_registry(paths, provider_rate, KRAKEN_REFERENCE_SOURCE)?;
    let result = kraken_instrument_inner(
        &mut registry,
        capture_store,
        canonical,
        requested_symbol,
        metadata,
        endpoint,
        deadline,
        cancellation,
    )
    .await;
    let closed = registry
        .shutdown()
        .map_err(|error| {
            tracing::warn!(stage = "kraken_registry_shutdown", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        });
    match (result, closed) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

async fn kraken_instrument_inner(
    registry: &mut AuthoritativeSourceRegistry,
    capture_store: Arc<SealedResearchJournalStore>,
    canonical: InstrumentId,
    requested_symbol: &str,
    metadata: SourceMetadata,
    endpoint: &str,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<CapturedReference, CryptoReferenceError> {
    let NetworkAccessPolicy::Allowlisted(policy) = metadata.network_policy() else {
        return Err(CryptoReferenceError::InvalidEvidence);
    };
    policy
        .authorize_request(endpoint)
        .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
    let registered = registry
        .register_or_resume_exact(metadata, super::crypto_reference::trusted_now()?)
        .map_err(|error| {
            tracing::warn!(stage = "kraken_register", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
    let session = registry
        .begin_session(
            &registered,
            SessionId::new(
                SourceIdentifier::try_from(format!("crypto-instrument-{}", uuid::Uuid::new_v4()))
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
            ),
            next_generation()?,
            super::crypto_reference::trusted_now()?,
        )
        .map_err(|error| {
            tracing::warn!(stage = "kraken_begin_session", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
    let result = async {
        let mut frames = registry
            .take_raw_frame_factory(&session)
            .map_err(|error| {
            tracing::warn!(stage = "kraken_frame_factory", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        })?;
        let budget = session
            .budget()
            .ok_or(CryptoReferenceError::TransportUnavailable)?;
        let permit = commit_reference_dispatch(&session, deadline, cancellation).await?;
        let operation = async {
            let (mut socket, _) = super::crypto_reference::within(deadline, cancellation, async {
                tokio_tungstenite::connect_async(endpoint)
                    .await
                    .map_err(|_| CryptoReferenceError::TransportUnavailable)
            })
            .await?;
            super::crypto_reference::within(deadline, cancellation, async {
                socket
                    .send(Message::Text(KRAKEN_SUBSCRIBE.into()))
                    .await
                    .map_err(|_| CryptoReferenceError::TransportUnavailable)
            })
            .await?;
            let mut acknowledged = false;
            for _ in 0..MAX_CONTROL_MESSAGES {
                let message = super::crypto_reference::within(deadline, cancellation, async {
                    socket
                        .next()
                        .await
                        .ok_or(CryptoReferenceError::ProviderReferenceUnavailable)?
                        .map_err(|_| CryptoReferenceError::TransportUnavailable)
                })
                .await?;
                let Message::Text(text) = message else {
                    continue;
                };
                if text.len() > MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES {
                    return Err(CryptoReferenceError::ProviderReferenceUnavailable);
                }
                let frame = frames
                    .try_frame(
                        TransportFrameKind::Text,
                        Bytes::copy_from_slice(text.as_bytes()),
                    )
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
                let value: Value = serde_json::from_slice(frame.payload())
                    .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
                if value.get("method").and_then(Value::as_str) == Some("subscribe") {
                    if value.pointer("/result/channel").and_then(Value::as_str)
                        != Some("instrument")
                        || value.get("success").and_then(Value::as_bool) != Some(true)
                    {
                        return Err(CryptoReferenceError::ProviderReferenceUnavailable);
                    }
                    acknowledged = true;
                } else if value.get("channel").and_then(Value::as_str) == Some("instrument")
                    && value.get("type").and_then(Value::as_str) == Some("snapshot")
                {
                    if !acknowledged || frame.source_id().as_str() != KRAKEN_REFERENCE_SOURCE {
                        return Err(CryptoReferenceError::InvalidEvidence);
                    }
                    let body = frame.payload().to_vec();
                    let digest =
                        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
                    let sealed = seal_original(
                        capture_store,
                        &body,
                        MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES,
                        digest,
                        deadline,
                        cancellation,
                    )
                    .await?;
                    let parsed = KrakenSpotPairReference::from_snapshot(&body, requested_symbol)
                        .map_err(|_| CryptoReferenceError::ProviderReferenceUnavailable)?;
                    let validated = session
                        .validate_live_frame(&frame)
                        .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
                    let accepted = AcceptedNativeReferenceCapture::from_frame(
                        canonical,
                        SourceId::try_from(super::crypto_reference::KRAKEN_NAMESPACE)
                            .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
                        ProviderInstrumentId::try_from(parsed.symbol())
                            .map_err(|_| CryptoReferenceError::InvalidEvidence)?,
                        &validated,
                        sealed,
                    )
                    .map_err(|_| CryptoReferenceError::InvalidEvidence)?;
                    return Ok(CapturedReference {
                        body,
                        body_digest: digest,
                        observed_at: frame.received_at(),
                        accepted,
                    });
                }
            }
            Err(CryptoReferenceError::ProviderReferenceUnavailable)
        };
        let result = operation.await;
        permit.release();
        if result.is_ok() {
            budget
                .record_success()
                .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
        }
        result
    }
    .await;
    let ended = registry
        .end_session(&session, session.started_at())
        .map_err(|error| {
            tracing::warn!(stage = "kraken_end_session", %error, "public crypto reference authority operation failed");
            CryptoReferenceError::CatalogUnavailable
        });
    match (result, ended) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

/// Waiting consumes no request or permit and never extends the caller's operation deadline.
async fn commit_reference_dispatch(
    session: &CurrentSourceSession,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, CryptoReferenceError> {
    const CONCURRENCY_RECHECK: Duration = Duration::from_millis(25);
    let budget = session
        .budget()
        .ok_or(CryptoReferenceError::TransportUnavailable)?;
    loop {
        super::crypto_reference::check_operation(deadline, cancellation)?;
        session
            .validate_current_lease()
            .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(until) => {
                let wait = budget
                    .remaining_wait(until)
                    .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
                wait_for_reference_budget(wait, deadline, cancellation).await?;
                continue;
            }
            BudgetReservationDecision::Unavailable(
                BudgetUnavailableReason::ConcurrencyExhausted,
            ) => {
                wait_for_reference_budget(CONCURRENCY_RECHECK, deadline, cancellation).await?;
                continue;
            }
            BudgetReservationDecision::Unavailable(_) => {
                return Err(CryptoReferenceError::TransportUnavailable);
            }
        };
        super::crypto_reference::check_operation(deadline, cancellation)?;
        session
            .validate_current_lease()
            .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
        match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(until) => {
                let wait = budget
                    .remaining_wait(until)
                    .map_err(|_| CryptoReferenceError::TransportUnavailable)?;
                wait_for_reference_budget(wait, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(BudgetUnavailableReason::ConcurrencyExhausted) => {
                wait_for_reference_budget(CONCURRENCY_RECHECK, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(_) => {
                return Err(CryptoReferenceError::TransportUnavailable);
            }
        }
    }
}

async fn wait_for_reference_budget(
    wait: Duration,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), CryptoReferenceError> {
    super::crypto_reference::check_operation(deadline, cancellation)?;
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(CryptoReferenceError::DeadlineElapsed)?;
    if wait >= remaining {
        let not_before = Instant::now()
            .checked_add(wait)
            .ok_or(CryptoReferenceError::ClockUnavailable)?;
        return Err(CryptoReferenceError::RateDeferred { not_before });
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(CryptoReferenceError::Cancelled),
        () = tokio::time::sleep_until(deadline.into()) => Err(CryptoReferenceError::DeadlineElapsed),
        () = tokio::time::sleep(wait) => super::crypto_reference::check_operation(deadline, cancellation),
    }
}

fn next_generation() -> Result<ConnectionGeneration, CryptoReferenceError> {
    let nanos = super::crypto_reference::trusted_now()?.unix_nanos();
    ConnectionGeneration::new(
        u64::try_from(nanos).map_err(|_| CryptoReferenceError::ClockUnavailable)?,
    )
    .map_err(|_| CryptoReferenceError::ClockUnavailable)
}
