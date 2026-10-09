use crate::{
    SchwabStreamerConnectionPermit, SchwabStreamerRequestAcknowledgement,
    SchwabStreamerRequestPermit, SchwabStreamerRuntimeAuthority, SchwabStreamerRuntimeEvent,
};
use market_squawk_sources::{
    BackoffPolicy, BudgetDecision, BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision,
    BudgetScope, ProviderBudgetPolicy, ProviderRateAuthority, ProviderRateDeclaration,
    SharedProviderBudget,
};
use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, pending};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, BarTimeSemantics, BarTimestampBasis, CanonicalStateDigest,
    CanonicalizationRule, CoverageStatus, Currency, DataQuality, DecodedLiveProvenanceInput,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    LiveEventClass, LiveEvidenceBinding, LiveProvenance, MarketBarAdjustment,
    MarketBarSessionEvidence, MarketBarSessionKind, MarketEvent, MetadataRevision,
    OptionComponentState, PayloadHash, PayloadReference, ProviderInstrumentId, RuleVersion,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    EncryptedFileSecretStore, LocalPaths, LocalSecretStoreError, SealedResearchJournalStore,
    SecretCancellation, SecretGeneration, SecretInteractionPolicy, SecretKey,
    SecretOperationControl, SecretRef, SecretStore, SecretValue,
};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryRequest, ExtractionRequest, OptionMarketBatchKind, SourceObject,
};
use sha2::Digest as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenAdmission, AccessTokenGeneration,
    CallbackOutcome, ChainRequest, ConnectionGeneration, ConnectionState, DesiredStateController,
    ExpirationChainRequest, HttpMethod, InboundStreamerFrame, InstrumentProjection,
    MarketDataService, MarketId, MoverFrequency, MoverSort, OAuthCallback, OAuthLoopbackBounds,
    OAuthLoopbackReceiver, OAuthLoopbackTlsAcceptError, OAuthLoopbackTlsAcceptFuture,
    OAuthLoopbackTlsAcceptor, OAuthLoopbackTlsStream, ParseBounds, PriceHistoryFrequency,
    PriceHistoryFrequencyType, PriceHistoryRequest, ProtectedSchwabOAuthAuthority,
    ProviderIdentifier, QuoteRequest, RawStreamerFrameKind, ReadOnlyRoute, RefreshTokenGeneration,
    RequestAdmission, ResponseHeaderEvidence, RestExecutionOutcome, RestTransportBounds,
    SchwabAccessTokenSource, SchwabAdapterError, SchwabApplicationCredentialReplacement,
    SchwabCanonicalError, SchwabCanonicalField, SchwabCaptureCoordinates,
    SchwabCredentialAuthorityBinding, SchwabDailyPriceHistoryCalendarRangeReceipt,
    SchwabDailyPriceHistoryPublicationRequest, SchwabHttpWire, SchwabHttpWireRequest,
    SchwabHttpWireResponse, SchwabMarketDataDelay, SchwabMarketDataQualification,
    SchwabOAuthAuthorityConfiguration, SchwabOAuthAuthorityError, SchwabOAuthAuthorityReceipt,
    SchwabOAuthAuthorityStatus, SchwabOAuthInteraction, SchwabOAuthSecretPolicy, SchwabOAuthWire,
    SchwabOAuthWireError, SchwabOAuthWireRequest, SchwabOAuthWireResponse,
    SchwabObservedCapabilityFamily, SchwabOptionCandidateOutcome,
    SchwabPriceHistoryCapabilityObservation, SchwabPriceHistoryMarketDataEvidence,
    SchwabResolvedProviderIdentity, SchwabRestExecutor, SchwabRestFamily,
    SchwabRestFamilyDoctorInput, SchwabRestOptionContractRequest,
    SchwabRestOptionMarketDataEvidence, SchwabRestOptionPublicationOutcome,
    SchwabRestOptionPublicationRequest, SchwabRestOptionUnderlyingRequest,
    SchwabRestQuoteMarketDataEvidence, SchwabRestQuotePublicationOutcome,
    SchwabRestQuotePublicationRequest, SchwabRestQuoteRecordRequest, SchwabSealedStreamerCapture,
    SchwabStreamerConnection, SchwabStreamerConnectionControl,
    SchwabStreamerConnectionControlSource, SchwabStreamerConnector, SchwabStreamerExecutor,
    SchwabStreamerFamilyDoctorAccumulator, SchwabStreamerFieldDictionary,
    SchwabStreamerQuoteMarketDataEvidence, SchwabStreamerQuotePublicationOutcome,
    SchwabStreamerQuotePublicationRequest, SchwabStreamerQuoteRecordRequest,
    SchwabStreamerSemanticField, SchwabTransportError, SchwabTransportTelemetry, StreamerAdmission,
    StreamerCaptureSink, StreamerCaptureSinkError, StreamerMicrobatch, StreamerResponseCode,
    StreamerSubscription, StreamerTransportBounds, TokenAuthorityError, TokenDecision,
    TransientAccessToken, build_instrument_search_request, build_market_hours_request,
    build_movers_request, canonicalize_option_chain, canonicalize_streamer_batch,
    parse_option_chain_response, parse_quote_response, parse_streamer_frame, parse_token_response,
    parse_user_preference,
};

use crate::canonical::{SchwabDailyPriceHistoryCandidateRequest, prepare_price_history_candidate};

#[derive(Debug)]
struct TestDailyHistoryCalendarRangeReceipt {
    publication_source_id: SourceId,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_request_digest: EvidenceDigest,
    venue_id: VenueId,
    interval: SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    evaluated_at: Timestamp,
    expires_at: Timestamp,
    completeness_evidence: EvidenceDigest,
    calendar_evidence: EvidenceDigest,
    receipt_digest: EvidenceDigest,
    periods: Box<[BarTimeSemantics]>,
}

impl SchwabDailyPriceHistoryCalendarRangeReceipt for TestDailyHistoryCalendarRangeReceipt {
    fn publication_source_id(&self) -> &SourceId {
        &self.publication_source_id
    }

    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    fn instrument_revision_digest(&self) -> EvidenceDigest {
        self.instrument_revision_digest
    }

    fn admitted_plan_digest(&self) -> EvidenceDigest {
        self.admitted_plan_digest
    }

    fn provider_request_digest(&self) -> EvidenceDigest {
        self.provider_request_digest
    }

    fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    fn adjustment(&self) -> MarketBarAdjustment {
        MarketBarAdjustment::Raw
    }

    fn requested_start(&self) -> Timestamp {
        self.requested_start
    }

    fn requested_end(&self) -> Timestamp {
        self.requested_end
    }

    fn knowledge_cutoff(&self) -> Timestamp {
        self.evaluated_at
    }

    fn evaluated_at(&self) -> Timestamp {
        self.evaluated_at
    }

    fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }

    fn calendar_evidence(&self) -> EvidenceDigest {
        self.calendar_evidence
    }

    fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }

    fn periods(&self) -> &[BarTimeSemantics] {
        &self.periods
    }

    fn validate_current_at(&self, checked_at: Timestamp) -> bool {
        checked_at >= self.evaluated_at && checked_at < self.expires_at
    }
}

struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("market-squawk-schwab-{}", Uuid::new_v4())))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ignored = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
struct ShortLivedOAuthWire {
    expires_in: u64,
    scope: Option<&'static str>,
}

impl Default for ShortLivedOAuthWire {
    fn default() -> Self {
        Self {
            expires_in: 30,
            scope: Some("market-data Quotes Market-Data"),
        }
    }
}

impl SchwabOAuthWire for ShortLivedOAuthWire {
    fn exchange(
        &self,
        _request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut response = serde_json::json!({
                "access_token": "short-access",
                "refresh_token": "short-refresh",
                "token_type": "Bearer",
                "expires_in": self.expires_in,
            });
            if let Some(scope) = self.scope {
                response["scope"] = serde_json::json!(scope);
            }
            SchwabOAuthWireResponse::try_new(
                200,
                serde_json::to_vec(&response).expect("bounded OAuth fixture response"),
                nonzero(4 * 1024),
            )
        })
    }
}

fn nonzero(value: usize) -> NonZeroUsize {
    match NonZeroUsize::new(value) {
        Some(value) => value,
        None => unreachable!("test value is nonzero"),
    }
}

fn admission() -> RequestAdmission {
    RequestAdmission::new(nonzero(16 * 1024), nonzero(8))
}

fn bounds() -> ParseBounds {
    ParseBounds::new(
        nonzero(64 * 1024),
        nonzero(64),
        nonzero(2_048),
        nonzero(16),
        32,
        8 * 1024,
    )
}

#[derive(Debug)]
struct BrowserProbeTlsAcceptor {
    attempts: AtomicUsize,
}

impl OAuthLoopbackTlsAcceptor for BrowserProbeTlsAcceptor {
    fn accept(&self, stream: TcpStream) -> OAuthLoopbackTlsAcceptFuture<'_> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if attempt == 0 {
                Err(OAuthLoopbackTlsAcceptError)
            } else {
                let stream: Box<dyn OAuthLoopbackTlsStream> = Box::new(stream);
                Ok(stream)
            }
        })
    }
}

#[tokio::test]
async fn oauth_lifecycle_and_read_only_route_allowlist_fail_closed() {
    let callback = OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=one-time&session=s1&state=correlation",
        "correlation",
        admission(),
    );
    let callback = match callback {
        Ok(CallbackOutcome::Authorized(callback)) => callback,
        outcome => panic!("unexpected callback outcome: {outcome:?}"),
    };
    assert_eq!(callback.expose_code(), "one-time");
    assert_eq!(callback.expose_session(), Some("s1"));
    assert!(matches!(
        OAuthCallback::parse(
            "https://localhost:8182/?code=one-time&state=correlation",
            "correlation",
            admission(),
        ),
        Err(SchwabAdapterError::InvalidCallback)
    ));
    assert!(matches!(
        OAuthCallback::parse(
            "https://127.0.0.1:8182/?code=one-time&state=correlation&error_description=mixed",
            "correlation",
            admission(),
        ),
        Err(SchwabAdapterError::InvalidCallback)
    ));

    let refresh = match RefreshTokenGeneration::try_new(NonZeroU64::MIN, 1_000) {
        Ok(value) => value,
        Err(error) => panic!("refresh generation rejected: {error}"),
    };
    let response = br#"{"access_token":"access","refresh_token":"refresh","token_type":"Bearer","expires_in":1800,"scope":"market-data"}"#;
    let (tokens, lifecycle) = match parse_token_response(response, 1_010, refresh, bounds()) {
        Ok(value) => value,
        Err(error) => panic!("token response rejected: {error}"),
    };
    assert_eq!(tokens.expose_access_token(), "access");
    assert_eq!(lifecycle.decision(1_010, 60), Ok(TokenDecision::Fresh));
    assert_eq!(
        lifecycle.decision(1_010 + ACCESS_TOKEN_MAX_LIFETIME_SECONDS - 60, 60),
        Ok(TokenDecision::Refresh)
    );
    assert_eq!(
        lifecycle.decision(refresh.expires_at_unix_seconds(), 60),
        Ok(TokenDecision::Reauthorize)
    );

    let extended = br#"{"access_token":"access","refresh_token":"refresh","token_type":"Bearer","expires_in":1800,"scope":"market-data","id_token":"ignored-identity","provider_extension":{"scope":"trading","expires_in":999999}}"#;
    let (extended_tokens, extended_lifecycle) =
        parse_token_response(extended, 1_010, refresh, bounds())
            .unwrap_or_else(|error| panic!("token extension response rejected: {error}"));
    assert_eq!(
        extended_tokens.expose_access_token(),
        tokens.expose_access_token()
    );
    assert_eq!(
        extended_tokens.expose_refresh_token(),
        tokens.expose_refresh_token()
    );
    assert_eq!(extended_tokens.scope(), tokens.scope());
    assert_eq!(extended_lifecycle, lifecycle);
    let duplicated = br#"{"access_token":"access","access_token":"replacement","refresh_token":"refresh","token_type":"Bearer","expires_in":1800,"id_token":"ignored-identity"}"#;
    assert!(matches!(
        parse_token_response(duplicated, 1_010, refresh, bounds()),
        Err(SchwabAdapterError::SchemaViolation)
    ));

    let quote = QuoteRequest::try_new(
        vec![
            ProviderIdentifier::try_new("AAPL").unwrap_or_else(|error| panic!("symbol: {error}")),
            ProviderIdentifier::try_new("SPY").unwrap_or_else(|error| panic!("symbol: {error}")),
        ],
        Vec::new(),
        None,
        admission(),
    )
    .unwrap_or_else(|error| panic!("quote request: {error}"));
    assert_eq!(quote.request().route(), ReadOnlyRoute::Quotes);
    assert_eq!(quote.request().requested_items(), 2);
    assert_eq!(
        ReadOnlyRoute::classify(
            HttpMethod::Get,
            "https://api.schwabapi.com/trader/v1/accounts"
        ),
        Err(SchwabAdapterError::RouteNotAllowed)
    );
    assert_eq!(
        ReadOnlyRoute::classify(
            HttpMethod::Get,
            "https://api.schwabapi.com/trader/v1/accounts/1/orders"
        ),
        Err(SchwabAdapterError::RouteNotAllowed)
    );
    assert_eq!(
        ReadOnlyRoute::classify(HttpMethod::Post, quote.request().url()),
        Err(SchwabAdapterError::RouteNotAllowed)
    );
    for forbidden in [
        "https://api.schwabapi.com/trader/v1/accounts/1/positions",
        "https://api.schwabapi.com/trader/v1/accounts/1/transactions",
        "https://api.schwabapi.com/trader/v1/accounts/1/orders/preview",
        "https://api.schwabapi.com/trader/v1/accounts/1/orders/replace",
        "https://api.schwabapi.com/trader/v1/accounts/1/orders/cancel",
        "https://api.schwabapi.com/trader/v1/accounts/1/money-movements",
    ] {
        assert_eq!(
            ReadOnlyRoute::classify(HttpMethod::Get, forbidden),
            Err(SchwabAdapterError::RouteNotAllowed)
        );
        assert_eq!(
            ReadOnlyRoute::classify(HttpMethod::Post, forbidden),
            Err(SchwabAdapterError::RouteNotAllowed)
        );
    }
    let chain = ChainRequest::new(
        ProviderIdentifier::try_new("SPY").unwrap_or_else(|error| panic!("symbol: {error}")),
    )
    .build(admission())
    .unwrap_or_else(|error| panic!("chain request: {error}"));
    assert_eq!(chain.route(), ReadOnlyRoute::Chains);

    for (request, authorized) in [
        (
            "GET /?code=one-time-browser&session=private-session&state=correlation HTTP/1.1\r\nHost: 127.0.0.1:8182\r\n\r\n",
            true,
        ),
        (
            "GET /?error=private-denial&error_description=private-description&state=correlation HTTP/1.1\r\nHost: 127.0.0.1:8182\r\n\r\n",
            false,
        ),
    ] {
        let tls = Arc::new(BrowserProbeTlsAcceptor {
            attempts: AtomicUsize::new(0),
        });
        let receiver = OAuthLoopbackReceiver::bind(
            tls.clone(),
            OAuthLoopbackBounds::try_new(
                Duration::from_secs(2),
                Duration::from_millis(250),
                Duration::from_millis(250),
                nonzero(2),
                nonzero(4 * 1024),
                nonzero(16),
            )
            .unwrap_or_else(|error| panic!("callback bounds: {error}")),
        )
        .await
        .unwrap_or_else(|error| panic!("callback listener: {error}"));
        let receive = tokio::spawn(async move {
            receiver
                .receive("correlation", CancellationToken::new())
                .await
        });
        let browser_probe = TcpStream::connect("127.0.0.1:8182")
            .await
            .unwrap_or_else(|error| panic!("browser TLS probe: {error}"));
        drop(browser_probe);
        let mut callback = TcpStream::connect("127.0.0.1:8182")
            .await
            .unwrap_or_else(|error| panic!("browser callback: {error}"));
        callback
            .write_all(request.as_bytes())
            .await
            .unwrap_or_else(|error| panic!("write browser callback: {error}"));
        let mut acknowledgement = Vec::new();
        callback
            .read_to_end(&mut acknowledgement)
            .await
            .unwrap_or_else(|error| panic!("read browser acknowledgement: {error}"));
        assert!(acknowledgement.starts_with(b"HTTP/1.1 200 OK\r\n"));
        let acknowledgement = std::str::from_utf8(&acknowledgement)
            .unwrap_or_else(|error| panic!("acknowledgement UTF-8: {error}"));
        let (headers, body) = acknowledgement
            .split_once("\r\n\r\n")
            .unwrap_or_else(|| panic!("missing acknowledgement header boundary"));
        assert!(headers.contains("Content-Type: text/html; charset=utf-8\r\n"));
        assert!(headers.contains("Cache-Control: no-store\r\n"));
        assert!(
            headers
                .contains("Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'")
        );
        assert!(headers.contains("Referrer-Policy: no-referrer\r\n"));
        let content_length = headers
            .split("\r\n")
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .unwrap_or_else(|| panic!("missing acknowledgement length"))
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("acknowledgement length: {error}"));
        assert_eq!(content_length, body.len());
        assert!(body.starts_with("<!doctype html>"));
        for private_value in [
            "one-time-browser",
            "private-session",
            "correlation",
            "private-denial",
            "private-description",
            "127.0.0.1:8182",
        ] {
            assert!(!acknowledgement.contains(private_value));
        }
        let outcome = receive
            .await
            .unwrap_or_else(|error| panic!("callback task: {error}"))
            .unwrap_or_else(|error| panic!("callback receive: {error}"));
        match (authorized, outcome) {
            (true, CallbackOutcome::Authorized(callback)) => {
                assert_eq!(callback.expose_code(), "one-time-browser");
                assert_eq!(callback.expose_session(), Some("private-session"));
            }
            (false, CallbackOutcome::Denied { error, description }) => {
                assert_eq!(error.as_ref(), "private-denial");
                assert_eq!(description.as_deref(), Some("private-description"));
            }
            _ => panic!("unexpected browser callback outcome"),
        }
        assert_eq!(tls.attempts.load(Ordering::SeqCst), 2);
    }
}

#[test]
fn rest_and_streamer_native_parsing_preserve_evidence_and_one_connection_semantics() {
    use crate::{
        NativeField, NativeFieldEntry, NativeScalar, OptionContractField, StreamerMetadataField,
    };

    let quote = br#"{
      "AAPL": {
        "assetMainType":"EQUITY", "assetSubType":"COE", "realtime":true,
        "quote":{"bidPrice":100.125,"askPrice":100.25,"bidSize":2,"askSize":3,"quoteTime":1710000000000,"futureField":"retained-only-raw"},
        "reference":{"cusip":"037833100","exchange":"Q"}
      }
    }"#;
    let parsed = parse_quote_response(quote, bounds())
        .unwrap_or_else(|error| panic!("quote payload: {error}"));
    assert_eq!(parsed.value().quotes().len(), 1);
    assert_eq!(parsed.value().quotes()[0].quote_fields().len(), 5);
    assert_eq!(parsed.unknown_fields().field_count(), 1);
    assert_eq!(
        parsed.unknown_fields().paths()[0].as_ref(),
        "$.AAPL.quote.futureField"
    );

    let chain = br#"{
      "symbol":"SPY","status":"SUCCESS","strategy":"SINGLE","numberOfContracts":2,
      "underlyingPrice":500.1,"daysToExpiration":1.0,
      "callExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"CALL","symbol":"SPY C","bid":1.2,"ask":1.3,"volatility":0.2,"delta":0.51,"gamma":0.03,"theta":-0.02,"vega":0.08,"rho":0.04,"openInterest":10}]}},
      "putExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"PUT","symbol":"SPY P","bid":1.1,"ask":1.4,"volatility":0.21,"delta":-0.49,"gamma":0.03,"theta":-0.02,"vega":0.08,"rho":-0.04,"openInterest":11}]}}
    }"#;
    let parsed_chain = parse_option_chain_response(chain, bounds())
        .unwrap_or_else(|error| panic!("chain payload: {error}"));
    assert_eq!(parsed_chain.value().contracts().len(), 2);
    assert!(matches!(
        parsed_chain.value().days_to_expiration(),
        NativeField::Value(value) if value.as_str() == "1.0"
    ));
    let option_candidates = canonicalize_option_chain(
        &parsed_chain,
        Timestamp::from_unix_nanos(1_710_000_000_000_000_000),
    )
    .unwrap_or_else(|error| panic!("option canonicalization: {error}"));
    assert_eq!(option_candidates.len(), 2);
    assert!(
        option_candidates
            .iter()
            .all(|value| matches!(value, SchwabOptionCandidateOutcome::Mapped(_)))
    );

    // The production response has 32 expirations, two strikes and both sides. Ordinary
    // scalar fields must not exhaust unknown-field admission; nested deliverables stay raw.
    let mut full_chain: serde_json::Value =
        serde_json::from_slice(chain).unwrap_or_else(|error| panic!("chain fixture: {error}"));
    let extra_fields = serde_json::json!({
        "bidAskSize":"2X3", "intrinsicValue":1.25, "extrinsicValue":0.5,
        "optionRoot":"SPY", "exerciseType":"A", "high52Week":5.25,
        "low52Week":0.25, "breakEven":501.25, "ssid":123456, "pennyPilot":true,
        "optionDeliverablesList":[{"symbol":"SPY","deliverableUnits":100.0}]
    });
    for (map_name, side) in [("callExpDateMap", "C"), ("putExpDateMap", "P")] {
        let template = full_chain[map_name]["2026-08-21:10"]["500.0"][0].clone();
        let mut expirations = serde_json::Map::new();
        for day in 0_u64..32 {
            let expiration = chrono::NaiveDate::from_ymd_opt(2026, 11, 1)
                .and_then(|date| date.checked_add_days(chrono::Days::new(day)))
                .expect("bounded fixture expiration");
            let mut strikes = serde_json::Map::new();
            for strike in [500, 501] {
                let mut contract = template.clone();
                contract.as_object_mut().expect("contract object").extend(
                    extra_fields
                        .as_object()
                        .expect("scalar fixture fields")
                        .clone(),
                );
                contract["symbol"] = serde_json::json!(format!("SPY {expiration}{side}{strike}"));
                strikes.insert(format!("{strike}.0"), serde_json::json!([contract]));
            }
            expirations.insert(format!("{expiration}:{}", day + 1), strikes.into());
        }
        full_chain[map_name] = expirations.into();
    }
    full_chain["numberOfContracts"] = serde_json::json!(128);
    let full_chain = serde_json::to_vec(&full_chain)
        .unwrap_or_else(|error| panic!("full chain fixture: {error}"));
    let production_bounds = ParseBounds::new(
        nonzero(4 * 1024 * 1024),
        nonzero(8 * 1024),
        nonzero(256 * 1024),
        nonzero(64),
        512,
        512 * 1024,
    );
    let full_chain = parse_option_chain_response(&full_chain, production_bounds)
        .unwrap_or_else(|error| panic!("production-shaped chain: {error}"));
    assert_eq!(full_chain.value().contracts().len(), 128);
    assert_eq!(
        full_chain.value().number_of_contracts(),
        &NativeField::Value(128)
    );
    assert!(matches!(
        full_chain.value().days_to_expiration(),
        NativeField::Value(value) if value.as_str() == "1.0"
    ));
    assert_eq!(full_chain.unknown_fields().field_count(), 128);
    assert!(full_chain.unknown_fields().encoded_bytes() > 0);
    assert!(
        full_chain
            .unknown_fields()
            .paths()
            .iter()
            .all(|path| { path.ends_with(".optionDeliverablesList") })
    );
    for contract in full_chain.value().contracts() {
        let field = |name| {
            contract
                .fields()
                .iter()
                .find(|entry| *entry.name() == name)
                .unwrap_or_else(|| panic!("missing observed scalar {name:?}"))
                .value()
        };
        for (name, expected) in [
            (OptionContractField::IntrinsicValue, "1.25"),
            (OptionContractField::ExtrinsicValue, "0.5"),
            (OptionContractField::High52Week, "5.25"),
            (OptionContractField::Low52Week, "0.25"),
            (OptionContractField::BreakEven, "501.25"),
            (OptionContractField::Ssid, "123456"),
        ] {
            assert_eq!(
                field(name).number().map(|value| value.as_str()),
                Some(expected)
            );
        }
        for (name, expected) in [
            (OptionContractField::BidAskSize, "2X3"),
            (OptionContractField::OptionRoot, "SPY"),
            (OptionContractField::ExerciseType, "A"),
        ] {
            assert_eq!(field(name).text(), Some(expected));
        }
        assert_eq!(
            field(OptionContractField::PennyPilot),
            &NativeScalar::Bool(true)
        );
    }

    let preference = br#"{
      "accounts":[{"accountNumber":"must-not-escape"}],
      "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
      "offers":[{"mktDataPermission":"NP","level2Permissions":true,"accountOffer":"must-not-escape"}]
    }"#;
    let bootstrap = parse_user_preference(preference, bounds())
        .unwrap_or_else(|error| panic!("bootstrap payload: {error}"));
    assert_eq!(
        bootstrap.value().socket_url(),
        "wss://streamer.example.test/ws"
    );
    assert_eq!(bootstrap.value().market_data_permission(), Some("NP"));
    assert_ne!(bootstrap.value().market_data_principal_sha256(), [0; 32]);
    assert!(bootstrap.unknown_fields().field_count() >= 2);

    let stream = br#"{
      "response":[{"service":"LEVELONE_EQUITIES","command":"SUBS","requestid":"2","timestamp":1710000000000,"content":{"code":0,"msg":"OK"}}],
      "data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","timestamp":1710000000001,"content":[{"key":"AAPL","delayed":false,"assetMainType":"EQUITY","assetSubType":"COE","cusip":"TEST00001","futureMetadata":{"retained":"only-in-raw"},"1":100.125,"2":100.25}]}]
    }"#;
    let frame = parse_streamer_frame(stream, bounds())
        .unwrap_or_else(|error| panic!("stream frame: {error}"));
    assert_eq!(
        frame.value().responses[0].code,
        StreamerResponseCode::Success
    );
    assert_eq!(frame.value().data[0].content[0].fields.len(), 2);
    let expected_metadata = [
        NativeFieldEntry::new(
            StreamerMetadataField::AssetMainType,
            NativeScalar::Text("EQUITY".into()),
        ),
        NativeFieldEntry::new(
            StreamerMetadataField::AssetSubType,
            NativeScalar::Text("COE".into()),
        ),
        NativeFieldEntry::new(
            StreamerMetadataField::Cusip,
            NativeScalar::Text("TEST00001".into()),
        ),
        NativeFieldEntry::new(StreamerMetadataField::Delayed, NativeScalar::Bool(false)),
    ];
    assert_eq!(
        frame.value().data[0].content[0].metadata.as_ref(),
        expected_metadata.as_slice()
    );
    assert_eq!(
        frame.raw_sha256(),
        <[u8; 32]>::from(sha2::Sha256::digest(stream))
    );
    assert_eq!(frame.unknown_fields().field_count(), 1);
    assert_eq!(
        frame.unknown_fields().paths()[0].as_ref(),
        "$.data[].content[].futureMetadata"
    );
    assert!(frame.unknown_fields().encoded_bytes() > 0);
    assert_ne!(frame.unknown_fields().digest(), [0; 32]);
    assert_eq!(
        expected_metadata.map(|entry| entry.name().as_str()),
        ["assetMainType", "assetSubType", "cusip", "delayed"]
    );
    let dictionary = SchwabStreamerFieldDictionary::official(MarketDataService::LevelOneEquities)
        .unwrap_or_else(|error| panic!("official dictionary: {error}"));
    assert_eq!(
        dictionary.field_ids().collect::<Vec<_>>(),
        vec![0, 1, 2, 4, 5, 34]
    );
    assert_eq!(
        dictionary.evidence(),
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            sha2::Sha256::digest(include_bytes!(
                "../resources/market-data-documentation-20260916.json"
            ))
            .into(),
        ),
    );
    let mapped = canonicalize_streamer_batch(&frame.value().data[0], &dictionary)
        .unwrap_or_else(|error| panic!("stream canonicalization: {error}"));
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].fields.len(), 2);
    assert_eq!(
        mapped[0].metadata,
        frame.value().data[0].content[0].metadata
    );
    assert_eq!(
        mapped[0].fields[0].meaning,
        SchwabStreamerSemanticField::BidPrice
    );
    assert_eq!(
        mapped[0].fields[1].meaning,
        SchwabStreamerSemanticField::AskPrice
    );
    let incomplete_dictionary = SchwabStreamerFieldDictionary::try_new(
        MarketDataService::LevelOneEquities,
        SourceIdentifier::try_from("schwab-streamer-test-fixture-v2")
            .unwrap_or_else(|error| panic!("dictionary version: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [8; 32]),
        vec![(1, SchwabStreamerSemanticField::BidPrice)],
    )
    .unwrap_or_else(|error| panic!("dictionary: {error}"));
    assert_eq!(
        canonicalize_streamer_batch(&frame.value().data[0], &incomplete_dictionary),
        Err(SchwabCanonicalError::UnknownStreamerField { field_id: 2 })
    );

    let mut metadata_fixture: serde_json::Value =
        serde_json::from_slice(stream).expect("named Streamer metadata fixture");
    for field in ["delayed", "assetMainType", "assetSubType", "cusip"] {
        metadata_fixture["data"][0]["content"][0][field] = serde_json::Value::Null;
    }
    let null_metadata = parse_streamer_frame(
        &serde_json::to_vec(&metadata_fixture).expect("null metadata encoding"),
        bounds(),
    )
    .expect("explicit null metadata remains distinct from absence");
    assert_eq!(null_metadata.value().data[0].content[0].metadata.len(), 4);
    assert!(
        null_metadata.value().data[0].content[0]
            .metadata
            .iter()
            .all(|entry| entry.value() == &NativeScalar::Null)
    );
    for (field, invalid) in [
        ("delayed", serde_json::json!("false")),
        ("assetMainType", serde_json::json!(true)),
        ("assetSubType", serde_json::json!(1)),
        ("cusip", serde_json::json!({"value": "TEST00001"})),
    ] {
        let mut invalid_fixture = metadata_fixture.clone();
        invalid_fixture["data"][0]["content"][0][field] = invalid;
        assert!(matches!(
            parse_streamer_frame(
                &serde_json::to_vec(&invalid_fixture).expect("invalid metadata encoding"),
                bounds(),
            ),
            Err(SchwabAdapterError::SchemaViolation)
        ));
    }
    for malformed_id in ["1x", "65536"] {
        let mut invalid_fixture = metadata_fixture.clone();
        invalid_fixture["data"][0]["content"][0][malformed_id] = serde_json::json!(1);
        assert!(matches!(
            parse_streamer_frame(
                &serde_json::to_vec(&invalid_fixture).expect("invalid field ID encoding"),
                bounds(),
            ),
            Err(SchwabAdapterError::SchemaViolation)
        ));
    }
    let delta = parse_streamer_frame(
        br#"{"data":[{"service":"LEVELONE_EQUITIES","command":"ADD","content":[{"key":"AAPL","1":100.5}]}]}"#,
        bounds(),
    )
    .expect("delta may omit named metadata");
    assert!(delta.value().data[0].content[0].metadata.is_empty());
    assert!(
        canonicalize_streamer_batch(&delta.value().data[0], &dictionary)
            .expect("canonical delta retains absent metadata")[0]
            .metadata
            .is_empty()
    );

    let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
    let subscription = StreamerSubscription::try_new(
        MarketDataService::LevelOneEquities,
        vec![ProviderIdentifier::try_new("AAPL").unwrap_or_else(|error| panic!("symbol: {error}"))],
        vec![0, 1, 2],
        stream_admission,
    )
    .unwrap_or_else(|error| panic!("subscription: {error}"));
    let mut controller = DesiredStateController::new(stream_admission);
    assert!(matches!(
        controller.replace_desired(subscription.clone()),
        Ok(None)
    ));
    let generation = ConnectionGeneration::new(NonZeroU64::MIN);
    assert_eq!(controller.begin_connect(generation), Ok(()));
    assert_eq!(
        controller.begin_connect(generation),
        Err(SchwabAdapterError::InvalidStreamerState)
    );
    assert_eq!(controller.socket_connected(generation), Ok(()));
    assert_eq!(
        controller.login_accepted(generation),
        Err(SchwabAdapterError::InvalidStreamerState)
    );
    assert!(matches!(
        controller.login_request(bootstrap.value(), ""),
        Err(SchwabAdapterError::InvalidStreamerState)
    ));
    assert_eq!(
        controller.login_accepted(generation),
        Err(SchwabAdapterError::InvalidStreamerState)
    );
    let assert_coordinates = |request: &crate::TransientStreamerRequest,
                              command: &str,
                              customer: &str,
                              correlation: &str| {
        let wire: serde_json::Value = serde_json::from_slice(request.expose_body())
            .unwrap_or_else(|error| panic!("command wire: {error}"));
        assert_eq!(wire["requests"].as_array().map(Vec::len), Some(1));
        let request_wire = &wire["requests"][0];
        assert_eq!(request_wire["command"], command);
        assert_eq!(request_wire["SchwabClientCustomerId"], customer);
        assert_eq!(request_wire["SchwabClientCorrelId"], correlation);
        if command != "LOGIN" {
            assert!(request_wire["parameters"].get("Authorization").is_none());
            let encoded = String::from_utf8_lossy(request.expose_body());
            assert!(!encoded.contains("streamer-test-access-token"));
            assert!(!encoded.contains("must-not-escape"));
            assert!(!encoded.contains("ACCOUNT_ACTIVITY"));
        }
    };
    let login = controller
        .login_request(bootstrap.value(), "streamer-test-access-token")
        .unwrap_or_else(|error| panic!("LOGIN request: {error}"));
    assert_coordinates(&login, "LOGIN", "customer", "correlation");
    drop(login);
    let debug = format!("{controller:?}");
    assert!(!debug.contains("\"customer\""));
    assert!(!debug.contains("\"correlation\""));
    assert!(!debug.contains("streamer-test-access-token"));
    assert!(matches!(
        controller.login_request(bootstrap.value(), "streamer-test-access-token"),
        Err(SchwabAdapterError::InvalidStreamerState)
    ));
    assert_eq!(controller.login_accepted(generation), Ok(()));
    assert_eq!(controller.state(), ConnectionState::Active(generation));
    let requests = controller
        .replay_desired()
        .unwrap_or_else(|error| panic!("desired replay: {error}"));
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].command(), "SUBS");
    assert_coordinates(&requests[0], "SUBS", "customer", "correlation");
    let replacement = controller
        .replace_desired(subscription.clone())
        .unwrap_or_else(|error| panic!("active SUBS: {error}"))
        .expect("active replacement must encode SUBS");
    assert_coordinates(&replacement, "SUBS", "customer", "correlation");
    let addition = StreamerSubscription::try_new(
        MarketDataService::LevelOneEquities,
        vec![ProviderIdentifier::try_new("MSFT").expect("bounded addition symbol")],
        vec![0, 1, 2],
        stream_admission,
    )
    .expect("bounded addition");
    let added = controller
        .add_desired(addition.clone())
        .unwrap_or_else(|error| panic!("active ADD: {error}"))
        .expect("active addition must encode ADD");
    assert_coordinates(&added, "ADD", "customer", "correlation");
    let removed = controller
        .remove_desired(addition)
        .unwrap_or_else(|error| panic!("active UNSUBS: {error}"))
        .expect("active removal must encode UNSUBS");
    assert_coordinates(&removed, "UNSUBS", "customer", "correlation");

    assert_eq!(controller.disconnected(generation), Ok(()));
    let next_generation = ConnectionGeneration::new(NonZeroU64::new(2).expect("next generation"));
    assert_eq!(controller.begin_connect(next_generation), Ok(()));
    assert_eq!(controller.socket_connected(next_generation), Ok(()));
    assert_eq!(
        controller.login_accepted(next_generation),
        Err(SchwabAdapterError::InvalidStreamerState)
    );
    let mut next_preference: serde_json::Value = serde_json::from_slice(preference)
        .unwrap_or_else(|error| panic!("next preference fixture: {error}"));
    next_preference["streamerInfo"][0]["schwabClientCustomerId"] =
        serde_json::json!("next-customer");
    next_preference["streamerInfo"][0]["schwabClientCorrelId"] =
        serde_json::json!("next-correlation");
    let next_preference = serde_json::to_vec(&next_preference)
        .unwrap_or_else(|error| panic!("next preference encoding: {error}"));
    let next_bootstrap = parse_user_preference(&next_preference, bounds())
        .unwrap_or_else(|error| panic!("next bootstrap: {error}"));
    let next_login = controller
        .login_request(next_bootstrap.value(), "streamer-test-access-token")
        .unwrap_or_else(|error| panic!("next LOGIN: {error}"));
    assert_coordinates(&next_login, "LOGIN", "next-customer", "next-correlation");
    drop(next_login);
    assert_eq!(controller.login_accepted(next_generation), Ok(()));
    let replayed = controller
        .replay_desired()
        .unwrap_or_else(|error| panic!("next desired replay: {error}"));
    assert_eq!(replayed.len(), 1);
    assert_coordinates(&replayed[0], "SUBS", "next-customer", "next-correlation");
    assert_eq!(controller.disconnected(next_generation), Ok(()));
    assert!(matches!(
        controller.replay_desired(),
        Err(SchwabAdapterError::InvalidStreamerState)
    ));

    for command in ["LOGIN", "SUBS", "ADD", "UNSUBS"] {
        assert!(StreamerResponseCode::Success.is_success_for(command));
        assert!(!StreamerResponseCode::SymbolLimit.is_success_for(command));
        assert!(!StreamerResponseCode::Other(21).is_success_for(command));
        assert!(!StreamerResponseCode::Other(999).is_success_for(command));
    }
    for (code, command) in [(26, "SUBS"), (27, "UNSUBS"), (28, "ADD")] {
        let response = serde_json::to_vec(&serde_json::json!({
            "response": [{
                "service": "LEVELONE_EQUITIES", "command": command,
                "requestid": "2", "timestamp": 1710000000000_u64,
                "content": {"code": code, "msg": "command succeeded"}
            }]
        }))
        .expect("bounded successful reply fixture");
        let parsed = parse_streamer_frame(&response, bounds())
            .unwrap_or_else(|error| panic!("command success response: {error}"));
        let response = &parsed.value().responses[0];
        assert_eq!(response.code, StreamerResponseCode::Other(code));
        assert!(response.code.is_success_for(&response.command));
        for candidate in ["LOGIN", "SUBS", "ADD", "UNSUBS", "VIEW"] {
            assert_eq!(
                response.code.is_success_for(candidate),
                candidate == command
            );
        }
    }
    assert!(!StreamerResponseCode::Other(29).is_success_for("VIEW"));

    let encoding_limited = StreamerAdmission::new(
        RequestAdmission::new(nonzero(1), nonzero(1)),
        nonzero(1),
        nonzero(16),
    );
    let mut encoding_limited = DesiredStateController::new(encoding_limited);
    assert_eq!(encoding_limited.begin_connect(generation), Ok(()));
    assert_eq!(encoding_limited.socket_connected(generation), Ok(()));
    assert!(matches!(
        encoding_limited.login_request(bootstrap.value(), "streamer-test-access-token"),
        Err(SchwabAdapterError::RequestNotAdmitted)
    ));
    assert_eq!(
        encoding_limited.login_accepted(generation),
        Err(SchwabAdapterError::InvalidStreamerState)
    );

    let one_service_admission = StreamerAdmission::new(admission(), nonzero(1), nonzero(16));
    let mut bounded = DesiredStateController::new(one_service_admission);
    bounded
        .add_desired(
            StreamerSubscription::try_new(
                MarketDataService::LevelOneEquities,
                vec![
                    ProviderIdentifier::try_new("AAPL")
                        .unwrap_or_else(|error| panic!("symbol: {error}")),
                ],
                vec![1],
                one_service_admission,
            )
            .unwrap_or_else(|error| panic!("subscription: {error}")),
        )
        .unwrap_or_else(|error| panic!("first service: {error}"));
    assert!(matches!(
        bounded.add_desired(
            StreamerSubscription::try_new(
                MarketDataService::LevelOneOptions,
                vec![
                    ProviderIdentifier::try_new("SPY  260821C00500000")
                        .unwrap_or_else(|error| panic!("option symbol: {error}"))
                ],
                vec![1],
                one_service_admission,
            )
            .unwrap_or_else(|error| panic!("subscription: {error}")),
        ),
        Err(SchwabAdapterError::RequestNotAdmitted)
    ));
}

#[test]
fn option_contract_missing_greek_retains_component_unavailability() {
    let chain = br#"{
      "symbol":"SPY","status":"SUCCESS","strategy":"SINGLE","numberOfContracts":1,
      "callExpDateMap":{"2026-08-21:10":{"500.0":[{
        "putCall":"CALL","symbol":"SPY C","bid":1.2,"ask":1.3,
        "volatility":0.2,"delta":0.51,"gamma":0.03,"theta":-0.02,"vega":0.08
      }]}}
    }"#;
    let parsed = parse_option_chain_response(chain, bounds())
        .unwrap_or_else(|error| panic!("chain payload: {error}"));
    let outcomes = canonicalize_option_chain(
        &parsed,
        Timestamp::from_unix_nanos(1_710_000_000_000_000_000),
    )
    .unwrap_or_else(|error| panic!("option canonicalization: {error}"));

    let [SchwabOptionCandidateOutcome::Mapped(candidate)] = outcomes.as_slice() else {
        panic!("missing optional Greek must not abstain the whole contract");
    };
    assert_eq!(candidate.rho, SchwabCanonicalField::Absent);
}

#[tokio::test]
async fn rest_price_history_seals_raw_evidence_but_denies_unverified_bar_semantics() {
    let temporary = TemporaryDirectory::new();
    let secrets = Arc::new(
        EncryptedFileSecretStore::try_open(
            temporary.path().join("oauth-secrets"),
            SecretValue::new("schwab-test-unlock".to_owned())
                .unwrap_or_else(|error| panic!("OAuth unlock: {error}")),
        )
        .unwrap_or_else(|error| panic!("OAuth secret store: {error}")),
    );
    let secret_authority: Arc<dyn SecretStore> = secrets.clone();
    let secret_control = SecretOperationControl::try_new(
        "schwab-test-application",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .unwrap_or_else(|error| panic!("OAuth secret control: {error}"));
    let application_key = SecretKey::try_new("market-squawk.schwab", "test-application")
        .unwrap_or_else(|error| panic!("application secret key: {error}"));
    let application_credential = secrets
        .create(
            &application_key,
            SecretGeneration::new(1)
                .unwrap_or_else(|error| panic!("application generation: {error}")),
            SecretValue::new(
                r#"{"version":1,"app_key":"test-app-key","app_secret":"test-app-secret"}"#
                    .to_owned(),
            )
            .unwrap_or_else(|error| panic!("application secret: {error}")),
            &secret_control,
        )
        .unwrap_or_else(|error| panic!("application credential: {error}"));
    let token_admission = AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1));
    let oauth_configuration = SchwabOAuthAuthorityConfiguration::try_new(
        Arc::clone(&secret_authority),
        Arc::new(ShortLivedOAuthWire::default()),
        application_credential.clone(),
        SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)
            .unwrap_or_else(|error| panic!("OAuth secret policy: {error}")),
        bounds(),
        token_admission,
        5,
    )
    .unwrap_or_else(|error| panic!("OAuth authority configuration: {error}"));
    let oauth_authority = ProtectedSchwabOAuthAuthority::try_open(
        temporary.path().join("oauth-authority"),
        oauth_configuration,
    )
    .await
    .unwrap_or_else(|error| panic!("OAuth authority: {error}"));
    let authorization = oauth_authority
        .authorization_request(
            "short-lived",
            admission(),
            SchwabOAuthInteraction::Background,
        )
        .await
        .unwrap_or_else(|error| panic!("OAuth authorization request: {error}"));
    assert!(
        authorization
            .expose_url()
            .contains("client_id=test-app-key")
    );
    assert!(
        authorization
            .expose_url()
            .contains("redirect_uri=https%3A%2F%2F127.0.0.1%3A8182")
    );
    assert!(!authorization.expose_url().contains("test-app-secret"));
    let callback = match OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=one-time&state=short-lived",
        "short-lived",
        admission(),
    ) {
        Ok(CallbackOutcome::Authorized(callback)) => callback,
        outcome => panic!("OAuth callback: {outcome:?}"),
    };
    let issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("OAuth issue clock: {error}"))
        .as_secs();
    let completed_oauth_receipt = oauth_authority
        .complete_authorization(&callback, issued_at, SchwabOAuthInteraction::Background)
        .await
        .unwrap_or_else(|error| panic!("OAuth completion: {error}"));
    let token = SchwabAccessTokenSource::acquire(&oauth_authority)
        .await
        .unwrap_or_else(|error| panic!("OAuth token acquisition: {error}"));
    let oauth_receipt = match oauth_authority
        .status()
        .await
        .unwrap_or_else(|error| panic!("OAuth status: {error}"))
    {
        SchwabOAuthAuthorityStatus::Active(receipt) => receipt,
        status => panic!("OAuth authority is not active after token acquisition: {status:?}"),
    };
    assert!(oauth_receipt.generation() >= completed_oauth_receipt.generation());
    assert_eq!(token.generation(), oauth_receipt.generation());
    assert_eq!(
        token.credential_authority(),
        oauth_receipt.credential_authority()
    );
    let market_data_session = SourceIdentifier::try_from("8d9bc9ee-fca2-4f1d-a077-5104408e3727")
        .unwrap_or_else(|error| panic!("market-data qualification session: {error}"));
    let start_millis = 1_704_067_200_000;
    let end_millis = 1_704_153_600_000;
    let history_request = PriceHistoryRequest::new(
        ProviderIdentifier::try_new("SPY").unwrap_or_else(|error| panic!("symbol: {error}")),
    )
    .frequency(
        PriceHistoryFrequencyType::Daily,
        PriceHistoryFrequency::new(NonZeroU16::MIN),
    )
    .range_millis(start_millis, end_millis)
    .unwrap_or_else(|error| panic!("history range: {error}"))
    .build(admission())
    .unwrap_or_else(|error| panic!("history request: {error}"));
    assert_eq!(
        history_request.request_target(),
        "/marketdata/v1/pricehistory?symbol=SPY&frequencyType=daily&frequency=1&startDate=1704067200000&endDate=1704153600000"
    );
    let history_body: &'static [u8] = br#"{
      "symbol":"SPY","empty":false,
      "candles":[{"open":475.00,"high":477.00,"low":474.50,"close":476.25,"volume":1000,"datetime":1704067200000}]
    }"#;
    let history =
        execute_market_fixture(&history_request, history_body, &token, token_admission).await;
    let observed_at = history.capture().receipt().received_at_unix_millis() / 1_000;
    let capability =
        SchwabPriceHistoryCapabilityObservation::try_observe(oauth_receipt, &history, observed_at)
            .unwrap_or_else(|error| panic!("history capability: {error}"));
    let different_series_oauth = SchwabOAuthAuthorityReceipt::for_test(
        oauth_receipt.generation(),
        SchwabCredentialAuthorityBinding::for_test(
            oauth_receipt
                .credential_authority()
                .application_credential_generation(),
            92,
        ),
    );
    assert_eq!(
        SchwabPriceHistoryCapabilityObservation::try_observe(
            different_series_oauth,
            &history,
            observed_at,
        ),
        Err(crate::SchwabVerticalError::InvalidCapabilityEvidence)
    );

    assert_eq!(
        SchwabPriceHistoryCapabilityObservation::try_observe(
            oauth_receipt,
            &history,
            oauth_receipt.access_expires_at_unix_seconds(),
        ),
        Err(crate::SchwabVerticalError::InvalidCapabilityEvidence)
    );

    let registered_coordinates = capture_coordinates();
    let provider_timestamp = Timestamp::from_unix_nanos(1_704_067_200_000_000_000);
    let period_end = Timestamp::from_unix_nanos(1_704_153_600_000_000_000);
    let session = MarketBarSessionEvidence::try_new(
        MarketBarSessionKind::Regular,
        SourceIdentifier::try_from("xnys-2024-session-calendar")
            .unwrap_or_else(|error| panic!("session ruleset: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [21; 32]),
    )
    .unwrap_or_else(|error| panic!("session evidence: {error}"));
    let time_semantics = BarTimeSemantics::try_new(
        provider_timestamp,
        period_end,
        BarTimestampBasis::PeriodStart,
        session,
    )
    .unwrap_or_else(|error| panic!("bar time semantics: {error}"));
    let instrument_id = "06dd06da-ef2d-44dd-bf28-b006da06b24b"
        .parse::<InstrumentId>()
        .unwrap_or_else(|error| panic!("instrument: {error}"));
    let identity = SchwabResolvedProviderIdentity::try_new(
        ProviderIdentifier::try_new("SPY").unwrap_or_else(|error| panic!("symbol: {error}")),
        ProviderInstrumentId::try_from("SPY")
            .unwrap_or_else(|error| panic!("provider instrument: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [22; 32]),
    )
    .unwrap_or_else(|error| panic!("resolved identity: {error}"));
    let venue_id =
        VenueId::try_from("XNYS").unwrap_or_else(|error| panic!("venue identity: {error}"));
    let feed = SourceIdentifier::try_from("schwab-daily-price-history")
        .unwrap_or_else(|error| panic!("feed: {error}"));
    let interval =
        SourceIdentifier::try_from("1d").unwrap_or_else(|error| panic!("interval: {error}"));
    let currency = Currency::try_from("USD").unwrap_or_else(|error| panic!("currency: {error}"));
    let instrument_revision_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [23; 32]);
    let admitted_plan_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [24; 32]);
    let completeness_evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, [25; 32]);
    let received_at = Timestamp::from_unix_nanos(
        i64::try_from(history.capture().receipt().received_at_unix_millis())
            .unwrap_or_else(|error| panic!("receive clock: {error}"))
            * 1_000_000,
    );
    let calendar_range = Arc::new(TestDailyHistoryCalendarRangeReceipt {
        publication_source_id: registered_coordinates.source_id().clone(),
        instrument_id,
        instrument_revision_digest,
        admitted_plan_digest,
        provider_request_digest: EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            history.capture().receipt().request_sha256(),
        ),
        venue_id: venue_id.clone(),
        interval: interval.clone(),
        requested_start: provider_timestamp,
        requested_end: period_end,
        evaluated_at: received_at,
        expires_at: received_at
            .checked_add_nanos(60_000_000_000)
            .unwrap_or_else(|error| panic!("calendar receipt expiry: {error}")),
        completeness_evidence,
        calendar_evidence: EvidenceDigest::new(DigestAlgorithm::Sha256, [21; 32]),
        receipt_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [27; 32]),
        periods: vec![time_semantics.clone()].into_boxed_slice(),
    });
    let request_for = || SchwabDailyPriceHistoryCandidateRequest {
        capability,
        oauth_authority: oauth_receipt,
        receipt: history.capture().receipt(),
        payload: history.payload(),
        accounting: history.accounting(),
        instrument_id,
        instrument_revision_digest,
        admitted_plan_digest,
        identity: identity.clone(),
        venue_id: venue_id.clone(),
        feed: feed.clone(),
        interval: interval.clone(),
        adjustment: MarketBarAdjustment::Raw,
        currency,
        calendar_range: calendar_range.as_ref(),
        ingested_at: received_at,
    };

    let candidate = prepare_price_history_candidate(request_for()).unwrap_or_else(|error| {
        panic!("pending history candidate without UserPreference: {error}")
    });
    assert_eq!(candidate.instrument_id(), instrument_id);
    assert_eq!(candidate.provider_instrument_id().as_str(), "SPY");
    assert_eq!(candidate.provider_symbol().as_str(), "SPY");
    assert_eq!(candidate.bars().len(), 1);
    assert_eq!(candidate.bars()[0].provider_timestamp(), provider_timestamp);
    assert_eq!(candidate.bars()[0].time_semantics(), &time_semantics);
    assert_eq!(candidate.bars()[0].open().to_string(), "475.00");
    assert_eq!(candidate.bars()[0].close().to_string(), "476.25");
    assert_ne!(candidate.mapping_digest().bytes(), [0; 32]);

    let route = history.capture().receipt().route();
    let token_generation = history.capture().receipt().token_generation();
    let received_at_unix_millis = history.capture().receipt().received_at_unix_millis();
    let response_sha256 = history.capture().receipt().body_sha256();
    let response_bytes = history.capture().receipt().body_bytes();
    let accounting = history.accounting();
    let deadline = received_at
        .checked_add_nanos(60_000_000_000)
        .unwrap_or_else(|error| panic!("publication deadline: {error}"));
    let discovery = DiscoveryRequest::try_new(
        registered_coordinates.dataset().clone(),
        None,
        NonZeroU16::MIN,
        deadline,
    )
    .unwrap_or_else(|error| panic!("history discovery request: {error}"));
    let object = SourceObject::try_new_with_availability(
        registered_coordinates.source_id().clone(),
        registered_coordinates.metadata_revision().clone(),
        &discovery,
        SourceIdentifier::try_from("schwab-price-history-SPY-20240101")
            .unwrap_or_else(|error| panic!("history object: {error}")),
        SourceIdentifier::try_from("application-json")
            .unwrap_or_else(|error| panic!("history media type: {error}")),
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            response_sha256,
        )),
        EffectiveInterval::new(received_at, None)
            .unwrap_or_else(|error| panic!("history effective interval: {error}")),
        None,
        AvailabilityEvidence::LocalFirstObserved {
            observed_at: received_at,
        },
        Some(response_bytes),
    )
    .unwrap_or_else(|error| panic!("history object: {error}"));
    let extraction_request = ExtractionRequest::try_new(
        object,
        NonZeroU32::MIN,
        NonZeroU64::new(1024 * 1024)
            .unwrap_or_else(|| panic!("history byte bound must be nonzero")),
        deadline,
    )
    .unwrap_or_else(|error| panic!("history extraction request: {error}"));
    let market_data = SchwabPriceHistoryMarketDataEvidence::try_new(
        venue_id.clone(),
        SchwabMarketDataQualification::try_from_rest_response(
            &history,
            oauth_receipt,
            market_data_session.clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
        )
        .expect("actual history response qualification without a probe"),
    )
    .unwrap_or_else(|error| panic!("history market-data evidence: {error}"));
    assert_eq!(market_data.delay(), SchwabMarketDataDelay::Unknown);
    let publication_request = SchwabDailyPriceHistoryPublicationRequest::new(
        capability,
        oauth_receipt,
        extraction_request,
        instrument_id,
        instrument_revision_digest,
        admitted_plan_digest,
        identity,
        market_data,
        currency,
        calendar_range,
        received_at,
    );
    let event_id = Uuid::new_v4();
    let (rejoin, seal_request) = history
        .into_pending_capture(registered_coordinates.clone(), event_id)
        .unwrap_or_else(|error| panic!("pending REST raw capture: {error}"))
        .into_sealing_parts();
    let paths = LocalPaths::prepare(temporary.path().join("raw-publication"))
        .unwrap_or_else(|error| panic!("raw publication paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("raw publication store: {error}"));
    let physical_seal = seal_request
        .seal(&store)
        .unwrap_or_else(|error| panic!("REST physical seal: {error}"));
    let sealed = rejoin
        .try_rejoin(physical_seal)
        .unwrap_or_else(|error| panic!("sealed REST response: {error}"));
    let persisted = sealed.persisted_receipt();
    assert_eq!(
        persisted.capture().source_id(),
        registered_coordinates.source_id()
    );
    assert_eq!(
        persisted.capture().dataset(),
        registered_coordinates.dataset()
    );
    assert_eq!(
        persisted.capture().pages()[0].body_digest().bytes(),
        response_sha256
    );
    let reopened = store
        .open_verified(persisted.segment())
        .unwrap_or_else(|error| panic!("reopen Schwab physical seal: {error}"));
    assert_eq!(reopened.records().len(), 1);
    assert_eq!(reopened.records()[0].event_id(), event_id);
    assert_eq!(reopened.records()[0].payload(), history_body);
    assert_eq!(token_generation.get(), oauth_receipt.generation().get());
    assert_eq!(
        received_at_unix_millis,
        reopened.records()[0].received_at().timestamp_millis() as u64
    );
    assert_eq!(accounting.provider_records, 1);
    assert_eq!(sealed.route(), route);
    assert!(matches!(
        sealed.into_daily_price_history_publication(publication_request),
        Err(crate::SchwabPriceHistoryPublicationError::SemanticsUnverified)
    ));

    let quote_request = QuoteRequest::try_new(
        vec![
            ProviderIdentifier::try_new("AAPL")
                .unwrap_or_else(|error| panic!("quote symbol: {error}")),
        ],
        Vec::new(),
        None,
        admission(),
    )
    .unwrap_or_else(|error| panic!("quote request: {error}"));
    let sealed_quote = assert_sealed_rest_family(
        quote_request.request(),
        br#"{"AAPL":{"assetMainType":"EQUITY","realtime":true,"quote":{"bidPrice":100.125,"askPrice":100.25,"bidSize":2,"askSize":3}}}"#,
        SchwabRestFamily::Quotes,
        &token,
        token_admission,
        &store,
    )
    .await;
    let quote_received_at = Timestamp::from_unix_nanos(
        i64::try_from(sealed_quote.receipt().received_at_unix_millis())
            .unwrap_or_else(|error| panic!("quote received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("quote received timestamp overflow")),
    );
    let quote_session = market_data_session.clone();
    let quote_generation = market_squawk_domain::ConnectionGeneration::new(
        sealed_quote.receipt().token_generation().get(),
    )
    .unwrap_or_else(|error| panic!("quote generation: {error}"));
    let quote_instrument = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("quote instrument: {error}"));
    let quote_venue = VenueId::try_from("schwab-us-equities")
        .unwrap_or_else(|error| panic!("quote venue: {error}"));
    let quote_qualification =
        test_rest_qualification(&sealed_quote, oauth_receipt, quote_session.clone());
    assert_eq!(quote_qualification.market_data_principal_sha256(), None);
    assert_eq!(quote_qualification.delay(), SchwabMarketDataDelay::RealTime);
    assert_eq!(
        quote_qualification.observation_evidence().bytes(),
        sealed_quote.receipt().body_sha256()
    );
    assert!(
        SchwabMarketDataQualification::try_from_sealed_rest_response(
            &sealed_quote,
            different_series_oauth,
            quote_session.clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
        )
        .is_err()
    );
    let unknown_quote = execute_market_fixture(
        quote_request.request(),
        br#"{"AAPL":{"assetMainType":"EQUITY","realtime":false,"quote":{"bidPrice":100.125,"askPrice":100.25,"bidSize":2,"askSize":3}}}"#,
        &token, token_admission,
    ).await;
    let unknown_qualification = SchwabMarketDataQualification::try_from_rest_response(
        &unknown_quote,
        oauth_receipt,
        quote_session.clone(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
    )
    .expect("actual response without real-time entitlement still qualifies honestly");
    assert_eq!(
        unknown_qualification.delay(),
        SchwabMarketDataDelay::Unknown
    );
    assert!(!quote_qualification.validates_rest_receipt(
        market_squawk_sources::SchwabMarketDataFamily::Quotes,
        unknown_quote.capture().receipt(),
    ));
    let wrong_token = SchwabOAuthAuthorityReceipt::for_test(
        AccessTokenGeneration::new(
            NonZeroU64::new(oauth_receipt.generation().get() + 1).expect("next token"),
        ),
        oauth_receipt.credential_authority(),
    );
    assert!(
        SchwabMarketDataQualification::try_from_rest_response(
            &unknown_quote,
            wrong_token,
            quote_session.clone(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
        )
        .is_err()
    );
    let quote_product = quote_qualification.provider_product().clone();
    let quote_channel = quote_qualification.provider_channel().clone();
    let quote_source_identifier = SourceIdentifier::try_from("AAPL")
        .unwrap_or_else(|error| panic!("quote source identifier: {error}"));
    let quote_payload_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        sealed_quote.receipt().body_sha256(),
    );
    let quote_binding = LiveEvidenceBinding::new(
        capture_coordinates().source_id().clone(),
        quote_session.clone(),
        capture_coordinates().metadata_revision().clone(),
        AuthorizationBasis::new(
            SourceIdentifier::try_from("schwab-read-only-oauth")
                .unwrap_or_else(|error| panic!("quote authorization: {error}")),
        ),
        quote_venue.clone(),
        quote_instrument,
        quote_generation,
        quote_product.clone(),
        quote_channel.clone(),
        LiveEventClass::Quote,
        quote_source_identifier.clone(),
        quote_payload_digest,
        CanonicalStateDigest::new(
            EvidenceDigest::new(DigestAlgorithm::Sha256, [45; 32]),
            CanonicalizationRule::new(
                SourceIdentifier::try_from("schwab-rest-quote-state-v1")
                    .unwrap_or_else(|error| panic!("quote rule: {error}")),
                RuleVersion::new(1).unwrap_or_else(|error| panic!("quote rule version: {error}")),
            ),
        ),
        None,
    )
    .unwrap_or_else(|error| panic!("quote binding: {error}"));
    let quote_provenance = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        quote_binding,
        None,
        quote_received_at,
        quote_received_at,
        quote_received_at,
        DataQuality::DirectUnverified,
        CoverageStatus::Unknown,
        PayloadReference::ContentHash(PayloadHash::new(
            DigestAlgorithm::Sha256,
            sealed_quote.receipt().body_sha256(),
        )),
    ))
    .unwrap_or_else(|error| panic!("quote provenance: {error}"));
    let quote_identity = SchwabResolvedProviderIdentity::try_new(
        ProviderIdentifier::try_new("AAPL")
            .unwrap_or_else(|error| panic!("quote provider symbol: {error}")),
        ProviderInstrumentId::try_from("AAPL")
            .unwrap_or_else(|error| panic!("quote provider instrument: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [46; 32]),
    )
    .unwrap_or_else(|error| panic!("quote identity: {error}"));
    let quote_reference =
        (|| -> Result<market_squawk_domain::MarketDataReference, Box<dyn std::error::Error>> {
            use market_squawk_domain::{
                EffectiveInterval, MarketDataInstrumentDefinition,
                MarketDataInstrumentDefinitionInput, ProviderIdentityEvidence,
                ProviderIdentityRecord, ProviderIdentityRecordInput, RevisionBoundPayloadEvidence,
            };
            let interval = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
            let identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: quote_instrument,
                source_id: SourceId::try_from("schwab-trader-api-instruments")?,
                provider_instrument_id: ProviderInstrumentId::try_from("AAPL")?,
                evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    [46; 32],
                )),
                source_timestamp: None,
                observed_at: quote_received_at,
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                    "schwab-instruments-test-v1",
                )?),
                validity: interval,
                supersedes: None,
            });
            let definition =
                MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                    instrument_id: quote_instrument,
                    reference_evidence: RevisionBoundPayloadEvidence::new(
                        MetadataRevision::new(SourceIdentifier::try_from("schwab-test-reference")?),
                        market_squawk_domain::ExactPayloadEvidence::from_content_digest(
                            EvidenceDigest::new(DigestAlgorithm::Sha256, [47; 32]),
                        ),
                    ),
                    effective_interval: interval,
                    asset_class: market_squawk_domain::AssetClass::Equity,
                    display_name: None,
                    quote_currency: market_squawk_domain::Currency::try_from("USD")?,
                    quote_currency_evidence:
                        market_squawk_domain::ExactPayloadEvidence::from_content_digest(
                            EvidenceDigest::new(DigestAlgorithm::Sha256, [48; 32]),
                        ),
                    venue_mappings: vec![],
                    provider_identities: vec![identity.clone()],
                    identifiers: vec![],
                })?;
            let json = serde_json::to_vec(&definition)?;
            let digest = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                <sha2::Sha256 as sha2::Digest>::digest(json).into(),
            );
            Ok(market_squawk_domain::MarketDataReference::try_new(
                &definition,
                digest,
                &identity,
                quote_received_at,
            )?)
        })()
        .unwrap_or_else(|error| panic!("quote reference: {error}"));
    let mismatched_quote_session = SourceIdentifier::try_from("schwab-rest-session-mismatch")
        .unwrap_or_else(|error| panic!("mismatched quote session: {error}"));
    assert!(matches!(
        SchwabRestQuoteMarketDataEvidence::try_new(
            mismatched_quote_session,
            quote_generation,
            quote_venue.clone(),
            quote_qualification.clone(),
        ),
        Err(crate::SchwabRestQuotePublicationError::InvalidEvidence)
    ));
    let quote_market_data = SchwabRestQuoteMarketDataEvidence::try_new(
        quote_session,
        quote_generation,
        quote_venue,
        quote_qualification,
    )
    .unwrap_or_else(|error| panic!("quote market data: {error}"));
    let outcome = sealed_quote
        .into_quote_publication(SchwabRestQuotePublicationRequest::new(vec![
            SchwabRestQuoteRecordRequest::new(
                quote_identity,
                quote_instrument,
                quote_source_identifier,
                quote_provenance,
                quote_reference,
                quote_market_data,
            ),
        ]))
        .unwrap_or_else(|error| panic!("typed REST quote publication: {error}"));
    let SchwabRestQuotePublicationOutcome::Published(publication) = outcome else {
        panic!("complete REST quote should publish a typed event batch");
    };
    assert!(publication.dispositions().is_empty());
    assert_eq!(publication.binding().record_count(), 1);
    assert!(matches!(
        publication.binding().batch().events(),
        [MarketEvent::MarketDataQuote(_)]
    ));
    assert_eq!(
        publication.binding().row_frames()[0].capture_page_ordinal(),
        0
    );
    let native_quote = &publication.binding().native_lineage().rows()[0];
    assert!(
        native_quote
            .windows(b"100.125".len())
            .any(|value| value == b"100.125")
    );

    let chain_request = ChainRequest::new(
        ProviderIdentifier::try_new("SPY").unwrap_or_else(|error| panic!("chain symbol: {error}")),
    )
    .build(admission())
    .unwrap_or_else(|error| panic!("chain request: {error}"));
    let sealed_chain = assert_sealed_rest_family(
        &chain_request,
        br#"{"symbol":"SPY","status":"SUCCESS","strategy":"SINGLE","numberOfContracts":1,"underlyingPrice":500.1,"callExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"CALL","symbol":"SPY C","bid":1.2,"ask":1.3,"bidSize":2,"askSize":3,"strikePrice":500.0,"expirationDate":"2026-08-21","multiplier":100,"volatility":0.2,"delta":0.51,"gamma":0.03,"theta":-0.02,"vega":0.08,"openInterest":10}]}}}"#,
        SchwabRestFamily::OptionChain,
        &token,
        token_admission,
        &store,
    )
    .await;
    let option_underlying_instrument = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("option underlying instrument: {error}"));
    let option_contract_instrument = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("option contract instrument: {error}"));
    let option_underlying_identity = SchwabResolvedProviderIdentity::try_new(
        ProviderIdentifier::try_new("SPY")
            .unwrap_or_else(|error| panic!("option underlying symbol: {error}")),
        ProviderInstrumentId::try_from("schwab:SPY")
            .unwrap_or_else(|error| panic!("option underlying provider identity: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [50; 32]),
    )
    .unwrap_or_else(|error| panic!("option underlying identity: {error}"));
    let option_contract_identity = SchwabResolvedProviderIdentity::try_new(
        ProviderIdentifier::try_new("SPY C")
            .unwrap_or_else(|error| panic!("option contract symbol: {error}")),
        ProviderInstrumentId::try_from("schwab:SPY-C-20260821-500")
            .unwrap_or_else(|error| panic!("option contract provider identity: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [51; 32]),
    )
    .unwrap_or_else(|error| panic!("option contract identity: {error}"));
    let option_underlying_revision = EvidenceDigest::new(DigestAlgorithm::Sha256, [52; 32]);
    let option_contract_revision = EvidenceDigest::new(DigestAlgorithm::Sha256, [53; 32]);
    let chain_received_at = Timestamp::from_unix_nanos(
        i64::try_from(sealed_chain.receipt().received_at_unix_millis())
            .unwrap_or_else(|error| panic!("chain received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("chain received timestamp overflow")),
    );
    let chain_qualification =
        test_rest_qualification(&sealed_chain, oauth_receipt, market_data_session.clone());
    let chain_outcome = sealed_chain
        .into_option_publication(SchwabRestOptionPublicationRequest::new(
            SchwabRestOptionUnderlyingRequest::new(
                option_underlying_identity.clone(),
                option_underlying_instrument,
                option_underlying_revision,
            ),
            vec![SchwabRestOptionContractRequest::new(
                option_contract_identity,
                option_contract_instrument,
                option_contract_revision,
                None,
            )],
            SchwabRestOptionMarketDataEvidence::try_new(
                Some(
                    VenueId::try_from("schwab-us-options")
                        .unwrap_or_else(|error| panic!("option venue: {error}")),
                ),
                chain_qualification,
                currency,
            )
            .unwrap_or_else(|error| panic!("option market-data evidence: {error}")),
            chain_received_at,
        ))
        .unwrap_or_else(|error| panic!("typed option-chain publication: {error}"));
    let SchwabRestOptionPublicationOutcome::Published(chain_publication) = chain_outcome else {
        panic!("resolved option chain must publish a typed option batch");
    };
    assert_eq!(
        chain_publication.binding().batch().kind(),
        OptionMarketBatchKind::Snapshots
    );
    assert_eq!(chain_publication.revision_plan().len(), 1);
    assert!(chain_publication.dispositions().is_empty());
    let [option_snapshot] = chain_publication
        .binding()
        .batch()
        .snapshots()
        .unwrap_or_else(|| panic!("missing option snapshot rows"))
    else {
        panic!("expected exactly one option snapshot");
    };
    assert_eq!(
        option_snapshot.rho().unavailable_reason(),
        Some(OptionComponentState::ProviderAbsent)
    );
    assert_eq!(
        chain_publication.binding().row_frames()[0].capture_page_ordinal(),
        0
    );
    assert!(
        chain_publication.binding().native_lineage().rows()[0]
            .windows(b"multiplier".len())
            .any(|value| value == b"multiplier")
    );

    let expiration_request = ExpirationChainRequest::new(
        ProviderIdentifier::try_new("SPY")
            .unwrap_or_else(|error| panic!("expiration symbol: {error}")),
    )
    .build(admission())
    .unwrap_or_else(|error| panic!("expiration request: {error}"));
    let sealed_expirations = assert_sealed_rest_family(
        &expiration_request,
        br#"{"expirationList":[{"expirationDate":"2026-08-21","daysToExpiration":10,"expirationType":"S","standard":true}]}"#,
        SchwabRestFamily::ExpirationChain,
        &token,
        token_admission,
        &store,
    )
    .await;
    let expiration_received_at = Timestamp::from_unix_nanos(
        i64::try_from(sealed_expirations.receipt().received_at_unix_millis())
            .unwrap_or_else(|error| panic!("expiration received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("expiration received timestamp overflow")),
    );
    let expiration_qualification =
        test_rest_qualification(&sealed_expirations, oauth_receipt, market_data_session);
    let expiration_outcome = sealed_expirations
        .into_option_publication(SchwabRestOptionPublicationRequest::new(
            SchwabRestOptionUnderlyingRequest::new(
                option_underlying_identity,
                option_underlying_instrument,
                option_underlying_revision,
            ),
            Vec::new(),
            SchwabRestOptionMarketDataEvidence::try_new(
                Some(
                    VenueId::try_from("schwab-us-options")
                        .unwrap_or_else(|error| panic!("expiration venue: {error}")),
                ),
                expiration_qualification,
                currency,
            )
            .unwrap_or_else(|error| panic!("expiration market-data evidence: {error}")),
            expiration_received_at,
        ))
        .unwrap_or_else(|error| panic!("typed expiration publication: {error}"));
    let SchwabRestOptionPublicationOutcome::Published(expiration_publication) = expiration_outcome
    else {
        panic!("resolved expiration catalog must publish a typed option batch");
    };
    assert_eq!(
        expiration_publication.binding().batch().kind(),
        OptionMarketBatchKind::Expirations
    );
    assert_eq!(expiration_publication.revision_plan().len(), 1);
    assert!(expiration_publication.dispositions().is_empty());
    assert_eq!(
        expiration_publication.binding().row_frames()[0].capture_page_ordinal(),
        0
    );
    assert!(
        expiration_publication.binding().native_lineage().rows()[0]
            .windows(b"days_to_expiration".len())
            .any(|value| value == b"days_to_expiration")
    );
    let empty_expirations = execute_market_fixture(
        &expiration_request,
        br#"{"expirationList":[]}"#,
        &token,
        token_admission,
    )
    .await;
    assert_eq!(empty_expirations.accounting().returned, 0);
    assert_eq!(empty_expirations.accounting().missing, 1);
    assert_eq!(empty_expirations.accounting().provider_records, 0);

    let hours_request = build_market_hours_request(vec![MarketId::Equity], None, admission())
        .unwrap_or_else(|error| panic!("hours request: {error}"));
    assert_sealed_rest_family(
        &hours_request,
        br#"{"equity":{"EQ":{"date":"2026-08-26","isOpen":true,"category":null,"sessionHours":{"regularMarket":[{"start":"2026-08-26T09:30:00-04:00","end":"2026-08-26T16:00:00-04:00"}]}}}}"#,
        SchwabRestFamily::MarketHours,
        &token,
        token_admission,
        &store,
    )
    .await;

    let movers_request = build_movers_request(
        ProviderIdentifier::try_new("$DJI")
            .unwrap_or_else(|error| panic!("movers symbol: {error}")),
        Some(MoverSort::PercentChangeUp),
        Some(MoverFrequency::Five),
        admission(),
    )
    .unwrap_or_else(|error| panic!("movers request: {error}"));
    assert_sealed_rest_family(
        &movers_request,
        br#"{"screenersSymbol":"$DJI","frequency":5,"screeners":[{"symbol":"AAPL","lastPrice":100.1}]}"#,
        SchwabRestFamily::Movers,
        &token,
        token_admission,
        &store,
    )
    .await;

    let instrument_request = build_instrument_search_request(
        ProviderIdentifier::try_new("AAPL")
            .unwrap_or_else(|error| panic!("instrument symbol: {error}")),
        InstrumentProjection::SymbolSearch,
        admission(),
    )
    .unwrap_or_else(|error| panic!("instrument request: {error}"));
    assert_sealed_rest_family(
        &instrument_request,
        br#"{"instruments":[{"cusip":"037833100","symbol":"AAPL","description":"APPLE INC","exchange":"Q","assetType":"EQUITY"}]}"#,
        SchwabRestFamily::Instruments,
        &token,
        token_admission,
        &store,
    )
    .await;

    let replacement_credential = secrets
        .create(
            &application_key,
            SecretGeneration::new(2)
                .unwrap_or_else(|error| panic!("replacement application generation: {error}")),
            SecretValue::new(
                r#"{"version":1,"app_key":"replacement-app-key","app_secret":"replacement-app-secret"}"#
                    .to_owned(),
            )
            .unwrap_or_else(|error| panic!("replacement application secret: {error}")),
            &secret_control,
        )
        .unwrap_or_else(|error| panic!("replacement application credential: {error}"));
    let replacement = SchwabApplicationCredentialReplacement::try_new(
        application_key.clone(),
        application_credential,
        replacement_credential.clone(),
    )
    .unwrap_or_else(|error| panic!("guarded application replacement: {error}"));
    let replaced_authority = oauth_authority
        .replace_application_credential(replacement, SchwabOAuthInteraction::Background)
        .await
        .unwrap_or_else(|error| panic!("application credential replacement: {error}"));
    let invalid_replacement_credential = secrets
        .create(
            &application_key,
            SecretGeneration::new(3)
                .unwrap_or_else(|error| panic!("invalid replacement generation: {error}")),
            SecretValue::new("invalid-replacement-envelope".to_owned())
                .unwrap_or_else(|error| panic!("invalid replacement secret: {error}")),
            &secret_control,
        )
        .unwrap_or_else(|error| panic!("invalid replacement credential: {error}"));
    let invalid_replacement = SchwabApplicationCredentialReplacement::try_new(
        application_key,
        replacement_credential.clone(),
        invalid_replacement_credential,
    )
    .unwrap_or_else(|error| panic!("invalid guarded replacement: {error}"));
    let failure = match replaced_authority
        .replace_application_credential(invalid_replacement, SchwabOAuthInteraction::Background)
        .await
    {
        Ok(_unexpected_authority) => panic!("invalid replacement envelope must fail closed"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.binding(),
        crate::SchwabApplicationCredentialReplacementBinding::Previous
    );
    assert!(matches!(
        failure.error(),
        SchwabOAuthAuthorityError::Adapter(SchwabAdapterError::InvalidInput)
    ));
    let (replaced_authority, retained_binding, _replacement_error) = failure.into_parts();
    assert_eq!(
        retained_binding,
        crate::SchwabApplicationCredentialReplacementBinding::Previous
    );
    assert!(matches!(
        replaced_authority
            .status()
            .await
            .unwrap_or_else(|error| panic!("replacement authority status: {error}")),
        SchwabOAuthAuthorityStatus::AwaitingAuthorization
    ));
    drop(replaced_authority);

    let restarted = ProtectedSchwabOAuthAuthority::try_open(
        temporary.path().join("oauth-authority"),
        SchwabOAuthAuthorityConfiguration::try_new(
            secret_authority,
            Arc::new(ShortLivedOAuthWire::default()),
            replacement_credential,
            SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)
                .unwrap_or_else(|error| panic!("restart OAuth secret policy: {error}")),
            bounds(),
            token_admission,
            5,
        )
        .unwrap_or_else(|error| panic!("restart OAuth configuration: {error}")),
    )
    .await
    .unwrap_or_else(|error| panic!("restart OAuth authority: {error}"));
    assert!(matches!(
        restarted
            .status()
            .await
            .unwrap_or_else(|error| panic!("restarted replacement authority status: {error}")),
        SchwabOAuthAuthorityStatus::AwaitingAuthorization
    ));
}

#[tokio::test]
async fn oauth_application_replacement_rejects_a_wrong_secret_series() {
    let temporary = TemporaryDirectory::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("OAuth lifecycle clock: {error}"))
        .as_secs();
    let fixture = authorized_oauth_fixture(temporary.path().join("wrong-series"), now).await;
    let replacement_ref = fixture
        .secrets
        .create(
            &fixture.application_key,
            SecretGeneration::new(2)
                .unwrap_or_else(|error| panic!("wrong-series candidate generation: {error}")),
            SecretValue::new(
                r#"{"version":1,"app_key":"replacement-key","app_secret":"replacement-secret"}"#
                    .to_owned(),
            )
            .unwrap_or_else(|error| panic!("wrong-series candidate: {error}")),
            &fixture.control,
        )
        .unwrap_or_else(|error| panic!("wrong-series candidate storage: {error}"));
    let replacement = SchwabApplicationCredentialReplacement::try_new(
        SecretKey::try_new("market-squawk.schwab", "wrong-application-series")
            .unwrap_or_else(|error| panic!("wrong application series: {error}")),
        fixture.application_ref.clone(),
        replacement_ref,
    )
    .unwrap_or_else(|error| panic!("wrong-series replacement input: {error}"));
    let failure = match fixture
        .authority
        .replace_application_credential(replacement, SchwabOAuthInteraction::Background)
        .await
    {
        Ok(_) => panic!("wrong application credential series must fail closed"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.binding(),
        crate::SchwabApplicationCredentialReplacementBinding::Previous
    );
    let (authority, _, _) = failure.into_parts();
    assert!(matches!(
        authority
            .status()
            .await
            .unwrap_or_else(|error| panic!("wrong-series OAuth status: {error}")),
        SchwabOAuthAuthorityStatus::Active(_)
    ));
}

#[tokio::test]
async fn oauth_authority_durably_reauthorizes_unusable_token_state() {
    let temporary = TemporaryDirectory::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("OAuth lifecycle clock: {error}"))
        .as_secs();

    let expired = authorized_oauth_fixture(
        temporary.path().join("expired"),
        now.checked_sub(604_800)
            .unwrap_or_else(|| panic!("expired OAuth issue time underflow")),
    )
    .await;
    assert!(matches!(
        expired
            .authority
            .status()
            .await
            .unwrap_or_else(|error| panic!("expired OAuth status: {error}")),
        SchwabOAuthAuthorityStatus::ReauthorizationRequired
    ));
    assert!(matches!(
        expired.secrets.read(&expired.token_ref, &expired.control),
        Err(LocalSecretStoreError::NotFound)
    ));
    let expired_root = expired.state_root.clone();
    let expired_secrets = expired.secrets.clone();
    let expired_application = expired.application_ref.clone();
    drop(expired.authority);
    let restarted = ProtectedSchwabOAuthAuthority::try_open(
        expired_root,
        test_oauth_configuration(
            expired_secrets,
            expired_application,
            Arc::new(ShortLivedOAuthWire::default()),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("restart expired OAuth authority: {error}"));
    assert!(matches!(
        restarted
            .status()
            .await
            .unwrap_or_else(|error| panic!("restarted expired OAuth status: {error}")),
        SchwabOAuthAuthorityStatus::ReauthorizationRequired
    ));

    let missing = authorized_oauth_fixture(temporary.path().join("missing"), now).await;
    missing
        .secrets
        .delete(&missing.token_ref, &missing.control)
        .unwrap_or_else(|error| panic!("delete protected token: {error}"));
    assert!(matches!(
        missing.authority.acquire().await,
        Err(TokenAuthorityError::ReauthorizationRequired)
    ));
    assert!(matches!(
        missing
            .authority
            .status()
            .await
            .unwrap_or_else(|error| panic!("missing token OAuth status: {error}")),
        SchwabOAuthAuthorityStatus::ReauthorizationRequired
    ));

    let grant_issued_at = now.checked_sub(30).expect("refresh issue time");
    let refreshed =
        authorized_oauth_fixture(temporary.path().join("refreshed"), grant_issued_at).await;
    let initial = match refreshed
        .authority
        .status()
        .await
        .expect("initial grant status")
    {
        SchwabOAuthAuthorityStatus::Active(receipt) => receipt,
        status => panic!("initial grant must be active: {status:?}"),
    };
    assert_eq!(
        initial.authorization_generation(),
        initial.generation().get()
    );
    drop(refreshed.authority);
    let authority = ProtectedSchwabOAuthAuthority::try_open(
        &refreshed.state_root,
        test_oauth_configuration(
            refreshed.secrets.clone(),
            refreshed.application_ref.clone(),
            Arc::new(ShortLivedOAuthWire {
                expires_in: 1,
                scope: Some(" Market-Data market-data Market-Data "),
            }),
        ),
    )
    .await
    .expect("reopen grant for explicit refresh scope");
    let token = authority.acquire().await.expect("refresh access token");
    let narrowed = match authority.status().await.expect("refreshed grant status") {
        SchwabOAuthAuthorityStatus::Active(receipt) => receipt,
        status => panic!("refreshed grant must be active: {status:?}"),
    };
    assert!(narrowed.generation() > initial.generation());
    assert_eq!(token.generation(), narrowed.generation());
    assert_eq!(
        narrowed.authorization_generation(),
        initial.authorization_generation()
    );
    assert_eq!(
        narrowed.credential_authority(),
        initial.credential_authority()
    );
    assert_eq!(
        narrowed.refresh_authorized_at_unix_seconds(),
        grant_issued_at
    );
    assert_ne!(
        narrowed.authorization_scope_sha256(),
        initial.authorization_scope_sha256()
    );
    let mut normalized_scope_hash = sha2::Sha256::new();
    normalized_scope_hash.update(b"market-squawk.schwab.oauth-authorization-scope/v1\0");
    normalized_scope_hash.update([1]);
    normalized_scope_hash.update(b"Market-Data market-data");
    assert_eq!(
        narrowed.authorization_scope_sha256(),
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            normalized_scope_hash.finalize().into()
        ),
    );
    drop(authority);

    let authority = ProtectedSchwabOAuthAuthority::try_open(
        &refreshed.state_root,
        test_oauth_configuration(
            refreshed.secrets.clone(),
            refreshed.application_ref.clone(),
            Arc::new(ShortLivedOAuthWire {
                expires_in: 1_800,
                scope: None,
            }),
        ),
    )
    .await
    .expect("reopen refreshed grant");
    assert_eq!(
        authority.status().await.expect("reopened grant status"),
        SchwabOAuthAuthorityStatus::Active(narrowed)
    );
    authority
        .acquire()
        .await
        .expect("refresh with omitted scope");
    let restored = match authority
        .status()
        .await
        .expect("omitted scope grant status")
    {
        SchwabOAuthAuthorityStatus::Active(receipt) => receipt,
        status => panic!("omitted scope grant must be active: {status:?}"),
    };
    assert!(restored.generation() > narrowed.generation());
    assert_eq!(
        restored.authorization_generation(),
        initial.authorization_generation()
    );
    assert_eq!(
        restored.authorization_scope_sha256(),
        initial.authorization_scope_sha256()
    );
    drop(authority);

    let authority = ProtectedSchwabOAuthAuthority::try_open(
        &refreshed.state_root,
        test_oauth_configuration(
            refreshed.secrets,
            refreshed.application_ref,
            Arc::new(ShortLivedOAuthWire {
                expires_in: 1_800,
                scope: Some("Market-Data Quotes market-data"),
            }),
        ),
    )
    .await
    .expect("reopen original grant scope");
    assert_eq!(
        authority
            .status()
            .await
            .expect("reopened original scope status"),
        SchwabOAuthAuthorityStatus::Active(restored)
    );
    authority
        .revoke(SchwabOAuthInteraction::Background)
        .await
        .expect("revoke grant");
    let callback = match OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=new-code&state=new-grant",
        "new-grant",
        admission(),
    )
    .expect("new code callback")
    {
        CallbackOutcome::Authorized(callback) => callback,
        outcome => panic!("new code callback must authorize: {outcome:?}"),
    };
    let new_grant = authority
        .complete_authorization(
            &callback,
            grant_issued_at,
            SchwabOAuthInteraction::Background,
        )
        .await
        .expect("new code authorization at the same timestamp");
    assert!(new_grant.authorization_generation() > restored.generation().get());
    assert_eq!(
        new_grant.authorization_generation(),
        new_grant.generation().get()
    );
    assert_eq!(
        new_grant.refresh_authorized_at_unix_seconds(),
        initial.refresh_authorized_at_unix_seconds()
    );
    assert_eq!(
        new_grant.authorization_scope_sha256(),
        initial.authorization_scope_sha256()
    );
}

#[derive(Clone, Copy, Debug)]
enum SensitiveSendCompletion {
    Success,
    NetworkError,
    Pending,
}

struct SensitiveHandoffConnection {
    completion: SensitiveSendCompletion,
    owner_address: usize,
    audit: crate::transport::SensitiveDropAudit,
}

impl fmt::Debug for SensitiveHandoffConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveHandoffConnection(..)")
    }
}

impl crate::transport::SealedSchwabStreamerConnection for SensitiveHandoffConnection {}

impl SchwabStreamerConnection for SensitiveHandoffConnection {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        let completion = self.completion;
        let owner_address = self.owner_address;
        let audit = self.audit.clone();
        Box::pin(async move {
            assert_eq!(payload.as_ptr() as usize, owner_address);
            assert_eq!(audit.cleared_drops(), 0);
            let result = match completion {
                SensitiveSendCompletion::Success => Ok(()),
                SensitiveSendCompletion::NetworkError => Err(SchwabTransportError::Network),
                SensitiveSendCompletion::Pending => pending().await,
            };
            assert_eq!(audit.cleared_drops(), 0);
            drop(payload);
            result
        })
    }

    fn send_pong<'a>(
        &'a mut self,
        _payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async { Err(SchwabTransportError::Protocol) })
    }

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(pending())
    }

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

// The existing wire doubles use the same durable shared-budget admission as production.
struct MockStreamerRateAuthority {
    budget: Arc<SharedProviderBudget>,
    generation: Mutex<Option<ConnectionGeneration>>,
    _authority: ProviderRateAuthority,
    _temporary: TemporaryDirectory,
}
impl fmt::Debug for MockStreamerRateAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MockStreamerRateAuthority")
            .finish_non_exhaustive()
    }
}
impl MockStreamerRateAuthority {
    fn new() -> Self {
        let temporary = TemporaryDirectory::new();
        std::fs::create_dir_all(temporary.path()).expect("rate fixture directory");
        let store = market_squawk_data::SqliteProviderRateStore::try_open(
            temporary.path().join("rate.sqlite3"),
        )
        .expect("rate fixture store");
        let authority = ProviderRateAuthority::try_new(Arc::new(store)).expect("rate authority");
        let subject = SourceIdentifier::try_from(format!("schwab-fixture-{}", Uuid::new_v4()))
            .expect("rate subject");
        let policy = ProviderBudgetPolicy::try_new(
            BudgetScope::with_authorization_account(
                SourceIdentifier::try_from("schwab").expect("provider"),
                subject.clone(),
            ),
            NonZeroU32::new(64).expect("request capacity"),
            NonZeroU64::new(60_000_000_000).expect("request window"),
            NonZeroU16::new(1).expect("single in-flight request"),
            BackoffPolicy::try_new(
                NonZeroU64::new(1_000_000).expect("backoff"),
                NonZeroU64::new(1_000_000_000).expect("maximum backoff"),
                0,
            )
            .expect("backoff policy"),
        )
        .expect("rate policy");
        let declaration = ProviderRateDeclaration::try_for_authorization_subject(policy, &subject)
            .expect("rate declaration");
        let budget = Arc::new(
            authority
                .register_budget(declaration)
                .expect("shared rate budget"),
        );
        Self {
            budget,
            generation: Mutex::new(None),
            _authority: authority,
            _temporary: temporary,
        }
    }
    fn acquire(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<BudgetPermit, SchwabTransportError> {
        mock_streamer_rate_active(cancellation, deadline)?;
        let BudgetReservationDecision::Ready(reservation) = self.budget.try_reserve_request()
        else {
            return Err(SchwabTransportError::Protocol);
        };
        mock_streamer_rate_active(cancellation, deadline)?;
        let BudgetDispatchDecision::Ready(permit) = reservation.commit_dispatch() else {
            return Err(SchwabTransportError::Protocol);
        };
        Ok(permit)
    }
}
fn mock_streamer_rate_active(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), SchwabTransportError> {
    if cancellation.is_cancelled() {
        Err(SchwabTransportError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(SchwabTransportError::Deadline)
    } else {
        Ok(())
    }
}
impl SchwabStreamerRuntimeAuthority for MockStreamerRateAuthority {
    fn observe(&self, event: SchwabStreamerRuntimeEvent) -> Result<(), SchwabTransportError> {
        let mut current = self
            .generation
            .lock()
            .map_err(|_| SchwabTransportError::Protocol)?;
        match event {
            SchwabStreamerRuntimeEvent::Connected { generation }
            | SchwabStreamerRuntimeEvent::Frame { generation, .. } => {
                assert_eq!(*current, Some(generation));
            }
            SchwabStreamerRuntimeEvent::Disconnected { generation, .. } => {
                assert_eq!(*current, Some(generation));
                *current = None;
            }
            SchwabStreamerRuntimeEvent::ConnectAttempt { .. }
            | SchwabStreamerRuntimeEvent::QueuePressure => {}
        }
        Ok(())
    }
    fn commit_connection<'a>(
        &'a self,
        generation: ConnectionGeneration,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Box<dyn SchwabStreamerConnectionPermit>, SchwabTransportError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let permit = self.acquire(cancellation, deadline)?;
            let mut current = self
                .generation
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?;
            assert!(current.is_none());
            *current = Some(generation);
            Ok(Box::new(MockStreamerConnectionRatePermit {
                budget: Arc::clone(&self.budget),
                permit,
            }) as Box<dyn SchwabStreamerConnectionPermit>)
        })
    }
    fn commit_request<'a>(
        &'a self,
        generation: ConnectionGeneration,
        service: Option<MarketDataService>,
        command: &'a str,
        request_id: &'a str,
        request_payload_sha256: EvidenceDigest,
        request_payload_bytes: u64,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerRequestPermit>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            assert_eq!(
                *self
                    .generation
                    .lock()
                    .map_err(|_| SchwabTransportError::Protocol)?,
                Some(generation)
            );
            assert!(!request_id.is_empty());
            assert_ne!(request_payload_sha256.bytes(), [0; 32]);
            assert!(request_payload_bytes > 0);
            assert!(match service {
                None => command == "LOGIN",
                Some(_) => matches!(command, "SUBS" | "ADD" | "UNSUBS"),
            });
            let permit = self.acquire(cancellation, deadline)?;
            Ok(Box::new(MockStreamerRequestRatePermit {
                budget: Arc::clone(&self.budget),
                permit,
                generation,
                service,
                command: command.to_owned(),
                request_id: request_id.to_owned(),
                request_payload_sha256,
                request_payload_bytes,
            }) as Box<dyn SchwabStreamerRequestPermit>)
        })
    }
}
#[derive(Debug)]
struct MockStreamerConnectionRatePermit {
    budget: Arc<SharedProviderBudget>,
    permit: BudgetPermit,
}
impl SchwabStreamerConnectionPermit for MockStreamerConnectionRatePermit {
    fn connected<'a>(
        self: Box<Self>,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            mock_streamer_rate_active(cancellation, deadline)?;
            self.budget
                .record_success()
                .map_err(|_| SchwabTransportError::Protocol)?;
            self.permit.release();
            Ok(())
        })
    }
}
#[derive(Debug)]
struct MockStreamerRequestRatePermit {
    budget: Arc<SharedProviderBudget>,
    permit: BudgetPermit,
    generation: ConnectionGeneration,
    service: Option<MarketDataService>,
    command: String,
    request_id: String,
    request_payload_sha256: EvidenceDigest,
    request_payload_bytes: u64,
}
impl SchwabStreamerRequestPermit for MockStreamerRequestRatePermit {
    fn settle<'a>(
        self: Box<Self>,
        acknowledgement: SchwabStreamerRequestAcknowledgement,
        cancellation: &'a CancellationToken,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            mock_streamer_rate_active(cancellation, deadline)?;
            assert_eq!(acknowledgement.generation(), self.generation);
            assert_eq!(acknowledgement.service(), self.service);
            assert_eq!(acknowledgement.command(), self.command);
            assert_eq!(acknowledgement.request_id(), self.request_id);
            assert_eq!(
                acknowledgement.request_payload_sha256(),
                self.request_payload_sha256
            );
            assert_eq!(
                acknowledgement.request_payload_bytes(),
                self.request_payload_bytes
            );
            assert!(acknowledgement.transport_ordinal().is_some());
            if acknowledgement.succeeded() {
                self.budget
                    .record_success()
                    .map_err(|_| SchwabTransportError::Protocol)?;
            } else {
                let BudgetDecision::WaitUntil(_) = self.budget.apply_refusal(0) else {
                    return Err(SchwabTransportError::Protocol);
                };
            }
            self.permit.release();
            Ok(())
        })
    }
}

#[tokio::test]
async fn bearer_network_handoffs_keep_one_zeroizing_owner_through_completion() {
    let rest_audit = crate::transport::SensitiveDropAudit::default();
    let mut rest =
        crate::transport::ReqwestSchwabAuthorizationMaterial::try_new("rest-bearer-secret")
            .unwrap_or_else(|error| panic!("REST bearer material: {error}"));
    rest.arm_drop_audit(rest_audit.clone());
    let rest_owner = rest.as_bytes().as_ptr();
    let header = rest
        .into_header()
        .unwrap_or_else(|error| panic!("REST bearer header: {error}"));
    assert_eq!(header.as_bytes().as_ptr(), rest_owner);
    let network_header = header.clone();
    drop(header);
    assert_eq!(rest_audit.cleared_drops(), 0);
    drop(network_header);
    assert_eq!(rest_audit.cleared_drops(), 1);
    assert_eq!(rest_audit.uncleared_drops(), 0);

    let preference = br#"{
      "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
      "offers":[{"mktDataPermission":"NP","level2Permissions":true}]
    }"#;
    let bootstrap = parse_user_preference(preference, bounds())
        .unwrap_or_else(|error| panic!("Streamer bootstrap: {error}"));
    for (completion, timeout, cancel, expected) in [
        (
            SensitiveSendCompletion::Success,
            Duration::from_secs(1),
            false,
            Ok(()),
        ),
        (
            SensitiveSendCompletion::NetworkError,
            Duration::from_secs(1),
            false,
            Err(SchwabTransportError::Network),
        ),
        (
            SensitiveSendCompletion::Pending,
            Duration::from_millis(1),
            false,
            Err(SchwabTransportError::Deadline),
        ),
        (
            SensitiveSendCompletion::Pending,
            Duration::from_secs(1),
            true,
            Err(SchwabTransportError::Cancelled),
        ),
    ] {
        let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
        let generation = ConnectionGeneration::new(
            NonZeroU64::new(1).unwrap_or_else(|| panic!("generation must be nonzero")),
        );
        let mut controller = DesiredStateController::new(stream_admission);
        controller
            .begin_connect(generation)
            .unwrap_or_else(|error| panic!("begin Streamer connection: {error}"));
        controller
            .socket_connected(generation)
            .unwrap_or_else(|error| panic!("connect Streamer socket: {error}"));
        let mut login = controller
            .login_request(bootstrap.value(), "streamer-bearer-secret")
            .unwrap_or_else(|error| panic!("Streamer login request: {error}"));
        let streamer_audit = crate::transport::SensitiveDropAudit::default();
        login.arm_drop_audit(streamer_audit.clone());
        let mut connection = SensitiveHandoffConnection {
            completion,
            owner_address: login.expose_body().as_ptr() as usize,
            audit: streamer_audit.clone(),
        };
        let rate_authority = MockStreamerRateAuthority::new();
        let setup_cancellation = CancellationToken::new();
        let setup_deadline = Instant::now() + Duration::from_secs(1);
        rate_authority
            .commit_connection(generation, &setup_cancellation, setup_deadline)
            .await
            .expect("fixture handshake permit")
            .connected(&setup_cancellation, setup_deadline)
            .await
            .expect("fixture connected");
        let cancellation = CancellationToken::new();
        if cancel {
            cancellation.cancel();
        }
        let result = crate::transport::send_streamer_request_for_test(
            &mut connection,
            login,
            &rate_authority,
            timeout,
            &cancellation,
        )
        .await;
        assert_eq!(result, expected);
        assert_eq!(streamer_audit.cleared_drops(), 1);
        assert_eq!(streamer_audit.uncleared_drops(), 0);
    }
}

#[tokio::test]
async fn streamer_microbatch_retains_validated_application_frames_without_token_material() {
    let telemetry = SchwabTransportTelemetry::default();
    let token_admission = AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1));

    let preference = br#"{
      "accounts":[{"accountNumber":"must-not-enter-stream-capture"}],
      "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
      "offers":[{"mktDataPermission":"NP","level2Permissions":true}]
    }"#;
    let bootstrap = parse_user_preference(preference, bounds())
        .unwrap_or_else(|error| panic!("bootstrap: {error}"));
    let login = Bytes::from_static(
        br#"{"response":[{"service":"ADMIN","command":"LOGIN","requestid":"1","timestamp":1710000000000,"content":{"code":0,"msg":"OK"}}]}"#,
    );
    let equities_subscribed = Bytes::from_static(
        br#"{"response":[{"service":"LEVELONE_EQUITIES","command":"SUBS","requestid":"2","timestamp":1710000000001,"content":{"code":26,"msg":"SUBS succeeded"}}]}"#,
    );
    let options_subscribed = Bytes::from_static(
        br#"{"response":[{"service":"LEVELONE_OPTIONS","command":"SUBS","requestid":"3","timestamp":1710000000002,"content":{"code":0,"msg":"OK"}}]}"#,
    );
    let mixed_market_data: &'static [u8] =
        br#"{"data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","timestamp":1710000000004,"content":[{"key":"AAPL","delayed":false,"assetMainType":"EQUITY","assetSubType":"COE","cusip":"TEST00001","1":100.125,"2":100.25,"3":2,"4":3}]},{"service":"LEVELONE_OPTIONS","command":"SUBS","timestamp":1710000000004,"content":[{"key":"AAPL_260116C100","delayed":true,"assetMainType":"OPTION","assetSubType":null,"cusip":null,"1":4.125,"2":4.25,"3":5,"4":6}]}]}"#;
    let malformed_selected_service: &'static [u8] =
        br#"{"data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","content":[{"key":"AAPL","1":}]}]}"#;
    let connector_state = Arc::new(Mutex::new(MockStreamerState {
        connects: 0,
        inbound: VecDeque::from([VecDeque::from([
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(login.clone())),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(equities_subscribed.clone())),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(options_subscribed.clone())),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(Bytes::from_static(
                mixed_market_data,
            ))),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(Bytes::from_static(
                mixed_market_data,
            ))),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(Bytes::from_static(
                mixed_market_data,
            ))),
            MockStreamerInbound::FlushBoundary,
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(Bytes::from_static(
                malformed_selected_service,
            ))),
        ])]),
        sent: Vec::new(),
    }));
    let connector = Arc::new(MockStreamerConnector {
        state: connector_state.clone(),
    });
    assert_eq!(
        StreamerTransportBounds::try_new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::ZERO,
            0,
            nonzero(64 * 1024),
            nonzero(65),
            nonzero(64 * 1024),
            Duration::from_millis(1),
        )
        .expect_err("zero reconnect delay must not admit a hot reconnect loop"),
        SchwabTransportError::InvalidConfiguration
    );
    let stream_bounds = StreamerTransportBounds::try_new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_millis(1),
        0,
        nonzero(64 * 1024),
        nonzero(65),
        nonzero(64 * 1024),
        Duration::from_millis(1),
    )
    .unwrap_or_else(|error| panic!("stream bounds: {error}"));
    let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
    let coordinates = capture_coordinates();
    let session_identifier = SourceIdentifier::try_from("8d9bc9ee-fca2-4f1d-a077-5104408e3727")
        .unwrap_or_else(|error| panic!("Streamer authority session: {error}"));
    let stream_identity = SourceIdentifier::try_from("schwab-streamer-connection-41")
        .unwrap_or_else(|error| panic!("stream identity: {error}"));
    let application_generation = ConnectionGeneration::new(
        NonZeroU64::new(41).unwrap_or_else(|| panic!("application generation must be nonzero")),
    );
    let control_source = Arc::new(MockStreamerControlSource {
        controls: Mutex::new(VecDeque::from([SchwabStreamerConnectionControl::new(
            application_generation,
            session_identifier.clone(),
            coordinates.clone(),
            stream_identity.clone(),
        )])),
    });
    let mut streamer = SchwabStreamerExecutor::try_new(
        connector,
        Arc::new(MockTokenSource { token_admission }),
        control_source,
        stream_admission,
        stream_bounds,
        bounds(),
        token_admission,
        telemetry,
        Arc::new(MockStreamerRateAuthority::new()),
    )
    .unwrap_or_else(|error| panic!("stream executor: {error}"));
    streamer
        .replace_desired(
            StreamerSubscription::try_new(
                MarketDataService::LevelOneEquities,
                vec![
                    ProviderIdentifier::try_new("AAPL")
                        .unwrap_or_else(|error| panic!("symbol: {error}")),
                ],
                vec![0, 1, 2, 3, 4],
                stream_admission,
            )
            .unwrap_or_else(|error| panic!("subscription: {error}")),
        )
        .unwrap_or_else(|error| panic!("desired state: {error}"));
    streamer
        .replace_desired(
            StreamerSubscription::try_new(
                MarketDataService::LevelOneOptions,
                vec![
                    ProviderIdentifier::try_new("AAPL_260116C100")
                        .unwrap_or_else(|error| panic!("option symbol: {error}")),
                ],
                vec![0, 1, 2, 3, 4],
                stream_admission,
            )
            .unwrap_or_else(|error| panic!("option subscription: {error}")),
        )
        .unwrap_or_else(|error| panic!("option desired state: {error}"));
    let cancellation = CancellationToken::new();
    let mut sink = CancellingCaptureSink {
        cancellation: cancellation.clone(),
        cancel_after: 7,
        microbatches: Vec::new(),
    };
    let run_error = streamer
        .run(bootstrap.value(), &mut sink, cancellation)
        .await
        .expect_err("malformed selected-service frame must close the typed Streamer run");
    assert_eq!(run_error, SchwabTransportError::Adapter);
    assert_eq!(sink.microbatches.len(), 7);
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path().join("stream-raw-publication"))
        .unwrap_or_else(|error| panic!("Streamer publication paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("Streamer publication store: {error}"));
    let mut microbatches = sink.microbatches.into_iter();
    let login_acknowledgement = seal_stream_microbatch(
        microbatches
            .next()
            .expect("missing LOGIN acknowledgement microbatch"),
        &store,
    );
    let login_reopened = store
        .open_verified(login_acknowledgement.persisted_receipt().segment())
        .expect("reopen LOGIN physical seal");
    let [login_record] = login_reopened.records() else {
        panic!("LOGIN seal must contain one exact native ACK");
    };
    assert_eq!(login_record.payload(), login.as_ref());
    assert!(login_acknowledgement.service_responses().is_empty());
    let equities_acknowledgement = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing equities acknowledgement microbatch")),
        &store,
    );
    let options_acknowledgement = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing options acknowledgement microbatch")),
        &store,
    );
    let equities_doctor_data = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing equities doctor data microbatch")),
        &store,
    );
    let options_doctor_data = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing options doctor data microbatch")),
        &store,
    );
    let sealed = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing publication microbatch")),
        &store,
    );
    let raw_only = seal_stream_microbatch(
        microbatches
            .next()
            .unwrap_or_else(|| panic!("missing malformed raw-only microbatch")),
        &store,
    );
    assert!(microbatches.next().is_none());

    let mut equities_doctor = SchwabStreamerFamilyDoctorAccumulator::try_from_ack_capture(
        MarketDataService::LevelOneEquities,
        equities_acknowledgement,
    )
    .unwrap_or_else(|rejection| panic!("typed Streamer acknowledgement: {:?}", rejection.error()));
    equities_doctor
        .try_push_data_capture(equities_doctor_data)
        .unwrap_or_else(|rejection| panic!("typed Streamer doctor data: {:?}", rejection.error()));
    let equities_streamer_doctor = equities_doctor
        .try_finish()
        .unwrap_or_else(|error| panic!("typed Streamer doctor handoff: {error}"));
    assert_eq!(equities_streamer_doctor.provider_records(), 1);
    let service_response = equities_streamer_doctor.acknowledgement();
    assert_eq!(service_response.status_code(), 26);
    assert!(service_response.succeeded());
    let stream_capacity = service_response
        .capacity_observation()
        .unwrap_or_else(|error| panic!("sealed Streamer capacity evidence: {error}"));
    assert_eq!(
        (stream_capacity.requested(), stream_capacity.returned()),
        (1, 1)
    );
    let mut options_doctor = SchwabStreamerFamilyDoctorAccumulator::try_from_ack_capture(
        MarketDataService::LevelOneOptions,
        options_acknowledgement,
    )
    .unwrap_or_else(|rejection| {
        panic!(
            "typed options Streamer acknowledgement: {:?}",
            rejection.error()
        )
    });
    options_doctor
        .try_push_data_capture(options_doctor_data)
        .unwrap_or_else(|rejection| panic!("typed options Streamer data: {:?}", rejection.error()));
    let options_streamer_doctor = options_doctor
        .try_finish()
        .unwrap_or_else(|error| panic!("typed options Streamer handoff: {error}"));
    assert_eq!(options_streamer_doctor.provider_records(), 1);

    let [raw_only_frame] = raw_only.frames() else {
        panic!("malformed Streamer evidence must retain one exact raw frame");
    };
    assert_eq!(raw_only_frame.kind(), RawStreamerFrameKind::Text);
    assert_eq!(
        raw_only_frame.payload_bytes(),
        u64::try_from(malformed_selected_service.len())
            .unwrap_or_else(|error| panic!("malformed payload bytes: {error}"))
    );
    assert_eq!(
        raw_only_frame.payload_digest().bytes(),
        <[u8; 32]>::from(sha2::Sha256::digest(malformed_selected_service))
    );
    let raw_only_persisted = raw_only.persisted_receipt();
    assert_ne!(raw_only_persisted.receipt_digest().bytes(), [0; 32]);
    let raw_only_reopened = store
        .open_verified(raw_only_persisted.segment())
        .unwrap_or_else(|error| panic!("reopen malformed Streamer physical seal: {error}"));
    let [raw_only_record] = raw_only_reopened.records() else {
        panic!("malformed Streamer seal must contain one raw record");
    };
    assert_eq!(raw_only_record.payload(), malformed_selected_service);
    let raw_only_received_at = Timestamp::from_unix_nanos(
        i64::try_from(raw_only_frame.received_at_unix_millis())
            .unwrap_or_else(|error| panic!("raw-only received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("raw-only received timestamp overflow")),
    );
    let streamer_oauth_authority = SchwabOAuthAuthorityReceipt::for_test(
        raw_only.streamer_receipt().token_generation(),
        raw_only.streamer_receipt().credential_authority(),
    );
    let streamer_principal = raw_only.streamer_receipt().market_data_principal_sha256();
    let raw_only_qualification = test_streamer_qualification(
        &equities_streamer_doctor,
        raw_only_received_at,
        streamer_oauth_authority,
        session_identifier.clone(),
    );
    assert!(
        !raw_only_qualification.validates_streamer_publication_coordinate(
            MarketDataService::LevelOneEquities,
            &equities_streamer_doctor,
            &raw_only,
            0,
            0,
            0,
        )
    );

    assert_eq!(sealed.coordinates(), &coordinates);
    assert_eq!(sealed.stream_identity(), &stream_identity);
    let [frame] = sealed.frames() else {
        panic!("publication capture must retain one data frame");
    };
    assert_eq!(
        frame.payload_bytes(),
        u64::try_from(mixed_market_data.len())
            .unwrap_or_else(|error| panic!("market-data payload bytes: {error}"))
    );
    assert_eq!(frame.kind(), RawStreamerFrameKind::Text);
    let frame_generation = frame.generation();
    let frame_digest = frame.payload_digest().bytes();
    let frame_received_at_unix_millis = frame.received_at_unix_millis();
    let persisted = sealed.persisted_receipt();
    assert_eq!(persisted.capture().source_id(), coordinates.source_id());
    assert_eq!(persisted.capture().dataset(), coordinates.dataset());
    assert_eq!(persisted.capture().stream_identity(), &stream_identity);
    assert_ne!(persisted.receipt_digest().bytes(), [0; 32]);
    assert_eq!(persisted.capture().frames().len(), 1);
    assert_eq!(persisted.capture().frames()[0].source_sequence(), None);
    let reopened = store
        .open_verified(persisted.segment())
        .unwrap_or_else(|error| panic!("reopen Streamer physical seal: {error}"));
    let [record] = reopened.records() else {
        panic!("sealed Streamer publication microbatch must contain one data frame");
    };
    assert_eq!(record.connection_id(), coordinates.connection_id());
    assert_eq!(record.payload(), mixed_market_data);
    assert_eq!(record.source_sequence(), None);
    let received_at = Timestamp::from_unix_nanos(
        i64::try_from(frame_received_at_unix_millis)
            .unwrap_or_else(|error| panic!("received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("received timestamp overflow")),
    );
    assert_eq!(
        sealed.streamer_receipt().credential_authority(),
        streamer_oauth_authority.credential_authority()
    );
    assert_eq!(
        sealed.streamer_receipt().session_identifier(),
        &session_identifier
    );
    assert_eq!(
        sealed.streamer_receipt().market_data_principal_sha256(),
        streamer_principal
    );
    let equities_qualification = test_streamer_qualification(
        &equities_streamer_doctor,
        received_at,
        streamer_oauth_authority,
        session_identifier.clone(),
    );
    let options_qualification = test_streamer_qualification(
        &options_streamer_doctor,
        received_at,
        streamer_oauth_authority,
        session_identifier.clone(),
    );
    assert_eq!(
        equities_qualification.market_data_principal_sha256(),
        Some(streamer_principal)
    );
    let rights = EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]);
    let capability = EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]);
    let wrong_series = SchwabOAuthAuthorityReceipt::for_test(
        sealed.streamer_receipt().token_generation(),
        SchwabCredentialAuthorityBinding::for_test(
            streamer_oauth_authority
                .credential_authority()
                .application_credential_generation(),
            92,
        ),
    );
    assert!(
        SchwabMarketDataQualification::try_from_streamer_handoff(
            &equities_streamer_doctor,
            received_at,
            wrong_series,
            session_identifier.clone(),
            rights,
            capability,
        )
        .is_err()
    );
    assert!(
        SchwabMarketDataQualification::try_from_streamer_handoff(
            &equities_streamer_doctor,
            received_at,
            streamer_oauth_authority,
            SourceIdentifier::try_from("d184e132-2f48-49df-98ff-d24898f8907a")
                .expect("wrong session"),
            rights,
            capability,
        )
        .is_err()
    );
    let equities_record = test_streamer_quote_record_request(
        &coordinates,
        &stream_identity,
        frame_generation,
        frame_digest,
        received_at,
        equities_qualification,
        MarketDataService::LevelOneEquities,
        0,
        "AAPL",
        "schwab-us-equities",
        41,
    );
    let options_record = test_streamer_quote_record_request(
        &coordinates,
        &stream_identity,
        frame_generation,
        frame_digest,
        received_at,
        options_qualification,
        MarketDataService::LevelOneOptions,
        1,
        "AAPL_260116C100",
        "schwab-us-options",
        51,
    );
    let publication_request = SchwabStreamerQuotePublicationRequest::new(
        vec![&equities_streamer_doctor, &options_streamer_doctor],
        vec![equities_record, options_record],
    );
    publication_request
        .validate_current_authority(
            streamer_oauth_authority,
            received_at,
            &session_identifier,
            rights,
            capability,
        )
        .expect("native proofs with current configured authority");
    assert!(
        publication_request
            .validate_current_authority(
                wrong_series,
                received_at,
                &session_identifier,
                rights,
                capability,
            )
            .is_err()
    );
    assert!(
        publication_request
            .validate_current_authority(
                streamer_oauth_authority,
                received_at,
                &session_identifier,
                EvidenceDigest::new(DigestAlgorithm::Sha256, [94; 32]),
                capability,
            )
            .is_err()
    );
    let outcome = sealed
        .into_level_one_quote_publication(publication_request)
        .unwrap_or_else(|error| panic!("typed Streamer publication: {error}"));
    let SchwabStreamerQuotePublicationOutcome::Published(publication) = outcome else {
        panic!("complete Level-One quote should publish a typed event batch");
    };
    assert!(publication.dispositions().is_empty());
    assert_eq!(publication.binding().record_count(), 2);
    let [
        MarketEvent::MarketDataQuote(equities),
        MarketEvent::MarketDataQuote(options),
    ] = publication.binding().batch().events()
    else {
        panic!("Streamer prices must retain currency-qualified decimal quote semantics");
    };
    for (quote, symbol, price, size) in [
        (equities, "AAPL", rust_decimal::Decimal::new(100_125, 3), 2),
        (
            options,
            "AAPL_260116C100",
            rust_decimal::Decimal::new(4_125, 3),
            5,
        ),
    ] {
        assert_eq!(quote.provenance().source_timestamp(), None);
        assert_eq!(quote.provenance().received_at(), received_at);
        assert_eq!(
            quote
                .reference()
                .provider_identity()
                .expect("native provider reference")
                .provider_instrument_id()
                .as_str(),
            symbol
        );
        let bid = quote.bid().expect("fixture bid is present");
        assert_eq!(bid.price().amount(), price);
        assert_eq!(
            bid.size(),
            &market_squawk_domain::MarketDataQuoteSize::UnresolvedUnit(
                rust_decimal::Decimal::from(size)
            )
        );
    }
    assert!(
        publication
            .binding()
            .row_frames()
            .iter()
            .all(|row| row.event_frame_ordinal() == 0)
    );
    let native_rows = publication.binding().native_lineage().rows();
    for (row, delayed, main_type, sub_type, cusip) in [
        (
            &native_rows[0],
            false,
            "EQUITY",
            serde_json::json!({"kind":"text","value":"COE"}),
            serde_json::json!({"kind":"text","value":"TEST00001"}),
        ),
        (
            &native_rows[1],
            true,
            "OPTION",
            serde_json::json!({"kind":"null"}),
            serde_json::json!({"kind":"null"}),
        ),
    ] {
        let native: serde_json::Value =
            serde_json::from_slice(row).expect("sealed native Streamer row");
        assert_eq!(
            native["metadata"],
            serde_json::json!([
                {"name":"assetMainType","value":{"kind":"text","value":main_type}},
                {"name":"assetSubType","value":sub_type},
                {"name":"cusip","value":cusip},
                {"name":"delayed","value":{"kind":"bool","value":delayed}},
            ])
        );
        assert_eq!(native["delay"]["kind"], "unknown");
        assert_eq!(native["quality"], "direct_unverified");
    }
    assert!(
        native_rows[0]
            .windows(b"100.125".len())
            .any(|value| value == b"100.125")
    );
    assert!(
        native_rows[1]
            .windows(b"AAPL_260116C100".len())
            .any(|value| value == b"AAPL_260116C100")
    );
    assert!(native_rows.iter().all(|row| {
        row.windows(b"field_id".len())
            .any(|value| value == b"field_id")
            && row
                .windows(b"streamer_doctor_capture_set_evidence".len())
                .any(|value| value == b"streamer_doctor_capture_set_evidence")
    }));
    let sidecar: serde_json::Value = serde_json::from_slice(
        publication
            .binding()
            .native_lineage()
            .batch_sidecar()
            .unwrap_or_else(|| panic!("mixed-service sidecar must be retained")),
    )
    .unwrap_or_else(|error| panic!("mixed-service sidecar: {error}"));
    let qualification_services = sidecar["qualifications"]
        .as_array()
        .unwrap_or_else(|| panic!("mixed-service qualification rows"))
        .iter()
        .map(|row| {
            row["service"]
                .as_str()
                .unwrap_or_else(|| panic!("qualification service"))
        })
        .collect::<Vec<_>>();
    assert!(sidecar["qualifications"].as_array().is_some_and(|rows| {
        rows.iter()
            .all(|row| row.get("streamer_doctor_capture_set_evidence").is_some())
    }));
    assert_eq!(
        qualification_services,
        ["LEVELONE_EQUITIES", "LEVELONE_OPTIONS"]
    );
    let state = connector_state
        .lock()
        .unwrap_or_else(|error| panic!("mock connector state: {error}"));
    assert_eq!(
        state.sent.as_slice(),
        [
            ("ADMIN".to_owned(), "LOGIN".to_owned()),
            ("LEVELONE_EQUITIES".to_owned(), "SUBS".to_owned()),
            ("LEVELONE_OPTIONS".to_owned(), "SUBS".to_owned()),
        ]
    );
    drop(state);

    let login_then_close = |request_id: u64| {
        VecDeque::from([
            MockStreamerInbound::Frame(InboundStreamerFrame::Text(Bytes::from(format!(
                r#"{{"response":[{{"service":"ADMIN","command":"LOGIN","requestid":"{request_id}","timestamp":1710000000000,"content":{{"code":0,"msg":"OK"}}}}]}}"#
            )))),
            MockStreamerInbound::Frame(InboundStreamerFrame::Close),
        ])
    };
    let reconnect_state = Arc::new(Mutex::new(MockStreamerState {
        connects: 0,
        inbound: VecDeque::from([
            login_then_close(1),
            login_then_close(3),
            login_then_close(5),
        ]),
        sent: Vec::new(),
    }));
    let reconnect_controls = (51_u64..=53)
        .map(|generation| {
            SchwabStreamerConnectionControl::new(
                ConnectionGeneration::new(
                    NonZeroU64::new(generation)
                        .unwrap_or_else(|| panic!("reconnect generation must be nonzero")),
                ),
                session_identifier.clone(),
                coordinates.clone(),
                stream_identity.clone(),
            )
        })
        .collect::<VecDeque<_>>();
    let reconnect_bounds = StreamerTransportBounds::try_new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_millis(1),
        2,
        nonzero(64 * 1024),
        nonzero(65),
        nonzero(64 * 1024),
        Duration::from_millis(1),
    )
    .unwrap_or_else(|error| panic!("reconnect bounds: {error}"));
    let mut reconnecting_streamer = SchwabStreamerExecutor::try_new(
        Arc::new(MockStreamerConnector {
            state: reconnect_state.clone(),
        }),
        Arc::new(MockTokenSource { token_admission }),
        Arc::new(MockStreamerControlSource {
            controls: Mutex::new(reconnect_controls),
        }),
        stream_admission,
        reconnect_bounds,
        bounds(),
        token_admission,
        SchwabTransportTelemetry::default(),
        Arc::new(MockStreamerRateAuthority::new()),
    )
    .unwrap_or_else(|error| panic!("reconnecting executor: {error}"));
    reconnecting_streamer
        .replace_desired(
            StreamerSubscription::try_new(
                MarketDataService::LevelOneEquities,
                vec![
                    ProviderIdentifier::try_new("AAPL")
                        .unwrap_or_else(|error| panic!("reconnect symbol: {error}")),
                ],
                vec![0, 1, 2, 3, 4],
                stream_admission,
            )
            .unwrap_or_else(|error| panic!("reconnect subscription: {error}")),
        )
        .unwrap_or_else(|error| panic!("reconnect desired state: {error}"));
    let reconnect_cancellation = CancellationToken::new();
    let mut reconnect_sink = CancellingCaptureSink {
        cancellation: reconnect_cancellation.clone(),
        cancel_after: usize::MAX,
        microbatches: Vec::new(),
    };
    assert_eq!(
        reconnecting_streamer
            .run(
                bootstrap.value(),
                &mut reconnect_sink,
                reconnect_cancellation.clone(),
            )
            .await,
        Err(SchwabTransportError::ReconnectExhausted)
    );
    assert!(!reconnect_cancellation.is_cancelled());
    let reconnect_state = reconnect_state
        .lock()
        .unwrap_or_else(|error| panic!("reconnect mock state: {error}"));
    assert_eq!(reconnect_state.connects, 3);
    assert_eq!(
        reconnect_state.sent.as_slice(),
        [
            ("ADMIN".to_owned(), "LOGIN".to_owned()),
            ("LEVELONE_EQUITIES".to_owned(), "SUBS".to_owned()),
            ("ADMIN".to_owned(), "LOGIN".to_owned()),
            ("LEVELONE_EQUITIES".to_owned(), "SUBS".to_owned()),
            ("ADMIN".to_owned(), "LOGIN".to_owned()),
            ("LEVELONE_EQUITIES".to_owned(), "SUBS".to_owned()),
        ]
    );
}

#[derive(Debug)]
struct MockHttpWire {
    response: Mutex<Option<SchwabHttpWireResponse>>,
    expected_route: ReadOnlyRoute,
    calls: Mutex<u64>,
}

impl MockHttpWire {
    fn new(response: SchwabHttpWireResponse, expected_route: ReadOnlyRoute) -> Self {
        Self {
            response: Mutex::new(Some(response)),
            expected_route,
            calls: Mutex::new(0),
        }
    }
}

impl SchwabHttpWire for MockHttpWire {
    fn get<'a>(
        &'a self,
        request: SchwabHttpWireRequest<'a>,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabHttpWireResponse, SchwabTransportError>> + Send + 'a>,
    > {
        Box::pin(async move {
            assert_eq!(request.request().route(), self.expected_route);
            let mut calls = self
                .calls
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?;
            *calls = calls.checked_add(1).ok_or(SchwabTransportError::Overflow)?;
            self.response
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .take()
                .ok_or(SchwabTransportError::Protocol)
        })
    }
}

async fn execute_market_fixture(
    request: &crate::ReadOnlyRequest,
    body: &'static [u8],
    token: &TransientAccessToken,
    token_admission: AccessTokenAdmission,
) -> crate::ExecutedRestResponse {
    let outcome = execute_fixture(request, body, token, token_admission).await;
    let capacity = outcome
        .capacity_observation()
        .unwrap_or_else(|error| panic!("response-owned capacity evidence: {error}"));
    let response = match outcome {
        RestExecutionOutcome::Accepted(response) => response,
        other => panic!("unexpected market REST outcome: {other:?}"),
    };
    let accounting = response.accounting();
    assert_eq!(capacity.requested(), accounting.requested);
    assert_eq!(capacity.returned(), accounting.returned);
    assert_eq!(capacity.missing(), accounting.missing);
    assert_eq!(capacity.unexpected(), accounting.unexpected);
    assert_eq!(
        capacity.request_bytes(),
        u64::try_from(request.request_target().as_bytes().len())
            .unwrap_or_else(|error| panic!("request-target bytes: {error}"))
    );
    assert_eq!(
        capacity.response_bytes(),
        response.capture().receipt().body_bytes()
    );
    assert_eq!(capacity.status(), response.capture().receipt().status());
    assert!(!capacity.validation_failed());
    response
}

async fn assert_sealed_rest_family(
    request: &crate::ReadOnlyRequest,
    body: &'static [u8],
    expected_family: SchwabRestFamily,
    token: &TransientAccessToken,
    token_admission: AccessTokenAdmission,
    store: &SealedResearchJournalStore,
) -> crate::SchwabSealedRestResponse {
    let response = execute_market_fixture(request, body, token, token_admission).await;
    let doctor_family = match expected_family {
        SchwabRestFamily::Quotes => SchwabObservedCapabilityFamily::Quotes,
        SchwabRestFamily::OptionChain => SchwabObservedCapabilityFamily::OptionChain,
        SchwabRestFamily::ExpirationChain => SchwabObservedCapabilityFamily::ExpirationChain,
        SchwabRestFamily::DailyPriceHistory => SchwabObservedCapabilityFamily::DailyPriceHistory,
        SchwabRestFamily::MarketHours => SchwabObservedCapabilityFamily::MarketHours,
        SchwabRestFamily::Movers => SchwabObservedCapabilityFamily::Movers,
        SchwabRestFamily::Instruments => SchwabObservedCapabilityFamily::Instruments,
    };
    let doctor = SchwabRestFamilyDoctorInput::try_new(doctor_family, &response)
        .unwrap_or_else(|error| panic!("typed {expected_family:?} doctor input: {error}"));
    assert_eq!(doctor.family(), doctor_family);
    let route = response.capture().receipt().route();
    let body_digest = response.capture().receipt().body_sha256();
    let body_bytes = response.capture().receipt().body_bytes();
    let received_at_unix_millis = response.capture().receipt().received_at_unix_millis();
    let provider_records = response.accounting().provider_records;
    let event_id = Uuid::new_v4();
    let pending = response
        .into_pending_capture(capture_coordinates(), event_id)
        .unwrap_or_else(|error| panic!("pending {expected_family:?} capture: {error}"));
    let (rejoin, seal_request) = pending.into_sealing_parts();
    let sealed_material = seal_request
        .seal(store)
        .unwrap_or_else(|error| panic!("seal {expected_family:?} capture: {error}"));
    let sealed = rejoin
        .try_rejoin(sealed_material)
        .unwrap_or_else(|error| panic!("rejoin {expected_family:?} capture: {error}"));
    assert_eq!(sealed.family(), expected_family);
    assert_eq!(sealed.route(), route);
    assert_eq!(sealed.receipt().body_sha256(), body_digest);
    assert_eq!(sealed.receipt().body_bytes(), body_bytes);
    assert_eq!(sealed.accounting().provider_records, provider_records);
    let persisted = sealed.persisted_receipt();
    let [page] = persisted.capture().pages() else {
        panic!("sealed {expected_family:?} response must have exactly one page");
    };
    assert_eq!(page.body_digest().bytes(), body_digest);
    assert_eq!(page.body_bytes(), body_bytes);
    let reopened = store
        .open_verified(persisted.segment())
        .unwrap_or_else(|error| panic!("reopen {expected_family:?} capture: {error}"));
    let [record] = reopened.records() else {
        panic!("sealed {expected_family:?} response must have exactly one raw record");
    };
    assert_eq!(record.event_id(), event_id);
    assert_eq!(record.payload(), body);
    assert_eq!(
        record.received_at().timestamp_millis() as u64,
        received_at_unix_millis
    );
    sealed
}

async fn execute_fixture(
    request: &crate::ReadOnlyRequest,
    body: &'static [u8],
    token: &TransientAccessToken,
    token_admission: AccessTokenAdmission,
) -> RestExecutionOutcome {
    let body = Bytes::from_static(body);
    let rest_bounds = RestTransportBounds::try_new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(2),
        nonzero(64 * 1024),
        nonzero(8),
        nonzero(2 * 1024),
    )
    .unwrap_or_else(|error| panic!("REST bounds: {error}"));
    let response = SchwabHttpWireResponse::try_new(
        200,
        request.url().to_owned(),
        Some(u64::try_from(body.len()).unwrap_or_else(|error| panic!("body length: {error}"))),
        vec![
            ResponseHeaderEvidence::try_new(
                "content-type".to_owned(),
                b"application/json".to_vec(),
            )
            .unwrap_or_else(|error| panic!("header evidence: {error}")),
        ],
        body,
        rest_bounds,
    )
    .unwrap_or_else(|error| panic!("mock response: {error}"));
    assert!(!format!("{response:?}").contains("must-not-enter-raw-capture"));
    let executor = SchwabRestExecutor::try_new(
        Arc::new(MockHttpWire::new(response, request.route())),
        rest_bounds,
        bounds(),
        token_admission,
        SchwabTransportTelemetry::default(),
    )
    .unwrap_or_else(|error| panic!("REST executor: {error}"));
    let outcome = executor
        .execute(request, token, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("REST execution: {error}"));
    assert_eq!(
        executor
            .telemetry()
            .snapshot()
            .unwrap_or_else(|error| panic!("REST telemetry: {error}"))
            .request_target_bytes_total,
        u64::try_from(request.request_target().as_bytes().len())
            .unwrap_or_else(|error| panic!("request-target telemetry bytes: {error}"))
    );
    outcome
}

#[derive(Debug)]
struct MockTokenSource {
    token_admission: AccessTokenAdmission,
}

impl SchwabAccessTokenSource for MockTokenSource {
    fn acquire(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<TransientAccessToken, TokenAuthorityError>> + Send + '_>>
    {
        Box::pin(async move { Ok(mock_token(self.token_admission)) })
    }
}

#[derive(Debug)]
struct MockStreamerControlSource {
    controls: Mutex<VecDeque<SchwabStreamerConnectionControl>>,
}

impl SchwabStreamerConnectionControlSource for MockStreamerControlSource {
    fn mint(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SchwabStreamerConnectionControl, SchwabTransportError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            self.controls
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .pop_front()
                .ok_or(SchwabTransportError::Protocol)
        })
    }
}

#[derive(Debug)]
struct MockStreamerState {
    connects: u64,
    inbound: VecDeque<VecDeque<MockStreamerInbound>>,
    sent: Vec<(String, String)>,
}

#[derive(Debug)]
enum MockStreamerInbound {
    Frame(InboundStreamerFrame),
    FlushBoundary,
}

#[derive(Debug)]
struct MockStreamerConnector {
    state: Arc<Mutex<MockStreamerState>>,
}

impl SchwabStreamerConnector for MockStreamerConnector {
    fn connect<'a>(
        &'a self,
        _endpoint: &'a str,
        _bounds: StreamerTransportBounds,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerConnection>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let inbound = {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| SchwabTransportError::Protocol)?;
                state.connects = state
                    .connects
                    .checked_add(1)
                    .ok_or(SchwabTransportError::Overflow)?;
                state
                    .inbound
                    .pop_front()
                    .ok_or(SchwabTransportError::Protocol)?
            };
            Ok(Box::new(MockStreamerConnection {
                state: self.state.clone(),
                inbound,
            }) as Box<dyn SchwabStreamerConnection>)
        })
    }
}

impl crate::transport::SealedSchwabStreamerConnector for MockStreamerConnector {}

struct MockStreamerConnection {
    state: Arc<Mutex<MockStreamerState>>,
    inbound: VecDeque<MockStreamerInbound>,
}

impl fmt::Debug for MockStreamerConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MockStreamerConnection(..)")
    }
}

impl crate::transport::SealedSchwabStreamerConnection for MockStreamerConnection {}

impl SchwabStreamerConnection for MockStreamerConnection {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            let value: serde_json::Value =
                serde_json::from_slice(&payload).map_err(|_| SchwabTransportError::Protocol)?;
            let request = value
                .get("requests")
                .and_then(serde_json::Value::as_array)
                .and_then(|requests| requests.first())
                .and_then(serde_json::Value::as_object)
                .ok_or(SchwabTransportError::Protocol)?;
            let service = request
                .get("service")
                .and_then(serde_json::Value::as_str)
                .ok_or(SchwabTransportError::Protocol)?
                .to_owned();
            let command = request
                .get("command")
                .and_then(serde_json::Value::as_str)
                .ok_or(SchwabTransportError::Protocol)?
                .to_owned();
            assert_eq!(
                request
                    .get("SchwabClientCustomerId")
                    .and_then(serde_json::Value::as_str),
                Some("customer")
            );
            assert_eq!(
                request
                    .get("SchwabClientCorrelId")
                    .and_then(serde_json::Value::as_str),
                Some("correlation")
            );
            if command != "LOGIN" {
                assert!(
                    request
                        .get("parameters")
                        .and_then(|parameters| parameters.get("Authorization"))
                        .is_none()
                );
            }
            self.state
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?
                .sent
                .push((service, command));
            Ok(())
        })
    }

    fn send_pong<'a>(
        &'a mut self,
        _payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            match self.inbound.pop_front() {
                Some(MockStreamerInbound::Frame(frame)) => Ok(Some(frame)),
                Some(MockStreamerInbound::FlushBoundary) => {
                    pending::<Result<Option<InboundStreamerFrame>, SchwabTransportError>>().await
                }
                None => {
                    pending::<Result<Option<InboundStreamerFrame>, SchwabTransportError>>().await
                }
            }
        })
    }

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

struct CancellingCaptureSink {
    cancellation: CancellationToken,
    cancel_after: usize,
    microbatches: Vec<StreamerMicrobatch>,
}

impl StreamerCaptureSink for CancellingCaptureSink {
    fn try_publish(
        &mut self,
        microbatch: StreamerMicrobatch,
    ) -> Result<(), StreamerCaptureSinkError> {
        self.microbatches.push(microbatch);
        if self.microbatches.len() >= self.cancel_after {
            self.cancellation.cancel();
        }
        Ok(())
    }
}

fn seal_stream_microbatch(
    microbatch: StreamerMicrobatch,
    store: &SealedResearchJournalStore,
) -> SchwabSealedStreamerCapture {
    let event_ids = (0..microbatch.frames().len())
        .map(|_| Uuid::new_v4())
        .collect::<Vec<_>>();
    let (pending, request) = microbatch
        .into_pending_capture(event_ids, bounds())
        .unwrap_or_else(|error| panic!("pending Streamer capture: {error}"));
    let sealed = request
        .seal(store)
        .unwrap_or_else(|error| panic!("Streamer physical seal: {error}"));
    pending
        .try_rejoin(sealed)
        .unwrap_or_else(|error| panic!("sealed Streamer capture: {error}"))
}

struct AuthorizedOAuthFixture {
    authority: ProtectedSchwabOAuthAuthority,
    secrets: Arc<EncryptedFileSecretStore>,
    control: SecretOperationControl,
    application_key: SecretKey,
    application_ref: SecretRef,
    token_ref: SecretRef,
    state_root: PathBuf,
}

async fn authorized_oauth_fixture(root: PathBuf, issued_at: u64) -> AuthorizedOAuthFixture {
    let secrets = Arc::new(
        EncryptedFileSecretStore::try_open(
            root.join("secrets"),
            SecretValue::new("schwab-lifecycle-test-unlock".to_owned())
                .unwrap_or_else(|error| panic!("lifecycle OAuth unlock: {error}")),
        )
        .unwrap_or_else(|error| panic!("lifecycle OAuth secret store: {error}")),
    );
    let control = SecretOperationControl::try_new(
        "schwab-lifecycle-test",
        Instant::now() + Duration::from_secs(60),
        0,
        SecretInteractionPolicy::Forbid,
        SecretCancellation::new(),
    )
    .unwrap_or_else(|error| panic!("lifecycle OAuth secret control: {error}"));
    let application_key = SecretKey::try_new("market-squawk.schwab", "lifecycle-application")
        .unwrap_or_else(|error| panic!("lifecycle application key: {error}"));
    let application_ref = secrets
        .create(
            &application_key,
            SecretGeneration::new(1)
                .unwrap_or_else(|error| panic!("lifecycle application generation: {error}")),
            SecretValue::new(
                r#"{"version":1,"app_key":"lifecycle-key","app_secret":"lifecycle-secret"}"#
                    .to_owned(),
            )
            .unwrap_or_else(|error| panic!("lifecycle application value: {error}")),
            &control,
        )
        .unwrap_or_else(|error| panic!("lifecycle application storage: {error}"));
    let token_key = SecretKey::try_new("market-squawk.schwab", "oauth-token")
        .unwrap_or_else(|error| panic!("lifecycle token key: {error}"));
    let token_ref = secrets
        .plan_create(
            &token_key,
            SecretGeneration::new(1)
                .unwrap_or_else(|error| panic!("lifecycle token generation: {error}")),
            &control,
        )
        .unwrap_or_else(|error| panic!("lifecycle token plan: {error}"))
        .target()
        .clone();
    let state_root = root.join("state");
    let authority = ProtectedSchwabOAuthAuthority::try_open(
        &state_root,
        test_oauth_configuration(
            secrets.clone(),
            application_ref.clone(),
            Arc::new(ShortLivedOAuthWire::default()),
        ),
    )
    .await
    .unwrap_or_else(|error| panic!("lifecycle OAuth authority: {error}"));
    let authorization = authority
        .authorization_request(
            "authority-lifecycle",
            admission(),
            SchwabOAuthInteraction::Background,
        )
        .await
        .unwrap_or_else(|error| panic!("lifecycle authorization request: {error}"));
    assert!(
        authorization
            .expose_url()
            .contains("client_id=lifecycle-key")
    );
    let callback = match OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=lifecycle-code&state=authority-lifecycle",
        "authority-lifecycle",
        admission(),
    ) {
        Ok(CallbackOutcome::Authorized(callback)) => callback,
        outcome => panic!("lifecycle OAuth callback: {outcome:?}"),
    };
    authority
        .complete_authorization(&callback, issued_at, SchwabOAuthInteraction::Background)
        .await
        .unwrap_or_else(|error| panic!("lifecycle OAuth completion: {error}"));
    secrets
        .read(&token_ref, &control)
        .unwrap_or_else(|error| panic!("lifecycle protected token: {error}"));
    AuthorizedOAuthFixture {
        authority,
        secrets,
        control,
        application_key,
        application_ref,
        token_ref,
        state_root,
    }
}

fn test_oauth_configuration(
    secrets: Arc<EncryptedFileSecretStore>,
    application_ref: SecretRef,
    wire: Arc<dyn SchwabOAuthWire>,
) -> SchwabOAuthAuthorityConfiguration {
    let secret_authority: Arc<dyn SecretStore> = secrets;
    SchwabOAuthAuthorityConfiguration::try_new(
        secret_authority,
        wire,
        application_ref,
        SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)
            .unwrap_or_else(|error| panic!("lifecycle OAuth secret policy: {error}")),
        bounds(),
        AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1)),
        5,
    )
    .unwrap_or_else(|error| panic!("lifecycle OAuth configuration: {error}"))
}

fn mock_token(admission: AccessTokenAdmission) -> TransientAccessToken {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("system clock: {error}"))
        .as_secs();
    TransientAccessToken::try_new(
        "mock-access-token".to_owned(),
        AccessTokenGeneration::new(NonZeroU64::MIN),
        SchwabCredentialAuthorityBinding::for_test(
            SecretGeneration::new(1)
                .unwrap_or_else(|error| panic!("mock credential generation: {error}")),
            91,
        ),
        now,
        now.checked_add(1_800)
            .unwrap_or_else(|| panic!("token expiry overflow")),
        admission,
    )
    .unwrap_or_else(|error| panic!("mock token: {error}"))
}

#[allow(
    clippy::too_many_arguments,
    reason = "the focused mixed-service proof keeps exact physical and semantic coordinates explicit"
)]
fn test_streamer_quote_record_request(
    coordinates: &SchwabCaptureCoordinates,
    stream_identity: &SourceIdentifier,
    frame_generation: ConnectionGeneration,
    frame_digest: [u8; 32],
    received_at: Timestamp,
    qualification: SchwabMarketDataQualification,
    service: MarketDataService,
    data_batch_ordinal: u16,
    symbol: &str,
    venue: &str,
    evidence_byte: u8,
) -> SchwabStreamerQuoteRecordRequest {
    let dictionary_version = match service {
        MarketDataService::LevelOneEquities => "schwab-streamer-fields-level-one-equities-v1",
        MarketDataService::LevelOneOptions => "schwab-streamer-fields-level-one-options-v1",
        _ => panic!("focused quote fixture requires an admitted Level-One service"),
    };
    let venue_id =
        VenueId::try_from(venue).unwrap_or_else(|error| panic!("mixed-service venue: {error}"));
    let instrument_id = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("mixed-service instrument id: {error}"));
    let source_identifier = SourceIdentifier::try_from(symbol)
        .unwrap_or_else(|error| panic!("mixed-service source identifier: {error}"));
    let canonical_state = CanonicalStateDigest::new(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [evidence_byte; 32]),
        CanonicalizationRule::new(
            SourceIdentifier::try_from("schwab-level-one-quote-state-v1")
                .unwrap_or_else(|error| panic!("canonical rule: {error}")),
            RuleVersion::new(1).unwrap_or_else(|error| panic!("rule version: {error}")),
        ),
    );
    let live_binding = LiveEvidenceBinding::new(
        coordinates.source_id().clone(),
        stream_identity.clone(),
        coordinates.metadata_revision().clone(),
        AuthorizationBasis::new(
            SourceIdentifier::try_from("schwab-read-only-oauth")
                .unwrap_or_else(|error| panic!("authorization basis: {error}")),
        ),
        venue_id.clone(),
        instrument_id,
        market_squawk_domain::ConnectionGeneration::new(frame_generation.get())
            .unwrap_or_else(|error| panic!("domain connection generation: {error}")),
        qualification.provider_product().clone(),
        qualification.provider_channel().clone(),
        LiveEventClass::Quote,
        source_identifier.clone(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, frame_digest),
        canonical_state,
        None,
    )
    .unwrap_or_else(|error| panic!("mixed-service live binding: {error}"));
    // The fixture carries an envelope time but no native QuoteTime field.
    let provenance = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        live_binding,
        None,
        received_at,
        received_at,
        received_at,
        DataQuality::DirectUnverified,
        CoverageStatus::Unknown,
        PayloadReference::ContentHash(PayloadHash::new(DigestAlgorithm::Sha256, frame_digest)),
    ))
    .unwrap_or_else(|error| panic!("mixed-service quote provenance: {error}"));
    let dictionary = SchwabStreamerFieldDictionary::try_new(
        service,
        SourceIdentifier::try_from(dictionary_version)
            .unwrap_or_else(|error| panic!("mixed-service dictionary version: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [evidence_byte.wrapping_add(1); 32]),
        vec![
            (1, SchwabStreamerSemanticField::BidPrice),
            (2, SchwabStreamerSemanticField::AskPrice),
            (3, SchwabStreamerSemanticField::BidSize),
            (4, SchwabStreamerSemanticField::AskSize),
        ],
    )
    .unwrap_or_else(|error| panic!("mixed-service dictionary: {error}"));
    let reference =
        (|| -> Result<market_squawk_domain::MarketDataReference, Box<dyn std::error::Error>> {
            use market_squawk_domain::{
                AssetClass, Currency, EffectiveInterval, ExactPayloadEvidence,
                MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput,
                MarketDataReference, ProviderIdentityEvidence, ProviderIdentityRecord,
                ProviderIdentityRecordInput, RevisionBoundPayloadEvidence,
            };
            let interval = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
            let identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id,
                source_id: SourceId::try_from("schwab-trader-api-instruments")?,
                provider_instrument_id: ProviderInstrumentId::try_from(symbol)?,
                evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    [evidence_byte.wrapping_add(2); 32],
                )),
                source_timestamp: None,
                observed_at: received_at,
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
                    "schwab-instruments-test-v1",
                )?),
                validity: interval,
                supersedes: None,
            });
            let definition =
                MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                    instrument_id,
                    reference_evidence: RevisionBoundPayloadEvidence::new(
                        MetadataRevision::new(SourceIdentifier::try_from("schwab-test-reference")?),
                        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                            DigestAlgorithm::Sha256,
                            [evidence_byte.wrapping_add(3); 32],
                        )),
                    ),
                    effective_interval: interval,
                    asset_class: match service {
                        MarketDataService::LevelOneEquities => AssetClass::Equity,
                        MarketDataService::LevelOneOptions => AssetClass::Option,
                        _ => panic!("focused quote fixture requires an admitted Level-One service"),
                    },
                    display_name: None,
                    quote_currency: Currency::try_from("USD")?,
                    quote_currency_evidence: ExactPayloadEvidence::from_content_digest(
                        EvidenceDigest::new(
                            DigestAlgorithm::Sha256,
                            [evidence_byte.wrapping_add(4); 32],
                        ),
                    ),
                    venue_mappings: vec![],
                    provider_identities: vec![identity.clone()],
                    identifiers: vec![],
                })?;
            let digest = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                <sha2::Sha256 as sha2::Digest>::digest(serde_json::to_vec(&definition)?).into(),
            );
            Ok(MarketDataReference::try_new(
                &definition,
                digest,
                &identity,
                received_at,
            )?)
        })()
        .unwrap_or_else(|error| panic!("mixed-service quote reference: {error}"));
    let market_evidence = SchwabStreamerQuoteMarketDataEvidence::try_new(venue_id, qualification)
        .unwrap_or_else(|error| panic!("mixed-service market evidence: {error}"));
    SchwabStreamerQuoteRecordRequest::new(
        0,
        data_batch_ordinal,
        0,
        dictionary,
        reference,
        provenance,
        market_evidence,
    )
}

fn test_rest_qualification(
    response: &crate::SchwabSealedRestResponse,
    oauth: SchwabOAuthAuthorityReceipt,
    session: SourceIdentifier,
) -> SchwabMarketDataQualification {
    SchwabMarketDataQualification::try_from_sealed_rest_response(
        response,
        oauth,
        session,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
    )
    .expect("actual sealed response qualification")
}

fn test_streamer_qualification(
    handoff: &crate::SchwabStreamerFamilyDoctorHandoff,
    observed_at: Timestamp,
    oauth: SchwabOAuthAuthorityReceipt,
    session: SourceIdentifier,
) -> SchwabMarketDataQualification {
    SchwabMarketDataQualification::try_from_streamer_handoff(
        handoff,
        observed_at,
        oauth,
        session,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [75; 32]),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [73; 32]),
    )
    .expect("actual same-socket ACK/data qualification")
}

fn capture_coordinates() -> SchwabCaptureCoordinates {
    let source_id = SourceId::try_from("schwab-market-data")
        .unwrap_or_else(|error| panic!("source id: {error}"));
    let revision = SourceIdentifier::try_from("schwab-native-v1")
        .map(MetadataRevision::new)
        .unwrap_or_else(|error| panic!("metadata revision: {error}"));
    let dataset = SourceIdentifier::try_from("schwab-provider-evidence")
        .unwrap_or_else(|error| panic!("dataset: {error}"));
    SchwabCaptureCoordinates::try_new(source_id, revision, dataset, Uuid::new_v4())
        .unwrap_or_else(|error| panic!("capture coordinates: {error}"))
}
