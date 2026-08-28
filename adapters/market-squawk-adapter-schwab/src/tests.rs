use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, pending};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{
    AuthorizationBasis, BarTimeSemantics, BarTimestampBasis, CanonicalStateDigest,
    CanonicalizationRule, CoverageStatus, Currency, DataQuality, DecodedLiveProvenanceInput,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    LiveEventClass, LiveEvidenceBinding, LiveProvenance, LotSize, MarketBarAdjustment,
    MarketBarSessionEvidence, MarketBarSessionKind, MarketDepth, MarketEvent, MetadataRevision,
    OptionComponentState, PayloadHash, PayloadReference, ProviderChannel, ProviderInstrumentId,
    ProviderProduct, RuleVersion, SourceId, SourceIdentifier, TickSize, Timestamp, VenueId,
};
use market_squawk_platform::{
    EncryptedFileSecretStore, LocalPaths, SealedResearchJournalStore, SecretCancellation,
    SecretGeneration, SecretInteractionPolicy, SecretKey, SecretOperationControl, SecretStore,
    SecretValue,
};
use market_squawk_sources::{
    AvailabilityEvidence, DiscoveryRequest, ExtractionRequest, OptionMarketBatchKind, SourceObject,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenAdmission, AccessTokenGeneration,
    CallbackOutcome, ChainRequest, ConnectionGeneration, ConnectionState, DesiredStateController,
    ExpirationChainRequest, HttpMethod, InboundStreamerFrame, InstrumentProjection,
    MarketDataService, MarketId, MoverFrequency, MoverSort, OAuthCallback, ParseBounds,
    PriceHistoryFrequency, PriceHistoryFrequencyType, PriceHistoryRequest,
    ProtectedSchwabOAuthAuthority, ProviderIdentifier, QuoteRequest, RawStreamerFrameKind,
    ReadOnlyRoute, RefreshTokenGeneration, RequestAdmission, ResponseHeaderEvidence,
    RestExecutionOutcome, RestTransportBounds, SchwabAccessTokenSource, SchwabAdapterError,
    SchwabApplicationCredentialReplacement, SchwabCanonicalError, SchwabCanonicalField,
    SchwabCaptureCoordinates, SchwabDailyPriceHistoryPublicationRequest, SchwabHttpWire,
    SchwabHttpWireRequest, SchwabHttpWireResponse, SchwabOAuthAuthorityConfiguration,
    SchwabOAuthAuthorityStatus, SchwabOAuthInteraction, SchwabOAuthSecretPolicy, SchwabOAuthWire,
    SchwabOAuthWireError, SchwabOAuthWireRequest, SchwabOAuthWireResponse,
    SchwabObservedCapabilityFamily, SchwabOptionCandidateOutcome,
    SchwabPriceHistoryCapabilityObservation, SchwabPriceHistoryMarketDataEvidence,
    SchwabResolvedProviderIdentity, SchwabRestDelayEvidence, SchwabRestExecutor, SchwabRestFamily,
    SchwabRestFamilyDoctorInput, SchwabRestOptionContractRequest,
    SchwabRestOptionMarketDataEvidence, SchwabRestOptionPublicationOutcome,
    SchwabRestOptionPublicationRequest, SchwabRestOptionUnderlyingRequest,
    SchwabRestQuoteMarketDataEvidence, SchwabRestQuotePublicationOutcome,
    SchwabRestQuotePublicationRequest, SchwabRestQuoteRecordRequest, SchwabStreamerConnection,
    SchwabStreamerConnectionControl, SchwabStreamerConnectionControlSource,
    SchwabStreamerConnector, SchwabStreamerDelayEvidence, SchwabStreamerExecutor,
    SchwabStreamerFamilyDoctorInput, SchwabStreamerFieldDictionary,
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
struct ShortLivedOAuthWire;

impl SchwabOAuthWire for ShortLivedOAuthWire {
    fn exchange(
        &self,
        _request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    > {
        Box::pin(async {
            SchwabOAuthWireResponse::try_new(
                200,
                br#"{"access_token":"short-access","refresh_token":"short-refresh","token_type":"Bearer","expires_in":30,"scope":"market-data"}"#.to_vec(),
                nonzero(4 * 1024),
            )
        })
    }
}

#[derive(Debug, Default)]
struct RetryableRefreshOAuthWire {
    calls: Mutex<u64>,
}

impl SchwabOAuthWire for RetryableRefreshOAuthWire {
    fn exchange(
        &self,
        _request: SchwabOAuthWireRequest,
    ) -> Pin<
        Box<dyn Future<Output = Result<SchwabOAuthWireResponse, SchwabOAuthWireError>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut calls = self
                .calls
                .lock()
                .map_err(|_| SchwabOAuthWireError::Network)?;
            *calls = calls.checked_add(1).ok_or(SchwabOAuthWireError::Network)?;
            let body = match *calls {
                1 => br#"{"access_token":"initial-access","refresh_token":"initial-refresh","token_type":"Bearer","expires_in":30,"scope":"market-data"}"#.as_slice(),
                2 => br#"{"malformed":"retryable-parse-failure"}"#.as_slice(),
                _ => br#"{"access_token":"refreshed-access","refresh_token":"refreshed-refresh","token_type":"Bearer","expires_in":30,"scope":"market-data"}"#.as_slice(),
            };
            SchwabOAuthWireResponse::try_new(200, body.to_vec(), nonzero(4 * 1024))
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

#[test]
fn oauth_lifecycle_and_read_only_route_allowlist_fail_closed() {
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
}

#[test]
fn rest_and_streamer_native_parsing_preserve_evidence_and_one_connection_semantics() {
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
      "underlyingPrice":500.1,
      "callExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"CALL","symbol":"SPY C","bid":1.2,"ask":1.3,"volatility":0.2,"delta":0.51,"gamma":0.03,"theta":-0.02,"vega":0.08,"rho":0.04,"openInterest":10}]}},
      "putExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"PUT","symbol":"SPY P","bid":1.1,"ask":1.4,"volatility":0.21,"delta":-0.49,"gamma":0.03,"theta":-0.02,"vega":0.08,"rho":-0.04,"openInterest":11}]}}
    }"#;
    let parsed_chain = parse_option_chain_response(chain, bounds())
        .unwrap_or_else(|error| panic!("chain payload: {error}"));
    assert_eq!(parsed_chain.value().contracts().len(), 2);
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
      "data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","timestamp":1710000000001,"content":[{"key":"AAPL","1":100.125,"2":100.25}]}]
    }"#;
    let frame = parse_streamer_frame(stream, bounds())
        .unwrap_or_else(|error| panic!("stream frame: {error}"));
    assert_eq!(
        frame.value().responses[0].code,
        StreamerResponseCode::Success
    );
    assert_eq!(frame.value().data[0].content[0].fields.len(), 2);
    let dictionary = SchwabStreamerFieldDictionary::try_new(
        MarketDataService::LevelOneEquities,
        SourceIdentifier::try_from("schwab-streamer-test-fixture-v1")
            .unwrap_or_else(|error| panic!("dictionary version: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]),
        vec![
            (1, SchwabStreamerSemanticField::BidPrice),
            (2, SchwabStreamerSemanticField::AskPrice),
        ],
    )
    .unwrap_or_else(|error| panic!("dictionary: {error}"));
    let mapped = canonicalize_streamer_batch(&frame.value().data[0], &dictionary)
        .unwrap_or_else(|error| panic!("stream canonicalization: {error}"));
    assert_eq!(mapped.len(), 1);
    assert_eq!(mapped[0].fields.len(), 2);
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
    assert_eq!(controller.login_accepted(generation), Ok(()));
    assert_eq!(controller.state(), ConnectionState::Active(generation));
    let requests = controller
        .replay_desired()
        .unwrap_or_else(|error| panic!("desired replay: {error}"));
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].command(), "SUBS");
    assert!(!String::from_utf8_lossy(requests[0].expose_body()).contains("ACCOUNT_ACTIVITY"));

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
async fn rest_price_history_moves_once_through_sealed_publication_and_excludes_user_preference() {
    let temporary = TemporaryDirectory::new();
    let secrets = Arc::new(
        EncryptedFileSecretStore::try_open(
            temporary.path().join("oauth-secrets"),
            SecretValue::new("schwab-test-unlock".to_owned())
                .unwrap_or_else(|error| panic!("OAuth unlock: {error}")),
        )
        .unwrap_or_else(|error| panic!("OAuth secret store: {error}")),
    );
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
        secrets.clone(),
        Arc::new(ShortLivedOAuthWire),
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
    let oauth_receipt = oauth_authority
        .complete_authorization(&callback, issued_at, SchwabOAuthInteraction::Background)
        .await
        .unwrap_or_else(|error| panic!("OAuth completion: {error}"));
    let token = mock_token(token_admission);
    let preference_request = crate::ReadOnlyRequest::user_preference(admission())
        .unwrap_or_else(|error| panic!("preference request: {error}"));
    let preference = execute_user_preference_fixture(
        &preference_request,
        br#"{
          "accounts":[{"accountNumber":"must-not-enter-raw-capture"}],
          "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
          "offers":[{"mktDataPermission":"NP"}]
        }"#,
        &token,
        token_admission,
    )
    .await;
    let changed_preference = execute_user_preference_fixture(
        &preference_request,
        br#"{
          "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
          "offers":[{"mktDataPermission":"NP"}],
          "changedBootstrapField":true
        }"#,
        &token,
        token_admission,
    )
    .await;
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
    let history_body: &'static [u8] = br#"{
      "symbol":"SPY","empty":false,
      "candles":[{"open":475.00,"high":477.00,"low":474.50,"close":476.25,"volume":1000,"datetime":1704067200000}]
    }"#;
    let history =
        execute_market_fixture(&history_request, history_body, &token, token_admission).await;
    let observed_at = history.capture().receipt().received_at_unix_millis() / 1_000;
    let capability = SchwabPriceHistoryCapabilityObservation::try_observe(
        oauth_receipt,
        &preference,
        &history,
        observed_at,
        Duration::from_secs(10),
    )
    .unwrap_or_else(|error| panic!("history capability: {error}"));

    assert_eq!(
        SchwabPriceHistoryCapabilityObservation::try_observe(
            oauth_receipt,
            &preference,
            &history,
            observed_at,
            Duration::from_secs(60),
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
    let request_for = |user_preference| SchwabDailyPriceHistoryCandidateRequest {
        capability,
        oauth_authority: oauth_receipt,
        user_preference,
        response: &history,
        instrument_id,
        instrument_revision_digest,
        admitted_plan_digest,
        identity: identity.clone(),
        venue_id: venue_id.clone(),
        feed: feed.clone(),
        interval: interval.clone(),
        adjustment: MarketBarAdjustment::Raw,
        currency,
        time_semantics: vec![time_semantics.clone()],
        completeness_evidence,
        ingested_at: received_at,
    };

    assert!(matches!(
        prepare_price_history_candidate(request_for(&changed_preference)),
        Err(SchwabCanonicalError::PendingHistoryBinding)
    ));
    let candidate = prepare_price_history_candidate(request_for(&preference))
        .unwrap_or_else(|error| panic!("pending history candidate: {error}"));
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
        feed.clone(),
        SchwabRestDelayEvidence::Unknown,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [26; 32]),
    )
    .unwrap_or_else(|error| panic!("history market-data evidence: {error}"));
    let publication_request = SchwabDailyPriceHistoryPublicationRequest::new(
        capability,
        oauth_receipt,
        &preference,
        extraction_request,
        instrument_id,
        instrument_revision_digest,
        admitted_plan_digest,
        identity,
        market_data,
        interval,
        MarketBarAdjustment::Raw,
        currency,
        vec![time_semantics],
        completeness_evidence,
        received_at,
    );
    let event_id = Uuid::new_v4();
    let (pending, seal_request) = history
        .into_pending_daily_price_history_publication(
            registered_coordinates.clone(),
            event_id,
            publication_request,
        )
        .unwrap_or_else(|error| panic!("pending REST publication: {error}"));
    let paths = LocalPaths::prepare(temporary.path().join("raw-publication"))
        .unwrap_or_else(|error| panic!("raw publication paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("raw publication store: {error}"));
    let sealed = seal_request
        .seal(&store)
        .unwrap_or_else(|error| panic!("REST physical seal: {error}"));
    let publication = pending
        .try_rejoin(sealed)
        .unwrap_or_else(|error| panic!("sealed REST publication: {error}"));
    assert_eq!(publication.market_data().route(), route);
    assert_eq!(
        publication.market_data().delay(),
        SchwabRestDelayEvidence::Unknown
    );
    assert_eq!(publication.revision_plan().len(), 1);
    assert!(publication.revision_plan().native_lineage_required());
    let binding = publication.sealed_capture_binding();
    assert_eq!(binding.batch().records().len(), 1);
    assert_eq!(binding.native_lineage().rows().len(), 1);
    let sidecar = binding
        .native_lineage()
        .batch_sidecar()
        .unwrap_or_else(|| panic!("missing Schwab REST native-lineage sidecar"));
    let sidecar_value: serde_json::Value = serde_json::from_slice(sidecar.semantic_payload())
        .unwrap_or_else(|error| panic!("Schwab REST sidecar JSON: {error}"));
    assert_eq!(sidecar_value["service"], "schwab-market-data-rest");
    assert_eq!(sidecar_value["route"], "price-history");
    assert_eq!(sidecar_value["feed"], "schwab-daily-price-history");
    assert_eq!(sidecar_value["venue"], "XNYS");
    assert_eq!(sidecar_value["delay"]["kind"], "unknown");
    let native_row: serde_json::Value =
        serde_json::from_slice(binding.native_lineage().rows()[0].semantic_payload())
            .unwrap_or_else(|error| panic!("Schwab REST native row JSON: {error}"));
    assert_eq!(native_row["datetime_millis"], 1_704_067_200_000_u64);
    assert_eq!(native_row["open"], "475.00");
    let persisted = binding
        .persisted_segment_receipt(0)
        .unwrap_or_else(|| panic!("missing Schwab physical receipt"));
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

    assert_eq!(preference.receipt().route(), ReadOnlyRoute::UserPreference);
    assert_eq!(preference.accounting().provider_records, 1);
    assert_eq!(
        preference.bootstrap().value().market_data_permission(),
        Some("NP")
    );
    assert!(!format!("{preference:?}").contains("must-not-enter-raw-capture"));

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
    let quote_session = SourceIdentifier::try_from("schwab-rest-quote-session-1")
        .unwrap_or_else(|error| panic!("quote session: {error}"));
    let quote_generation = market_squawk_domain::ConnectionGeneration::new(
        sealed_quote.receipt().token_generation().get(),
    )
    .unwrap_or_else(|error| panic!("quote generation: {error}"));
    let quote_instrument = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("quote instrument: {error}"));
    let quote_venue = VenueId::try_from("schwab-us-equities")
        .unwrap_or_else(|error| panic!("quote venue: {error}"));
    let quote_product = ProviderProduct::new(
        SourceIdentifier::try_from("schwab-rest")
            .unwrap_or_else(|error| panic!("quote product: {error}")),
    );
    let quote_channel = ProviderChannel::new(
        SourceIdentifier::try_from("quotes")
            .unwrap_or_else(|error| panic!("quote channel: {error}")),
    );
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
        ProviderInstrumentId::try_from("schwab:AAPL")
            .unwrap_or_else(|error| panic!("quote provider instrument: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [46; 32]),
    )
    .unwrap_or_else(|error| panic!("quote identity: {error}"));
    let quote_market_data = SchwabRestQuoteMarketDataEvidence::try_new(
        quote_session,
        quote_generation,
        SourceIdentifier::try_from("schwab-rest-quotes")
            .unwrap_or_else(|error| panic!("quote feed: {error}")),
        quote_venue,
        MarketDepth::TopOfBook,
        SchwabRestDelayEvidence::RealTime,
        DataQuality::DirectUnverified,
        quote_product,
        quote_channel,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [47; 32]),
    )
    .unwrap_or_else(|error| panic!("quote market data: {error}"));
    let outcome = sealed_quote
        .into_quote_publication(SchwabRestQuotePublicationRequest::new(vec![
            SchwabRestQuoteRecordRequest::new(
                quote_identity,
                quote_instrument,
                quote_source_identifier,
                quote_provenance,
                TickSize::power_of_ten(3)
                    .unwrap_or_else(|error| panic!("quote tick size: {error}")),
                LotSize::try_from_decimal(rust_decimal::Decimal::ONE)
                    .unwrap_or_else(|error| panic!("quote lot size: {error}")),
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
        [MarketEvent::Quote(_)]
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
    let option_market_data = |channel: &str| {
        SchwabRestOptionMarketDataEvidence::try_new(
            SourceIdentifier::try_from("schwab-rest-options")
                .unwrap_or_else(|error| panic!("option feed: {error}")),
            Some(
                VenueId::try_from("schwab-us-options")
                    .unwrap_or_else(|error| panic!("option venue: {error}")),
            ),
            MarketDepth::TopOfBook,
            SchwabRestDelayEvidence::Unknown,
            ProviderProduct::new(
                SourceIdentifier::try_from("schwab-rest")
                    .unwrap_or_else(|error| panic!("option product: {error}")),
            ),
            ProviderChannel::new(
                SourceIdentifier::try_from(channel)
                    .unwrap_or_else(|error| panic!("option channel: {error}")),
            ),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [54; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [55; 32]),
            EvidenceDigest::new(DigestAlgorithm::Sha256, [56; 32]),
            currency,
        )
        .unwrap_or_else(|error| panic!("option market-data evidence: {error}"))
    };
    let chain_received_at = Timestamp::from_unix_nanos(
        i64::try_from(sealed_chain.receipt().received_at_unix_millis())
            .unwrap_or_else(|error| panic!("chain received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("chain received timestamp overflow")),
    );
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
            option_market_data("chains"),
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
    let expiration_outcome = sealed_expirations
        .into_option_publication(SchwabRestOptionPublicationRequest::new(
            SchwabRestOptionUnderlyingRequest::new(
                option_underlying_identity,
                option_underlying_instrument,
                option_underlying_revision,
            ),
            Vec::new(),
            option_market_data("expiration-chain"),
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
        application_credential,
        replacement_credential.clone(),
    )
    .unwrap_or_else(|error| panic!("guarded application replacement: {error}"));
    let replaced_authority = oauth_authority
        .replace_application_credential(replacement, SchwabOAuthInteraction::Background)
        .await
        .unwrap_or_else(|error| panic!("application credential replacement: {error}"));
    assert!(matches!(
        replaced_authority
            .status()
            .await
            .unwrap_or_else(|error| panic!("replacement authority status: {error}")),
        SchwabOAuthAuthorityStatus::AwaitingAuthorization
    ));

    let retry_authority = ProtectedSchwabOAuthAuthority::try_open(
        temporary.path().join("oauth-retry-authority"),
        SchwabOAuthAuthorityConfiguration::try_new(
            secrets,
            Arc::new(RetryableRefreshOAuthWire::default()),
            replacement_credential,
            SchwabOAuthSecretPolicy::try_new(Duration::from_secs(30), 0)
                .unwrap_or_else(|error| panic!("retry OAuth secret policy: {error}")),
            bounds(),
            token_admission,
            5,
        )
        .unwrap_or_else(|error| panic!("retry OAuth configuration: {error}")),
    )
    .await
    .unwrap_or_else(|error| panic!("retry OAuth authority: {error}"));
    let retry_callback = match OAuthCallback::parse(
        "https://127.0.0.1:8182/?code=retry-code&state=retry-refresh",
        "retry-refresh",
        admission(),
    ) {
        Ok(CallbackOutcome::Authorized(callback)) => callback,
        outcome => panic!("retry OAuth callback: {outcome:?}"),
    };
    let retry_issued_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("retry OAuth issue clock: {error}"))
        .as_secs()
        .checked_sub(26)
        .unwrap_or_else(|| panic!("retry issue clock underflow"));
    retry_authority
        .complete_authorization(
            &retry_callback,
            retry_issued_at,
            SchwabOAuthInteraction::Background,
        )
        .await
        .unwrap_or_else(|error| panic!("retry OAuth completion: {error}"));
    assert!(matches!(
        SchwabAccessTokenSource::acquire(&retry_authority).await,
        Err(TokenAuthorityError::Unavailable)
    ));
    let retained = retry_authority
        .status()
        .await
        .unwrap_or_else(|error| panic!("retained OAuth status: {error}"));
    assert!(matches!(
        retained,
        SchwabOAuthAuthorityStatus::Active(receipt) if receipt.generation().get() == 1
    ));
    let refreshed = SchwabAccessTokenSource::acquire(&retry_authority)
        .await
        .unwrap_or_else(|error| panic!("retryable refresh recovery: {error}"));
    assert_eq!(refreshed.generation().get(), 2);
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
    let subscribed = Bytes::from_static(
        br#"{"response":[{"service":"LEVELONE_EQUITIES","command":"SUBS","requestid":"2","timestamp":1710000000001,"content":{"code":0,"msg":"OK"}}]}"#,
    );
    let market_data: &'static [u8] =
        br#"{"data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","timestamp":1710000000002,"content":[{"key":"AAPL","1":100.125,"2":100.25,"3":2,"4":3}]}]}"#;
    let connector_state = Arc::new(Mutex::new(MockStreamerState {
        connects: 0,
        inbound: Some(VecDeque::from([
            InboundStreamerFrame::Text(login),
            InboundStreamerFrame::Text(subscribed),
            InboundStreamerFrame::Text(Bytes::from_static(market_data)),
        ])),
        sent: Vec::new(),
    }));
    let connector = Arc::new(MockStreamerConnector {
        state: connector_state.clone(),
    });
    let stream_bounds = StreamerTransportBounds::try_new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::ZERO,
        0,
        nonzero(64 * 1024),
        nonzero(65),
        nonzero(64 * 1024),
        Duration::from_millis(1),
    )
    .unwrap_or_else(|error| panic!("stream bounds: {error}"));
    let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
    let coordinates = capture_coordinates();
    let stream_identity = SourceIdentifier::try_from("schwab-streamer-connection-41")
        .unwrap_or_else(|error| panic!("stream identity: {error}"));
    let application_generation = ConnectionGeneration::new(
        NonZeroU64::new(41).unwrap_or_else(|| panic!("application generation must be nonzero")),
    );
    let control_source = Arc::new(MockStreamerControlSource {
        controls: Mutex::new(VecDeque::from([SchwabStreamerConnectionControl::new(
            application_generation,
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
    let cancellation = CancellationToken::new();
    let mut sink = CancellingCaptureSink {
        cancellation: cancellation.clone(),
        microbatches: Vec::new(),
    };
    streamer
        .run(bootstrap.value(), &mut sink, cancellation)
        .await
        .unwrap_or_else(|error| panic!("stream run: {error}"));
    assert_eq!(sink.microbatches.len(), 1);
    let stream_microbatch = sink
        .microbatches
        .pop()
        .unwrap_or_else(|| panic!("missing stream microbatch"));
    assert!(!format!("{stream_microbatch:?}").contains("LEVELONE_EQUITIES"));
    assert_eq!(
        stream_microbatch.receipt().token_generation(),
        AccessTokenGeneration::new(NonZeroU64::MIN)
    );
    assert_eq!(stream_microbatch.receipt().frame_count(), 1);
    assert_eq!(
        stream_microbatch.connection().generation(),
        application_generation
    );
    assert_eq!(stream_microbatch.connection().coordinates(), &coordinates);
    assert_eq!(
        stream_microbatch.connection().stream_identity(),
        &stream_identity
    );
    assert_ne!(stream_microbatch.receipt().content_sha256(), [0; 32]);
    assert_ne!(stream_microbatch.receipt().observation_sha256(), [0; 32]);
    let [frame] = stream_microbatch.frames() else {
        panic!("validated stream microbatch did not retain exactly one frame");
    };
    assert!(!format!("{frame:?}").contains("LEVELONE_EQUITIES"));
    assert_eq!(frame.kind(), RawStreamerFrameKind::Text);
    assert_eq!(frame.generation(), stream_microbatch.receipt().generation());
    assert_eq!(frame.ordinal(), stream_microbatch.receipt().first_ordinal());
    assert_eq!(frame.ordinal(), stream_microbatch.receipt().last_ordinal());
    assert_eq!(frame.payload(), market_data);
    assert_ne!(frame.payload_sha256(), [0; 32]);
    assert_eq!(
        frame.received_at_unix_millis(),
        stream_microbatch.receipt().first_received_at_unix_millis()
    );
    assert_eq!(
        frame.received_at_unix_millis(),
        stream_microbatch.receipt().last_received_at_unix_millis()
    );
    assert!(
        !frame
            .payload()
            .windows(b"mock-access-token".len())
            .any(|window| window == b"mock-access-token")
    );
    let frame_ordinal = frame.ordinal();
    let frame_generation = frame.generation();
    let frame_digest = frame.payload_sha256();
    let frame_received_at_unix_millis = frame.received_at_unix_millis();
    let event_id = Uuid::new_v4();
    let (pending, seal_request) = stream_microbatch
        .into_pending_capture(vec![event_id], bounds())
        .unwrap_or_else(|error| panic!("pending Streamer capture: {error}"));
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path().join("stream-raw-publication"))
        .unwrap_or_else(|error| panic!("Streamer publication paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("Streamer publication store: {error}"));
    let sealed_material = seal_request
        .seal(&store)
        .unwrap_or_else(|error| panic!("Streamer physical seal: {error}"));
    let sealed = pending
        .try_rejoin(sealed_material)
        .unwrap_or_else(|error| panic!("sealed Streamer capture: {error}"));
    assert_eq!(sealed.coordinates(), &coordinates);
    assert_eq!(sealed.stream_identity(), &stream_identity);
    assert_eq!(sealed.frames().len(), 1);
    assert_eq!(sealed.frames()[0].event_id(), event_id);
    assert_eq!(sealed.frames()[0].transport_ordinal(), frame_ordinal);
    assert_eq!(sealed.frames()[0].payload_digest().bytes(), frame_digest);
    assert_eq!(
        sealed.frames()[0].received_at_unix_millis(),
        frame_received_at_unix_millis
    );
    let persisted = sealed.persisted_receipt();
    assert_eq!(persisted.capture().source_id(), coordinates.source_id());
    assert_eq!(persisted.capture().dataset(), coordinates.dataset());
    assert_eq!(persisted.capture().stream_identity(), &stream_identity);
    assert_ne!(persisted.receipt_digest().bytes(), [0; 32]);
    assert_eq!(persisted.capture().frames().len(), 1);
    assert_eq!(
        persisted.capture().frames()[0].event_id(),
        *event_id.as_bytes()
    );
    assert_eq!(persisted.capture().frames()[0].source_sequence(), None);
    let streamer_doctor =
        SchwabStreamerFamilyDoctorInput::try_new(MarketDataService::LevelOneEquities, &sealed)
            .unwrap_or_else(|error| panic!("typed Streamer doctor input: {error}"));
    assert_eq!(streamer_doctor.provider_records(), 1);
    let reopened = store
        .open_verified(persisted.segment())
        .unwrap_or_else(|error| panic!("reopen Streamer physical seal: {error}"));
    let [record] = reopened.records() else {
        panic!("sealed Streamer microbatch must contain one raw frame");
    };
    assert_eq!(record.event_id(), event_id);
    assert_eq!(record.connection_id(), coordinates.connection_id());
    assert_eq!(record.payload(), market_data);
    assert_eq!(record.source_sequence(), None);
    let received_at = Timestamp::from_unix_nanos(
        i64::try_from(frame_received_at_unix_millis)
            .unwrap_or_else(|error| panic!("received milliseconds: {error}"))
            .checked_mul(1_000_000)
            .unwrap_or_else(|| panic!("received timestamp overflow")),
    );
    let source_timestamp = Timestamp::from_unix_nanos(1_710_000_000_002_000_000);
    let instrument_id = InstrumentId::try_from(Uuid::new_v4())
        .unwrap_or_else(|error| panic!("instrument id: {error}"));
    let venue_id =
        VenueId::try_from("schwab-us-equities").unwrap_or_else(|error| panic!("venue: {error}"));
    let provider_product = ProviderProduct::new(
        SourceIdentifier::try_from("schwab-streamer")
            .unwrap_or_else(|error| panic!("provider product: {error}")),
    );
    let provider_channel = ProviderChannel::new(
        SourceIdentifier::try_from("LEVELONE_EQUITIES")
            .unwrap_or_else(|error| panic!("provider channel: {error}")),
    );
    let source_identifier = SourceIdentifier::try_from("AAPL")
        .unwrap_or_else(|error| panic!("source identifier: {error}"));
    let canonical_state = CanonicalStateDigest::new(
        EvidenceDigest::new(DigestAlgorithm::Sha256, [41; 32]),
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
        provider_product.clone(),
        provider_channel.clone(),
        LiveEventClass::Quote,
        source_identifier.clone(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, frame_digest),
        canonical_state,
        None,
    )
    .unwrap_or_else(|error| panic!("live binding: {error}"));
    let provenance = LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        live_binding,
        Some(source_timestamp),
        received_at,
        received_at,
        received_at,
        DataQuality::DirectUnverified,
        CoverageStatus::Unknown,
        PayloadReference::ContentHash(PayloadHash::new(DigestAlgorithm::Sha256, frame_digest)),
    ))
    .unwrap_or_else(|error| panic!("quote provenance: {error}"));
    let dictionary = SchwabStreamerFieldDictionary::try_new(
        MarketDataService::LevelOneEquities,
        SourceIdentifier::try_from("schwab-streamer-fields-level-one-equities-v1")
            .unwrap_or_else(|error| panic!("dictionary version: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [42; 32]),
        vec![
            (1, SchwabStreamerSemanticField::BidPrice),
            (2, SchwabStreamerSemanticField::AskPrice),
            (3, SchwabStreamerSemanticField::BidSize),
            (4, SchwabStreamerSemanticField::AskSize),
        ],
    )
    .unwrap_or_else(|error| panic!("dictionary: {error}"));
    let identity = SchwabResolvedProviderIdentity::try_new(
        ProviderIdentifier::try_new("AAPL")
            .unwrap_or_else(|error| panic!("provider symbol: {error}")),
        ProviderInstrumentId::try_from("schwab:AAPL")
            .unwrap_or_else(|error| panic!("provider instrument: {error}")),
        EvidenceDigest::new(DigestAlgorithm::Sha256, [43; 32]),
    )
    .unwrap_or_else(|error| panic!("resolved identity: {error}"));
    let market_evidence = SchwabStreamerQuoteMarketDataEvidence::try_new(
        MarketDataService::LevelOneEquities,
        SourceIdentifier::try_from("schwab-level-one-equities")
            .unwrap_or_else(|error| panic!("feed: {error}")),
        venue_id,
        MarketDepth::TopOfBook,
        SchwabStreamerDelayEvidence::Unknown,
        DataQuality::DirectUnverified,
        provider_product,
        provider_channel,
        EvidenceDigest::new(DigestAlgorithm::Sha256, [44; 32]),
    )
    .unwrap_or_else(|error| panic!("market evidence: {error}"));
    let outcome = sealed
        .into_level_one_quote_publication(SchwabStreamerQuotePublicationRequest::new(vec![
            SchwabStreamerQuoteRecordRequest::new(
                0,
                0,
                0,
                dictionary,
                identity,
                instrument_id,
                source_identifier,
                provenance,
                TickSize::power_of_ten(3).unwrap_or_else(|error| panic!("tick size: {error}")),
                LotSize::try_from_decimal(rust_decimal::Decimal::ONE)
                    .unwrap_or_else(|error| panic!("lot size: {error}")),
                market_evidence,
            ),
        ]))
        .unwrap_or_else(|error| panic!("typed Streamer publication: {error}"));
    let SchwabStreamerQuotePublicationOutcome::Published(publication) = outcome else {
        panic!("complete Level-One quote should publish a typed event batch");
    };
    assert!(publication.dispositions().is_empty());
    assert_eq!(publication.binding().record_count(), 1);
    assert!(matches!(
        publication.binding().batch().events(),
        [MarketEvent::Quote(_)]
    ));
    assert_eq!(
        publication.binding().row_frames()[0].event_frame_ordinal(),
        0
    );
    let native_row = &publication.binding().native_lineage().rows()[0];
    assert!(
        native_row
            .windows(b"100.125".len())
            .any(|value| value == b"100.125")
    );
    assert!(
        native_row
            .windows(b"field_id".len())
            .any(|value| value == b"field_id")
    );
    let state = connector_state
        .lock()
        .unwrap_or_else(|error| panic!("mock connector state: {error}"));
    assert_eq!(
        state.sent.as_slice(),
        [
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
    match execute_fixture(request, body, token, token_admission).await {
        RestExecutionOutcome::Accepted(response) => response,
        other => panic!("unexpected market REST outcome: {other:?}"),
    }
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

async fn execute_user_preference_fixture(
    request: &crate::ReadOnlyRequest,
    body: &'static [u8],
    token: &TransientAccessToken,
    token_admission: AccessTokenAdmission,
) -> crate::SchwabUserPreferenceEvidence {
    match execute_fixture(request, body, token, token_admission).await {
        RestExecutionOutcome::AcceptedUserPreference(response) => response,
        other => panic!("unexpected User Preference outcome: {other:?}"),
    }
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
    executor
        .execute(request, token, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("REST execution: {error}"))
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
    inbound: Option<VecDeque<InboundStreamerFrame>>,
    sent: Vec<(String, String)>,
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
                state.inbound.take().ok_or(SchwabTransportError::Protocol)?
            };
            Ok(Box::new(MockStreamerConnection {
                state: self.state.clone(),
                inbound,
            }) as Box<dyn SchwabStreamerConnection>)
        })
    }
}

struct MockStreamerConnection {
    state: Arc<Mutex<MockStreamerState>>,
    inbound: VecDeque<InboundStreamerFrame>,
}

impl fmt::Debug for MockStreamerConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MockStreamerConnection(..)")
    }
}

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
                Some(frame) => Ok(Some(frame)),
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
    microbatches: Vec<StreamerMicrobatch>,
}

impl StreamerCaptureSink for CancellingCaptureSink {
    fn try_publish(
        &mut self,
        microbatch: StreamerMicrobatch,
    ) -> Result<(), StreamerCaptureSinkError> {
        self.microbatches.push(microbatch);
        self.cancellation.cancel();
        Ok(())
    }
}

fn mock_token(admission: AccessTokenAdmission) -> TransientAccessToken {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|error| panic!("system clock: {error}"))
        .as_secs();
    TransientAccessToken::try_new(
        "mock-access-token".to_owned(),
        AccessTokenGeneration::new(NonZeroU64::MIN),
        now,
        now.checked_add(1_800)
            .unwrap_or_else(|| panic!("token expiry overflow")),
        admission,
    )
    .unwrap_or_else(|error| panic!("mock token: {error}"))
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
