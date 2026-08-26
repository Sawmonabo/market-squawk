use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, pending};
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{
    BarTimeSemantics, BarTimestampBasis, Currency, DigestAlgorithm, EvidenceDigest, InstrumentId,
    MarketBarAdjustment, MarketBarSessionEvidence, MarketBarSessionKind, MetadataRevision,
    ProviderInstrumentId, RevisionNumber, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_platform::{
    EncryptedFileSecretStore, LocalPaths, SecretCancellation, SecretGeneration,
    SecretInteractionPolicy, SecretKey, SecretOperationControl, SecretStore, SecretValue,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenAdmission, AccessTokenGeneration,
    CallbackOutcome, ChainRequest, ConnectionGeneration, ConnectionState, DesiredStateController,
    HttpMethod, InboundStreamerFrame, MarketDataService, OAuthCallback, ParseBounds,
    PriceHistoryFrequency, PriceHistoryFrequencyType, PriceHistoryRequest,
    ProtectedSchwabOAuthAuthority, ProviderIdentifier, QuoteRequest, ReadOnlyRoute,
    RefreshTokenGeneration, RequestAdmission, ResponseHeaderEvidence, RestExecutionOutcome,
    RestTransportBounds, SchwabAccessTokenSource, SchwabAdapterError, SchwabCanonicalError,
    SchwabCaptureCoordinates, SchwabDailyPriceHistoryPublicationRequest, SchwabHttpWire,
    SchwabHttpWireRequest, SchwabHttpWireResponse, SchwabOAuthAuthorityConfiguration,
    SchwabOAuthInteraction, SchwabOAuthSecretPolicy, SchwabOAuthWire, SchwabOAuthWireError,
    SchwabOAuthWireRequest, SchwabOAuthWireResponse, SchwabOptionCandidateAbstention,
    SchwabOptionCandidateOutcome, SchwabPriceHistoryCapabilityObservation,
    SchwabResolvedProviderIdentity, SchwabRestExecutor, SchwabSealedPriceHistoryCapture,
    SchwabSealedStreamerMicrobatchCapture, SchwabStreamerConnection, SchwabStreamerConnector,
    SchwabStreamerExecutor, SchwabStreamerFieldDictionary, SchwabStreamerSemanticField,
    SchwabTransportError, SchwabTransportTelemetry, StreamerAdmission, StreamerCaptureSink,
    StreamerCaptureSinkError, StreamerMicrobatch, StreamerResponseCode, StreamerSubscription,
    StreamerTransportBounds, TokenAuthorityError, TokenDecision, TransientAccessToken,
    canonicalize_option_chain, canonicalize_price_history, canonicalize_streamer_batch,
    parse_option_chain_response, parse_quote_response, parse_streamer_frame, parse_token_response,
    parse_user_preference,
};

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
fn option_contract_missing_required_greek_is_not_counted_complete() {
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

    assert!(matches!(
        outcomes.as_slice(),
        [SchwabOptionCandidateOutcome::Abstained {
            reason: SchwabOptionCandidateAbstention::MissingRequiredGreek(
                crate::OptionContractField::Rho
            ),
            ..
        }]
    ));
}

#[tokio::test]
async fn daily_price_history_seals_exact_lineage_and_rejects_changed_bootstrap() {
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
        secrets,
        Arc::new(ShortLivedOAuthWire),
        application_credential,
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
    let preference = execute_fixture(
        &preference_request,
        br#"{
          "streamerInfo":[{"streamerSocketUrl":"wss://streamer.example.test/ws","schwabClientCustomerId":"customer","schwabClientCorrelId":"correlation","schwabClientChannel":"channel","schwabClientFunctionId":"function"}],
          "offers":[{"mktDataPermission":"NP"}]
        }"#,
        &token,
        token_admission,
    )
    .await;
    let changed_preference = execute_fixture(
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
    let history = execute_fixture(
        &history_request,
        br#"{
          "symbol":"SPY","empty":false,
          "candles":[{"open":475.00,"high":477.00,"low":474.50,"close":476.25,"volume":1000,"datetime":1704067200000}]
        }"#,
        &token,
        token_admission,
    )
    .await;
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

    let paths = LocalPaths::prepare(temporary.path().join("capture"))
        .unwrap_or_else(|error| panic!("temporary paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("sealed research store: {error}"));
    let registered_coordinates = capture_coordinates();
    let changed_coordinates = SchwabCaptureCoordinates::try_new(
        registered_coordinates.source_id().clone(),
        registered_coordinates.metadata_revision().clone(),
        SourceIdentifier::try_from("schwab-changed-dataset")
            .unwrap_or_else(|error| panic!("changed dataset: {error}")),
        Uuid::new_v4(),
    )
    .unwrap_or_else(|error| panic!("changed capture coordinates: {error}"));
    let changed_connection_coordinates = SchwabCaptureCoordinates::try_new(
        registered_coordinates.source_id().clone(),
        registered_coordinates.metadata_revision().clone(),
        registered_coordinates.dataset().clone(),
        Uuid::new_v4(),
    )
    .unwrap_or_else(|error| panic!("changed connection coordinates: {error}"));
    let seal_history = |coordinates| {
        SchwabSealedPriceHistoryCapture::try_seal(&history, coordinates, Uuid::new_v4(), &store)
            .unwrap_or_else(|error| panic!("sealed history capture: {error}"))
    };
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
    let published_at = Timestamp::from_unix_nanos(
        i64::try_from(observed_at + 1).unwrap_or_else(|error| panic!("publication clock: {error}"))
            * 1_000_000_000,
    );
    let expired_publication_at = Timestamp::from_unix_nanos(
        i64::try_from(oauth_receipt.access_expires_at_unix_seconds())
            .unwrap_or_else(|error| panic!("OAuth expiry clock: {error}"))
            * 1_000_000_000,
    );
    let request_for = |user_preference, sealed_capture, publication_at| {
        SchwabDailyPriceHistoryPublicationRequest {
            capability,
            oauth_authority: oauth_receipt,
            user_preference,
            response: &history,
            sealed_capture,
            capture_coordinates: registered_coordinates.clone(),
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
            published_at: publication_at,
            revision: RevisionNumber::new(1).unwrap_or_else(|error| panic!("revision: {error}")),
        }
    };

    assert_eq!(
        canonicalize_price_history(request_for(
            &changed_preference,
            seal_history(registered_coordinates.clone()),
            published_at,
        )),
        Err(SchwabCanonicalError::PublicationBinding)
    );
    assert_eq!(
        canonicalize_price_history(request_for(
            &preference,
            seal_history(changed_coordinates),
            published_at,
        )),
        Err(SchwabCanonicalError::PublicationBinding)
    );
    assert_eq!(
        canonicalize_price_history(request_for(
            &preference,
            seal_history(changed_connection_coordinates),
            published_at,
        )),
        Err(SchwabCanonicalError::PublicationBinding)
    );
    assert_eq!(
        canonicalize_price_history(request_for(
            &preference,
            seal_history(registered_coordinates.clone()),
            expired_publication_at,
        )),
        Err(SchwabCanonicalError::PublicationBinding)
    );
    let publication = canonicalize_price_history(request_for(
        &preference,
        seal_history(registered_coordinates.clone()),
        published_at,
    ))
    .unwrap_or_else(|error| panic!("history publication: {error}"));
    assert_eq!(publication.bars().len(), 1);
    assert_ne!(publication.canonical_digest().bytes(), [0; 32]);
    let lineage = publication.lineage();
    assert_eq!(lineage.capability, capability);
    assert_eq!(lineage.oauth_authority, oauth_receipt);
    assert_eq!(lineage.capture_coordinates, registered_coordinates);
    assert_eq!(
        lineage.user_preference_receipt.route(),
        ReadOnlyRoute::UserPreference
    );
    assert_ne!(lineage.user_preference_observation_sha256, [0; 32]);
    assert_eq!(
        lineage.instrument_revision_digest,
        instrument_revision_digest
    );
    assert_eq!(lineage.admitted_plan_digest, admitted_plan_digest);
    assert_eq!(lineage.provider_symbol.as_str(), "SPY");
    assert_eq!(lineage.resolution_evidence.bytes(), [22; 32]);
    assert_eq!(lineage.time_semantics.as_ref(), [time_semantics]);
    assert_eq!(lineage.ingested_at, received_at);
}

#[tokio::test]
async fn bounded_mock_transport_captures_exact_market_data_without_token_material() {
    let quote_request = QuoteRequest::try_new(
        vec![ProviderIdentifier::try_new("AAPL").unwrap_or_else(|error| panic!("symbol: {error}"))],
        Vec::new(),
        None,
        admission(),
    )
    .unwrap_or_else(|error| panic!("quote request: {error}"));
    let quote_body = Bytes::from_static(
        br#"{"AAPL":{"assetMainType":"EQUITY","realtime":true,"quote":{"bidPrice":100.125,"askPrice":100.25,"quoteTime":1710000000000}}}"#,
    );
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
        quote_request.request().url().to_owned(),
        Some(
            u64::try_from(quote_body.len()).unwrap_or_else(|error| panic!("body length: {error}")),
        ),
        vec![
            ResponseHeaderEvidence::try_new(
                "content-type".to_owned(),
                b"application/json".to_vec(),
            )
            .unwrap_or_else(|error| panic!("header evidence: {error}")),
        ],
        quote_body.clone(),
        rest_bounds,
    )
    .unwrap_or_else(|error| panic!("mock response: {error}"));
    let rest_wire = Arc::new(MockHttpWire::new(response, ReadOnlyRoute::Quotes));
    let telemetry = SchwabTransportTelemetry::default();
    let token_admission = AccessTokenAdmission::new(nonzero(4 * 1024), Duration::from_secs(1));
    let rest = SchwabRestExecutor::try_new(
        rest_wire.clone(),
        rest_bounds,
        bounds(),
        token_admission,
        telemetry.clone(),
    )
    .unwrap_or_else(|error| panic!("REST executor: {error}"));
    let token = mock_token(token_admission);
    let outcome = rest
        .execute(quote_request.request(), &token, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("REST execution: {error}"));
    let accepted = match outcome {
        RestExecutionOutcome::Accepted(value) => value,
        other => panic!("unexpected REST outcome: {other:?}"),
    };
    assert_eq!(accepted.capture().body(), &quote_body);
    assert_eq!(accepted.accounting().requested, 1);
    assert_eq!(accepted.accounting().returned, 1);
    assert_eq!(accepted.accounting().missing, 0);
    assert_eq!(rest_wire.calls(), 1);
    let coordinates = capture_coordinates();
    let rest_material = accepted
        .capture()
        .clone()
        .try_into_provider_capture_material(coordinates.clone(), Uuid::new_v4())
        .unwrap_or_else(|error| panic!("REST capture material: {error}"));
    assert_eq!(rest_material.records().len(), 1);
    assert_eq!(rest_material.records()[0].payload(), quote_body.as_ref());

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
    let market_data = Bytes::from_static(
        br#"{"data":[{"service":"LEVELONE_EQUITIES","command":"SUBS","timestamp":1710000000002,"content":[{"key":"AAPL","1":100.125,"2":100.25}]}]}"#,
    );
    let connector_state = Arc::new(Mutex::new(MockStreamerState {
        connects: 0,
        inbound: Some(VecDeque::from([
            InboundStreamerFrame::Text(login),
            InboundStreamerFrame::Text(subscribed),
            InboundStreamerFrame::Text(market_data.clone()),
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
        nonzero(8),
        nonzero(64 * 1024),
        Duration::from_millis(1),
    )
    .unwrap_or_else(|error| panic!("stream bounds: {error}"));
    let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
    let mut streamer = SchwabStreamerExecutor::try_new(
        connector,
        Arc::new(MockTokenSource { token_admission }),
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
                vec![0, 1, 2],
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
    let current_request_identity = stream_microbatch
        .receipt()
        .request_set_identity_for_token_generation(stream_microbatch.receipt().token_generation());
    let next_token_generation = AccessTokenGeneration::new(
        NonZeroU64::new(2).unwrap_or_else(|| panic!("token generation must be nonzero")),
    );
    let next_request_identity = stream_microbatch
        .receipt()
        .request_set_identity_for_token_generation(next_token_generation);
    assert_ne!(current_request_identity, next_request_identity);
    let event_id = Uuid::new_v4();
    let temporary = TemporaryDirectory::new();
    let paths = LocalPaths::prepare(temporary.path().join("stream-capture"))
        .unwrap_or_else(|error| panic!("stream capture paths: {error}"));
    let store = paths
        .sealed_research_journal_store()
        .unwrap_or_else(|error| panic!("stream capture store: {error}"));
    let sealed = SchwabSealedStreamerMicrobatchCapture::try_seal(
        stream_microbatch,
        coordinates.clone(),
        vec![event_id],
        &store,
    )
    .unwrap_or_else(|error| panic!("sealed stream capture: {error}"));
    let capture = sealed.receipt().capture();
    assert_eq!(capture.request_set_identity(), current_request_identity);
    assert_eq!(capture.source_id(), coordinates.source_id());
    assert_eq!(capture.metadata_revision(), coordinates.metadata_revision());
    assert_eq!(capture.dataset(), coordinates.dataset());
    let reopened = store
        .open_verified(sealed.receipt().segment())
        .unwrap_or_else(|error| panic!("reopened stream capture: {error}"));
    let [record] = reopened.records() else {
        panic!("sealed stream capture did not retain exactly one frame");
    };
    assert_eq!(record.event_id(), event_id);
    assert_eq!(record.connection_id(), coordinates.connection_id());
    assert_eq!(record.source(), coordinates.source_id().as_str());
    assert_eq!(record.source_sequence(), Some(0));
    assert_eq!(record.payload(), market_data.as_ref());
    assert!(
        !record
            .payload()
            .windows(b"mock-access-token".len())
            .any(|window| window == b"mock-access-token")
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
    response: SchwabHttpWireResponse,
    expected_route: ReadOnlyRoute,
    calls: Mutex<u64>,
}

impl MockHttpWire {
    fn new(response: SchwabHttpWireResponse, expected_route: ReadOnlyRoute) -> Self {
        Self {
            response,
            expected_route,
            calls: Mutex::new(0),
        }
    }

    fn calls(&self) -> u64 {
        *self
            .calls
            .lock()
            .unwrap_or_else(|error| panic!("mock HTTP calls: {error}"))
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
            Ok(self.response.clone())
        })
    }
}

async fn execute_fixture(
    request: &crate::ReadOnlyRequest,
    body: &'static [u8],
    token: &TransientAccessToken,
    token_admission: AccessTokenAdmission,
) -> crate::ExecutedRestResponse {
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
    let executor = SchwabRestExecutor::try_new(
        Arc::new(MockHttpWire::new(response, request.route())),
        rest_bounds,
        bounds(),
        token_admission,
        SchwabTransportTelemetry::default(),
    )
    .unwrap_or_else(|error| panic!("REST executor: {error}"));
    match executor
        .execute(request, token, CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("REST execution: {error}"))
    {
        RestExecutionOutcome::Accepted(response) => response,
        other => panic!("unexpected REST outcome: {other:?}"),
    }
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
