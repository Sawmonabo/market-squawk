use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, pending};
use std::num::{NonZeroU64, NonZeroUsize};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ACCESS_TOKEN_MAX_LIFETIME_SECONDS, AccessTokenAdmission, AccessTokenGeneration,
    CallbackOutcome, ChainRequest, ConnectionGeneration, ConnectionState, DesiredStateController,
    HttpMethod, InboundStreamerFrame, MarketDataService, OAuthCallback, ParseBounds,
    ProviderIdentifier, QuoteRequest, ReadOnlyRoute, RefreshTokenGeneration, RequestAdmission,
    ResponseHeaderEvidence, RestExecutionOutcome, RestTransportBounds, SchwabAccessTokenSource,
    SchwabAdapterError, SchwabCanonicalError, SchwabCaptureCoordinates, SchwabDataUsePurpose,
    SchwabHttpWire, SchwabHttpWireRequest, SchwabHttpWireResponse, SchwabOptionCandidateOutcome,
    SchwabOwnerUseAuthorization, SchwabRestExecutor, SchwabStreamerConnection,
    SchwabStreamerConnector, SchwabStreamerExecutor, SchwabStreamerFieldDictionary,
    SchwabStreamerSemanticField, SchwabTransportError, SchwabTransportTelemetry, StreamerAdmission,
    StreamerCaptureSink, StreamerCaptureSinkError, StreamerMicrobatch, StreamerResponseCode,
    StreamerSubscription, StreamerTransportBounds, TokenAuthorityError, TokenDecision,
    TransientAccessToken, canonicalize_option_chain, canonicalize_streamer_batch,
    parse_option_chain_response, parse_quote_response, parse_streamer_frame, parse_token_response,
    parse_user_preference,
};

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
      "callExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"CALL","symbol":"SPY C","bid":1.2,"ask":1.3,"delta":0.51,"openInterest":10}]}},
      "putExpDateMap":{"2026-08-21:10":{"500.0":[{"putCall":"PUT","symbol":"SPY P","bid":1.1,"ask":1.4,"delta":-0.49,"openInterest":11}]}}
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

    let use_authorization = SchwabOwnerUseAuthorization::OWNER_PRIVATE_RESEARCH;
    assert!(use_authorization.private_retrieval());
    assert!(use_authorization.persistence());
    assert!(use_authorization.backtesting());
    assert!(use_authorization.forecasting());
    assert!(use_authorization.model_training_and_operation());
    assert!(!use_authorization.sale());
    assert!(!use_authorization.redistribution());
    assert!(use_authorization.allows(SchwabDataUsePurpose::Backtesting));
    assert!(!use_authorization.allows(SchwabDataUsePurpose::Sale));
    assert!(!use_authorization.allows(SchwabDataUsePurpose::Redistribution));
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
    let rest_wire = Arc::new(MockHttpWire::new(response));
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
    let token_source = Arc::new(MockTokenSource { token_admission });
    let stream_admission = StreamerAdmission::new(admission(), nonzero(4), nonzero(16));
    let mut streamer = SchwabStreamerExecutor::try_new(
        connector,
        token_source,
        stream_admission,
        stream_bounds,
        bounds(),
        token_admission,
        telemetry.clone(),
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
    let exit = streamer
        .run(bootstrap.value(), &mut sink, cancellation)
        .await
        .unwrap_or_else(|error| panic!("stream run: {error}"));
    assert_eq!(exit, crate::StreamerRunExit::Cancelled);
    assert_eq!(sink.microbatches.len(), 1);
    assert_eq!(sink.microbatches[0].frames().len(), 1);
    assert_eq!(sink.microbatches[0].frames()[0].payload(), &market_data);
    assert!(
        !sink.microbatches[0].frames()[0]
            .payload()
            .windows(b"mock-access-token".len())
            .any(|window| window == b"mock-access-token")
    );
    let stream_material = sink
        .microbatches
        .pop()
        .unwrap_or_else(|| panic!("missing stream microbatch"))
        .try_into_provider_capture_material(coordinates, vec![Uuid::new_v4()])
        .unwrap_or_else(|error| panic!("stream capture material: {error}"));
    assert_eq!(stream_material.records().len(), 1);
    assert_eq!(stream_material.records()[0].payload(), market_data.as_ref());
    let state = connector_state
        .lock()
        .unwrap_or_else(|error| panic!("mock connector state: {error}"));
    assert_eq!(state.connects, 1);
    assert_eq!(state.sent.len(), 2);
    assert_eq!(state.sent[0], ("ADMIN".to_owned(), "LOGIN".to_owned()));
    assert_eq!(
        state.sent[1],
        ("LEVELONE_EQUITIES".to_owned(), "SUBS".to_owned())
    );
    let measured = telemetry
        .snapshot()
        .unwrap_or_else(|error| panic!("telemetry: {error}"));
    assert_eq!(measured.rest_requests_total, 1);
    assert_eq!(measured.streamer_connections_total, 1);
    assert_eq!(measured.streamer_frames_total, 3);
    assert_eq!(measured.streamer_frames_captured_total, 1);
    assert_eq!(measured.streamer_events_total, 1);
}

#[derive(Debug)]
struct MockHttpWire {
    response: SchwabHttpWireResponse,
    calls: Mutex<u64>,
}

impl MockHttpWire {
    fn new(response: SchwabHttpWireResponse) -> Self {
        Self {
            response,
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
            assert_eq!(request.request().route(), ReadOnlyRoute::Quotes);
            let mut calls = self
                .calls
                .lock()
                .map_err(|_| SchwabTransportError::Protocol)?;
            *calls = calls.checked_add(1).ok_or(SchwabTransportError::Overflow)?;
            Ok(self.response.clone())
        })
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
