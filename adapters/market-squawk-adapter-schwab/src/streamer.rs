use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest as _, Sha256};
use url::Url;
use zeroize::Zeroizing;

use crate::rest::{
    NativeFieldEntry, NativeScalar, ParseContext, ParsedNative, ProviderIdentifier,
    parse_json_payload,
};
use crate::{ParseBounds, RequestAdmission, SchwabAdapterError};

const MAX_BOOTSTRAP_VALUE_BYTES: usize = 4 * 1024;
const MARKET_DATA_PRINCIPAL_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/schwab-market-data-principal/v1\0";

/// Runtime admission for one Streamer generation.
///
/// Every value is a local safety bound, not a provider-published symbol, field, service, command,
/// or frame ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamerAdmission {
    request: RequestAdmission,
    max_services: NonZeroUsize,
    max_fields_per_service: NonZeroUsize,
}

impl StreamerAdmission {
    pub const fn new(
        request: RequestAdmission,
        max_services: NonZeroUsize,
        max_fields_per_service: NonZeroUsize,
    ) -> Self {
        Self {
            request,
            max_services,
            max_fields_per_service,
        }
    }
    pub const fn request(self) -> RequestAdmission {
        self.request
    }
    pub const fn max_services(self) -> usize {
        self.max_services.get()
    }
    pub const fn max_fields_per_service(self) -> usize {
        self.max_fields_per_service.get()
    }
}

/// Minimum read-only Streamer connection coordinates.
pub struct StreamerBootstrap {
    socket_url: Box<str>,
    market_data_principal_sha256: [u8; 32],
    customer_id: Zeroizing<String>,
    correlation_id: Zeroizing<String>,
    channel: Zeroizing<String>,
    function_id: Zeroizing<String>,
    market_data_permission: Option<Box<str>>,
    level_two_permission: Option<bool>,
}

impl StreamerBootstrap {
    pub fn socket_url(&self) -> &str {
        &self.socket_url
    }
    /// Stable, one-way binding for the Schwab market-data principal selected by OAuth.
    ///
    /// The provider customer identifier itself remains zeroizing and unobservable. This digest is
    /// suitable for collision/currentness evidence; it grants no account, order, or trading
    /// authority.
    pub const fn market_data_principal_sha256(&self) -> [u8; 32] {
        self.market_data_principal_sha256
    }
    pub fn market_data_permission(&self) -> Option<&str> {
        self.market_data_permission.as_deref()
    }
    pub const fn level_two_permission(&self) -> Option<bool> {
        self.level_two_permission
    }
}

impl fmt::Debug for StreamerBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerBootstrap")
            .field("socket_url", &self.socket_url)
            .field("coordinates", &"[REDACTED]")
            .field("market_data_permission", &self.market_data_permission)
            .field("level_two_permission", &self.level_two_permission)
            .finish()
    }
}

/// Exactly one selected Streamer bootstrap record.
pub type StreamerBootstrapResponse = ParsedNative<StreamerBootstrap>;

/// Extracts only Streamer coordinates and market-data offer evidence. Account preferences and all
/// unrelated fields are discarded after their paths/digest are retained in unknown diagnostics.
pub fn parse_user_preference(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<StreamerBootstrapResponse, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut root = object(value)?;
    let infos = root
        .remove("streamerInfo")
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let infos = infos
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    if infos.len() != 1 {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    context.take_record()?;
    let mut info = object(infos[0].clone())?;
    let socket_url = required_text(&mut info, "streamerSocketUrl")?;
    validate_socket_url(&socket_url)?;
    let customer_id = required_text(&mut info, "schwabClientCustomerId")?;
    let correlation_id = required_text(&mut info, "schwabClientCorrelId")?;
    let channel = required_text(&mut info, "schwabClientChannel")?;
    let function_id = required_text(&mut info, "schwabClientFunctionId")?;
    validate_bootstrap_value(&customer_id)?;
    validate_bootstrap_value(&correlation_id)?;
    validate_bootstrap_value(&channel)?;
    validate_bootstrap_value(&function_id)?;
    let market_data_principal_sha256 = market_data_principal_digest(&customer_id);
    record_remaining(&info, "$.streamerInfo[0]", &mut context)?;

    let mut market_data_permission = None;
    let mut level_two_permission = None;
    if let Some(offers) = root.remove("offers") {
        let offers = offers
            .as_array()
            .ok_or(SchwabAdapterError::SchemaViolation)?;
        if offers.len() > 1 {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        if let Some(offer) = offers.first() {
            let mut offer = object(offer.clone())?;
            market_data_permission = optional_text(&mut offer, "mktDataPermission")?;
            level_two_permission = optional_bool(&mut offer, "level2Permissions")?;
            record_remaining(&offer, "$.offers[0]", &mut context)?;
        }
    }
    record_remaining(&root, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.trader.user-preference.streamer-bootstrap",
        digest,
        context.finish(),
        StreamerBootstrap {
            socket_url: socket_url.into_boxed_str(),
            market_data_principal_sha256,
            customer_id: Zeroizing::new(customer_id),
            correlation_id: Zeroizing::new(correlation_id),
            channel: Zeroizing::new(channel),
            function_id: Zeroizing::new(function_id),
            market_data_permission: market_data_permission.map(String::into_boxed_str),
            level_two_permission,
        },
    ))
}

fn market_data_principal_digest(customer_id: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(MARKET_DATA_PRINCIPAL_DIGEST_DOMAIN);
    hasher.update((customer_id.len() as u64).to_be_bytes());
    hasher.update(customer_id.as_bytes());
    hasher.finalize().into()
}

/// Selected market-data-only Streamer services. `ACCOUNT_ACTIVITY` is unrepresentable.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MarketDataService {
    LevelOneEquities,
    LevelOneOptions,
    LevelOneFutures,
    LevelOneFuturesOptions,
    LevelOneForex,
    NyseBook,
    NasdaqBook,
    OptionsBook,
    ChartEquity,
    ChartFutures,
    ScreenerEquity,
    ScreenerOption,
}

impl MarketDataService {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LevelOneEquities => "LEVELONE_EQUITIES",
            Self::LevelOneOptions => "LEVELONE_OPTIONS",
            Self::LevelOneFutures => "LEVELONE_FUTURES",
            Self::LevelOneFuturesOptions => "LEVELONE_FUTURES_OPTIONS",
            Self::LevelOneForex => "LEVELONE_FOREX",
            Self::NyseBook => "NYSE_BOOK",
            Self::NasdaqBook => "NASDAQ_BOOK",
            Self::OptionsBook => "OPTIONS_BOOK",
            Self::ChartEquity => "CHART_EQUITY",
            Self::ChartFutures => "CHART_FUTURES",
            Self::ScreenerEquity => "SCREENER_EQUITY",
            Self::ScreenerOption => "SCREENER_OPTION",
        }
    }
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "LEVELONE_EQUITIES" => Self::LevelOneEquities,
            "LEVELONE_OPTIONS" => Self::LevelOneOptions,
            "LEVELONE_FUTURES" => Self::LevelOneFutures,
            "LEVELONE_FUTURES_OPTIONS" => Self::LevelOneFuturesOptions,
            "LEVELONE_FOREX" => Self::LevelOneForex,
            "NYSE_BOOK" => Self::NyseBook,
            "NASDAQ_BOOK" => Self::NasdaqBook,
            "OPTIONS_BOOK" => Self::OptionsBook,
            "CHART_EQUITY" => Self::ChartEquity,
            "CHART_FUTURES" => Self::ChartFutures,
            "SCREENER_EQUITY" => Self::ScreenerEquity,
            "SCREENER_OPTION" => Self::ScreenerOption,
            _ => return None,
        })
    }
}

/// Subscription command semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerCommand {
    Subscribe,
    Add,
    Unsubscribe,
}
impl StreamerCommand {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Subscribe => "SUBS",
            Self::Add => "ADD",
            Self::Unsubscribe => "UNSUBS",
        }
    }
}

/// Runtime-admitted desired subscription for one selected service.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerSubscription {
    service: MarketDataService,
    keys: BTreeSet<ProviderIdentifier>,
    field_ids: BTreeSet<u16>,
}

impl StreamerSubscription {
    pub fn try_new(
        service: MarketDataService,
        keys: Vec<ProviderIdentifier>,
        field_ids: Vec<u16>,
        admission: StreamerAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        let key_count = keys.len();
        let field_count = field_ids.len();
        let keys = keys.into_iter().collect::<BTreeSet<_>>();
        let field_ids = field_ids.into_iter().collect::<BTreeSet<_>>();
        if keys.len() != key_count || field_ids.len() != field_count {
            return Err(SchwabAdapterError::InvalidInput);
        }
        if keys.is_empty()
            || keys.len() > admission.request().max_items()
            || field_ids.is_empty()
            || field_ids.len() > admission.max_fields_per_service()
        {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(Self {
            service,
            keys,
            field_ids,
        })
    }
    pub const fn service(&self) -> MarketDataService {
        self.service
    }
    pub fn keys(&self) -> &BTreeSet<ProviderIdentifier> {
        &self.keys
    }
    pub fn field_ids(&self) -> &BTreeSet<u16> {
        &self.field_ids
    }
}

/// Monotonic socket generation used to scope replay/dedup evidence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectionGeneration(NonZeroU64);
impl ConnectionGeneration {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// One-socket lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionState {
    Disconnected,
    Connecting(ConnectionGeneration),
    AwaitingLogin(ConnectionGeneration),
    Active(ConnectionGeneration),
    Closing(ConnectionGeneration),
}

/// Serialized one-connection desired-state controller.
#[derive(Debug)]
pub struct DesiredStateController {
    state: ConnectionState,
    last_generation: Option<ConnectionGeneration>,
    desired: BTreeMap<MarketDataService, StreamerSubscription>,
    next_request_id: NonZeroU64,
    admission: StreamerAdmission,
}

impl DesiredStateController {
    pub fn new(admission: StreamerAdmission) -> Self {
        Self {
            state: ConnectionState::Disconnected,
            last_generation: None,
            desired: BTreeMap::new(),
            next_request_id: NonZeroU64::MIN,
            admission,
        }
    }
    pub const fn state(&self) -> ConnectionState {
        self.state
    }
    pub fn desired(&self) -> &BTreeMap<MarketDataService, StreamerSubscription> {
        &self.desired
    }
    pub fn begin_connect(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), SchwabAdapterError> {
        if self.state != ConnectionState::Disconnected
            || self.last_generation.is_some_and(|last| generation <= last)
        {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        self.state = ConnectionState::Connecting(generation);
        self.last_generation = Some(generation);
        Ok(())
    }
    pub fn socket_connected(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), SchwabAdapterError> {
        if self.state != ConnectionState::Connecting(generation) {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        self.state = ConnectionState::AwaitingLogin(generation);
        Ok(())
    }
    pub fn login_accepted(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), SchwabAdapterError> {
        if self.state != ConnectionState::AwaitingLogin(generation) {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        self.state = ConnectionState::Active(generation);
        Ok(())
    }
    pub fn disconnected(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), SchwabAdapterError> {
        if !matches!(self.state, ConnectionState::Connecting(current) | ConnectionState::AwaitingLogin(current) | ConnectionState::Active(current) | ConnectionState::Closing(current) if current == generation)
        {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        self.state = ConnectionState::Disconnected;
        Ok(())
    }
    pub fn replace_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<Option<TransientStreamerRequest>, SchwabAdapterError> {
        if !self.desired.contains_key(&subscription.service)
            && self.desired.len() >= self.admission.max_services()
        {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        let request = if matches!(self.state, ConnectionState::Active(_)) {
            Some(self.subscription_request(&subscription, StreamerCommand::Subscribe)?)
        } else {
            None
        };
        self.desired.insert(subscription.service, subscription);
        Ok(request)
    }
    pub fn add_desired(
        &mut self,
        addition: StreamerSubscription,
    ) -> Result<Option<TransientStreamerRequest>, SchwabAdapterError> {
        if !self.desired.contains_key(&addition.service)
            && self.desired.len() >= self.admission.max_services()
        {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        let combined = match self.desired.get(&addition.service) {
            Some(existing) => {
                let mut keys = existing.keys.clone();
                keys.extend(addition.keys.iter().cloned());
                let mut fields = existing.field_ids.clone();
                fields.extend(addition.field_ids.iter().copied());
                StreamerSubscription::try_new(
                    addition.service,
                    keys.into_iter().collect(),
                    fields.into_iter().collect(),
                    self.admission,
                )?
            }
            None => addition.clone(),
        };
        let request = if matches!(self.state, ConnectionState::Active(_)) {
            Some(self.subscription_request(&addition, StreamerCommand::Add)?)
        } else {
            None
        };
        self.desired.insert(combined.service, combined);
        Ok(request)
    }
    pub fn remove_desired(
        &mut self,
        removal: StreamerSubscription,
    ) -> Result<Option<TransientStreamerRequest>, SchwabAdapterError> {
        let existing = self
            .desired
            .get(&removal.service)
            .cloned()
            .ok_or(SchwabAdapterError::InvalidInput)?;
        if !removal.keys.is_subset(&existing.keys) || removal.field_ids != existing.field_ids {
            return Err(SchwabAdapterError::InvalidInput);
        }
        let remaining_keys = existing
            .keys
            .difference(&removal.keys)
            .cloned()
            .collect::<BTreeSet<_>>();
        let request = if matches!(self.state, ConnectionState::Active(_)) {
            Some(self.subscription_request(&removal, StreamerCommand::Unsubscribe)?)
        } else {
            None
        };
        if remaining_keys.is_empty() {
            self.desired.remove(&removal.service);
        } else {
            self.desired.insert(
                removal.service,
                StreamerSubscription {
                    service: removal.service,
                    keys: remaining_keys,
                    field_ids: existing.field_ids.clone(),
                },
            );
        }
        Ok(request)
    }
    pub fn replay_desired(&mut self) -> Result<Vec<TransientStreamerRequest>, SchwabAdapterError> {
        if !matches!(self.state, ConnectionState::Active(_)) {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        let subscriptions = self.desired.values().cloned().collect::<Vec<_>>();
        subscriptions
            .iter()
            .map(|subscription| self.subscription_request(subscription, StreamerCommand::Subscribe))
            .collect()
    }
    pub fn login_request(
        &mut self,
        bootstrap: &StreamerBootstrap,
        access_token: &str,
    ) -> Result<TransientStreamerRequest, SchwabAdapterError> {
        if !matches!(self.state, ConnectionState::AwaitingLogin(_))
            || access_token.is_empty()
            || access_token.len() > MAX_BOOTSTRAP_VALUE_BYTES
        {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        let request_id = self.take_request_id()?;
        TransientStreamerRequest::login(
            bootstrap,
            access_token,
            request_id,
            self.admission.request(),
        )
    }
    fn subscription_request(
        &mut self,
        subscription: &StreamerSubscription,
        command: StreamerCommand,
    ) -> Result<TransientStreamerRequest, SchwabAdapterError> {
        let request_id = self.take_request_id()?;
        TransientStreamerRequest::subscription(
            subscription,
            command,
            request_id,
            self.admission.request(),
        )
    }
    fn take_request_id(&mut self) -> Result<NonZeroU64, SchwabAdapterError> {
        let current = self.next_request_id;
        self.next_request_id = NonZeroU64::new(
            current
                .get()
                .checked_add(1)
                .ok_or(SchwabAdapterError::ArithmeticOverflow)?,
        )
        .ok_or(SchwabAdapterError::ArithmeticOverflow)?;
        Ok(current)
    }
}

/// Secret-bearing Streamer request bytes, zeroized on drop and never serializable.
pub struct TransientStreamerRequest {
    body: Zeroizing<Vec<u8>>,
    service: Option<MarketDataService>,
    command: Box<str>,
    request_id: NonZeroU64,
}
impl TransientStreamerRequest {
    fn login(
        bootstrap: &StreamerBootstrap,
        token: &str,
        request_id: NonZeroU64,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        #[derive(Serialize)]
        struct Parameters<'a> {
            #[serde(rename = "Authorization")]
            authorization: &'a str,
            #[serde(rename = "SchwabClientChannel")]
            channel: &'a str,
            #[serde(rename = "SchwabClientFunctionId")]
            function_id: &'a str,
        }
        let request = WireRequest {
            service: "ADMIN",
            command: "LOGIN",
            requestid: request_id.get().to_string(),
            customer_id: &bootstrap.customer_id,
            correlation_id: &bootstrap.correlation_id,
            parameters: Parameters {
                authorization: token,
                channel: &bootstrap.channel,
                function_id: &bootstrap.function_id,
            },
        };
        Self::encode(None, "LOGIN", request_id, &request, admission)
    }
    fn subscription(
        subscription: &StreamerSubscription,
        command: StreamerCommand,
        request_id: NonZeroU64,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        #[derive(Serialize)]
        struct Parameters {
            keys: String,
            fields: String,
        }
        let keys = subscription
            .keys
            .iter()
            .map(ProviderIdentifier::as_str)
            .collect::<Vec<_>>()
            .join(",");
        let fields = subscription
            .field_ids
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let request = WireMarketRequest {
            service: subscription.service.as_str(),
            command: command.as_str(),
            requestid: request_id.get().to_string(),
            parameters: Parameters { keys, fields },
        };
        Self::encode(
            Some(subscription.service),
            command.as_str(),
            request_id,
            &request,
            admission,
        )
    }
    fn encode<T: Serialize>(
        service: Option<MarketDataService>,
        command: &str,
        request_id: NonZeroU64,
        request: &T,
        admission: RequestAdmission,
    ) -> Result<Self, SchwabAdapterError> {
        let mut body = Zeroizing::new(Vec::new());
        serde_json::to_writer(
            &mut *body,
            &Envelope {
                requests: [request],
            },
        )
        .map_err(|_| SchwabAdapterError::SchemaViolation)?;
        if body.len() > admission.max_request_bytes() {
            return Err(SchwabAdapterError::RequestNotAdmitted);
        }
        Ok(Self {
            body,
            service,
            command: command.to_owned().into_boxed_str(),
            request_id,
        })
    }
    pub fn expose_body(&self) -> &[u8] {
        &self.body
    }
    pub const fn service(&self) -> Option<MarketDataService> {
        self.service
    }
    pub fn command(&self) -> &str {
        &self.command
    }
    pub const fn request_id(&self) -> NonZeroU64 {
        self.request_id
    }
}
impl fmt::Debug for TransientStreamerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransientStreamerRequest")
            .field("service", &self.service)
            .field("command", &self.command)
            .field("request_id", &self.request_id)
            .field("body", &"[REDACTED]")
            .finish()
    }
}

#[derive(Serialize)]
struct Envelope<T> {
    requests: [T; 1],
}
#[derive(Serialize)]
struct WireRequest<'a, T> {
    service: &'static str,
    command: &'static str,
    requestid: String,
    #[serde(rename = "SchwabClientCustomerId")]
    customer_id: &'a str,
    #[serde(rename = "SchwabClientCorrelId")]
    correlation_id: &'a str,
    parameters: T,
}
#[derive(Serialize)]
struct WireMarketRequest<T> {
    service: &'static str,
    command: &'static str,
    requestid: String,
    parameters: T,
}

/// Closed Streamer response code; unknown codes retain their provider value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerResponseCode {
    Success,
    SymbolLimit,
    Other(i64),
}
impl StreamerResponseCode {
    fn new(value: i64) -> Self {
        match value {
            0 => Self::Success,
            19 => Self::SymbolLimit,
            value => Self::Other(value),
        }
    }
}

/// One command response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerResponse {
    pub service: Box<str>,
    pub command: Box<str>,
    pub request_id: Box<str>,
    pub timestamp_millis: Option<u64>,
    pub code: StreamerResponseCode,
    pub message: NativeScalar,
}
/// One numeric field ID and exact value. Semantic mapping waits for the frozen field dictionary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerFieldEvidence {
    pub field_id: u16,
    pub value: StreamerNativeValue,
}
/// Exact bounded Streamer value. Nested numeric fields admit named-book payload structure without
/// converting it into an open canonical map.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamerNativeValue {
    Scalar(NativeScalar),
    Sequence(Box<[StreamerNativeValue]>),
    Fields(Box<[StreamerNestedField]>),
}
/// One nested numeric field used by provider-native book records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerNestedField {
    pub field_id: u16,
    pub value: StreamerNativeValue,
}
/// One Streamer content record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerContent {
    pub key: ProviderIdentifier,
    pub fields: Box<[StreamerFieldEvidence]>,
}
/// One selected-service data batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerDataBatch {
    pub service: MarketDataService,
    pub command: Box<str>,
    pub timestamp_millis: Option<u64>,
    pub content: Box<[StreamerContent]>,
}
/// Provider notification/heartbeat fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerNotification {
    pub fields: Box<[NativeFieldEntry<StreamerNotificationField>]>,
}
/// Closed notification field dictionary.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum StreamerNotificationField {
    Heartbeat,
}
/// One bounded Streamer envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerFrame {
    pub responses: Box<[StreamerResponse]>,
    pub data: Box<[StreamerDataBatch]>,
    pub notifications: Box<[StreamerNotification]>,
}

pub fn parse_streamer_frame(
    bytes: &[u8],
    bounds: ParseBounds,
) -> Result<ParsedNative<StreamerFrame>, SchwabAdapterError> {
    let (value, digest, mut context) = parse_json_payload(bytes, bounds)?;
    let mut root = object(value)?;
    let responses = parse_responses(root.remove("response"), &mut context)?;
    let data = parse_data(root.remove("data"), &mut context)?;
    let notifications = parse_notifications(root.remove("notify"), &mut context)?;
    if responses.is_empty() && data.is_empty() && notifications.is_empty() {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    record_remaining(&root, "$", &mut context)?;
    Ok(ParsedNative::new(
        "schwab.streamer.frame",
        digest,
        context.finish(),
        StreamerFrame {
            responses,
            data,
            notifications,
        },
    ))
}

fn parse_responses(
    value: Option<Value>,
    context: &mut ParseContext,
) -> Result<Box<[StreamerResponse]>, SchwabAdapterError> {
    let Some(value) = value else {
        return Ok(Box::default());
    };
    let values = value
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let mut output = Vec::new();
    for value in values {
        context.take_record()?;
        let mut item = object(value.clone())?;
        let service = required_text(&mut item, "service")?;
        if service != "ADMIN" && MarketDataService::parse(&service).is_none() {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        let command = required_text(&mut item, "command")?;
        if !matches!(
            command.as_str(),
            "LOGIN" | "LOGOUT" | "SUBS" | "ADD" | "UNSUBS"
        ) {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        let request_id = required_text(&mut item, "requestid")?;
        let timestamp_millis = optional_u64(&mut item, "timestamp")?;
        let mut content = object(
            item.remove("content")
                .ok_or(SchwabAdapterError::SchemaViolation)?,
        )?;
        let code = required_i64(&mut content, "code")?;
        let message = NativeScalar::try_from_json(
            content
                .remove("msg")
                .ok_or(SchwabAdapterError::SchemaViolation)?,
        )?;
        record_remaining(&content, "$.response[].content", context)?;
        record_remaining(&item, "$.response[]", context)?;
        output.push(StreamerResponse {
            service: service.into_boxed_str(),
            command: command.into_boxed_str(),
            request_id: request_id.into_boxed_str(),
            timestamp_millis,
            code: StreamerResponseCode::new(code),
            message,
        });
    }
    Ok(output.into_boxed_slice())
}
fn parse_data(
    value: Option<Value>,
    context: &mut ParseContext,
) -> Result<Box<[StreamerDataBatch]>, SchwabAdapterError> {
    let Some(value) = value else {
        return Ok(Box::default());
    };
    let values = value
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let mut batches = Vec::new();
    for value in values {
        context.take_record()?;
        let mut item = object(value.clone())?;
        let service = MarketDataService::parse(&required_text(&mut item, "service")?)
            .ok_or(SchwabAdapterError::SchemaViolation)?;
        let command = required_text(&mut item, "command")?;
        if !matches!(command.as_str(), "SUBS" | "ADD") {
            return Err(SchwabAdapterError::SchemaViolation);
        }
        let timestamp_millis = optional_u64(&mut item, "timestamp")?;
        let contents = item
            .remove("content")
            .ok_or(SchwabAdapterError::SchemaViolation)?;
        let contents = contents
            .as_array()
            .ok_or(SchwabAdapterError::SchemaViolation)?;
        let mut parsed = Vec::new();
        let mut keys = BTreeSet::new();
        for content in contents {
            context.take_record()?;
            let mut content = object(content.clone())?;
            let key = ProviderIdentifier::try_new(required_text(&mut content, "key")?)?;
            if !keys.insert(key.clone()) {
                return Err(SchwabAdapterError::SchemaViolation);
            }
            let mut fields = Vec::new();
            for (field, value) in content {
                let field_id = field
                    .parse::<u16>()
                    .map_err(|_| SchwabAdapterError::SchemaViolation)?;
                fields.push(StreamerFieldEvidence {
                    field_id,
                    value: parse_streamer_value(value)?,
                });
            }
            parsed.push(StreamerContent {
                key,
                fields: fields.into_boxed_slice(),
            });
        }
        record_remaining(&item, "$.data[]", context)?;
        batches.push(StreamerDataBatch {
            service,
            command: command.into_boxed_str(),
            timestamp_millis,
            content: parsed.into_boxed_slice(),
        });
    }
    Ok(batches.into_boxed_slice())
}
fn parse_notifications(
    value: Option<Value>,
    context: &mut ParseContext,
) -> Result<Box<[StreamerNotification]>, SchwabAdapterError> {
    let Some(value) = value else {
        return Ok(Box::default());
    };
    let values = value
        .as_array()
        .ok_or(SchwabAdapterError::SchemaViolation)?;
    let mut output = Vec::new();
    for value in values {
        context.take_record()?;
        let item = object(value.clone())?;
        let mut fields = Vec::new();
        for (key, value) in item {
            if key == "heartbeat" {
                fields.push(NativeFieldEntry::new(
                    StreamerNotificationField::Heartbeat,
                    NativeScalar::try_from_json(value)?,
                ));
            } else {
                context.record_unknown("$.notify[]", &key, &value)?;
            }
        }
        output.push(StreamerNotification {
            fields: fields.into_boxed_slice(),
        });
    }
    Ok(output.into_boxed_slice())
}

fn parse_streamer_value(value: Value) -> Result<StreamerNativeValue, SchwabAdapterError> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            NativeScalar::try_from_json(value).map(StreamerNativeValue::Scalar)
        }
        Value::Array(values) => values
            .into_iter()
            .map(parse_streamer_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Vec::into_boxed_slice)
            .map(StreamerNativeValue::Sequence),
        Value::Object(values) => {
            let mut fields = Vec::new();
            for (field, value) in values {
                let field_id = field
                    .parse::<u16>()
                    .map_err(|_| SchwabAdapterError::SchemaViolation)?;
                fields.push(StreamerNestedField {
                    field_id,
                    value: parse_streamer_value(value)?,
                });
            }
            Ok(StreamerNativeValue::Fields(fields.into_boxed_slice()))
        }
    }
}

fn object(value: Value) -> Result<Map<String, Value>, SchwabAdapterError> {
    value
        .as_object()
        .cloned()
        .ok_or(SchwabAdapterError::SchemaViolation)
}
fn required_text(object: &mut Map<String, Value>, key: &str) -> Result<String, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::String(value))
            if !value.is_empty() && value.len() <= MAX_BOOTSTRAP_VALUE_BYTES =>
        {
            Ok(value)
        }
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn optional_text(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<String>, SchwabAdapterError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value))
            if !value.is_empty() && value.len() <= MAX_BOOTSTRAP_VALUE_BYTES =>
        {
            Ok(Some(value))
        }
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn optional_bool(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<bool>, SchwabAdapterError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(value)),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn optional_u64(
    object: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<u64>, SchwabAdapterError> {
    match object.remove(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => value
            .as_u64()
            .map(Some)
            .ok_or(SchwabAdapterError::SchemaViolation),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn required_i64(object: &mut Map<String, Value>, key: &str) -> Result<i64, SchwabAdapterError> {
    match object.remove(key) {
        Some(Value::Number(value)) => value.as_i64().ok_or(SchwabAdapterError::SchemaViolation),
        _ => Err(SchwabAdapterError::SchemaViolation),
    }
}
fn record_remaining(
    object: &Map<String, Value>,
    path: &str,
    context: &mut ParseContext,
) -> Result<(), SchwabAdapterError> {
    for (key, value) in object {
        context.record_unknown(path, key, value)?;
    }
    Ok(())
}
fn validate_bootstrap_value(value: &str) -> Result<(), SchwabAdapterError> {
    if value.is_empty() || value.len() > MAX_BOOTSTRAP_VALUE_BYTES || value.contains(['\r', '\n']) {
        Err(SchwabAdapterError::SchemaViolation)
    } else {
        Ok(())
    }
}
fn validate_socket_url(value: &str) -> Result<(), SchwabAdapterError> {
    let url = Url::parse(value).map_err(|_| SchwabAdapterError::SchemaViolation)?;
    if url.scheme() != "wss"
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SchwabAdapterError::SchemaViolation);
    }
    Ok(())
}
