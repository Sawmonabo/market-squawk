//! Sealed authenticated bootstrap for the configured Alpaca IEX live symbol set.

use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use bytes::Bytes;
use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{
    ActiveLiveSourceGeneration, ApiEndpointRule, BudgetDecision, HttpRequestBounds, PathScope,
    QueryParameterRule, QuerySensitivity, RawMarketSink, SharedProviderBudget, SourceError,
    TransportFrameKind, apply_http_retry_after,
};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, RETRY_AFTER};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::config::{
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT, ALPACA_STOCKS_SNAPSHOTS_ENDPOINT,
    AlpacaIexBootSnapshotPolicy, AlpacaInstrumentMapping, AlpacaTransportLimits,
};
use crate::historical_calendar::{
    authenticated_bounded_get, hardened_client, singleton_bounded_header,
};
use crate::{AlpacaCredentials, AlpacaError};

const SNAPSHOTS_PATH: &str = "/v2/stocks/snapshots";
const MAX_SYMBOL_QUERY_BYTES: usize =
    ALPACA_BASIC_EQUITY_SYMBOL_LIMIT * 32 + ALPACA_BASIC_EQUITY_SYMBOL_LIMIT.saturating_sub(1);
const MAX_ENCODED_QUERY_BYTES: u16 = 2_048;
const MAX_HEADER_BYTES: usize = 128;
const USER_AGENT: &str = "market-squawk/0.1 alpaca-iex-live-bootstrap";

/// Code-owned request coordinates for one configured IEX snapshot bootstrap.
#[derive(Clone, Debug)]
pub(crate) struct AlpacaIexBootSnapshotContract {
    target: Box<str>,
    url: Url,
    request_bounds: HttpRequestBounds,
    maximum_body_bytes: usize,
    endpoint_rule: ApiEndpointRule,
}

impl AlpacaIexBootSnapshotContract {
    pub(crate) fn try_new(
        mappings: &[AlpacaInstrumentMapping],
        limits: AlpacaTransportLimits,
    ) -> Result<Self, AlpacaError> {
        if mappings.is_empty() || mappings.len() > ALPACA_BASIC_EQUITY_SYMBOL_LIMIT {
            return Err(AlpacaError::InvalidCoverage);
        }
        let symbol_bytes = mappings
            .iter()
            .map(|mapping| mapping.symbol().len())
            .try_fold(mappings.len().saturating_sub(1), |total, bytes| {
                total.checked_add(bytes)
            })
            .filter(|bytes| *bytes != 0 && *bytes <= MAX_SYMBOL_QUERY_BYTES)
            .ok_or(AlpacaError::InvalidCoverage)?;
        let mut symbols = String::new();
        symbols
            .try_reserve_exact(symbol_bytes)
            .map_err(|_| AlpacaError::Allocation)?;
        for (index, mapping) in mappings.iter().enumerate() {
            if index != 0 {
                symbols.push(',');
            }
            symbols.push_str(mapping.symbol());
        }
        if symbols.len() != symbol_bytes {
            return Err(AlpacaError::InvalidCoverage);
        }
        let target_bytes = ALPACA_STOCKS_SNAPSHOTS_ENDPOINT
            .len()
            .checked_add("?symbols=".len())
            .and_then(|bytes| bytes.checked_add(symbol_bytes))
            .and_then(|bytes| bytes.checked_add("&feed=iex".len()))
            .ok_or(AlpacaError::Allocation)?;
        let mut target = String::new();
        target
            .try_reserve_exact(target_bytes)
            .map_err(|_| AlpacaError::Allocation)?;
        target.push_str(ALPACA_STOCKS_SNAPSHOTS_ENDPOINT);
        target.push_str("?symbols=");
        target.push_str(&symbols);
        target.push_str("&feed=iex");
        let url = Url::parse(&target).map_err(|_| AlpacaError::Protocol)?;
        let mut query = url.query_pairs();
        let exact_query = query
            .next()
            .is_some_and(|(key, value)| key.as_ref() == "symbols" && value.as_ref() == symbols)
            && query
                .next()
                .is_some_and(|(key, value)| key.as_ref() == "feed" && value.as_ref() == "iex")
            && query.next().is_none();
        if url.scheme() != "https"
            || url.host_str() != Some("data.alpaca.markets")
            || url.path() != SNAPSHOTS_PATH
            || url.as_str() != target
            || url.fragment().is_some()
            || !exact_query
        {
            return Err(AlpacaError::Protocol);
        }
        let maximum_body_bytes = limits.max_frame_bytes();
        let request_bounds = request_bounds(limits, maximum_body_bytes)?;
        let symbols_bound = u16::try_from(symbol_bytes).map_err(|_| AlpacaError::Protocol)?;
        let endpoint_rule = ApiEndpointRule::try_new(
            ALPACA_STOCKS_SNAPSHOTS_ENDPOINT,
            PathScope::Exact,
            vec![
                QueryParameterRule::try_new(
                    SourceIdentifier::try_from("symbols")?,
                    symbols_bound,
                    false,
                    QuerySensitivity::Public,
                )?,
                QueryParameterRule::try_new_exact_public(
                    SourceIdentifier::try_from("feed")?,
                    SourceIdentifier::try_from("iex")?,
                )?,
            ],
            2,
            MAX_ENCODED_QUERY_BYTES,
        )?;
        Ok(Self {
            target: target.into_boxed_str(),
            url,
            request_bounds,
            maximum_body_bytes,
            endpoint_rule,
        })
    }

    pub(crate) const fn target(&self) -> &str {
        &self.target
    }

    pub(crate) const fn request_bounds(&self) -> HttpRequestBounds {
        self.request_bounds
    }

    pub(crate) const fn maximum_body_bytes(&self) -> usize {
        self.maximum_body_bytes
    }

    pub(crate) const fn endpoint_rule(&self) -> &ApiEndpointRule {
        &self.endpoint_rule
    }
}

/// Hardened executor for the sole code-owned IEX bootstrap request.
#[derive(Debug)]
pub(crate) struct AlpacaIexBootSnapshotTransport {
    contract: AlpacaIexBootSnapshotContract,
    client: reqwest::Client,
}

impl AlpacaIexBootSnapshotTransport {
    pub(crate) fn try_new(contract: &AlpacaIexBootSnapshotContract) -> Result<Self, AlpacaError> {
        Ok(Self {
            contract: contract.clone(),
            client: hardened_client(contract.request_bounds(), USER_AGENT)?,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exact generation, budget, sink, credentials, and cancellation stay explicit"
    )]
    pub(crate) async fn acquire_and_publish(
        &self,
        metadata: &market_squawk_sources::SourceMetadata,
        credentials: &AlpacaCredentials,
        authority: &mut ActiveLiveSourceGeneration,
        budget: &SharedProviderBudget,
        sink: &mut dyn RawMarketSink,
        cancellation: &CancellationToken,
    ) -> Result<(), SourceError> {
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        metadata
            .network_policy()
            .authorize(self.contract.target())
            .map_err(|_| SourceError::InvalidProtocolState)?;
        authority.validate_current()?;
        let permit = match budget.try_acquire() {
            BudgetDecision::Ready(permit) => permit,
            BudgetDecision::WaitUntil(deadline) => {
                return Err(SourceError::BudgetWaitUntil { deadline });
            }
            BudgetDecision::Unavailable(reason) => {
                return Err(SourceError::BudgetUnavailable { reason });
            }
        };
        let transport_deadline = Instant::now()
            .checked_add(Duration::from_nanos(
                self.contract.request_bounds().total_timeout_nanos(),
            ))
            .ok_or(SourceError::InvalidProtocolState)?;
        let sink_deadline = sink.next_deadline();
        let deadline = sink_deadline.map_or(transport_deadline, |deadline| {
            deadline.min(transport_deadline)
        });
        let response = authenticated_bounded_get(
            &self.client,
            credentials,
            &self.contract.url,
            self.contract.request_bounds(),
            self.contract.maximum_body_bytes(),
            deadline,
            cancellation,
        )
        .await
        .map_err(|error| {
            map_transport_error(
                error,
                sink,
                sink_deadline,
                self.contract.maximum_body_bytes(),
            )
        })?;
        validate_rate_headers(&response.headers)?;
        let retry_after =
            singleton_bounded_header(&response.headers, RETRY_AFTER, MAX_HEADER_BYTES)
                .map_err(|_| SourceError::InvalidProtocolState)?;
        if response.status == 429 || response.status >= 500 {
            let refusal = apply_http_retry_after(budget, retry_after.as_deref(), 1_000);
            permit.release();
            return Err(SourceError::from_applied_budget_refusal(refusal));
        }
        if matches!(response.status, 401 | 403) {
            return Err(SourceError::Unauthorized);
        }
        if response.status != 200 || !is_exact_json_content_type(&response.headers)? {
            return Err(SourceError::InvalidProtocolState);
        }
        budget
            .record_success()
            .map_err(|reason| SourceError::BudgetUnavailable { reason })?;
        permit.release();
        if cancellation.is_cancelled() {
            return Err(SourceError::Cancelled);
        }
        ensure_before_deadline(sink, sink_deadline, deadline)?;
        authority.validate_current()?;
        let frame = authority
            .frames_mut()?
            .try_frame(TransportFrameKind::Text, Bytes::from(response.body))?;
        if frame.frame_id().get() != 1 {
            return Err(SourceError::InvalidProtocolState);
        }
        ensure_before_deadline(sink, sink_deadline, deadline)?;
        sink.try_publish(frame)?;
        Ok(())
    }
}

fn request_bounds(
    limits: AlpacaTransportLimits,
    maximum_body_bytes: usize,
) -> Result<HttpRequestBounds, AlpacaError> {
    let policy = AlpacaIexBootSnapshotPolicy::from_transport_limits(limits);
    if policy.maximum_body_bytes() != maximum_body_bytes {
        return Err(AlpacaError::InvalidTransportLimits);
    }
    let connect = nonzero_nanos(limits.connect_timeout())?;
    let read = nonzero_nanos(limits.io_timeout())?;
    let total = nonzero_nanos(policy.total_timeout())?;
    HttpRequestBounds::try_new(
        NonZeroU64::new(connect).ok_or(AlpacaError::InvalidTransportLimits)?,
        NonZeroU64::new(read).ok_or(AlpacaError::InvalidTransportLimits)?,
        NonZeroU64::new(total).ok_or(AlpacaError::InvalidTransportLimits)?,
        0,
        NonZeroU64::new(
            u64::try_from(maximum_body_bytes).map_err(|_| AlpacaError::InvalidTransportLimits)?,
        )
        .ok_or(AlpacaError::InvalidTransportLimits)?,
    )
    .map_err(Into::into)
}

fn nonzero_nanos(duration: Duration) -> Result<u64, AlpacaError> {
    u64::try_from(duration.as_nanos())
        .ok()
        .filter(|value| *value != 0)
        .ok_or(AlpacaError::InvalidTransportLimits)
}

fn map_transport_error(
    error: AlpacaError,
    sink: &mut dyn RawMarketSink,
    sink_deadline: Option<Instant>,
    maximum_body_bytes: usize,
) -> SourceError {
    match error {
        AlpacaError::Cancelled => SourceError::Cancelled,
        AlpacaError::DeadlineExceeded => {
            let now = Instant::now();
            if sink_deadline.is_some_and(|deadline| now >= deadline) {
                sink.poll_deadline(now)
                    .map_or_else(SourceError::from, |()| SourceError::InvalidProtocolState)
            } else {
                SourceError::ConnectionIdle
            }
        }
        AlpacaError::InvalidCredentials | AlpacaError::InvalidAuthorization => {
            SourceError::Unauthorized
        }
        AlpacaError::BodyTooLarge => SourceError::FrameTooLarge {
            max: maximum_body_bytes,
        },
        AlpacaError::Network => SourceError::Network,
        _ => SourceError::InvalidProtocolState,
    }
}

fn validate_rate_headers(headers: &HeaderMap) -> Result<(), SourceError> {
    let limit = optional_u32_header(headers, HeaderName::from_static("x-ratelimit-limit"))?;
    let remaining = optional_u32_header(headers, HeaderName::from_static("x-ratelimit-remaining"))?;
    let reset = optional_i64_header(headers, HeaderName::from_static("x-ratelimit-reset"))?;
    if limit
        .zip(remaining)
        .is_some_and(|(limit, remaining)| remaining > limit)
        || reset.is_some_and(|reset| reset < 0)
    {
        return Err(SourceError::InvalidProtocolState);
    }
    Ok(())
}

fn optional_u32_header(headers: &HeaderMap, name: HeaderName) -> Result<Option<u32>, SourceError> {
    optional_integer_header(headers, name)
}

fn optional_i64_header(headers: &HeaderMap, name: HeaderName) -> Result<Option<i64>, SourceError> {
    optional_integer_header(headers, name)
}

fn optional_integer_header<T>(
    headers: &HeaderMap,
    name: HeaderName,
) -> Result<Option<T>, SourceError>
where
    T: std::str::FromStr,
{
    singleton_bounded_header(headers, name, MAX_HEADER_BYTES)
        .map_err(|_| SourceError::InvalidProtocolState)?
        .map(|value| {
            std::str::from_utf8(&value)
                .ok()
                .and_then(|value| value.parse::<T>().ok())
                .ok_or(SourceError::InvalidProtocolState)
        })
        .transpose()
}

fn is_exact_json_content_type(headers: &HeaderMap) -> Result<bool, SourceError> {
    singleton_bounded_header(headers, CONTENT_TYPE, MAX_HEADER_BYTES)
        .map_err(|_| SourceError::InvalidProtocolState)
        .map(|value| value.is_some_and(|value| value.eq_ignore_ascii_case(b"application/json")))
}

fn ensure_before_deadline(
    sink: &mut dyn RawMarketSink,
    sink_deadline: Option<Instant>,
    effective_deadline: Instant,
) -> Result<(), SourceError> {
    let now = Instant::now();
    if now < effective_deadline {
        return Ok(());
    }
    if sink_deadline.is_some_and(|deadline| now >= deadline) {
        sink.poll_deadline(now)?;
        return Err(SourceError::InvalidProtocolState);
    }
    Err(SourceError::ConnectionIdle)
}
