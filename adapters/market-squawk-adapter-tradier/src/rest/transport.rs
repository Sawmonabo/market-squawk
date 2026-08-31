use std::sync::Arc;
use std::time::Duration;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use market_squawk_sources::{
    ActiveLiveSourceGeneration, BudgetDispatchDecision, BudgetReservationDecision,
    SharedProviderBudget, SourceError, TransportFrameKind,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderValue,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use url::Url;
use zeroize::Zeroizing;

use crate::TradierRateLimitEvidence;
use crate::config::TradierSourceConfig;
use crate::source::{TradierAccountInner, collect_bounded, rate_limited_error};

use super::{TradierRestError, TradierRestEvidence};

pub(super) async fn fetch_json(
    authority: &mut ActiveLiveSourceGeneration,
    account: &Arc<TradierAccountInner>,
    budget: &SharedProviderBudget,
    config: &TradierSourceConfig,
    url: Url,
    cancellation: CancellationToken,
) -> Result<Arc<TradierRestEvidence>, TradierRestError> {
    if cancellation.is_cancelled() {
        return Err(TradierRestError::Cancelled);
    }
    authority.validate_current()?;
    config
        .metadata()
        .network_policy()
        .authorize(url.as_str())
        .map_err(|_| TradierRestError::NetworkPolicy)?;
    let reservation = match budget.try_reserve_request() {
        BudgetReservationDecision::Ready(reservation) => reservation,
        BudgetReservationDecision::WaitUntil(deadline) => {
            return Err(TradierRestError::Source(SourceError::BudgetWaitUntil {
                deadline,
            }));
        }
        BudgetReservationDecision::Unavailable(reason) => {
            return Err(TradierRestError::Source(SourceError::BudgetUnavailable {
                reason,
            }));
        }
    };
    let maximum = usize::try_from(config.transport_limits().http().max_response_bytes())
        .map_err(|_| TradierRestError::InvalidResponse)?;
    let timeout = Duration::from_nanos(config.transport_limits().http().total_timeout_nanos());
    let request_url = url.as_str().to_owned();
    let operation = async {
        let authorization = Zeroizing::new(format!("Bearer {}", account.token.expose()));
        let mut authorization_header = HeaderValue::try_from(authorization.as_str())
            .map_err(|_| TradierRestError::Source(SourceError::Unauthorized))?;
        authorization_header.set_sensitive(true);
        let mut authorization = authorization;
        authorization.clear();
        let request = account
            .client
            .get(url.clone())
            .header(ACCEPT, "application/json")
            .header(ACCEPT_ENCODING, "identity")
            .header(AUTHORIZATION, authorization_header);
        let permit = match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(deadline) => {
                return Err(TradierRestError::Source(SourceError::BudgetWaitUntil {
                    deadline,
                }));
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(TradierRestError::Source(SourceError::BudgetUnavailable {
                    reason,
                }));
            }
        };
        let response = request
            .send()
            .await
            .map_err(|_| TradierRestError::Source(SourceError::Network));
        let response = response?;
        if response.url() != &url {
            return Err(TradierRestError::InvalidResponse);
        }
        let status = response.status();
        let headers = response.headers().clone();
        if matches!(status.as_u16(), 401 | 403) {
            return Err(TradierRestError::Source(SourceError::Unauthorized));
        }
        let rate = TradierRateLimitEvidence::try_from_headers(&headers).ok();
        if let Some(evidence) = rate {
            account.record_rate_limit(evidence)?;
        }
        if status.as_u16() == 429 || status.is_server_error() {
            return Err(TradierRestError::Source(rate_limited_error(
                budget, &headers, rate,
            )));
        }
        if status.as_u16() != 200 {
            return Err(TradierRestError::Source(SourceError::ProviderUnavailable));
        }
        let rate = rate.ok_or(TradierRestError::InvalidRateLimitEvidence)?;
        let content_type =
            singleton_header(&headers, CONTENT_TYPE)?.ok_or(TradierRestError::InvalidResponse)?;
        if !content_type
            .split(|byte| *byte == b';')
            .next()
            .is_some_and(|value| value.trim_ascii().eq_ignore_ascii_case(b"application/json"))
        {
            return Err(TradierRestError::InvalidResponse);
        }
        if singleton_header(&headers, CONTENT_ENCODING)?
            .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
        {
            return Err(TradierRestError::InvalidResponse);
        }
        if response
            .content_length()
            .is_some_and(|length| usize::try_from(length).map_or(true, |length| length > maximum))
        {
            return Err(TradierRestError::Source(SourceError::FrameTooLarge {
                max: maximum,
            }));
        }
        let body = collect_bounded(response.bytes_stream(), maximum).await?;
        if body.is_empty() {
            return Err(TradierRestError::InvalidResponse);
        }
        authority.validate_current()?;
        budget
            .record_success()
            .map_err(|_| TradierRestError::Source(SourceError::ProviderUnavailable))?;
        permit.release();
        let digest = EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(&body).into());
        let frame = authority
            .frames_mut()?
            .try_frame(TransportFrameKind::Text, body)?;
        authority.validate_current()?;
        Ok(Arc::new(TradierRestEvidence::new(
            frame,
            digest,
            rate,
            request_url,
        )))
    };
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(TradierRestError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| TradierRestError::Source(SourceError::Network))?
        }
    }
}

fn singleton_header(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Result<Option<&[u8]>, TradierRestError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || value.as_bytes().len() > 256 {
        return Err(TradierRestError::InvalidResponse);
    }
    Ok(Some(value.as_bytes()))
}
