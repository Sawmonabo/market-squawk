use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use market_squawk_domain::Timestamp;
use market_squawk_platform::{SealedResearchJournalStore, VerifiedResearchObject};
use market_squawk_sources::{
    ExtractionAuthority, ExtractionSourceError, HttpRequestBounds, NetworkAccessPolicy,
    SourceError, SourceMetadata,
};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, ETAG, HeaderMap,
    HeaderName, LAST_MODIFIED, RETRY_AFTER, USER_AGENT,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::archive::{
    NasdaqHttpResponseEvidence, NasdaqReferenceOperationControl, OPTIONS_URL,
    logical_object_admission,
};
use crate::model::NasdaqDirectoryKind;
use crate::source::{
    NASDAQ_LISTED_URL, NasdaqReferenceIngestError, NasdaqReferenceRetryEvidence,
    NasdaqSymbolDirectorySourceError, OTHER_LISTED_URL,
};

const USER_AGENT_VALUE: &str = concat!(
    "market-squawk/",
    env!("CARGO_PKG_VERSION"),
    " nasdaq-symbol-directory-adapter"
);

#[derive(Clone, Debug)]
pub(crate) struct RetrievedDirectory {
    pub(crate) kind: NasdaqDirectoryKind,
    pub(crate) bytes: Bytes,
    pub(crate) received_at: Timestamp,
    pub(crate) last_modified_at: Timestamp,
    pub(crate) sha256_hex: String,
    pub(crate) response_evidence: NasdaqHttpResponseEvidence,
}

/// One exact HTTP body already admitted and verified by the shared logical-object store.
pub(crate) struct RetrievedReferenceObject {
    pub(crate) response_evidence: NasdaqHttpResponseEvidence,
    pub(crate) verified_object: VerifiedResearchObject,
}

impl std::fmt::Debug for RetrievedReferenceObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetrievedReferenceObject")
            .field("response_evidence", &self.response_evidence)
            .field("verified_object", &self.verified_object)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct NasdaqHttpClient {
    client: reqwest::Client,
    max_response_bytes: usize,
    total_timeout: Duration,
}

impl NasdaqHttpClient {
    pub(crate) fn try_new(
        metadata: &SourceMetadata,
    ) -> Result<Self, NasdaqSymbolDirectorySourceError> {
        metadata
            .network_policy()
            .authorize(NASDAQ_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        metadata
            .network_policy()
            .authorize(OTHER_LISTED_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        metadata
            .network_policy()
            .authorize(OPTIONS_URL)
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(NasdaqSymbolDirectorySourceError::InvalidMetadata);
        };
        let bounds = endpoint_policy.request_bounds();
        let max_response_bytes = usize::try_from(bounds.max_response_bytes())
            .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)?;
        let client = build_client(bounds)?;
        Ok(Self {
            client,
            max_response_bytes,
            total_timeout: Duration::from_nanos(bounds.total_timeout_nanos()),
        })
    }

    pub(crate) async fn fetch(
        &self,
        metadata: &SourceMetadata,
        authority: &ExtractionAuthority,
        kind: NasdaqDirectoryKind,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedDirectory, ExtractionSourceError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled);
        }
        let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
        authority.validate_current()?;
        if authority.metadata() != metadata || !metadata.is_effective_at(now) {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let url = endpoint(kind).ok_or(SourceError::InvalidProtocolState)?;
        let max_response_bytes = self.max_response_bytes.min(kind.maximum_source_bytes());
        metadata
            .network_policy()
            .authorize(url)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let permit = authority.try_network_request(url)?;
        let in_flight = permit.authorize_send(url)?;
        let operation = async {
            let transport_started = Instant::now();
            let response = self
                .client
                .get(url)
                .header(ACCEPT, "text/plain")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .send()
                .await
                .map_err(|_| ExtractionSourceError::Source(SourceError::Network))?;
            if response.url().as_str() != url {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .map(|value| value.as_bytes().to_vec());
            if status == 429 || status == 503 {
                let deadline = in_flight.apply_retry_after_header(retry_after.as_deref(), 0)?;
                return Err(SourceError::BudgetWaitUntil { deadline }.into());
            }
            if status == 401 || status == 403 {
                return Err(SourceError::Unauthorized.into());
            }
            if status != 200 {
                return Err(SourceError::ProviderUnavailable.into());
            }
            let declared_content_length = declared_content_length(response.headers())?;
            if declared_content_length.is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| length > max_response_bytes)
            }) {
                return Err(SourceError::FrameTooLarge {
                    max: max_response_bytes,
                }
                .into());
            }
            let content_encoding = response
                .headers()
                .get(CONTENT_ENCODING)
                .map(|value| value.as_bytes().to_vec());
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .map(|value| value.as_bytes().to_vec());
            let retained_content_encoding = retained_header(response.headers(), &CONTENT_ENCODING)?;
            let retained_content_type = retained_header(response.headers(), &CONTENT_TYPE)?
                .ok_or(SourceError::InvalidProtocolState)?;
            let etag = retained_header(response.headers(), &ETAG)?;
            if content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
                || !content_type_is_text(content_type.as_deref())
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let last_modified_at = retained_header(response.headers(), &LAST_MODIFIED)?
                .as_deref()
                .and_then(|value| httpdate::parse_http_date(value).ok())
                .and_then(system_time_to_timestamp)
                .ok_or(SourceError::InvalidProtocolState)?;

            let mut stream = response.bytes_stream();
            let mut body = BytesMut::new();
            while let Some(chunk) = stream.next().await {
                in_flight.validate_current()?;
                let chunk = chunk.map_err(|_| SourceError::Network)?;
                let next =
                    body.len()
                        .checked_add(chunk.len())
                        .ok_or(SourceError::FrameTooLarge {
                            max: max_response_bytes,
                        })?;
                if next > max_response_bytes {
                    return Err(SourceError::FrameTooLarge {
                        max: max_response_bytes,
                    }
                    .into());
                }
                in_flight.validate_response_size(
                    u64::try_from(next).map_err(|_| SourceError::InvalidProtocolState)?,
                )?;
                body.extend_from_slice(&chunk);
            }
            let transport_elapsed_nanos = elapsed_nanos(transport_started, timeout)?;
            let bytes = body.freeze();
            if bytes.is_empty() {
                return Err(SourceError::InvalidProtocolState.into());
            }
            in_flight.validate_response_size(
                u64::try_from(bytes.len()).map_err(|_| SourceError::InvalidProtocolState)?,
            )?;
            if declared_content_length
                .is_some_and(|declared| u64::try_from(bytes.len()) != Ok(declared))
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let received_at =
                system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
            if last_modified_at > received_at {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let digest = Sha256::digest(&bytes);
            let sha256_hex = format!("{digest:x}");
            let response_evidence = NasdaqHttpResponseEvidence::try_new(
                status,
                retained_content_type,
                retained_content_encoding,
                declared_content_length,
                etag,
                transport_elapsed_nanos,
                last_modified_at,
                received_at,
            )
            .map_err(|_| SourceError::InvalidProtocolState)?;
            in_flight.record_success()?;
            Ok(RetrievedDirectory {
                kind,
                bytes,
                received_at,
                last_modified_at,
                sha256_hex,
                response_evidence,
            })
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled),
            result = tokio::time::timeout(timeout, operation) => {
                result.map_err(|_| ExtractionSourceError::DeadlineExceeded)?
            }
        }
    }

    pub(crate) async fn fetch_logical_object(
        &self,
        metadata: &SourceMetadata,
        authority: &ExtractionAuthority,
        kind: NasdaqDirectoryKind,
        store: &SealedResearchJournalStore,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedReferenceObject, NasdaqReferenceIngestError> {
        if cancellation.is_cancelled() {
            return Err(ExtractionSourceError::Cancelled.into());
        }
        let control =
            NasdaqReferenceOperationControl::try_new_for_source(deadline, cancellation, authority)?;
        let now = system_timestamp()?;
        authority.validate_current()?;
        if authority.metadata() != metadata || !metadata.is_effective_at(now) {
            return Err(SourceError::InvalidProtocolState.into());
        }
        let url = endpoint(kind).ok_or(crate::NasdaqReferenceError::ProviderContractUnavailable)?;
        let max_response_bytes = self.max_response_bytes.min(kind.maximum_source_bytes());
        metadata
            .network_policy()
            .authorize(url)
            .map_err(|_| SourceError::InvalidProtocolState)?;
        let timeout = remaining_timeout(deadline, now, self.total_timeout)?;
        let permit = authority.try_network_request(url)?;
        let in_flight = permit.authorize_send(url)?;
        let operation = async {
            let transport_started = Instant::now();
            let response = self
                .client
                .get(url)
                .header(ACCEPT, "text/plain")
                .header(ACCEPT_ENCODING, "identity")
                .header(USER_AGENT, USER_AGENT_VALUE)
                .send()
                .await
                .map_err(|_| SourceError::Network)?;
            if response.url().as_str() != url {
                return Err(NasdaqReferenceIngestError::from(
                    SourceError::InvalidProtocolState,
                ));
            }
            let status = response.status().as_u16();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .map(|value| value.as_bytes().to_vec());
            if status == 429 || status == 503 {
                let retry_deadline =
                    in_flight.apply_retry_after_header(retry_after.as_deref(), 0)?;
                let evidence = NasdaqReferenceRetryEvidence::try_new(
                    kind,
                    status,
                    retry_after.is_some(),
                    elapsed_nanos(transport_started, timeout)?,
                    system_timestamp()?,
                    retry_deadline,
                )?;
                return Err(NasdaqReferenceIngestError::RetryRequired { evidence });
            }
            if status == 401 || status == 403 {
                return Err(SourceError::Unauthorized.into());
            }
            if status != 200 {
                return Err(SourceError::ProviderUnavailable.into());
            }
            let declared_content_length = declared_content_length(response.headers())?;
            if declared_content_length.is_some_and(|length| {
                usize::try_from(length).map_or(true, |length| length > max_response_bytes)
            }) {
                return Err(SourceError::FrameTooLarge {
                    max: max_response_bytes,
                }
                .into());
            }
            let content_encoding = response
                .headers()
                .get(CONTENT_ENCODING)
                .map(|value| value.as_bytes().to_vec());
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .map(|value| value.as_bytes().to_vec());
            let retained_content_encoding = retained_header(response.headers(), &CONTENT_ENCODING)?;
            let retained_content_type = retained_header(response.headers(), &CONTENT_TYPE)?
                .ok_or(SourceError::InvalidProtocolState)?;
            let etag = retained_header(response.headers(), &ETAG)?;
            if content_encoding
                .as_deref()
                .is_some_and(|value| !value.eq_ignore_ascii_case(b"identity"))
                || !content_type_is_text(content_type.as_deref())
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let last_modified_at = retained_header(response.headers(), &LAST_MODIFIED)?
                .as_deref()
                .and_then(|value| httpdate::parse_http_date(value).ok())
                .and_then(system_time_to_timestamp)
                .ok_or(SourceError::InvalidProtocolState)?;
            let mut pending = store
                .begin_logical_object(logical_object_admission(kind)?)
                .map_err(crate::NasdaqReferenceError::from)?;
            let mut body_bytes = 0_usize;
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                control.checkpoint()?;
                in_flight.validate_current()?;
                let chunk = chunk.map_err(|_| SourceError::Network)?;
                body_bytes =
                    body_bytes
                        .checked_add(chunk.len())
                        .ok_or(SourceError::FrameTooLarge {
                            max: max_response_bytes,
                        })?;
                if body_bytes > max_response_bytes {
                    return Err(SourceError::FrameTooLarge {
                        max: max_response_bytes,
                    }
                    .into());
                }
                in_flight.validate_response_size(
                    u64::try_from(body_bytes).map_err(|_| SourceError::InvalidProtocolState)?,
                )?;
                let mut remaining = chunk.as_ref();
                while !remaining.is_empty() {
                    control.checkpoint()?;
                    let written = pending
                        .write_admitted(remaining)
                        .map_err(crate::NasdaqReferenceError::from)?;
                    if written == 0 {
                        return Err(crate::NasdaqReferenceError::InvalidObjectReceipt.into());
                    }
                    remaining = remaining
                        .get(written..)
                        .ok_or(crate::NasdaqReferenceError::InvalidObjectReceipt)?;
                }
            }
            let transport_elapsed_nanos = elapsed_nanos(transport_started, timeout)?;
            if body_bytes == 0 {
                return Err(SourceError::InvalidProtocolState.into());
            }
            in_flight.validate_response_size(
                u64::try_from(body_bytes).map_err(|_| SourceError::InvalidProtocolState)?,
            )?;
            if declared_content_length
                .is_some_and(|declared| u64::try_from(body_bytes) != Ok(declared))
            {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let received_at = system_timestamp()?;
            if last_modified_at > received_at {
                return Err(SourceError::InvalidProtocolState.into());
            }
            let response_evidence = NasdaqHttpResponseEvidence::try_new(
                status,
                retained_content_type,
                retained_content_encoding,
                declared_content_length,
                etag,
                transport_elapsed_nanos,
                last_modified_at,
                received_at,
            )?;
            in_flight.record_success()?;
            let verified_object = store
                .finish_logical_object(pending, &control)
                .map_err(crate::NasdaqReferenceError::from)?;
            if verified_object.size_bytes()
                != u64::try_from(body_bytes).map_err(|_| SourceError::InvalidProtocolState)?
            {
                return Err(crate::NasdaqReferenceError::InvalidObjectReceipt.into());
            }
            Ok(RetrievedReferenceObject {
                response_evidence,
                verified_object,
            })
        };
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ExtractionSourceError::Cancelled.into()),
            result = tokio::time::timeout(timeout, operation) => {
                result.map_err(|_| NasdaqReferenceIngestError::Extraction(ExtractionSourceError::DeadlineExceeded))?
            }
        }
    }
}

fn build_client(
    bounds: HttpRequestBounds,
) -> Result<reqwest::Client, NasdaqSymbolDirectorySourceError> {
    reqwest::Client::builder()
        .https_only(true)
        .tls_backend_rustls()
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .referer(false)
        .retry(reqwest::retry::never())
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_zstd()
        .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
        .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
        .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
        .build()
        .map_err(|_| NasdaqSymbolDirectorySourceError::InvalidMetadata)
}

fn endpoint(kind: NasdaqDirectoryKind) -> Option<&'static str> {
    match kind {
        NasdaqDirectoryKind::NasdaqListed => Some(NASDAQ_LISTED_URL),
        NasdaqDirectoryKind::OtherListed => Some(OTHER_LISTED_URL),
        NasdaqDirectoryKind::Bonds => None,
        NasdaqDirectoryKind::Options => Some(OPTIONS_URL),
    }
}

fn content_type_is_text(value: Option<&[u8]>) -> bool {
    value
        .and_then(|value| std::str::from_utf8(value).ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/plain"))
}

fn retained_header(headers: &HeaderMap, name: &HeaderName) -> Result<Option<String>, SourceError> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| SourceError::InvalidProtocolState)
        })
        .transpose()?;
    if values.next().is_some() {
        return Err(SourceError::InvalidProtocolState);
    }
    Ok(value)
}

fn declared_content_length(headers: &HeaderMap) -> Result<Option<u64>, SourceError> {
    retained_header(headers, &CONTENT_LENGTH)?
        .map(|value| {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(SourceError::InvalidProtocolState);
            }
            value
                .parse::<u64>()
                .map_err(|_| SourceError::InvalidProtocolState)
        })
        .transpose()
}

fn elapsed_nanos(started: Instant, maximum: Duration) -> Result<u64, SourceError> {
    let elapsed = u64::try_from(started.elapsed().as_nanos())
        .ok()
        .filter(|elapsed| *elapsed > 0)
        .ok_or(SourceError::InvalidProtocolState)?;
    if u128::from(elapsed) > maximum.as_nanos() {
        return Err(SourceError::InvalidProtocolState);
    }
    Ok(elapsed)
}

pub(crate) fn system_timestamp() -> Result<Timestamp, NasdaqSymbolDirectorySourceError> {
    system_time_to_timestamp(SystemTime::now()).ok_or(NasdaqSymbolDirectorySourceError::Clock)
}

fn system_time_to_timestamp(time: SystemTime) -> Option<Timestamp> {
    let duration = time.duration_since(UNIX_EPOCH).ok()?;
    let nanos = u128::from(duration.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(duration.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())?;
    Some(Timestamp::from_unix_nanos(nanos))
}

pub(crate) fn ensure_deadline_open(deadline: Timestamp) -> Result<(), ExtractionSourceError> {
    let now = system_timestamp().map_err(|_| SourceError::TrustedTimeUnavailable)?;
    if deadline <= now {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn remaining_timeout(
    deadline: Timestamp,
    now: Timestamp,
    configured_total: Duration,
) -> Result<Duration, ExtractionSourceError> {
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .and_then(|value| u64::try_from(value).ok())
        .filter(|value| *value > 0)
        .map(Duration::from_nanos)
        .ok_or(ExtractionSourceError::DeadlineExceeded)?;
    Ok(remaining.min(configured_total))
}
