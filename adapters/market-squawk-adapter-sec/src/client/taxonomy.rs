//! Hidden multi-origin transport for one bounded filing taxonomy closure.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::StreamExt as _;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, Timestamp,
};
use market_squawk_sources::{
    ExtractionAuthority, FILING_TAXONOMY_SOURCE_AUTHORITIES, FilingTaxonomyRequestHeaderClass,
    FilingTaxonomySourceAuthority, MAX_PROVIDER_CAPTURE_PAGE_BYTES, SEC_EDGAR_AUTHORITY,
};
use reqwest::header::{ACCEPT_ENCODING, RETRY_AFTER};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::xbrl::{MAX_TAXONOMY_ARTIFACT_BYTES, SecTaxonomyAcquisitionRequest, SecTaxonomyClosure};
use crate::{
    RawEvidenceStore, RetrievedSecBytes, RetrievedSubmissions, SecClientError, SecContact,
    SecFilingXbrlCaptureHandoff, SecRepresentationRegistry,
};

const PRODUCT_ONLY_USER_AGENT: &str = concat!(
    "Market-Squawk/",
    env!("CARGO_PKG_VERSION"),
    " taxonomy-retrieval (+https://github.com/Sawmonabo/market-squawk)"
);

#[derive(Debug)]
struct TaxonomyBudgetState {
    next_dispatch: tokio::time::Instant,
}

struct TaxonomyHttpClient {
    authority: FilingTaxonomySourceAuthority,
    client: reqwest::Client,
    minimum_dispatch_interval: Duration,
    budget: Mutex<TaxonomyBudgetState>,
}

impl std::fmt::Debug for TaxonomyHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaxonomyHttpClient")
            .field("source_id", &self.authority.source_id())
            .finish_non_exhaustive()
    }
}

/// Persistent application-source-local clients and budgets for the closed authority catalog.
#[derive(Debug)]
pub(crate) struct TaxonomyClientSet {
    clients: BTreeMap<SourceId, TaxonomyHttpClient>,
}

impl TaxonomyClientSet {
    pub(crate) fn try_new(contact: &SecContact) -> Result<Self, SecClientError> {
        let mut clients = BTreeMap::new();
        for authority in FILING_TAXONOMY_SOURCE_AUTHORITIES {
            let source_id = authority
                .canonical_source_id()
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
            let endpoint_policy = authority
                .endpoint_policy()
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
            let profile = endpoint_policy.client_profile();
            if !profile.automatic_redirects_disabled()
                || !profile.ambient_system_proxy_disabled()
                || !profile.implicit_retries_disabled()
                || !profile.counts_post_decompression_bytes()
            {
                return Err(SecClientError::UnsafeClientProfile);
            }
            let bounds = endpoint_policy.request_bounds();
            let user_agent = match authority.request_header_class() {
                FilingTaxonomyRequestHeaderClass::SecIdentifyingContact => contact.user_agent(),
                FilingTaxonomyRequestHeaderClass::ProductOnlyNoSecContact => {
                    PRODUCT_ONLY_USER_AGENT
                }
            };
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .referer(false)
                .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
                .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
                .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
                .user_agent(user_agent)
                .build()?;
            let budget = authority
                .budget_policy()
                .map_err(|_| SecClientError::UnsafeBudgetPolicy)?;
            let minimum_dispatch_interval = Duration::from_nanos(
                budget
                    .window_nanos()
                    .checked_div(u64::from(budget.requests_per_window()))
                    .filter(|nanos| *nanos > 0)
                    .ok_or(SecClientError::UnsafeBudgetPolicy)?,
            );
            if clients
                .insert(
                    source_id,
                    TaxonomyHttpClient {
                        authority,
                        client,
                        minimum_dispatch_interval,
                        budget: Mutex::new(TaxonomyBudgetState {
                            next_dispatch: tokio::time::Instant::now(),
                        }),
                    },
                )
                .is_some()
            {
                return Err(SecClientError::InvalidCaptureMaterial);
            }
        }
        Ok(Self { clients })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the exact request, source authority, persistence, deadline, and cancellation are independent boundaries"
    )]
    async fn fetch(
        &self,
        sec_authority: &ExtractionAuthority,
        root_metadata_revision: &MetadataRevision,
        request: &SecTaxonomyAcquisitionRequest,
        raw_store: Arc<RawEvidenceStore>,
        representations: Arc<SecRepresentationRegistry>,
        blocking_admission: Arc<Semaphore>,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        ensure_before_deadline(deadline)?;
        let authority = request.authority().map_err(SecClientError::Xbrl)?;
        let source_id = authority
            .canonical_source_id()
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
        let client = self
            .clients
            .get(&source_id)
            .filter(|client| client.authority == authority)
            .ok_or(SecClientError::InvalidCaptureMaterial)?;
        authority
            .endpoint_policy()
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?
            .authorize_request(request.physical_locator())?;

        let mut budget = tokio::select! {
            budget = client.budget.lock() => budget,
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        let now = tokio::time::Instant::now();
        if budget.next_dispatch > now {
            tokio::select! {
                () = tokio::time::sleep_until(budget.next_dispatch) => {}
                () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
                () = tokio::time::sleep(remaining_until(deadline)?) => {
                    return Err(SecClientError::DeadlineExceeded);
                }
            }
        }
        ensure_before_deadline(deadline)?;
        budget.next_dispatch = tokio::time::Instant::now()
            .checked_add(client.minimum_dispatch_interval)
            .ok_or(SecClientError::ClockOutOfRange)?;

        let mut sec_in_flight = if authority == SEC_EDGAR_AUTHORITY {
            Some(
                sec_authority
                    .try_network_request(request.physical_locator())?
                    .authorize_send(request.physical_locator())?,
            )
        } else {
            None
        };
        let response = tokio::select! {
            response = client
                .client
                .get(request.physical_locator())
                .header(ACCEPT_ENCODING, "identity")
                .send() => match response {
                    Ok(response) => response,
                    Err(error) => {
                        if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                        return Err(error.into());
                    }
                },
            () = cancellation.cancelled() => {
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::Cancelled);
            }
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        if let Some(in_flight) = sec_in_flight.as_ref() {
            in_flight.validate_current()?;
        }
        let status = response.status();
        if status.is_redirection() {
            if let Some(in_flight) = sec_in_flight.take() {
                in_flight.release();
            }
            return Err(SecClientError::InvalidRedirect);
        }
        if matches!(status.as_u16(), 429 | 503) {
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .map(|value| value.as_bytes().to_vec());
            if let Some(wait) = parse_retry_after(retry_after.as_deref()) {
                if let Some(next) = tokio::time::Instant::now().checked_add(wait)
                    && next > budget.next_dispatch
                {
                    budget.next_dispatch = next;
                }
            }
            if let Some(in_flight) = sec_in_flight.take() {
                let _deadline =
                    in_flight.apply_retry_after_header(retry_after.as_deref(), 5_000)?;
            }
            return Err(SecClientError::HttpStatus(status.as_u16()));
        }
        if !status.is_success() {
            if let Some(in_flight) = sec_in_flight.take() {
                in_flight.release();
            }
            return Err(SecClientError::HttpStatus(status.as_u16()));
        }
        let validators = match super::response_validators(response.headers()) {
            Ok(validators) => validators,
            Err(error) => {
                if let Some(in_flight) = sec_in_flight.take() {
                    in_flight.release();
                }
                return Err(error);
            }
        };
        let request_bounds = authority
            .endpoint_policy()
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?
            .request_bounds();
        let maximum = request_bounds
            .max_response_bytes()
            .min(MAX_PROVIDER_CAPTURE_PAGE_BYTES)
            .min(MAX_TAXONOMY_ARTIFACT_BYTES)
            .min(request.maximum_response_bytes());
        if let Some(length) = response.content_length() {
            if let Some(in_flight) = sec_in_flight.as_ref() {
                in_flight.validate_response_size(length)?;
            }
            if length == 0 || length > maximum {
                if let Some(in_flight) = sec_in_flight.take() {
                    in_flight.release();
                }
                return Err(SecClientError::ResponseTooLarge);
            }
        }
        let read_timeout = Duration::from_nanos(request_bounds.read_timeout_nanos());
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            if let Some(in_flight) = sec_in_flight.as_ref() {
                in_flight.validate_current()?;
            }
            let next = tokio::select! {
                result = tokio::time::timeout(read_timeout, stream.next()) => {
                    result.map_err(|_| SecClientError::ReadTimeout)?
                }
                () = cancellation.cancelled() => {
                    if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                    return Err(SecClientError::Cancelled);
                }
                () = tokio::time::sleep(remaining_until(deadline)?) => {
                    if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                    return Err(SecClientError::DeadlineExceeded);
                }
            };
            let Some(chunk) = next else { break };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    if let Some(in_flight) = sec_in_flight.take() {
                        in_flight.release();
                    }
                    return Err(error.into());
                }
            };
            let length = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or(SecClientError::ResponseTooLarge)?;
            let length_u64 = u64::try_from(length).map_err(|_| SecClientError::ResponseTooLarge)?;
            if let Some(in_flight) = sec_in_flight.as_ref() {
                in_flight.validate_response_size(length_u64)?;
            }
            if length_u64 > maximum {
                if let Some(in_flight) = sec_in_flight.take() {
                    in_flight.release();
                }
                return Err(SecClientError::ResponseTooLarge);
            }
            bytes
                .try_reserve(chunk.len())
                .map_err(|_| SecClientError::AllocationFailed)?;
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            if let Some(in_flight) = sec_in_flight.take() {
                in_flight.release();
            }
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        let body_received_at = super::system_timestamp()?;
        let physical_locator = request.physical_locator().to_owned();
        let request_identity =
            taxonomy_request_identity(&source_id, request.logical_locator(), &physical_locator);
        let metadata_revision = if authority == SEC_EDGAR_AUTHORITY {
            root_metadata_revision.clone()
        } else {
            authority
                .metadata_revision()
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?
        };
        let retained_source_id = source_id.clone();
        let blocking_permit = tokio::select! {
            permit = blocking_admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => {
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::Cancelled);
            }
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        let worker_cancellation = cancellation.child_token();
        let worker_token = worker_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _blocking_permit = blocking_permit;
            let evidence = raw_store.persist_cancellable(&bytes, &worker_token)?;
            let size_bytes =
                u64::try_from(bytes.len()).map_err(|_| SecClientError::ResponseTooLarge)?;
            // The registry commit is source-qualified and completes before these bytes can be
            // returned to the graph parser.
            let representation = representations.record_source_success_cancellable(
                &retained_source_id,
                &physical_locator,
                evidence,
                size_bytes,
                validators,
                &worker_token,
            )?;
            Ok::<_, SecClientError>((bytes, representation))
        });
        let retained = tokio::select! {
            result = &mut worker => {
                match result {
                    Ok(Ok(retained)) => retained,
                    Ok(Err(error)) => {
                        if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                        return Err(error);
                    }
                    Err(_) => {
                        if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                        return Err(SecClientError::BlockingWorkerFailed);
                    }
                }
            }
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::Cancelled);
            }
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                worker_cancellation.cancel();
                if let Some(in_flight) = sec_in_flight.take() { in_flight.release(); }
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        if let Err(error) = ensure_before_deadline(deadline) {
            if let Some(in_flight) = sec_in_flight.take() {
                in_flight.release();
            }
            return Err(error);
        }
        let retrieved = match super::retrieved_from_representation(
            retained.0,
            retained.1,
            &source_id,
            &metadata_revision,
            request_identity,
            status.as_u16(),
            body_received_at,
        ) {
            Ok(retrieved) => retrieved,
            Err(error) => {
                if let Some(in_flight) = sec_in_flight.take() {
                    in_flight.release();
                }
                return Err(error);
            }
        };
        if let Some(in_flight) = sec_in_flight.take() {
            in_flight.record_success()?;
        }
        Ok(retrieved)
    }
}

impl super::SecEdgarSource {
    /// Retrieves the complete bounded taxonomy dependency graph for one already captured filing
    /// and closes it through the existing opaque filing-XBRL handoff.
    pub async fn fetch_filing_xbrl_capture(
        &self,
        authority: &ExtractionAuthority,
        submissions: RetrievedSubmissions,
        accession: &str,
        filing_document: RetrievedSecBytes,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecFilingXbrlCaptureHandoff, SecClientError> {
        self.validate_authority(authority)?;
        ensure_before_deadline(deadline)?;
        let source_id = self.metadata.source_id().clone();
        let metadata_revision = self.metadata.revision().clone();
        let mut closure = SecTaxonomyClosure::try_start(
            &filing_document,
            source_id.clone(),
            metadata_revision.clone(),
            self.parser_limits,
            &cancellation,
        )?;
        while let Some(request) = closure.next_request(&cancellation)? {
            ensure_before_deadline(deadline)?;
            let artifact = self
                .taxonomy_clients
                .fetch(
                    authority,
                    &metadata_revision,
                    &request,
                    Arc::clone(&self.raw_store),
                    Arc::clone(&self.representation_registry),
                    Arc::clone(&self.blocking_admission),
                    deadline,
                    &cancellation,
                )
                .await?;
            closure.accept_captured(request, artifact, &cancellation)?;
        }
        let artifacts = closure.finish(&cancellation)?;
        ensure_before_deadline(deadline)?;
        self.validate_authority(authority)?;
        let operation_cancellation = cancellation.child_token();
        let worker_cancellation = operation_cancellation.clone();
        let raw_store = Arc::clone(&self.raw_store);
        let representation_registry = Arc::clone(&self.representation_registry);
        let identities = Arc::clone(&self.identities);
        let parser_limits = self.parser_limits;
        let accession = accession.to_owned();
        let preparation =
            self.run_validation_blocking(&operation_cancellation, move |worker_token| {
                crate::extraction::prepare_filing_xbrl_capture_from_state(
                    raw_store,
                    representation_registry,
                    identities,
                    source_id,
                    metadata_revision,
                    parser_limits,
                    submissions,
                    &accession,
                    filing_document,
                    artifacts,
                    worker_token,
                )
            });
        tokio::pin!(preparation);
        let handoff = tokio::select! {
            result = &mut preparation => result?,
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                return Err(SecClientError::Cancelled);
            }
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                worker_cancellation.cancel();
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        self.validate_authority(authority)?;
        Ok(handoff)
    }
}

fn taxonomy_request_identity(
    source_id: &SourceId,
    logical_locator: &str,
    physical_locator: &str,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/sec-taxonomy-http-get-request/v1");
    for field in [
        source_id.as_str().as_bytes(),
        logical_locator.as_bytes(),
        physical_locator.as_bytes(),
    ] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn parse_retry_after(field: Option<&[u8]>) -> Option<Duration> {
    let value = std::str::from_utf8(field?).ok()?.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    retry_at.duration_since(SystemTime::now()).ok()
}

fn ensure_before_deadline(deadline: Timestamp) -> Result<(), SecClientError> {
    if super::system_timestamp()? >= deadline {
        Err(SecClientError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn remaining_until(deadline: Timestamp) -> Result<Duration, SecClientError> {
    let nanos = deadline
        .unix_nanos()
        .checked_sub(super::system_timestamp()?.unix_nanos())
        .filter(|nanos| *nanos > 0)
        .ok_or(SecClientError::DeadlineExceeded)?;
    Ok(Duration::from_nanos(
        u64::try_from(nanos).map_err(|_| SecClientError::DeadlineExceeded)?,
    ))
}
