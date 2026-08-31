//! Hidden multi-origin transport for one bounded filing taxonomy closure.

use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, Timestamp,
};
use market_squawk_sources::{
    BudgetDispatchDecision, BudgetPermit, BudgetReservationDecision, ExtractionAuthority,
    FILING_TAXONOMY_SOURCE_AUTHORITIES, FilingTaxonomyRequestHeaderClass,
    FilingTaxonomySourceAuthority, InFlightExtractionRequest, MAX_PROVIDER_CAPTURE_PAGE_BYTES,
    ProviderRateAuthority, ProviderRateDeclaration, ProviderRateResponseClass,
    ProviderRateResponseSettlement, ProviderRateRetryAfterDisposition, SEC_EDGAR_AUTHORITY,
    SealedProviderCaptureBinding, SharedProviderBudget,
};
use reqwest::header::{ACCEPT_ENCODING, RETRY_AFTER};
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;
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

struct TaxonomyHttpClient {
    authority: FilingTaxonomySourceAuthority,
    client: reqwest::Client,
    shared_budget: Option<SharedProviderBudget>,
}

impl std::fmt::Debug for TaxonomyHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaxonomyHttpClient")
            .field("source_id", &self.authority.source_id())
            .finish_non_exhaustive()
    }
}

enum TaxonomyInFlightRequest {
    Sec(InFlightExtractionRequest),
    External(BudgetPermit),
}

impl TaxonomyInFlightRequest {
    fn validate_current(&self, sec_authority: &ExtractionAuthority) -> Result<(), SecClientError> {
        sec_authority.validate_current()?;
        if let Self::Sec(in_flight) = self {
            in_flight.validate_current()?;
        }
        Ok(())
    }

    fn abandon(self) -> Result<(), SecClientError> {
        match self {
            Self::Sec(in_flight) => {
                in_flight.release();
                Ok(())
            }
            Self::External(in_flight) => {
                let _receipt = in_flight
                    .settle_response(ProviderRateResponseSettlement::abandoned_unknown())
                    .map_err(SecClientError::TaxonomyRateUnavailable)?;
                Ok(())
            }
        }
    }

    fn settle_complete(
        self,
        completed_response_bytes: u64,
        response_class: ProviderRateResponseClass,
        retry_after_field: Option<&[u8]>,
        retry_after: ProviderRateRetryAfterDisposition,
    ) -> Result<(), SecClientError> {
        match self {
            Self::Sec(in_flight) => match response_class {
                ProviderRateResponseClass::ValidatedSuccess => {
                    in_flight.record_success()?;
                    Ok(())
                }
                ProviderRateResponseClass::ProviderRefusal => {
                    let _deadline = in_flight.apply_retry_after_header(retry_after_field, 5_000)?;
                    Ok(())
                }
                ProviderRateResponseClass::HttpProviderError
                | ProviderRateResponseClass::ProviderBodyError
                | ProviderRateResponseClass::InvalidProviderResponse
                | ProviderRateResponseClass::KnownCompleteLocalAbort
                | ProviderRateResponseClass::AbandonedUnknown => {
                    in_flight.release();
                    Ok(())
                }
            },
            Self::External(in_flight) => {
                let retry_after = if matches!(
                    response_class,
                    ProviderRateResponseClass::ProviderRefusal
                        | ProviderRateResponseClass::InvalidProviderResponse
                ) {
                    retry_after
                } else {
                    ProviderRateRetryAfterDisposition::Absent
                };
                let settlement = ProviderRateResponseSettlement::try_new(
                    completed_response_bytes,
                    response_class,
                    retry_after,
                    0,
                )
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
                let _receipt = in_flight
                    .settle_response(settlement)
                    .map_err(SecClientError::TaxonomyRateUnavailable)?;
                Ok(())
            }
        }
    }
}

/// Closed durable rate allocations for the four hidden external taxonomy publishers.
pub struct FilingTaxonomySharedRateBudgets {
    by_source: BTreeMap<SourceId, SharedProviderBudget>,
}

impl std::fmt::Debug for FilingTaxonomySharedRateBudgets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FilingTaxonomySharedRateBudgets")
            .field("publisher_count", &self.by_source.len())
            .finish_non_exhaustive()
    }
}

impl FilingTaxonomySharedRateBudgets {
    /// Registers every hidden external publisher against the application's sole durable rate
    /// authority. SEC-owned taxonomy traffic remains in the aggregate SEC extraction authority.
    pub fn try_new(provider_rate: &ProviderRateAuthority) -> Result<Self, SecClientError> {
        const EXTERNAL_PUBLISHER_COUNT: usize = 4;

        let mut by_source = BTreeMap::new();
        for authority in FILING_TAXONOMY_SOURCE_AUTHORITIES {
            if authority == SEC_EDGAR_AUTHORITY {
                continue;
            }
            let source_id = authority
                .canonical_source_id()
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
            let endpoint_policy = authority
                .endpoint_policy()
                .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
            let budget_policy = authority
                .budget_policy()
                .map_err(|_| SecClientError::UnsafeBudgetPolicy)?;
            let declaration =
                ProviderRateDeclaration::try_for_endpoint(budget_policy, &endpoint_policy)
                    .map_err(SecClientError::TaxonomyRateRegistration)?;
            let budget = provider_rate
                .register_budget(declaration)
                .map_err(SecClientError::TaxonomyRateRegistration)?;
            if by_source.insert(source_id, budget).is_some() {
                return Err(SecClientError::InvalidCaptureMaterial);
            }
        }
        if by_source.len() != EXTERNAL_PUBLISHER_COUNT {
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        Ok(Self { by_source })
    }

    fn into_inner(self) -> BTreeMap<SourceId, SharedProviderBudget> {
        self.by_source
    }
}

/// Persistent hardened clients bound to the closed publisher catalog and durable rate budgets.
#[derive(Debug)]
pub(crate) struct TaxonomyClientSet {
    clients: BTreeMap<SourceId, TaxonomyHttpClient>,
}

impl TaxonomyClientSet {
    pub(crate) fn try_new(
        contact: &SecContact,
        shared_budgets: FilingTaxonomySharedRateBudgets,
    ) -> Result<Self, SecClientError> {
        let mut shared_budgets = shared_budgets.into_inner();
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
            let shared_budget = if authority == SEC_EDGAR_AUTHORITY {
                if shared_budgets.remove(&source_id).is_some() {
                    return Err(SecClientError::InvalidCaptureMaterial);
                }
                None
            } else {
                Some(
                    shared_budgets
                        .remove(&source_id)
                        .ok_or(SecClientError::MissingSharedBudget)?,
                )
            };
            if clients
                .insert(
                    source_id,
                    TaxonomyHttpClient {
                        authority,
                        client,
                        shared_budget,
                    },
                )
                .is_some()
            {
                return Err(SecClientError::InvalidCaptureMaterial);
            }
        }
        if !shared_budgets.is_empty() {
            return Err(SecClientError::InvalidCaptureMaterial);
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
        let endpoint_policy = authority
            .endpoint_policy()
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
        endpoint_policy.authorize_request(request.physical_locator())?;
        let request_bounds = endpoint_policy.request_bounds();
        let maximum = request_bounds
            .max_response_bytes()
            .min(MAX_PROVIDER_CAPTURE_PAGE_BYTES)
            .min(MAX_TAXONOMY_ARTIFACT_BYTES)
            .min(request.maximum_response_bytes());
        let request_send = client
            .client
            .get(request.physical_locator())
            .header(ACCEPT_ENCODING, "identity");
        let mut in_flight = Some(if authority == SEC_EDGAR_AUTHORITY {
            TaxonomyInFlightRequest::Sec(
                sec_authority
                    .try_network_request(request.physical_locator())?
                    .authorize_send(request.physical_locator())?,
            )
        } else {
            let budget = client
                .shared_budget
                .as_ref()
                .ok_or(SecClientError::MissingSharedBudget)?;
            let maximum_response_bytes = NonZeroU64::new(request_bounds.max_response_bytes())
                .ok_or(SecClientError::UnsafeBudgetPolicy)?;
            TaxonomyInFlightRequest::External(
                acquire_external_request(budget, maximum_response_bytes, deadline, cancellation)
                    .await?,
            )
        });
        let send_deadline_wait = match remaining_until(deadline) {
            Ok(wait) => wait,
            Err(error) => {
                abandon_request(&mut in_flight)?;
                return Err(error);
            }
        };
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::Cancelled);
            }
            () = tokio::time::sleep(send_deadline_wait) => {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::DeadlineExceeded);
            }
            response = request_send.send() => match response {
                    Ok(response) => response,
                    Err(error) => {
                        abandon_request(&mut in_flight)?;
                        return Err(error.into());
                    }
                },
        };
        if let Err(error) = in_flight
            .as_ref()
            .ok_or(SecClientError::InvalidCaptureMaterial)?
            .validate_current(sec_authority)
        {
            abandon_request(&mut in_flight)?;
            return Err(error);
        }
        let status = response.status();
        let retry_after = response.headers().get(RETRY_AFTER).cloned();
        let retry_after_disposition = ProviderRateRetryAfterDisposition::parse_http(
            retry_after.as_ref().map(|value| value.as_bytes()),
        );
        if matches!(status.as_u16(), 429 | 503) {
            let retry_after_field = retry_after.as_ref().map(|value| value.as_bytes());
            drop(response);
            settle_response(
                &mut in_flight,
                0,
                ProviderRateResponseClass::ProviderRefusal,
                retry_after_field,
                retry_after_disposition,
            )?;
            return Err(SecClientError::HttpStatus(status.as_u16()));
        }
        let validators = if status.is_success() {
            Some(super::response_validators(response.headers()))
        } else {
            None
        };
        if let Some(length) = response.content_length() {
            if length > maximum {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::ResponseTooLarge);
            }
        }
        let read_timeout = Duration::from_nanos(request_bounds.read_timeout_nanos());
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            if let Err(error) = in_flight
                .as_ref()
                .ok_or(SecClientError::InvalidCaptureMaterial)?
                .validate_current(sec_authority)
            {
                abandon_request(&mut in_flight)?;
                return Err(error);
            }
            let response_deadline_wait = match remaining_until(deadline) {
                Ok(wait) => wait,
                Err(error) => {
                    abandon_request(&mut in_flight)?;
                    return Err(error);
                }
            };
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    abandon_request(&mut in_flight)?;
                    return Err(SecClientError::Cancelled);
                }
                () = tokio::time::sleep(response_deadline_wait) => {
                    abandon_request(&mut in_flight)?;
                    return Err(SecClientError::DeadlineExceeded);
                }
                result = tokio::time::timeout(read_timeout, stream.next()) => {
                    match result {
                        Ok(result) => result,
                        Err(_) => {
                            abandon_request(&mut in_flight)?;
                            return Err(SecClientError::ReadTimeout);
                        }
                    }
                }
            };
            let Some(chunk) = next else { break };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    abandon_request(&mut in_flight)?;
                    return Err(error.into());
                }
            };
            let Some(length) = bytes.len().checked_add(chunk.len()) else {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::ResponseTooLarge);
            };
            let Ok(length_u64) = u64::try_from(length) else {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::ResponseTooLarge);
            };
            if length_u64 > maximum {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::ResponseTooLarge);
            }
            if bytes.try_reserve(chunk.len()).is_err() {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::AllocationFailed);
            }
            bytes.extend_from_slice(&chunk);
        }
        let completed_response_bytes = match u64::try_from(bytes.len()) {
            Ok(bytes) => bytes,
            Err(_) => {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::ResponseTooLarge);
            }
        };
        let retry_after_field = retry_after.as_ref().map(|value| value.as_bytes());
        if status.is_redirection() {
            settle_response(
                &mut in_flight,
                completed_response_bytes,
                ProviderRateResponseClass::InvalidProviderResponse,
                retry_after_field,
                retry_after_disposition,
            )?;
            return Err(SecClientError::InvalidRedirect);
        }
        if !status.is_success() {
            settle_response(
                &mut in_flight,
                completed_response_bytes,
                ProviderRateResponseClass::HttpProviderError,
                None,
                ProviderRateRetryAfterDisposition::Absent,
            )?;
            return Err(SecClientError::HttpStatus(status.as_u16()));
        }
        let validators = match validators {
            Some(Ok(validators)) => validators,
            Some(Err(error)) => {
                settle_response(
                    &mut in_flight,
                    completed_response_bytes,
                    ProviderRateResponseClass::InvalidProviderResponse,
                    retry_after_field,
                    retry_after_disposition,
                )?;
                return Err(error);
            }
            None => {
                abandon_request(&mut in_flight)?;
                return Err(SecClientError::InvalidCaptureMaterial);
            }
        };
        if bytes.is_empty() {
            settle_response(
                &mut in_flight,
                completed_response_bytes,
                ProviderRateResponseClass::InvalidProviderResponse,
                retry_after_field,
                retry_after_disposition,
            )?;
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        let body_received_at = match super::system_timestamp() {
            Ok(timestamp) => timestamp,
            Err(error) => {
                settle_local_abort(&mut in_flight, completed_response_bytes)?;
                return Err(error);
            }
        };
        let physical_locator = request.physical_locator().to_owned();
        let request_identity =
            taxonomy_request_identity(&source_id, request.logical_locator(), &physical_locator);
        let metadata_revision = match if authority == SEC_EDGAR_AUTHORITY {
            Ok(root_metadata_revision.clone())
        } else {
            authority.metadata_revision()
        } {
            Ok(revision) => revision,
            Err(_) => {
                settle_local_abort(&mut in_flight, completed_response_bytes)?;
                return Err(SecClientError::InvalidCaptureMaterial);
            }
        };
        let retained_source_id = source_id.clone();
        let retained = super::run_joined_blocking(
            blocking_admission,
            cancellation,
            Some(deadline),
            move |worker_token| {
                let evidence = raw_store.persist_cancellable(&bytes, worker_token)?;
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
                    worker_token,
                )?;
                Ok::<_, SecClientError>((bytes, representation))
            },
        )
        .await;
        let retained = match retained {
            Ok(retained) => retained,
            Err(error) => {
                settle_local_abort(&mut in_flight, completed_response_bytes)?;
                return Err(error);
            }
        };
        if let Err(error) = ensure_before_deadline(deadline) {
            settle_local_abort(&mut in_flight, completed_response_bytes)?;
            return Err(error);
        }
        if cancellation.is_cancelled() {
            settle_local_abort(&mut in_flight, completed_response_bytes)?;
            return Err(SecClientError::Cancelled);
        }
        if let Err(error) = in_flight
            .as_ref()
            .ok_or(SecClientError::InvalidCaptureMaterial)?
            .validate_current(sec_authority)
        {
            settle_local_abort(&mut in_flight, completed_response_bytes)?;
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
                settle_local_abort(&mut in_flight, completed_response_bytes)?;
                return Err(error);
            }
        };
        settle_response(
            &mut in_flight,
            completed_response_bytes,
            ProviderRateResponseClass::ValidatedSuccess,
            None,
            ProviderRateRetryAfterDisposition::Absent,
        )?;
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
        sealed_root: SealedProviderCaptureBinding,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecFilingXbrlCaptureHandoff, SecClientError> {
        self.validate_authority(authority)?;
        ensure_before_deadline(deadline)?;
        let source_id = self.metadata.source_id().clone();
        let metadata_revision = self.metadata.revision().clone();
        let admission_submissions = submissions.clone();
        let admission_filing = filing_document.clone();
        let admission_accession = accession.to_owned();
        let admission_raw_store = Arc::clone(&self.raw_store);
        let admission_representations = Arc::clone(&self.representation_registry);
        let admission_source_id = source_id.clone();
        let admission_revision = metadata_revision.clone();
        let parser_limits = self.parser_limits;
        let admitted_root = self
            .run_validation_blocking_until(&cancellation, deadline, move |worker_token| {
                crate::extraction::admit_filing_xbrl_root_from_sealed_binding(
                    sealed_root,
                    admission_raw_store,
                    admission_representations,
                    admission_source_id,
                    admission_revision,
                    parser_limits,
                    &admission_submissions,
                    &admission_accession,
                    &admission_filing,
                    worker_token,
                )
            })
            .await?;
        ensure_before_deadline(deadline)?;
        let mut closure = SecTaxonomyClosure::try_start(
            admitted_root.filing_document(),
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
        let raw_store = Arc::clone(&self.raw_store);
        let identities = Arc::clone(&self.identities);
        let parser_limits = self.parser_limits;
        let handoff = self
            .run_validation_blocking_until(&cancellation, deadline, move |worker_token| {
                crate::extraction::prepare_filing_xbrl_capture_from_admitted_root(
                    raw_store,
                    identities,
                    source_id,
                    metadata_revision,
                    parser_limits,
                    submissions,
                    admitted_root,
                    artifacts,
                    worker_token,
                )
            })
            .await?;
        self.validate_authority(authority)?;
        Ok(handoff)
    }
}

async fn acquire_external_request(
    budget: &SharedProviderBudget,
    maximum_response_bytes: NonZeroU64,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<BudgetPermit, SecClientError> {
    loop {
        ensure_before_deadline(deadline)?;
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        let reservation = match budget.try_reserve_request() {
            BudgetReservationDecision::Ready(reservation) => reservation,
            BudgetReservationDecision::WaitUntil(wait_until) => {
                wait_for_budget(budget, wait_until, deadline, cancellation).await?;
                continue;
            }
            BudgetReservationDecision::Unavailable(reason) => {
                return Err(SecClientError::TaxonomyRateUnavailable(reason));
            }
        };
        if cancellation.is_cancelled() {
            reservation.release();
            return Err(SecClientError::Cancelled);
        }
        if let Err(error) = ensure_before_deadline(deadline) {
            reservation.release();
            return Err(error);
        }
        match reservation.commit_dispatch_with_response_bound(maximum_response_bytes) {
            BudgetDispatchDecision::Ready(permit) => return Ok(permit),
            BudgetDispatchDecision::WaitUntil(wait_until) => {
                wait_for_budget(budget, wait_until, deadline, cancellation).await?;
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(SecClientError::TaxonomyRateUnavailable(reason));
            }
        }
    }
}

async fn wait_for_budget(
    budget: &SharedProviderBudget,
    wait_until: market_squawk_sources::MonotonicInstant,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), SecClientError> {
    let budget_wait = budget
        .remaining_wait(wait_until)
        .map_err(SecClientError::TaxonomyRateUnavailable)?;
    let deadline_wait = remaining_until(deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SecClientError::Cancelled),
        () = tokio::time::sleep(deadline_wait) => Err(SecClientError::DeadlineExceeded),
        () = tokio::time::sleep(budget_wait) => Ok(()),
    }
}

fn abandon_request(in_flight: &mut Option<TaxonomyInFlightRequest>) -> Result<(), SecClientError> {
    in_flight
        .take()
        .ok_or(SecClientError::InvalidCaptureMaterial)?
        .abandon()
}

fn settle_response(
    in_flight: &mut Option<TaxonomyInFlightRequest>,
    completed_response_bytes: u64,
    response_class: ProviderRateResponseClass,
    retry_after_field: Option<&[u8]>,
    retry_after: ProviderRateRetryAfterDisposition,
) -> Result<(), SecClientError> {
    in_flight
        .take()
        .ok_or(SecClientError::InvalidCaptureMaterial)?
        .settle_complete(
            completed_response_bytes,
            response_class,
            retry_after_field,
            retry_after,
        )
}

fn settle_local_abort(
    in_flight: &mut Option<TaxonomyInFlightRequest>,
    completed_response_bytes: u64,
) -> Result<(), SecClientError> {
    settle_response(
        in_flight,
        completed_response_bytes,
        ProviderRateResponseClass::KnownCompleteLocalAbort,
        None,
        ProviderRateRetryAfterDisposition::Absent,
    )
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
