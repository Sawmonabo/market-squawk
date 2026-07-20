//! Registered, allowlisted, budgeted SEC HTTP retrieval.

mod contracts;

pub(crate) use contracts::system_timestamp;
pub use contracts::{
    RetrievedCompanyFacts, RetrievedSecBytes, RetrievedSubmissions, RetrievedXbrlDocument,
    SecClientError, SecContact, SecExtractionHealth, SecExtractionHealthState, SecObjectLocator,
};
use contracts::{health_for_http_status, validation_health_for_error};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt as _;
use market_squawk_domain::{
    AvailabilityEvidence, DigestAlgorithm, EvidenceDigest, ProviderIdentityRegistry,
};
use market_squawk_sources::{
    AuthorizationMode, BudgetDecision, EndpointPolicy, NetworkAccessPolicy, RegisteredSource,
    SharedProviderBudget, SourceMetadata, SourceMetadataProvider, TlsProviderCapability,
    apply_http_retry_after,
};
use reqwest::header::{
    ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, LOCATION, RETRY_AFTER,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    CompanyFactsDocument, RawEvidenceStore, SecHttpValidators, SecParserLimits, SecRepresentation,
    SecRepresentationRegistry, SubmissionsArchive, SubmissionsDocument, XbrlDocumentContext,
    XbrlDocumentParser,
};

const MAX_EXPLICIT_RETRIES: usize = 3;
const SEC_REQUEST_CEILING_PER_SECOND: u32 = 10;
const ONE_SECOND_NANOS: u64 = 1_000_000_000;
const MAX_BLOCKING_WORKERS: usize = 4;

/// Production SEC source bound to registered metadata, shared budget, and local raw persistence.
#[derive(Debug)]
pub struct SecEdgarSource {
    metadata: SourceMetadata,
    registered: RegisteredSource,
    endpoint_policy: EndpointPolicy,
    budget: SharedProviderBudget,
    client: reqwest::Client,
    raw_store: Arc<RawEvidenceStore>,
    representation_registry: Arc<SecRepresentationRegistry>,
    identities: Arc<ProviderIdentityRegistry>,
    blocking_admission: Arc<Semaphore>,
    extraction_health: Mutex<SecExtractionHealth>,
    parser_limits: SecParserLimits,
}

impl SecEdgarSource {
    /// Constructs a source only from current registration and an installed TLS capability.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is a distinct authority, identity, persistence, or parsing capability"
    )]
    pub fn try_new(
        metadata: SourceMetadata,
        registered: RegisteredSource,
        contact: SecContact,
        tls_provider: TlsProviderCapability,
        raw_store: RawEvidenceStore,
        representation_registry: SecRepresentationRegistry,
        identities: ProviderIdentityRegistry,
        parser_limits: SecParserLimits,
    ) -> Result<Self, SecClientError> {
        if metadata.source_id() != registered.source_id()
            || metadata.revision() != registered.revision()
            || metadata.source_class() != market_squawk_sources::SourceClass::RegulatoryFiling
            || !metadata.capabilities().extraction()
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
        {
            return Err(SecClientError::RegistrationMismatch);
        }
        let endpoint_policy = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.clone(),
            NetworkAccessPolicy::Denied => return Err(SecClientError::NetworkDenied),
        };
        let budget = registered
            .budget()
            .cloned()
            .ok_or(SecClientError::MissingSharedBudget)?;
        let budget_policy = metadata
            .budget_policy()
            .ok_or(SecClientError::MissingSharedBudget)?;
        if budget_policy.requests_per_window() > SEC_REQUEST_CEILING_PER_SECOND
            || budget_policy.window_nanos() < ONE_SECOND_NANOS
            || budget_policy.max_concurrent() > SEC_REQUEST_CEILING_PER_SECOND as u16
        {
            return Err(SecClientError::UnsafeBudgetPolicy);
        }
        let profile = endpoint_policy.client_profile();
        if !profile.automatic_redirects_disabled()
            || !profile.ambient_system_proxy_disabled()
            || !profile.implicit_retries_disabled()
            || !profile.counts_post_decompression_bytes()
        {
            return Err(SecClientError::UnsafeClientProfile);
        }
        for required in [
            SecObjectLocator::submissions("0")?,
            SecObjectLocator::company_facts("0")?,
            SecObjectLocator::companion("CIK0000000000-submissions-001.json")?,
            SecObjectLocator::filing_document("0", "0000000000-00-000000", "filing.xml")?,
        ] {
            endpoint_policy.authorize_request(required.url())?;
        }
        let _consumed_provider_identity = tls_provider.provider_id();
        let bounds = endpoint_policy.request_bounds();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .connect_timeout(Duration::from_nanos(bounds.connect_timeout_nanos()))
            .read_timeout(Duration::from_nanos(bounds.read_timeout_nanos()))
            .timeout(Duration::from_nanos(bounds.total_timeout_nanos()))
            .user_agent(contact.user_agent())
            .build()?;
        let blocking_workers =
            usize::from(budget_policy.max_concurrent()).min(MAX_BLOCKING_WORKERS);
        Ok(Self {
            metadata,
            registered,
            endpoint_policy,
            budget,
            client,
            raw_store: Arc::new(raw_store),
            representation_registry: Arc::new(representation_registry),
            identities: Arc::new(identities),
            blocking_admission: Arc::new(Semaphore::new(blocking_workers)),
            extraction_health: Mutex::new(SecExtractionHealth {
                state: SecExtractionHealthState::Ready,
                observed_at: system_timestamp()?,
                http_status: None,
            }),
            parser_limits,
        })
    }

    /// Returns current extraction health without exposing credentials or response content.
    pub fn extraction_health(&self) -> Result<SecExtractionHealth, SecClientError> {
        self.extraction_health
            .lock()
            .map(|health| *health)
            .map_err(|_| SecClientError::HealthStatePoisoned)
    }

    /// Retrieves and parses current submissions without inventing publication time.
    pub async fn fetch_submissions(
        &self,
        cik: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedSubmissions, SecClientError> {
        let raw = self
            .retrieve(&SecObjectLocator::submissions(cik)?, &cancellation)
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let document = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                SubmissionsDocument::parse_with_cancellation(&bytes, limits, worker_cancellation)
                    .map_err(Into::into)
            })
            .await?;
        Ok(RetrievedSubmissions::new(document, raw, Vec::new()))
    }

    /// Retrieves and parses one submissions companion file.
    pub async fn fetch_submissions_archive(
        &self,
        name: &str,
        cancellation: CancellationToken,
    ) -> Result<(SubmissionsArchive, RetrievedSecBytes), SecClientError> {
        let raw = self
            .retrieve(&SecObjectLocator::companion(name)?, &cancellation)
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let document = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                SubmissionsDocument::parse_archive_with_cancellation(
                    &bytes,
                    limits,
                    worker_cancellation,
                )
                .map_err(Into::into)
            })
            .await?;
        Ok((document, raw))
    }

    /// Retrieves and parses current SEC Company Facts.
    pub async fn fetch_company_facts(
        &self,
        cik: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedCompanyFacts, SecClientError> {
        let raw = self
            .retrieve(&SecObjectLocator::company_facts(cik)?, &cancellation)
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let document = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                CompanyFactsDocument::parse_with_cancellation(&bytes, limits, worker_cancellation)
                    .map_err(Into::into)
            })
            .await?;
        Ok(RetrievedCompanyFacts { document, raw })
    }

    /// Retrieves and persists one validated filing document for bounded XBRL parsing.
    pub async fn fetch_filing_document(
        &self,
        cik: &str,
        accession: &str,
        document: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        self.retrieve(
            &SecObjectLocator::filing_document(cik, accession, document)?,
            &cancellation,
        )
        .await
    }

    /// Retrieves, persists, and parses one filing XBRL or Inline-XBRL document.
    pub async fn fetch_xbrl_document(
        &self,
        cik: &str,
        accession: &str,
        document: &str,
        taxonomy_set: market_squawk_domain::XbrlTaxonomySet,
        cancellation: CancellationToken,
    ) -> Result<RetrievedXbrlDocument, SecClientError> {
        let raw = self
            .fetch_filing_document(cik, accession, document, cancellation)
            .await?;
        let parsed = XbrlDocumentParser::parse(
            raw.bytes(),
            self.parser_limits,
            XbrlDocumentContext::new(
                market_squawk_domain::SourceIdentifier::try_from(accession)?,
                taxonomy_set,
                market_squawk_domain::ExactPayloadEvidence::from_content_digest(raw.evidence()),
                raw.received_at(),
            ),
        )?;
        Ok(RetrievedXbrlDocument {
            document: parsed,
            raw,
        })
    }

    async fn retrieve(
        &self,
        locator: &SecObjectLocator,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        let now = system_timestamp()?;
        if !self.metadata.is_effective_at(now)
            || self.registered.source_id() != self.metadata.source_id()
            || self.registered.revision() != self.metadata.revision()
        {
            return Err(SecClientError::InactiveAuthority);
        }
        let mut current = locator.url().to_owned();
        let mut redirects = 0_u8;
        let mut retries = 0_usize;
        loop {
            self.endpoint_policy.authorize_request(&current)?;
            let conditional = self.representation_registry.conditional_request(&current)?;
            let permit = loop {
                match self.budget.try_acquire() {
                    BudgetDecision::Ready(permit) => break permit,
                    BudgetDecision::WaitUntil(deadline) => {
                        let wait = self.budget.remaining_wait(deadline)?;
                        tokio::select! {
                            () = tokio::time::sleep(wait) => {}
                            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
                        }
                    }
                    BudgetDecision::Unavailable(reason) => {
                        return Err(SecClientError::BudgetUnavailable(reason));
                    }
                }
            };
            let mut request = self.client.get(&current);
            if let Some(validators) = conditional {
                if let Some(etag) = validators.etag() {
                    request = request.header(IF_NONE_MATCH, etag);
                }
                if let Some(last_modified) = validators.last_modified() {
                    request = request.header(IF_MODIFIED_SINCE, last_modified);
                }
            }
            let response = tokio::select! {
                response = request.send() => match response {
                    Ok(response) => response,
                    Err(error) => {
                        self.update_health(SecExtractionHealthState::ProviderUnavailable, None)?;
                        return Err(error.into());
                    }
                },
                () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
            };
            let status = response.status();
            if status.is_redirection() && status.as_u16() != 304 {
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(SecClientError::InvalidRedirect)?;
                let target = Url::parse(&current)
                    .and_then(|base| base.join(location))
                    .map_err(|_| SecClientError::InvalidRedirect)?
                    .to_string();
                self.endpoint_policy
                    .authorize_redirect_from(&current, &target, false)?;
                redirects = redirects
                    .checked_add(1)
                    .ok_or(SecClientError::RedirectLimitExceeded)?;
                if redirects > self.endpoint_policy.request_bounds().max_redirects() {
                    return Err(SecClientError::RedirectLimitExceeded);
                }
                permit.release();
                current = target;
                continue;
            }
            if status.as_u16() == 429 || status.as_u16() == 503 {
                self.update_health(
                    health_for_http_status(status.as_u16()),
                    Some(status.as_u16()),
                )?;
                let retry_after = response
                    .headers()
                    .get(RETRY_AFTER)
                    .map(|value| value.as_bytes());
                permit.release();
                if retries >= MAX_EXPLICIT_RETRIES {
                    return Err(SecClientError::RetryLimitExceeded);
                }
                retries += 1;
                match apply_http_retry_after(&self.budget, retry_after, 5_000) {
                    BudgetDecision::Ready(permit) => permit.release(),
                    BudgetDecision::WaitUntil(deadline) => {
                        let wait = self.budget.remaining_wait(deadline)?;
                        tokio::select! {
                            () = tokio::time::sleep(wait) => {}
                            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
                        }
                    }
                    BudgetDecision::Unavailable(reason) => {
                        return Err(SecClientError::BudgetUnavailable(reason));
                    }
                }
                continue;
            }
            if status.as_u16() == 304 {
                let validators = self.response_validators(response.headers())?;
                permit.release();
                self.budget.record_success()?;
                let raw_store = Arc::clone(&self.raw_store);
                let representations = Arc::clone(&self.representation_registry);
                let retained_locator = current.clone();
                let max_bytes = self.endpoint_policy.request_bounds().max_response_bytes();
                let retrieved = self
                    .run_blocking(cancellation, move |worker_cancellation| {
                        let retained = representations.record_not_modified_cancellable(
                            &retained_locator,
                            validators,
                            worker_cancellation,
                        )?;
                        let bytes = raw_store.read_verified_bounded_cancellable(
                            &retained.evidence(),
                            max_bytes.min(retained.size_bytes()),
                            worker_cancellation,
                        )?;
                        if u64::try_from(bytes.len()).ok() != Some(retained.size_bytes()) {
                            return Err(SecClientError::RawEvidenceMismatch);
                        }
                        Ok(retrieved_from_representation(bytes, retained))
                    })
                    .await;
                return self.finish_local_retrieval(retrieved);
            }
            if !status.is_success() {
                let health = health_for_http_status(status.as_u16());
                self.update_health(health, Some(status.as_u16()))?;
                return Err(SecClientError::HttpStatus(status.as_u16()));
            }
            let validators = self.response_validators(response.headers())?;
            if let Some(length) = response.content_length() {
                self.endpoint_policy.validate_response_size(length)?;
            }
            let read_timeout =
                Duration::from_nanos(self.endpoint_policy.request_bounds().read_timeout_nanos());
            let max_bytes = self.endpoint_policy.request_bounds().max_response_bytes();
            let initial_capacity = response
                .content_length()
                .and_then(|size| usize::try_from(size).ok())
                .unwrap_or(0)
                .min(1024 * 1024);
            let mut bytes = Vec::new();
            bytes
                .try_reserve(initial_capacity)
                .map_err(|_| SecClientError::AllocationFailed)?;
            let mut stream = response.bytes_stream();
            loop {
                let next = tokio::select! {
                    result = tokio::time::timeout(read_timeout, stream.next()) => {
                        match result {
                            Ok(next) => next,
                            Err(_) => {
                                self.update_health(
                                    SecExtractionHealthState::ProviderUnavailable,
                                    None,
                                )?;
                                return Err(SecClientError::ReadTimeout);
                            }
                        }
                    }
                    () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
                };
                let Some(chunk) = next else { break };
                let chunk = match chunk {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        self.update_health(SecExtractionHealthState::ProviderUnavailable, None)?;
                        return Err(error.into());
                    }
                };
                let new_len = bytes
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(SecClientError::ResponseTooLarge)?;
                if u64::try_from(new_len).map_err(|_| SecClientError::ResponseTooLarge)? > max_bytes
                {
                    return Err(SecClientError::ResponseTooLarge);
                }
                bytes
                    .try_reserve(chunk.len())
                    .map_err(|_| SecClientError::AllocationFailed)?;
                bytes.extend_from_slice(&chunk);
            }
            permit.release();
            self.budget.record_success()?;
            let raw_store = Arc::clone(&self.raw_store);
            let representations = Arc::clone(&self.representation_registry);
            let retained_locator = current.clone();
            let retrieved = self
                .run_blocking(cancellation, move |worker_cancellation| {
                    let evidence = raw_store.persist_cancellable(&bytes, worker_cancellation)?;
                    let computed: [u8; 32] = Sha256::digest(&bytes).into();
                    if evidence != EvidenceDigest::new(DigestAlgorithm::Sha256, computed) {
                        return Err(SecClientError::RawEvidenceMismatch);
                    }
                    let size_bytes =
                        u64::try_from(bytes.len()).map_err(|_| SecClientError::ResponseTooLarge)?;
                    let retained = representations.record_success_cancellable(
                        &retained_locator,
                        evidence,
                        size_bytes,
                        validators,
                        worker_cancellation,
                    )?;
                    Ok(retrieved_from_representation(bytes, retained))
                })
                .await;
            return self.finish_local_retrieval(retrieved);
        }
    }

    pub(crate) async fn run_blocking<T, F>(
        &self,
        cancellation: &CancellationToken,
        work: F,
    ) -> Result<T, SecClientError>
    where
        T: Send + 'static,
        F: FnOnce(&CancellationToken) -> Result<T, SecClientError> + Send + 'static,
    {
        let admission = Arc::clone(&self.blocking_admission);
        let permit = tokio::select! {
            permit = admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
        };
        let worker_cancellation = cancellation.child_token();
        let worker_token = worker_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work(&worker_token)
        });
        tokio::select! {
            result = &mut worker => result.map_err(|_| SecClientError::BlockingWorkerFailed)?,
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                Err(SecClientError::Cancelled)
            }
        }
    }

    pub(crate) async fn run_validation_blocking<T, F>(
        &self,
        cancellation: &CancellationToken,
        work: F,
    ) -> Result<T, SecClientError>
    where
        T: Send + 'static,
        F: FnOnce(&CancellationToken) -> Result<T, SecClientError> + Send + 'static,
    {
        let result = self.run_blocking(cancellation, work).await;
        self.finish_validation(result)
    }

    fn finish_validation<T>(&self, result: Result<T, SecClientError>) -> Result<T, SecClientError> {
        match result {
            Ok(value) => {
                self.update_health(SecExtractionHealthState::Ready, None)?;
                Ok(value)
            }
            Err(error) => {
                if let Some(state) = validation_health_for_error(&error) {
                    self.update_health(state, None)?;
                }
                Err(error)
            }
        }
    }

    fn finish_local_retrieval(
        &self,
        result: Result<RetrievedSecBytes, SecClientError>,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        match result {
            Ok(retrieved) => {
                self.update_health(SecExtractionHealthState::Ready, None)?;
                Ok(retrieved)
            }
            Err(SecClientError::Cancelled) => Err(SecClientError::Cancelled),
            Err(error) => {
                self.update_health(SecExtractionHealthState::LocalFailure, None)?;
                Err(error)
            }
        }
    }

    fn update_health(
        &self,
        state: SecExtractionHealthState,
        http_status: Option<u16>,
    ) -> Result<(), SecClientError> {
        let mut health = self
            .extraction_health
            .lock()
            .map_err(|_| SecClientError::HealthStatePoisoned)?;
        *health = SecExtractionHealth {
            state,
            observed_at: system_timestamp()?,
            http_status,
        };
        Ok(())
    }

    fn response_validators(
        &self,
        headers: &reqwest::header::HeaderMap,
    ) -> Result<SecHttpValidators, SecClientError> {
        match response_validators(headers) {
            Ok(validators) => Ok(validators),
            Err(error) => {
                self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
                Err(error)
            }
        }
    }

    pub(crate) fn raw_store(&self) -> Arc<RawEvidenceStore> {
        Arc::clone(&self.raw_store)
    }

    pub(crate) fn identity_registry(&self) -> Arc<ProviderIdentityRegistry> {
        Arc::clone(&self.identities)
    }

    pub(crate) const fn parser_limits(&self) -> SecParserLimits {
        self.parser_limits
    }
}

impl SourceMetadataProvider for SecEdgarSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

fn response_validators(
    headers: &reqwest::header::HeaderMap,
) -> Result<SecHttpValidators, SecClientError> {
    let etag = headers
        .get(ETAG)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| SecClientError::InvalidValidatorHeader)?;
    let last_modified = headers
        .get(LAST_MODIFIED)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| SecClientError::InvalidValidatorHeader)?;
    SecHttpValidators::try_new(etag, last_modified).map_err(Into::into)
}

fn retrieved_from_representation(
    bytes: Vec<u8>,
    representation: SecRepresentation,
) -> RetrievedSecBytes {
    RetrievedSecBytes {
        bytes: Bytes::from(bytes),
        evidence: representation.evidence(),
        received_at: representation.first_observed_at(),
        availability: AvailabilityEvidence::local_first_observed(
            representation.first_observed_at(),
        ),
        locator: Some(representation.locator().to_owned()),
        retrieval_revision: Some(representation.retrieval_revision()),
    }
}
