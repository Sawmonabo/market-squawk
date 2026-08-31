//! Registered, allowlisted, budgeted SEC HTTP retrieval.

mod contracts;
mod taxonomy;

pub use contracts::{
    RetrievedCompanyFacts, RetrievedSecBytes, RetrievedSubmissions, RetrievedXbrlDocument,
    SecClientError, SecContact, SecExtractionHealth, SecExtractionHealthState, SecObjectLocator,
};
pub(crate) use contracts::{deterministic_capture_uuid, system_timestamp};
use contracts::{health_for_http_status, validation_health_for_error};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::StreamExt as _;
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ProviderIdentityRegistry, ProviderInstrumentId, SourceId,
    SourceIdentifier, Timestamp,
};
use market_squawk_platform::{
    ResearchObjectControl, ResearchObjectControlError, ResearchObjectControlPoint,
    SealedResearchJournalStore,
};
use market_squawk_sources::{
    AuthorizationMode, ExtractionAuthority, ExtractionAuthorityError, ExtractionRedirectPermit,
    HttpRequestBounds, MAX_PROVIDER_CAPTURE_PAGE_BYTES, NetworkAccessPolicy,
    ProviderCapturePageReceipt, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    SourceMetadata, SourceMetadataProvider, TlsProviderCapability,
};
use reqwest::header::{
    ACCEPT_ENCODING, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, LOCATION,
    RETRY_AFTER,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    CompanyFactsDocument, RawEvidenceStore, SEC_APPLICATION_MAX_CONCURRENT_REQUESTS,
    SEC_APPLICATION_REQUESTS_PER_SECOND, SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND,
    SEC_PROVIDER_RATE_SCOPE, SecAuthoritativeIdentifierNamespace, SecBulkCapture, SecBulkCoverage,
    SecBulkDoctorReport, SecBulkDoctorState, SecBulkFamily, SecBulkLayoutManifest,
    SecBulkMediaKind, SecBulkParseLimits, SecBulkSelection, SecBulkTransportEvidence,
    SecFundIdentityAuthority, SecFundPartitionAdmissions, SecFundPendingLogicalRows,
    SecFundPublicationScope, SecGovernedIdentityReceipt, SecHttpValidators, SecParserLimits,
    SecPendingBulkLogicalPublication, SecPreparedFundLogicalPublication, SecRepresentation,
    SecRepresentationRegistry, SubmissionsArchive, SubmissionsDocument, XbrlDocumentContext,
    XbrlDocumentParser, inspect_bulk_archive, recover_bulk_archive,
};

const ONE_SECOND_NANOS: u64 = 1_000_000_000;
const MAX_BLOCKING_WORKERS: usize = 4;
const MAX_NPORT_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_NCEN_ARCHIVE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_BULK_README_BYTES: u64 = 16 * 1024 * 1024;
const STREAM_CHANNEL_CHUNKS: usize = 8;

/// Production SEC source bound to exact metadata and local persistence.
#[derive(Debug)]
pub struct SecEdgarSource {
    metadata: SourceMetadata,
    client: reqwest::Client,
    raw_store: Arc<RawEvidenceStore>,
    representation_registry: Arc<SecRepresentationRegistry>,
    identities: Arc<ProviderIdentityRegistry>,
    blocking_admission: Arc<Semaphore>,
    extraction_health: Mutex<SecExtractionHealth>,
    parser_limits: SecParserLimits,
    taxonomy_clients: taxonomy::TaxonomyClientSet,
}

impl SecEdgarSource {
    /// Constructs a source from exact metadata and an installed TLS capability.
    ///
    /// Runtime discovery and extraction remain impossible without a matching registry-minted
    /// [`ExtractionAuthority`]. The source deliberately retains no registration handle, endpoint
    /// authorization, or provider-budget capability that could substitute for that authority.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is distinct metadata, identity, persistence, or parsing state"
    )]
    pub fn try_new(
        metadata: SourceMetadata,
        contact: SecContact,
        tls_provider: TlsProviderCapability,
        raw_store: RawEvidenceStore,
        representation_registry: SecRepresentationRegistry,
        identities: ProviderIdentityRegistry,
        parser_limits: SecParserLimits,
    ) -> Result<Self, SecClientError> {
        if metadata.source_class() != market_squawk_sources::SourceClass::RegulatoryFiling
            || !metadata.capabilities().extraction()
            || metadata.authorization().mode() != AuthorizationMode::PublicInterface
        {
            return Err(SecClientError::RegistrationMismatch);
        }
        let endpoint_policy = match metadata.network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => policy.clone(),
            NetworkAccessPolicy::Denied => return Err(SecClientError::NetworkDenied),
        };
        let budget_policy = metadata
            .budget_policy()
            .ok_or(SecClientError::MissingSharedBudget)?;
        if budget_policy.requests_per_window() != SEC_APPLICATION_REQUESTS_PER_SECOND
            || budget_policy.requests_per_window() > SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND
            || budget_policy.window_nanos() != ONE_SECOND_NANOS
            || budget_policy.window_count() != 1
            || budget_policy.max_concurrent() != SEC_APPLICATION_MAX_CONCURRENT_REQUESTS
            || budget_policy.scope().as_source_identifier().as_str() != SEC_PROVIDER_RATE_SCOPE
            || budget_policy.scope().authorization_account().is_some()
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
            SecObjectLocator::bulk_submissions()?,
            SecObjectLocator::bulk_company_facts()?,
            SecObjectLocator::quarterly_bulk_archive(
                crate::SecBulkFamily::Nport,
                crate::SecQuarter::try_new(2026, 2).map_err(|_| SecClientError::InvalidLocator)?,
            )?,
            SecObjectLocator::quarterly_bulk_archive(
                crate::SecBulkFamily::Ncen,
                crate::SecQuarter::try_new(2026, 2).map_err(|_| SecClientError::InvalidLocator)?,
            )?,
            SecObjectLocator::quarterly_bulk_readme(crate::SecBulkFamily::Nport)?,
            SecObjectLocator::quarterly_bulk_readme(crate::SecBulkFamily::Ncen)?,
        ] {
            endpoint_policy.authorize_request(required.url())?;
        }
        let _consumed_provider_identity = tls_provider.provider_id();
        let taxonomy_clients = taxonomy::TaxonomyClientSet::try_new(&contact)?;
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
            taxonomy_clients,
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
        authority: &ExtractionAuthority,
        cik: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedSubmissions, SecClientError> {
        self.validate_authority(authority)?;
        let raw = self
            .retrieve(
                authority,
                &SecObjectLocator::submissions(cik)?,
                &cancellation,
            )
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let document = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                SubmissionsDocument::parse_with_cancellation(&bytes, limits, worker_cancellation)
                    .map_err(Into::into)
            })
            .await?;
        self.validate_authority(authority)?;
        Ok(RetrievedSubmissions::new(document, raw, Vec::new()))
    }

    /// Retrieves and parses one submissions companion file.
    pub async fn fetch_submissions_archive(
        &self,
        authority: &ExtractionAuthority,
        name: &str,
        cancellation: CancellationToken,
    ) -> Result<(SubmissionsArchive, RetrievedSecBytes), SecClientError> {
        self.validate_authority(authority)?;
        let raw = self
            .retrieve(
                authority,
                &SecObjectLocator::companion(name)?,
                &cancellation,
            )
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
        self.validate_authority(authority)?;
        Ok((document, raw))
    }

    /// Retrieves and parses current SEC Company Facts.
    pub async fn fetch_company_facts(
        &self,
        authority: &ExtractionAuthority,
        cik: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedCompanyFacts, SecClientError> {
        self.validate_authority(authority)?;
        let raw = self
            .retrieve(
                authority,
                &SecObjectLocator::company_facts(cik)?,
                &cancellation,
            )
            .await?;
        let bytes = raw.bytes().clone();
        let limits = self.parser_limits;
        let document = self
            .run_validation_blocking(&cancellation, move |worker_cancellation| {
                CompanyFactsDocument::parse_with_cancellation(&bytes, limits, worker_cancellation)
                    .map_err(Into::into)
            })
            .await?;
        self.validate_authority(authority)?;
        Ok(RetrievedCompanyFacts { document, raw })
    }

    /// Retrieves and persists one validated filing document for bounded XBRL parsing.
    pub async fn fetch_filing_document(
        &self,
        authority: &ExtractionAuthority,
        cik: &str,
        accession: &str,
        document: &str,
        cancellation: CancellationToken,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        self.validate_authority(authority)?;
        self.retrieve(
            authority,
            &SecObjectLocator::filing_document(cik, accession, document)?,
            &cancellation,
        )
        .await
    }

    /// Retrieves, persists, and parses one filing XBRL or Inline-XBRL document.
    pub async fn fetch_xbrl_document(
        &self,
        authority: &ExtractionAuthority,
        cik: &str,
        accession: &str,
        document: &str,
        taxonomy_set: market_squawk_domain::XbrlTaxonomySet,
        cancellation: CancellationToken,
    ) -> Result<RetrievedXbrlDocument, SecClientError> {
        self.validate_authority(authority)?;
        let raw = self
            .fetch_filing_document(authority, cik, accession, document, cancellation.clone())
            .await?;
        let parsed = XbrlDocumentParser::parse_with_cancellation(
            raw.bytes(),
            self.parser_limits,
            XbrlDocumentContext::new(
                market_squawk_domain::SourceIdentifier::try_from(accession)?,
                taxonomy_set,
                market_squawk_domain::ExactPayloadEvidence::from_content_digest(raw.evidence()),
                raw.received_at(),
            ),
            &cancellation,
        )?;
        self.validate_authority(authority)?;
        Ok(RetrievedXbrlDocument {
            document: parsed,
            raw,
        })
    }

    /// Streams one exact quarterly archive directly into the immutable raw store.
    ///
    /// Redirects are rejected for bulk artifacts so family/quarter origin cannot drift. The
    /// bounded channel applies backpressure between transport and the admitted blocking writer;
    /// neither the compressed archive nor any decoded table is assembled in memory.
    pub async fn fetch_bulk_archive(
        &self,
        authority: &ExtractionAuthority,
        selection: &SecBulkSelection,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecBulkCapture, SecClientError> {
        let maximum = match selection.family() {
            SecBulkFamily::Nport => MAX_NPORT_ARCHIVE_BYTES,
            SecBulkFamily::Ncen => MAX_NCEN_ARCHIVE_BYTES,
        };
        self.retrieve_streamed_bulk(
            authority,
            selection,
            SecObjectLocator::quarterly_bulk_archive(selection.family(), selection.quarter())?,
            maximum,
            deadline,
            &cancellation,
        )
        .await
    }

    /// Streams and seals the exact official PDF readme paired with a quarterly archive.
    pub async fn fetch_bulk_readme(
        &self,
        authority: &ExtractionAuthority,
        selection: &SecBulkSelection,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecBulkCapture, SecClientError> {
        self.retrieve_streamed_bulk(
            authority,
            selection,
            SecObjectLocator::quarterly_bulk_readme(selection.family())?,
            MAX_BULK_README_BYTES,
            deadline,
            &cancellation,
        )
        .await
    }

    /// Retrieves, seals, reopens, and inspects one exact quarterly generation under one absolute
    /// end-to-end deadline. A timeout cancels both transport and admitted blocking validation.
    pub async fn fetch_and_inspect_bulk(
        &self,
        authority: &ExtractionAuthority,
        selection: &SecBulkSelection,
        limits: SecBulkParseLimits,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecBulkLayoutManifest, SecClientError> {
        let remaining = remaining_until(deadline)?;
        let operation_cancellation = cancellation.child_token();
        let operation_token = operation_cancellation.clone();
        tokio::select! {
            result = async {
                let archive = self.fetch_bulk_archive(
                    authority,
                    selection,
                    deadline,
                    operation_token.clone(),
                ).await?;
                let readme = self.fetch_bulk_readme(
                    authority,
                    selection,
                    deadline,
                    operation_token.clone(),
                ).await?;
                ensure_before_deadline(deadline)?;
                let store = Arc::clone(&self.raw_store);
                self.run_blocking(&operation_token, move |worker_cancellation| {
                    inspect_bulk_archive(
                        &store,
                        archive,
                        readme,
                        limits,
                        deadline,
                        worker_cancellation,
                    )
                    .map_err(|_| SecClientError::InvalidCaptureMaterial)
                })
                .await
            } => {
                ensure_before_deadline(deadline)?;
                result
            }
            () = tokio::time::sleep(remaining) => {
                operation_cancellation.cancel();
                Err(SecClientError::DeadlineExceeded)
            }
            () = cancellation.cancelled() => {
                operation_cancellation.cancel();
                Err(SecClientError::Cancelled)
            }
        }
    }

    /// Reopens one captured quarterly graph and prepares one bounded fund publication.
    ///
    /// Heavy raw copying, complete archive verification, identity mapping, and native/row-map
    /// sealing run under this source's existing blocking-work admission. The source's private raw
    /// store is never exposed: the exact archive and official readme are copied into the caller's
    /// application-owned journal, re-read through EOF, and consumed by the existing code-owned
    /// canonical preparation path.
    #[allow(
        clippy::too_many_arguments,
        reason = "captured layout, fund scope, physical bounds, clocks, identity, and application storage are independent authorities"
    )]
    pub async fn prepare_fund_logical_publication<A>(
        &self,
        manifest: SecBulkLayoutManifest,
        scope: SecFundPublicationScope,
        limits: SecBulkParseLimits,
        admissions: SecFundPartitionAdmissions,
        ingested_at: Timestamp,
        deadline: Timestamp,
        identity_authority: A,
        journal: Arc<SealedResearchJournalStore>,
        cancellation: CancellationToken,
    ) -> Result<SecPreparedFundLogicalPublication, crate::SecBulkError>
    where
        A: SecFundIdentityAuthority + Send + 'static,
    {
        ensure_before_deadline(deadline)?;
        validate_fund_preparation_request(&manifest, &scope, ingested_at, deadline)?;
        let admission_remaining = remaining_until(deadline)?;
        let admission = Arc::clone(&self.blocking_admission);
        let permit = tokio::select! {
            permit = admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => return Err(crate::SecBulkError::Cancelled),
            () = tokio::time::sleep(admission_remaining) => {
                return Err(crate::SecBulkError::DeadlineExceeded);
            }
        };
        let remaining = remaining_until(deadline)?;
        let worker_cancellation = cancellation.child_token();
        let worker_token = worker_cancellation.clone();
        let raw_store = Arc::clone(&self.raw_store);
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            prepare_fund_from_captured_graph(
                &raw_store,
                manifest,
                scope,
                limits,
                admissions,
                ingested_at,
                deadline,
                identity_authority,
                &journal,
                &worker_token,
            )
        });
        tokio::select! {
            result = &mut worker => {
                result
                    .map_err(|_| crate::SecBulkError::Client(SecClientError::BlockingWorkerFailed))?
            }
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                Err(crate::SecBulkError::Cancelled)
            }
            () = tokio::time::sleep(remaining) => {
                worker_cancellation.cancel();
                Err(crate::SecBulkError::DeadlineExceeded)
            }
        }
    }

    /// Produces secret-free root-activation evidence after exact capture and layout inspection.
    pub async fn doctor_bulk(
        &self,
        authority: &ExtractionAuthority,
        selection: &SecBulkSelection,
        manifest: Option<&SecBulkLayoutManifest>,
        limits: SecBulkParseLimits,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<SecBulkDoctorReport, SecClientError> {
        ensure_before_deadline(deadline)?;
        self.validate_authority(authority)?;
        let observed_at = system_timestamp()?;
        ensure_before_deadline(deadline)?;
        let Some(manifest) = manifest else {
            return Ok(SecBulkDoctorReport::new(
                selection.clone(),
                SecBulkDoctorState::InvalidEvidence,
                observed_at,
                None,
            ));
        };
        if manifest.capture().selection() != selection
            || manifest.official_readme_capture().selection() != selection
            || observed_at < manifest.capture().first_observed_at()
            || observed_at < manifest.official_readme_capture().first_observed_at()
        {
            return Ok(SecBulkDoctorReport::new(
                selection.clone(),
                SecBulkDoctorState::InvalidEvidence,
                observed_at,
                None,
            ));
        }
        let request_bounds = self.request_bounds(authority)?;
        if manifest.capture().size_bytes() > request_bounds.max_response_bytes()
            || manifest.official_readme_capture().size_bytes() > request_bounds.max_response_bytes()
        {
            return Ok(SecBulkDoctorReport::new(
                selection.clone(),
                SecBulkDoctorState::RequestBoundsInsufficient,
                observed_at,
                None,
            ));
        }
        let expected = manifest.clone();
        let store = Arc::clone(&self.raw_store);
        let operation_cancellation = cancellation.child_token();
        let operation_token = operation_cancellation.clone();
        let remaining = remaining_until(deadline)?;
        let recovered = tokio::select! {
            result = self.run_blocking(&operation_token, move |worker_cancellation| {
                match recover_bulk_archive(
                    &store,
                    &expected,
                    limits,
                    deadline,
                    worker_cancellation,
                ) {
                    Ok(recovered) => Ok(Some(recovered)),
                    Err(crate::SecBulkError::DeadlineExceeded)
                    | Err(crate::SecBulkError::RawEvidence(
                        crate::RawEvidenceError::DeadlineExceeded,
                    )) => Err(SecClientError::DeadlineExceeded),
                    Err(crate::SecBulkError::Cancelled)
                    | Err(crate::SecBulkError::RawEvidence(crate::RawEvidenceError::Cancelled)) => {
                        Err(SecClientError::Cancelled)
                    }
                    Err(_) => Ok(None),
                }
            }) => result?,
            () = tokio::time::sleep(remaining) => {
                operation_cancellation.cancel();
                return Err(SecClientError::DeadlineExceeded);
            }
            () = cancellation.cancelled() => {
                operation_cancellation.cancel();
                return Err(SecClientError::Cancelled);
            }
        };
        ensure_before_deadline(deadline)?;
        self.validate_authority(authority)?;
        let observed_at = system_timestamp()?;
        ensure_before_deadline(deadline)?;
        let Some(recovered) = recovered else {
            return Ok(SecBulkDoctorReport::new(
                selection.clone(),
                SecBulkDoctorState::InvalidEvidence,
                observed_at,
                None,
            ));
        };
        let state = match selection.coverage() {
            SecBulkCoverage::DerivedAsFiledIncludingAmendments => SecBulkDoctorState::Ready,
            SecBulkCoverage::AcceptedSchemaExcluded { .. } => {
                SecBulkDoctorState::ReadyWithDeclaredCoverageGap
            }
        };
        Ok(SecBulkDoctorReport::new(
            selection.clone(),
            state,
            observed_at,
            Some(&recovered),
        ))
    }

    async fn retrieve_streamed_bulk(
        &self,
        authority: &ExtractionAuthority,
        selection: &SecBulkSelection,
        locator: SecObjectLocator,
        provider_local_maximum: u64,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<SecBulkCapture, SecClientError> {
        let deadline_wait = tokio::time::sleep(remaining_until(deadline)?);
        tokio::pin!(deadline_wait);
        ensure_before_deadline(deadline)?;
        self.validate_authority(authority)?;
        let request_bounds = self.request_bounds(authority)?;
        let effective_maximum = request_bounds
            .max_response_bytes()
            .min(provider_local_maximum);
        if effective_maximum == 0 {
            return Err(SecClientError::ResponseTooLarge);
        }
        let current = locator.url().to_owned();
        let media_kind = if current == selection.archive_locator().as_str() {
            SecBulkMediaKind::Zip
        } else if current == selection.readme_locator().as_str() {
            SecBulkMediaKind::Pdf
        } else {
            return Err(SecClientError::InvalidLocator);
        };
        let in_flight = authority
            .try_network_request(&current)?
            .authorize_send(&current)?;
        let response = tokio::select! {
            response = self.client.get(&current).header(ACCEPT_ENCODING, "identity").send() => {
                match response {
                    Ok(response) => response,
                    Err(error) => {
                        self.update_health(SecExtractionHealthState::ProviderUnavailable, None)?;
                        return Err(error.into());
                    }
                }
            }
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
            () = &mut deadline_wait => return Err(SecClientError::DeadlineExceeded),
        };
        in_flight.validate_current()?;
        let status = response.status();
        if status.is_redirection() {
            drop(response);
            in_flight.release();
            self.update_health(
                SecExtractionHealthState::InvalidResponse,
                Some(status.as_u16()),
            )?;
            return Err(SecClientError::InvalidRedirect);
        }
        if status.as_u16() == 429 || status.as_u16() == 503 {
            self.update_health(
                health_for_http_status(status.as_u16()),
                Some(status.as_u16()),
            )?;
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .map(|value| value.as_bytes().to_vec());
            drop(response);
            let deadline = in_flight.apply_retry_after_header(retry_after.as_deref(), 5_000)?;
            return Err(SecClientError::Authority(
                ExtractionAuthorityError::BudgetWaitUntil { deadline },
            ));
        }
        if !status.is_success() {
            self.update_health(
                health_for_http_status(status.as_u16()),
                Some(status.as_u16()),
            )?;
            drop(response);
            in_flight.release();
            return Err(SecClientError::HttpStatus(status.as_u16()));
        }
        let validators = self.response_validators(response.headers())?;
        let transport_validators = validators.clone();
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .map(|value| value.to_str())
            .transpose()
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?
            .map(str::to_owned);
        let expected_bytes = response.content_length();
        if let Some(length) = expected_bytes {
            in_flight.validate_response_size(length)?;
            if length == 0 || length > effective_maximum {
                self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
                return Err(SecClientError::ResponseTooLarge);
            }
        }
        let admission = Arc::clone(&self.blocking_admission);
        let permit = tokio::select! {
            permit = admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
            () = &mut deadline_wait => return Err(SecClientError::DeadlineExceeded),
        };
        let (sender, receiver) = mpsc::channel(STREAM_CHANNEL_CHUNKS);
        let raw_store = Arc::clone(&self.raw_store);
        let representations = Arc::clone(&self.representation_registry);
        let retained_locator = current.clone();
        let worker_cancellation = cancellation.child_token();
        let worker_token = worker_cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let receipt = raw_store.persist_stream_receiver(
                receiver,
                expected_bytes,
                effective_maximum,
                &worker_token,
            )?;
            representations
                .record_success_cancellable(
                    &retained_locator,
                    receipt.evidence(),
                    receipt.size_bytes(),
                    validators,
                    &worker_token,
                )
                .map_err(Into::into)
        });
        let read_timeout = Duration::from_nanos(request_bounds.read_timeout_nanos());
        let mut observed = 0_u64;
        let mut stream = response.bytes_stream();
        loop {
            in_flight.validate_current()?;
            let next = tokio::select! {
                result = tokio::time::timeout(read_timeout, stream.next()) => {
                    match result {
                        Ok(next) => next,
                        Err(_) => {
                            worker_cancellation.cancel();
                            drop(sender);
                            let _ = worker.await;
                            self.update_health(SecExtractionHealthState::ProviderUnavailable, None)?;
                            return Err(SecClientError::ReadTimeout);
                        }
                    }
                }
                result = &mut worker => {
                    return match result {
                        Ok(Ok(_)) => Err(SecClientError::InvalidCaptureMaterial),
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err(SecClientError::BlockingWorkerFailed),
                    };
                }
                () = cancellation.cancelled() => {
                    worker_cancellation.cancel();
                    drop(sender);
                    let _ = worker.await;
                    return Err(SecClientError::Cancelled);
                }
                () = &mut deadline_wait => {
                    worker_cancellation.cancel();
                    drop(sender);
                    return Err(SecClientError::DeadlineExceeded);
                }
            };
            let Some(chunk) = next else { break };
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    worker_cancellation.cancel();
                    drop(sender);
                    let _ = worker.await;
                    self.update_health(SecExtractionHealthState::ProviderUnavailable, None)?;
                    return Err(error.into());
                }
            };
            observed = observed
                .checked_add(
                    u64::try_from(chunk.len()).map_err(|_| SecClientError::ResponseTooLarge)?,
                )
                .ok_or(SecClientError::ResponseTooLarge)?;
            in_flight.validate_response_size(observed)?;
            if observed > effective_maximum || expected_bytes.is_some_and(|size| observed > size) {
                worker_cancellation.cancel();
                drop(sender);
                let _ = worker.await;
                self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
                return Err(SecClientError::ResponseTooLarge);
            }
            tokio::select! {
                result = sender.send(chunk) => {
                    if result.is_err() {
                        worker_cancellation.cancel();
                        drop(sender);
                        let result = worker.await;
                        return match result {
                            Ok(Err(error)) => Err(error),
                            _ => Err(SecClientError::BlockingWorkerFailed),
                        };
                    }
                }
                result = &mut worker => {
                    return match result {
                        Ok(Ok(_)) => Err(SecClientError::InvalidCaptureMaterial),
                        Ok(Err(error)) => Err(error),
                        Err(_) => Err(SecClientError::BlockingWorkerFailed),
                    };
                }
                () = cancellation.cancelled() => {
                    worker_cancellation.cancel();
                    drop(sender);
                    let _ = worker.await;
                    return Err(SecClientError::Cancelled);
                }
                () = &mut deadline_wait => {
                    worker_cancellation.cancel();
                    drop(sender);
                    return Err(SecClientError::DeadlineExceeded);
                }
            }
        }
        drop(stream);
        let body_received_at = system_timestamp()?;
        drop(sender);
        let representation = tokio::select! {
            result = &mut worker => {
                result.map_err(|_| SecClientError::BlockingWorkerFailed)??
            }
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                let _ = worker.await;
                return Err(SecClientError::Cancelled);
            }
            () = &mut deadline_wait => {
                worker_cancellation.cancel();
                return Err(SecClientError::DeadlineExceeded);
            }
        };
        ensure_before_deadline(deadline)?;
        in_flight.validate_current()?;
        self.validate_authority(authority)?;
        if expected_bytes.is_some_and(|length| length != observed)
            || representation.size_bytes() != observed
            || representation.locator() != current
            || body_received_at > representation.first_observed_at()
            || representation.first_observed_at() > deadline
        {
            self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
            return Err(SecClientError::InvalidCaptureMaterial);
        }
        let capture = SecBulkCapture::try_new(
            selection.clone(),
            SourceIdentifier::try_from(current)?,
            representation.evidence(),
            representation.size_bytes(),
            representation.first_observed_at(),
            representation.retrieval_revision(),
            SecBulkTransportEvidence::try_new(
                status.as_u16(),
                media_kind,
                media_type.as_deref(),
                transport_validators,
                body_received_at,
            )
            .map_err(|_| SecClientError::InvalidCaptureMaterial)?,
        )
        .map_err(|_| SecClientError::InvalidCaptureMaterial)?;
        self.update_health(SecExtractionHealthState::Ready, None)?;
        in_flight.record_success()?;
        Ok(capture)
    }

    async fn retrieve(
        &self,
        authority: &ExtractionAuthority,
        locator: &SecObjectLocator,
        cancellation: &CancellationToken,
    ) -> Result<RetrievedSecBytes, SecClientError> {
        self.validate_authority(authority)?;
        let request_bounds = self.request_bounds(authority)?;
        let mut current = locator.url().to_owned();
        let mut redirect_permit: Option<ExtractionRedirectPermit> = None;
        let mut force_unconditional = false;
        loop {
            self.validate_authority(authority)?;
            let conditional = if force_unconditional {
                None
            } else {
                self.representation_registry.conditional_request(&current)?
            };
            let request_identity = sec_request_identity(&current, conditional.as_ref());
            let mut request = self.client.get(&current);
            if let Some(validators) = conditional {
                if let Some(etag) = validators.etag() {
                    request = request.header(IF_NONE_MATCH, etag);
                }
                if let Some(last_modified) = validators.last_modified() {
                    request = request.header(IF_MODIFIED_SINCE, last_modified);
                }
            }
            let in_flight = match redirect_permit.take() {
                Some(permit) => permit.authorize_send(&current)?,
                None => authority
                    .try_network_request(&current)?
                    .authorize_send(&current)?,
            };
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
            in_flight.validate_current()?;
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
                drop(response);
                redirect_permit =
                    Some(in_flight.authorize_redirect_from(&current, &target, false)?);
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
                    .map(|value| value.as_bytes().to_vec());
                drop(response);
                let deadline = in_flight.apply_retry_after_header(retry_after.as_deref(), 5_000)?;
                return Err(SecClientError::Authority(
                    ExtractionAuthorityError::BudgetWaitUntil { deadline },
                ));
            }
            if status.as_u16() == 304 {
                drop(response);
                in_flight.validate_current()?;
                self.validate_authority(authority)?;
                if force_unconditional {
                    in_flight.release();
                    self.update_health(SecExtractionHealthState::InvalidResponse, Some(304))?;
                    return Err(SecClientError::InvalidCaptureMaterial);
                }
                in_flight.record_success()?;
                // A 304 has no provider body to seal. Repeat once without validators so an exact
                // successful response body, not a local cache replay, backs publication.
                force_unconditional = true;
                redirect_permit = None;
                continue;
            }
            if !status.is_success() {
                let health = health_for_http_status(status.as_u16());
                self.update_health(health, Some(status.as_u16()))?;
                drop(response);
                in_flight.validate_current()?;
                in_flight.release();
                return Err(SecClientError::HttpStatus(status.as_u16()));
            }
            let validators = self.response_validators(response.headers())?;
            let effective_max_response_bytes = request_bounds
                .max_response_bytes()
                .min(MAX_PROVIDER_CAPTURE_PAGE_BYTES);
            if let Some(length) = response.content_length() {
                in_flight.validate_response_size(length)?;
                if length > effective_max_response_bytes {
                    self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
                    return Err(SecClientError::ResponseTooLarge);
                }
            }
            let read_timeout = Duration::from_nanos(request_bounds.read_timeout_nanos());
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
                in_flight.validate_current()?;
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
                let new_size =
                    u64::try_from(new_len).map_err(|_| SecClientError::ResponseTooLarge)?;
                in_flight.validate_response_size(new_size)?;
                if new_size > effective_max_response_bytes {
                    self.update_health(SecExtractionHealthState::InvalidResponse, None)?;
                    return Err(SecClientError::ResponseTooLarge);
                }
                bytes
                    .try_reserve(chunk.len())
                    .map_err(|_| SecClientError::AllocationFailed)?;
                bytes.extend_from_slice(&chunk);
            }
            drop(stream);
            in_flight.validate_current()?;
            let body_received_at = system_timestamp()?;
            let response_status = status.as_u16();
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
                    Ok((bytes, retained))
                })
                .await;
            in_flight.validate_current()?;
            self.validate_authority(authority)?;
            let retrieved = retrieved.and_then(|(bytes, retained)| {
                retrieved_from_representation(
                    bytes,
                    retained,
                    self.metadata.source_id(),
                    self.metadata.revision(),
                    request_identity,
                    response_status,
                    body_received_at,
                )
            });
            let retrieved = self.finish_local_retrieval(retrieved)?;
            in_flight.record_success()?;
            return Ok(retrieved);
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
        run_joined_blocking(
            Arc::clone(&self.blocking_admission),
            cancellation,
            None,
            work,
        )
        .await
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

    pub(crate) async fn run_validation_blocking_until<T, F>(
        &self,
        cancellation: &CancellationToken,
        deadline: Timestamp,
        work: F,
    ) -> Result<T, SecClientError>
    where
        T: Send + 'static,
        F: FnOnce(&CancellationToken) -> Result<T, SecClientError> + Send + 'static,
    {
        let result = run_joined_blocking(
            Arc::clone(&self.blocking_admission),
            cancellation,
            Some(deadline),
            work,
        )
        .await;
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
                let state = match error {
                    SecClientError::ResponseTooLarge
                    | SecClientError::InvalidCaptureMaterial
                    | SecClientError::ProviderCapture(_) => {
                        SecExtractionHealthState::InvalidResponse
                    }
                    _ => SecExtractionHealthState::LocalFailure,
                };
                self.update_health(state, None)?;
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

    pub(crate) fn validate_authority(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<(), SecClientError> {
        if authority.metadata() != &self.metadata {
            return Err(SecClientError::RegistrationMismatch);
        }
        authority.validate_current().map_err(Into::into)
    }

    fn request_bounds(
        &self,
        authority: &ExtractionAuthority,
    ) -> Result<HttpRequestBounds, SecClientError> {
        self.validate_authority(authority)?;
        match authority.metadata().network_policy() {
            NetworkAccessPolicy::Allowlisted(policy) => Ok(policy.request_bounds()),
            NetworkAccessPolicy::Denied => Err(SecClientError::NetworkDenied),
        }
    }

    pub(crate) fn raw_store(&self) -> Arc<RawEvidenceStore> {
        Arc::clone(&self.raw_store)
    }

    pub(crate) fn representation_registry(&self) -> Arc<SecRepresentationRegistry> {
        Arc::clone(&self.representation_registry)
    }

    pub(crate) fn retained_representation(
        &self,
        locator: &SecObjectLocator,
    ) -> Result<Option<SecRepresentation>, SecClientError> {
        self.representation_registry
            .representation(locator.url())
            .map_err(Into::into)
    }

    pub(crate) fn identity_registry(&self) -> Arc<ProviderIdentityRegistry> {
        Arc::clone(&self.identities)
    }

    /// Resolves one closed SEC-native identifier through the checked, conflict-quarantining
    /// provider-identity registry and returns the only receipt accepted by bulk canonical mapping.
    pub fn resolve_bulk_identity(
        &self,
        namespace: SecAuthoritativeIdentifierNamespace,
        authority_source_id: &SourceId,
        authoritative_identifier: &SourceIdentifier,
        at: Timestamp,
    ) -> Result<SecGovernedIdentityReceipt, crate::SecBulkError> {
        let provider_identifier =
            ProviderInstrumentId::try_from(authoritative_identifier.as_str())?;
        let record = self
            .identities
            .provider_identity_at(authority_source_id, &provider_identifier, at)
            .ok_or(crate::SecBulkError::UnresolvedIdentity)?;
        SecGovernedIdentityReceipt::from_registry_record(
            namespace,
            authoritative_identifier.clone(),
            record,
        )
    }

    pub(crate) const fn parser_limits(&self) -> SecParserLimits {
        self.parser_limits
    }
}

pub(crate) async fn run_joined_blocking<T, F>(
    admission: Arc<Semaphore>,
    cancellation: &CancellationToken,
    deadline: Option<Timestamp>,
    work: F,
) -> Result<T, SecClientError>
where
    T: Send + 'static,
    F: FnOnce(&CancellationToken) -> Result<T, SecClientError> + Send + 'static,
{
    let permit = if let Some(deadline) = deadline {
        tokio::select! {
            permit = admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                return Err(SecClientError::DeadlineExceeded);
            }
        }
    } else {
        tokio::select! {
            permit = admission.acquire_owned() => {
                permit.map_err(|_| SecClientError::BlockingAdmissionClosed)?
            }
            () = cancellation.cancelled() => return Err(SecClientError::Cancelled),
        }
    };
    let worker_cancellation = cancellation.child_token();
    let worker_token = worker_cancellation.clone();
    let mut worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work(&worker_token)
    });
    if let Some(deadline) = deadline {
        tokio::select! {
            result = &mut worker => {
                result.map_err(|_| SecClientError::BlockingWorkerFailed)?
            }
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                let _ = worker.await.map_err(|_| SecClientError::BlockingWorkerFailed)?;
                Err(SecClientError::Cancelled)
            }
            () = tokio::time::sleep(remaining_until(deadline)?) => {
                worker_cancellation.cancel();
                let _ = worker.await.map_err(|_| SecClientError::BlockingWorkerFailed)?;
                Err(SecClientError::DeadlineExceeded)
            }
        }
    } else {
        tokio::select! {
            result = &mut worker => {
                result.map_err(|_| SecClientError::BlockingWorkerFailed)?
            }
            () = cancellation.cancelled() => {
                worker_cancellation.cancel();
                let _ = worker.await.map_err(|_| SecClientError::BlockingWorkerFailed)?;
                Err(SecClientError::Cancelled)
            }
        }
    }
}

impl SourceMetadataProvider for SecEdgarSource {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

fn validate_fund_preparation_request(
    manifest: &SecBulkLayoutManifest,
    scope: &SecFundPublicationScope,
    ingested_at: Timestamp,
    deadline: Timestamp,
) -> Result<(), crate::SecBulkError> {
    let archive = manifest.capture();
    let readme = manifest.official_readme_capture();
    if archive.selection().family() != scope.family()
        || readme.selection() != archive.selection()
        || archive.transport().body_received_at() > ingested_at
        || readme.transport().body_received_at() > ingested_at
        || archive.first_observed_at() > ingested_at
        || readme.first_observed_at() > ingested_at
        || ingested_at >= deadline
    {
        return Err(crate::SecBulkError::InvalidChronology);
    }
    SecPendingBulkLogicalPublication::logical_object_admissions(manifest)?;
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "captured graph, mapping authority, bounded storage, and clocks must remain explicit"
)]
fn prepare_fund_from_captured_graph<A>(
    raw_store: &RawEvidenceStore,
    manifest: SecBulkLayoutManifest,
    scope: SecFundPublicationScope,
    limits: SecBulkParseLimits,
    admissions: SecFundPartitionAdmissions,
    ingested_at: Timestamp,
    deadline: Timestamp,
    mut identity_authority: A,
    journal: &SealedResearchJournalStore,
    cancellation: &CancellationToken,
) -> Result<SecPreparedFundLogicalPublication, crate::SecBulkError>
where
    A: SecFundIdentityAuthority,
{
    validate_fund_preparation_request(&manifest, &scope, ingested_at, deadline)?;
    let (archive_admission, readme_admission) =
        SecPendingBulkLogicalPublication::logical_object_admissions(&manifest)?;
    let mut archive_stage = journal.begin_logical_object(archive_admission)?;
    let mut readme_stage = match journal.begin_logical_object(readme_admission) {
        Ok(stage) => stage,
        Err(error) => {
            journal.abort_logical_object(archive_stage)?;
            return Err(error.into());
        }
    };
    let pending = match SecPendingBulkLogicalPublication::stage_from_raw_store(
        raw_store,
        manifest,
        limits,
        deadline,
        cancellation,
        &mut archive_stage,
        &mut readme_stage,
    ) {
        Ok(pending) => pending,
        Err(error) => {
            let archive_abort = journal.abort_logical_object(archive_stage);
            let readme_abort = journal.abort_logical_object(readme_stage);
            archive_abort?;
            readme_abort?;
            return Err(error);
        }
    };
    let control = SecFundPreparationControl {
        cancellation,
        deadline,
    };
    let archive = journal.finish_logical_object(archive_stage, &control)?;
    let readme = journal.finish_logical_object(readme_stage, &control)?;
    pending
        .verify_and_stage(
            archive,
            readme,
            limits,
            deadline,
            cancellation,
            &control,
            SecFundPendingLogicalRows::new(scope),
        )?
        .prepare_fund_logical_publication(
            &mut identity_authority,
            ingested_at,
            admissions,
            journal,
            &control,
        )
}

struct SecFundPreparationControl<'a> {
    cancellation: &'a CancellationToken,
    deadline: Timestamp,
}

impl ResearchObjectControl for SecFundPreparationControl<'_> {
    fn checkpoint(
        &self,
        _point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError> {
        if self.cancellation.is_cancelled() {
            return Err(ResearchObjectControlError::Cancelled);
        }
        match system_timestamp() {
            Ok(observed_at) if observed_at < self.deadline => Ok(()),
            Ok(_) => Err(ResearchObjectControlError::DeadlineExceeded),
            Err(_) => Err(ResearchObjectControlError::Unavailable),
        }
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

pub(crate) fn retrieved_from_representation(
    bytes: Vec<u8>,
    representation: SecRepresentation,
    source_id: &SourceId,
    metadata_revision: &market_squawk_domain::MetadataRevision,
    request_identity: EvidenceDigest,
    http_status: u16,
    body_received_at: market_squawk_domain::Timestamp,
) -> Result<RetrievedSecBytes, SecClientError> {
    if representation.source_id() != source_id {
        return Err(SecClientError::InvalidCaptureMaterial);
    }
    let body_bytes = u64::try_from(bytes.len()).map_err(|_| SecClientError::ResponseTooLarge)?;
    let page = ProviderCapturePageReceipt::try_new(
        0,
        request_identity,
        None,
        None,
        http_status,
        body_bytes,
        representation.evidence(),
        body_received_at,
    )?;
    let dataset = SourceIdentifier::try_from(representation.locator())?;
    let capture_receipt = ProviderCaptureSetReceipt::try_new(
        source_id.clone(),
        metadata_revision.clone(),
        dataset,
        request_identity,
        ProviderCaptureTerminalDisposition::StandaloneResponse,
        vec![page],
    )?;
    Ok(RetrievedSecBytes::captured_online(
        bytes,
        representation.evidence(),
        representation.first_observed_at(),
        representation.locator().to_owned(),
        representation.retrieval_revision(),
        capture_receipt,
    ))
}

fn sec_request_identity(locator: &str, conditional: Option<&SecHttpValidators>) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sec-http-get-request/v1");
    hash_capture_field(&mut hash, b"GET");
    hash_capture_field(&mut hash, locator.as_bytes());
    match conditional {
        Some(validators) => {
            hash.update([1]);
            hash_optional_capture_field(&mut hash, validators.etag().map(str::as_bytes));
            hash_optional_capture_field(&mut hash, validators.last_modified().map(str::as_bytes));
        }
        None => hash.update([0]),
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn remaining_until(deadline: Timestamp) -> Result<Duration, SecClientError> {
    let now = system_timestamp()?;
    let remaining = deadline
        .unix_nanos()
        .checked_sub(now.unix_nanos())
        .filter(|remaining| *remaining > 0)
        .ok_or(SecClientError::DeadlineExceeded)?;
    let remaining = u64::try_from(remaining).map_err(|_| SecClientError::DeadlineExceeded)?;
    Ok(Duration::from_nanos(remaining))
}

fn ensure_before_deadline(deadline: Timestamp) -> Result<(), SecClientError> {
    if system_timestamp()? >= deadline {
        Err(SecClientError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn hash_optional_capture_field(hash: &mut Sha256, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash_capture_field(hash, value);
        }
        None => hash.update([0]),
    }
}

fn hash_capture_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}
