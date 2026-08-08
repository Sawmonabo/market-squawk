//! Canonical `ExtractionSource` composition for SEC submissions and Company Facts.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    AvailabilityEvidence, CompanyIdentityObservation, CompanyIdentityObservationInput,
    CompanyIdentitySurface, DataQuality, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    FormerCompanyName, ProviderIdentityRegistry, ProviderReportedSecurityAssociation,
    ResearchContext, ResearchObservation, SchemaVersion, SourceId, SourceIdentifier, Timestamp,
    VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, DiscoveryBatch, DiscoveryRequest,
    ExtractionAuthority, ExtractionBatch, ExtractionRecord, ExtractionRequest,
    ExtractionRevisionEvidence, ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError,
    MAX_EXTRACTION_RECORD_BYTES, ObservedProviderOrder, SourceError, SourceMetadataProvider,
    SourceObject,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{
    RawEvidenceStore, RetrievedCompanyFacts, RetrievedSecBytes, RetrievedSubmissions,
    SecClientError, SecCompositeBounds, SecEdgarSource, SecNormalizationError, SecParserError,
    SecParserLimits, normalize_company_facts_with_cancellation,
    normalize_filings_with_cancellation,
};

const RESEARCH_RECORD_SCHEMA: &str = "market-squawk-research-v3";

/// SEC analytical extraction paired with optional evidence-bound company identity.
///
/// Company identity remains research metadata and cannot establish a tradable instrument, venue
/// mapping, or execution-quality observation.
#[derive(Debug)]
pub struct SecExtractionResult {
    batch: ExtractionBatch,
    company_identity: Option<CompanyIdentityObservation>,
}

impl SecExtractionResult {
    /// Returns the ordinary source-agnostic analytical batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns identity evidence parsed from the same exact retrieved representation.
    pub const fn company_identity(&self) -> Option<&CompanyIdentityObservation> {
        self.company_identity.as_ref()
    }

    /// Consumes this result into its analytical and identity components.
    pub fn into_parts(self) -> (ExtractionBatch, Option<CompanyIdentityObservation>) {
        (self.batch, self.company_identity)
    }

    fn into_batch(self) -> ExtractionBatch {
        self.batch
    }
}

impl SecEdgarSource {
    /// Builds provider-owned revision evidence aligned to one extracted SEC batch.
    ///
    /// Exact canonical source-record identity is the version token. The SEC acceptance timestamp,
    /// or filing civil date when no acceptance timestamp is published, is the provider order.
    /// Neither coordinate is promoted to first-public-availability evidence.
    ///
    /// # Errors
    ///
    /// Returns [`SecClientError::RegistrationMismatch`] when the batch belongs to another source
    /// metadata revision. Returns [`SecClientError::InvalidCompositeRepresentation`] when a record
    /// lacks provider publication order, and [`SecClientError::RevisionAuthority`] when bounded
    /// exact-evidence invariants fail.
    pub fn revision_plan(
        &self,
        batch: &ExtractionBatch,
    ) -> Result<ExtractionRevisionPlan, SecClientError> {
        if batch.request().object().source_id() != self.metadata().source_id()
            || batch.request().object().metadata_revision() != self.metadata().revision()
        {
            return Err(SecClientError::RegistrationMismatch);
        }
        let mut evidence = Vec::new();
        evidence
            .try_reserve_exact(batch.records().len())
            .map_err(|_| SecClientError::AllocationFailed)?;
        for record in batch.records() {
            let version = record.revision().as_str().as_bytes();
            let published = record
                .published_time()
                .cloned()
                .ok_or(SecClientError::InvalidCompositeRepresentation)?;
            let order = ObservedProviderOrder::try_new(published, version)?;
            evidence.push(ExtractionRevisionEvidence::provider_supplied(
                version, order,
            )?);
        }
        ExtractionRevisionPlan::try_new(evidence).map_err(Into::into)
    }

    /// Extracts SEC analytical records with company identity from the same exact source bytes.
    ///
    /// The ordinary [`ExtractionSource`] implementation delegates here and discards only the
    /// adapter-specific sidecar. Callers that own company-identity publication use this method so
    /// no second raw-store read or parser pass is required.
    pub fn extract_with_company_identity(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<SecExtractionResult, ExtractionSourceError>> {
        let raw_store = self.raw_store();
        let identities = self.identity_registry();
        let source_id = self.metadata().source_id().clone();
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let remaining = deadline_remaining(request.deadline())?;
            let worker_cancellation = cancellation.child_token();
            let worker_authority = authority.clone();
            let worker = self.run_validation_blocking(&worker_cancellation, move |worker_token| {
                extract_blocking(
                    request,
                    raw_store,
                    identities,
                    source_id,
                    worker_authority,
                    worker_token,
                )
            });
            tokio::pin!(worker);
            tokio::select! {
                result = &mut worker => {
                    let extracted = result.map_err(map_client_error)?;
                    self.validate_authority(&authority).map_err(map_client_error)?;
                    Ok(extracted)
                },
                () = tokio::time::sleep(remaining) => {
                    worker_cancellation.cancel();
                    Err(ExtractionSourceError::DeadlineExceeded)
                }
                () = cancellation.cancelled() => {
                    worker_cancellation.cancel();
                    Err(ExtractionSourceError::Cancelled)
                }
            }
        })
    }
}

enum DatasetLocator<'a> {
    Submissions(&'a str),
    CompanyFacts(&'a str),
}

impl<'a> DatasetLocator<'a> {
    fn parse(dataset: &'a str) -> Result<Self, ExtractionSourceError> {
        if let Some(cik) = dataset.strip_prefix("sec.submissions.cik.") {
            return Ok(Self::Submissions(cik));
        }
        if let Some(cik) = dataset.strip_prefix("sec.company-facts.cik.") {
            return Ok(Self::CompanyFacts(cik));
        }
        Err(invalid_protocol())
    }
}

impl ExtractionSource for SecEdgarSource {
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>> {
        Box::pin(async move {
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let child = cancellation.child_token();
            let remaining = deadline_remaining(request.deadline())?;
            let dataset = DatasetLocator::parse(request.dataset().as_str())?;
            let (retrieved, object_id) = tokio::time::timeout(remaining, async {
                match dataset {
                    DatasetLocator::Submissions(cik) => self
                        .fetch_complete_submissions(
                            &authority,
                            cik,
                            SecCompositeBounds::production_defaults(),
                            request.deadline(),
                            child.clone(),
                        )
                        .await
                        .and_then(|value| {
                            SourceIdentifier::try_from(format!(
                                "sec.submissions.composite.CIK{}",
                                value.document().cik()
                            ))
                            .map(|object_id| (value.raw().clone(), object_id))
                            .map_err(Into::into)
                        }),
                    DatasetLocator::CompanyFacts(cik) => self
                        .fetch_company_facts(&authority, cik, child.clone())
                        .await
                        .and_then(|value| {
                            let object_id = value
                                .raw()
                                .locator()
                                .ok_or(SecClientError::InvalidCompositeRepresentation)
                                .and_then(|locator| {
                                    SourceIdentifier::try_from(locator).map_err(Into::into)
                                })?;
                            Ok((value.raw().clone(), object_id))
                        }),
                }
            })
            .await
            .map_err(|_| {
                child.cancel();
                ExtractionSourceError::DeadlineExceeded
            })?
            .map_err(map_client_error)?;
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            let object = SourceObject::try_new(
                self.metadata().source_id().clone(),
                self.metadata().revision().clone(),
                &request,
                object_id,
                SourceIdentifier::try_from("application/json").map_err(|_| invalid_protocol())?,
                ExactPayloadEvidence::from_content_digest(retrieved.evidence()),
                market_squawk_domain::EffectiveInterval::new(retrieved.received_at(), None)
                    .map_err(|_| invalid_protocol())?,
                None,
                Some(u64::try_from(retrieved.bytes().len()).map_err(|_| invalid_protocol())?),
            )?;
            let batch = DiscoveryBatch::try_new(&request, vec![object])?;
            self.validate_authority(&authority)
                .map_err(map_client_error)?;
            Ok(batch)
        })
    }

    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>> {
        let extracted = self.extract_with_company_identity(authority, request, cancellation);
        Box::pin(async move { extracted.await.map(SecExtractionResult::into_batch) })
    }
}

fn extract_blocking(
    request: ExtractionRequest,
    raw_store: Arc<RawEvidenceStore>,
    identities: Arc<ProviderIdentityRegistry>,
    source_id: SourceId,
    authority: ExtractionAuthority,
    cancellation: &CancellationToken,
) -> Result<SecExtractionResult, SecClientError> {
    authority.validate_current()?;
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let bytes = raw_store.read_verified_bounded_cancellable(
        &request.object().evidence().content_digest(),
        request.max_bytes(),
        cancellation,
    )?;
    authority.validate_current()?;
    let received_at = request.object().effective_interval().starts_at();
    let availability = AvailabilityEvidence::LocalFirstObserved {
        observed_at: received_at,
    };
    let parser_limits = request_parser_limits(&request, bytes.len())?;
    let ingested_at = crate::client::system_timestamp()?;
    let (observations, company_identity) =
        match DatasetLocator::parse(request.object().dataset().as_str())
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?
        {
            DatasetLocator::Submissions(_) => {
                let retrieved = crate::composite::restore_online_submissions(
                    &raw_store,
                    &bytes,
                    request.object().evidence().content_digest(),
                    SecCompositeBounds::production_defaults(),
                    parser_limits,
                    cancellation,
                )?;
                let observations = normalize_filings_with_cancellation(
                    &source_id,
                    &identities,
                    &retrieved,
                    ingested_at,
                    cancellation,
                )?;
                let company_identity = company_identity_from_submissions(
                    &request,
                    &source_id,
                    &retrieved,
                    ingested_at,
                    cancellation,
                )?;
                (observations, Some(company_identity))
            }
            DatasetLocator::CompanyFacts(_) => {
                let retrieved = RetrievedCompanyFacts::restored(
                    bytes,
                    request.object().evidence().content_digest(),
                    received_at,
                    availability,
                    parser_limits,
                    cancellation,
                )?;
                let observations = normalize_company_facts_with_cancellation(
                    &source_id,
                    &identities,
                    &retrieved,
                    ingested_at,
                    cancellation,
                )?;
                let company_identity = company_identity_from_company_facts(
                    &request,
                    &source_id,
                    &retrieved,
                    ingested_at,
                    cancellation,
                )?;
                (observations, Some(company_identity))
            }
        };
    authority.validate_current()?;
    let mut records = Vec::new();
    records
        .try_reserve(observations.len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    for observation in observations {
        authority.validate_current()?;
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        records.push(canonical_record(
            &request,
            observation,
            &authority,
            cancellation,
        )?);
    }
    authority.validate_current()?;
    let batch = ExtractionBatch::try_new(&request, records)
        .map_err(|_| SecClientError::InvalidCompositeRepresentation)?;
    authority.validate_current()?;
    Ok(SecExtractionResult {
        batch,
        company_identity,
    })
}

fn company_identity_from_submissions(
    request: &ExtractionRequest,
    source_id: &SourceId,
    retrieved: &RetrievedSubmissions,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<CompanyIdentityObservation, SecClientError> {
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let metadata = retrieved.document().company_metadata();
    let mut former_names = Vec::new();
    former_names
        .try_reserve_exact(metadata.former_names().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    for former_name in metadata.former_names() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        former_names.push(FormerCompanyName::try_new(
            former_name.name(),
            former_name.effective_from(),
            former_name.effective_to(),
        )?);
    }
    let mut associations = Vec::new();
    associations
        .try_reserve_exact(metadata.ticker_exchange_pairs().len())
        .map_err(|_| SecClientError::AllocationFailed)?;
    for association in metadata.ticker_exchange_pairs() {
        if cancellation.is_cancelled() {
            return Err(SecClientError::Cancelled);
        }
        associations.push(ProviderReportedSecurityAssociation::try_new(
            association.ticker(),
            association.exchange(),
        )?);
    }
    let identity_raw = retrieved.current_component();
    CompanyIdentityObservation::try_new(CompanyIdentityObservationInput {
        schema_version: SchemaVersion::CURRENT,
        source_id: source_id.clone(),
        provider_company_id: retrieved.document().cik().clone(),
        surface: CompanyIdentitySurface::SecSubmissions,
        conformed_name: metadata.conformed_name().to_owned(),
        former_names,
        entity_type: metadata.entity_type().map(str::to_owned),
        sic: metadata.sic().map(str::to_owned),
        sic_description: metadata.sic_description().map(str::to_owned),
        associations,
        parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
            request.object().evidence().content_digest(),
        ),
        identity_payload_evidence: retrieved_payload_evidence(identity_raw)?,
        received_at: identity_raw.received_at(),
        availability: identity_raw.availability().clone(),
        ingested_at,
        quality: DataQuality::OfficialDelayed,
    })
    .map_err(Into::into)
}

fn company_identity_from_company_facts(
    request: &ExtractionRequest,
    source_id: &SourceId,
    retrieved: &RetrievedCompanyFacts,
    ingested_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<CompanyIdentityObservation, SecClientError> {
    if cancellation.is_cancelled() {
        return Err(SecClientError::Cancelled);
    }
    let identity_raw = retrieved.raw();
    CompanyIdentityObservation::try_new(CompanyIdentityObservationInput {
        schema_version: SchemaVersion::CURRENT,
        source_id: source_id.clone(),
        provider_company_id: retrieved.document().cik().clone(),
        surface: CompanyIdentitySurface::SecCompanyFacts,
        conformed_name: retrieved.document().entity_name().to_owned(),
        former_names: Vec::new(),
        entity_type: None,
        sic: None,
        sic_description: None,
        associations: Vec::new(),
        parent_ingest_payload_evidence: ExactPayloadEvidence::from_content_digest(
            request.object().evidence().content_digest(),
        ),
        identity_payload_evidence: retrieved_payload_evidence(identity_raw)?,
        received_at: identity_raw.received_at(),
        availability: identity_raw.availability().clone(),
        ingested_at,
        quality: DataQuality::OfficialDelayed,
    })
    .map_err(Into::into)
}

fn retrieved_payload_evidence(
    retrieved: &RetrievedSecBytes,
) -> Result<ExactPayloadEvidence, SecClientError> {
    match (retrieved.locator(), retrieved.retrieval_revision()) {
        (Some(locator), Some(revision)) => Ok(ExactPayloadEvidence::with_version_pinned_locator(
            retrieved.evidence(),
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(locator)?,
                SourceIdentifier::try_from(revision.to_string())?,
            ),
        )),
        (None, None) => Ok(ExactPayloadEvidence::from_content_digest(
            retrieved.evidence(),
        )),
        _ => Err(SecClientError::InvalidCompositeRepresentation),
    }
}

fn canonical_record(
    request: &ExtractionRequest,
    observation: ResearchObservation,
    authority: &ExtractionAuthority,
    cancellation: &CancellationToken,
) -> Result<ExtractionRecord, SecClientError> {
    authority.validate_current()?;
    let context = observation_context(&observation)?;
    let time = context.time();
    let availability = extraction_availability(context.provenance().availability());
    let revision = context.provenance().source_identifier().clone();
    let mut writer = CanonicalRecordWriter::new(cancellation);
    if serde_json::to_writer(&mut writer, &observation).is_err() {
        return if cancellation.is_cancelled() {
            Err(SecClientError::Cancelled)
        } else {
            Err(SecClientError::CompositeSerialization)
        };
    }
    let payload = writer.into_inner();
    authority.validate_current()?;
    let digest: [u8; 32] = Sha256::digest(&payload).into();
    ExtractionRecord::try_new_with_time(
        request,
        SourceIdentifier::try_from(RESEARCH_RECORD_SCHEMA)?,
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            digest,
        )),
        time.effective().clone(),
        time.published().cloned(),
        availability,
        revision,
        time.superseded().cloned(),
        Bytes::from(payload),
    )
    .map_err(|_| SecClientError::InvalidCompositeRepresentation)
}

fn extraction_availability(availability: &AvailabilityEvidence) -> ExtractionAvailabilityEvidence {
    match availability {
        AvailabilityEvidence::Evidenced {
            available_at,
            evidence,
        } => ExtractionAvailabilityEvidence::Observed {
            available_at: *available_at,
            evidence: evidence.clone(),
        },
        AvailabilityEvidence::LocalFirstObserved { observed_at } => {
            ExtractionAvailabilityEvidence::LocalFirstObserved {
                observed_at: *observed_at,
            }
        }
        AvailabilityEvidence::Inferred {
            inferred_at,
            method,
        } => ExtractionAvailabilityEvidence::Inferred {
            inferred_at: *inferred_at,
            method: method.clone(),
        },
        AvailabilityEvidence::Unknown => ExtractionAvailabilityEvidence::Unknown,
    }
}

struct CanonicalRecordWriter<'a> {
    payload: Vec<u8>,
    cancellation: &'a CancellationToken,
}

impl<'a> CanonicalRecordWriter<'a> {
    const fn new(cancellation: &'a CancellationToken) -> Self {
        Self {
            payload: Vec::new(),
            cancellation,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.payload
    }
}

impl Write for CanonicalRecordWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self.cancellation.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "SEC canonical record serialization cancelled",
            ));
        }
        let new_len = self
            .payload
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("SEC canonical record is too large"))?;
        if new_len > MAX_EXTRACTION_RECORD_BYTES {
            return Err(std::io::Error::other("SEC canonical record is too large"));
        }
        self.payload
            .try_reserve(buffer.len())
            .map_err(|_| std::io::Error::other("SEC canonical record allocation failed"))?;
        self.payload.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn observation_context(
    observation: &ResearchObservation,
) -> Result<&ResearchContext, SecClientError> {
    match observation {
        ResearchObservation::Filing(observation) => Ok(observation.context()),
        ResearchObservation::Fundamental(observation) => Ok(observation.context()),
        _ => Err(SecClientError::InvalidCompositeRepresentation),
    }
}

fn request_parser_limits(
    request: &ExtractionRequest,
    decoded_bytes: usize,
) -> Result<SecParserLimits, SecClientError> {
    let decoded_limit = decoded_bytes.max(1);
    let string_limit = decoded_limit.min(256 * 1024);
    let total_string_limit = decoded_limit.min(24 * 1024 * 1024).max(string_limit);
    let retained_output_limit = decoded_limit
        .checked_mul(4)
        .ok_or(SecClientError::InvalidCompositeRepresentation)?
        .min(128 * 1024 * 1024)
        .max(total_string_limit);
    SecParserLimits::try_new(
        decoded_limit,
        usize::try_from(request.max_records())
            .map_err(|_| SecClientError::InvalidCompositeRepresentation)?,
        128,
        string_limit,
        total_string_limit,
        retained_output_limit,
    )
    .map_err(Into::into)
}

fn deadline_remaining(
    deadline: market_squawk_domain::Timestamp,
) -> Result<Duration, ExtractionSourceError> {
    let now = crate::client::system_timestamp().map_err(map_client_error)?;
    let remaining = deadline.unix_nanos().saturating_sub(now.unix_nanos());
    if remaining <= 0 {
        Err(ExtractionSourceError::DeadlineExceeded)
    } else {
        u64::try_from(remaining)
            .map(Duration::from_nanos)
            .map_err(|_| ExtractionSourceError::DeadlineExceeded)
    }
}

fn map_client_error(error: SecClientError) -> ExtractionSourceError {
    let source = match error {
        SecClientError::Cancelled => return ExtractionSourceError::Cancelled,
        SecClientError::DeadlineExceeded => return ExtractionSourceError::DeadlineExceeded,
        SecClientError::Authority(error) => return ExtractionSourceError::Authority(error),
        SecClientError::HttpStatus(401 | 403) => SourceError::Unauthorized,
        SecClientError::HttpStatus(429 | 503) => SourceError::ProviderUnavailable,
        SecClientError::ClockOutOfRange => SourceError::TrustedTimeUnavailable,
        SecClientError::Parser(SecParserError::Cancelled)
        | SecClientError::Normalization(SecNormalizationError::Cancelled) => {
            return ExtractionSourceError::Cancelled;
        }
        SecClientError::Parser(_)
        | SecClientError::CompanyIdentity(_)
        | SecClientError::Normalization(_)
        | SecClientError::Xbrl(_)
        | SecClientError::RevisionAuthority(_)
        | SecClientError::RegistrationMismatch
        | SecClientError::InvalidCompositeRepresentation
        | SecClientError::InvalidCompanionSet => SourceError::InvalidProtocolState,
        _ => SourceError::Network,
    };
    ExtractionSourceError::Source(source)
}

fn invalid_protocol() -> ExtractionSourceError {
    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
}
