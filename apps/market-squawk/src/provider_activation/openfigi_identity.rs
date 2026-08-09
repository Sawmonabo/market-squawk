//! Bounded FIGI identity publication for caller-selected current U.S. listings.
//!
//! This coordinator never crawls the directory. It resolves only an explicit, bounded set of
//! symbol/MIC keys against the process-local Nasdaq snapshot, submits deterministic public
//! OpenFIGI batches, and publishes only unambiguous assigned FIGIs. Nasdaq names and symbols stay
//! session-only; durable definitions contain no trading terms or execution eligibility.

use std::collections::BTreeMap;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_openfigi::{
    MAX_OPENFIGI_RESPONSE_BYTES, OPENFIGI_PUBLIC_MAX_JOBS, OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS,
    OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW, OPENFIGI_V3_MAPPING_URL, OPENFIGI_V3_PROVIDER,
    OpenFigiAccess, OpenFigiClient, OpenFigiClientError, OpenFigiConflictReason,
    OpenFigiIdentityCandidate, OpenFigiListingMappingJob, OpenFigiMappingOutcome,
    OpenFigiMappingReceipt, OpenFigiModelError,
};
use market_squawk_data::{
    MarketDataInstrumentCatalogError, MarketDataInstrumentReadCapability,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
    MarketDataInstrumentSynchronizationReceipt, market_data_instrument_id,
};
use market_squawk_domain::{
    AssetClass, AssignmentVerification, AuthorizationBasis, ChecksumCapability, CoverageDelay,
    Currency, DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, ExternalIdentifier, ExternalIdentifierRecord,
    ExternalIdentifierRecordInput, Figi, IdentifierEntitlement, IdentifierRightsPolicyReference,
    InstrumentId, MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionError,
    MarketDataInstrumentDefinitionInput, MetadataRevision, RevisionBoundPayloadEvidence,
    SchemaVersion, SequenceCapability, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    BudgetWindowSemantics, CoverageTopology, EndpointPolicy, ExtractionAuthority, FreshnessPolicy,
    HistoricalCapability, HttpRequestBounds, InstrumentCoverage, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderBudgetWindow, ProviderRateAuthority, RegistryError,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceMetadataProvider, SourceProtocolProfile, TlsProviderCapability,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::nasdaq_reference::{
    MAXIMUM_SELECTED_LISTING_IDENTITIES, NasdaqCurrentListing, NasdaqListingKey,
    NasdaqReferenceUniverseError, NasdaqReferenceUniverseService,
};

const OPENFIGI_SOURCE_ID: &str = "openfigi-v3-public-identity";
const OPENFIGI_METADATA_REVISION: &str = "openfigi-v3-public-identity-v1";
const OPENFIGI_AUTHORIZATION_BASIS: &str = "openfigi-official-public-api";
const OPENFIGI_FIGI_POLICY_ID: &str = "openfigi-figi-public-domain-v1";
const OPENFIGI_API_DOCUMENTATION_REFERENCE: &str = "https://www.openfigi.com/api/documentation";
const OPENFIGI_FIGI_TERMS_REFERENCE: &str = "https://www.openfigi.com/docs/terms-of-service";
const DEFINITION_REFERENCE_REVISION: &str = "openfigi-market-data-definition-v1";
const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;
const DAY_NANOS: u64 = 24 * 60 * MINUTE_NANOS;

/// Exact locally reviewed official OpenFIGI terms evidence and its validity interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenFigiPublicTermsEvidence {
    terms_evidence: ExactPayloadEvidence,
    api_documentation_evidence: ExactPayloadEvidence,
    effective: EffectiveInterval,
}

impl OpenFigiPublicTermsEvidence {
    pub(crate) fn try_new(
        terms_evidence: ExactPayloadEvidence,
        api_documentation_evidence: ExactPayloadEvidence,
        effective: EffectiveInterval,
    ) -> Result<Self, OpenFigiIdentityPublicationError> {
        if !evidence_matches_reference(&terms_evidence, OPENFIGI_FIGI_TERMS_REFERENCE)
            || !evidence_matches_reference(
                &api_documentation_evidence,
                OPENFIGI_API_DOCUMENTATION_REFERENCE,
            )
        {
            return Err(OpenFigiIdentityPublicationError::InvalidConfiguration);
        }
        Ok(Self {
            terms_evidence,
            api_documentation_evidence,
            effective,
        })
    }
}

/// Quote currency plus exact evidence from a caller-owned policy authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EvidenceBoundQuoteCurrency {
    currency: Currency,
    evidence: ExactPayloadEvidence,
}

impl EvidenceBoundQuoteCurrency {
    pub(crate) fn try_new(
        currency: Currency,
        evidence: ExactPayloadEvidence,
    ) -> Result<Self, OpenFigiIdentityPublicationError> {
        if evidence.content_digest().bytes() == [0; 32] {
            return Err(OpenFigiIdentityPublicationError::InvalidConfiguration);
        }
        Ok(Self { currency, evidence })
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.currency
    }

    pub(crate) const fn evidence(&self) -> &ExactPayloadEvidence {
        &self.evidence
    }
}

/// Caller-owned authority for evidence-backed quote-currency decisions.
///
/// Implementations must derive decisions from their own admitted evidence. Nasdaq directory and
/// OpenFIGI mapping fields are not quote-currency evidence and are never supplied to this trait.
pub(crate) trait OpenFigiQuoteCurrencyPolicy: Send + Sync + 'static {
    fn quote_currency_for(&self, listing: &NasdaqListingKey) -> Option<EvidenceBoundQuoteCurrency>;
}

/// Durable disposition for one exact FIGI mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenFigiCatalogDisposition {
    InsertedOrAdvanced,
    Replayed,
}

/// Final fail-closed result for one deduplicated listing key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum OpenFigiIdentityPublicationStatus {
    ListingNotCurrent,
    NoMatch,
    Ambiguous {
        candidates: Box<[OpenFigiIdentityCandidate]>,
    },
    ProviderConflict {
        reason: OpenFigiConflictReason,
    },
    ProviderError {
        message_digest: EvidenceDigest,
    },
    IdentityConflict {
        permanent_figi: Figi,
    },
    QuoteCurrencyUnavailable,
    Exact {
        candidate: OpenFigiIdentityCandidate,
        instrument_id: InstrumentId,
        catalog_disposition: OpenFigiCatalogDisposition,
    },
}

/// One key and its final identity-publication status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenFigiIdentityPublicationResult {
    listing: NasdaqListingKey,
    status: OpenFigiIdentityPublicationStatus,
}

impl OpenFigiIdentityPublicationResult {
    pub(crate) const fn listing(&self) -> &NasdaqListingKey {
        &self.listing
    }

    pub(crate) const fn status(&self) -> &OpenFigiIdentityPublicationStatus {
        &self.status
    }
}

/// Complete bounded provider and catalog evidence returned to the application owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OpenFigiIdentityPublicationReceipt {
    results: Box<[OpenFigiIdentityPublicationResult]>,
    provider_receipts: Box<[OpenFigiMappingReceipt]>,
    catalog_receipt: Option<MarketDataInstrumentSynchronizationReceipt>,
}

impl OpenFigiIdentityPublicationReceipt {
    pub(crate) fn results(&self) -> &[OpenFigiIdentityPublicationResult] {
        &self.results
    }

    pub(crate) fn provider_receipts(&self) -> &[OpenFigiMappingReceipt] {
        &self.provider_receipts
    }

    pub(crate) const fn catalog_receipt(
        &self,
    ) -> Option<&MarketDataInstrumentSynchronizationReceipt> {
        self.catalog_receipt.as_ref()
    }
}

/// Application-owned public OpenFIGI coordinator for an explicit selected listing subset.
pub(crate) struct OpenFigiIdentityPublisher {
    nasdaq: Arc<NasdaqReferenceUniverseService>,
    client: OpenFigiClient,
    extraction: ExtractionAuthority,
    registry: StdMutex<Option<AuthoritativeSourceRegistry>>,
    catalog_writer: MarketDataInstrumentSynchronizationCapability,
    catalog_reader: MarketDataInstrumentReadCapability,
    quote_currency_policy: Arc<dyn OpenFigiQuoteCurrencyPolicy>,
    operation: Mutex<()>,
    lifecycle: CancellationToken,
}

impl std::fmt::Debug for OpenFigiIdentityPublisher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenFigiIdentityPublisher")
            .field("source_id", self.client.metadata().source_id())
            .field("access", &OpenFigiAccess::Public)
            .finish_non_exhaustive()
    }
}

impl OpenFigiIdentityPublisher {
    /// Constructs one no-key fixed-endpoint client on the product-wide provider budget.
    #[allow(
        clippy::too_many_arguments,
        reason = "source, catalog, TLS, rights, and currency authorities remain explicit"
    )]
    pub(crate) fn try_new(
        nasdaq: Arc<NasdaqReferenceUniverseService>,
        provider_rate: ProviderRateAuthority,
        tls_provider: TlsProviderCapability,
        public_terms: OpenFigiPublicTermsEvidence,
        catalog_writer: MarketDataInstrumentSynchronizationCapability,
        catalog_reader: MarketDataInstrumentReadCapability,
        quote_currency_policy: Arc<dyn OpenFigiQuoteCurrencyPolicy>,
    ) -> Result<Self, OpenFigiIdentityPublicationError> {
        let metadata = source_metadata(&public_terms)?;
        let client =
            OpenFigiClient::try_new(metadata.clone(), OpenFigiAccess::Public, tls_provider)?;
        let resolver = Arc::new(provider_rate.clone());
        let mut registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
            resolver,
            provider_rate,
        )?;
        let registered = registry.register(metadata, system_timestamp()?)?;
        let extraction = registry.extraction_authority(&registered, &client)?;
        Ok(Self {
            nasdaq,
            client,
            extraction,
            registry: StdMutex::new(Some(registry)),
            catalog_writer,
            catalog_reader,
            quote_currency_policy,
            operation: Mutex::new(()),
            lifecycle: CancellationToken::new(),
        })
    }

    /// Resolves and atomically publishes only exact FIGIs for one caller-selected subset.
    pub(crate) async fn resolve_selected_and_publish(
        &self,
        mut listings: Vec<NasdaqListingKey>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OpenFigiIdentityPublicationReceipt, OpenFigiIdentityPublicationError> {
        normalize_selection(&mut listings)?;
        ensure_open(deadline, cancellation, &self.lifecycle)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(OpenFigiIdentityPublicationError::DeadlineExceeded);
        }
        let _operation = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(OpenFigiIdentityPublicationError::Cancelled);
            }
            () = self.lifecycle.cancelled() => {
                return Err(OpenFigiIdentityPublicationError::ShuttingDown);
            }
            result = tokio::time::timeout(remaining, self.operation.lock()) => {
                result.map_err(|_| OpenFigiIdentityPublicationError::DeadlineExceeded)?
            }
        };
        ensure_open(deadline, cancellation, &self.lifecycle)?;

        let selected = self
            .nasdaq
            .selected_current_listings(&listings, deadline, cancellation)
            .await?;
        let by_key = selected
            .into_iter()
            .map(|listing| (listing.key().clone(), listing))
            .collect::<BTreeMap<_, _>>();
        let mut statuses = vec![None; listings.len()];
        let mut jobs = Vec::new();
        jobs.try_reserve_exact(by_key.len())
            .map_err(|_| OpenFigiIdentityPublicationError::Capacity)?;
        for (index, key) in listings.iter().enumerate() {
            let Some(listing) = by_key.get(key) else {
                statuses[index] = Some(OpenFigiIdentityPublicationStatus::ListingNotCurrent);
                continue;
            };
            jobs.push(OpenFigiListingMappingJob::try_new(
                listing.source_id().clone(),
                listing.metadata_revision().clone(),
                listing.source_payload_evidence().clone(),
                listing.source_timestamp(),
                listing.observed_at(),
                key.symbol().clone(),
                key.mic().clone(),
            )?);
        }

        let mut provider_receipts = Vec::new();
        provider_receipts
            .try_reserve_exact(jobs.len().div_ceil(OPENFIGI_PUBLIC_MAX_JOBS))
            .map_err(|_| OpenFigiIdentityPublicationError::Capacity)?;
        let mut exact = Vec::new();
        exact
            .try_reserve_exact(jobs.len())
            .map_err(|_| OpenFigiIdentityPublicationError::Capacity)?;
        for batch in jobs.chunks(OPENFIGI_PUBLIC_MAX_JOBS) {
            ensure_open(deadline, cancellation, &self.lifecycle)?;
            let receipt_index = provider_receipts.len();
            let receipt = self
                .client
                .map_nasdaq_listings(
                    &self.extraction,
                    batch.to_vec(),
                    None,
                    wall_deadline(deadline)?,
                    cancellation.clone(),
                )
                .await?;
            for result in receipt.results() {
                let key = NasdaqListingKey::new(
                    result.job().symbol().clone(),
                    result.job().mic().clone(),
                );
                let index = listings
                    .binary_search(&key)
                    .map_err(|_| OpenFigiIdentityPublicationError::InconsistentProviderResult)?;
                match result.outcome() {
                    OpenFigiMappingOutcome::Exact(candidate) => {
                        exact.push(PendingExact {
                            result_index: index,
                            receipt_index,
                            candidate: candidate.clone(),
                        });
                    }
                    OpenFigiMappingOutcome::NoMatch => {
                        statuses[index] = Some(OpenFigiIdentityPublicationStatus::NoMatch);
                    }
                    OpenFigiMappingOutcome::Ambiguous { candidates } => {
                        statuses[index] = Some(OpenFigiIdentityPublicationStatus::Ambiguous {
                            candidates: candidates.clone().into_boxed_slice(),
                        });
                    }
                    OpenFigiMappingOutcome::Conflict { reason } => {
                        statuses[index] =
                            Some(OpenFigiIdentityPublicationStatus::ProviderConflict {
                                reason: *reason,
                            });
                    }
                    OpenFigiMappingOutcome::ProviderError { message_digest } => {
                        statuses[index] = Some(OpenFigiIdentityPublicationStatus::ProviderError {
                            message_digest: *message_digest,
                        });
                    }
                }
            }
            provider_receipts.push(receipt);
        }

        let mut figi_counts = BTreeMap::<Figi, usize>::new();
        for pending in &exact {
            let count = figi_counts
                .entry(pending.candidate.exchange_figi().clone())
                .or_default();
            *count = count
                .checked_add(1)
                .ok_or(OpenFigiIdentityPublicationError::Capacity)?;
        }
        let rights_policy = figi_rights_policy()?;
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(exact.len())
            .map_err(|_| OpenFigiIdentityPublicationError::Capacity)?;
        for pending in exact {
            ensure_open(deadline, cancellation, &self.lifecycle)?;
            let permanent_figi = pending.candidate.exchange_figi().clone();
            if figi_counts.get(&permanent_figi).copied().unwrap_or(0) != 1 {
                statuses[pending.result_index] =
                    Some(OpenFigiIdentityPublicationStatus::IdentityConflict { permanent_figi });
                continue;
            }
            let key = &listings[pending.result_index];
            let Some(currency) = self.quote_currency_policy.quote_currency_for(key) else {
                statuses[pending.result_index] =
                    Some(OpenFigiIdentityPublicationStatus::QuoteCurrencyUnavailable);
                continue;
            };
            let listing = by_key
                .get(key)
                .ok_or(OpenFigiIdentityPublicationError::InconsistentProviderResult)?;
            let provider_receipt = provider_receipts
                .get(pending.receipt_index)
                .ok_or(OpenFigiIdentityPublicationError::InconsistentProviderResult)?;
            let instrument_id = market_data_instrument_id(&permanent_figi)?;
            let reference_evidence = definition_reference_evidence(
                listing,
                provider_receipt,
                &pending.candidate,
                &currency,
            );
            let current =
                self.catalog_reader
                    .latest_by_figi(&permanent_figi, deadline, cancellation)?;
            let (definition, disposition) = if let Some(current) = current {
                if definition_matches(
                    current.definition(),
                    instrument_id,
                    listing.asset_class(),
                    &reference_evidence,
                    &currency,
                    provider_receipt,
                    &rights_policy,
                ) {
                    (
                        current.definition().clone(),
                        OpenFigiCatalogDisposition::Replayed,
                    )
                } else {
                    if provider_receipt.received_at()
                        <= current.definition().effective_interval().starts_at()
                    {
                        return Err(OpenFigiIdentityPublicationError::NonMonotonicRevision);
                    }
                    (
                        build_definition(
                            instrument_id,
                            permanent_figi.clone(),
                            listing.asset_class(),
                            reference_evidence,
                            currency,
                            provider_receipt,
                            rights_policy.clone(),
                        )?,
                        OpenFigiCatalogDisposition::InsertedOrAdvanced,
                    )
                }
            } else {
                (
                    build_definition(
                        instrument_id,
                        permanent_figi.clone(),
                        listing.asset_class(),
                        reference_evidence,
                        currency,
                        provider_receipt,
                        rights_policy.clone(),
                    )?,
                    OpenFigiCatalogDisposition::InsertedOrAdvanced,
                )
            };
            definitions.push(definition);
            statuses[pending.result_index] = Some(OpenFigiIdentityPublicationStatus::Exact {
                candidate: pending.candidate,
                instrument_id,
                catalog_disposition: disposition,
            });
        }

        let catalog_receipt = if definitions.is_empty() {
            None
        } else {
            let expected = definitions.len();
            Some(self.catalog_writer.synchronize(
                MarketDataInstrumentSynchronization::try_new(definitions, expected)?,
                deadline,
                cancellation,
            )?)
        };
        let mut results = Vec::new();
        results
            .try_reserve_exact(listings.len())
            .map_err(|_| OpenFigiIdentityPublicationError::Capacity)?;
        for (listing, status) in listings.into_iter().zip(statuses) {
            results.push(OpenFigiIdentityPublicationResult {
                listing,
                status: status
                    .ok_or(OpenFigiIdentityPublicationError::InconsistentProviderResult)?,
            });
        }
        Ok(OpenFigiIdentityPublicationReceipt {
            results: results.into_boxed_slice(),
            provider_receipts: provider_receipts.into_boxed_slice(),
            catalog_receipt,
        })
    }

    pub(crate) fn begin_shutdown(&self) {
        self.lifecycle.cancel();
    }

    pub(crate) async fn finish_shutdown(
        &self,
        deadline: Instant,
    ) -> Result<(), OpenFigiIdentityPublicationError> {
        self.lifecycle.cancel();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(OpenFigiIdentityPublicationError::DeadlineExceeded);
        }
        let _operation = tokio::time::timeout(remaining, self.operation.lock())
            .await
            .map_err(|_| OpenFigiIdentityPublicationError::DeadlineExceeded)?;
        let registry = self
            .registry
            .lock()
            .map_err(|_| OpenFigiIdentityPublicationError::ShuttingDown)?
            .take();
        if let Some(registry) = registry {
            registry.shutdown()?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PendingExact {
    result_index: usize,
    receipt_index: usize,
    candidate: OpenFigiIdentityCandidate,
}

fn normalize_selection(
    listings: &mut Vec<NasdaqListingKey>,
) -> Result<(), OpenFigiIdentityPublicationError> {
    if listings.is_empty() || listings.len() > MAXIMUM_SELECTED_LISTING_IDENTITIES {
        return Err(OpenFigiIdentityPublicationError::InvalidSelection);
    }
    listings.sort_unstable();
    listings.dedup();
    if listings.is_empty() || listings.len() > MAXIMUM_SELECTED_LISTING_IDENTITIES {
        return Err(OpenFigiIdentityPublicationError::InvalidSelection);
    }
    Ok(())
}

fn build_definition(
    instrument_id: InstrumentId,
    permanent_figi: Figi,
    asset_class: AssetClass,
    reference_evidence: ExactPayloadEvidence,
    quote_currency: EvidenceBoundQuoteCurrency,
    receipt: &OpenFigiMappingReceipt,
    rights_policy: IdentifierRightsPolicyReference,
) -> Result<MarketDataInstrumentDefinition, OpenFigiIdentityPublicationError> {
    let effective = EffectiveInterval::new(receipt.received_at(), None)
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidDefinition)?;
    let reference_revision = MetadataRevision::new(
        SourceIdentifier::try_from(DEFINITION_REFERENCE_REVISION)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
    );
    Ok(MarketDataInstrumentDefinition::try_new(
        MarketDataInstrumentDefinitionInput {
            instrument_id,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                reference_revision,
                reference_evidence,
            ),
            effective_interval: effective,
            asset_class,
            display_name: None,
            quote_currency: quote_currency.currency(),
            quote_currency_evidence: quote_currency.evidence().clone(),
            venue_mappings: Vec::new(),
            provider_identities: Vec::new(),
            identifiers: vec![ExternalIdentifierRecord::new(
                ExternalIdentifierRecordInput {
                    identifier: ExternalIdentifier::Figi(permanent_figi),
                    assignment_verification: AssignmentVerification::VerifiedAssigned,
                    source_id: receipt.source_id().clone(),
                    source_evidence: receipt.response().evidence().clone(),
                    source_timestamp: None,
                    observed_at: receipt.received_at(),
                    validity: effective,
                    rights_policy,
                },
            )],
        },
    )?)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every semantic field is compared before retaining an old observation time"
)]
fn definition_matches(
    definition: &MarketDataInstrumentDefinition,
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    reference_evidence: &ExactPayloadEvidence,
    quote_currency: &EvidenceBoundQuoteCurrency,
    receipt: &OpenFigiMappingReceipt,
    rights_policy: &IdentifierRightsPolicyReference,
) -> bool {
    if definition.instrument_id() != instrument_id
        || definition
            .reference_revision()
            .as_source_identifier()
            .as_str()
            != DEFINITION_REFERENCE_REVISION
        || definition.reference_payload_evidence() != reference_evidence
        || definition.effective_interval().ends_at().is_some()
        || definition.asset_class() != asset_class
        || definition.display_name().is_some()
        || definition.quote_currency() != quote_currency.currency()
        || definition.quote_currency_evidence().content_digest()
            != quote_currency.evidence().content_digest()
        || !definition.venue_mappings().is_empty()
        || !definition.provider_identities().is_empty()
        || definition.identifiers().len() != 1
    {
        return false;
    }
    let identifier = &definition.identifiers()[0];
    matches!(
        identifier.identifier(),
        ExternalIdentifier::Figi(figi) if figi == definition.permanent_figi()
    ) && identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
        && identifier.source_id() == receipt.source_id()
        && identifier.source_evidence() == receipt.response().evidence()
        && identifier.source_timestamp().is_none()
        && identifier.rights_policy() == rights_policy
}

fn definition_reference_evidence(
    listing: &NasdaqCurrentListing,
    receipt: &OpenFigiMappingReceipt,
    candidate: &OpenFigiIdentityCandidate,
    quote_currency: &EvidenceBoundQuoteCurrency,
) -> ExactPayloadEvidence {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/openfigi-selected-listing-definition/v1");
    update_text(&mut hasher, listing.source_id().as_str());
    update_text(
        &mut hasher,
        listing.metadata_revision().as_source_identifier().as_str(),
    );
    update_evidence(&mut hasher, listing.source_payload_evidence());
    hasher.update(listing.source_timestamp().unix_nanos().to_be_bytes());
    update_text(&mut hasher, listing.key().symbol().as_str());
    update_text(&mut hasher, listing.key().mic().as_str());
    hasher.update([asset_class_code(listing.asset_class())]);
    update_text(&mut hasher, receipt.source_id().as_str());
    update_text(
        &mut hasher,
        receipt.metadata_revision().as_source_identifier().as_str(),
    );
    update_evidence(&mut hasher, receipt.coverage_evidence());
    update_evidence(&mut hasher, receipt.request().evidence());
    update_evidence(&mut hasher, receipt.response().evidence());
    update_text(&mut hasher, candidate.exchange_figi().as_str());
    update_optional_figi(&mut hasher, candidate.composite_figi());
    update_optional_figi(&mut hasher, candidate.share_class_figi());
    update_text(&mut hasher, quote_currency.currency().as_str());
    update_evidence(&mut hasher, quote_currency.evidence());
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn source_metadata(
    public_terms: &OpenFigiPublicTermsEvidence,
) -> Result<SourceMetadata, OpenFigiIdentityPublicationError> {
    let provider = SourceIdentifier::try_from(OPENFIGI_V3_PROVIDER)
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let contract_evidence = source_contract_evidence(
        &public_terms.terms_evidence,
        &public_terms.api_documentation_evidence,
        public_terms.effective,
    )?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(
            SourceIdentifier::try_from(OPENFIGI_AUTHORIZATION_BASIS)
                .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        ),
        public_terms.terms_evidence.clone(),
        public_terms.effective,
    );
    let venues = market_squawk_adapter_nasdaq_symbols::NASDAQ_SYMBOL_DIRECTORY_VENUES
        .iter()
        .map(|venue| {
            market_squawk_domain::VenueId::try_from(*venue)
                .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let bounds = HttpRequestBounds::try_new(
        nonzero_u64(5 * SECOND_NANOS)?,
        nonzero_u64(15 * SECOND_NANOS)?,
        nonzero_u64(20 * SECOND_NANOS)?,
        0,
        nonzero_u64(
            u64::try_from(MAX_OPENFIGI_RESPONSE_BYTES)
                .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        )?,
    )
    .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let endpoint = EndpointPolicy::try_new_with_bounds([OPENFIGI_V3_MAPPING_URL], bounds)
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let request_limit = NonZeroU32::new(OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW)
        .ok_or(OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let request_window = NonZeroU64::new(OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS)
        .ok_or(OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let budget_window = ProviderBudgetWindow::try_new(
        request_limit,
        request_window,
        BudgetWindowSemantics::Sliding,
    )
    .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    let budget = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::new(provider.clone()),
        &[budget_window],
        NonZeroU16::new(1).ok_or(OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        BackoffPolicy::try_new(
            nonzero_u64(SECOND_NANOS)?,
            nonzero_u64(MINUTE_NANOS)?,
            2_000,
        )
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
    )
    .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(OPENFIGI_SOURCE_ID)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(
                SourceIdentifier::try_from(OPENFIGI_METADATA_REVISION)
                    .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
            ),
            contract_evidence.clone(),
        ),
        SourceClass::LicensedDataset,
        provider,
        authorization,
        SourceCoverage::try_instrument(
            contract_evidence,
            public_terms.effective,
            vec![AssetClass::Equity, AssetClass::Fund],
            CoverageTopology::partial_venues(venues)
                .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
            InstrumentCoverage::partial(),
            None,
            CoverageDelay::Delayed(MINUTE_NANOS),
            DeliveryEvidence::Indirect,
        )
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        DataQuality::Aggregated,
        NetworkAccessPolicy::Allowlisted(endpoint),
        FreshnessPolicy::try_new(DAY_NANOS, DAY_NANOS, DAY_NANOS, DAY_NANOS, MINUTE_NANOS)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        Some(budget),
        SourceCapabilities::new(
            false,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            false,
        ),
        SourceProtocolProfile::NotLive,
    ))
    .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)
}

fn source_contract_evidence(
    terms: &ExactPayloadEvidence,
    api_documentation: &ExactPayloadEvidence,
    effective: EffectiveInterval,
) -> Result<ExactPayloadEvidence, OpenFigiIdentityPublicationError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/openfigi-public-identity-source/v1");
    update_text(&mut hasher, OPENFIGI_V3_MAPPING_URL);
    update_text(&mut hasher, OPENFIGI_FIGI_TERMS_REFERENCE);
    update_text(&mut hasher, OPENFIGI_API_DOCUMENTATION_REFERENCE);
    hasher.update(
        u64::try_from(OPENFIGI_PUBLIC_MAX_JOBS)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?
            .to_be_bytes(),
    );
    hasher.update(OPENFIGI_PUBLIC_REQUESTS_PER_WINDOW.to_be_bytes());
    hasher.update(OPENFIGI_PUBLIC_REQUEST_WINDOW_NANOS.to_be_bytes());
    hasher.update(effective.starts_at().unix_nanos().to_be_bytes());
    match effective.ends_at() {
        Some(end) => {
            hasher.update([1]);
            hasher.update(end.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    update_evidence(&mut hasher, terms);
    update_evidence(&mut hasher, api_documentation);
    Ok(ExactPayloadEvidence::from_content_digest(
        EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into()),
    ))
}

fn evidence_matches_reference(evidence: &ExactPayloadEvidence, expected: &str) -> bool {
    evidence.content_digest().bytes() != [0; 32]
        && evidence
            .version_pinned_locator()
            .is_some_and(|locator| locator.reference().as_str() == expected)
}

fn figi_rights_policy() -> Result<IdentifierRightsPolicyReference, OpenFigiIdentityPublicationError>
{
    Ok(IdentifierRightsPolicyReference::new(
        SourceIdentifier::try_from(OPENFIGI_FIGI_POLICY_ID)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
        IdentifierEntitlement::PublicDomain,
        SourceIdentifier::try_from(OPENFIGI_FIGI_TERMS_REFERENCE)
            .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?,
    ))
}

fn update_text(hasher: &mut Sha256, value: &str) {
    hasher.update(value.as_bytes());
    hasher.update([0]);
}

fn update_evidence(hasher: &mut Sha256, evidence: &ExactPayloadEvidence) {
    hasher.update([match evidence.content_digest().algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hasher.update(evidence.content_digest().bytes());
}

fn update_optional_figi(hasher: &mut Sha256, figi: Option<&Figi>) {
    match figi {
        Some(figi) => {
            hasher.update([1]);
            update_text(hasher, figi.as_str());
        }
        None => hasher.update([0]),
    }
}

const fn asset_class_code(asset_class: AssetClass) -> u8 {
    match asset_class {
        AssetClass::Equity => 1,
        AssetClass::Fund => 2,
        AssetClass::FixedIncome => 3,
        AssetClass::Option => 4,
        AssetClass::Future => 5,
        AssetClass::ForeignExchange => 6,
        AssetClass::Crypto => 7,
        AssetClass::Commodity => 8,
        AssetClass::Index => 9,
        AssetClass::Cash => 10,
    }
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, OpenFigiIdentityPublicationError> {
    NonZeroU64::new(value).ok_or(OpenFigiIdentityPublicationError::InvalidConfiguration)
}

fn wall_deadline(deadline: Instant) -> Result<Timestamp, OpenFigiIdentityPublicationError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(OpenFigiIdentityPublicationError::DeadlineExceeded);
    }
    let nanos = i64::try_from(remaining.as_nanos())
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)?;
    system_timestamp()?
        .checked_add_nanos(nanos)
        .map_err(|_| OpenFigiIdentityPublicationError::InvalidConfiguration)
}

fn system_timestamp() -> Result<Timestamp, OpenFigiIdentityPublicationError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| OpenFigiIdentityPublicationError::Clock)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(OpenFigiIdentityPublicationError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn ensure_open(
    deadline: Instant,
    cancellation: &CancellationToken,
    lifecycle: &CancellationToken,
) -> Result<(), OpenFigiIdentityPublicationError> {
    if lifecycle.is_cancelled() {
        Err(OpenFigiIdentityPublicationError::ShuttingDown)
    } else if cancellation.is_cancelled() {
        Err(OpenFigiIdentityPublicationError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OpenFigiIdentityPublicationError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Bounded identity-resolution, evidence, or catalog-publication failure.
#[derive(Debug, Error)]
pub(crate) enum OpenFigiIdentityPublicationError {
    #[error("OpenFIGI identity publication configuration is invalid")]
    InvalidConfiguration,
    #[error("OpenFIGI listing selection is empty or exceeds its hard bound")]
    InvalidSelection,
    #[error("OpenFIGI identity publication capacity is unavailable")]
    Capacity,
    #[error("OpenFIGI returned a result outside the submitted selected listing set")]
    InconsistentProviderResult,
    #[error("changed FIGI identity evidence did not advance effective time")]
    NonMonotonicRevision,
    #[error("OpenFIGI identity definition is invalid")]
    InvalidDefinition,
    #[error("OpenFIGI identity publication wall clock is unavailable")]
    Clock,
    #[error("OpenFIGI identity publication was cancelled")]
    Cancelled,
    #[error("OpenFIGI identity publication deadline elapsed")]
    DeadlineExceeded,
    #[error("OpenFIGI identity publisher is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Provider(#[from] OpenFigiClientError),
    #[error(transparent)]
    Mapping(#[from] OpenFigiModelError),
    #[error(transparent)]
    Nasdaq(#[from] NasdaqReferenceUniverseError),
    #[error(transparent)]
    Catalog(#[from] MarketDataInstrumentCatalogError),
    #[error(transparent)]
    Definition(#[from] MarketDataInstrumentDefinitionError),
}
