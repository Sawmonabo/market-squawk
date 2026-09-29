//! Original issuer facts shared by canonical reference admission and provider corroboration.
//!
//! These actual issuer snapshots establish a US-listed fund's CUSIP, ticker, venue and USD
//! quotation. They are neither current price observations nor provider exchange-code mappings.
//! Display text retains the admitted official directory or native provider provenance.

use super::*;
use market_squawk_domain::{
    Currency, Cusip, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
    Ticker, Timestamp, VenueId, VersionPinnedSourceLocator,
};
use market_squawk_domain::{ExternalIdentifierRecordInput, IdentifierRightsPolicyReference};
type Error = MarketDataInstrumentCatalogError;
use sha2::{Digest as _, Sha256};

/// Immutable source-qualified acquisition scope. No constructor accepts caller-authored facts.
#[derive(Clone, Debug)]
pub struct OfficialIssuerInstrumentReference {
    source_id: SourceId,
    cusip: Cusip,
    symbol: Ticker,
    venue: VenueId,
    quote_currency: Currency,
    identity_evidence: ExactPayloadEvidence,
    currency_evidence: ExactPayloadEvidence,
    observed_at: Timestamp,
    identity_bytes: &'static [u8],
    currency_bytes: &'static [u8],
}

impl OfficialIssuerInstrumentReference {
    /// Returns the owner-predeclared primary and accompanying acquisition scopes in that order.
    /// These original documents admit only their own exact securities. Wider issuer coverage is
    /// added here with original evidence; callers cannot supply identity or currency assertions.
    /// Canonical IDs are allocated only by the catalog publication transaction.
    pub fn predeclared_benchmarks() -> Result<[Self; 2], Error> {
        let spy = exact_document(
            include_bytes!("issuer_reference/spy-listing.html"),
            "043dc2e35b96394b401afe7207dff0b76ed7fc07eb18855a9d01d8d7ad6b5cbd",
            "https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy",
        )?;
        let vti_currency = exact_document(
            include_bytes!("issuer_reference/vti-currency.pdf"),
            "a1beb701fc5954853e9f97bb58a9bb029a50c17a5724ba3be37f05885d5c6b7f",
            "https://www.vanguardmexico.com/content/dam/intl/americas/documents/mexico/en/brochure-vanguard-portfolio-solutions-v2.pdf",
        )?;
        let vti_identity = exact_document(
            include_bytes!("issuer_reference/vti-listing.pdf"),
            "0a21a4b9ac79389469774307b40bb91ea81999a339f87f5fe4df3006e7b60ee1",
            "https://fund-docs.vanguard.com/FA0970_MX.pdf",
        )?;
        Ok([
            Self::from_verified_rule(
                "ssga-official-fund-reference",
                "78462F103",
                "SPY",
                spy.clone(),
                spy,
                1_788_855_645_798_340_000,
                include_bytes!("issuer_reference/spy-listing.html"),
                include_bytes!("issuer_reference/spy-listing.html"),
            )?,
            Self::from_verified_rule(
                "vanguard-official-fund-reference",
                "922908769",
                "VTI",
                vti_identity,
                vti_currency,
                1_788_855_647_481_313_000,
                include_bytes!("issuer_reference/vti-listing.pdf"),
                include_bytes!("issuer_reference/vti-currency.pdf"),
            )?,
        ])
    }

    fn from_verified_rule(
        source_id: &str,
        cusip: &str,
        symbol: &str,
        identity_evidence: ExactPayloadEvidence,
        currency_evidence: ExactPayloadEvidence,
        observed_at: i64,
        identity_bytes: &'static [u8],
        currency_bytes: &'static [u8],
    ) -> Result<Self, Error> {
        Ok(Self {
            source_id: SourceId::try_from(source_id).map_err(|_| Error::InvalidInput)?,
            cusip: Cusip::try_from(cusip).map_err(|_| Error::InvalidInput)?,
            symbol: Ticker::try_from(symbol).map_err(|_| Error::InvalidInput)?,
            venue: VenueId::try_from("ARCX").map_err(|_| Error::InvalidInput)?,
            quote_currency: Currency::try_from("USD").map_err(|_| Error::InvalidInput)?,
            identity_evidence,
            currency_evidence,
            observed_at: Timestamp::from_unix_nanos(observed_at),
            identity_bytes,
            currency_bytes,
        })
    }

    pub const fn cusip(&self) -> &Cusip {
        &self.cusip
    }
    pub const fn symbol(&self) -> &Ticker {
        &self.symbol
    }
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }
    pub const fn identity_evidence(&self) -> &ExactPayloadEvidence {
        &self.identity_evidence
    }
    pub const fn currency_evidence(&self) -> &ExactPayloadEvidence {
        &self.currency_evidence
    }
    pub const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }

    pub fn validate_listing(
        &self,
        listing: &ListingReferenceRecord,
        observed_at: Timestamp,
    ) -> Result<(), Error> {
        if observed_at < self.observed_at
            || listing.generation().published_at() > observed_at
            || listing.effective_at() > observed_at
            || listing.is_test_issue()
            || !listing.is_etf()
            || listing.listing_venue() != &self.venue
            || listing.provider_symbol() != self.symbol.as_str()
        {
            return Err(Error::SourceIdentityConflict);
        }
        Ok(())
    }
}

fn exact_document(
    bytes: &[u8],
    expected_sha256: &str,
    official_url: &str,
) -> Result<ExactPayloadEvidence, Error> {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    let hex = hex_digest(&digest);
    if hex != expected_sha256 {
        return Err(Error::CorruptCatalog);
    }
    Ok(ExactPayloadEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        VersionPinnedSourceLocator::new(
            SourceIdentifier::try_from(official_url).map_err(|_| Error::InvalidInput)?,
            SourceIdentifier::try_from(format!("sha256:{hex}").as_str())
                .map_err(|_| Error::InvalidInput)?,
        ),
    ))
}

fn hex_digest(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Admits original issuer facts against one authentic current official listing.
    /// No caller can supply a canonical ID, currency, or external identifier to this path.
    pub fn publish_issuer_reference(
        &self,
        issuer: OfficialIssuerInstrumentReference,
        listing: ListingReferenceRecord,
        expected_current: Option<MarketDataInstrumentRecord>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, Error> {
        check_operation(deadline, cancellation)?;
        let reader = ListingReferenceReadCapability::new(
            Arc::clone(&self.authority),
            listing.generation().dataset().clone(),
            listing.generation().source_id().clone(),
        );
        self.authority
            .try_lock()
            .map_err(|_| Error::AuthorityUnavailable)?
            .publish_market_data_issuer_reference(
                issuer,
                listing,
                expected_current,
                &reader,
                deadline,
                cancellation,
            )
    }
}

impl CatalogAuthority {
    fn publish_market_data_issuer_reference(
        &self,
        issuer: OfficialIssuerInstrumentReference,
        listing: ListingReferenceRecord,
        expected_current: Option<MarketDataInstrumentRecord>,
        reader: &ListingReferenceReadCapability,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, Error> {
        let connection = &self.catalog().connection;
        connection.busy_timeout(std::time::Duration::ZERO)?;
        let result = (|| {
            install_progress_handler(connection, deadline, cancellation)?;
            reader.require_current_in_catalog(
                self,
                listing.generation(),
                deadline,
                cancellation,
            )?;
            let transaction = connection.unchecked_transaction()?;
            let admitted_at = trusted_catalog_now(&transaction)?;
            issuer.validate_listing(&listing, admitted_at)?;
            require_listing_membership(&transaction, &listing)?;
            let listing_policy = listing_rights_policy(&transaction, &listing, admitted_at)?;
            let mut budget = ResultBudget::new(self.catalog().result_bytes);
            let provider_symbol = ProviderInstrumentId::try_from(issuer.symbol.as_str())
                .map_err(|_| Error::InvalidInput)?;
            let current = resolve_reference_identity(
                &transaction,
                issuer.cusip.as_str(),
                issuer.symbol.as_str(),
                &listing,
                listing.generation().source_id(),
                &provider_symbol,
                admitted_at,
                &mut budget,
            )?;
            if current != expected_current {
                return Err(Error::ReferencePositionConflict);
            }

            // Preserve an already admitted identity and every provider's original evidence.
            // A fresh directory observation is not a reason to rewrite canonical history.
            let result = if let Some(current) = current {
                let definition = current.definition();
                if definition.asset_class() != AssetClass::Fund
                    || definition.quote_currency() != issuer.quote_currency
                    || !definition.identifiers().iter().any(|record|
                        matches!(record.identifier(), ExternalIdentifier::Cusip(value) if value == &issuer.cusip)
                        && record.assignment_verification() == AssignmentVerification::VerifiedAssigned
                        && interval_contains(record.validity(), admitted_at))
                    || !definition.venue_mappings().iter().any(|mapping|
                        mapping.venue_id() == &issuer.venue && mapping.venue_symbol().as_str() == issuer.symbol.as_str())
                { return Err(Error::SourceIdentityConflict); }
                current
            } else {
                let instrument_id = InstrumentId::try_from(uuid::Uuid::new_v4())
                    .map_err(|_| Error::InvalidInput)?;
                let effective_at =
                    std::cmp::max(issuer.observed_at, listing.source_file().available_at());
                if effective_at > admitted_at {
                    return Err(Error::InvalidInput);
                }
                let validity =
                    EffectiveInterval::new(effective_at, None).map_err(|_| Error::InvalidInput)?;
                let mut revision = Sha256::new();
                revision.update(b"market-squawk/official-issuer-reference/v1\0");
                revision.update(listing.generation().generation_digest().bytes());
                revision.update(listing.record_payload_evidence().content_digest().bytes());
                revision.update(issuer.identity_evidence.content_digest().bytes());
                revision.update(issuer.currency_evidence.content_digest().bytes());
                revision.update(issuer.observed_at.unix_nanos().to_be_bytes());
                let revision: [u8; 32] = revision.finalize().into();
                let issuer_policy = IdentifierRightsPolicyReference::new(
                    SourceIdentifier::try_from("owner-authorized-issuer-reference-local-use-v1")
                        .map_err(|_| Error::InvalidInput)?,
                    IdentifierEntitlement::LicensedInternalUse,
                    issuer
                        .identity_evidence
                        .version_pinned_locator()
                        .ok_or(Error::InvalidInput)?
                        .reference()
                        .clone(),
                );
                let identifiers = vec![
                    ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
                        identifier: ExternalIdentifier::Cusip(issuer.cusip.clone()),
                        assignment_verification: AssignmentVerification::VerifiedAssigned,
                        source_id: issuer.source_id.clone(),
                        source_evidence: issuer.identity_evidence.clone(),
                        source_timestamp: None,
                        observed_at: issuer.observed_at,
                        validity,
                        rights_policy: issuer_policy,
                    }),
                    ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
                        identifier: ExternalIdentifier::Ticker(issuer.symbol.clone()),
                        assignment_verification: AssignmentVerification::VerifiedAssigned,
                        source_id: listing.generation().source_id().clone(),
                        source_evidence: listing.record_payload_evidence().clone(),
                        source_timestamp: Some(listing.effective_at()),
                        observed_at: listing.source_file().received_at(),
                        validity,
                        rights_policy: listing_policy.clone(),
                    }),
                ];
                let definition =
                    MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                        instrument_id,
                        reference_evidence: RevisionBoundPayloadEvidence::new(
                            MetadataRevision::new(
                                SourceIdentifier::try_from(
                                    format!(
                                        "official-issuer-reference-sha256:{}",
                                        hex_digest(&revision)
                                    )
                                    .as_str(),
                                )
                                .map_err(|_| Error::InvalidInput)?,
                            ),
                            issuer.identity_evidence.clone(),
                        ),
                        effective_interval: validity,
                        asset_class: AssetClass::Fund,
                        display_name: Some(
                            MarketDataDisplayName::try_new(
                                listing.display_name(),
                                listing.generation().source_id().clone(),
                                listing.record_payload_evidence().clone(),
                                listing_policy,
                            )
                            .map_err(|_| Error::InvalidInput)?,
                        ),
                        quote_currency: issuer.quote_currency,
                        quote_currency_evidence: issuer.currency_evidence.clone(),
                        venue_mappings: vec![VenueMapping::new(
                            issuer.venue.clone(),
                            VenueSymbol::try_from(issuer.symbol.as_str())
                                .map_err(|_| Error::InvalidInput)?,
                        )],
                        provider_identities: Vec::new(),
                        identifiers,
                    })
                    .map_err(|_| Error::InvalidInput)?;
                let prepared = prepare_definitions(vec![definition].into_boxed_slice())?
                    .pop()
                    .ok_or(Error::InvalidInput)?;
                if prepared.json.len() > self.catalog().result_bytes.max_record_bytes() {
                    return Err(Error::ResultByteLimitExceeded);
                }
                let PublicationPlan::Insert {
                    sequence,
                    previous,
                    identity_is_new,
                } = plan_publication(&transaction, &prepared)?
                else {
                    return Err(Error::CorruptCatalog);
                };
                insert_definition(
                    &transaction,
                    &self.provider_identity_generation,
                    &prepared,
                    sequence,
                    previous,
                    identity_is_new,
                    admitted_at,
                )?;
                MarketDataInstrumentRecord {
                    definition: prepared.definition,
                    revision_digest: digest(prepared.digest),
                    revision_sequence: sequence,
                    published_at: admitted_at,
                }
            };
            for (evidence, bytes) in [
                (&issuer.identity_evidence, issuer.identity_bytes),
                (&issuer.currency_evidence, issuer.currency_bytes),
            ] {
                retain_document(
                    &transaction,
                    evidence,
                    bytes,
                    issuer.observed_at,
                    admitted_at,
                )?;
            }
            let changed = transaction.execute(
                "INSERT OR IGNORE INTO market_data_issuer_reference_admissions
                 (revision_digest, listing_generation_digest, listing_record_revision, identity_document_digest, currency_document_digest, admitted_at_ns)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![result.revision_digest().bytes(), listing.generation().generation_digest().bytes(), listing.record_revision().as_str(), issuer.identity_evidence.content_digest().bytes(), issuer.currency_evidence.content_digest().bytes(), admitted_at.unix_nanos()],
            )?;
            let exact: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM market_data_issuer_reference_admissions WHERE revision_digest=?1 AND listing_generation_digest=?2 AND listing_record_revision=?3 AND identity_document_digest=?4 AND currency_document_digest=?5 AND admitted_at_ns<=?6)",
                params![result.revision_digest().bytes(), listing.generation().generation_digest().bytes(), listing.record_revision().as_str(), issuer.identity_evidence.content_digest().bytes(), issuer.currency_evidence.content_digest().bytes(), admitted_at.unix_nanos()], |row| row.get(0),
            )?;
            if !exact {
                return Err(Error::CorruptCatalog);
            }
            if changed != 0 {
                let mut audit = Sha256::new();
                for value in [
                    result.revision_digest(),
                    listing.generation().generation_digest(),
                    listing.record_payload_evidence().content_digest(),
                    issuer.identity_evidence.content_digest(),
                    issuer.currency_evidence.content_digest(),
                ] {
                    audit.update(value.bytes());
                }
                append_audit(
                    &transaction,
                    "market-data-instrument.issuer-reference-admitted",
                    &result.definition().instrument_id().to_string(),
                    audit.finalize().into(),
                    admitted_at,
                )?;
            }
            check_operation(deadline, cancellation)?;
            reader.require_current_in_catalog(
                self,
                listing.generation(),
                deadline,
                cancellation,
            )?;
            listing_rights_policy(&transaction, &listing, trusted_catalog_now(&transaction)?)?;
            transaction.commit()?;
            Ok(result)
        })();
        let progress_cleanup = clear_progress_handler(connection);
        let busy_cleanup = connection.busy_timeout(self.catalog().busy_timeout);
        let result = classify_operation(result, deadline, cancellation);
        progress_cleanup?;
        busy_cleanup?;
        result
    }
}

fn require_listing_membership(
    transaction: &Transaction<'_>,
    listing: &ListingReferenceRecord,
) -> Result<(), Error> {
    let present: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM listing_reference_memberships WHERE generation_digest=?1 AND file_kind=?2 AND provider_row_number=?3 AND provider_symbol=?4 AND record_revision=?5 AND record_algorithm=?6 AND record_payload_digest=?7)",
        params![listing.generation().generation_digest().bytes(), listing.source_file().kind().database_name(), listing.provider_row_number(), listing.provider_symbol(), listing.record_revision().as_str(), match listing.record_payload_evidence().content_digest().algorithm() { DigestAlgorithm::Sha256 => 1, DigestAlgorithm::Blake3 => 2 }, listing.record_payload_evidence().content_digest().bytes()],
        |row| row.get(0),
    )?;
    if !present {
        return Err(Error::SourceIdentityConflict);
    }
    Ok(())
}

fn listing_rights_policy(
    transaction: &Transaction<'_>,
    listing: &ListingReferenceRecord,
    at: Timestamp,
) -> Result<IdentifierRightsPolicyReference, Error> {
    let basis: Option<String> = transaction.query_row(
        "SELECT basis_reference FROM source_rights WHERE rights_id=?1 AND source_id=?2 AND (operation_mask & 6)=6 AND admitted_at_ns<=?3 AND (authorization_expires_at_ns IS NULL OR authorization_expires_at_ns>?3)",
        params![listing.generation().rights_id(), listing.generation().source_id().as_str(), at.unix_nanos()], |row| row.get(0),
    ).optional()?;
    Ok(IdentifierRightsPolicyReference::new(
        SourceIdentifier::try_from(
            format!(
                "authorization-sha256:{}",
                hex_digest(&listing.generation().rights_id())
            )
            .as_str(),
        )
        .map_err(|_| Error::InvalidInput)?,
        IdentifierEntitlement::LicensedInternalUse,
        SourceIdentifier::try_from(basis.ok_or(Error::SourceIdentityConflict)?.as_str())
            .map_err(|_| Error::InvalidInput)?,
    ))
}

fn retain_document(
    transaction: &Transaction<'_>,
    evidence: &ExactPayloadEvidence,
    bytes: &[u8],
    observed_at: Timestamp,
    at: Timestamp,
) -> Result<(), Error> {
    let locator = evidence
        .version_pinned_locator()
        .ok_or(Error::InvalidInput)?;
    if bytes.is_empty()
        || bytes.len() > 8 * 1024 * 1024
        || observed_at > at
        || evidence.content_digest().algorithm() != DigestAlgorithm::Sha256
        || <[u8; 32]>::from(Sha256::digest(bytes)) != evidence.content_digest().bytes()
        || locator.version().as_str()
            != format!("sha256:{}", hex_digest(&evidence.content_digest().bytes()))
    {
        return Err(Error::CorruptCatalog);
    }
    transaction.execute(
        "INSERT OR IGNORE INTO market_data_issuer_documents(document_digest,source_reference,observed_at_ns,payload,retained_at_ns) VALUES (?1,?2,?3,?4,?5)",
        params![evidence.content_digest().bytes(), locator.reference().as_str(), observed_at.unix_nanos(), bytes, at.unix_nanos()],
    )?;
    let exact: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM market_data_issuer_documents WHERE document_digest=?1 AND source_reference=?2 AND observed_at_ns=?3 AND payload=?4 AND retained_at_ns<=?5)",
        params![evidence.content_digest().bytes(), locator.reference().as_str(), observed_at.unix_nanos(), bytes, at.unix_nanos()], |row| row.get(0),
    )?;
    if !exact {
        return Err(Error::CorruptCatalog);
    }
    Ok(())
}
