//! Canonical identities admitted from complete original Alpaca contract reference captures.
//!
//! This is the existing non-execution instrument catalog writer, not an execution-term factory.
//! The adapter's constructor-private original proof supplies assigned identifiers. The catalog
//! allocates internal UUIDs only after exact original custody, current source/rights, and an
//! independently admitted USD equity/fund underlying have been joined inside one transaction.

use super::*;
use crate::ProviderCaptureOriginalReceipt;
use market_squawk_adapter_alpaca::{
    AlpacaOptionContractReferenceSet, AlpacaOriginalOptionContract,
};
use market_squawk_domain::{
    ExternalIdentifierRecordInput, IdentifierRightsPolicyReference, OccOptionIdentity,
    SourceIdentifier, VersionPinnedSourceLocator,
};

use sha2::{Digest as _, Sha256};

type Error = MarketDataInstrumentCatalogError;

/// Complete source-owned reference graph and the existing authority that permits its admission.
/// No caller-provided canonical option ID, currency, external identifier, or economics is accepted.
pub struct AlpacaOptionReferenceAdmission {
    /// Exact registered Alpaca source generation.
    pub source: SourceMetadata,
    /// One current rights decision for each original page, in original page order.
    pub rights: Vec<RightsDecisionInput>,
    /// Exact catalog-owned original custody receipts, in complete session order.
    pub originals: Vec<ProviderCaptureOriginalReceipt>,
    /// Opaque complete original response proof, shared without copying its decoded rows.
    pub contracts: Arc<AlpacaOptionContractReferenceSet>,
    /// Exact current non-execution underlying definition selected by the ordinary caller.
    pub underlying: MarketDataInstrumentRecord,
    /// Exact registered IEX source whose authenticated asset UUID assertion owns the underlying.
    pub underlying_asset_namespace: SourceId,
}

impl fmt::Debug for AlpacaOptionReferenceAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AlpacaOptionReferenceAdmission")
            .field("source", self.source.source_id())
            .field("original_pages", &self.originals.len())
            .field("underlying", &self.underlying.definition().instrument_id())
            .field(
                "underlying_asset_namespace",
                &self.underlying_asset_namespace,
            )
            .finish_non_exhaustive()
    }
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Atomically creates or corroborates the complete original contract set in this catalog.
    /// Source facts and original custody remain separate from live publication authorization.
    pub fn publish_alpaca_option_references(
        &self,
        input: AlpacaOptionReferenceAdmission,
        precommit: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSynchronizationReceipt, Error> {
        check_operation(deadline, cancellation)?;
        precommit
            .validate_precommit()
            .map_err(reference_precommit_error)?;
        self.authority
            .try_lock()
            .map_err(|_| Error::AuthorityUnavailable)?
            .publish_alpaca_option_references(input, precommit, deadline, cancellation)
    }
}

impl CatalogAuthority {
    fn publish_alpaca_option_references(
        &self,
        input: AlpacaOptionReferenceAdmission,
        precommit: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentSynchronizationReceipt, Error> {
        let count = input.contracts.contracts().count();
        if count > MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS
            || input.originals.is_empty()
            || input.originals.len() != input.contracts.captures().count()
            || input.rights.len() != input.originals.len()
            || input.source.provider().as_str() != "alpaca-market-data"
            || !matches!(
                input.underlying.definition().asset_class(),
                AssetClass::Equity | AssetClass::Fund
            )
            || input.underlying.definition().quote_currency().as_str() != "USD"
        {
            return Err(Error::InvalidInput);
        }
        let connection = &self.catalog().connection;
        connection.busy_timeout(std::time::Duration::ZERO)?;
        let result = (|| {
            install_progress_handler(connection, deadline, cancellation)?;
            precommit
                .validate_catalog_precommit(self)
                .map_err(reference_precommit_error)?;
            require_current_market_data_instrument(
                self,
                &input.underlying,
                deadline,
                cancellation,
            )?;
            let asset_source = self
                .catalog()
                .source(&input.underlying_asset_namespace)?
                .ok_or(Error::SourceIdentityConflict)?;
            let [asset_venue] = asset_source.coverage().topology().venues() else {
                return Err(Error::SourceIdentityConflict);
            };
            if asset_source.provider().as_str() != "alpaca-market-data"
                || !asset_source.capabilities().live()
                || asset_venue.as_str() != "iex"
            {
                return Err(Error::SourceIdentityConflict);
            }
            if self.catalog().source(input.source.source_id())?.as_ref() != Some(&input.source) {
                return Err(Error::SourceIdentityConflict);
            }
            let transaction = connection.unchecked_transaction()?;
            let admitted_at = trusted_catalog_now(&transaction)?;
            if !input.source.is_effective_at(admitted_at) {
                return Err(Error::SourceIdentityConflict);
            }
            // Only one bounded page-level authority per physical original; no per-contract raw clone.
            let mut rights = Vec::new();
            rights
                .try_reserve_exact(input.rights.len())
                .map_err(|_| Error::ResultByteLimitExceeded)?;
            for (index, ((original, capture), decision)) in input
                .originals
                .iter()
                .zip(input.contracts.captures())
                .zip(input.rights.iter())
                .enumerate()
            {
                check_operation(deadline, cancellation)?;
                let retained = super::super::provider_capture::original::load(
                    &transaction,
                    original.session(),
                    original.ordinal(),
                )?
                .ok_or(Error::SourceIdentityConflict)?;
                let [page] = capture.capture().pages() else {
                    return Err(Error::InvalidInput);
                };
                if retained != *original
                    || original.ordinal() as usize != index
                    || usize::from(original.expected_count()) != input.originals.len()
                    || original.session() != input.originals[0].session()
                    || original.dataset().as_str() != "market_squawk.option_snapshots"
                    || original.capture() != capture.capture()
                    || original.physical().sealed_capture_receipt_digest()
                        != capture.receipt_digest()
                    || capture.capture().source_id() != input.source.source_id()
                    || capture.capture().metadata_revision() != input.source.revision()
                    || decision.source_id != *input.source.source_id()
                    || decision.payload_digest != capture.capture().observation_digest()
                    || decision.retrieved_at != page.received_at()
                    || !matches!(&decision.basis, crate::RightsBasis::ReviewedTerms(_))
                    || page.http_status() != 200
                    || page.received_at() > admitted_at
                    || !input.source.is_effective_at(page.received_at())
                {
                    return Err(Error::SourceIdentityConflict);
                }
                let decision = SourceRightsDecision::try_new(decision.clone())
                    .map_err(super::super::CatalogError::from)?;
                require_option_rights(&decision, admitted_at)?;
                require_unique_underlying(
                    &transaction,
                    &input.underlying,
                    input.contracts.request().underlying_symbol(),
                    page.received_at(),
                    &mut ResultBudget::new(self.catalog().result_bytes),
                )?;
                rights.push(decision);
            }
            let mut batch_digest = Sha256::new();
            batch_digest.update(b"market-squawk/alpaca-option-reference-batch/v1\0");
            batch_digest.update(input.originals[0].session().bytes());
            batch_digest.update(input.underlying.revision_digest().bytes());
            let mut inserted = 0usize;
            let mut replayed = 0usize;
            let mut underlying_asset_id = None;
            // Decode/prepare/insert one bounded definition at a time. All rows commit together;
            // any failed identity/rights/currentness check rolls back the entire reference batch.
            for original in input.contracts.contracts() {
                check_operation(deadline, cancellation)?;
                let page_index = input
                    .originals
                    .iter()
                    .position(|receipt| {
                        receipt.capture() == original.capture().capture()
                            && receipt.physical().sealed_capture_receipt_digest()
                                == original.capture().receipt_digest()
                    })
                    .ok_or(Error::SourceIdentityConflict)?;
                if original.underlying_symbol() != input.contracts.request().underlying_symbol()
                    || underlying_asset_id.is_some_and(|id| id != original.underlying_asset_id())
                {
                    return Err(Error::SourceIdentityConflict);
                }
                underlying_asset_id = Some(original.underlying_asset_id());
                validate_underlying(
                    &input.underlying,
                    &input.underlying_asset_namespace,
                    original,
                )?;
                let current = resolve_option_identity(
                    &transaction,
                    input.source.source_id(),
                    original,
                    &mut ResultBudget::new(self.catalog().result_bytes),
                )?;
                let record = if let Some(current) = current.as_ref()
                    && current_option_reference_matches(
                        current,
                        input.source.source_id(),
                        original,
                        &input.underlying,
                    )? {
                    replayed = replayed
                        .checked_add(1)
                        .ok_or(Error::ResultByteLimitExceeded)?;
                    current.clone()
                } else {
                    let instrument_id = current
                        .as_ref()
                        .map(|record| record.definition().instrument_id())
                        .map(Ok)
                        .unwrap_or_else(|| {
                            InstrumentId::try_from(uuid::Uuid::new_v4())
                                .map_err(|_| Error::InvalidInput)
                        })?;
                    let definition = option_definition(
                        &input,
                        original,
                        page_index,
                        &rights[page_index],
                        instrument_id,
                        current.as_ref(),
                    )?;
                    let mut definitions = prepare_definitions(vec![definition].into_boxed_slice())?;
                    let prepared = definitions.pop().ok_or(Error::InvalidInput)?;
                    if prepared.json.len() > self.catalog().result_bytes.max_record_bytes() {
                        return Err(Error::ResultByteLimitExceeded);
                    }
                    let (sequence, previous, identity_is_new) =
                        match plan_publication(&transaction, &prepared)? {
                            PublicationPlan::Insert {
                                sequence,
                                previous,
                                identity_is_new,
                            } => (sequence, previous, identity_is_new),
                            PublicationPlan::Replay => return Err(Error::CorruptCatalog),
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
                    inserted = inserted
                        .checked_add(1)
                        .ok_or(Error::ResultByteLimitExceeded)?;
                    MarketDataInstrumentRecord {
                        definition: prepared.definition,
                        revision_digest: digest(prepared.digest),
                        revision_sequence: sequence,
                        published_at: admitted_at,
                    }
                };
                batch_digest.update(record.definition().instrument_id().as_uuid().as_bytes());
                batch_digest.update(record.revision_digest().bytes());
            }
            check_operation(deadline, cancellation)?;
            precommit
                .validate_catalog_precommit(self)
                .map_err(reference_precommit_error)?;
            require_current_market_data_instrument(
                self,
                &input.underlying,
                deadline,
                cancellation,
            )?;
            let commit_at = trusted_catalog_now(&transaction)?;
            if !input.source.is_effective_at(commit_at)
                || self.catalog().source(input.source.source_id())?.as_ref() != Some(&input.source)
            {
                return Err(Error::SourceIdentityConflict);
            }
            for decision in &rights {
                require_option_rights(decision, commit_at)?;
                super::super::storage::persist_rights(&transaction, decision, commit_at)?;
            }
            let batch_digest =
                EvidenceDigest::new(DigestAlgorithm::Sha256, batch_digest.finalize().into());
            append_audit(
                &transaction,
                "market-data-instrument.alpaca-option-references-published",
                &hex(input.originals[0].session().bytes()),
                batch_digest.bytes(),
                commit_at,
            )?;
            check_operation(deadline, cancellation)?;
            precommit
                .validate_catalog_precommit(self)
                .map_err(reference_precommit_error)?;
            transaction.commit()?;
            if inserted != 0 {
                self.catalog()
                    .publication_observer
                    .record(crate::DataPublication::Reference);
            }
            Ok(MarketDataInstrumentSynchronizationReceipt {
                batch_digest,
                submitted: count,
                inserted,
                replayed,
            })
        })();
        let progress_cleanup = clear_progress_handler(connection);
        let busy_cleanup = connection.busy_timeout(self.catalog().busy_timeout);
        let result = classify_operation(result, deadline, cancellation);
        progress_cleanup?;
        busy_cleanup?;
        result
    }
}

fn require_option_rights(rights: &SourceRightsDecision, at: Timestamp) -> Result<(), Error> {
    for operation in [SourceOperation::Persist, SourceOperation::Display] {
        let request = crate::IngestIdentity::try_new(
            rights.source_id().clone(),
            rights.payload_digest(),
            operation,
            "market-data-alpaca-option-reference",
        )
        .map_err(super::super::CatalogError::from)?;
        rights
            .authorize_at(&request, at)
            .map_err(super::super::CatalogError::from)?;
    }
    Ok(())
}

fn validate_underlying(
    record: &MarketDataInstrumentRecord,
    source: &SourceId,
    original: &AlpacaOriginalOptionContract,
) -> Result<(), Error> {
    let definition = record.definition();
    let at = original.received_at();
    if record.published_at() > at || !interval_contains(definition.effective_interval(), at)
        || definition.provider_identity_at(
            source,
            &ProviderInstrumentId::try_from(original.underlying_asset_id().to_string())
                .map_err(|_| Error::InvalidInput)?,
            at,
        ).is_none()
        || !definition.identifiers().iter().any(|identifier|
            matches!(identifier.identifier(), ExternalIdentifier::Ticker(ticker) if ticker.as_str() == original.underlying_symbol())
            && identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
            && identifier.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted
            && interval_contains(identifier.validity(), at))
        || original.quote_currency().map_err(|_| Error::InvalidInput)? != definition.quote_currency()
        || original.quote_currency_evidence().map_err(|_| Error::InvalidInput)?.version_pinned_locator().is_none()
    { return Err(Error::SourceIdentityConflict); }
    Ok(())
}

// The captured underlying alias must have exactly one currently admitted assigned identity.
fn require_unique_underlying(
    transaction: &Transaction<'_>,
    expected: &MarketDataInstrumentRecord,
    symbol: &str,
    at: Timestamp,
    budget: &mut ResultBudget,
) -> Result<(), Error> {
    let sql = format!("SELECT DISTINCT {STORED_COLUMNS}
        FROM market_data_instrument_current AS current_
        JOIN market_data_instrument_revisions AS revisions ON revisions.revision_digest=current_.revision_digest
        JOIN market_data_instrument_search_terms AS terms ON terms.revision_digest=current_.revision_digest
        WHERE terms.term_kind='external_identifier' AND terms.normalized_term=?1
        ORDER BY revisions.instrument_id LIMIT 3");
    let mut statement = transaction.prepare(&sql)?;
    let rows = statement.query_map([normalize(symbol)], decode_stored_row)?;
    let mut selected = None;
    let mut scanned = 0usize;
    for row in rows {
        scanned += 1;
        if scanned == 3 {
            return Err(Error::SourceIdentityConflict);
        }
        let row = row?;
        charge_row(&row, budget)?;
        let record = rebuild_record(row)?;
        if record.published_at() > at
            || !interval_contains(record.definition().effective_interval(), at)
        {
            continue;
        }
        let assigned = record.definition().identifiers().iter().any(|identifier|
            matches!(identifier.identifier(), ExternalIdentifier::Ticker(ticker) if ticker.as_str() == symbol)
            && identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
            && identifier.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted
            && interval_contains(identifier.validity(), at));
        if !assigned {
            continue;
        }
        if selected.is_some() || record != *expected {
            return Err(Error::SourceIdentityConflict);
        }
        selected = Some(record);
    }
    if selected.is_none() {
        return Err(Error::SourceIdentityConflict);
    }
    Ok(())
}

fn resolve_option_identity(
    transaction: &Transaction<'_>,
    source: &SourceId,
    original: &AlpacaOriginalOptionContract,
    budget: &mut ResultBudget,
) -> Result<Option<MarketDataInstrumentRecord>, Error> {
    let sql = format!("SELECT DISTINCT {STORED_COLUMNS}
        FROM market_data_instrument_current AS current_
        JOIN market_data_instrument_revisions AS revisions ON revisions.revision_digest=current_.revision_digest
        JOIN market_data_instrument_search_terms AS terms ON terms.revision_digest=current_.revision_digest
        WHERE (terms.term_kind='external_identifier' AND terms.normalized_term=?1)
           OR (terms.term_kind='provider_symbol' AND terms.source_id=?2 AND terms.display_term IN (?3,?4))
        ORDER BY revisions.instrument_id LIMIT 3");
    let mut statement = transaction.prepare(&sql)?;
    let rows = statement.query_map(
        params![
            normalize(&original.occ_identity().to_string()),
            source.as_str(),
            original.symbol(),
            original.id().to_string()
        ],
        decode_stored_row,
    )?;
    let mut selected = None;
    for row in rows {
        let row = row?;
        charge_row(&row, budget)?;
        let record = rebuild_record(row)?;
        if selected.is_some()
            || record.definition().asset_class() != AssetClass::Option
            || !interval_contains(
                record.definition().effective_interval(),
                original.received_at(),
            )
            || record.definition().quote_currency()
                != original.quote_currency().map_err(|_| Error::InvalidInput)?
            || !assigned_occ_matches(
                record.definition(),
                original.occ_identity(),
                original.received_at(),
            )
        {
            return Err(Error::SourceIdentityConflict);
        }
        // Provider ID reassignment or a different verified OCC assignment is not a harmless refresh.
        for identity in record.definition().provider_identities() {
            if identity.source_id() == source
                && interval_contains(identity.validity(), original.received_at())
            {
                let id = identity.provider_instrument_id().as_str();
                if (uuid::Uuid::parse_str(id).is_ok() && id != original.id().to_string())
                    || (uuid::Uuid::parse_str(id).is_err() && id != original.symbol())
                {
                    return Err(Error::SourceIdentityConflict);
                }
            }
        }
        selected = Some(record);
    }
    Ok(selected)
}

fn assigned_occ_matches(
    definition: &MarketDataInstrumentDefinition,
    occ: &OccOptionIdentity,
    at: Timestamp,
) -> bool {
    let mut matched = false;
    for identifier in definition.identifiers() {
        if identifier.assignment_verification() != AssignmentVerification::VerifiedAssigned
            || !interval_contains(identifier.validity(), at)
        {
            continue;
        }
        if let ExternalIdentifier::OccOption(value) = identifier.identifier() {
            if value != occ
                || identifier.rights_policy().entitlement()
                    == IdentifierEntitlement::UnknownOrRestricted
            {
                return false;
            }
            matched = true;
        }
    }
    matched
}

fn current_option_reference_matches(
    record: &MarketDataInstrumentRecord,
    source: &SourceId,
    original: &AlpacaOriginalOptionContract,
    underlying: &MarketDataInstrumentRecord,
) -> Result<bool, Error> {
    let at = original.received_at();
    let symbol =
        ProviderInstrumentId::try_from(original.symbol()).map_err(|_| Error::InvalidInput)?;
    let uuid = ProviderInstrumentId::try_from(original.id().to_string())
        .map_err(|_| Error::InvalidInput)?;
    let canonical_parent = format!(
        "market-squawk:instrument:{}",
        underlying.definition().instrument_id()
    );
    let provider_parent = format!("alpaca:underlying-asset:{}", original.underlying_asset_id());
    let mut parents_complete = true;
    for identity in record
        .definition()
        .provider_identities()
        .iter()
        .filter(|identity| {
            identity.source_id() == source && interval_contains(identity.validity(), at)
        })
    {
        let mut canonical_found = false;
        let mut provider_found = false;
        for locator in identity.evidence().locators() {
            let reference = locator.reference().as_str();
            if reference.starts_with("market-squawk:instrument:") {
                if reference != canonical_parent {
                    return Err(Error::SourceIdentityConflict);
                }
                canonical_found = true;
            }
            if reference.starts_with("alpaca:underlying-asset:") {
                if reference != provider_parent {
                    return Err(Error::SourceIdentityConflict);
                }
                provider_found = true;
            }
        }
        parents_complete &= canonical_found && provider_found;
    }
    Ok(parents_complete
        && record.definition().venue_mappings().iter().any(|mapping| {
            mapping.venue_id().as_str() == "alpaca-indicative-options"
                && mapping.venue_symbol().as_str() == original.symbol()
        })
        && record
            .definition()
            .provider_identity_at(source, &symbol, at)
            .is_some()
        && record
            .definition()
            .provider_identity_at(source, &uuid, at)
            .is_some()
        && record.definition().quote_currency_evidence()
            == &original
                .quote_currency_evidence()
                .map_err(|_| Error::InvalidInput)?
        && assigned_occ_matches(record.definition(), original.occ_identity(), at))
}

fn option_definition(
    input: &AlpacaOptionReferenceAdmission,
    original: &AlpacaOriginalOptionContract,
    page_index: usize,
    rights: &SourceRightsDecision,
    instrument_id: InstrumentId,
    current: Option<&MarketDataInstrumentRecord>,
) -> Result<MarketDataInstrumentDefinition, Error> {
    let at = original.received_at();
    let validity = EffectiveInterval::new(at, None).map_err(|_| Error::InvalidInput)?;
    let body = original.capture().capture().pages()[0].body_digest();
    let currency_evidence = original
        .quote_currency_evidence()
        .map_err(|_| Error::InvalidInput)?;
    let payload = ExactPayloadEvidence::with_version_pinned_locator(
        body,
        VersionPinnedSourceLocator::new(
            SourceIdentifier::try_from(
                market_squawk_adapter_alpaca::ALPACA_OPTION_CONTRACT_REFERENCE_ENDPOINT,
            )
            .map_err(|_| Error::InvalidInput)?,
            SourceIdentifier::try_from(format!("sha256:{}", hex(body.bytes())))
                .map_err(|_| Error::InvalidInput)?,
        ),
    );
    let mut revision = Sha256::new();
    revision.update(b"market-squawk/alpaca-option-reference/v1\0");
    for digest in [
        body,
        input.originals[page_index].digest(),
        input.underlying.revision_digest(),
        currency_evidence.content_digest(),
    ] {
        revision.update(digest.bytes());
    }
    revision.update(original.id().as_bytes());
    revision.update(original.underlying_asset_id().as_bytes());
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!(
            "alpaca-option-reference-v1:{}",
            hex(revision.finalize().into())
        ))
        .map_err(|_| Error::InvalidInput)?,
    );
    let reference_evidence = RevisionBoundPayloadEvidence::new(revision.clone(), payload.clone());
    let mut identities = current.map_or_else(Vec::new, |record| {
        record.definition().provider_identities().to_vec()
    });
    identities.retain(|identity| identity.source_id() != input.source.source_id());
    let provider_evidence = ProviderIdentityEvidence::try_with_locators(
        body,
        vec![
            ProviderIdentityLocator::new(
                SourceIdentifier::try_from(format!(
                    "alpaca:underlying-asset:{}",
                    original.underlying_asset_id()
                ))
                .map_err(|_| Error::InvalidInput)?,
                SourceIdentifier::try_from(format!("sha256:{}", hex(body.bytes())))
                    .map_err(|_| Error::InvalidInput)?,
            ),
            ProviderIdentityLocator::new(
                SourceIdentifier::try_from(format!(
                    "market-squawk:instrument:{}",
                    input.underlying.definition().instrument_id()
                ))
                .map_err(|_| Error::InvalidInput)?,
                SourceIdentifier::try_from(format!(
                    "sha256:{}",
                    hex(input.underlying.revision_digest().bytes())
                ))
                .map_err(|_| Error::InvalidInput)?,
            ),
            ProviderIdentityLocator::new(
                SourceIdentifier::try_from(format!(
                    "market-squawk:provider-original:{}:{}",
                    hex(input.originals[page_index].session().bytes()),
                    input.originals[page_index].ordinal()
                ))
                .map_err(|_| Error::InvalidInput)?,
                SourceIdentifier::try_from(format!(
                    "sha256:{}",
                    hex(input.originals[page_index].digest().bytes())
                ))
                .map_err(|_| Error::InvalidInput)?,
            ),
        ],
    )
    .map_err(|_| Error::InvalidInput)?;
    for id in [original.symbol().to_owned(), original.id().to_string()] {
        identities.push(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
            instrument_id,
            source_id: input.source.source_id().clone(),
            provider_instrument_id: ProviderInstrumentId::try_from(id)
                .map_err(|_| Error::InvalidInput)?,
            evidence: provider_evidence.clone(),
            source_timestamp: None,
            observed_at: at,
            metadata_revision: revision.clone(),
            validity,
            supersedes: None,
        }));
    }
    let mut identifiers = current.map_or_else(Vec::new, |record| {
        record.definition().identifiers().to_vec()
    });
    identifiers
        .retain(|identifier| !matches!(identifier.identifier(), ExternalIdentifier::OccOption(_)));
    identifiers.push(ExternalIdentifierRecord::new(
        ExternalIdentifierRecordInput {
            identifier: ExternalIdentifier::OccOption(original.occ_identity().clone()),
            assignment_verification: AssignmentVerification::VerifiedAssigned,
            source_id: input.source.source_id().clone(),
            source_evidence: payload,
            source_timestamp: None,
            observed_at: at,
            validity,
            rights_policy: IdentifierRightsPolicyReference::new(
                SourceIdentifier::try_from(format!(
                    "alpaca-option-reference-rights:{}",
                    hex(rights.fingerprint())
                ))
                .map_err(|_| Error::InvalidInput)?,
                IdentifierEntitlement::LicensedInternalUse,
                SourceIdentifier::try_from(rights.basis().reference())
                    .map_err(|_| Error::InvalidInput)?,
            ),
        },
    ));
    MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
        instrument_id,
        reference_evidence,
        effective_interval: validity,
        asset_class: AssetClass::Option,
        display_name: current.and_then(|record| record.definition().display_name().cloned()),
        quote_currency: original.quote_currency().map_err(|_| Error::InvalidInput)?,
        quote_currency_evidence: currency_evidence,
        venue_mappings: {
            let mut mappings = current.map_or_else(Vec::new, |record| {
                record.definition().venue_mappings().to_vec()
            });
            let venue =
                VenueId::try_from("alpaca-indicative-options").map_err(|_| Error::InvalidInput)?;
            if mappings.iter().any(|mapping| {
                mapping.venue_id() == &venue && mapping.venue_symbol().as_str() != original.symbol()
            }) {
                return Err(Error::SourceIdentityConflict);
            }
            mappings.retain(|mapping| mapping.venue_id() != &venue);
            mappings.push(VenueMapping::new(
                venue,
                VenueSymbol::try_from(original.symbol()).map_err(|_| Error::InvalidInput)?,
            ));
            mappings
        },
        provider_identities: identities,
        identifiers,
    })
    .map_err(|_| Error::InvalidInput)
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
