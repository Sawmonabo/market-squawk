//! Alpaca asset UUID admission into the existing non-execution instrument catalog.
//! Canonical creation joins a current official listing to one sealed active native asset.
//! Existing securities retain their original issuer evidence and all other provider assertions.

use super::issuer_reference::{listing_rights_policy, require_listing_membership};
use super::*;
use market_squawk_adapter_alpaca::AlpacaOriginalAssetReference;
use market_squawk_domain::{
    ExternalIdentifierRecordInput, IdentifierRightsPolicyReference, SourceIdentifier, Ticker,
    VersionPinnedSourceLocator,
};
use sha2::Digest as _;

type Error = MarketDataInstrumentCatalogError;

/// One physically sealed authenticated Paper asset response and its exact current join.
pub struct AlpacaAssetReferenceAdmission {
    pub source: SourceMetadata,
    pub rights: RightsDecisionInput,
    pub capture: ProviderWholeCaptureToken,
    pub asset: AlpacaOriginalAssetReference,
    pub official_listing: ListingReferenceRecord,
    pub expected_current: Option<MarketDataInstrumentRecord>,
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Creates or enriches one native security with listing, rights and capture custody checked
    /// under the same catalog writer lock and durable publication transaction.
    pub fn publish_alpaca_asset_reference(
        &self,
        input: AlpacaAssetReferenceAdmission,
        precommit: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, Error> {
        check_operation(deadline, cancellation)?;
        precommit
            .validate_precommit()
            .map_err(reference_precommit_error)?;
        let listing_reader = ListingReferenceReadCapability::new(
            Arc::clone(&self.authority),
            input.official_listing.generation().dataset().clone(),
            input.official_listing.generation().source_id().clone(),
        );
        self.authority
            .try_lock()
            .map_err(|_| Error::AuthorityUnavailable)?
            .publish_alpaca_asset_reference(
                input,
                &listing_reader,
                precommit,
                deadline,
                cancellation,
            )
    }
}

impl CatalogAuthority {
    fn publish_alpaca_asset_reference(
        &self,
        input: AlpacaAssetReferenceAdmission,
        listing_reader: &ListingReferenceReadCapability,
        precommit: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, Error> {
        let at = input.asset.received_at();
        let [page] = input.asset.capture().capture().pages() else {
            return Err(Error::InvalidInput);
        };
        let sealed = input.capture.persisted_receipt();
        if input.source.provider().as_str() != "alpaca-market-data"
            || !input.asset.is_active_us_equity()
            || !input
                .source
                .coverage()
                .asset_classes()
                .contains(&AssetClass::Equity)
            || sealed != input.asset.capture()
            || input.asset.capture().capture().source_id() != input.source.source_id()
            || input.asset.capture().capture().metadata_revision() != input.source.revision()
            || input.rights.source_id != *input.source.source_id()
            || input.rights.payload_digest != input.asset.capture().capture().observation_digest()
            || input.rights.retrieved_at != at
            || !matches!(&input.rights.basis, crate::RightsBasis::ReviewedTerms(_))
            || page.http_status() != 200
            || page.received_at() != at
            || !input.source.is_effective_at(at)
            || input.official_listing.is_test_issue()
            || input.official_listing.provider_symbol() != input.asset.symbol()
            || input.official_listing.listing_venue().as_str() != primary_venue(&input.asset)?
            || input.official_listing.source_file().available_at() > at
            || input.official_listing.generation().published_at() > at
        {
            return Err(Error::SourceIdentityConflict);
        }
        let rights = SourceRightsDecision::try_new(input.rights.clone())
            .map_err(super::super::CatalogError::from)?;
        let connection = &self.catalog().connection;
        connection.busy_timeout(std::time::Duration::ZERO)?;
        let result = (|| {
            install_progress_handler(connection, deadline, cancellation)?;
            precommit
                .validate_catalog_precommit(self)
                .map_err(reference_precommit_error)?;
            listing_reader.require_current_in_catalog(
                self,
                input.official_listing.generation(),
                deadline,
                cancellation,
            )?;
            if let Some(expected) = input.expected_current.as_ref() {
                require_current_market_data_instrument(self, expected, deadline, cancellation)?;
            }
            if self.catalog().source(input.source.source_id())?.as_ref() != Some(&input.source) {
                self.catalog().register_source(&input.source, at)?;
            }
            if self.catalog().source(input.source.source_id())?.as_ref() != Some(&input.source) {
                return Err(Error::SourceIdentityConflict);
            }
            let transaction = connection.unchecked_transaction()?;
            let commit_at = trusted_catalog_now(&transaction)?;
            if at > commit_at || !input.source.is_effective_at(commit_at) {
                return Err(Error::SourceIdentityConflict);
            }
            require_listing_membership(&transaction, &input.official_listing)?;
            let listing_policy =
                listing_rights_policy(&transaction, &input.official_listing, commit_at)?;
            for operation in [SourceOperation::Persist, SourceOperation::Display] {
                let request = crate::IngestIdentity::try_new(
                    rights.source_id().clone(),
                    rights.payload_digest(),
                    operation,
                    "market-data-alpaca-asset-reference",
                )
                .map_err(super::super::CatalogError::from)?;
                rights
                    .authorize_at(&request, commit_at)
                    .map_err(super::super::CatalogError::from)?;
            }
            let current = resolve_current(
                &transaction,
                &input,
                &mut ResultBudget::new(self.catalog().result_bytes),
            )?;
            if let Some(expected) = input.expected_current.as_ref()
                && current.as_ref() != Some(expected)
            {
                return Err(Error::ReferencePositionConflict);
            }
            if let Some(current) = current.as_ref() {
                validate_current(current, &input)?;
                // A caller without an expected record may replay an exact native UUID; a
                // listing-only match requires the existing record's explicit CAS expectation.
                if input.expected_current.is_none() && !has_native_identity(current, &input)? {
                    return Err(Error::ReferencePositionConflict);
                }
            }
            let record = if let Some(current) = current.as_ref()
                && already_accepted(current, &input)?
            {
                current.clone()
            } else {
                let instrument_id = current
                    .as_ref()
                    .map(|record| Ok(record.definition().instrument_id()))
                    .unwrap_or_else(|| {
                        InstrumentId::try_from(uuid::Uuid::new_v4())
                            .map_err(|_| Error::InvalidInput)
                    })?;
                let next =
                    build_definition(&input, current.as_ref(), instrument_id, listing_policy)?;
                let mut prepared = prepare_definitions(vec![next].into_boxed_slice())?;
                let prepared = prepared.pop().ok_or(Error::InvalidInput)?;
                if prepared.json.len() > self.catalog().result_bytes.max_record_bytes() {
                    return Err(Error::ResultByteLimitExceeded);
                }
                match plan_publication(&transaction, &prepared)? {
                    PublicationPlan::Insert {
                        sequence,
                        previous,
                        identity_is_new,
                    } => {
                        if identity_is_new != current.is_none() {
                            return Err(Error::SourceIdentityConflict);
                        }
                        insert_definition(
                            &transaction,
                            &self.provider_identity_generation,
                            &prepared,
                            sequence,
                            previous,
                            identity_is_new,
                            commit_at,
                        )?;
                        MarketDataInstrumentRecord {
                            definition: prepared.definition,
                            revision_digest: digest(prepared.digest),
                            revision_sequence: sequence,
                            published_at: commit_at,
                        }
                    }
                    PublicationPlan::Replay => current.ok_or(Error::CorruptCatalog)?,
                }
            };
            let final_at = trusted_catalog_now(&transaction)?;
            rights
                .validate_at(final_at)
                .map_err(super::super::CatalogError::from)?;
            if !input.source.is_effective_at(final_at)
                || self.catalog().source(input.source.source_id())?.as_ref() != Some(&input.source)
            {
                return Err(Error::SourceIdentityConflict);
            }
            listing_reader.require_current_in_catalog(
                self,
                input.official_listing.generation(),
                deadline,
                cancellation,
            )?;
            listing_rights_policy(&transaction, &input.official_listing, final_at)?;
            super::super::storage::persist_rights(&transaction, &rights, commit_at)?;
            let capture = ProviderMacroPlanCompletionCapture::try_from_live(input.capture)?;
            retain_provider_macro_plan_completion_capture(&transaction, &capture, commit_at)?;
            append_audit(
                &transaction,
                "market-data-instrument.alpaca-asset-reference-published",
                input.asset.symbol(),
                record.revision_digest().bytes(),
                commit_at,
            )?;
            check_operation(deadline, cancellation)?;
            precommit
                .validate_catalog_precommit(self)
                .map_err(reference_precommit_error)?;
            transaction.commit()?;
            Ok(record)
        })();
        let progress_cleanup = clear_progress_handler(connection);
        let busy_cleanup = connection.busy_timeout(self.catalog().busy_timeout);
        let result = classify_operation(result, deadline, cancellation);
        progress_cleanup?;
        busy_cleanup?;
        result
    }
}

fn already_accepted(
    current: &MarketDataInstrumentRecord,
    input: &AlpacaAssetReferenceAdmission,
) -> Result<bool, Error> {
    let definition = current.definition();
    let native_id = ProviderInstrumentId::try_from(input.asset.id().to_string())
        .map_err(|_| Error::InvalidInput)?;
    if definition.provider_identities().iter().any(|identity| {
        identity.source_id() == input.source.source_id()
            && (identity.provider_instrument_id() != &native_id
                || identity.instrument_id() != definition.instrument_id())
    }) {
        return Err(Error::SourceIdentityConflict);
    }
    let accepted = definition.provider_identities().iter().find(|identity| {
        identity.source_id() == input.source.source_id()
            && identity.provider_instrument_id() == &native_id
    });
    Ok(accepted.is_some_and(|identity| {
        identity.evidence().content_digest()
            == input.asset.capture().capture().pages()[0].body_digest()
            && interval_contains(identity.validity(), input.asset.received_at())
            && definition.venue_mappings().iter().any(|mapping| {
                mapping.venue_id().as_str() == "iex"
                    && mapping.venue_symbol().as_str() == input.asset.symbol()
            })
    }))
}

fn resolve_current(
    transaction: &Transaction<'_>,
    input: &AlpacaAssetReferenceAdmission,
    budget: &mut ResultBudget,
) -> Result<Option<MarketDataInstrumentRecord>, Error> {
    // Use the existing canonical search index and namespace-qualified native key. The listing
    // mapping is a collision check, not authority to choose or manufacture another identity.
    let sql = format!("SELECT DISTINCT {STORED_COLUMNS}
        FROM market_data_instrument_current AS current_
        JOIN market_data_instrument_revisions AS revisions ON revisions.revision_digest=current_.revision_digest
        JOIN market_data_instrument_search_terms AS terms ON terms.revision_digest=current_.revision_digest
        WHERE (terms.term_kind='provider_symbol' AND terms.source_id=?1 AND terms.display_term=?2)
           OR (terms.term_kind='venue_symbol' AND terms.normalized_term=?3 AND EXISTS (
                SELECT 1 FROM json_each(revisions.definition_json, '$.venue_mappings') AS venue
                WHERE json_extract(venue.value, '$.venue_id')=?4 AND json_extract(venue.value, '$.venue_symbol')=?5))
        ORDER BY revisions.instrument_id LIMIT 3");
    let mut statement = transaction.prepare(&sql)?;
    let rows = statement.query_map(
        params![
            input.source.source_id().as_str(),
            input.asset.id().to_string(),
            normalize(input.asset.symbol()),
            input.official_listing.listing_venue().as_str(),
            input.asset.symbol(),
        ],
        decode_stored_row,
    )?;
    let mut selected = None;
    for row in rows {
        let row = row?;
        charge_row(&row, budget)?;
        let record = rebuild_record(row)?;
        if selected.is_some() {
            return Err(Error::SourceIdentityConflict);
        }
        validate_current(&record, input)?;
        selected = Some(record);
    }
    Ok(selected)
}

fn validate_current(
    current: &MarketDataInstrumentRecord,
    input: &AlpacaAssetReferenceAdmission,
) -> Result<(), Error> {
    let definition = current.definition();
    let at = input.asset.received_at();
    if definition.asset_class() != listing_asset_class(&input.official_listing)
        || definition.quote_currency() != input.asset.quote_currency().map_err(|_| Error::InvalidInput)?
        // Replaying the retained original is valid even though its durable catalog commit
        // followed HTTP receipt. Only the already accepted exact body/native UUID may do so.
        || (current.published_at() > at && !already_accepted(current, input)?)
        || !interval_contains(definition.effective_interval(), at)
        || !has_assigned_ticker(definition, &input.asset, at)
        || !matches_primary_listing(definition, &input.asset)
        || definition.venue_mappings().iter().any(|mapping| {
            mapping.venue_id().as_str() == "iex" && mapping.venue_symbol().as_str() != input.asset.symbol()
        })
    {
        return Err(Error::SourceIdentityConflict);
    }
    // A different native UUID under this same authority cannot replace an existing assertion.
    let native = ProviderInstrumentId::try_from(input.asset.id().to_string())
        .map_err(|_| Error::InvalidInput)?;
    if definition.provider_identities().iter().any(|identity| {
        identity.source_id() == input.source.source_id()
            && (identity.provider_instrument_id() != &native
                || identity.instrument_id() != definition.instrument_id()
                || !interval_contains(identity.validity(), at))
    }) {
        return Err(Error::SourceIdentityConflict);
    }
    Ok(())
}

fn listing_asset_class(listing: &ListingReferenceRecord) -> AssetClass {
    if listing.is_etf() {
        AssetClass::Fund
    } else {
        AssetClass::Equity
    }
}

fn has_native_identity(
    current: &MarketDataInstrumentRecord,
    input: &AlpacaAssetReferenceAdmission,
) -> Result<bool, Error> {
    let native = ProviderInstrumentId::try_from(input.asset.id().to_string())
        .map_err(|_| Error::InvalidInput)?;
    Ok(current
        .definition()
        .provider_identity_at(input.source.source_id(), &native, input.asset.received_at())
        .is_some())
}

fn has_assigned_ticker(
    definition: &MarketDataInstrumentDefinition,
    asset: &AlpacaOriginalAssetReference,
    at: Timestamp,
) -> bool {
    definition.identifiers().iter().any(|identifier| {
        matches!(identifier.identifier(), ExternalIdentifier::Ticker(ticker) if ticker.as_str() == asset.symbol())
            && identifier.assignment_verification() == AssignmentVerification::VerifiedAssigned
            && identifier.rights_policy().entitlement() != IdentifierEntitlement::UnknownOrRestricted
            && interval_contains(identifier.validity(), at)
    })
}

fn matches_primary_listing(
    definition: &MarketDataInstrumentDefinition,
    asset: &AlpacaOriginalAssetReference,
) -> bool {
    let Ok(venue) = primary_venue(asset) else {
        return false;
    };
    definition.venue_mappings().iter().any(|mapping| {
        mapping.venue_id().as_str().eq_ignore_ascii_case(venue)
            && mapping.venue_symbol().as_str() == asset.symbol()
    })
}

fn primary_venue(asset: &AlpacaOriginalAssetReference) -> Result<&'static str, Error> {
    Ok(match asset.exchange() {
        "NASDAQ" => "XNAS",
        "NYSE" => "XNYS",
        "ARCA" | "NYSEARCA" => "ARCX",
        "AMEX" => "XASE",
        "BATS" => "BATS",
        _ => return Err(Error::SourceIdentityConflict),
    })
}

fn build_definition(
    input: &AlpacaAssetReferenceAdmission,
    current: Option<&MarketDataInstrumentRecord>,
    instrument_id: InstrumentId,
    listing_policy: IdentifierRightsPolicyReference,
) -> Result<MarketDataInstrumentDefinition, Error> {
    let current = current.map(MarketDataInstrumentRecord::definition);
    let at = input.asset.received_at();
    let validity = EffectiveInterval::new(at, None).map_err(|_| Error::InvalidInput)?;
    let body = input.asset.capture().capture().pages()[0].body_digest();
    let mut revision = Sha256::new();
    revision.update(b"market-squawk/alpaca-asset-reference/v1\0");
    revision.update(body.bytes());
    revision.update(input.asset.id().as_bytes());
    revision.update(instrument_id.as_uuid().as_bytes());
    revision.update(
        input
            .official_listing
            .generation()
            .generation_digest()
            .bytes(),
    );
    revision.update(
        input
            .official_listing
            .record_payload_evidence()
            .content_digest()
            .bytes(),
    );
    let currency_evidence = input
        .asset
        .quote_currency_evidence()
        .map_err(|_| Error::InvalidInput)?;
    revision.update(currency_evidence.content_digest().bytes());
    let revision = MetadataRevision::new(
        SourceIdentifier::try_from(format!(
            "alpaca-asset-reference-v1:{}",
            hex(revision.finalize().into()),
        ))
        .map_err(|_| Error::InvalidInput)?,
    );
    let evidence = ProviderIdentityEvidence::try_with_locators(
        body,
        vec![ProviderIdentityLocator::new(
            SourceIdentifier::try_from(format!(
                "market-squawk:provider-capture:{}",
                hex(input.asset.capture().receipt_digest().bytes())
            ))
            .map_err(|_| Error::InvalidInput)?,
            SourceIdentifier::try_from(format!("sha256:{}", hex(body.bytes())))
                .map_err(|_| Error::InvalidInput)?,
        )],
    )
    .map_err(|_| Error::InvalidInput)?;
    let native_id = ProviderInstrumentId::try_from(input.asset.id().to_string())
        .map_err(|_| Error::InvalidInput)?;
    let mut identities = current
        .map(|definition| definition.provider_identities().to_vec())
        .unwrap_or_default();
    if identities.iter().any(|identity| {
        identity.source_id() == input.source.source_id()
            && (identity.provider_instrument_id() != &native_id
                || identity.instrument_id() != instrument_id)
    }) {
        return Err(Error::SourceIdentityConflict);
    }
    identities.retain(|identity| identity.source_id() != input.source.source_id());
    identities.push(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id,
        source_id: input.source.source_id().clone(),
        provider_instrument_id: native_id,
        evidence,
        source_timestamp: None,
        observed_at: at,
        metadata_revision: revision.clone(),
        validity,
        supersedes: None,
    }));
    let mut mappings = if let Some(current) = current {
        current.venue_mappings().to_vec()
    } else {
        vec![VenueMapping::new(
            input.official_listing.listing_venue().clone(),
            VenueSymbol::try_from(input.asset.symbol()).map_err(|_| Error::InvalidInput)?,
        )]
    };
    let iex = VenueId::try_from("iex").map_err(|_| Error::InvalidInput)?;
    if mappings.iter().any(|mapping| {
        mapping.venue_id() == &iex && mapping.venue_symbol().as_str() != input.asset.symbol()
    }) {
        return Err(Error::SourceIdentityConflict);
    }
    mappings.retain(|mapping| mapping.venue_id() != &iex);
    mappings.push(VenueMapping::new(
        iex,
        VenueSymbol::try_from(input.asset.symbol()).map_err(|_| Error::InvalidInput)?,
    ));
    let (reference_evidence, display_name, identifiers) = if let Some(current) = current {
        (
            current.reference_evidence().clone(),
            current.display_name().cloned(),
            current.identifiers().to_vec(),
        )
    } else {
        let payload = ExactPayloadEvidence::with_version_pinned_locator(
            body,
            VersionPinnedSourceLocator::new(
                SourceIdentifier::try_from(format!(
                    "market-squawk:provider-capture:{}",
                    hex(input.asset.capture().receipt_digest().bytes())
                ))
                .map_err(|_| Error::InvalidInput)?,
                SourceIdentifier::try_from(format!("sha256:{}", hex(body.bytes())))
                    .map_err(|_| Error::InvalidInput)?,
            ),
        );
        let identifiers = vec![ExternalIdentifierRecord::new(
            ExternalIdentifierRecordInput {
                identifier: ExternalIdentifier::Ticker(
                    Ticker::try_from(input.asset.symbol()).map_err(|_| Error::InvalidInput)?,
                ),
                assignment_verification: AssignmentVerification::VerifiedAssigned,
                source_id: input.official_listing.generation().source_id().clone(),
                source_evidence: input.official_listing.record_payload_evidence().clone(),
                source_timestamp: Some(input.official_listing.effective_at()),
                observed_at: input.official_listing.source_file().received_at(),
                validity,
                rights_policy: listing_policy.clone(),
            },
        )];
        (
            RevisionBoundPayloadEvidence::new(revision, payload),
            Some(
                MarketDataDisplayName::try_new(
                    input.official_listing.display_name(),
                    input.official_listing.generation().source_id().clone(),
                    input.official_listing.record_payload_evidence().clone(),
                    listing_policy,
                )
                .map_err(|_| Error::InvalidInput)?,
            ),
            identifiers,
        )
    };
    MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
        instrument_id,
        // Canonical issuer/listing authority is unchanged. Alpaca's separate provider assertion
        // and retained whole capture carry its own origin and content evidence.
        reference_evidence,
        effective_interval: validity,
        asset_class: current
            .map(MarketDataInstrumentDefinition::asset_class)
            .unwrap_or_else(|| listing_asset_class(&input.official_listing)),
        display_name,
        quote_currency: input
            .asset
            .quote_currency()
            .map_err(|_| Error::InvalidInput)?,
        quote_currency_evidence: current
            .map(|definition| definition.quote_currency_evidence().clone())
            .unwrap_or(currency_evidence),
        venue_mappings: mappings,
        provider_identities: identities,
        identifiers,
    })
    .map_err(|_| Error::InvalidInput)
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
