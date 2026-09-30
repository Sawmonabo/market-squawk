//! Alpaca asset UUID admission into the existing non-execution instrument catalog.
//! The accepted canonical security comes from the current issuer/listing catalog record;
//! Alpaca supplies only its own asset UUID, symbol, exchange, and original response.

use super::*;
use market_squawk_adapter_alpaca::AlpacaOriginalAssetReference;
use market_squawk_domain::SourceIdentifier;
use sha2::Digest as _;

type Error = MarketDataInstrumentCatalogError;

/// One physically sealed authenticated Paper asset response and its exact current join.
pub struct AlpacaAssetReferenceAdmission {
    pub source: SourceMetadata,
    pub rights: RightsDecisionInput,
    pub capture: ProviderWholeCaptureToken,
    pub asset: AlpacaOriginalAssetReference,
    pub expected_current: MarketDataInstrumentRecord,
}

impl MarketDataInstrumentSynchronizationCapability {
    /// Adds the provider-native UUID and IEX subscription symbol with source rights and custody
    /// checked under the same catalog writer lock and durable publication transaction.
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
        self.authority
            .try_lock()
            .map_err(|_| Error::AuthorityUnavailable)?
            .publish_alpaca_asset_reference(input, precommit, deadline, cancellation)
    }
}

impl CatalogAuthority {
    fn publish_alpaca_asset_reference(
        &self,
        input: AlpacaAssetReferenceAdmission,
        precommit: &dyn IngestPrecommitAuthority,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketDataInstrumentRecord, Error> {
        let definition = input.expected_current.definition();
        let at = input.asset.received_at();
        let [page] = input.asset.capture().capture().pages() else {
            return Err(Error::InvalidInput);
        };
        let sealed = input.capture.persisted_receipt();
        if input.source.provider().as_str() != "alpaca-market-data"
            || !matches!(
                definition.asset_class(),
                AssetClass::Equity | AssetClass::Fund
            )
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
            || !interval_contains(definition.effective_interval(), at)
            || input.expected_current.published_at() > at
            || !matches_primary_listing(definition, &input.asset)
            || !has_assigned_ticker(definition, &input.asset, at)
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
            require_current_market_data_instrument(
                self,
                &input.expected_current,
                deadline,
                cancellation,
            )?;
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
            let record = if already_accepted(&input)? {
                input.expected_current.clone()
            } else {
                let next = build_definition(&input)?;
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
                        if identity_is_new {
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
                    PublicationPlan::Replay => input.expected_current.clone(),
                }
            };
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

fn already_accepted(input: &AlpacaAssetReferenceAdmission) -> Result<bool, Error> {
    let definition = input.expected_current.definition();
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
    let venue = match asset.exchange() {
        "NASDAQ" => "XNAS",
        "NYSE" => "XNYS",
        "ARCA" | "NYSEARCA" => "ARCX",
        "AMEX" => "XASE",
        "BATS" => "BATS",
        _ => return false,
    };
    definition.venue_mappings().iter().any(|mapping| {
        mapping.venue_id().as_str().eq_ignore_ascii_case(venue)
            && mapping.venue_symbol().as_str() == asset.symbol()
    })
}

fn build_definition(
    input: &AlpacaAssetReferenceAdmission,
) -> Result<MarketDataInstrumentDefinition, Error> {
    let current = input.expected_current.definition();
    let at = input.asset.received_at();
    let validity = EffectiveInterval::new(at, None).map_err(|_| Error::InvalidInput)?;
    let body = input.asset.capture().capture().pages()[0].body_digest();
    let mut revision = Sha256::new();
    revision.update(b"market-squawk/alpaca-asset-reference/v1\0");
    revision.update(body.bytes());
    revision.update(input.asset.id().as_bytes());
    revision.update(current.instrument_id().as_uuid().as_bytes());
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
    let mut identities = current.provider_identities().to_vec();
    if identities.iter().any(|identity| {
        identity.source_id() == input.source.source_id()
            && (identity.provider_instrument_id() != &native_id
                || identity.instrument_id() != current.instrument_id())
    }) {
        return Err(Error::SourceIdentityConflict);
    }
    identities.retain(|identity| identity.source_id() != input.source.source_id());
    identities.push(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: current.instrument_id(),
        source_id: input.source.source_id().clone(),
        provider_instrument_id: native_id,
        evidence,
        source_timestamp: None,
        observed_at: at,
        metadata_revision: revision.clone(),
        validity,
        supersedes: None,
    }));
    let mut mappings = current.venue_mappings().to_vec();
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
    MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
        instrument_id: current.instrument_id(),
        // Canonical issuer/listing authority is unchanged. Alpaca's separate provider assertion
        // and retained whole capture carry its own origin and content evidence.
        reference_evidence: current.reference_evidence().clone(),
        effective_interval: validity,
        asset_class: current.asset_class(),
        display_name: current.display_name().cloned(),
        quote_currency: current.quote_currency().clone(),
        quote_currency_evidence: current.quote_currency_evidence().clone(),
        venue_mappings: mappings,
        provider_identities: identities,
        identifiers: current.identifiers().to_vec(),
    })
    .map_err(|_| Error::InvalidInput)
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
