//! Validated platform-to-adapter instrument composition.

use market_squawk_adapter_coinbase::{CoinbaseConfigError, CoinbaseProductMapping};
use market_squawk_data::MarketDataInstrumentRecord;
use market_squawk_domain::{
    AssetClass, EffectiveInterval, IdentityError, InstrumentDefinition, InstrumentError,
    ProviderIdentityKey, ProviderInstrumentId, ProviderProduct, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_platform::CoinbaseSourceConfig;
use market_squawk_sources::{
    ProviderNativeInstrumentAttestation, ProviderNativeInstrumentAttestationError,
    ProviderNativeInstrumentAttestationInput,
};
use thiserror::Error;

const COINBASE_VENUE: &str = "coinbase-exchange";
pub(crate) const COINBASE_PUBLIC_SOURCE: &str = "coinbase-exchange-public";
const COINBASE_DIRECT_SOURCE_PREFIX: &str = "coinbase-exchange-direct-";

#[derive(Debug)]
pub(super) struct ProductionInstrumentSet {
    adapter_mappings: Box<[CoinbaseProductMapping]>,
}

/// Returns the sole source namespace used by the public Coinbase profile.
pub(crate) fn public_source_id() -> Result<SourceId, CoinbaseNativeIdentityError> {
    SourceId::try_from(COINBASE_PUBLIC_SOURCE).map_err(Into::into)
}

/// Derives the sole source namespace used by one authenticated Direct product.
pub(crate) fn direct_source_id(
    product: &ProviderProduct,
) -> Result<SourceId, CoinbaseNativeIdentityError> {
    SourceId::try_from(format!(
        "{COINBASE_DIRECT_SOURCE_PREFIX}{}",
        product.as_source_identifier().as_str().to_ascii_lowercase()
    ))
    .map_err(Into::into)
}

/// Selects one exact source-qualified provider identity from an immutable catalog record.
pub(crate) fn select_product_mapping(
    product: ProviderProduct,
    expected_definition: &InstrumentDefinition,
    source_id: &SourceId,
    record: &MarketDataInstrumentRecord,
    selected_at: Timestamp,
) -> Result<CoinbaseProductMapping, CoinbaseNativeIdentityError> {
    let definition = record.definition();
    if definition.instrument_id() != expected_definition.instrument_id()
        || definition.asset_class() != AssetClass::Crypto
        || definition.asset_class() != expected_definition.asset_class()
        || definition.quote_currency() != expected_definition.quote_currency()
    {
        return Err(CoinbaseNativeIdentityError::RecordMismatch);
    }

    let provider_instrument_id =
        ProviderInstrumentId::try_from(product.as_source_identifier().as_str())?;
    let venue = VenueId::try_from(COINBASE_VENUE)?;
    let expected_venue_mapping = expected_definition
        .venue_mappings()
        .iter()
        .find(|mapping| {
            mapping.venue_id() == &venue
                && mapping.venue_symbol().as_str() == provider_instrument_id.as_str()
        })
        .ok_or(CoinbaseNativeIdentityError::RecordMismatch)?;
    let venue_mapping = definition
        .venue_mappings()
        .iter()
        .find(|mapping| *mapping == expected_venue_mapping)
        .cloned()
        .ok_or(CoinbaseNativeIdentityError::RecordMismatch)?;
    let attestation = ProviderNativeInstrumentAttestation::try_select(
        ProviderNativeInstrumentAttestationInput {
            definition,
            definition_revision_digest: record.revision_digest(),
            definition_published_at: record.published_at(),
            provider_key: ProviderIdentityKey::new(source_id.clone(), provider_instrument_id),
            venue_mapping,
            selected_at,
        },
    )?;
    CoinbaseProductMapping::try_new(product, attestation).map_err(Into::into)
}

/// Intersects source authorization with every selected native-identity validity interval.
pub(crate) fn narrow_effective_interval(
    authorization: EffectiveInterval,
    mappings: &[CoinbaseProductMapping],
) -> Result<EffectiveInterval, CoinbaseNativeIdentityError> {
    if mappings.is_empty() {
        return Err(CoinbaseNativeIdentityError::RecordMismatch);
    }
    let mut starts_at = authorization.starts_at();
    let mut ends_at = authorization.ends_at();
    for mapping in mappings {
        let attestation = mapping.instrument_attestation();
        starts_at = starts_at
            .max(attestation.valid_from())
            .max(attestation.selected_at());
        ends_at = earliest_end(ends_at, attestation.valid_until());
    }
    EffectiveInterval::new(starts_at, ends_at).map_err(Into::into)
}

fn earliest_end(left: Option<Timestamp>, right: Option<Timestamp>) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    }
}

impl ProductionInstrumentSet {
    pub(super) fn adapter_mappings(&self) -> &[CoinbaseProductMapping] {
        &self.adapter_mappings
    }

    pub(super) fn try_new(
        config: &CoinbaseSourceConfig,
        records: &[MarketDataInstrumentRecord],
        source_id: &SourceId,
    ) -> Result<Self, ProductionInstrumentError> {
        if records.len() != config.instruments().len() {
            return Err(ProductionInstrumentError::RecordSetMismatch);
        }
        let mut adapter_mappings = Vec::new();
        adapter_mappings
            .try_reserve_exact(config.instruments().len())
            .map_err(|_error| ProductionInstrumentError::AllocationFailed)?;
        for mapping in config.instruments() {
            let definition = mapping.definition().clone();
            let product = ProviderProduct::new(SourceIdentifier::try_from(mapping.product())?);
            let mut matches = records
                .iter()
                .filter(|record| record.definition().instrument_id() == definition.instrument_id());
            let record = matches
                .next()
                .ok_or(ProductionInstrumentError::RecordSetMismatch)?;
            if matches.next().is_some() {
                return Err(ProductionInstrumentError::RecordSetMismatch);
            }
            adapter_mappings.push(
                select_product_mapping(
                    product,
                    &definition,
                    source_id,
                    record,
                    record.published_at(),
                )
                .map_err(|_error| ProductionInstrumentError::NativeIdentity)?,
            );
        }
        Ok(Self {
            adapter_mappings: adapter_mappings.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProductionInstrumentError {
    #[error("production instrument mapping allocation failed")]
    AllocationFailed,
    #[error("production instrument identity is invalid")]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error("production instrument catalog record set does not exactly cover configuration")]
    RecordSetMismatch,
    #[error("Coinbase native instrument evidence rejected production configuration")]
    NativeIdentity,
}

/// Source-qualified Coinbase identity selection failed closed before provider startup.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub(crate) enum CoinbaseNativeIdentityError {
    #[error("Coinbase market-data definition does not match the configured route")]
    RecordMismatch,
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    NativeIdentity(#[from] ProviderNativeInstrumentAttestationError),
    #[error(transparent)]
    Adapter(#[from] CoinbaseConfigError),
    #[error(transparent)]
    Interval(#[from] InstrumentError),
}
