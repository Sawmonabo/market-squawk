//! Checked current instrument definitions.

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    AssetClass, ExternalIdentifierRecord, InstrumentError, ProviderIdentityConflict,
    ProviderIdentityRecord, ProviderIdentityRegistry, TradingStatus, VenueMapping,
};
use crate::{
    Currency, Denomination, InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId,
    LotSize, ProviderInstrumentId, SourceId, TickSize, Timestamp,
};

/// Current instrument reference definition with invariant-preserving private fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InstrumentDefinition {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    execution_terms: InstrumentExecutionTerms,
    venue_mappings: Vec<VenueMapping>,
    provider_identity_registry: ProviderIdentityRegistry,
    identifiers: Vec<ExternalIdentifierRecord>,
    trading_status: TradingStatus,
}

/// Complete current reference-master input for constructing [`InstrumentDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentDefinitionInput {
    /// Stable internal instrument identity.
    pub instrument_id: InstrumentId,
    /// Monotonic revision binding every execution term.
    pub definition_revision: InstrumentDefinitionRevision,
    /// Broad asset family.
    pub asset_class: AssetClass,
    /// Explicit settlement denomination.
    pub primary_denomination: Denomination,
    /// Currency in which prices are quoted.
    pub quote_currency: Currency,
    /// Exact price increment.
    pub tick_size: TickSize,
    /// Exact quantity increment.
    pub lot_size: LotSize,
    /// Exact positive multiplier applied to one price-tick by quantity-lot unit.
    pub contract_multiplier: Decimal,
    /// Current venue-symbol mappings.
    pub venue_mappings: Vec<VenueMapping>,
    /// Source-qualified provider identity records.
    pub provider_identities: Vec<ProviderIdentityRecord>,
    /// Evidence-bearing external identifier records.
    pub identifiers: Vec<ExternalIdentifierRecord>,
    /// Current reference-master trading status.
    pub trading_status: TradingStatus,
}

struct InstrumentDefinitionParts {
    instrument_id: InstrumentId,
    definition_revision: InstrumentDefinitionRevision,
    asset_class: AssetClass,
    primary_denomination: Denomination,
    quote_currency: Currency,
    tick_size: TickSize,
    lot_size: LotSize,
    contract_multiplier: Decimal,
    venue_mappings: Vec<VenueMapping>,
    identifiers: Vec<ExternalIdentifierRecord>,
    trading_status: TradingStatus,
}

impl InstrumentDefinition {
    /// Constructs a current instrument definition.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::DuplicateVenueMapping`] for multiple current mappings in one
    /// venue; historical mappings belong in [`super::SymbolIdentityRecord`] intervals. Returns
    /// [`InstrumentError::ProviderIdentityInstrumentMismatch`] when any accepted or quarantined
    /// provider assertion names a different stable instrument and
    /// [`InstrumentError::DuplicateExternalIdentifier`] when the same typed external identifier is
    /// attached more than once.
    ///
    /// Provider revision graphs additionally return
    /// [`InstrumentError::MissingProviderIdentitySupersession`],
    /// [`InstrumentError::MissingProviderIdentityPredecessor`],
    /// [`InstrumentError::ProviderIdentitySupersessionCycle`],
    /// [`InstrumentError::AmbiguousProviderIdentitySuccessor`], or
    /// [`InstrumentError::InvalidProviderIdentityTransition`] when their evidence is incomplete,
    /// cyclic, branching, or temporally overlapping.
    pub fn try_new(input: InstrumentDefinitionInput) -> Result<Self, InstrumentError> {
        let InstrumentDefinitionInput {
            instrument_id,
            definition_revision,
            asset_class,
            primary_denomination,
            quote_currency,
            tick_size,
            lot_size,
            contract_multiplier,
            venue_mappings,
            provider_identities,
            identifiers,
            trading_status,
        } = input;
        let provider_identity_registry =
            ProviderIdentityRegistry::try_from_records(provider_identities)?;
        Self::try_new_with_registry(
            InstrumentDefinitionParts {
                instrument_id,
                definition_revision,
                asset_class,
                primary_denomination,
                quote_currency,
                tick_size,
                lot_size,
                contract_multiplier,
                venue_mappings,
                identifiers,
                trading_status,
            },
            provider_identity_registry,
        )
    }

    fn try_new_with_registry(
        parts: InstrumentDefinitionParts,
        provider_identity_registry: ProviderIdentityRegistry,
    ) -> Result<Self, InstrumentError> {
        let InstrumentDefinitionParts {
            instrument_id,
            definition_revision,
            asset_class,
            primary_denomination,
            quote_currency,
            tick_size,
            lot_size,
            contract_multiplier,
            venue_mappings,
            identifiers,
            trading_status,
        } = parts;
        for (index, mapping) in venue_mappings.iter().enumerate() {
            if venue_mappings
                .iter()
                .skip(index + 1)
                .any(|candidate| candidate.venue_id == mapping.venue_id)
            {
                return Err(InstrumentError::DuplicateVenueMapping {
                    venue: mapping.venue_id.clone(),
                });
            }
        }
        let provider_assertions = provider_identity_registry.accepted().iter().chain(
            provider_identity_registry
                .conflicts()
                .iter()
                .flat_map(|conflict| conflict.competing_assertions()),
        );
        for record in provider_assertions {
            if record.instrument_id() != instrument_id {
                return Err(InstrumentError::ProviderIdentityInstrumentMismatch {
                    definition: instrument_id,
                    record: record.instrument_id(),
                });
            }
        }
        for (index, record) in identifiers.iter().enumerate() {
            if identifiers
                .iter()
                .skip(index + 1)
                .any(|candidate| candidate.identifier() == record.identifier())
            {
                return Err(InstrumentError::DuplicateExternalIdentifier);
            }
        }
        let execution_terms = InstrumentExecutionTerms::try_new(
            instrument_id,
            definition_revision,
            tick_size,
            lot_size,
            quote_currency,
            primary_denomination,
            contract_multiplier,
        )
        .map_err(InstrumentError::InvalidExecutionTerms)?;
        Ok(Self {
            instrument_id,
            asset_class,
            execution_terms,
            venue_mappings,
            provider_identity_registry,
            identifiers,
            trading_status,
        })
    }

    /// Returns the stable internal identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the broad asset family.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// Returns the explicitly typed primary settlement denomination.
    pub const fn primary_denomination(&self) -> Denomination {
        self.execution_terms.settlement_denomination()
    }

    /// Returns the exact price increment.
    pub const fn tick_size(&self) -> TickSize {
        self.execution_terms.price_tick()
    }

    /// Returns the exact price increment under execution naming.
    pub const fn price_tick(&self) -> TickSize {
        self.execution_terms.price_tick()
    }

    /// Returns the exact quantity increment.
    pub const fn lot_size(&self) -> LotSize {
        self.execution_terms.lot_size()
    }

    /// Returns the revision that binds the execution terms.
    pub const fn definition_revision(&self) -> InstrumentDefinitionRevision {
        self.execution_terms.definition_revision()
    }

    /// Returns the immutable, instrument-bound execution terms.
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.execution_terms
    }

    /// Returns the quote currency used to interpret price ticks.
    pub const fn quote_currency(&self) -> Currency {
        self.execution_terms.quote_currency()
    }

    /// Returns the currency settlement identity, or `None` for explicit asset settlement.
    pub const fn settlement_currency(&self) -> Option<Currency> {
        self.execution_terms.settlement_currency()
    }

    /// Returns the exact positive contract multiplier.
    pub const fn contract_multiplier(&self) -> Decimal {
        self.execution_terms.contract_multiplier()
    }

    /// Returns current venue mappings.
    pub fn venue_mappings(&self) -> &[VenueMapping] {
        &self.venue_mappings
    }

    /// Returns source-qualified provider identities without collapsing source namespaces.
    pub fn provider_identities(&self) -> &[ProviderIdentityRecord] {
        self.provider_identity_registry.accepted()
    }

    /// Returns quarantined competing provider assertions in deterministic order.
    pub fn provider_identity_conflicts(&self) -> &[ProviderIdentityConflict] {
        self.provider_identity_registry.conflicts()
    }

    /// Returns the checked aggregate owning accepted and quarantined provider identity evidence.
    pub const fn provider_identity_registry(&self) -> &ProviderIdentityRegistry {
        &self.provider_identity_registry
    }

    /// Resolves an accepted provider mapping at an effective instant.
    ///
    /// Quarantined evidence is never considered. Revision graph validation guarantees this cannot
    /// return an arbitrary winner from overlapping accepted assertions.
    pub fn provider_identity_at(
        &self,
        source_id: &SourceId,
        provider_instrument_id: &ProviderInstrumentId,
        at: Timestamp,
    ) -> Option<&ProviderIdentityRecord> {
        self.provider_identity_registry
            .provider_identity_at(source_id, provider_instrument_id, at)
    }

    /// Returns syntactically validated external identifiers.
    pub fn identifiers(&self) -> &[ExternalIdentifierRecord] {
        &self.identifiers
    }

    /// Returns current reference-master trading status.
    pub const fn trading_status(&self) -> TradingStatus {
        self.trading_status
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentDefinitionWire {
    instrument_id: InstrumentId,
    definition_revision: InstrumentDefinitionRevision,
    asset_class: AssetClass,
    primary_denomination: Denomination,
    quote_currency: Currency,
    tick_size: TickSize,
    lot_size: LotSize,
    contract_multiplier: Decimal,
    venue_mappings: Vec<VenueMapping>,
    #[serde(default)]
    provider_identity_registry: ProviderIdentityRegistry,
    identifiers: Vec<ExternalIdentifierRecord>,
    trading_status: TradingStatus,
}

impl<'de> Deserialize<'de> for InstrumentDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentDefinitionWire::deserialize(deserializer)?;
        Self::try_new_with_registry(
            InstrumentDefinitionParts {
                instrument_id: wire.instrument_id,
                definition_revision: wire.definition_revision,
                asset_class: wire.asset_class,
                primary_denomination: wire.primary_denomination,
                quote_currency: wire.quote_currency,
                tick_size: wire.tick_size,
                lot_size: wire.lot_size,
                contract_multiplier: wire.contract_multiplier,
                venue_mappings: wire.venue_mappings,
                identifiers: wire.identifiers,
                trading_status: wire.trading_status,
            },
            wire.provider_identity_registry,
        )
        .map_err(serde::de::Error::custom)
    }
}
