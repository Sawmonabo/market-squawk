use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    Denomination, InstrumentId, LotSize, MetadataRevision, ProviderInstrumentId, SourceId,
    TickSize, Timestamp, VenueId, VenueSymbol,
};

#[path = "instrument/identifier_records.rs"]
mod identifier_records;
#[path = "instrument/provider_identities.rs"]
mod provider_identities;

pub use identifier_records::{
    AssignmentVerification, ExternalIdentifier, ExternalIdentifierRecord,
    ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
    IdentifierSyntaxVerification,
};
pub use provider_identities::{
    ProviderIdentityConflict, ProviderIdentityConflictReason, ProviderIdentityEvidence,
    ProviderIdentityKey, ProviderIdentityLocator, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderIdentitySupersession,
};

/// A broad instrument asset family, separate from Task 4 evidence classifications.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClass {
    /// Equity security.
    Equity,
    /// Fixed-income security.
    FixedIncome,
    /// Listed or OTC option.
    Option,
    /// Futures contract or venue-defined futures combination.
    Future,
    /// Foreign-exchange instrument.
    ForeignExchange,
    /// Cryptoasset spot or derivative product.
    Crypto,
    /// Commodity instrument not otherwise represented as a future.
    Commodity,
    /// Fund or exchange-traded product.
    Fund,
    /// Index or benchmark.
    Index,
    /// Cash balance or cash-equivalent instrument.
    Cash,
}

/// Reference-master trading status. Live integrity and eligibility remain separate Task 4 types.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingStatus {
    /// Instrument is active according to its reference source.
    Active,
    /// Instrument is temporarily halted.
    Halted,
    /// Instrument is inactive but retained historically.
    Inactive,
    /// Instrument is delisted and retained historically.
    Delisted,
}

/// An instrument's symbol mapping in one venue namespace.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VenueMapping {
    venue_id: VenueId,
    venue_symbol: VenueSymbol,
}

impl VenueMapping {
    /// Constructs a venue symbol mapping.
    ///
    /// Source-native IDs belong in source-qualified [`ProviderIdentityRecord`] values.
    pub fn new(venue_id: VenueId, venue_symbol: VenueSymbol) -> Self {
        Self {
            venue_id,
            venue_symbol,
        }
    }

    /// Returns the venue namespace.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the venue-native symbol.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }
}

impl fmt::Display for VenueMapping {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.venue_id, self.venue_symbol)
    }
}

/// Instrument-definition or effective-identity invariant failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstrumentError {
    /// An effective interval ended at or before its start.
    InvalidEffectiveInterval,
    /// A lifecycle transition or roll mapped an instrument to itself.
    SelfTransition,
    /// An instrument definition contained multiple current mappings for one venue.
    DuplicateVenueMapping {
        /// Duplicated venue.
        venue: VenueId,
    },
    /// An instrument definition attached the same typed identifier more than once.
    DuplicateExternalIdentifier,
    /// A provider identity referenced a different stable instrument.
    ProviderIdentityInstrumentMismatch {
        /// Stable instrument owned by the definition.
        definition: InstrumentId,
        /// Stable instrument carried by the provider record.
        record: InstrumentId,
    },
    /// A non-initial metadata revision omitted its required predecessor edge.
    MissingProviderIdentitySupersession {
        /// Revision missing its predecessor edge.
        revision: MetadataRevision,
    },
    /// A predecessor edge referenced a revision absent from the same natural key.
    MissingProviderIdentityPredecessor {
        /// Revision carrying the invalid edge.
        revision: MetadataRevision,
        /// Referenced predecessor that was not retained.
        predecessor: MetadataRevision,
    },
    /// Provider identity predecessor edges form a cycle.
    ProviderIdentitySupersessionCycle {
        /// Revision participating in the detected cycle.
        revision: MetadataRevision,
    },
    /// More than one revision claims to replace the same predecessor.
    AmbiguousProviderIdentitySuccessor {
        /// Predecessor with multiple successors.
        revision: MetadataRevision,
    },
    /// A successor overlaps or follows an open-ended predecessor.
    InvalidProviderIdentityTransition {
        /// Revision being replaced.
        predecessor: MetadataRevision,
        /// Revision claiming to replace it.
        successor: MetadataRevision,
    },
}

impl fmt::Display for InstrumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEffectiveInterval => {
                formatter.write_str("effective interval end must be later than its start")
            }
            Self::SelfTransition => {
                formatter.write_str("identity transition must change instrument")
            }
            Self::DuplicateVenueMapping { venue } => {
                write!(formatter, "duplicate current venue mapping for {venue}")
            }
            Self::DuplicateExternalIdentifier => {
                formatter.write_str("duplicate external identifier attachment")
            }
            Self::ProviderIdentityInstrumentMismatch { definition, record } => write!(
                formatter,
                "provider identity instrument {record} does not match definition {definition}"
            ),
            Self::MissingProviderIdentitySupersession { revision } => write!(
                formatter,
                "provider identity revision {} requires explicit supersession evidence",
                revision.as_source_identifier()
            ),
            Self::MissingProviderIdentityPredecessor {
                revision,
                predecessor,
            } => write!(
                formatter,
                "provider identity revision {} references missing predecessor {}",
                revision.as_source_identifier(),
                predecessor.as_source_identifier()
            ),
            Self::ProviderIdentitySupersessionCycle { revision } => write!(
                formatter,
                "provider identity supersession cycle includes {}",
                revision.as_source_identifier()
            ),
            Self::AmbiguousProviderIdentitySuccessor { revision } => write!(
                formatter,
                "provider identity revision {} has multiple successors",
                revision.as_source_identifier()
            ),
            Self::InvalidProviderIdentityTransition {
                predecessor,
                successor,
            } => write!(
                formatter,
                "provider identity transition {} -> {} overlaps or leaves the predecessor current",
                predecessor.as_source_identifier(),
                successor.as_source_identifier()
            ),
        }
    }
}

impl std::error::Error for InstrumentError {}

/// A half-open effective-time interval `[starts_at, ends_at)`.
///
/// `None` is a first-class open end; constructors never invent an end timestamp.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct EffectiveInterval {
    starts_at: Timestamp,
    ends_at: Option<Timestamp>,
}

impl EffectiveInterval {
    /// Constructs an ordered, optionally open-ended effective interval.
    ///
    /// # Errors
    ///
    /// Rejects an end at or before the start.
    pub fn new(starts_at: Timestamp, ends_at: Option<Timestamp>) -> Result<Self, InstrumentError> {
        match ends_at {
            Some(end) if end <= starts_at => Err(InstrumentError::InvalidEffectiveInterval),
            _ => Ok(Self { starts_at, ends_at }),
        }
    }

    /// Returns the inclusive interval start.
    pub const fn starts_at(self) -> Timestamp {
        self.starts_at
    }

    /// Returns the exclusive interval end, or `None` when still effective.
    pub const fn ends_at(self) -> Option<Timestamp> {
        self.ends_at
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectiveIntervalWire {
    starts_at: Timestamp,
    ends_at: Option<Timestamp>,
}

impl<'de> Deserialize<'de> for EffectiveInterval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EffectiveIntervalWire::deserialize(deserializer)?;
        Self::new(wire.starts_at, wire.ends_at).map_err(serde::de::Error::custom)
    }
}

/// A venue-symbol validity record retaining stable internal identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolIdentityRecord {
    instrument_id: InstrumentId,
    venue_id: VenueId,
    venue_symbol: VenueSymbol,
    validity: EffectiveInterval,
}

impl SymbolIdentityRecord {
    /// Constructs a symbol-history record without inventing an end time.
    pub fn new(
        instrument_id: InstrumentId,
        venue_id: VenueId,
        venue_symbol: VenueSymbol,
        validity: EffectiveInterval,
    ) -> Self {
        Self {
            instrument_id,
            venue_id,
            venue_symbol,
            validity,
        }
    }

    /// Returns the stable internal instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the venue namespace.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the source-preserved venue symbol.
    pub const fn venue_symbol(&self) -> &VenueSymbol {
        &self.venue_symbol
    }

    /// Returns the effective interval.
    pub const fn validity(&self) -> EffectiveInterval {
        self.validity
    }
}

impl fmt::Display for SymbolIdentityRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.venue_id, self.venue_symbol)
    }
}

/// The identity-level lifecycle transition persisted before canonical Task 4 event payloads exist.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum LifecycleTransitionKind {
    /// Instrument merged into a distinct stable internal identity.
    Merger {
        /// Successor instrument.
        successor: InstrumentId,
    },
    /// Instrument was delisted with no invented successor.
    Delisting,
}

/// An effective identity lifecycle transition.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct LifecycleTransition {
    instrument_id: InstrumentId,
    effective_at: Timestamp,
    kind: LifecycleTransitionKind,
}

impl LifecycleTransition {
    /// Constructs an identity lifecycle transition.
    ///
    /// # Errors
    ///
    /// Rejects a merger whose successor is the same instrument.
    pub fn new(
        instrument_id: InstrumentId,
        effective_at: Timestamp,
        kind: LifecycleTransitionKind,
    ) -> Result<Self, InstrumentError> {
        if matches!(kind, LifecycleTransitionKind::Merger { successor } if successor == instrument_id)
        {
            Err(InstrumentError::SelfTransition)
        } else {
            Ok(Self {
                instrument_id,
                effective_at,
                kind,
            })
        }
    }

    /// Returns when the transition became effective.
    pub const fn effective_at(self) -> Timestamp {
        self.effective_at
    }

    /// Returns the instrument undergoing the lifecycle transition.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the typed lifecycle transition.
    pub const fn kind(self) -> LifecycleTransitionKind {
        self.kind
    }
}

impl fmt::Display for LifecycleTransition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}@{}:",
            self.instrument_id,
            self.effective_at.unix_nanos()
        )?;
        match self.kind {
            LifecycleTransitionKind::Merger { successor } => {
                write!(formatter, "merger:{successor}")
            }
            LifecycleTransitionKind::Delisting => formatter.write_str("delisting"),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleTransitionWire {
    instrument_id: InstrumentId,
    effective_at: Timestamp,
    kind: LifecycleTransitionKind,
}

impl<'de> Deserialize<'de> for LifecycleTransition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LifecycleTransitionWire::deserialize(deserializer)?;
        Self::new(wire.instrument_id, wire.effective_at, wire.kind)
            .map_err(serde::de::Error::custom)
    }
}

/// An effective mapping from an expiring contract identity to its roll successor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct ContractRollMapping {
    from_instrument_id: InstrumentId,
    to_instrument_id: InstrumentId,
    effective_at: Timestamp,
}

impl ContractRollMapping {
    /// Constructs a contract-roll mapping between distinct instruments.
    ///
    /// # Errors
    ///
    /// Rejects a self mapping.
    pub fn new(
        from_instrument_id: InstrumentId,
        to_instrument_id: InstrumentId,
        effective_at: Timestamp,
    ) -> Result<Self, InstrumentError> {
        if from_instrument_id == to_instrument_id {
            Err(InstrumentError::SelfTransition)
        } else {
            Ok(Self {
                from_instrument_id,
                to_instrument_id,
                effective_at,
            })
        }
    }

    /// Returns the roll target instrument.
    pub const fn to_instrument_id(self) -> InstrumentId {
        self.to_instrument_id
    }

    /// Returns the expiring source instrument.
    pub const fn from_instrument_id(self) -> InstrumentId {
        self.from_instrument_id
    }

    /// Returns when the roll mapping becomes effective.
    pub const fn effective_at(self) -> Timestamp {
        self.effective_at
    }
}

impl fmt::Display for ContractRollMapping {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}->{}@{}",
            self.from_instrument_id,
            self.to_instrument_id,
            self.effective_at.unix_nanos()
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractRollMappingWire {
    from_instrument_id: InstrumentId,
    to_instrument_id: InstrumentId,
    effective_at: Timestamp,
}

impl<'de> Deserialize<'de> for ContractRollMapping {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ContractRollMappingWire::deserialize(deserializer)?;
        Self::new(
            wire.from_instrument_id,
            wire.to_instrument_id,
            wire.effective_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Current instrument reference definition with invariant-preserving private fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InstrumentDefinition {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    primary_denomination: Denomination,
    tick_size: TickSize,
    lot_size: LotSize,
    venue_mappings: Vec<VenueMapping>,
    provider_identities: Vec<ProviderIdentityRecord>,
    provider_identity_conflicts: Vec<ProviderIdentityConflict>,
    identifiers: Vec<ExternalIdentifierRecord>,
    trading_status: TradingStatus,
}

/// Complete current reference-master input for constructing [`InstrumentDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentDefinitionInput {
    /// Stable internal instrument identity.
    pub instrument_id: InstrumentId,
    /// Broad asset family.
    pub asset_class: AssetClass,
    /// Explicit settlement denomination.
    pub primary_denomination: Denomination,
    /// Exact price increment.
    pub tick_size: TickSize,
    /// Exact quantity increment.
    pub lot_size: LotSize,
    /// Current venue-symbol mappings.
    pub venue_mappings: Vec<VenueMapping>,
    /// Source-qualified provider identity records.
    pub provider_identities: Vec<ProviderIdentityRecord>,
    /// Evidence-bearing external identifier records.
    pub identifiers: Vec<ExternalIdentifierRecord>,
    /// Current reference-master trading status.
    pub trading_status: TradingStatus,
}

impl InstrumentDefinition {
    /// Constructs a current instrument definition.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::DuplicateVenueMapping`] for multiple current mappings in one
    /// venue; historical mappings belong in [`SymbolIdentityRecord`] intervals. Returns
    /// [`InstrumentError::ProviderIdentityInstrumentMismatch`] when an accepted provider assertion
    /// names a different stable instrument and [`InstrumentError::DuplicateExternalIdentifier`]
    /// when the same typed external identifier is attached more than once.
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
            asset_class,
            primary_denomination,
            tick_size,
            lot_size,
            venue_mappings,
            provider_identities,
            identifiers,
            trading_status,
        } = input;
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
        let (provider_identities, provider_identity_conflicts) =
            provider_identities::normalize_provider_identities(provider_identities)?;
        for record in &provider_identities {
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
        Ok(Self {
            instrument_id,
            asset_class,
            primary_denomination,
            tick_size,
            lot_size,
            venue_mappings,
            provider_identities,
            provider_identity_conflicts,
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
        self.primary_denomination
    }

    /// Returns the exact price increment.
    pub const fn tick_size(&self) -> TickSize {
        self.tick_size
    }

    /// Returns the exact quantity increment.
    pub const fn lot_size(&self) -> LotSize {
        self.lot_size
    }

    /// Returns current venue mappings.
    pub fn venue_mappings(&self) -> &[VenueMapping] {
        &self.venue_mappings
    }

    /// Returns source-qualified provider identities without collapsing source namespaces.
    pub fn provider_identities(&self) -> &[ProviderIdentityRecord] {
        &self.provider_identities
    }

    /// Returns quarantined competing provider assertions in deterministic order.
    pub fn provider_identity_conflicts(&self) -> &[ProviderIdentityConflict] {
        &self.provider_identity_conflicts
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
        provider_identities::provider_identity_at(
            &self.provider_identities,
            &self.provider_identity_conflicts,
            source_id,
            provider_instrument_id,
            at,
        )
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
    asset_class: AssetClass,
    primary_denomination: Denomination,
    tick_size: TickSize,
    lot_size: LotSize,
    venue_mappings: Vec<VenueMapping>,
    #[serde(default)]
    provider_identities: Vec<ProviderIdentityRecord>,
    #[serde(default)]
    provider_identity_conflicts: Vec<ProviderIdentityConflict>,
    identifiers: Vec<ExternalIdentifierRecord>,
    trading_status: TradingStatus,
}

impl<'de> Deserialize<'de> for InstrumentDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = InstrumentDefinitionWire::deserialize(deserializer)?;
        let mut provider_identities = wire.provider_identities;
        for conflict in wire.provider_identity_conflicts {
            provider_identities.extend_from_slice(conflict.competing_assertions());
        }
        Self::try_new(InstrumentDefinitionInput {
            instrument_id: wire.instrument_id,
            asset_class: wire.asset_class,
            primary_denomination: wire.primary_denomination,
            tick_size: wire.tick_size,
            lot_size: wire.lot_size,
            venue_mappings: wire.venue_mappings,
            provider_identities,
            identifiers: wire.identifiers,
            trading_status: wire.trading_status,
        })
        .map_err(serde::de::Error::custom)
    }
}
