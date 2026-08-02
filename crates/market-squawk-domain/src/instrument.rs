use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{InstrumentId, MetadataRevision, OrderContractError, Timestamp, VenueId, VenueSymbol};

#[path = "instrument/definition.rs"]
mod definition;
#[path = "instrument/identifier_records.rs"]
mod identifier_records;
#[path = "instrument/provider_identities.rs"]
mod provider_identities;

pub use definition::{InstrumentDefinition, InstrumentDefinitionInput};
pub use identifier_records::{
    AssignmentVerification, ExternalIdentifier, ExternalIdentifierRecord,
    ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
    IdentifierSyntaxVerification,
};
pub use provider_identities::{
    ProviderIdentityCollection, ProviderIdentityConflict, ProviderIdentityConflictReason,
    ProviderIdentityEvidence, ProviderIdentityIngestOutcome, ProviderIdentityKey,
    ProviderIdentityLocator, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderIdentityRegistry, ProviderIdentitySupersession,
};

/// A broad instrument asset family, separate from live-evidence classifications.
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

/// Reference-master trading status. Live integrity and eligibility remain separate types.
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
    /// Execution terms were internally invalid.
    InvalidExecutionTerms(OrderContractError),
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
    /// A bounded provider-identity collection exceeded its documented capacity.
    ProviderIdentityCapacityExceeded {
        /// Collection whose bound was exceeded.
        collection: ProviderIdentityCollection,
        /// Maximum number of retained elements.
        max: usize,
    },
    /// A new revision was submitted while its natural provider key remained quarantined.
    ProviderIdentityKeyQuarantined {
        /// Natural key requiring explicit conflict resolution before revision advancement.
        key: ProviderIdentityKey,
    },
}

impl fmt::Display for InstrumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidExecutionTerms(error) => error.fmt(formatter),
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
            Self::ProviderIdentityCapacityExceeded { collection, max } => {
                write!(
                    formatter,
                    "provider identity {collection} exceeds maximum capacity {max}"
                )
            }
            Self::ProviderIdentityKeyQuarantined { key } => write!(
                formatter,
                "provider identity {}:{} is quarantined and cannot advance revisions",
                key.source_id(),
                key.provider_instrument_id()
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

/// The identity-level lifecycle transition persisted before canonical event payloads exist.
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
