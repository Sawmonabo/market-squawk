//! Stable, evidence-bound instrument identity for non-execution market data.

use std::fmt;
use std::marker::PhantomData;

use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{
    AssetClass, EffectiveInterval, ExternalIdentifier, ExternalIdentifierRecord,
    IdentifierEntitlement, IdentifierRightsPolicyReference, InstrumentError, ProviderIdentityKey,
    ProviderIdentityRecord, ProviderIdentityRegistry, VenueMapping,
};
use crate::{
    Currency, ExactPayloadEvidence, InstrumentId, MetadataRevision, ProviderInstrumentId,
    RevisionBoundPayloadEvidence, SourceId, Timestamp, VenueId,
};

/// Maximum UTF-8 bytes retained for an optional market-data display name.
pub const MAX_MARKET_DATA_DISPLAY_NAME_BYTES: usize = 512;
/// Maximum current venue mappings retained by one market-data definition.
pub const MAX_MARKET_DATA_VENUE_MAPPINGS: usize = 64;
/// Maximum provider-identity revisions retained by one market-data definition.
pub const MAX_MARKET_DATA_PROVIDER_IDENTITIES: usize = 256;
/// Maximum external identifier records retained by one market-data definition.
pub const MAX_MARKET_DATA_EXTERNAL_IDENTIFIERS: usize = 64;

/// Optional, bounded display text with its own source, exact payload, and rights evidence.
///
/// A source name whose reuse rights have not been admitted must remain session-only and cannot
/// construct or deserialize this type.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataDisplayName {
    value: Box<str>,
    source_id: SourceId,
    source_evidence: ExactPayloadEvidence,
    rights_policy: IdentifierRightsPolicyReference,
}

impl MarketDataDisplayName {
    /// Constructs persistable display text from explicit source and rights evidence.
    ///
    /// # Errors
    ///
    /// Rejects empty, untrimmed, oversized, or control-bearing text, an all-zero evidence digest,
    /// or a rights policy that remains unknown or restricted.
    pub fn try_new(
        value: impl Into<String>,
        source_id: SourceId,
        source_evidence: ExactPayloadEvidence,
        rights_policy: IdentifierRightsPolicyReference,
    ) -> Result<Self, MarketDataInstrumentDefinitionError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_MARKET_DATA_DISPLAY_NAME_BYTES
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(MarketDataInstrumentDefinitionError::InvalidDisplayName);
        }
        if evidence_is_empty(&source_evidence) {
            return Err(MarketDataInstrumentDefinitionError::EmptyDisplayNameEvidence);
        }
        if rights_policy.entitlement() == IdentifierEntitlement::UnknownOrRestricted {
            return Err(MarketDataInstrumentDefinitionError::DisplayNameRightsNotAdmitted);
        }
        Ok(Self {
            value: value.into_boxed_str(),
            source_id,
            source_evidence,
            rights_policy,
        })
    }

    /// Returns the exact source-authored display text.
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Returns the source namespace that supplied the text.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact payload evidence for the source representation containing the text.
    pub const fn source_evidence(&self) -> &ExactPayloadEvidence {
        &self.source_evidence
    }

    /// Returns the explicit rights decision under which the text may be retained.
    pub const fn rights_policy(&self) -> &IdentifierRightsPolicyReference {
        &self.rights_policy
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketDataDisplayNameWire {
    value: String,
    source_id: SourceId,
    source_evidence: ExactPayloadEvidence,
    rights_policy: IdentifierRightsPolicyReference,
}

impl<'de> Deserialize<'de> for MarketDataDisplayName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MarketDataDisplayNameWire::deserialize(deserializer)?;
        Self::try_new(
            wire.value,
            wire.source_id,
            wire.source_evidence,
            wire.rights_policy,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Stable reference identity for market-data display, with no execution authority or terms.
///
/// The caller supplies the repository-owned stable [`InstrumentId`]; this type never derives it
/// from a ticker, FIGI, venue symbol, provider symbol, or directory row. Tick size, lot size,
/// multiplier, trading status, execution eligibility, and execution-term construction are
/// intentionally absent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentDefinition {
    instrument_id: InstrumentId,
    reference_evidence: RevisionBoundPayloadEvidence,
    effective_interval: EffectiveInterval,
    asset_class: AssetClass,
    display_name: Option<MarketDataDisplayName>,
    quote_currency: Currency,
    quote_currency_evidence: ExactPayloadEvidence,
    venue_mappings: Box<[VenueMapping]>,
    provider_identity_registry: ProviderIdentityRegistry,
    identifiers: Box<[ExternalIdentifierRecord]>,
}

/// Complete input for constructing a checked [`MarketDataInstrumentDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataInstrumentDefinitionInput {
    /// Stable internal identity explicitly minted by the repository-owned identity authority.
    pub instrument_id: InstrumentId,
    /// Reference-source revision atomically bound to the exact payload that established it.
    pub reference_evidence: RevisionBoundPayloadEvidence,
    /// Half-open interval for this complete reference definition.
    pub effective_interval: EffectiveInterval,
    /// Broad, non-execution asset family.
    pub asset_class: AssetClass,
    /// Optional persistable name; unresolved-rights names must remain absent/session-only.
    pub display_name: Option<MarketDataDisplayName>,
    /// Explicit currency needed to interpret market-data prices.
    pub quote_currency: Currency,
    /// Exact payload evidence establishing the quote currency.
    pub quote_currency_evidence: ExactPayloadEvidence,
    /// Bounded current venue-symbol mappings.
    pub venue_mappings: Vec<VenueMapping>,
    /// Bounded source-qualified provider identity assertions.
    pub provider_identities: Vec<ProviderIdentityRecord>,
    /// Bounded evidence-bearing optional external identifiers.
    pub identifiers: Vec<ExternalIdentifierRecord>,
}

impl MarketDataInstrumentDefinition {
    /// Validates and constructs a stable non-execution market-data definition.
    ///
    /// Provider assertions are admitted through [`ProviderIdentityRegistry`] so revision graphs
    /// remain deterministic. Same-key/same-revision input is rejected rather than coalesced or
    /// quarantined: this definition contains accepted evidence only.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataInstrumentDefinitionError`] for any bound, evidence, identity,
    /// uniqueness, entitlement, or effective-time failure. Provider revision-graph failures retain
    /// their typed [`InstrumentError`] source.
    pub fn try_new(
        input: MarketDataInstrumentDefinitionInput,
    ) -> Result<Self, MarketDataInstrumentDefinitionError> {
        let MarketDataInstrumentDefinitionInput {
            instrument_id,
            reference_evidence,
            effective_interval,
            asset_class,
            display_name,
            quote_currency,
            quote_currency_evidence,
            mut venue_mappings,
            provider_identities,
            mut identifiers,
        } = input;

        if evidence_is_empty(reference_evidence.payload_evidence()) {
            return Err(MarketDataInstrumentDefinitionError::EmptyReferenceEvidence);
        }
        if evidence_is_empty(&quote_currency_evidence) {
            return Err(MarketDataInstrumentDefinitionError::EmptyQuoteCurrencyEvidence);
        }
        if venue_mappings.len() > MAX_MARKET_DATA_VENUE_MAPPINGS {
            return Err(
                MarketDataInstrumentDefinitionError::VenueMappingCapacityExceeded {
                    max: MAX_MARKET_DATA_VENUE_MAPPINGS,
                },
            );
        }
        if provider_identities.len() > MAX_MARKET_DATA_PROVIDER_IDENTITIES {
            return Err(
                MarketDataInstrumentDefinitionError::ProviderIdentityCapacityExceeded {
                    max: MAX_MARKET_DATA_PROVIDER_IDENTITIES,
                },
            );
        }
        if identifiers.len() > MAX_MARKET_DATA_EXTERNAL_IDENTIFIERS {
            return Err(
                MarketDataInstrumentDefinitionError::ExternalIdentifierCapacityExceeded {
                    max: MAX_MARKET_DATA_EXTERNAL_IDENTIFIERS,
                },
            );
        }

        venue_mappings.sort_by(|left, right| {
            left.venue_id()
                .cmp(right.venue_id())
                .then_with(|| left.venue_symbol().cmp(right.venue_symbol()))
        });
        if let Some(duplicate) = venue_mappings
            .windows(2)
            .find(|pair| pair[0].venue_id() == pair[1].venue_id())
        {
            return Err(MarketDataInstrumentDefinitionError::DuplicateVenueMapping {
                venue: duplicate[0].venue_id().clone(),
            });
        }

        for (index, record) in provider_identities.iter().enumerate() {
            if record.instrument_id() != instrument_id {
                return Err(
                    MarketDataInstrumentDefinitionError::ProviderIdentityInstrumentMismatch {
                        definition: instrument_id,
                        record: record.instrument_id(),
                    },
                );
            }
            if record.evidence().content_digest().bytes() == [0; 32] {
                return Err(MarketDataInstrumentDefinitionError::EmptyProviderIdentityEvidence);
            }
            if let Some(duplicate) = provider_identities
                .iter()
                .skip(index + 1)
                .find(|candidate| {
                    candidate.source_id() == record.source_id()
                        && candidate.provider_instrument_id() == record.provider_instrument_id()
                        && candidate.metadata_revision() == record.metadata_revision()
                })
            {
                return Err(
                    MarketDataInstrumentDefinitionError::DuplicateProviderIdentityRevision {
                        key: duplicate.key(),
                        revision: duplicate.metadata_revision().clone(),
                    },
                );
            }
        }
        let provider_identity_registry =
            ProviderIdentityRegistry::try_from_records(provider_identities)
                .map_err(MarketDataInstrumentDefinitionError::ProviderIdentityRegistry)?;
        if !provider_identity_registry.conflicts().is_empty() {
            return Err(MarketDataInstrumentDefinitionError::ProviderIdentityConflict);
        }

        identifiers.sort_by(|left, right| {
            external_identifier_kind_rank(left.identifier())
                .cmp(&external_identifier_kind_rank(right.identifier()))
                .then_with(|| {
                    left.identifier()
                        .to_string()
                        .cmp(&right.identifier().to_string())
                })
        });
        for (index, record) in identifiers.iter().enumerate() {
            if evidence_is_empty(record.source_evidence()) {
                return Err(MarketDataInstrumentDefinitionError::EmptyExternalIdentifierEvidence);
            }
            if identifiers
                .iter()
                .skip(index + 1)
                .any(|candidate| candidate.identifier() == record.identifier())
            {
                return Err(MarketDataInstrumentDefinitionError::DuplicateExternalIdentifier);
            }
        }
        Ok(Self {
            instrument_id,
            reference_evidence,
            effective_interval,
            asset_class,
            display_name,
            quote_currency,
            quote_currency_evidence,
            venue_mappings: venue_mappings.into_boxed_slice(),
            provider_identity_registry,
            identifiers: identifiers.into_boxed_slice(),
        })
    }

    /// Returns the stable internal identity supplied by the repository-owned identity authority.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the reference revision and exact payload evidence as one binding.
    pub const fn reference_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.reference_evidence
    }

    /// Returns the bounded source-authored reference revision.
    pub const fn reference_revision(&self) -> &MetadataRevision {
        self.reference_evidence.metadata_revision()
    }

    /// Returns exact payload evidence establishing the reference revision.
    pub const fn reference_payload_evidence(&self) -> &ExactPayloadEvidence {
        self.reference_evidence.payload_evidence()
    }

    /// Returns the half-open interval for the complete definition.
    pub const fn effective_interval(&self) -> EffectiveInterval {
        self.effective_interval
    }

    /// Returns the broad, non-execution asset family.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// Returns optional persistable display text with its own source and rights evidence.
    pub const fn display_name(&self) -> Option<&MarketDataDisplayName> {
        self.display_name.as_ref()
    }

    /// Returns the explicit currency used to interpret market-data prices.
    pub const fn quote_currency(&self) -> Currency {
        self.quote_currency
    }

    /// Returns exact payload evidence establishing the quote currency.
    pub const fn quote_currency_evidence(&self) -> &ExactPayloadEvidence {
        &self.quote_currency_evidence
    }

    /// Returns current venue-symbol mappings in deterministic venue/symbol order.
    pub fn venue_mappings(&self) -> &[VenueMapping] {
        &self.venue_mappings
    }

    /// Returns accepted provider identities in deterministic natural-key/revision order.
    pub fn provider_identities(&self) -> &[ProviderIdentityRecord] {
        self.provider_identity_registry.accepted()
    }

    /// Resolves an accepted provider identity at an effective instant.
    pub fn provider_identity_at(
        &self,
        source_id: &SourceId,
        provider_instrument_id: &ProviderInstrumentId,
        at: Timestamp,
    ) -> Option<&ProviderIdentityRecord> {
        self.provider_identity_registry
            .provider_identity_at(source_id, provider_instrument_id, at)
    }

    /// Returns evidence-bearing external identifiers.
    pub fn identifiers(&self) -> &[ExternalIdentifierRecord] {
        &self.identifiers
    }
}

#[derive(Serialize)]
struct MarketDataInstrumentDefinitionWireRef<'a> {
    instrument_id: InstrumentId,
    reference_evidence: &'a RevisionBoundPayloadEvidence,
    effective_interval: EffectiveInterval,
    asset_class: AssetClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<&'a MarketDataDisplayName>,
    quote_currency: Currency,
    quote_currency_evidence: &'a ExactPayloadEvidence,
    venue_mappings: &'a [VenueMapping],
    provider_identities: &'a [ProviderIdentityRecord],
    identifiers: &'a [ExternalIdentifierRecord],
}

impl Serialize for MarketDataInstrumentDefinition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        MarketDataInstrumentDefinitionWireRef {
            instrument_id: self.instrument_id,
            reference_evidence: &self.reference_evidence,
            effective_interval: self.effective_interval,
            asset_class: self.asset_class,
            display_name: self.display_name.as_ref(),
            quote_currency: self.quote_currency,
            quote_currency_evidence: &self.quote_currency_evidence,
            venue_mappings: &self.venue_mappings,
            provider_identities: self.provider_identity_registry.accepted(),
            identifiers: &self.identifiers,
        }
        .serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketDataInstrumentDefinitionWire {
    instrument_id: InstrumentId,
    reference_evidence: RevisionBoundPayloadEvidence,
    effective_interval: EffectiveInterval,
    asset_class: AssetClass,
    #[serde(default)]
    display_name: Option<MarketDataDisplayName>,
    quote_currency: Currency,
    quote_currency_evidence: ExactPayloadEvidence,
    venue_mappings: BoundedVec<VenueMapping, MAX_MARKET_DATA_VENUE_MAPPINGS>,
    provider_identities: BoundedVec<ProviderIdentityRecord, MAX_MARKET_DATA_PROVIDER_IDENTITIES>,
    identifiers: BoundedVec<ExternalIdentifierRecord, MAX_MARKET_DATA_EXTERNAL_IDENTIFIERS>,
}

impl<'de> Deserialize<'de> for MarketDataInstrumentDefinition {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = MarketDataInstrumentDefinitionWire::deserialize(deserializer)?;
        Self::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: wire.instrument_id,
            reference_evidence: wire.reference_evidence,
            effective_interval: wire.effective_interval,
            asset_class: wire.asset_class,
            display_name: wire.display_name,
            quote_currency: wire.quote_currency,
            quote_currency_evidence: wire.quote_currency_evidence,
            venue_mappings: wire.venue_mappings.into_inner(),
            provider_identities: wire.provider_identities.into_inner(),
            identifiers: wire.identifiers.into_inner(),
        })
        .map_err(serde::de::Error::custom)
    }
}

/// Failure to construct or deserialize a non-execution market-data definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarketDataInstrumentDefinitionError {
    /// Optional display text was empty, untrimmed, oversized, or contained a control character.
    InvalidDisplayName,
    /// The display-name payload digest was the all-zero sentinel.
    EmptyDisplayNameEvidence,
    /// The display-name rights policy was unknown or restricted.
    DisplayNameRightsNotAdmitted,
    /// The reference-definition payload digest was the all-zero sentinel.
    EmptyReferenceEvidence,
    /// The quote-currency payload digest was the all-zero sentinel.
    EmptyQuoteCurrencyEvidence,
    /// A venue-mapping collection exceeded its hard bound.
    VenueMappingCapacityExceeded {
        /// Maximum permitted mappings.
        max: usize,
    },
    /// A provider-identity collection exceeded its hard bound.
    ProviderIdentityCapacityExceeded {
        /// Maximum permitted records.
        max: usize,
    },
    /// An external-identifier collection exceeded its hard bound.
    ExternalIdentifierCapacityExceeded {
        /// Maximum permitted records.
        max: usize,
    },
    /// More than one current symbol mapping was supplied for a venue.
    DuplicateVenueMapping {
        /// Duplicated venue namespace.
        venue: VenueId,
    },
    /// A provider identity referenced a different stable instrument.
    ProviderIdentityInstrumentMismatch {
        /// Stable identity owned by the definition.
        definition: InstrumentId,
        /// Stable identity carried by the provider assertion.
        record: InstrumentId,
    },
    /// The same provider natural key and metadata revision appeared more than once.
    DuplicateProviderIdentityRevision {
        /// Source-qualified provider key.
        key: ProviderIdentityKey,
        /// Repeated provider revision.
        revision: MetadataRevision,
    },
    /// A provider identity carried an all-zero evidence digest.
    EmptyProviderIdentityEvidence,
    /// Provider normalization produced a quarantined conflict instead of accepted evidence.
    ProviderIdentityConflict,
    /// Provider revision-graph validation failed.
    ProviderIdentityRegistry(InstrumentError),
    /// The same typed external identifier was attached more than once.
    DuplicateExternalIdentifier,
    /// An external identifier carried an all-zero evidence digest.
    EmptyExternalIdentifierEvidence,
}

impl fmt::Display for MarketDataInstrumentDefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDisplayName => write!(
                formatter,
                "market-data display name must be nonempty, trimmed, control-free, and at most {MAX_MARKET_DATA_DISPLAY_NAME_BYTES} UTF-8 bytes"
            ),
            Self::EmptyDisplayNameEvidence => formatter
                .write_str("market-data display name requires nonzero exact payload evidence"),
            Self::DisplayNameRightsNotAdmitted => formatter.write_str(
                "market-data display name rights must be explicitly admitted before persistence",
            ),
            Self::EmptyReferenceEvidence => formatter.write_str(
                "market-data reference revision requires nonzero exact payload evidence",
            ),
            Self::EmptyQuoteCurrencyEvidence => formatter
                .write_str("market-data quote currency requires nonzero exact payload evidence"),
            Self::VenueMappingCapacityExceeded { max } => {
                write!(formatter, "market-data venue mappings exceed maximum {max}")
            }
            Self::ProviderIdentityCapacityExceeded { max } => {
                write!(
                    formatter,
                    "market-data provider identities exceed maximum {max}"
                )
            }
            Self::ExternalIdentifierCapacityExceeded { max } => {
                write!(
                    formatter,
                    "market-data external identifiers exceed maximum {max}"
                )
            }
            Self::DuplicateVenueMapping { venue } => {
                write!(formatter, "duplicate market-data venue mapping for {venue}")
            }
            Self::ProviderIdentityInstrumentMismatch { definition, record } => write!(
                formatter,
                "provider identity instrument {record} does not match market-data definition {definition}"
            ),
            Self::DuplicateProviderIdentityRevision { key, revision } => write!(
                formatter,
                "duplicate market-data provider identity revision {} for {}:{}",
                revision.as_source_identifier(),
                key.source_id(),
                key.provider_instrument_id()
            ),
            Self::EmptyProviderIdentityEvidence => formatter
                .write_str("market-data provider identity requires nonzero exact payload evidence"),
            Self::ProviderIdentityConflict => formatter.write_str(
                "market-data definition accepts no quarantined provider identity conflicts",
            ),
            Self::ProviderIdentityRegistry(error) => {
                write!(
                    formatter,
                    "invalid market-data provider identity graph: {error}"
                )
            }
            Self::DuplicateExternalIdentifier => {
                formatter.write_str("duplicate market-data external identifier record")
            }
            Self::EmptyExternalIdentifierEvidence => formatter.write_str(
                "market-data external identifier requires nonzero exact payload evidence",
            ),
        }
    }
}

impl std::error::Error for MarketDataInstrumentDefinitionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ProviderIdentityRegistry(error) => Some(error),
            _ => None,
        }
    }
}

struct BoundedVec<T, const MAX: usize>(Vec<T>);

impl<T, const MAX: usize> BoundedVec<T, MAX> {
    fn into_inner(self) -> Vec<T> {
        self.0
    }
}

impl<'de, T, const MAX: usize> Deserialize<'de> for BoundedVec<T, MAX>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoundedVecVisitor<T, const MAX: usize>(PhantomData<T>);

        impl<'de, T, const MAX: usize> Visitor<'de> for BoundedVecVisitor<T, MAX>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T, MAX>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a sequence containing at most {MAX} elements")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let capacity = sequence.size_hint().unwrap_or(0).min(MAX);
                let mut values = Vec::with_capacity(capacity);
                while values.len() < MAX {
                    let Some(value) = sequence.next_element()? else {
                        return Ok(BoundedVec(values));
                    };
                    values.push(value);
                }
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(serde::de::Error::custom(format_args!(
                        "sequence exceeds maximum of {MAX} elements"
                    )));
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVecVisitor(PhantomData))
    }
}

fn evidence_is_empty(evidence: &ExactPayloadEvidence) -> bool {
    evidence.content_digest().bytes() == [0; 32]
}

const fn external_identifier_kind_rank(identifier: &ExternalIdentifier) -> u8 {
    match identifier {
        ExternalIdentifier::Ticker(_) => 0,
        ExternalIdentifier::Cusip(_) => 1,
        ExternalIdentifier::Isin(_) => 2,
        ExternalIdentifier::Sedol(_) => 3,
        ExternalIdentifier::Figi(_) => 4,
        ExternalIdentifier::OccOption(_) => 5,
        ExternalIdentifier::Futures(_) => 6,
        ExternalIdentifier::CryptoPair(_) => 7,
        ExternalIdentifier::ChainAddress(_) => 8,
    }
}
